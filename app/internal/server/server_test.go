package server

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"simple-chat/internal/pow"
	"simple-chat/internal/upstream"
)

// upstreamFixture answers the full request pipeline over httptest.
type upstreamFixture struct {
	t           *testing.T
	srv         *httptest.Server
	sessions    atomic.Int64
	deletes     atomic.Int64
	completions atomic.Int64
}

func newUpstreamFixture(t *testing.T) *upstreamFixture {
	f := &upstreamFixture{t: t}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"user": map[string]any{"token": "tok", "id": "u1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.sessions.Add(1)
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"chat_session": map[string]any{"id": "sess1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		f.deletes.Add(1)
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": nil,
		}})
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"challenge": solvableChallenge(r),
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-ds-pow-response") == "" {
			w.WriteHeader(400)
			return
		}
		f.completions.Add(1)
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		// App encoding (apk-alignment.md I3, revised): encodeDefaults
		// always sends both flags as literal booleans.
		if body["thinking_enabled"] != true || body["search_enabled"] != false {
			f.t.Errorf("thinking must default on, search off: %v", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"hello\"}]}}}\n")
		io.WriteString(w, "data: {\"v\":\" world\"}\n")
		io.WriteString(w, "data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":7}]}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	})
	f.srv = httptest.NewServer(mux)
	return f
}

// solvableChallenge returns a real PoW challenge (answer 42).
func solvableChallenge(r *http.Request) map[string]any {
	var body map[string]any
	json.NewDecoder(r.Body).Decode(&body)
	target, _ := body["target_path"].(string)
	h := pow.HashV1([]byte("testsalt_1700000000_42"))
	return map[string]any{
		"algorithm":   "HashV1",
		"challenge":   hex.EncodeToString(h[:]),
		"salt":        "testsalt",
		"expire_at":   1700000000,
		"difficulty":  144000,
		"signature":   "sig",
		"target_path": target,
	}
}

// mc is a shorthand for map[string]any.
type mc = map[string]any

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func newTestServer(t *testing.T, upstreamURL string) *httptest.Server {
	srv, err := NewServer(Config{
		UpstreamBase: upstreamURL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(srv.Handler())
}

func TestChatCompletionsNonStream(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "hello world" {
		t.Errorf("choices wrong: %+v", out.Choices)
	}
	if out.Choices[0].Message.Role != "assistant" {
		t.Errorf("role = %q", out.Choices[0].Message.Role)
	}
	if out.Usage.TotalTokens != 7 {
		t.Errorf("usage = %+v", out.Usage)
	}
	// Session lifecycle is app-like now (apk-behavior.md §8): the session
	// persists after the turn — no delete to wait for. Default-policy
	// keep/cap-evict behavior is covered by session_policy_test.go.
}

func TestChatCompletionsStream(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content type = %s", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	for _, want := range []string{
		`"object":"chat.completion.chunk"`,
		`"role":"assistant"`,
		`"content":"hello"`,
		`"content":" world"`,
		`"finish_reason":"stop"`,
		`"total_tokens":7`,
		"data: [DONE]",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("stream missing %s\nstream: %s", want, s)
		}
	}
}

func TestChatCompletionsInvalidModel(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if up.completions.Load() != 0 {
		t.Error("no upstream completion should fire for a bad model")
	}
}

// TestChatCompletionsToolsRejected used to assert tools → 400. Inverted by
// TASK_STRIP_FIELDS: tools are silently stripped and the request succeeds —
// covered by strip_fields_test.go (TestChatCompletionsToolsStrippedNotRejected).

func TestModelsEndpoint(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "deepseek-flash") {
		t.Errorf("body = %s", body)
	}
}

func TestHealthz(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAccountsFilePermissionsWarning(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/accounts.json"
	if err := writeAccountsFile(path, []upstream.Account{{Mobile: "1", Password: "p"}}, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := loadAccounts(t, path)
	if err == nil {
		t.Fatal("expected a permissions warning/error for 0644 accounts file")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Errorf("error should mention 0600: %v", err)
	}
}

// The upstream signals the per-account parallel generation limit as a
// mid-stream `event: hint` error ("parallel_chat_limit"). With the retry
// ladder (R1) a single retryable occurrence is retried on a fresh account —
// so this fixture fails EVERY completion with the limit, pinning the
// exhausted-ladder mapping: 429 + Retry-After, not an empty 200 completion.
func TestParallelChatLimitMapsTo429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/users/login":
			writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
				"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
					"user": map[string]any{"token": "tok", "id": "u1"},
				},
			}})
		case "/api/v0/chat_session/create":
			writeJSON(w, mc{
				"code": 0, "msg": "", "data": mc{
					"biz_code": 0, "biz_msg": "", "biz_data": mc{
						"chat_session": mc{"id": "s1"},
					},
				},
			})
		case "/api/v0/chat/create_pow_challenge":
			writeJSON(w, mc{
				"code": 0, "msg": "", "data": mc{
					"biz_code": 0, "biz_msg": "", "biz_data": mc{
						"challenge": solvableChallenge(r),
					},
				},
			})
		case "/api/v0/chat/completion":
			calls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"another generation is running\",\"clear_response\":true,\"finish_reason\":\"parallel_chat_limit\"}\n\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		}
	}))
	defer srv.Close()
	gw := newTestServer(t, srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 429, body = %s", resp.StatusCode, body)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
	var got struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Error.Code != "parallel_chat_limit" {
		t.Errorf("code = %q, want parallel_chat_limit", got.Error.Code)
	}
}

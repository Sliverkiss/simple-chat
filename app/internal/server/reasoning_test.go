package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// The reasoning-mode contract (deep thinking default ON):
//   (a) default request → upstream payload thinking_enabled:true
//   (b) opt-out → thinking_enabled:false
//   (c) malformed switch → 400, no upstream completion
//   (d) stream THINK+RESPONSE → reasoning_content deltas before content deltas,
//       one shared chunk id
//   (e) non-stream THINK+RESPONSE → both message fields populated
//   (f) THINK-only stream → reasoning_content + empty content + clean stop
//   (g) no-THINK stream → unchanged behavior (content only)
//   (h) think→response transition mid-stream

// reasoningFixture is an upstream mock that records the thinking_enabled flag
// of every completion payload and emits a THINK+RESPONSE SSE stream.
type reasoningFixture struct {
	srv         *httptest.Server
	thinking    []bool // thinking_enabled observed per completion
	complete    atomic.Int64
	deleteCount atomic.Int64
}

// newReasoningFixture builds the upstream. thinkStream controls the SSE body:
// THINK+RESPONSE mixed, THINK-only, or RESPONSE-only.
func newReasoningFixture(t *testing.T, streamBody string) *reasoningFixture {
	f := &reasoningFixture{}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"user": map[string]any{"token": "tok", "id": "u1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"chat_session": map[string]any{"id": "sess1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		f.deleteCount.Add(1)
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
		f.complete.Add(1)
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		thinking, _ := body["thinking_enabled"].(bool)
		f.thinking = append(f.thinking, thinking)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, streamBody)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// mixedStream: THINK fragment first, then RESPONSE; simulates think→response
// transition mid-stream (fragment APPEND after the initial payload).
const mixedStream = "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"let me \"}]}}}\n" +
	"data: {\"v\":\"compute: \"}\n" +
	"data: {\"p\":\"response/fragments\",\"o\":\"APPEND\",\"v\":[{\"type\":\"RESPONSE\",\"content\":\"42\"}]}\n" +
	"data: {\"v\":\" it is\"}\n" +
	"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":9}]}\n" +
	"data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n" +
	"event: close\ndata: {}\n"

// thinkOnlyStream: reasoning arrives but the answer never does — still a
// valid completion with empty content.
const thinkOnlyStream = "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"hm \"}]}}}\n" +
	"data: {\"v\":\"...\"}\n" +
	"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":5}]}\n" +
	"data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n" +
	"event: close\ndata: {}\n"

// responseOnlyStream: upstream ignored thinking — zero THINK fragments is
// NOT an error (best-effort thinking).
const responseOnlyStream = "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"plain \"}]}}}\n" +
	"data: {\"v\":\"answer\"}\n" +
	"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":4}]}\n" +
	"data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n" +
	"event: close\ndata: {}\n"

func postReasoningCompletion(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	raw, _ := io.ReadAll(resp.Body)
	return resp, string(raw)
}

const defaultBody = `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
const optOutBody = `{"model":"deepseek-flash","thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`

// (a) default request → thinking_enabled:true upstream.
func TestReasoningDefaultOnSendsThinkingEnabled(t *testing.T) {
	f := newReasoningFixture(t, responseOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, _ := postReasoningCompletion(t, srv.URL, defaultBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.thinking) != 1 || !f.thinking[0] {
		t.Errorf("upstream thinking_enabled = %v, want true", f.thinking)
	}
}

// (b) opt-out → thinking_enabled:false upstream.
func TestReasoningOptOutSendsThinkingDisabled(t *testing.T) {
	f := newReasoningFixture(t, responseOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, _ := postReasoningCompletion(t, srv.URL, optOutBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.thinking) != 1 || f.thinking[0] {
		t.Errorf("upstream thinking_enabled = %v, want false", f.thinking)
	}
}

// (c) malformed switch → 400, nothing reaches upstream.
func TestReasoningMalformedSwitchRejected(t *testing.T) {
	f := newReasoningFixture(t, responseOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL,
		`{"model":"deepseek-flash","thinking":{"type":"maybe"},"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, body)
	}
	if f.complete.Load() != 0 {
		t.Error("no upstream completion should fire for a malformed switch")
	}
}

// (d) stream THINK+RESPONSE: reasoning deltas first, content after, one id.
func TestStreamReasoningBeforeContent(t *testing.T) {
	f := newReasoningFixture(t, mixedStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, `{"model":"deepseek-flash","stream":true,`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content type = %s", ct)
	}

	type chunk struct {
		ID      string `json:"id"`
		Choices []struct {
			Delta struct {
				Role            string `json:"role"`
				Content         string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	var chunks []chunk
	id := ""
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var c chunk
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", line, err)
		}
		if len(c.Choices) == 0 {
			continue // error/usage-only frames
		}
		if c.ID != "" {
			if id == "" {
				id = c.ID
			} else if c.ID != id {
				t.Errorf("chunk id drifted: %q vs %q", c.ID, id)
			}
		}
		chunks = append(chunks, c)
	}

	var reasoning, content []string
	firstRole := ""
	for _, c := range chunks {
		d := c.Choices[0].Delta
		if d.Role != "" {
			firstRole = d.Role
		}
		if d.ReasoningContent != "" {
			reasoning = append(reasoning, d.ReasoningContent)
		}
		if d.Content != "" {
			content = append(content, d.Content)
		}
	}
	if got := strings.Join(reasoning, ""); got != "let me compute: " {
		t.Errorf("reasoning = %q, want %q", got, "let me compute: ")
	}
	if got := strings.Join(content, ""); got != "42 it is" {
		t.Errorf("content = %q, want %q", got, "42 it is")
	}
	if firstRole != "assistant" {
		t.Errorf("first delta role = %q, want assistant", firstRole)
	}
	// Order: every reasoning delta must precede every content delta.
	firstContent := -1
	lastReasoning := -1
	for i, c := range chunks {
		d := c.Choices[0].Delta
		if d.Content != "" && firstContent < 0 {
			firstContent = i
		}
		if d.ReasoningContent != "" {
			lastReasoning = i
		}
	}
	if firstContent < 0 || lastReasoning > firstContent {
		t.Errorf("reasoning (idx %d) must precede content (idx %d)", lastReasoning, firstContent)
	}
	// Clean terminal state even with thinking on.
	if !strings.Contains(body, `"finish_reason":"stop"`) || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("stream must finish cleanly: %s", body)
	}
}

// (e) non-stream THINK+RESPONSE → both message fields populated.
func TestNonStreamReasoningAndContent(t *testing.T) {
	f := newReasoningFixture(t, mixedStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, defaultBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Role             string `json:"role"`
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 {
		t.Fatalf("choices: %s", body)
	}
	m := out.Choices[0].Message
	if m.Role != "assistant" || m.Content != "42 it is" {
		t.Errorf("message wrong: %+v", m)
	}
	if m.ReasoningContent != "let me compute: " {
		t.Errorf("reasoning_content = %q, want %q", m.ReasoningContent, "let me compute: ")
	}
	if out.Usage.TotalTokens != 9 {
		t.Errorf("usage = %+v (must pass through thinking-inclusive usage once)", out.Usage)
	}
}

// (f) THINK-only stream → reasoning_content + empty content + clean stop.
func TestNonStreamThinkOnlyIsValidCompletion(t *testing.T) {
	f := newReasoningFixture(t, thinkOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, defaultBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("think-only must be a clean completion: %s", body)
	}
	if out.Choices[0].Message.Content != "" {
		t.Errorf("content = %q, want empty", out.Choices[0].Message.Content)
	}
	if out.Choices[0].Message.ReasoningContent != "hm ..." {
		t.Errorf("reasoning_content = %q, want %q", out.Choices[0].Message.ReasoningContent, "hm ...")
	}
}

// THINK-only STREAM variant: reasoning deltas + empty content + clean stop,
// and — critically — the empty-output retry ladder must NOT re-run it.
func TestStreamThinkOnlyCleanStop(t *testing.T) {
	f := newReasoningFixture(t, thinkOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, `{"model":"deepseek-flash","stream":true,`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"reasoning_content":"hm "`) ||
		!strings.Contains(body, `"reasoning_content":"..."`) {
		t.Errorf("reasoning deltas missing: %s", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("must finish cleanly: %s", body)
	}
	if f.complete.Load() != 1 {
		t.Errorf("completions = %d, want 1 (thinking-only output must not trigger the empty-output retry)", f.complete.Load())
	}
}

// (g) no-THINK stream with thinking requested → unchanged behavior.
func TestNoThinkStreamUnchanged(t *testing.T) {
	f := newReasoningFixture(t, responseOnlyStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, `{"model":"deepseek-flash","stream":true,`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if strings.Contains(body, "reasoning_content") {
		t.Errorf("no THINK fragments → no reasoning_content in output: %s", body)
	}
	if !strings.Contains(body, `"content":"plain "`) || !strings.Contains(body, `"content":"answer"`) {
		t.Errorf("content deltas missing: %s", body)
	}
}

// (h) opt-out with a THINK-shaped upstream stream: reasoning was not
// requested — thinking deltas must still not leak into content.
func TestOptOutDropsThinkDeltas(t *testing.T) {
	f := newReasoningFixture(t, mixedStream)
	srv := newTestServer(t, f.srv.URL)
	defer srv.Close()

	resp, body := postReasoningCompletion(t, srv.URL, `{"model":"deepseek-flash","thinking":{"type":"disabled"},`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out.Choices[0].Message.Content != "42 it is" {
		t.Errorf("content = %q", out.Choices[0].Message.Content)
	}
	if out.Choices[0].Message.ReasoningContent != "" {
		t.Errorf("opt-out must not emit reasoning_content: %q", out.Choices[0].Message.ReasoningContent)
	}
}

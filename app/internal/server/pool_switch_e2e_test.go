package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// These HTTP-only tests observe the physical identity in the login-derived
// bearer token, not the pool's random order or its internal lease state.
type switchCall struct {
	identity, session, prompt string
}

type switchFixture struct {
	server  *httptest.Server
	mu      sync.Mutex
	created map[string]string // session -> physical identity
	calls   []switchCall
	failure string // first completion only
}

const switchPrompt = "user: plain retry prompt\n"
const switchRequest = `{"model":"deepseek-flash","messages":[{"role":"user","content":"plain retry prompt"}]}`
const switchStreamRequest = `{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"plain retry prompt"}]}`

func newSwitchFixture(t *testing.T, failure string) *switchFixture {
	t.Helper()
	f := &switchFixture{created: make(map[string]string), failure: failure}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Mobile string `json:"mobile"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mobile == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok-" + req.Mobile, "id": req.Mobile}}}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		identity := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-")
		f.mu.Lock()
		id := fmt.Sprintf("switch-session-%d", len(f.created)+1)
		f.created[id] = identity
		f.mu.Unlock()
		sessOK(w, id)
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Session string `json:"chat_session_id"`
			Prompt  string `json:"prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		identity := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-")
		f.mu.Lock()
		f.calls = append(f.calls, switchCall{identity, body.Session, body.Prompt})
		n := len(f.calls)
		f.mu.Unlock()
		if n == 1 {
			switch f.failure {
			case "parallel":
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"another generation running\",\"clear_response\":true,\"finish_reason\":\"parallel_chat_limit\"}\n\n")
				io.WriteString(w, "event: close\ndata: {}\n")
			case "http500":
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, "temporary upstream failure")
			case "cut":
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
				w.(http.Flusher).Flush()
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					conn.Close()
				}
			case "empty":
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
				io.WriteString(w, "event: close\ndata: {}\n")
			case "after-delta":
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
				io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"failed\",\"clear_response\":false,\"finish_reason\":\"parallel_chat_limit\"}\n\n")
				io.WriteString(w, "event: close\ndata: {}\n")
			}
			return
		}
		streamOK(w, "switched-answer")
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *switchFixture) snapshot() ([]switchCall, map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := append([]switchCall(nil), f.calls...)
	created := make(map[string]string, len(f.created))
	for k, v := range f.created {
		created[k] = v
	}
	return calls, created
}

func newSwitchGateway(t *testing.T, f *switchFixture, accounts []upstream.Account) *httptest.Server {
	t.Helper()
	s, err := NewServer(Config{UpstreamBase: f.server.URL, Accounts: accounts, RandomSeed: 1, MaxInflight: 1, QueueWait: 50 * time.Millisecond, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(s.Handler())
	t.Cleanup(func() { gw.Close(); s.Shutdown() })
	return gw
}

func postSwitchChat(t *testing.T, gw *httptest.Server, streaming bool) (int, string) {
	t.Helper()
	body := switchRequest
	if streaming {
		body = switchStreamRequest
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(got)
}

func switchAccounts() []upstream.Account {
	return []upstream.Account{{Mobile: "13800000000", Password: "fixture"}, {Mobile: "13900000000", Password: "fixture"}}
}

func assertSwitchCalls(t *testing.T, f *switchFixture, count int) []switchCall {
	t.Helper()
	calls, created := f.snapshot()
	if len(calls) != count {
		t.Fatalf("completion calls = %+v, want %d", calls, count)
	}
	for _, c := range calls {
		if c.identity == "" || c.session == "" || created[c.session] != c.identity || c.prompt != switchPrompt {
			t.Errorf("completion must carry its own account's session and unchanged plain prompt: call=%+v created=%v", c, created)
		}
	}
	if count == 2 {
		if calls[0].identity == calls[1].identity {
			t.Errorf("retry reused physical identity %q: %+v", calls[0].identity, calls)
		}
		if calls[0].session == calls[1].session {
			t.Errorf("retry reused session: %+v", calls)
		}
	}
	return calls
}

func TestPlainChatRetryUsesDistinctAccountAndSession(t *testing.T) {
	for _, failure := range []string{"parallel", "http500", "cut"} {
		t.Run(failure, func(t *testing.T) {
			f := newSwitchFixture(t, failure)
			gw := newSwitchGateway(t, f, switchAccounts())
			status, body := postSwitchChat(t, gw, false)
			if status != http.StatusOK || !strings.Contains(body, "switched-answer") || strings.Contains(body, "partial") {
				t.Errorf("retry response = %d %s, want only second account's answer", status, body)
			}
			assertSwitchCalls(t, f, 2)
		})
	}
}

func TestPlainChatStreamingRetryBeforeFirstDeltaSwitchesIdentity(t *testing.T) {
	f := newSwitchFixture(t, "parallel")
	gw := newSwitchGateway(t, f, switchAccounts())
	status, body := postSwitchChat(t, gw, true)
	if status != http.StatusOK || !strings.Contains(body, `"content":"switched-answer"`) || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("stream retry = %d %s", status, body)
	}
	assertSwitchCalls(t, f, 2)
}

func TestPlainChatNoRetryAfterFirstSSEDelta(t *testing.T) {
	f := newSwitchFixture(t, "after-delta")
	gw := newSwitchGateway(t, f, switchAccounts())
	status, body := postSwitchChat(t, gw, true)
	if status != http.StatusOK || !strings.Contains(body, `"content":"partial"`) || !strings.Contains(body, `"code":"stream_error"`) || strings.Contains(body, "switched-answer") {
		t.Errorf("committed stream = %d %s", status, body)
	}
	assertSwitchCalls(t, f, 1)
}

func TestPlainChatSingleAccountRetryFailsSafelyWithoutSelfRetry(t *testing.T) {
	for _, tc := range []struct {
		failure string
		status  int
		code    string
	}{
		{"parallel", http.StatusTooManyRequests, `"code":"parallel_chat_limit"`},
		{"http500", http.StatusBadGateway, `"code":"upstream_failure"`},
		{"cut", http.StatusBadGateway, `"code":"stream_error"`},
		{"empty", http.StatusOK, `"content":""`},
	} {
		t.Run(tc.failure, func(t *testing.T) {
			f := newSwitchFixture(t, tc.failure)
			gw := newSwitchGateway(t, f, switchAccounts()[:1])
			status, body := postSwitchChat(t, gw, false)
			if status != tc.status || !strings.Contains(body, tc.code) || strings.Contains(body, "switched-answer") {
				t.Errorf("single-account fallback = %d %s, want %d %s", status, body, tc.status, tc.code)
			}
			assertSwitchCalls(t, f, 1)
		})
	}
}

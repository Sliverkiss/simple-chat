package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"simple-chat/internal/upstream"
)

// streamErrorFixture serves one completion that dies mid-stream with a hint
// error, so the gateway must terminate the client stream without a clean
// finish_reason:"stop" frame.
func streamErrorFixture(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/users/login":
			writeJSON(w, mc{
				"code": 0, "msg": "", "data": mc{
					"biz_code": 0, "biz_msg": "", "biz_data": mc{
						"user": mc{"token": "tok", "id": "u1"},
					},
				},
			})
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
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
			io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"boom\",\"clear_response\":false,\"finish_reason\":\"some_failure\"}\n\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		default:
			writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
		}
	}))
	return srv, &calls
}

// After a mid-stream error frame the gateway must NOT emit a clean
// finish_reason:"stop" chunk — that makes truncated output look successful to
// standard OpenAI clients. [DONE] still terminates the stream.
func TestStreamErrorNeverEmitsCleanStop(t *testing.T) {
	up, _ := streamErrorFixture(t)
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (stream already committed)", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)

	if !strings.Contains(s, `"code":"stream_error"`) {
		t.Errorf("stream missing the error frame\nstream: %s", s)
	}
	if strings.Contains(s, `"finish_reason":"stop"`) {
		t.Errorf("clean stop frame must not follow a stream error\nstream: %s", s)
	}
	if !strings.Contains(s, "data: [DONE]") {
		t.Errorf("stream must still end with [DONE]\nstream: %s", s)
	}
	// The delta delivered before the error must survive — truncation is the
	// error's fault, not a reason to drop already-streamed content.
	if !strings.Contains(s, `"content":"partial"`) {
		t.Errorf("pre-error delta must be forwarded\nstream: %s", s)
	}
}

// Non-stream path: error surfaces as an HTTP error response, never as a
// finish_reason:"stop" completion (already correct — regression guard).
func TestNonStreamErrorSurfacesAsHTTPError(t *testing.T) {
	up, _ := streamErrorFixture(t)
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("non-stream error must not be a 200 completion: %s", body)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), `"finish_reason":"stop"`) {
		t.Errorf("non-stream error response must not claim clean stop: %s", body)
	}
}

// Sanity: the fixture upstream is reachable through the normal pool path.
func TestStreamErrorFixtureReachable(t *testing.T) {
	up, calls := streamErrorFixture(t)
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Errorf("completion calls = %d, want 1", calls.Load())
	}
	_ = upstream.DefaultBaseURL // keep the import honest in solo runs
}

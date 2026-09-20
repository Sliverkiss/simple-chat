package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"simple-chat/internal/upstream"
)

// ladderMux builds an upstream mux with standard auth/session/pow/delete
// handlers and a custom completion handler.
func ladderMux(t *testing.T, completion func(w http.ResponseWriter, r *http.Request)) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		sessOK(w, "s1")
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		powOK(w, r)
	})
	mux.HandleFunc("POST /api/v0/chat/completion", completion)
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	return mux
}

// A stream that emits one delta, then errors, then closes: the delta is
// delivered, the error arrives as an error frame, and the terminating chunk
// carries the upstream's finish_reason — never a clean "stop" (R2 + R1
// no-retry-after-first-byte interplay).
func TestStreamMidErrorAfterDeltaDeliversErrorFrame(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
		io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"died\",\"clear_response\":false,\"finish_reason\":\"server_busy\"}\n\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	for _, want := range []string{
		`"content":"partial"`,
		`"code":"stream_error"`,
		`"finish_reason":"server_busy"`,
		"data: [DONE]",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("stream missing %s\nstream: %s", want, s)
		}
	}
	if strings.Contains(s, `"finish_reason":"stop"`) {
		t.Errorf("clean stop after mid-stream error is forbidden\nstream: %s", s)
	}
}

// A transport cut mid-stream (post-delta) surfaces an error frame, not a
// clean stop.
func TestStreamTransportCutAfterDeltaSurfacesError(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		hj, _ := w.(http.Hijacker)
		if conn, _, err := hj.Hijack(); err == nil {
			conn.Close() // transport cut without close event
		}
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"content":"partial"`) {
		t.Errorf("pre-cut delta must be delivered: %s", s)
	}
	if strings.Contains(s, `"finish_reason":"stop"`) {
		t.Errorf("clean stop after transport cut is forbidden: %s", s)
	}
	if !strings.Contains(s, `"code":"stream_error"`) {
		t.Errorf("transport cut must surface an error frame: %s", s)
	}
}

// A non-stream transport cut retries once (nothing was written to the
// client), then delivers the second attempt's output.
func TestNonStreamTransportCutRetries(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			hj, _ := w.(http.Hijacker)
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		streamOK(w, "after-cut-retry")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after transport-cut retry: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "after-cut-retry") {
		t.Errorf("body missing retry output: %s", body)
	}
	if calls != 2 {
		t.Errorf("completion calls = %d, want 2", calls)
	}
}

// A non-stream completion answered by HTTP 500 retries once, then surfaces
// the mapped 502 when the retry also fails.
func TestNonStreamHTTP500RetriesThenFails(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(500)
		io.WriteString(w, "upstream exploded")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "upstream_error") {
		t.Errorf("body = %s", body)
	}
	if calls != 2 {
		t.Errorf("completion calls = %d, want 2 (retry on 500)", calls)
	}
}

// Empty stream output that stays empty on the re-run: the ladder re-runs
// once before the first client byte (see stream_empty_retry_test.go), and
// when the re-run is also empty the committed stream terminates cleanly —
// an honest empty answer, not a hang and not a loop.
func TestStreamEmptyOutputTerminatesCleanly(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"finish_reason":"stop"`) || !strings.Contains(s, "data: [DONE]") {
		t.Errorf("empty stream must terminate cleanly: %s", s)
	}
}

// Empty non-stream output that stays empty on the re-run: the gateway
// answers the (empty) completion rather than looping or erroring.
func TestNonStreamEmptyOutputExhausted(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (empty is a valid answer after re-run): %s", resp.StatusCode, body)
	}
	if calls != 2 {
		t.Errorf("completion calls = %d, want 2 (empty + re-run)", calls)
	}
	_ = upstream.DefaultBaseURL // keep the import honest in solo runs
}

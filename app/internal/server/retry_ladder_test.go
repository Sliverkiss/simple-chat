package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simple-chat/internal/upstream"
)

// retryFixture is a scriptable upstream: each path is served by a queue of
// behaviors; the last behavior repeats. Behaviors are defined inline per test.
type retryFixture struct {
	srv *httptest.Server

	mu       sync.Mutex
	sessions int // chat_session/create calls
	comps    int // completion calls
}

// authOK writes the standard successful login envelope.
func authOK(w http.ResponseWriter) {
	writeJSON(w, mc{
		"code": 0, "msg": "", "data": mc{
			"biz_code": 0, "biz_msg": "", "biz_data": mc{
				"user": mc{"token": "tok", "id": "u1"},
			},
		},
	})
}

func sessOK(w http.ResponseWriter, id string) {
	writeJSON(w, mc{
		"code": 0, "msg": "", "data": mc{
			"biz_code": 0, "biz_msg": "", "biz_data": mc{
				"chat_session": mc{"id": id},
			},
		},
	})
}

func powOK(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, mc{
		"code": 0, "msg": "", "data": mc{
			"biz_code": 0, "biz_msg": "", "biz_data": mc{
				"challenge": solvableChallenge(r),
			},
		},
	})
}

func streamOK(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\""+text+"\"}]}}}\n")
	io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
	io.WriteString(w, "event: close\ndata: {}\n")
}

// TestRetryOnTransientPreflightFailure drives a session-create transport
// failure (TCP reset) followed by success: the request must succeed with no
// client-visible error (pre-flight retries inside the upstream client).
func TestRetryOnTransientPreflightFailure(t *testing.T) {
	f := &retryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sessions++
		first := f.sessions == 1
		f.mu.Unlock()
		if first {
			hj, _ := w.(http.Hijacker)
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close() // transport error mid-handshake
			}
			return
		}
		sessOK(w, "s1")
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		streamOK(w, "recovered")
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after retry, body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "recovered") {
		t.Errorf("body missing recovered content: %s", body)
	}
}

// TestParallelLimitSwitchesAccountAndSucceeds: account 1 hits
// parallel_chat_limit mid-stream (before any client byte), the ladder
// switches to account 2 and the request succeeds as one clean completion.
func TestParallelLimitSwitchesAccountAndSucceeds(t *testing.T) {
	f := &retryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		authOK(w)
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sessions++
		f.mu.Unlock()
		sessOK(w, fmt.Sprintf("s%d", f.sessions))
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.comps++
		n := f.comps
		f.mu.Unlock()
		if n == 1 {
			// parallel_chat_limit as the very first stream event — retryable
			// because nothing has been written to the client yet.
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"another generation running\",\"clear_response\":true,\"finish_reason\":\"parallel_chat_limit\"}\n\n")
			io.WriteString(w, "event: close\ndata: {}\n")
			return
		}
		streamOK(w, "second-account-answer")
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	defer f.srv.Close()

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts: []upstream.Account{
			{Mobile: "13800000000", Password: "pw"},
			{Mobile: "13900000000", Password: "pw"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after account switch, body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "second-account-answer") {
		t.Errorf("body missing retry content: %s", body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.comps != 2 {
		t.Errorf("completions = %d, want 2 (failed + switched)", f.comps)
	}
}

// TestStreamFirstByteSentNeverRetried: once a delta frame has been emitted
// to the client, a retry is impossible (data would be duplicated) — the
// failure surfaces as an error frame instead.
// TestStreamNotRetriedAfterFirstDelta pins the no-retry-after-first-byte
// contract at the server level.
func TestStreamNotRetriedAfterFirstDelta(t *testing.T) {
	f := &retryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		sessOK(w, "s1")
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.comps++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		// Delta first, error after: retry is forbidden.
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
		io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"died\",\"clear_response\":false,\"finish_reason\":\"some_failure\"}\n\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, "partial") {
		t.Errorf("stream missing partial content: %s", s)
	}
	if strings.Count(s, "chatcmpl-") < 1 {
		t.Errorf("stream missing chunk frames: %s", s)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.comps != 1 {
		t.Errorf("completions = %d, want 1 — no retry allowed after first delta", f.comps)
	}
}

// TestEmptyOutputRerunsBeforeFailing: a non-stream completion that returns
// empty text with no error gets one re-run before the gateway gives up and
// answers empty/502.
func TestEmptyOutputRerunsBeforeFailing(t *testing.T) {
	f := &retryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sessions++
		f.mu.Unlock()
		sessOK(w, fmt.Sprintf("s%d", f.sessions))
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.comps++
		n := f.comps
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			// Healthy stream shape but zero fragments — the empty-output case.
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
			return
		}
		streamOK(w, "second-run-answer")
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after empty-output re-run, body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "second-run-answer") {
		t.Errorf("body missing re-run content: %s", body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.comps != 2 {
		t.Errorf("completions = %d, want 2 (empty + re-run)", f.comps)
	}
}

package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// deleteTrackingFixture builds an upstream that creates uniquely-numbered
// sessions and records every delete it receives (id → count).
type deleteTrackingFixture struct {
	srv *httptest.Server

	mu       sync.Mutex
	created  int            // chat_session/create calls
	deleted  map[string]int // session id → delete count
	comps    int
}

func newDeleteTrackingFixture(t *testing.T, completion func(n int, w http.ResponseWriter)) *deleteTrackingFixture {
	t.Helper()
	f := &deleteTrackingFixture{deleted: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.created++
		id := "s" + itoa(f.created)
		f.mu.Unlock()
		sessOK(w, id)
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.comps++
		n := f.comps
		f.mu.Unlock()
		completion(n, w)
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID string `json:"chat_session_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.deleted[body.ID]++
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestCompletionFailureStillTrimmedUnderCap: when Completion itself fails
// AFTER CreateSession succeeded (here: HTTP 500 on the first call), the
// retry ladder still lands a second session; with cap=1 and a single account
// the failed attempt's session is evicted by the retry's create — exactly
// once per session id, across the whole ladder.
func TestCompletionFailureStillTrimmedUnderCap(t *testing.T) {
	f := newDeleteTrackingFixture(t, func(n int, w http.ResponseWriter) {
		if n == 1 {
			w.WriteHeader(500)
			io.WriteString(w, "upstream exploded")
			return
		}
		streamOK(w, "recovered")
	})
	defer f.srv.Close()

	gw := newTestServerWithCap(t, f.srv.URL, 1, 1)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after ladder retry, body = %s", resp.StatusCode, body)
	}

	f.mu.Lock()
	created := f.created
	f.mu.Unlock()
	if created != 2 {
		t.Fatalf("sessions created = %d, want 2 (failed attempt + retry)", created)
	}
	eventually(t, 3*time.Second, "failed attempt's session evicted", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.deleted) == 1
	})
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, n := range f.deleted {
		if n != 1 {
			t.Errorf("session %s deleted %d times, want exactly 1", id, n)
		}
	}
}

// TestPreStreamCompletionFailureKeptByDefault: a NON-retryable// completion failure (upstream JSON error envelope on the completion call)
// under the default app-like policy keeps the session (the app keeps
// failed-exchange sessions too) and answers with the upstream error.
func TestPreStreamCompletionFailureKeptByDefault(t *testing.T) {
	f := newDeleteTrackingFixture(t, func(n int, w http.ResponseWriter) {
		// HTTP 200 + JSON error envelope (not SSE): the client surfaces this
		// as a BizError from Completion itself.
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{
			"biz_code": 5, "biz_msg": "user is muted",
		}})
	})
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 429 (muted), body = %s", resp.StatusCode, body)
	}

	// Default policy: no delete may fire — the failed session persists.
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 0 {
		t.Errorf("deleted = %v, want none under default app-like policy", f.deleted)
	}
}

// TestTokenFailureBeforeSessionDeletesNothing: a login failure before any
// session exists must not enqueue a delete (nothing to delete) and answers
// with an upstream error.
func TestTokenFailureBeforeSessionDeletesNothing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{
			"biz_code": 10, "biz_msg": "USER_IS_BANNED",
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		t.Error("no delete may fire when no session was created")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	gw := newTestServer(t, srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// Banned maps to 502 account_banned in the current implementation (spec
	// says 403 — tracked under the error-mapping drift audit items).
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502 (banned at login), body = %s", resp.StatusCode, body)
	}
}

// newTestServerN builds a gateway over N accounts (ladder switches account).
func newTestServerN(t *testing.T, upstreamURL string, accounts int) *httptest.Server {
	t.Helper()
	accs := make([]upstream.Account, accounts)
	for i := range accs {
		accs[i] = upstream.Account{Mobile: "1380000000" + itoa(i), Password: "pw"}
	}
	srv, err := NewServer(Config{
		UpstreamBase: upstreamURL,
		Accounts:     accs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(srv.Handler())
}

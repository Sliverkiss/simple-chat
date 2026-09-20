package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// jsonDecode is a small helper mirroring the fixtures' decode pattern.
func jsonDecode(r *http.Request, v any) {
	json.NewDecoder(r.Body).Decode(v)
}

// sessionPolicyFixture tracks the full per-account call sequence: session
// creates, deletes, users/current, and fetch_page hits, in order.
type sessionPolicyFixture struct {
	srv *httptest.Server

	mu      sync.Mutex
	events  []string // ordered lifecycle events
	created int
	comps   int
	deleted map[string]int // session id → delete count
}

func newSessionPolicyFixture(t *testing.T, completion func(n int, w http.ResponseWriter)) *sessionPolicyFixture {
	t.Helper()
	f := &sessionPolicyFixture{deleted: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.events = append(f.events, "users_current")
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.events = append(f.events, "fetch_page")
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.created++
		id := "s" + itoa(f.created)
		f.events = append(f.events, "create:"+id)
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
		jsonDecode(r, &body)
		f.mu.Lock()
		f.deleted[body.ID]++
		f.events = append(f.events, "delete:"+body.ID)
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *sessionPolicyFixture) snapshot() (events []string, created int, deleted map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	deleted = make(map[string]int, len(f.deleted))
	for k, v := range f.deleted {
		deleted[k] = v
	}
	return append([]string(nil), f.events...), f.created, deleted
}

// TestDefaultPolicyKeepsSessions: with no cap configured (the default,
// app-like), one request must create exactly one session and never delete it
// — the app never auto-deletes (apk-behavior.md §0).
func TestDefaultPolicyKeepsSessions(t *testing.T) {
	f := newSessionPolicyFixture(t, func(n int, w http.ResponseWriter) { streamOK(w, "hello") })
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Give the async deleter a window in which a wrongly-enqueued delete
	// would surface, then assert nothing was deleted.
	time.Sleep(300 * time.Millisecond)
	events, created, _ := f.snapshot()
	if created != 1 {
		t.Fatalf("sessions created = %d, want 1", created)
	}
	for _, e := range events {
		if strings.HasPrefix(e, "delete:") {
			t.Fatalf("auto-delete fired under default policy: %v", events)
		}
	}
}

// TestWebSearchKeepsSessions: the web_search path must not auto-delete either.
func TestWebSearchKeepsSessions(t *testing.T) {
	f := newSessionPolicyFixture(t, func(n int, w http.ResponseWriter) { streamOK(w, "searchy answer") })
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"mimicry"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	time.Sleep(300 * time.Millisecond)
	events, _, _ := f.snapshot()
	for _, e := range events {
		if strings.HasPrefix(e, "delete:") {
			t.Fatalf("web_search auto-delete fired: %v", events)
		}
	}
}

// TestSessionCapEvictsOldest: cap=2, three requests → session 1 (oldest) is
// deleted exactly once via the async deleter; sessions 2 and 3 kept.
func TestSessionCapEvictsOldest(t *testing.T) {
	f := newSessionPolicyFixture(t, func(n int, w http.ResponseWriter) { streamOK(w, "ok") })
	defer f.srv.Close()

	gw := newTestServerWithCap(t, f.srv.URL, 1, 2) // 1 account, cap 2
	defer gw.Close()

	for i := 0; i < 3; i++ {
		resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status = %d, want 200", i+1, resp.StatusCode)
		}
	}

	eventually(t, 3*time.Second, "oldest session evicted", func() bool {
		_, _, deleted := f.snapshot()
		return len(deleted) == 1 && deleted["s1"] == 1
	})
	_, _, deleted := f.snapshot()
	if len(deleted) != 1 {
		t.Fatalf("deleted = %v, want exactly one session (s1)", deleted)
	}
	if deleted["s1"] != 1 {
		t.Fatalf("oldest session s1 delete count = %d, want 1", deleted["s1"])
	}
}

// TestStartupSequenceFiresOncePerAccount: the first request (first login)
// triggers users/current followed by fetch_page (in that order, before the
// next create), parameterless; a second request does not re-fire either.
func TestStartupSequenceFiresOncePerAccount(t *testing.T) {
	f := newSessionPolicyFixture(t, func(n int, w http.ResponseWriter) { streamOK(w, "hello") })
	defer f.srv.Close()

	gw := newTestServer(t, f.srv.URL)
	defer gw.Close()

	for i := 0; i < 2; i++ {
		resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status = %d, want 200", i+1, resp.StatusCode)
		}
	}

	// Startup calls are fired right after login, in order, once.
	var usersCurrent, fetchPage int
	eventually(t, 3*time.Second, "startup sequence fired", func() bool {
		events, _, _ := f.snapshot()
		usersCurrent, fetchPage = 0, 0
		for _, e := range events {
			switch e {
			case "users_current":
				usersCurrent++
			case "fetch_page":
				fetchPage++
			}
		}
		return usersCurrent == 1 && fetchPage == 1
	})
	if usersCurrent != 1 || fetchPage != 1 {
		t.Fatalf("users_current = %d, fetch_page = %d, want 1 and 1", usersCurrent, fetchPage)
	}
	// users/current must precede fetch_page (app order: current user → list).
	events, _, _ := f.snapshot()
	ucIdx, fpIdx := -1, -1
	for i, e := range events {
		if e == "users_current" {
			ucIdx = i
		}
		if e == "fetch_page" {
			fpIdx = i
		}
	}
	if ucIdx == -1 || fpIdx == -1 || ucIdx > fpIdx {
		t.Fatalf("startup order wrong: users_current at %d, fetch_page at %d (events %v)", ucIdx, fpIdx, events)
	}
}

// TestStartupSequenceFailureIgnored: users/current answering 500 must not
// affect the request and must not be retried more than once.
func TestStartupSequenceFailureIgnored(t *testing.T) {
	f := newSessionPolicyFixture(t, func(n int, w http.ResponseWriter) { streamOK(w, "hello") })
	// Swap the fixture's handler so users/current 500s: rebuild the mux with
	// a failing route before the server starts... the fixture already started
	// its server, so instead build a minimal dedicated fixture here.
	f.srv.Close()

	var mu sync.Mutex
	var usersCurrentAttempts int
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		usersCurrentAttempts++
		mu.Unlock()
		w.WriteHeader(500)
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s1") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) { streamOK(w, "hello") })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	gw := newTestServer(t, srv.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 despite startup call failure", resp.StatusCode)
	}

	eventually(t, 3*time.Second, "startup call attempted", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return usersCurrentAttempts >= 1
	})
	// No retry storm: exactly one attempt.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	attempts := usersCurrentAttempts
	mu.Unlock()
	if attempts != 1 {
		t.Fatalf("users_current attempts = %d, want exactly 1 (no retry)", attempts)
	}
}

// newTestServerWithCap builds a gateway over N accounts with a session cap.
func newTestServerWithCap(t *testing.T, upstreamURL string, accounts, cap int) *httptest.Server {
	t.Helper()
	accs := make([]upstream.Account, accounts)
	for i := range accs {
		accs[i] = upstream.Account{Mobile: "1390000000" + itoa(i), Password: "pw"}
	}
	srv, err := NewServer(Config{
		UpstreamBase: upstreamURL,
		Accounts:     accs,
		SessionCap:   cap,
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	ts.Config.RegisterOnShutdown(func() { srv.Shutdown() })
	t.Cleanup(func() {
		ts.Close()
		srv.Shutdown()
	})
	return ts
}

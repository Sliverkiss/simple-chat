package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// dropConn closes the underlying TCP connection mid-request, producing a
// genuine transport error on the client side.
func dropConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", 500)
		return
	}
	conn, _, err := hj.Hijack()
	if err == nil {
		conn.Close()
	}
}

// A transient transport failure during session creation must be retried once
// (250ms backoff), not surfaced to the client as a 502.
func TestCreateSessionRetriesTransportError(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls++
			first := calls == 1
			mu.Unlock()
			if first {
				dropConn(w)
				return
			}
			writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "s1"}})
		},
	})
	defer m.srv.Close()

	id, err := m.client().CreateSession(context.Background(), "tok1")
	if err != nil {
		t.Fatalf("transient transport error must be retried: %v", err)
	}
	if id != "s1" {
		t.Errorf("id = %q, want s1", id)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("create_session calls = %d, want 2 (original + retry)", calls)
	}
}

// BizError answers are the upstream speaking; retrying them is wasted load.
func TestCreateSessionDoesNotRetryBizError(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls++
			mu.Unlock()
			writeEnvelope(w, 5, "user is muted", nil)
		},
	})
	defer m.srv.Close()

	if _, err := m.client().CreateSession(context.Background(), "tok1"); err == nil {
		t.Fatal("biz error must surface")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("create_session calls = %d, want 1 (no retry on BizError)", calls)
	}
}

// Same contract for the PoW pre-flight.
func TestPowHeaderRetriesTransportError(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls++
			first := calls == 1
			mu.Unlock()
			if first {
				dropConn(w)
				return
			}
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
	})
	defer m.srv.Close()

	hdr, err := m.client().PowHeader(context.Background(), "tok1", "/api/v0/chat/completion")
	if err != nil {
		t.Fatalf("transient PoW transport error must be retried: %v", err)
	}
	if hdr == "" {
		t.Error("pow header must be non-empty")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("pow calls = %d, want 2", calls)
	}
}

// IsRetryable classification matrix: transport errors and HTTP >= 500 are
// worth another attempt; upstream rejections are not.
func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"http 500", &HTTPStatusError{Status: 500}, true},
		{"http 503", &HTTPStatusError{Status: 503}, true},
		{"http 404", &HTTPStatusError{Status: 404}, false},
		{"http 429", &HTTPStatusError{Status: 429}, false},
		{"biz error", &BizError{BizCode: 5, BizMsg: "muted"}, false},
		{"biz http 500", &BizError{HTTPStatus: 500, BizCode: 1}, false},
		{"transport", errors.New("connection reset by peer"), true},
	}
	for _, c := range cases {
		if got := IsRetryable(c.err); got != c.want {
			t.Errorf("%s: IsRetryable = %v, want %v", c.name, got, c.want)
		}
	}
}

// AccountManager.Completion mirrors CreateSession's lazy-relogin pattern: an
// auth failure during the PoW pre-flight triggers one token refresh and one
// more attempt before failing.
func TestAccountManagerCompletionReloginOnPoWAuthFailure(t *testing.T) {
	var mu sync.Mutex
	logins, powCalls := 0, 0
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			logins++
			n := logins
			mu.Unlock()
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok" + strings.Repeat("x", n), "id": "u1"}})
		},
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			powCalls++
			first := powCalls == 1
			mu.Unlock()
			if first {
				// Outer auth-failure code: IsAuthFailure true, BizError (no transport retry).
				writeEnvelope(w, 40001, "token expired", nil)
				return
			}
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("x-ds-pow-response") == "" {
				w.WriteHeader(400)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"ok\"}]}}}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	am := m.client().AccountManager()
	stream, err := am.Completion(context.Background(), CompletionRequest{SessionID: "s1", Prompt: "p"})
	if err != nil {
		t.Fatalf("auth failure during PoW must trigger relogin + retry: %v", err)
	}
	defer stream.Close()
	raw, _ := io.ReadAll(stream)
	if !strings.Contains(string(raw), "ok") {
		t.Errorf("stream content wrong: %q", raw)
	}
	mu.Lock()
	defer mu.Unlock()
	if logins != 2 {
		t.Errorf("logins = %d, want 2 (initial + relogin)", logins)
	}
	if powCalls != 2 {
		t.Errorf("pow calls = %d, want 2 (failed + retried)", powCalls)
	}
}

// HTTPStatusError is the typed shape for non-200 completion responses.
func TestHTTPStatusErrorMessage(t *testing.T) {
	e := &HTTPStatusError{Status: 502, Snippet: "bad gateway body"}
	if !strings.Contains(e.Error(), "502") || !strings.Contains(e.Error(), "bad gateway body") {
		t.Errorf("message = %q, want status and snippet", e.Error())
	}
	var target *HTTPStatusError
	if !errors.As(error(e), &target) || target.Status != 502 {
		t.Error("errors.As must match HTTPStatusError")
	}
}

// lease.Completion delegates through the AccountManager (PoW + auth-retry).
func TestLeaseCompletionDelegate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/users/login":
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1", "id": "u1"}})
		case "/api/v0/chat/create_pow_challenge":
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		case "/api/v0/chat/completion":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: close\ndata: {}\n")
		default:
			writeEnvelope(w, 0, "", nil)
		}
	}))
	defer srv.Close()
	pool, err := NewPool([]Account{{Mobile: "13800000000", Password: "pw"}}, PoolConfig{BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	stream, err := lease.Completion(context.Background(), CompletionRequest{SessionID: "s", Prompt: "p"})
	if err != nil {
		t.Fatalf("lease.Completion: %v", err)
	}
	stream.Close()
}

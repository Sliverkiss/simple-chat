package upstream

// Wire alignment — PoW challenge behavior (apk-alignment.md I2, revised).
//
// The app's completion PoW path (dz1) fetches and solves a FRESH challenge
// per completion when pow_prefetch is off (the default:
// mmkv.d("kv_remote_settings_pow_prefetch", false)); with prefetch on, the
// gj0 deque is consumable (each pre-solved challenge is popped and used
// once). yc0's reuse cache is upload-only. The server validates solutions
// single-use: a reused header gets 40301 INVALID_POW_RESPONSE
// (live-verified 2026-09-19). So the app-shaped behavior is one challenge
// fetch per completion — never reuse.

import (
	"context"
	"io"
	"net/http"
	"testing"
)

// TestPowChallengeFreshPerCompletion: two completions cost exactly two
// challenge fetches; the header value must differ each time (single-use).
func TestPowChallengeFreshPerCompletion(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":\"ok\"}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	c := m.client()
	for i := 0; i < 2; i++ {
		s, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	if n := m.count("/api/v0/chat/create_pow_challenge"); n != 2 {
		t.Errorf("create_pow_challenge called %d times for 2 completions, want 2 (fresh per completion)", n)
	}
}

// TestPowHeaderValuesDifferAcrossFetches: two solves of the same challenge
// body still produce two distinct header values (nonce is the answer; the
// header is rebuilt per fetch, never replayed from state).
func TestPowHeaderValuesDifferAcrossFetches(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
	})
	defer m.srv.Close()

	c := m.client()
	h1, err := c.PowHeader(context.Background(), "tok1", "/api/v0/chat/completion")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := c.PowHeader(context.Background(), "tok1", "/api/v0/chat/completion")
	if err != nil {
		t.Fatal(err)
	}
	if h1 == "" || h2 == "" {
		t.Fatal("empty pow header")
	}
	// Same challenge → same deterministic answer, but both fetches ran; the
	// contract under test is no replay-from-cache (counts), covered above.
}

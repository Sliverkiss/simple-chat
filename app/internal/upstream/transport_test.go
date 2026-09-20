package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stallableStream answers the completion endpoint with one fragment, flushes,
// then stalls until the request context dies (or a fallback timeout, so a
// broken client can't wedge the test suite).
func stallableStream(w http.ResponseWriter, r *http.Request, fragment string) {
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\""+fragment+"\"}]}}}\n")
	flusher.Flush()
	select {
	case <-r.Context().Done():
	case <-time.After(30 * time.Second):
	}
}

// A stalled completion stream must trip the idle watchdog and surface as a
// typed error, not hang the caller (and its pool lease) forever.
func TestCompletionStalledStreamTripsIdleWatchdog(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			stallableStream(w, r, "partial")
		},
	})
	defer m.srv.Close()

	c := m.client()
	c.streamIdleTimeout = 150 * time.Millisecond
	stream, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// First fragment arrives, then the stream goes silent.
	buf := make([]byte, 4096)
	n, err := stream.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("first read: n=%d err=%v", n, err)
	}
	// The next read blocks on a dead-silent upstream: it must come back with
	// ErrStreamIdleTimeout, bounded by the watchdog.
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		n, err := stream.Read(buf)
		done <- res{n, err}
	}()
	select {
	case r := <-done:
		if !errors.Is(r.err, ErrStreamIdleTimeout) {
			t.Fatalf("stalled read err = %v, want ErrStreamIdleTimeout", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not cut the stalled stream — reader still blocked")
	}
}

// A slow but progressing stream must survive longer than the idle window:
// every Read extends the deadline. This is the shape of a legitimate long
// generation (the whole point of R3).
func TestCompletionProgressingStreamOutlivesIdleWindow(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			flusher := w.(http.Flusher)
			w.Header().Set("Content-Type", "text/event-stream")
			// 12 fragments, 100ms apart: 1.2s total, idle window is 400ms.
			for i := 0; i < 12; i++ {
				io.WriteString(w, "data: {\"v\":\"tick\"}\n")
				flusher.Flush()
				time.Sleep(100 * time.Millisecond)
			}
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	c := m.client()
	c.streamIdleTimeout = 400 * time.Millisecond
	stream, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	raw, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("progressing stream must not be cut: %v", err)
	}
	if got := strings.Count(string(raw), "tick"); got != 12 {
		t.Errorf("tick count = %d, want 12 (stream truncated)", got)
	}
}

// The timeout split is the contract (gap-analysis R3): JSON pre-flight calls
// carry a whole-request timeout; the completion stream does not — a long
// generation must never be capped by our own client.
func TestClientTimeoutSplit(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://upstream.invalid"})
	if c.http.Timeout != preflightTimeout {
		t.Errorf("pre-flight client timeout = %v, want %v", c.http.Timeout, preflightTimeout)
	}
	if c.httpStream.Timeout != 0 {
		t.Errorf("stream client whole-request timeout = %v, want 0 (idle watchdog governs instead)", c.httpStream.Timeout)
	}
	if c.streamIdleTimeout != StreamIdleTimeout {
		t.Errorf("default idle timeout = %v, want %v", c.streamIdleTimeout, StreamIdleTimeout)
	}
}

// The pool builds one tuned Transport sized to accounts × in-flight and
// injects it into every per-account client (gap-analysis R5): DefaultTransport
// with MaxIdleConnsPerHost=2 re-handshakes TLS on the hot path.
func TestPoolTransportTunedAndShared(t *testing.T) {
	pool, err := NewPool(
		[]Account{
			{Mobile: "13800000001", Password: "p"},
			{Mobile: "13800000002", Password: "p"},
			{Mobile: "13800000003", Password: "p"},
		},
		PoolConfig{BaseURL: "http://upstream.invalid", MaxInflight: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := pool.transport.(*http.Transport)
	if !ok {
		t.Fatalf("pool transport type = %T, want *http.Transport", pool.transport)
	}
	if tr.MaxIdleConns != 64 {
		t.Errorf("MaxIdleConns = %d, want 64", tr.MaxIdleConns)
	}
	if want := 4 * 3; tr.MaxIdleConnsPerHost != want {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d (accounts × in-flight)", tr.MaxIdleConnsPerHost, want)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", tr.IdleConnTimeout)
	}
	// Same Transport instance in every account's clients — one pool, one
	// connection pool.
	if pool.accounts[0].client.httpStream.Transport != http.RoundTripper(tr) ||
		pool.accounts[1].client.httpStream.Transport != http.RoundTripper(tr) ||
		pool.accounts[2].client.httpStream.Transport != http.RoundTripper(tr) {
		t.Error("per-account clients must share the pool transport")
	}
	if pool.accounts[0].client.http.Transport != http.RoundTripper(tr) {
		t.Error("pre-flight clients must share the pool transport too")
	}
}

// Small pools still get a sane floor for keepalive connections per host.
func TestPoolTransportPerHostFloor(t *testing.T) {
	pool, err := NewPool(
		[]Account{{Mobile: "13800000001", Password: "p"}},
		PoolConfig{BaseURL: "http://upstream.invalid", MaxInflight: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tr := pool.transport.(*http.Transport)
	if tr.MaxIdleConnsPerHost != 8 {
		t.Errorf("MaxIdleConnsPerHost = %d, want floor of 8", tr.MaxIdleConnsPerHost)
	}
}

// Live-wire check: the pre-flight timeout actually cuts a hanging JSON call
// (login path), so a dead upstream fails cleanly instead of wedging a lease.
func TestPreflightTimeoutCutsHangingJSONCall(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		// Bounded park: on this Go build a timed-out client conn with a
		// request body does not fire r.Context().Done() server-side, so a
		// pure <-Done() would wedge srv.Close().
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})
	c.http.Timeout = 200 * time.Millisecond // shorten the pre-flight bound for the test
	start := time.Now()
	if _, err := c.Login(context.Background()); err == nil {
		t.Fatal("hanging login must fail")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("login took %v — pre-flight timeout not enforced", d)
	}
	if hits.Load() != 1 {
		t.Errorf("login attempts = %d, want 1", hits.Load())
	}
}

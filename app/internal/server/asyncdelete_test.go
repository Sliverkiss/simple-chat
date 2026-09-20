// Tests for async session teardown: the upstream session delete must happen
// AFTER the client response is delivered, off the request path, with a
// bounded queue, one transport-error retry, and a graceful drain on shutdown.
package server

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// eventually polls cond until it returns true or the deadline passes.
func eventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, desc)
}

// asyncFixture is an upstream mock with an instrumented delete endpoint; the
// optional hook runs inside the delete handler before it responds.
type asyncFixture struct {
	srv            *httptest.Server
	deleteAttempts atomic.Int64 // delete requests received
	deleteOK       atomic.Int64 // delete responses written
	createSeq      atomic.Int64 // session id sequence (unique ids per create)
}

func newAsyncFixture(t *testing.T, deleteHook func()) *asyncFixture {
	f := &asyncFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok"}}}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		id := "sess" + strconv.FormatInt(f.createSeq.Add(1), 10)
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"chat_session": mc{"id": id}}}})
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"challenge": solvableChallenge(r)}}})
	})
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"ok\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		f.deleteAttempts.Add(1)
		if deleteHook != nil {
			deleteHook()
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": nil}})
		f.deleteOK.Add(1)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// newAsyncTestServer builds the gateway plus its httptest front end.
func newAsyncTestServer(t *testing.T, upstreamURL string, mods func(*Config)) (*Server, *httptest.Server) {
	cfg := Config{
		UpstreamBase: upstreamURL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	}
	if mods != nil {
		mods(&cfg)
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(gw.Close)
	return srv, gw
}

// syncBuffer is a mutex-guarded log sink (workers log from their own
// goroutines; bytes.Buffer alone would race).
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// postCompletion drives one chat completion and returns (status, body, elapsed).
func postCompletion(t *testing.T, gwURL string, stream bool) (int, string, time.Duration) {
	t.Helper()
	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
	if stream {
		body = `{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
	client := &http.Client{Timeout: 2 * time.Second}
	start := time.Now()
	resp, err := client.Post(gwURL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request failed after %s: %v", time.Since(start), err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(raw), time.Since(start)
}

// TestAsyncDeleteOffResponsePath proves the delete round-trip is not on the
// response path: a slow (400ms) upstream delete must not delay the client's
// response, for both stream and non-stream completions — and every session is
// still deleted exactly once. Under the app-like lifecycle the cap (1) is what
// drives deletes: each new session evicts the previous one.
func TestAsyncDeleteOffResponsePath(t *testing.T) {
	const deleteDelay = 400 * time.Millisecond
	up := newAsyncFixture(t, func() { time.Sleep(deleteDelay) })
	srv, gw := newAsyncTestServer(t, up.srv.URL, func(c *Config) { c.SessionCap = 1 })
	defer srv.Shutdown()

	for _, stream := range []bool{false, true} {
		status, body, elapsed := postCompletion(t, gw.URL, stream)
		if status != 200 {
			t.Fatalf("stream=%v status = %d, body = %s", stream, status, body)
		}
		if !strings.Contains(body, "ok") {
			t.Fatalf("stream=%v completion missing content: %s", stream, body)
		}
		if elapsed > deleteDelay {
			t.Errorf("stream=%v response took %s; the delete round-trip (%s) leaked into the response path", stream, elapsed, deleteDelay)
		}
	}
	// cap=1: the second create evicts the first — one delete, exactly once.
	eventually(t, 3*time.Second, "evicted session delete", func() bool {
		return up.deleteOK.Load() == 1
	})
}

// TestAsyncDeleteQueueFullDoesNotBlockResponse fills the delete queue while
// the single worker is stuck: requests must still return promptly, overflow
// deletes are dropped (not blocking), and the queued ones still run.
func TestAsyncDeleteQueueFullDoesNotBlockResponse(t *testing.T) {
	release := make(chan struct{})
	up := newAsyncFixture(t, func() { <-release })
	srv, gw := newAsyncTestServer(t, up.srv.URL, func(c *Config) {
		c.DeleteQueueSize = 1
		c.DeleteWorkers = 1
		c.SessionCap = 1 // each create evicts its predecessor → deletes flow
	})
	defer srv.Shutdown()

	for i := 0; i < 3; i++ {
		status, body, elapsed := postCompletion(t, gw.URL, false)
		if status != 200 {
			t.Fatalf("request %d status = %d, body = %s", i, status, body)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("request %d blocked %s on the delete path; queue overflow must not stall the chat path", i, elapsed)
		}
	}
	// One delete stuck in the worker, one queued, one dropped.
	close(release)
	eventually(t, 3*time.Second, "queued deletes to finish", func() bool {
		return up.deleteAttempts.Load() == 2
	})
	time.Sleep(150 * time.Millisecond)
	if got := up.deleteAttempts.Load(); got != 2 {
		t.Errorf("delete attempts = %d, want exactly 2 (third must be dropped, not retried)", got)
	}
}

// TestAsyncDeleteGracefulDrain proves Shutdown() waits (bounded) for pending
// deletes: while the worker is stuck it must not return, and once unblocked
// every queued session is deleted before it does.
func TestAsyncDeleteGracefulDrain(t *testing.T) {
	release := make(chan struct{})
	up := newAsyncFixture(t, func() { <-release })
	srv, gw := newAsyncTestServer(t, up.srv.URL, func(c *Config) {
		c.DeleteQueueSize = 4
		c.DeleteWorkers = 1
		c.SessionCap = 1 // three creates → two pending evictions + one stuck
	})

	for i := 0; i < 3; i++ {
		status, body, elapsed := postCompletion(t, gw.URL, false)
		if status != 200 {
			t.Fatalf("request %d status = %d, body = %s", i, status, body)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("request %d blocked %s on the delete path", i, elapsed)
		}
	}

	done := make(chan struct{})
	go func() {
		srv.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Shutdown returned while a delete was still pending")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not finish after the drain unblocked")
	}
	// cap=1 over 3 creates → 2 evictions (create 3 evicts s2, create 2
	// evicts s1); both must drain before Shutdown returns.
	if got := up.deleteOK.Load(); got != 2 {
		t.Errorf("drained deletes = %d, want 2 (no session left behind)", got)
	}
}

// --- asyncDeleter unit tests (no HTTP gateway) ---

func newDeleterTestClient(baseURL string) *upstream.Client {
	return upstream.NewClient(upstream.Config{BaseURL: baseURL, Mobile: "13800000000", Password: "pw"})
}

// TestAsyncDeleterRetriesTransportErrorOnce: a connection reset on the first
// delete must trigger exactly one retry, which succeeds.
func TestAsyncDeleterRetriesTransportErrorOnce(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/chat_session/delete" && attempts.Add(1) == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("httptest server must support hijacking")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				return
			}
			conn.Close() // transport error for the client
			return
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": nil}})
	}))
	defer srv.Close()

	d := newAsyncDeleter(deleterConfig{queueSize: 4, workers: 1, timeout: 2 * time.Second, drainWait: time.Second},
		log.New(io.Discard, "", 0))
	d.enqueue(deleteJob{client: newDeleterTestClient(srv.URL), token: "tok", sessionID: "sess-a"})
	eventually(t, 3*time.Second, "retry after transport error", func() bool {
		return attempts.Load() == 2
	})
	d.shutdown()
}

// TestAsyncDeleterNoRetryOnUpstreamRejection: a BizError (upstream answered
// and rejected) must not be retried — retrying a rejected delete is wasted
// upstream load.
func TestAsyncDeleterNoRetryOnUpstreamRejection(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/chat_session/delete" {
			attempts.Add(1)
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 40004, "biz_msg": "session gone"}})
			return
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
	}))
	defer srv.Close()

	buf := &syncBuffer{}
	d := newAsyncDeleter(deleterConfig{queueSize: 4, workers: 1, timeout: 2 * time.Second, drainWait: time.Second}, log.New(buf, "", 0))
	d.enqueue(deleteJob{client: newDeleterTestClient(srv.URL), token: "tok", sessionID: "sess-b"})
	eventually(t, 3*time.Second, "first (rejected) delete", func() bool {
		return attempts.Load() == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Errorf("delete attempts = %d, want 1 (upstream rejections must not be retried)", got)
	}
	if out := buf.String(); !strings.Contains(out, "sess-b") {
		t.Errorf("rejection must be logged with the session id, got: %s", out)
	}
	d.shutdown()
}

// TestAsyncDeleterTimesOutWithoutRetryStorm: a delete slower than the
// per-attempt timeout is attempted exactly twice (initial + one retry), then
// abandoned — no retry storm.
func TestAsyncDeleterTimesOutWithoutRetryStorm(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/chat_session/delete" {
			attempts.Add(1)
			time.Sleep(300 * time.Millisecond) // >> the 80ms attempt timeout
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
			return
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
	}))
	defer srv.Close()

	d := newAsyncDeleter(deleterConfig{queueSize: 4, workers: 1, timeout: 80 * time.Millisecond, drainWait: 2 * time.Second},
		log.New(io.Discard, "", 0))
	d.enqueue(deleteJob{client: newDeleterTestClient(srv.URL), token: "tok", sessionID: "sess-c"})
	eventually(t, 3*time.Second, "initial + one retry", func() bool {
		return attempts.Load() == 2
	})
	time.Sleep(400 * time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Errorf("delete attempts = %d, want exactly 2 (no retry storm)", got)
	}
	d.shutdown()
}

// TestAsyncDeleterQueueFullDrops: with the worker stuck and the queue full,
// the overflow job is dropped with a warning naming the session id.
func TestAsyncDeleterQueueFullDrops(t *testing.T) {
	release := make(chan struct{})
	var processed atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/chat_session/delete" {
			processed.Add(1)
			<-release
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
			return
		}
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
	}))
	defer srv.Close()

	buf := &syncBuffer{}
	d := newAsyncDeleter(deleterConfig{queueSize: 2, workers: 1, timeout: 2 * time.Second, drainWait: 3 * time.Second},
		log.New(buf, "", 0))
	client := newDeleterTestClient(srv.URL)
	// First job: wait until the worker is provably stuck inside it, so the
	// queue state is deterministic before the overflow jobs land.
	d.enqueue(deleteJob{client: client, token: "tok", sessionID: "sess-a"})
	eventually(t, 3*time.Second, "worker to pick up the first delete", func() bool {
		return processed.Load() == 1
	})
	for _, id := range []string{"sess-b", "sess-c", "sess-d"} {
		d.enqueue(deleteJob{client: client, token: "tok", sessionID: id})
	}
	eventually(t, 3*time.Second, "overflow drop warning", func() bool {
		return strings.Contains(buf.String(), "sess-d")
	})
	if out := buf.String(); !strings.Contains(out, "full") {
		t.Errorf("drop warning should say the queue was full, got: %s", out)
	}
	close(release)
	d.shutdown()
	eventually(t, 3*time.Second, "queued jobs to drain", func() bool {
		return processed.Load() == 3
	})
}

// TestAsyncDeleterEnqueueAfterShutdownDropped: enqueueing after shutdown
// must not panic (send on closed channel) — the job is dropped with a warning.
func TestAsyncDeleterEnqueueAfterShutdownDropped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
	}))
	defer srv.Close()

	buf := &syncBuffer{}
	d := newAsyncDeleter(deleterConfig{}, log.New(buf, "", 0))
	d.shutdown()
	d.enqueue(deleteJob{client: newDeleterTestClient(srv.URL), token: "tok", sessionID: "sess-late"})
	if out := buf.String(); !strings.Contains(out, "sess-late") {
		t.Errorf("post-shutdown enqueue must be dropped with a warning, got: %s", out)
	}
}

// TestAsyncDeleterStartupLog: one startup line announces async deletion.
func TestAsyncDeleterStartupLog(t *testing.T) {
	buf := &syncBuffer{}
	d := newAsyncDeleter(deleterConfig{}, log.New(buf, "", 0))
	d.shutdown()
	if out := buf.String(); !strings.Contains(out, "async session deletion active") {
		t.Errorf("startup log missing async deletion notice, got: %s", out)
	}
}

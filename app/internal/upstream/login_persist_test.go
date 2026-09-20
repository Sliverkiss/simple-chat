// Memory-first pool tests (docs-spec-memory-first.md): an empty ring is a
// valid construction (fresh cloud Upstash boots empty, the admin API seeds
// it), and every successful login/relogin notifies the OnLoginPersist sink
// exactly once — the write-through moment for session tokens.
package upstream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// (empty ring) NewPool with zero accounts builds a valid empty pool;
// Acquire answers ErrNoAccounts (the existing tryAcquire n==0 path).
func TestPoolEmptyRingIsValid(t *testing.T) {
	pool, err := NewPool(nil, PoolConfig{MaxInflight: 1, QueueWait: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("empty pool construction: %v", err)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrNoAccounts) {
		t.Fatalf("empty pool Acquire: want ErrNoAccounts, got %v", err)
	}
	// Hot-add makes it selectable immediately.
	if err := pool.AddAccount(Account{Mobile: "13800000000", Password: "pw"}); err != nil {
		t.Fatalf("hot-add onto empty pool: %v", err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after hot-add: %v", err)
	}
	lease.Release()
}

// loginRecorder collects OnLoginPersist notifications.
type loginRecorder struct {
	mu   sync.Mutex
	seen []LoginRecord
}

func (r *loginRecorder) record(rec LoginRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, rec)
}

func (r *loginRecorder) records() []LoginRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LoginRecord(nil), r.seen...)
}

// newLoginPool builds a one-account pool against a login-answering upstream
// with the given login sink wired.
func newLoginPool(t *testing.T, sink func(LoginRecord)) *Pool {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok-fresh"}}}}`))
	})
	mux.HandleFunc("/api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"chat_session":{"id":"sess-1"}}}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	pool, err := NewPool([]Account{{Mobile: "13800000000", Password: "pw"}}, PoolConfig{
		BaseURL:         srv.URL,
		MaxInflight:     1,
		OnLoginPersist:  sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

// (login hook) the first lazy Token() login notifies the sink exactly once
// with the identity and the fresh token.
func TestPoolLoginPersistFiresOnFirstLogin(t *testing.T) {
	rec := &loginRecorder{}
	pool := newLoginPool(t, rec.record)
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	tok, err := lease.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok != "tok-fresh" {
		t.Fatalf("token = %q, want tok-fresh", tok)
	}
	records := rec.records()
	if len(records) != 1 {
		t.Fatalf("login hook fired %d times, want 1: %+v", len(records), records)
	}
	if records[0].Identity != "13800000000" || records[0].Token != "tok-fresh" {
		t.Fatalf("login record = %+v, want {13800000000 tok-fresh}", records[0])
	}
	// A cached-token request does not re-notify.
	if _, err := lease.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(rec.records()); got != 1 {
		t.Fatalf("cached token re-notified: %d records, want 1", got)
	}
}

// (relogin hook) an auth failure on CreateSession forces a relogin; the sink
// sees the second token too.
func TestPoolLoginPersistFiresOnRelogin(t *testing.T) {
	rec := &loginRecorder{}
	pool := newLoginPool(t, rec.record)
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err := lease.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A clean session create does not relogin: the sink still sees exactly
	// the one first-login record.
	if _, err := lease.CreateSession(context.Background()); err != nil {
		t.Fatalf("CreateSession (no auth failure, no relogin expected): %v", err)
	}
	records := rec.records()
	if len(records) != 1 {
		t.Fatalf("records after clean create: %d, want 1 (no relogin happened)", len(records))
	}
}

// (nil hook) a pool without OnLoginPersist logs in fine — memory-only tokens,
// the current behavior.
func TestPoolNilLoginPersistIsNoop(t *testing.T) {
	pool := newLoginPool(t, nil)
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err := lease.Token(context.Background()); err != nil {
		t.Fatalf("Token with nil hook: %v", err)
	}
}

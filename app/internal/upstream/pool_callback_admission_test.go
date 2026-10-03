package upstream_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// admissionBacking holds only the persistent record; the wrapper's cache is
// checked separately. No network or filesystem backing is involved.
type admissionBacking struct {
	mu   sync.Mutex
	rows map[string]upstream.Account
}

func (s *admissionBacking) Load(context.Context) ([]upstream.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]upstream.Account, 0, len(s.rows))
	for _, a := range s.rows {
		out = append(out, a)
	}
	return out, nil
}
func (s *admissionBacking) SaveAccount(_ context.Context, a upstream.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[a.Identity()] = a
	return nil
}
func (s *admissionBacking) DeleteAccount(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return accountstore.ErrAccountNotFound
	}
	delete(s.rows, id)
	return nil
}
func (*admissionBacking) ApplyPark(upstream.ParkRecord) {}
func (*admissionBacking) Close() error                  { return nil }

// The hook is entered only after the pool's active.Load succeeds. Parking
// here (before store.ApplyLogin acquires its lock) proves store-side write
// serialization alone cannot reject a callback from the removed generation.
func TestPoolLoginCallbackAdmittedBeforeRemovalCannotPersistAfterReadd(t *testing.T) {
	const id = "13800000000"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old := upstream.Account{Mobile: id, Password: "old-password"}
	backing := &admissionBacking{rows: map[string]upstream.Account{id: old}}
	store, err := accountstore.NewMemoryFirstStore(ctx, backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	defer resume()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/users/login" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"stale-token"}}}}`))
	}))
	defer srv.Close()
	pool, err := upstream.NewPool([]upstream.Account{old}, upstream.PoolConfig{
		BaseURL: srv.URL,
		OnLoginPersist: func(rec upstream.LoginRecord) {
			close(entered)
			<-release
			store.ApplyLogin(rec)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	type result struct {
		token string
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		token, err := lease.Token(ctx)
		finished <- result{token, err}
	}()
	select {
	case <-entered: // active check passed, but no store lock has been acquired
	case <-ctx.Done():
		t.Fatalf("login did not reach persistence hook: %v", ctx.Err())
	}
	if !pool.RemoveAccount(id) {
		t.Fatal("old generation was not removed")
	}
	if err := store.DeleteAccount(ctx, id); err != nil {
		t.Fatalf("delete old persisted account: %v", err)
	}
	fresh := upstream.Account{Mobile: id, Password: "new-password", SessionToken: "new-token"}
	if err := store.SaveAccount(ctx, fresh); err != nil {
		t.Fatalf("save new persisted account: %v", err)
	}
	if err := pool.AddAccount(fresh); err != nil {
		t.Fatalf("add new pool generation: %v", err)
	}
	resume() // stale callback attempts store admission after the new save
	select {
	case got := <-finished:
		if got.err != nil || got.token != "stale-token" {
			t.Fatalf("old login result: token=%q err=%v", got.token, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("stale callback did not complete: %v", ctx.Err())
	}
	for label, source := range map[string]func(context.Context) ([]upstream.Account, error){
		"backing": backing.Load,
		"cache":   store.Load,
	} {
		rows, err := source(ctx)
		if err != nil {
			t.Fatalf("%s load: %v", label, err)
		}
		if len(rows) != 1 || rows[0].Password != fresh.Password || rows[0].SessionToken != fresh.SessionToken {
			t.Errorf("%s contains stale login after identity re-add: %+v; want new password and token", label, rows)
		}
	}
}

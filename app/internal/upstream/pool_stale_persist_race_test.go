package upstream_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// gatedBacking pauses the OLD generation's actual upsert, not the login
// response or the pool's active check. The cache can therefore be deleted and
// recreated while an already-admitted write is in flight.
type gatedBacking struct {
	mu       sync.Mutex
	accounts map[string]upstream.Account
	oldWrite chan struct{}
	resume   chan struct{}
}

func (s *gatedBacking) Load(context.Context) ([]upstream.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]upstream.Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		out = append(out, a)
	}
	return out, nil
}

func (s *gatedBacking) SaveAccount(_ context.Context, a upstream.Account) error {
	if a.SessionToken == "token-1" {
		close(s.oldWrite)
		<-s.resume
	}
	s.mu.Lock()
	s.accounts[a.Identity()] = a
	s.mu.Unlock()
	return nil
}

func (s *gatedBacking) DeleteAccount(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accounts[id]; !ok {
		return accountstore.ErrAccountNotFound
	}
	delete(s.accounts, id)
	return nil
}

func (s *gatedBacking) ApplyPark(upstream.ParkRecord) {}
func (s *gatedBacking) Close() error                  { return nil }

func TestPoolInflightLoginPersistenceCannotOverwriteReaddedAccount(t *testing.T) {
	const id = "13800000000"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backing := &gatedBacking{
		accounts: map[string]upstream.Account{id: {Mobile: id, Password: "old-password"}},
		oldWrite: make(chan struct{}),
		resume:   make(chan struct{}),
	}
	var resumeOnce sync.Once
	resumeOld := func() { resumeOnce.Do(func() { close(backing.resume) }) }
	defer resumeOld()
	store, err := accountstore.NewMemoryFirstStore(ctx, backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/users/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"token-%d"}}}}`, logins.Add(1))
	}))
	defer srv.Close()
	pool, err := upstream.NewPool([]upstream.Account{{Mobile: id, Password: "old-password"}}, upstream.PoolConfig{
		BaseURL: srv.URL, OnLoginPersist: store.ApplyLogin,
	})
	if err != nil {
		t.Fatal(err)
	}
	old, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	type loginResult struct {
		token string
		err   error
	}
	oldResult := make(chan loginResult, 1)
	go func() {
		token, err := old.Token(ctx)
		oldResult <- loginResult{token, err}
	}()
	select {
	case <-backing.oldWrite: // active.Load passed; ApplyLogin reached backing.SaveAccount
	case <-ctx.Done():
		t.Fatalf("old login did not reach backing write: %v", ctx.Err())
	}
	if !pool.RemoveAccount(id) {
		t.Fatal("old account not removed from pool")
	}
	if err := store.DeleteAccount(ctx, id); err != nil {
		t.Fatal(err)
	}
	freshAccount := upstream.Account{Mobile: id, Password: "new-password"}
	if err := store.SaveAccount(ctx, freshAccount); err != nil {
		t.Fatal(err)
	}
	if err := pool.AddAccount(freshAccount); err != nil {
		t.Fatal(err)
	}
	fresh, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if token, err := fresh.Token(ctx); err != nil || token != "token-2" {
		t.Fatalf("new generation login = %q, %v; want token-2", token, err)
	}
	resumeOld() // force stale backing write AFTER the new generation's write
	select {
	case result := <-oldResult:
		if result.err != nil || result.token != "token-1" {
			t.Fatalf("old login = %+v; want token-1", result)
		}
	case <-ctx.Done():
		t.Fatalf("old persistence did not complete: %v", ctx.Err())
	}
	rows, err := backing.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Password != freshAccount.Password || rows[0].SessionToken != "token-2" {
		t.Fatalf("stale login overwrote re-added account in backing: %+v; want new-password/token-2", rows)
	}
	cached, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cached) != 1 || cached[0].Password != freshAccount.Password || cached[0].SessionToken != "token-2" {
		t.Fatalf("unexpected cache after re-add: %+v", cached)
	}
}

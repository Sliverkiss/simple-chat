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
// response or the pool's active check. A serialized store must complete that
// upsert before a concurrent delete/re-add can reach the backing store.
type gatedBacking struct {
	mu       sync.Mutex
	accounts map[string]upstream.Account
	oldWrite chan struct{}
	resume   chan struct{}
	writes   chan string // ordered backing mutations, emitted while holding mu
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
	s.writes <- "save:" + a.SessionToken
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
	s.writes <- "delete"
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
		writes:   make(chan string, 4),
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
	freshAccount := upstream.Account{Mobile: id, Password: "new-password"}
	mutationStarted := make(chan struct{})
	mutationResult := make(chan error, 1)
	go func() {
		close(mutationStarted)
		if err := store.DeleteAccount(ctx, id); err != nil {
			mutationResult <- fmt.Errorf("delete: %w", err)
			return
		}
		if err := store.SaveAccount(ctx, freshAccount); err != nil {
			mutationResult <- fmt.Errorf("re-add save: %w", err)
			return
		}
		mutationResult <- pool.AddAccount(freshAccount)
	}()
	select {
	case <-mutationStarted:
	case <-ctx.Done():
		t.Fatalf("delete/re-add did not start: %v", ctx.Err())
	}
	// The old backing write is already admitted. Release it while the
	// delete/re-add is concurrent; store serialization must order old, delete,
	// then new. Waiting for DeleteAccount before release would deadlock.
	resumeOld()
	select {
	case err := <-mutationResult:
		if err != nil {
			t.Fatalf("delete/re-add: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("delete/re-add did not complete: %v", ctx.Err())
	}
	select {
	case result := <-oldResult:
		if result.err != nil || result.token != "token-1" {
			t.Fatalf("old login token mismatch: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatalf("old persistence did not complete: %v", ctx.Err())
	}
	fresh, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if token, err := fresh.Token(ctx); err != nil || token != "token-2" {
		t.Fatalf("new generation login token mismatch: %v", err)
	}
	for _, want := range []string{"save:token-1", "delete", "save:", "save:token-2"} {
		select {
		case got := <-backing.writes:
			if got != want {
				t.Fatalf("backing mutation order: got %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("backing mutation %q missing: %v", want, ctx.Err())
		}
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

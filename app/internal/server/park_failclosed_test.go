package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// A failed durable park cannot safely be followed by a fresh lease in the
// running process. The backing fake deliberately retains the old ready row.
type failingParkBacking struct {
	*memFake
	fail atomic.Bool
}

func (b *failingParkBacking) SaveAccount(ctx context.Context, a upstream.Account) error {
	if b.fail.Load() && a.ParkKind != "" {
		return errors.New("storage unavailable")
	}
	return b.memFake.SaveAccount(ctx, a)
}

func TestFailedParkWriteStopsNewLeasesInSameProcess(t *testing.T) {
	backing := &failingParkBacking{memFake: newMemFake([]upstream.Account{{Mobile: "100", Password: "pw"}})}
	store, err := accountstore.NewMemoryFirstStore(context.Background(), backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewServer(Config{Accounts: accounts, ParkStore: store})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Shutdown()
	lease, err := gateway.pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	backing.fail.Store(true)
	lease.NoteError(&upstream.BizError{BizCode: 5})
	lease.Release()
	// Admin may subsequently replace the pool row with a ready account. A
	// process-wide latch must still refuse admission after the failed write.
	if ok := gateway.pool.RemoveAccount("100"); !ok {
		t.Fatal("account missing")
	}
	if err := gateway.pool.AddAccount(upstream.Account{Mobile: "100", Password: "pw"}); err != nil {
		t.Fatal(err)
	}
	if fresh, err := gateway.pool.Acquire(context.Background()); err == nil {
		fresh.Release()
		t.Fatal("new lease admitted after failed park write")
	}
	ts := httptest.NewServer(gateway.Handler())
	defer ts.Close()
	status, _ := postHi(t, ts.URL)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("failed park should reject new chat: HTTP %d", status)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Fatal("unhealthy service advertised as ready")
	}
	persisted, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 || persisted[0].ParkKind != "" {
		t.Fatalf("fixture no longer models a lost write: %+v", persisted)
	}
	// This is an explicit negative boundary demonstration, NOT a safety
	// assertion: a fresh process cannot infer the lost mute from old ready.
	restarted, err := accountstore.NewMemoryFirstStore(context.Background(), backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	bootRows, err := restarted.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newProcess, err := NewServer(Config{Accounts: bootRows, ParkStore: restarted})
	if err != nil {
		t.Fatal(err)
	}
	defer newProcess.Shutdown()
	unsafeLease, err := newProcess.pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("fixture no longer exposes the unavoidable restart gap: %v", err)
	}
	unsafeLease.Release()
}

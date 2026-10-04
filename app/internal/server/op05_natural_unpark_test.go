package server

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

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// Fail only the natural clear, not the initial park or unrelated writes.
// The backing keeps the old muted row when the clear is rejected.
type failingUnparkBacking struct {
	*memFake
	identity string
	clears   atomic.Int32
}

func (b *failingUnparkBacking) SaveAccount(ctx context.Context, a upstream.Account) error {
	if a.Identity() == b.identity && a.ParkKind == "" {
		b.clears.Add(1)
		return errors.New("clear unavailable")
	}
	return b.memFake.SaveAccount(ctx, a)
}

func TestOP05ExpiredParkClearFailsWithinAcquireClosesAdmission(t *testing.T) {
	const parkedID = "100"
	// RFC3339 rounds to seconds: use a future whole-second boundary.
	until := time.Now().Add(2 * time.Second).Truncate(time.Second)
	parked := upstream.Account{Mobile: parkedID, Password: "fictional", ParkKind: "muted", ParkUntil: until.Format(time.RFC3339), ParkReason: "fixture"}
	ready := upstream.Account{Mobile: "101", Password: "fictional"}
	backing := &failingUnparkBacking{memFake: newMemFake([]upstream.Account{parked, ready}), identity: parkedID}
	store, err := accountstore.NewMemoryFirstStore(context.Background(), backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	var upstreamCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer up.Close()
	// Explicit ring order: expired park is reconciled before a distinct
	// ready candidate, independent of map iteration and scored selection.
	gateway, err := NewServer(Config{Accounts: []upstream.Account{parked, ready}, ParkStore: store, UpstreamBase: up.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Shutdown()
	if d := time.Until(until); d > 0 {
		time.Sleep(d + 10*time.Millisecond)
	}
	lease, err := gateway.pool.Acquire(context.Background())
	if lease != nil {
		lease.Release()
		t.Fatal("the same Acquire issued a lease after its clear write failed")
	}
	if !errors.Is(err, upstream.ErrParkPersistence) {
		t.Fatalf("Acquire error = %v, want ErrParkPersistence", err)
	}
	if !store.ParkWriteFailed() || backing.clears.Load() != 1 {
		t.Fatalf("natural clear did not latch: failed=%v clears=%d", store.ParkWriteFailed(), backing.clears.Load())
	}
	rows, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Identity() == parkedID && row.ParkKind != "muted" {
			t.Fatalf("backing incorrectly cleared the parked row: %+v", row)
		}
	}
	gw := httptest.NewServer(gateway.Handler())
	defer gw.Close()
	for _, tc := range []struct{ path, body string }{
		{"/v1/chat/completions", switchRequest},
		{"/v1/web_search", `{"query":"fixture"}`},
	} {
		resp, err := gw.Client().Post(gw.URL+tc.path, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s HTTP %d, want 503", tc.path, resp.StatusCode)
		}
	}
	resp, err := gw.Client().Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz HTTP %d, want 503", resp.StatusCode)
	}
	if !gateway.pool.RemoveAccount(ready.Identity()) {
		t.Fatal("ready account missing before hot replacement")
	}
	if err := gateway.pool.AddAccount(ready); err != nil {
		t.Fatal(err)
	}
	if lease, err := gateway.pool.Acquire(context.Background()); lease != nil || !errors.Is(err, upstream.ErrParkPersistence) {
		if lease != nil {
			lease.Release()
		}
		t.Fatalf("hot replacement reopened admission: lease=%v err=%v", lease, err)
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("upstream calls = %d, want zero", upstreamCalls.Load())
	}
}

// Memory-first store tests: boot loads once, the request path performs zero
// backing reads, write-through happens exactly at the spec'd moments. The
// backing is a counting fake — no real Redis, no JSON file.
package accountstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// countingStore wraps a Store and counts every method call per verb.
type countingStore struct {
	inner Store

	mu     sync.Mutex
	loads  int
	saves  int
	deletes int
	parks  int
}

func (c *countingStore) Load(ctx context.Context) ([]upstream.Account, error) {
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	return c.inner.Load(ctx)
}

func (c *countingStore) SaveAccount(ctx context.Context, acct upstream.Account) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return c.inner.SaveAccount(ctx, acct)
}

func (c *countingStore) DeleteAccount(ctx context.Context, identity string) error {
	c.mu.Lock()
	c.deletes++
	c.mu.Unlock()
	return c.inner.DeleteAccount(ctx, identity)
}

func (c *countingStore) ApplyPark(rec upstream.ParkRecord) {
	c.mu.Lock()
	c.parks++
	c.mu.Unlock()
	c.inner.ApplyPark(rec)
}

func (c *countingStore) Close() error { return c.inner.Close() }

func (c *countingStore) counts() (loads, saves, deletes, parks int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.saves, c.deletes, c.parks
}

// memEnv wires a memory-first store over a counting fake over an in-memory
// map. Seed accounts are written through the fake directly (no boot reads).
type memFake struct {
	mu       sync.Mutex
	accounts map[string]upstream.Account
	failSave error
}

func newMemFake(accounts ...upstream.Account) *memFake {
	f := &memFake{accounts: map[string]upstream.Account{}}
	for _, a := range accounts {
		f.accounts[a.Identity()] = a
	}
	return f
}

func (f *memFake) Load(ctx context.Context) ([]upstream.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []upstream.Account
	for _, a := range f.accounts {
		out = append(out, a)
	}
	return out, nil
}

func (f *memFake) SaveAccount(ctx context.Context, acct upstream.Account) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSave != nil {
		return f.failSave
	}
	f.accounts[acct.Identity()] = acct
	return nil
}

func (f *memFake) DeleteAccount(ctx context.Context, identity string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.accounts[identity]; !ok {
		return ErrAccountNotFound
	}
	delete(f.accounts, identity)
	return nil
}

func (f *memFake) ApplyPark(rec upstream.ParkRecord) {}

func (f *memFake) Close() error { return nil }

// memTestEnv builds the memory-first store over a counted fake.
func memTestEnv(t *testing.T, accounts ...upstream.Account) (*MemoryFirstStore, *countingStore, *memFake) {
	t.Helper()
	fake := newMemFake(accounts...)
	counted := &countingStore{inner: fake}
	store, err := NewMemoryFirstStore(context.Background(), counted, nil)
	if err != nil {
		t.Fatalf("NewMemoryFirstStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, counted, fake
}

// (boot) exactly one backing Load at construction; later Loads serve the
// cache with zero further backing reads.
func TestMemoryFirstBootLoadsOnce(t *testing.T) {
	store, counted, _ := memTestEnv(t, upstream.Account{Mobile: "100", Password: "pw"})
	if loads, _, _, _ := counted.counts(); loads != 1 {
		t.Fatalf("boot performed %d backing Loads, want exactly 1", loads)
	}
	for i := 0; i < 3; i++ {
		loaded, err := store.Load(context.Background())
		if err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
		if len(loaded) != 1 || loaded[0].Mobile != "100" {
			t.Fatalf("Load %d returned %+v", i, loaded)
		}
	}
	if loads, _, _, _ := counted.counts(); loads != 1 {
		t.Fatalf("request-path Loads hit the backing: %d total, want 1 (boot only)", loads)
	}
}

// (boot) an empty backing store is a valid state — no error, empty pool.
func TestMemoryFirstEmptyBootIsValid(t *testing.T) {
	store, counted, _ := memTestEnv(t)
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("empty boot: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("empty boot returned %d accounts, want 0", len(loaded))
	}
	if loads, _, _, _ := counted.counts(); loads != 1 {
		t.Fatalf("empty boot performed %d Loads, want 1", loads)
	}
}

// (write-through) ApplyPark merges in memory and writes exactly one full
// record through — no backing Load, no GET.
func TestMemoryFirstApplyParkWritesThroughOnce(t *testing.T) {
	store, counted, _ := memTestEnv(t, upstream.Account{Mobile: "100", Password: "pw"})
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: until, Reason: "user is muted"})
	loads, saves, _, _ := counted.counts()
	if loads != 1 {
		t.Errorf("ApplyPark performed a backing read (loads=%d, want 1 boot-only)", loads)
	}
	if saves != 1 {
		t.Errorf("ApplyPark wrote %d records, want exactly 1", saves)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].ParkKind != "muted" || loaded[0].ParkReason != "user is muted" {
		t.Fatalf("park did not merge into cache: %+v", loaded[0])
	}
}

// (write-through) ApplyLogin sets the session token on the cached record and
// writes through once; the token round-trips through a fresh wrapper over the
// same backing (the data a restart needs to resume).
func TestMemoryFirstApplyLoginRoundTripsToken(t *testing.T) {
	store, counted, fake := memTestEnv(t, upstream.Account{Mobile: "100", Password: "pw"})
	store.ApplyLogin(upstream.LoginRecord{Identity: "100", Token: "tok-abc-123"})
	loads, saves, _, _ := counted.counts()
	if loads != 1 {
		t.Errorf("ApplyLogin performed a backing read (loads=%d, want 1 boot-only)", loads)
	}
	if saves != 1 {
		t.Errorf("ApplyLogin wrote %d records, want exactly 1", saves)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].SessionToken != "tok-abc-123" {
		t.Fatalf("token not in cached record: %+v", loaded[0])
	}
	// Restart: a fresh wrapper over the same backing sees the token.
	restarted, err := NewMemoryFirstStore(context.Background(), fake, nil)
	if err != nil {
		t.Fatalf("restart boot: %v", err)
	}
	defer restarted.Close()
	again, err := restarted.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].SessionToken != "tok-abc-123" {
		t.Fatalf("token did not survive restart: %+v", again)
	}
}

// (admin surface) SaveAccount/DeleteAccount write through exactly once and
// the cache follows; a backing save error propagates with the cache
// untouched.
func TestMemoryFirstAdminOpsWriteThrough(t *testing.T) {
	store, counted, fake := memTestEnv(t)
	// Upload.
	if err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "100", Password: "pw", DeviceID: "dev"}); err != nil {
		t.Fatalf("SaveAccount: %v", err)
	}
	if _, saves, deletes, _ := counted.counts(); saves != 1 || deletes != 0 {
		t.Fatalf("upload counts: saves=%d deletes=%d, want 1/0", saves, deletes)
	}
	// Duplicate check surface (the admin API's store.Load) sees it in memory.
	loaded, _ := store.Load(context.Background())
	if len(loaded) != 1 || loaded[0].Mobile != "100" {
		t.Fatalf("upload not visible in cache: %+v", loaded)
	}
	// Remove.
	if err := store.DeleteAccount(context.Background(), "100"); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if _, saves, deletes, _ := counted.counts(); saves != 1 || deletes != 1 {
		t.Fatalf("remove counts: saves=%d deletes=%d, want 1/1", saves, deletes)
	}
	loaded, _ = store.Load(context.Background())
	if len(loaded) != 0 {
		t.Fatalf("remove not visible in cache: %+v", loaded)
	}
	if err := store.DeleteAccount(context.Background(), "100"); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("double delete: want ErrAccountNotFound, got %v", err)
	}
	// Backing save failure propagates; cache untouched.
	fake.mu.Lock()
	fake.failSave = errors.New("redis outage")
	fake.mu.Unlock()
	err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "101", Password: "pw"})
	if err == nil {
		t.Fatal("backing save failure must propagate")
	}
	loaded, _ = store.Load(context.Background())
	if len(loaded) != 0 {
		t.Fatalf("failed save leaked into cache: %+v", loaded)
	}
}

// (fail-soft) a backing write failure in ApplyPark/ApplyLogin logs and never
// propagates; the in-memory state stays correct.
func TestMemoryFirstApplyFailSoft(t *testing.T) {
	store, counted, fake := memTestEnv(t, upstream.Account{Mobile: "100", Password: "pw"})
	fake.mu.Lock()
	fake.failSave = errors.New("redis outage")
	fake.mu.Unlock()
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	store.ApplyLogin(upstream.LoginRecord{Identity: "100", Token: "tok"})
	loaded, _ := store.Load(context.Background())
	if loaded[0].ParkKind != "muted" || loaded[0].SessionToken != "tok" {
		t.Fatalf("in-memory state wrong after failed writes: %+v", loaded[0])
	}
	if _, saves, _, _ := counted.counts(); saves != 2 {
		t.Fatalf("failed writes: %d attempts, want 2", saves)
	}
}

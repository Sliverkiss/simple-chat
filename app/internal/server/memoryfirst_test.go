// Server-level memory-first tests (docs-spec-memory-first.md): a server
// backed by an empty store (fresh cloud Upstash shape) boots without a crash
// loop, and the request path performs zero backing reads after boot. The
// backing is a counting fake store — no real Redis.
package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// countStore counts every Store call; it wraps any inner store.
type countStore struct {
	inner accountstore.Store

	mu     sync.Mutex
	loads  int
	saves  int
	deletes int
	parks  int
	logins int
}

func (c *countStore) Load(ctx context.Context) ([]upstream.Account, error) {
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	return c.inner.Load(ctx)
}

func (c *countStore) SaveAccount(ctx context.Context, acct upstream.Account) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return c.inner.SaveAccount(ctx, acct)
}

func (c *countStore) DeleteAccount(ctx context.Context, identity string) error {
	c.mu.Lock()
	c.deletes++
	c.mu.Unlock()
	return c.inner.DeleteAccount(ctx, identity)
}

func (c *countStore) ApplyPark(rec upstream.ParkRecord) {
	c.mu.Lock()
	c.parks++
	c.mu.Unlock()
	c.inner.ApplyPark(rec)
}

func (c *countStore) ApplyLogin(rec upstream.LoginRecord) {
	c.mu.Lock()
	c.logins++
	c.mu.Unlock()
	// Not forwarded: the memory-first store writes logins through as full
	// SaveAccount records; the logins counter just proves the hook fired.
}

func (c *countStore) Close() error { return c.inner.Close() }

func (c *countStore) stats() (loads, saves, deletes, parks, logins int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.saves, c.deletes, c.parks, c.logins
}

// newMemoryFirstSrv builds a server over a memory-first store wrapping a
// counting fake over the given seed accounts, mirroring main's flow: the
// boot Load happens inside NewMemoryFirstStore, then the cached accounts
// seed the pool ring.
func newMemoryFirstSrv(t *testing.T, upURL string, seed []upstream.Account) (*httptest.Server, *countStore) {
	t.Helper()
	fake := newMemFake(seed)
	counted := &countStore{inner: fake}
	store, err := accountstore.NewMemoryFirstStore(context.Background(), counted, nil)
	if err != nil {
		t.Fatalf("NewMemoryFirstStore: %v", err)
	}
	accounts, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("post-boot Load: %v", err)
	}
	srv, err := NewServer(Config{
		UpstreamBase: upURL,
		Accounts:     accounts,
		ParkStore:    store,
		MaxInflight:  2,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Shutdown()
		store.Close()
	})
	return ts, counted
}

// memFake is the in-memory backing store (lives here to avoid importing
// accountstore test internals).
type memFake struct {
	mu       sync.Mutex
	accounts map[string]upstream.Account
}

func newMemFake(seed []upstream.Account) *memFake {
	f := &memFake{accounts: map[string]upstream.Account{}}
	for _, a := range seed {
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
	f.accounts[acct.Identity()] = acct
	return nil
}

func (f *memFake) DeleteAccount(ctx context.Context, identity string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.accounts[identity]; !ok {
		return accountstore.ErrAccountNotFound
	}
	delete(f.accounts, identity)
	return nil
}

func (f *memFake) ApplyPark(rec upstream.ParkRecord) {}

func (f *memFake) ApplyLogin(rec upstream.LoginRecord) {}

func (f *memFake) Close() error { return nil }

// (boot) a server over an empty store constructs, serves /healthz, lists
// zero accounts, and accepts an admin upload — no crash loop. This is the
// fresh-cloud-Upstash deployment shape.
func TestServerEmptyMemoryFirstStoreBoots(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	ts, counted := newMemoryFirstSrv(t, up.srv.URL, nil)

	// healthz serves.
	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status %d", resp.StatusCode)
	}

	// Admin list: zero accounts, but the surface works.
	resp, err = ts.Client().Get(ts.URL + "/admin/accounts")
	if err != nil {
		t.Fatalf("admin list: %v", err)
	}
	defer resp.Body.Close()
	var listed struct {
		Accounts []map[string]any `json:"accounts"`
	}
	json.NewDecoder(resp.Body).Decode(&listed)
	if len(listed.Accounts) != 0 {
		t.Fatalf("empty store listed %d accounts", len(listed.Accounts))
	}

	// Admin upload seeds the store + pool; no crash, 200.
	upload := `{"accounts":[{"mobile":"13800000000","password":"pw"}]}`
	resp, err = ts.Client().Post(ts.URL+"/admin/accounts", "application/json", strings.NewReader(upload))
	if err != nil {
		t.Fatalf("admin upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin upload status %d: %s", resp.StatusCode, body)
	}

	// A completion flows end-to-end on the hot-added account.
	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
	resp, err = ts.Client().Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("completion status %d: %s", resp.StatusCode, b)
	}

	// Upload wrote through exactly once (seed) — plus device-id mints if the
	// admin path mints one.
	_, saves, _, _, _ := counted.stats()
	if saves < 1 {
		t.Fatalf("upload performed %d backing saves, want >=1", saves)
	}
}

// (memory-first) the request path performs zero backing reads after boot:
// one full chat completion over a seeded account, then assert the backing
// Load count is still exactly the one boot call.
func TestServerRequestPathNoBackingReads(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	seed := []upstream.Account{{Mobile: "13800000000", Password: "pw", DeviceID: "dev-1"}}
	ts, counted := newMemoryFirstSrv(t, up.srv.URL, seed)

	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hello"}]}`
	resp, err := ts.Client().Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("completion status %d: %s", resp.StatusCode, b)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out["choices"].([]any)) == 0 {
		t.Fatal("completion returned no choices")
	}

	loads, saves, _, _, logins := counted.stats()
	if loads != 1 {
		t.Fatalf("request path hit the backing: loads=%d, want 1 (boot only)", loads)
	}
	// The login write-through moment fired once: the pool's OnLoginPersist
	// sink reached the memory-first store, which materialized it as exactly
	// one full-record backing save carrying the fresh token.
	if logins != 0 {
		t.Fatalf("backing saw %d direct ApplyLogin calls, want 0 (the memory-first store owns that seam)", logins)
	}
	if saves != 1 {
		t.Fatalf("login write-through saves=%d, want exactly 1", saves)
	}
	// The persisted record carries the session token (restart-resume data).
	fake := counted.inner.(*memFake)
	fake.mu.Lock()
	stored, ok := fake.accounts["13800000000"]
	fake.mu.Unlock()
	if !ok || stored.SessionToken != "tok" {
		t.Fatalf("session token not written through: %+v", stored)
	}
}

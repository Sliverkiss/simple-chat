package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// lifecycleStore implements the real Store contract with synchronized in-memory
// rows. Gates pause only after a completed operation, never while holding mu.
// No Redis, file persistence, or upstream authentication is involved.
type lifecycleStore struct {
	mu   sync.Mutex
	rows map[string]upstream.Account

	deleted      chan struct{}
	resumeDelete chan struct{}
	loadedTwice  chan struct{}
	resumeLoads  chan struct{}
	loads        int
}

var _ accountstore.Store = (*lifecycleStore)(nil)

func (s *lifecycleStore) Load(context.Context) ([]upstream.Account, error) {
	s.mu.Lock()
	rows := make([]upstream.Account, 0, len(s.rows))
	for _, a := range s.rows {
		rows = append(rows, a)
	}
	if s.loadedTwice != nil {
		s.loads++
		if s.loads == 2 {
			close(s.loadedTwice)
		}
	}
	s.mu.Unlock()
	if s.resumeLoads != nil {
		<-s.resumeLoads
	}
	return rows, nil
}
func (s *lifecycleStore) SaveAccount(_ context.Context, a upstream.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[a.Identity()] = a
	return nil
}
func (s *lifecycleStore) DeleteAccount(_ context.Context, id string) error {
	s.mu.Lock()
	_, found := s.rows[id]
	delete(s.rows, id)
	s.mu.Unlock()
	if !found {
		return accountstore.ErrAccountNotFound
	}
	if s.deleted != nil {
		close(s.deleted)
		<-s.resumeDelete
	}
	return nil
}
func (*lifecycleStore) ApplyPark(upstream.ParkRecord) {}
func (*lifecycleStore) Close() error                  { return nil }

// Only the test's loopback HTTP handler is used; uploads use channel=web with
// a supplied device id, so they never need upstream login or device minting.
func lifecycleServer(t *testing.T, store *lifecycleStore, seed []upstream.Account) (*httptest.Server, *Server) {
	t.Helper()
	fakeUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected upstream request: %s", r.URL.Path)
		http.Error(w, "unexpected upstream request", http.StatusInternalServerError)
	}))
	t.Cleanup(fakeUpstream.Close)
	s, err := NewServer(Config{UpstreamBase: fakeUpstream.URL, Accounts: seed, ParkStore: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)
	httpServer := httptest.NewServer(s.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer, s
}

type lifecycleResult struct {
	code int
	body string
	err  error
}

func lifecycleRequest(srv *httptest.Server, method, path, body string) lifecycleResult {
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		return lifecycleResult{err: err}
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return lifecycleResult{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return lifecycleResult{code: resp.StatusCode, body: string(b), err: err}
}
func lifecycleWait(t *testing.T, ch <-chan struct{}, stage string) bool {
	t.Helper()
	select {
	case <-ch:
		return true
	case <-time.After(2 * time.Second):
		t.Logf("%s did not overlap; releasing gate (serialized implementation)", stage)
		return false
	}
}
func lifecycleReceive(t *testing.T, ch <-chan lifecycleResult, stage string) lifecycleResult {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("%s: %v", stage, r.err)
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out", stage)
		return lifecycleResult{}
	}
}
func lifecycleHas(snaps []upstream.AccountStatus, id string) bool {
	for _, snap := range snaps {
		if snap.Account.Identity() == id {
			return true
		}
	}
	return false
}

// The DELETE's store operation has committed, but its pool removal has not.
// A concurrent POST may re-create the identity, then DELETE retires that new
// slot: persisted account present, live account missing (logical RED).
func TestAdminLifecycleDeleteUploadSameIdentitySerializes(t *testing.T) {
	const id = "race@example.test"
	seed := upstream.Account{Email: id, Password: "old", Channel: "web", DeviceID: "device-old"}
	store := &lifecycleStore{rows: map[string]upstream.Account{id: seed}, deleted: make(chan struct{}), resumeDelete: make(chan struct{})}
	srv, s := lifecycleServer(t, store, []upstream.Account{seed})
	deletes := make(chan lifecycleResult, 1)
	go func() { deletes <- lifecycleRequest(srv, "DELETE", "/admin/accounts/"+id, "") }()
	if !lifecycleWait(t, store.deleted, "DELETE store removal") {
		close(store.resumeDelete)
		t.Fatal("DELETE never reached gated backing store")
	}
	posts := make(chan lifecycleResult, 1)
	go func() {
		posts <- lifecycleRequest(srv, "POST", "/admin/accounts", fmt.Sprintf(`{"email":%q,"password":"old","channel":"web","device_id":"device-old"}`, id))
	}()
	// If lifecycle mutations are serialized, POST cannot complete until the
	// DELETE finishes; the timeout only releases that gate on a fixed version.
	var post lifecycleResult
	select {
	case post = <-posts:
	case <-time.After(2 * time.Second):
		t.Log("POST waited for DELETE; releasing gated backing store")
	}
	close(store.resumeDelete)
	del := lifecycleReceive(t, deletes, "DELETE")
	if post.code == 0 && post.err == nil {
		post = lifecycleReceive(t, posts, "POST")
	}
	if del.err != nil || del.code != 200 || post.err != nil || post.code != 200 {
		t.Fatalf("DELETE=%d %s (%v); POST=%d %s (%v)", del.code, del.body, del.err, post.code, post.body, post.err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Password != "old" {
		t.Fatalf("POST must persist replacement: rows=%d, err=%v", len(loaded), err)
	}
	if !lifecycleHas(s.pool.Snapshot(), id) {
		t.Fatal("lifecycle divergence: POST persisted replacement but DELETE removed its live pool slot")
	}
}

// Both uploads see the same empty Load before either saves. A correct
// lifecycle serialization allows one 200 and one 409; the current handler
// returns two 200s and may retain the first credentials in the live pool.
func TestAdminLifecycleConcurrentDuplicateUploadsConflict(t *testing.T) {
	const id = "double@example.test"
	store := &lifecycleStore{rows: map[string]upstream.Account{}, loadedTwice: make(chan struct{}), resumeLoads: make(chan struct{})}
	srv, s := lifecycleServer(t, store, nil)
	results := make(chan lifecycleResult, 2)
	for _, password := range []string{"first", "second"} {
		body := fmt.Sprintf(`{"email":%q,"password":%q,"channel":"web","device_id":"device-fixed"}`, id, password)
		go func() { results <- lifecycleRequest(srv, "POST", "/admin/accounts", body) }()
	}
	lifecycleWait(t, store.loadedTwice, "both duplicate-check Loads")
	close(store.resumeLoads)
	first := lifecycleReceive(t, results, "first upload result")
	second := lifecycleReceive(t, results, "second upload result")
	loaded, err := store.Load(context.Background())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("backing store rows=%d, err=%v", len(loaded), err)
	}
	snaps := s.pool.Snapshot()
	if len(snaps) != 1 || snaps[0].Account.Password != loaded[0].Password {
		t.Errorf("lifecycle divergence: persisted credentials differ from the live generation (store=%q, pool slots=%d)", loaded[0].Password, len(snaps))
	}
	if first.code != 200 && second.code != 200 || first.code != 409 && second.code != 409 {
		t.Errorf("concurrent duplicate uploads must yield one 200 and one 409; got %d %s and %d %s", first.code, first.body, second.code, second.body)
	}
}

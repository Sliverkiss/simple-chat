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
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// This fake implements the persistence backing rather than the runtime cache;
// each memory-first store receives a fresh cache over the same durable rows.
// Load clears expired timed parks like the production JSON/Redis stores.
type operationalBacking struct{ *memFake }

func (b *operationalBacking) Load(ctx context.Context) ([]upstream.Account, error) {
	rows, err := b.memFake.Load(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ParkKind != "muted" && rows[i].ParkKind != "risk" {
			continue
		}
		until, err := time.Parse(time.RFC3339, rows[i].ParkUntil)
		if err != nil || !time.Now().After(until) {
			continue
		}
		rows[i].ParkKind, rows[i].ParkUntil, rows[i].ParkReason, rows[i].ParkedAt = "", "", "", ""
		if err := b.SaveAccount(ctx, rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// The fake upstream only exposes per-identity call counts, never credentials.
type operationalUpstream struct {
	srv       *httptest.Server
	mu        sync.Mutex
	calls     map[string]map[string]int
	muteA     bool
	muteUntil time.Time
}

func newOperationalUpstream(t *testing.T) *operationalUpstream {
	t.Helper()
	f := &operationalUpstream{calls: make(map[string]map[string]int), muteUntil: time.Now().Add(30 * time.Minute).Truncate(time.Second)}
	hit := func(id, kind string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.calls[id] == nil {
			f.calls[id] = make(map[string]int)
		}
		f.calls[id][kind]++
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Mobile string `json:"mobile"`
		}
		_ = json.NewDecoder(r.Body).Decode(&v)
		hit(v.Mobile, "login")
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok-" + v.Mobile}}}})
	})
	identity := func(r *http.Request) string { return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-") }
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		id := identity(r)
		hit(id, "session")
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"chat_session": mc{"id": "sess-" + id}}}})
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		id := identity(r)
		f.mu.Lock()
		if f.calls[id] == nil {
			f.calls[id] = make(map[string]int)
		}
		f.calls[id]["completion"]++
		mute := id == "100" && f.muteA
		f.mu.Unlock()
		if mute {
			w.WriteHeader(http.StatusTooManyRequests)
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 5, "biz_msg": "muted", "biz_data": mc{"mute_until": f.muteUntil.Unix()}}})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, searchStream)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *operationalUpstream) setMuteA() { f.mu.Lock(); f.muteA = true; f.mu.Unlock() }
func (f *operationalUpstream) count(id, kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id][kind]
}

func bootOperational(t *testing.T, backing *operationalBacking, upstreamURL string) (*Server, *httptest.Server) {
	t.Helper()
	store, err := accountstore.NewMemoryFirstStore(context.Background(), backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(Config{UpstreamBase: upstreamURL, Accounts: rows, ParkStore: store, QueueWait: 25 * time.Millisecond, RandomSeed: 1})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { gw.Close(); srv.Shutdown(); _ = store.Close() })
	return srv, gw
}

func operationalRequest(t *testing.T, gw *httptest.Server, endpoint string, want int) {
	t.Helper()
	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
	if endpoint == "/v1/web_search" {
		body = `{"query":"hi"}`
	}
	resp, err := gw.Client().Post(gw.URL+endpoint, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("%s HTTP %d, want %d: %s", endpoint, resp.StatusCode, want, payload)
	}
}

func assertNoUpstream(t *testing.T, f *operationalUpstream, identities ...string) {
	t.Helper()
	for _, id := range identities {
		for _, kind := range []string{"login", "session", "completion"} {
			if n := f.count(id, kind); n != 0 {
				t.Fatalf("parked identity %s received %s (%d calls)", id, kind, n)
			}
		}
	}
}

// OP-01/03/04: boot from backing, use both real HTTP routes, discover biz5,
// write through, and boot a NEW store/pool/server over the SAME backing.
func TestOperationalMuteFromBackingThroughRestart(t *testing.T) {
	up := newOperationalUpstream(t)
	bDeadline := time.Now().Add(2 * time.Second).Truncate(time.Second)
	bUntil := bDeadline.UTC().Format(time.RFC3339)
	backing := &operationalBacking{newMemFake([]upstream.Account{
		{Mobile: "100", Password: "fictional", DeviceID: "fixture-a"},
		{Mobile: "101", Password: "fictional", DeviceID: "fixture-b", ParkKind: "muted", ParkUntil: bUntil},
		{Mobile: "102", Password: "fictional", DeviceID: "fixture-c", ParkKind: "banned"},
	})}
	first, gw := bootOperational(t, backing, up.srv.URL)
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, gw, endpoint, http.StatusOK)
	}
	assertNoUpstream(t, up, "101", "102")
	for _, state := range first.pool.Snapshot() {
		want := map[string]string{"100": "ready", "101": "muted", "102": "banned"}[state.Account.Mobile]
		if state.State != want {
			t.Fatalf("startup state %s = %s, want %s", state.Account.Mobile, state.State, want)
		}
	}
	if up.count("100", "completion") != 2 {
		t.Fatal("ready A did not serve chat and search")
	}

	// B remains parked while the first biz5 is discovered by the same
	// running server. No probabilistic selection or second live store needed.
	up.setMuteA()
	operationalRequest(t, gw, "/v1/chat/completions", http.StatusTooManyRequests)
	parked, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range parked {
		if row.Mobile == "100" {
			until, parseErr := time.Parse(time.RFC3339, row.ParkUntil)
			if row.ParkKind != "muted" || parseErr != nil || !until.Equal(up.muteUntil.Add(time.Hour)) {
				t.Fatalf("A park kind=%q until=%q; want muted at T+1h", row.ParkKind, row.ParkUntil)
			}
		}
	}
	// A stale ready duplicate pool slot for the same physical identity must
	// share A's parked manager, rather than bypassing the mute.
	if err := first.pool.AddAccount(upstream.Account{Mobile: "100", Password: "fictional", DeviceID: "fixture-a"}); err != nil {
		t.Fatal(err)
	}
	before := up.count("100", "completion")
	beforeSessions := up.count("100", "session")
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, gw, endpoint, http.StatusTooManyRequests)
	}
	if got := up.count("100", "session"); got != beforeSessions {
		t.Fatalf("new lease created a session for muted A: %d -> %d", beforeSessions, got)
	}
	if got := up.count("100", "completion"); got != before {
		t.Fatalf("new lease reached muted A: %d -> %d", before, got)
	}
	// Once B's original park expires, a NEW store/server loaded from the
	// same backing must choose only B, never A or the permanently banned C.
	time.Sleep(time.Until(bDeadline) + 20*time.Millisecond)
	// Stop the first server before the new process to keep one writer live.
	gw.Close()
	first.Shutdown()
	restarted, next := bootOperational(t, backing, up.srv.URL)
	beforeSessions = up.count("100", "session")
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, next, endpoint, http.StatusOK)
	}
	if got := up.count("100", "session"); got != beforeSessions {
		t.Fatalf("A session created after restart before expiry: %d -> %d", beforeSessions, got)
	}
	if got := up.count("100", "completion"); got != before {
		t.Fatalf("A called after restart before expiry: %d -> %d", before, got)
	}
	if up.count("101", "completion") != 2 {
		t.Fatal("recovered B did not serve chat and search")
	}
	if got := up.count("100", "login"); got != 1 {
		t.Fatalf("muted A logged in again after restart: %d logins", got)
	}
	assertNoUpstream(t, up, "102")
	for _, s := range restarted.pool.Snapshot() {
		if s.Account.Mobile == "100" && s.State != "muted" {
			t.Fatalf("A state after restart = %s", s.State)
		}
		if s.Account.Mobile == "102" && s.State != "banned" {
			t.Fatalf("C state after restart = %s", s.State)
		}
	}
	persisted, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range persisted {
		if row.Mobile == "101" && (row.ParkKind != "" || row.ParkUntil != "") {
			t.Fatalf("B recovery did not clear persisted park: kind=%q until=%q", row.ParkKind, row.ParkUntil)
		}
	}
	// Simulate passing the T+1h boundary without waiting an hour: only the
	// fictional backing row's deadline is advanced after stopping this
	// server. The live process's natural deadline is exercised separately
	// below by TestOperationalPersistedMuteNaturallyRecovers.
	next.Close()
	restarted.Shutdown()
	for _, row := range persisted {
		if row.Mobile != "100" {
			continue
		}
		row.ParkUntil = time.Now().Add(-time.Second).UTC().Format(time.RFC3339)
		if err := backing.SaveAccount(context.Background(), row); err != nil {
			t.Fatal(err)
		}
	}
	up.mu.Lock()
	up.muteA = false
	up.mu.Unlock()
	recoveredServer, recovered := bootOperational(t, backing, up.srv.URL)
	for _, state := range recoveredServer.pool.Snapshot() {
		if state.Account.Mobile == "100" && state.State != "ready" {
			t.Fatalf("A was not eligible after deadline: %s", state.State)
		}
	}
	if ok := recoveredServer.pool.RemoveAccount("101"); !ok {
		t.Fatal("healthy B missing from restarted pool")
	}
	// With A uniquely eligible, the real routes exercise recovered A.
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, recovered, endpoint, http.StatusOK)
	}
	if up.count("100", "completion") != before+2 {
		t.Fatal("recovered A did not serve chat and search")
	}
	rows, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Mobile == "100" && (row.ParkKind != "" || row.ParkUntil != "") {
			t.Fatalf("expired A park not cleared on boot: kind=%q until=%q", row.ParkKind, row.ParkUntil)
		}
	}
	assertNoUpstream(t, up, "102")
}

// OP-04 boundary without sleeping through the production T+1h grace: a
// separate persisted short mute expires naturally, clears backing, and then
// serves both routes; the permanent ban still never receives a request.
func TestOperationalPersistedMuteNaturallyRecovers(t *testing.T) {
	up := newOperationalUpstream(t)
	deadline := time.Now().Add(2 * time.Second).Truncate(time.Second)
	backing := &operationalBacking{newMemFake([]upstream.Account{
		{Mobile: "100", Password: "fictional", DeviceID: "fixture-a", ParkKind: "muted", ParkUntil: deadline.UTC().Format(time.RFC3339)},
		{Mobile: "102", Password: "fictional", DeviceID: "fixture-c", ParkKind: "banned"},
	})}
	_, gw := bootOperational(t, backing, up.srv.URL)
	// Before the deadline no real HTTP route may contact either parked
	// account; queue wait is intentionally bounded by the test configuration.
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, gw, endpoint, http.StatusTooManyRequests)
	}
	assertNoUpstream(t, up, "100", "102")
	// A later real request triggers lazy unpark; no manual store edits.
	time.Sleep(time.Until(deadline) + 20*time.Millisecond)
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		operationalRequest(t, gw, endpoint, http.StatusOK)
	}
	if up.count("100", "completion") != 2 {
		t.Fatal("expired A did not serve both routes")
	}
	assertNoUpstream(t, up, "102")
	rows, err := backing.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Mobile == "100" && (row.ParkKind != "" || row.ParkUntil != "") {
			t.Fatalf("natural recovery did not clear backing park: kind=%q until=%q", row.ParkKind, row.ParkUntil)
		}
		if row.Mobile == "102" && row.ParkKind != "banned" {
			t.Fatalf("permanent ban cleared: kind=%q", row.ParkKind)
		}
	}
}

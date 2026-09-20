// Hot add/remove (admin account management): accounts join and leave the
// ring at runtime without a restart. The invariants pinned here: a hot-added
// account is selectable by the very next Acquire and behaves identically to
// a startup-loaded one (device id, park restore, startup sequence on first
// login); a removed account receives no new acquisitions but its in-flight
// leases finish naturally; round-robin fairness over the survivors holds.
package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// hotFixture counts logins and startup-sequence endpoints per mobile, so a
// test can prove the one-shot launch traffic fires for a hot-added account.
type hotFixture struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits map[string]int // "login:<mobile>", "users:<mobile>", "page:<mobile>"
}

func (f *hotFixture) hit(kind, mobile string) {
	f.mu.Lock()
	f.hits[kind+":"+mobile]++
	f.mu.Unlock()
}

func (f *hotFixture) count(kind, mobile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[kind+":"+mobile]
}

func newHotFixture(t *testing.T) *hotFixture {
	t.Helper()
	f := &hotFixture{hits: map[string]int{}}
	mux := http.NewServeMux()
	login := func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mobile, _ := body["mobile"].(string)
		if mobile == "" {
			mobile, _ = body["email"].(string)
		}
		f.hit("login", mobile)
		fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok-%s"}}}}`, mobile)
	}
	mux.HandleFunc("POST /api/v0/users/login", login)
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		mobile := stringsTrimTok(r)
		f.hit("users", mobile)
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"id":"u1"}}}}`)
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		mobile := stringsTrimTok(r)
		f.hit("page", mobile)
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"session_list":[],"has_more":false}}}`)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// stringsTrimTok derives the mobile from the per-account bearer token.
func stringsTrimTok(r *http.Request) string {
	return stringsTrimPrefix(r.Header.Get("Authorization"), "Bearer tok-")
}

func stringsTrimPrefix(s, p string) string {
	if len(s) >= len(p) && s[:len(p)] == p {
		return s[len(p):]
	}
	return s
}

// (hot-add) a hot-added account joins the round-robin immediately — no
// restart, no gap; the rotation continues over the grown ring.
func TestPoolAddAccountJoinsRotation(t *testing.T) {
	pool := newTestPool(t, 2, 5, nil)
	got := acquireSeq(t, pool, 2)
	if got[0] != "13800000000" || got[1] != "13800000001" {
		t.Fatalf("pre-add rotation = %v", got)
	}
	if err := pool.AddAccount(Account{Mobile: "13800000002", Password: "pw"}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	got = acquireSeq(t, pool, 6)
	want := []string{"13800000000", "13800000001", "13800000002", "13800000000", "13800000001", "13800000002"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("post-add rotation = %v, want %v", got, want)
		}
	}
}

// (hot-add) invalid accounts are rejected and leave the ring untouched.
func TestPoolAddAccountInvalidRejected(t *testing.T) {
	pool := newTestPool(t, 1, 5, nil)
	if err := pool.AddAccount(Account{Mobile: "13800000009", Password: "p", Region: "mars"}); !errors.Is(err, ErrUnknownRegion) {
		t.Fatalf("want ErrUnknownRegion, got %v", err)
	}
	if err := pool.AddAccount(Account{Email: "web@example.com", Password: "p", Channel: "web"}); !errors.Is(err, ErrWebChannelNeedsDeviceID) {
		t.Fatalf("want ErrWebChannelNeedsDeviceID, got %v", err)
	}
	if st := pool.Status(); len(st) != 1 {
		t.Fatalf("ring changed by rejected adds: %v", st)
	}
}

// (startup equivalence) the one-shot app-launch sequence (users/current then
// fetch_page) fires on a hot-added account's first login, exactly like a
// startup-loaded account.
func TestPoolAddAccountFiresStartupSequenceOnFirstUse(t *testing.T) {
	f := newHotFixture(t)
	pool, err := NewPool([]Account{{Mobile: "13800000000", Password: "pw"}}, PoolConfig{BaseURL: f.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.AddAccount(Account{Mobile: "13800000001", Password: "pw"}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Account().Mobile != "13800000001" {
		t.Fatalf("second acquire = %s, want the hot-added account", lease.Account().Mobile)
	}
	if _, err := lease.Token(context.Background()); err != nil {
		t.Fatalf("Token: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count("users", "13800000001") >= 1 && f.count("page", "13800000001") >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("startup sequence never fired for hot-added account: users=%d page=%d",
		f.count("users", "13800000001"), f.count("page", "13800000001"))
}

// (hot-remove) a removed account receives no new acquisitions; an unknown
// id reports not-found.
func TestPoolRemoveAccountStopsSelection(t *testing.T) {
	pool := newTestPool(t, 3, 5, nil)
	if !pool.RemoveAccount("13800000001") {
		t.Fatal("RemoveAccount(mid) = false, want true")
	}
	got := acquireSeq(t, pool, 6)
	for _, m := range got {
		if m == "13800000001" {
			t.Fatalf("removed account still selected: %v", got)
		}
	}
	if len(got) != 6 {
		t.Fatalf("acquire count = %d", len(got))
	}
	if pool.RemoveAccount("99999999999") {
		t.Fatal("RemoveAccount(unknown) = true, want false")
	}
}

// (hot-remove) an in-flight lease on a removed account stays fully
// functional (login, release) — the lease holds the account reference, not
// a ring slot — and the slot returns cleanly.
func TestPoolRemoveAccountInflightLeaseFinishes(t *testing.T) {
	f := newHotFixture(t)
	pool, err := NewPool(
		[]Account{{Mobile: "13800000000", Password: "pw"}, {Mobile: "13800000001", Password: "pw"}},
		PoolConfig{BaseURL: f.srv.URL, MaxInflight: 1, QueueWait: 30 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	l0, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	l1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l1.Account().Mobile != "13800000001" {
		t.Fatalf("second lease = %s", l1.Account().Mobile)
	}
	if !pool.RemoveAccount("13800000001") {
		t.Fatal("RemoveAccount = false")
	}
	// The in-flight lease still logs in through its own client.
	tok, err := l1.Token(context.Background())
	if err != nil {
		t.Fatalf("in-flight Token after remove: %v", err)
	}
	if tok != "tok-13800000001" {
		t.Fatalf("token = %q", tok)
	}
	l1.Release() // must not panic or corrupt anything
	l1.Release() // double release stays a no-op
	// Only the surviving account remains; with its slot held, the pool is
	// busy (the removed account must NOT absorb traffic).
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("want ErrPoolBusy over the removed account, got %v", err)
	}
	l0.Release()
	next, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	if next.Account().Mobile != "13800000000" {
		t.Fatalf("post-remove acquire = %s, want the survivor", next.Account().Mobile)
	}
}

// (hot-remove) removing every account empties the ring: Acquire fails fast
// with ErrNoAccounts (no queue wait — nothing can free a slot), and a
// subsequent hot-add revives the pool.
func TestPoolRemoveAllThenAddRevives(t *testing.T) {
	pool := newTestPool(t, 1, 5, nil)
	if !pool.RemoveAccount("13800000000") {
		t.Fatal("RemoveAccount = false")
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrNoAccounts) {
		t.Fatalf("want ErrNoAccounts on empty ring, got %v", err)
	}
	if err := pool.AddAccount(Account{Mobile: "13800000007", Password: "pw"}); err != nil {
		t.Fatalf("AddAccount into empty pool: %v", err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire after revive: %v", err)
	}
	if lease.Account().Mobile != "13800000007" {
		t.Fatalf("revived acquire = %s", lease.Account().Mobile)
	}
	lease.Release()
}

// (hot-remove) round-robin stays fair over the survivors: after a removal
// before the cursor, the next acquisitions still cover every survivor — no
// account is skipped or double-served.
func TestPoolRemoveAccountKeepsRotationFair(t *testing.T) {
	pool := newTestPool(t, 3, 5, nil)
	acquireSeq(t, pool, 2) // cursor now at account 2
	pool.RemoveAccount("13800000000")
	got := acquireSeq(t, pool, 2)
	seen := map[string]bool{}
	for _, m := range got {
		seen[m] = true
	}
	if !seen["13800000001"] || !seen["13800000002"] {
		t.Fatalf("unfair rotation after remove: %v", got)
	}
}

// (list material) Snapshot reports the live runtime state: health, park
// window/reason, in-flight counts, and token warmth.
func TestPoolSnapshotReportsLiveState(t *testing.T) {
	pool := newTestPool(t, 2, 2, nil)
	l1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Hour)
	l1.NoteError(&BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: until})
	l1.Release()

	l2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l2.Account().Mobile != "13800000001" {
		t.Fatalf("second acquire = %s (muted account must be skipped)", l2.Account().Mobile)
	}
	if _, err := l2.Token(context.Background()); err != nil {
		t.Fatalf("Token: %v", err)
	}
	l2.Release()

	snaps := pool.Snapshot()
	if len(snaps) != 2 {
		t.Fatalf("snapshot has %d accounts, want 2", len(snaps))
	}
	byMobile := map[string]AccountStatus{}
	for _, s := range snaps {
		byMobile[s.Account.Mobile] = s
	}
	muted := byMobile["13800000000"]
	if muted.State != "muted" || muted.ParkKind != "muted" {
		t.Errorf("muted account snapshot: %+v", muted)
	}
	if muted.ParkUntil.IsZero() || time.Until(muted.ParkUntil) < 50*time.Minute {
		t.Errorf("muted park until = %v, want ~1h ahead", muted.ParkUntil)
	}
	if muted.ParkReason == "" || muted.TokenWarm {
		t.Errorf("muted park reason/warmth: %q %v", muted.ParkReason, muted.TokenWarm)
	}
	if muted.Inflight != 0 || muted.MaxInflight != 2 {
		t.Errorf("muted inflight = %d/%d, want 0/2", muted.Inflight, muted.MaxInflight)
	}
	ready := byMobile["13800000001"]
	if ready.State != "ready" || ready.ParkKind != "" || !ready.TokenWarm {
		t.Errorf("ready account snapshot: %+v", ready)
	}
	if ready.Account.Password != "pw" {
		t.Errorf("snapshot must carry the full record, password = %q", ready.Account.Password)
	}
}

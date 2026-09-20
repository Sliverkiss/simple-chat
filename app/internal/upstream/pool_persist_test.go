// Park-state persistence tests (TASK_MUTE): the pool's in-memory parking is
// the fast path; these tests pin the persistence contract — the sink fires on
// transitions, persisted fields seed a restarted pool, and parked accounts
// receive zero upstream traffic.
package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// persistRecorder captures ParkRecords handed to the persistence sink.
type persistRecorder struct {
	mu   sync.Mutex
	recs []ParkRecord
}

func (r *persistRecorder) sink(rec ParkRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec)
}

func (r *persistRecorder) all() []ParkRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ParkRecord(nil), r.recs...)
}

func (r *persistRecorder) contains(kind BanState) bool {
	for _, rec := range r.all() {
		if rec.Kind == kind {
			return true
		}
	}
	return false
}

// countingFixture counts every upstream request per account mobile, so a test
// can prove a parked account received zero traffic.
type countingFixture struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests map[string]int
}

func (f *countingFixture) count(mobile string) {
	f.mu.Lock()
	f.requests[mobile]++
	f.mu.Unlock()
}

func (f *countingFixture) hits(mobile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[mobile]
}

func newCountingFixture(t *testing.T) *countingFixture {
	t.Helper()
	f := &countingFixture{requests: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mobile, _ := body["mobile"].(string)
		f.count(mobile)
		fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok-%s"}}}}`, mobile)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.count(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-"))
		w.WriteHeader(http.StatusNotFound)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// (a) parking a muted account fires the persistence sink with the window.
func TestNoteErrorMutePersistsViaSink(t *testing.T) {
	rec := &persistRecorder{}
	pool := newTestPool(t, 1, 5, func(c PoolConfig) PoolConfig {
		c.OnParkPersist = rec.sink
		return c
	})
	l, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Hour)
	l.NoteError(&BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: until})
	l.Release()

	recs := rec.all()
	if len(recs) != 1 {
		t.Fatalf("sink records = %d, want 1", len(recs))
	}
	got := recs[0]
	if got.Mobile != "13800000000" || got.Kind != BanMuted {
		t.Errorf("record = {%s %d}, want {13800000000 muted}", got.Mobile, got.Kind)
	}
	if !got.Until.Equal(until) {
		t.Errorf("until = %v, want %v", got.Until, until)
	}
	if !strings.Contains(got.Reason, "muted") {
		t.Errorf("reason = %q, want it to carry the upstream message", got.Reason)
	}
}

// (a, banned) a banned account persists with no expiry window.
func TestNoteErrorBannedPersistsViaSink(t *testing.T) {
	rec := &persistRecorder{}
	pool := newTestPool(t, 1, 5, func(c PoolConfig) PoolConfig {
		c.OnParkPersist = rec.sink
		return c
	})
	l, _ := pool.Acquire(context.Background())
	l.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	l.Release()

	recs := rec.all()
	if len(recs) != 1 || recs[0].Kind != BanBanned {
		t.Fatalf("records = %+v, want one banned", recs)
	}
	if !recs[0].Until.IsZero() {
		t.Errorf("banned until = %v, want zero (forever)", recs[0].Until)
	}
}

// (b) a restart (fresh pool) with a future mute_until in the account fields
// keeps the account out of rotation — and it receives ZERO upstream requests.
func TestPoolRestoresParkedAccountFromDisk(t *testing.T) {
	f := newCountingFixture(t)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	accounts := []Account{
		{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: future, ParkReason: "user is muted"},
		{Mobile: "101", Password: "pw"},
	}
	var logs []string
	var logMu sync.Mutex
	pool, err := NewPool(accounts, PoolConfig{
		BaseURL:     f.srv.URL,
		MaxInflight: 5,
		QueueWait:   50 * time.Millisecond,
		Logger: func(format string, args ...any) {
			logMu.Lock()
			logs = append(logs, fmt.Sprintf(format, args...))
			logMu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile != "101" {
			t.Fatalf("acquire %d = %s: parked account must never be leased", i, lease.Account().Mobile)
		}
		if _, err := lease.Token(context.Background()); err != nil {
			t.Fatalf("token on healthy account: %v", err)
		}
		lease.Release()
	}
	if n := f.hits("100"); n != 0 {
		t.Fatalf("parked account received %d upstream requests, want 0", n)
	}
	// Copy under the mutex: the startup-sequence goroutine can still be
	// appending through the pool logger.
	logMu.Lock()
	joined := strings.Join(logs, "\n")
	logMu.Unlock()
	if !strings.Contains(joined, "restored from disk") || !strings.Contains(joined, "still MUTED until") {
		t.Errorf("restore log missing; logs:\n%s", joined)
	}
	if !strings.Contains(joined, "1 parked at startup") {
		t.Errorf("startup summary missing parked count; logs:\n%s", joined)
	}
}

// (c) a restart with a past mute_until rotates normally.
func TestPoolExpiredParkOnLoadIsReady(t *testing.T) {
	f := newCountingFixture(t)
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	accounts := []Account{
		{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: past, ParkReason: "stale"},
		{Mobile: "101", Password: "pw"},
	}
	pool, err := NewPool(accounts, PoolConfig{BaseURL: f.srv.URL, MaxInflight: 5, QueueWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	got := acquireSeq(t, pool, 4)
	want := []string{"100", "101", "100", "101"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rotation = %v, want %v (expired park must not block)", got, want)
		}
	}
}

// (d) a banned account stays out of rotation forever across restarts.
func TestPoolRestoresBannedForever(t *testing.T) {
	f := newCountingFixture(t)
	accounts := []Account{
		{Mobile: "100", Password: "pw", ParkKind: "banned", ParkReason: "USER_IS_BANNED"},
		{Mobile: "101", Password: "pw"},
	}
	pool, err := NewPool(accounts, PoolConfig{BaseURL: f.srv.URL, MaxInflight: 5, QueueWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile == "100" {
			t.Fatal("restored-banned account must never be leased")
		}
		lease.Release()
	}
	// A pool whose only account is restored-banned answers ErrNoAccounts.
	pool1, err := NewPool([]Account{accounts[0]}, PoolConfig{BaseURL: f.srv.URL, MaxInflight: 5, QueueWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool1.Acquire(context.Background()); !errors.Is(err, ErrNoAccounts) {
		t.Fatalf("want ErrNoAccounts, got %v", err)
	}
}

// (e) a natural unpark (park expiry) clears the persisted fields via the sink.
func TestNaturalUnparkClearsPersistedFields(t *testing.T) {
	rec := &persistRecorder{}
	pool := newTestPool(t, 1, 5, func(c PoolConfig) PoolConfig {
		c.OnParkPersist = rec.sink
		return c
	})
	l, _ := pool.Acquire(context.Background())
	l.NoteError(&BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: time.Now().Add(80 * time.Millisecond)})
	l.Release()
	if !rec.contains(BanMuted) {
		t.Fatal("park was not persisted")
	}
	// Wait for the natural unpark: acquire succeeds again.
	deadline := time.Now().Add(2 * time.Second)
	for {
		l2, err := pool.Acquire(context.Background())
		if err == nil {
			l2.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("account never returned from mute")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rec.contains(BanNone) {
		t.Fatal("natural unpark did not clear the persisted park fields")
	}
}

// (f) concurrent park/unpark transitions through the sink stay race-free.
func TestConcurrentParkTransitionsThroughSink(t *testing.T) {
	f := newCountingFixture(t)
	rec := &persistRecorder{}
	accounts := []Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
		{Mobile: "102", Password: "pw"},
	}
	pool, err := NewPool(accounts, PoolConfig{
		BaseURL:     f.srv.URL,
		MaxInflight: 4,
		QueueWait:   10 * time.Millisecond,
		OnParkPersist: rec.sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				lease, err := pool.Acquire(context.Background())
				if err != nil {
					continue // all busy/parked this pass; next round retries
				}
				// Short windows: parks expire quickly, driving concurrent
				// unpark clears in other goroutines' acquire passes.
				lease.NoteError(&BizError{
					BizCode:  5,
					BizMsg:   "user is muted",
					MuteUntil: time.Now().Add(time.Duration(20+g*5) * time.Millisecond),
				})
				lease.Release()
			}
		}(g)
	}
	wg.Wait()
	// All parks have short windows; every account must return to rotation.
	deadline := time.Now().Add(3 * time.Second)
	for {
		ok := true
		for i := 0; i < 3; i++ {
			lease, err := pool.Acquire(context.Background())
			if err != nil {
				ok = false
				break
			}
			lease.Release()
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("accounts never returned to rotation after concurrent parks")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestParkKindRoundTrip(t *testing.T) {
	for _, b := range []BanState{BanBanned, BanMuted, BanRiskDevice} {
		if ParseParkKind(ParkKindName(b)) != b {
			t.Errorf("round trip %d failed", b)
		}
	}
	if ParseParkKind("") != BanNone || ParseParkKind("banana") != BanNone {
		t.Error("unknown park kinds must parse to BanNone")
	}
	if ParkKindName(BanNone) != "" {
		t.Error("BanNone must render empty")
	}
}

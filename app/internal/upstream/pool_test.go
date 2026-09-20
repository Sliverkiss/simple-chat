package upstream

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newTestPool builds a pool over N accounts against a mock upstream.
func newTestPool(t *testing.T, n, maxInflight int, mutate func(PoolConfig) PoolConfig) *Pool {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok"}}}}`)
		_ = body
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	accounts := make([]Account, n)
	for i := range accounts {
		accounts[i] = Account{
			Mobile:   fmt.Sprintf("1380000000%d", i),
			Password: "pw",
		}
	}
	cfg := PoolConfig{BaseURL: srv.URL, MaxInflight: maxInflight, QueueWait: 50 * time.Millisecond}
	if mutate != nil {
		cfg = mutate(cfg)
	}
	pool, err := NewPool(accounts, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func acquireSeq(t *testing.T, pool *Pool, n int) []string {
	t.Helper()
	var got []string
	for i := 0; i < n; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		got = append(got, lease.Account().Mobile)
		lease.Release()
	}
	return got
}

func TestPoolRoundRobinRotation(t *testing.T) {
	pool := newTestPool(t, 3, 5, nil)
	got := acquireSeq(t, pool, 6)
	want := []string{"13800000000", "13800000001", "13800000002", "13800000000", "13800000001", "13800000002"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rotation = %v, want %v", got, want)
		}
	}
}

func TestPoolInflightCapBlocksThenResumes(t *testing.T) {
	pool := newTestPool(t, 1, 1, nil)
	l1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The single slot is held: a second acquire must exhaust the queue wait
	// and fail with ErrPoolBusy carrying a positive Retry-After.
	_, err = pool.Acquire(context.Background())
	if !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("want ErrPoolBusy, got %v", err)
	}
	var busy *PoolBusyError
	if !errors.As(err, &busy) || busy.RetryAfter <= 0 {
		t.Fatalf("want PoolBusyError with RetryAfter > 0, got %v", err)
	}
	l1.Release()
	l2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	l2.Release()
}

func TestPoolSkipsFullAccount(t *testing.T) {
	pool := newTestPool(t, 2, 1, nil)
	l1, _ := pool.Acquire(context.Background()) // account 0, slot held
	if l1.Account().Mobile != "13800000000" {
		t.Fatalf("first acquire = %s, want account 0", l1.Account().Mobile)
	}
	l2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l2.Account().Mobile != "13800000001" {
		t.Fatalf("second acquire = %s, want account 1 (account 0 is full)", l2.Account().Mobile)
	}
	l1.Release()
	l2.Release()
	// Ring now points back at account 0; strict round-robin resumes there.
	l3, _ := pool.Acquire(context.Background())
	if l3.Account().Mobile != "13800000000" {
		t.Fatalf("third acquire = %s, want account 0 (ring resumed)", l3.Account().Mobile)
	}
	l3.Release()
}

func TestPoolBannedSkippedForever(t *testing.T) {
	pool := newTestPool(t, 2, 5, nil)
	l, _ := pool.Acquire(context.Background())
	if l.Account().Mobile != "13800000000" {
		t.Fatalf("first acquire = %s", l.Account().Mobile)
	}
	l.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	l.Release()
	for i := 0; i < 6; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile == "13800000000" {
			t.Fatal("banned account must never be selected again")
		}
		lease.Release()
	}
}

func TestPoolMutedParksUntilMuteUntil(t *testing.T) {
	pool := newTestPool(t, 1, 5, nil)
	l, _ := pool.Acquire(context.Background())
	until := time.Now().Add(120 * time.Millisecond)
	l.NoteError(&BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: until})
	l.Release()

	// While muted the pool must refuse; the queue wait (50ms) expires first.
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("want ErrPoolBusy while muted, got %v", err)
	}
	// After mute_until passes the account rejoins rotation.
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
		time.Sleep(30 * time.Millisecond)
	}
}

func TestPoolMutedWithoutUntilUsesDefault(t *testing.T) {
	pool := newTestPool(t, 1, 5, func(c PoolConfig) PoolConfig {
		c.MuteParkDefault = 80 * time.Millisecond
		return c
	})
	l, _ := pool.Acquire(context.Background())
	l.NoteError(&BizError{BizCode: 5, BizMsg: "user is muted"}) // no MuteUntil
	l.Release()
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("want ErrPoolBusy during default mute park, got %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		l2, err := pool.Acquire(context.Background())
		if err == nil {
			l2.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("account never returned from default mute park")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPoolRiskDeviceCooldown(t *testing.T) {
	pool := newTestPool(t, 1, 5, func(c PoolConfig) PoolConfig {
		c.RiskCooldown = 80 * time.Millisecond
		return c
	})
	l, _ := pool.Acquire(context.Background())
	l.NoteError(&BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"})
	l.Release()
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("want ErrBusy during risk cooldown, got %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		l2, err := pool.Acquire(context.Background())
		if err == nil {
			l2.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("account never returned from risk cooldown")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPoolAllBannedFailsFast(t *testing.T) {
	pool := newTestPool(t, 2, 5, nil)
	for i := 0; i < 2; i++ {
		l, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		l.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
		l.Release()
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrNoAccounts) {
		t.Fatalf("want ErrNoAccounts, got %v", err)
	}
}

func TestPoolRejectsInvalidAccounts(t *testing.T) {
	if _, err := NewPool([]Account{{Mobile: "1", Password: "p", Region: "mars"}}, PoolConfig{MaxInflight: 1}); !errors.Is(err, ErrUnknownRegion) {
		t.Fatalf("want ErrUnknownRegion, got %v", err)
	}
	// An empty ring is valid (docs-spec-memory-first.md): fresh cloud Upstash
	// boots empty. Invalid accounts still reject.
	if _, err := NewPool(nil, PoolConfig{MaxInflight: 1}); err != nil {
		t.Fatalf("empty pool must construct, got %v", err)
	}
}

func TestParseMuteUntil(t *testing.T) {
	// unix seconds as number
	ts := time.Unix(1900000000, 0)
	if got := parseMuteUntil([]byte(fmt.Sprintf(`{"mute_until":%d}`, ts.Unix()))); !got.Equal(ts) {
		t.Errorf("numeric unix seconds: got %v, want %v", got, ts)
	}
	// unix seconds as string
	if got := parseMuteUntil([]byte(`{"mute_until":"1900000000"}`)); !got.Equal(ts) {
		t.Errorf("string unix seconds: got %v, want %v", got, ts)
	}
	// RFC3339 (1900000000 = 2030-03-17T17:46:40Z)
	rfc := "2030-03-17T17:46:40Z"
	if got := parseMuteUntil([]byte(fmt.Sprintf(`{"mute_until":%q}`, rfc))); !got.Equal(ts) {
		t.Errorf("RFC3339: got %v, want %v", got, ts)
	}
	// garbage → zero time
	if got := parseMuteUntil([]byte(`{"mute_until":null}`)); !got.IsZero() {
		t.Errorf("null: got %v, want zero", got)
	}
}

func TestPoolConcurrentRotationAndCaps(t *testing.T) {
	// 2 accounts, cap 1 each: 5 concurrent acquires must all succeed (some
	// via queue wait) and total in-flight per account never exceeds 1.
	pool := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig {
		c.QueueWait = 5 * time.Second
		return c
	})
	var mu sync.Mutex
	inflight := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := pool.Acquire(context.Background())
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			inflight[lease.Account().Mobile]++
			if inflight[lease.Account().Mobile] > 1 {
				t.Errorf("in-flight cap violated for %s", lease.Account().Mobile)
			}
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			inflight[lease.Account().Mobile]--
			mu.Unlock()
			lease.Release()
		}()
	}
	wg.Wait()
}
func TestPoolDuplicateEntriesAreSeparateSlots(t *testing.T) {
	// accounts.json may legitimately repeat one credential (the live smoke
	// test does this): each entry is its own ring slot with its own
	// AccountManager, giving the same physical account more slots. Banning
	// one slot must not kill the other.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok"}}}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	accounts := []Account{
		{Mobile: "13800000000", Password: "pw"},
		{Mobile: "13800000000", Password: "pw"},
	}
	pool, err := NewPool(accounts, PoolConfig{BaseURL: srv.URL, MaxInflight: 1, QueueWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(pool.accounts); n != 2 {
		t.Fatalf("pool has %d slots, want 2 (duplicate entries = separate slots)", n)
	}
	// Both slots usable concurrently: each has its own semaphore.
	l1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	l2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Banning one slot leaves the other alive.
	l1.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	l1.Release()
	l2.Release()
	l3, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	l3.Release()
}

func TestLeaseClientAndDelegates(t *testing.T) {
	pool := newTestPool(t, 1, 5, nil)
	l, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l.Client() == nil {
		t.Fatal("lease must expose its client")
	}
	if l.Account().normalizedRegion() != "cn" {
		t.Errorf("region = %q", l.Account().normalizedRegion())
	}
	tok, err := l.Token(context.Background())
	if err != nil || tok != "tok" {
		t.Fatalf("token = %q, err = %v", tok, err)
	}
	sess, err := l.CreateSession(context.Background())
	if err != nil {
		// No /chat_session/create handler in this fixture: expect error, fine.
		_ = sess
	} else if sess == "" {
		t.Error("session id empty")
	}
	// NoteError with non-ban error is a no-op.
	l.NoteError(nil)
	l.NoteError(errors.New("plain"))
	st := pool.Status()
	if len(st) != 1 || st[0]["state"] != "ready" {
		t.Errorf("status = %v", st)
	}
	l.Release()
	// Double release must not panic.
	l.Release()
}

func TestPoolStatusMutedAndBanned(t *testing.T) {
	pool := newTestPool(t, 2, 5, nil)
	l1, _ := pool.Acquire(context.Background())
	l1.NoteError(&BizError{BizCode: 5, BizMsg: "muted"})
	l1.Release()
	l2, _ := pool.Acquire(context.Background())
	if l2.Account().Mobile != "13800000001" {
		t.Fatalf("second acquire = %s (muted acct skipped)", l2.Account().Mobile)
	}
	l2.NoteError(&BizError{BizCode: 10, BizMsg: "banned"})
	l2.Release()
	st := pool.Status()
	states := map[string]string{}
	for _, row := range st {
		states[row["mobile"].(string)] = row["state"].(string)
	}
	if states["13800000000"] != "muted" {
		t.Errorf("acct 0 state = %q, want muted", states["13800000000"])
	}
	if states["13800000001"] != "banned" {
		t.Errorf("acct 1 state = %q, want banned", states["13800000001"])
	}
}

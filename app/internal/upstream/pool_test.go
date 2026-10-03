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

func TestPoolRandomSelectionIsReproducibleAndDistributed(t *testing.T) {
	pool := newTestPool(t, 3, 5, func(c PoolConfig) PoolConfig {
		c.RandomSeed = 42
		return c
	})
	got := acquireSeq(t, pool, 900)
	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	for i := 0; i < 3; i++ {
		if counts[fmt.Sprintf("1380000000%d", i)] < 250 {
			t.Fatalf("account %d selected only %d/900 times: %v", i, counts[fmt.Sprintf("1380000000%d", i)], counts)
		}
	}
}

func TestPoolRandomSelectionSkipsParkedAndBusyAccounts(t *testing.T) {
	pool := newTestPool(t, 3, 1, func(c PoolConfig) PoolConfig { c.RandomSeed = 7; return c })
	pool.accounts[0].slots <- struct{}{}
	pool.accounts[1].am.mu.Lock()
	pool.accounts[1].am.ban = BanBanned
	pool.accounts[1].am.mu.Unlock()
	defer func() { <-pool.accounts[0].slots }()
	for i := 0; i < 50; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile != "13800000002" {
			t.Fatalf("selected unavailable account %s", lease.Account().Mobile)
		}
		lease.Release()
	}
}

func TestPoolRandomSelectionHotAddRemoveConcurrent(t *testing.T) {
	pool := newTestPool(t, 2, 2, func(c PoolConfig) PoolConfig { c.RandomSeed = 9; c.QueueWait = time.Second; return c })
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				lease, err := pool.Acquire(context.Background())
				if err == nil {
					lease.Release()
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("138000009%d", i)
		if err := pool.AddAccount(Account{Mobile: id, Password: "pw"}); err != nil {
			t.Fatal(err)
		}
		if !pool.RemoveAccount(id) {
			t.Fatalf("failed to remove %s", id)
		}
	}
	wg.Wait()
}

func TestPoolSelectionPrefersAvailableCapacity(t *testing.T) {
	pool := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig {
		c.RandomSeed = 7
		return c
	})
	busy, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Release()

	counts := map[string]int{}
	for i := 0; i < 12; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		counts[lease.Account().Mobile]++
		lease.Release()
	}
	if counts[busy.Account().Mobile] != 0 {
		t.Fatalf("busy account selected: %v", counts)
	}
}

func TestPoolAcquireReleaseIsOneRequestLifecycle(t *testing.T) {
	pool := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig { c.RandomSeed = 11; return c })
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	selected := lease.Account().Mobile
	if got := pool.Snapshot()[0].Inflight + pool.Snapshot()[1].Inflight; got != 1 {
		t.Fatalf("inflight after acquire = %d", got)
	}
	lease.Release()
	for _, row := range pool.Snapshot() {
		if row.Inflight != 0 {
			t.Fatalf("inflight after release = %d for %s", row.Inflight, row.Account.Mobile)
		}
	}
	lease2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease2.Account().Mobile == "" || selected == "" {
		t.Fatal("lease account identity missing")
	}
	lease2.Release()
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
	l1, _ := pool.Acquire(context.Background()) // one slot held
	first := l1.Account().Mobile
	l2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l2.Account().Mobile == first {
		t.Fatalf("second acquire reused full account %s", first)
	}
	l1.Release()
	l2.Release()
	l3, _ := pool.Acquire(context.Background())
	if l3.Account().Mobile == "" {
		t.Fatal("third acquire returned empty account")
	}
	l3.Release()
}

func TestPoolBannedSkippedForever(t *testing.T) {
	pool := newTestPool(t, 2, 5, nil)
	l, _ := pool.Acquire(context.Background())
	banned := l.Account().Mobile
	l.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	l.Release()
	for i := 0; i < 6; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile == banned {
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
func TestPoolDuplicateEntriesSharePhysicalIdentity(t *testing.T) {
	pool, err := NewPool([]Account{
		{Mobile: "13800000000", Password: "pw"},
		{Mobile: "13800000000", Password: "pw"},
	}, PoolConfig{MaxInflight: 1, QueueWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(pool.Snapshot()); n != 2 {
		t.Fatalf("pool has %d rows, want 2", n)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if other, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
		if other != nil {
			other.Release()
		}
		t.Fatalf("duplicate must share capacity, got %v", err)
	}
	lease.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	for _, row := range pool.Snapshot() {
		if row.State != "banned" {
			t.Fatalf("duplicate state = %s, want banned", row.State)
		}
	}
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
	if l2.Account().Mobile == l1.Account().Mobile {
		t.Fatalf("second acquire selected muted account %s", l2.Account().Mobile)
	}
	l2.NoteError(&BizError{BizCode: 10, BizMsg: "banned"})
	l2.Release()
	st := pool.Status()
	states := map[string]string{}
	for _, row := range st {
		states[row["mobile"].(string)] = row["state"].(string)
	}
	if states[l1.Account().Mobile] != "muted" {
		t.Errorf("acct %s state = %q, want muted", l1.Account().Mobile, states[l1.Account().Mobile])
	}
	if states[l2.Account().Mobile] != "banned" {
		t.Errorf("acct %s state = %q, want banned", l2.Account().Mobile, states[l2.Account().Mobile])
	}
}

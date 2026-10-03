package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireWithWaitExcludingSelectsOtherNearBestAccounts(t *testing.T) {
	pool := newTestPool(t, 4, 2, func(c PoolConfig) PoolConfig { c.RandomSeed = 42; return c })
	// Account 3 has much worse observed latency and must not enter the near-best draw.
	pool.Observe(&Lease{pa: pool.accounts[3]}, 10*time.Second)
	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		lease, _, err := pool.AcquireWithWaitExcluding(context.Background(), pool.accounts[0].account.Identity())
		if err != nil {
			t.Fatal(err)
		}
		counts[lease.Account().Identity()]++
		lease.Release()
	}
	if counts[pool.accounts[0].account.Identity()] != 0 || counts[pool.accounts[3].account.Identity()] != 0 {
		t.Fatalf("excluded or slow account selected: %v", counts)
	}
	if counts[pool.accounts[1].account.Identity()] == 0 || counts[pool.accounts[2].account.Identity()] == 0 {
		t.Fatalf("near-best candidates not both selected: %v", counts)
	}
}

func TestAcquireWithWaitExcludingNoAlternativeIsTerminal(t *testing.T) {
	pool := newTestPool(t, 1, 2, nil)
	started := time.Now()
	lease, _, err := pool.AcquireWithWaitExcluding(context.Background(), pool.accounts[0].account.Identity())
	if lease != nil || !errors.Is(err, ErrNoAlternativeAccount) {
		t.Fatalf("lease=%v err=%v, want terminal no alternative", lease, err)
	}
	if time.Since(started) >= pool.cfg.QueueWait {
		t.Fatal("no alternative should not wait for its own excluded account")
	}
	// Ordinary acquisition remains unaffected.
	lease, _, err = pool.AcquireWithWait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
}

func TestAcquireWithWaitExcludingDuplicateIdentityAndParked(t *testing.T) {
	pool := newTestPool(t, 3, 1, nil)
	pool.accounts[1].account = pool.accounts[0].account // same physical identity in another slot
	pool.accounts[2].am.mu.Lock()
	pool.accounts[2].am.ban = BanMuted
	pool.accounts[2].am.parkUntil = time.Now().Add(time.Hour)
	pool.accounts[2].am.mu.Unlock()
	lease, _, err := pool.AcquireWithWaitExcluding(context.Background(), pool.accounts[0].account.Identity())
	if lease != nil || !errors.Is(err, ErrNoAlternativeAccount) {
		t.Fatalf("lease=%v err=%v, want no eligible distinct identity", lease, err)
	}
}

func TestAcquireWithWaitExcludingWaitsOnlyForOtherBusyAccount(t *testing.T) {
	pool := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig { c.QueueWait = time.Second; return c })
	other := pool.accounts[1]
	other.slots <- struct{}{}
	go func() {
		time.Sleep(30 * time.Millisecond)
		<-other.slots
	}()
	lease, _, err := pool.AcquireWithWaitExcluding(context.Background(), pool.accounts[0].account.Identity())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Account().Identity() != other.account.Identity() {
		t.Fatalf("selected %s instead of %s", lease.Account().Identity(), other.account.Identity())
	}
	lease.Release()
}

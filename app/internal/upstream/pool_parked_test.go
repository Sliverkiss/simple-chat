package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestParkedPoolWithoutKnownDeadlineDoesNotClaimRecovery(t *testing.T) {
	p := newTestPool(t, 1, 1, func(c PoolConfig) PoolConfig { c.QueueWait = time.Second; return c })
	pa := p.accounts[0]
	pa.am.mu.Lock()
	pa.am.ban = BanRiskDevice // malformed in-memory state, no recovery date
	pa.am.mu.Unlock()
	start := time.Now()
	lease, _, err := p.AcquireWithWait(context.Background())
	if lease != nil || !errors.Is(err, ErrNoEligibleAccount) || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("lease=%v err=%v elapsed=%v", lease, err, time.Since(start))
	}
}

func TestParkedPoolReturnsToServiceAtDeadline(t *testing.T) {
	p := newTestPool(t, 1, 1, func(c PoolConfig) PoolConfig { c.QueueWait = time.Second; return c })
	pa := p.accounts[0]
	until := time.Now().Add(35 * time.Millisecond)
	pa.am.mu.Lock()
	pa.am.ban = BanRiskDevice
	pa.am.parkUntil = until
	pa.am.mu.Unlock()
	start := time.Now()
	lease, _, err := p.AcquireWithWait(context.Background())
	var parked *PoolParkedError
	if lease != nil || !errors.As(err, &parked) || !parked.Until.Equal(until) || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("lease=%v err=%v elapsed=%v", lease, err, time.Since(start))
	}
	time.Sleep(time.Until(until) + 5*time.Millisecond)
	lease, _, err = p.AcquireWithWait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
}

func TestParkDeadlineExpiresDuringSelectionRechecksEligibility(t *testing.T) {
	p := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig { c.QueueWait = time.Second; return c })
	first, second := p.accounts[0], p.accounts[1]
	first.am.mu.Lock()
	first.am.ban = BanRiskDevice
	first.am.parkUntil = time.Now().Add(30 * time.Millisecond)
	first.am.mu.Unlock()
	second.am.mu.Lock()
	second.am.ban = BanBanned
	result := make(chan error, 1)
	go func() {
		lease, _, err := p.AcquireWithWait(context.Background())
		if lease != nil {
			lease.Release()
		}
		result <- err
	}()
	time.Sleep(80 * time.Millisecond) // first expires while scanning locked second
	second.am.mu.Unlock()
	if err := <-result; err != nil {
		t.Fatalf("deadline passed before classification; must select recovered account: %v", err)
	}
}

func TestUnknownDeadlineAlongsideKnownDoesNotInventEarliest(t *testing.T) {
	p := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig { c.QueueWait = time.Second; return c })
	p.accounts[0].am.mu.Lock()
	p.accounts[0].am.ban = BanRiskDevice
	p.accounts[0].am.parkUntil = time.Now().Add(time.Minute)
	p.accounts[0].am.mu.Unlock()
	p.accounts[1].am.mu.Lock()
	p.accounts[1].am.ban = BanRiskDevice
	p.accounts[1].am.mu.Unlock()
	lease, _, err := p.AcquireWithWait(context.Background())
	if lease != nil || !errors.Is(err, ErrNoEligibleAccount) {
		t.Fatalf("mixed known/unknown recovery: lease=%v err=%v", lease, err)
	}
}

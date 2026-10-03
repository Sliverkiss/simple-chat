package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Duplicate records are one physical account, not additional request capacity.
func TestPoolDuplicateIdentitySharesInflightCapacity(t *testing.T) {
	const identity = "13800000000"
	pool, err := NewPool([]Account{
		{Mobile: identity, Password: "pw"},
		{Mobile: identity, Password: "pw"},
	}, PoolConfig{MaxInflight: 1, QueueWait: time.Millisecond, RandomSeed: 1})
	if err != nil {
		t.Fatal(err)
	}

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Release()
	second, err := pool.Acquire(context.Background())
	if second != nil {
		defer second.Release()
		t.Errorf("second concurrent lease for physical identity %s must not be granted", identity)
	}
	if !errors.Is(err, ErrPoolBusy) {
		t.Errorf("second acquire with identity at capacity: want ErrPoolBusy, got %v", err)
	}
}

// Parking either duplicate must park the identity, while a distinct account remains usable.
func TestPoolDuplicateIdentitySharesParkState(t *testing.T) {
	const a = "13800000000"
	const b = "13800000001"
	cases := []struct {
		name    string
		failure *BizError
		state   string
	}{
		{name: "ban", failure: &BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"}, state: "banned"},
		{name: "mute", failure: &BizError{BizCode: 5, BizMsg: "user is muted"}, state: "muted"},
		{name: "risk", failure: &BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}, state: "risk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewPool([]Account{
				{Mobile: a, Password: "pw"},
				{Mobile: a, Password: "pw"},
				{Mobile: b, Password: "pw"},
			}, PoolConfig{
				MaxInflight: 1, QueueWait: time.Millisecond, RandomSeed: 1,
				MuteParkDefault: time.Hour, RiskCooldown: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			// Exclude B to select A without depending on random account ordering.
			lease, _, err := pool.AcquireWithWaitExcluding(context.Background(), b)
			if err != nil {
				t.Fatalf("acquire A: %v", err)
			}
			if got := lease.Account().Identity(); got != a {
				lease.Release()
				t.Fatalf("acquire identity = %q, want %q", got, a)
			}
			lease.NoteError(tc.failure)
			lease.Release()

			rows := pool.Snapshot()
			var seenA, seenB int
			for _, row := range rows {
				switch row.Account.Identity() {
				case a:
					seenA++
					if row.State != tc.state {
						t.Errorf("duplicate A state = %q, want %q", row.State, tc.state)
					}
				case b:
					seenB++
					if row.State != "ready" {
						t.Errorf("distinct B state = %q, want ready", row.State)
					}
				}
			}
			if seenA != 2 || seenB != 1 {
				t.Fatalf("snapshot identity counts A=%d B=%d, want A=2 B=1", seenA, seenB)
			}

			// No duplicate A slot is eligible even if B is explicitly excluded.
			again, _, err := pool.AcquireWithWaitExcluding(context.Background(), b)
			if again != nil {
				again.Release()
				t.Errorf("parked identity A was selected through another slot")
			}
			if !errors.Is(err, ErrNoAlternativeAccount) {
				t.Errorf("acquire excluding B after A parked: want ErrNoAlternativeAccount, got %v", err)
			}
			other, err := pool.Acquire(context.Background())
			if err != nil {
				t.Fatalf("distinct B should remain available: %v", err)
			}
			defer other.Release()
			if got := other.Account().Identity(); got != b {
				t.Errorf("selected %q after A parked, want distinct B %q", got, b)
			}
		})
	}
}

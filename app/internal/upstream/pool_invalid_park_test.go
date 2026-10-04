package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPersistedUnknownParkOverridesTimedDuplicate(t *testing.T) {
	for _, order := range []string{"unknown-first", "known-first"} {
		t.Run(order, func(t *testing.T) {
			known := Account{Mobile: "100", Password: "pw", ParkKind: "risk", ParkUntil: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}
			unknown := Account{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: "broken"}
			rows := []Account{known, unknown}
			if order == "unknown-first" {
				rows = []Account{unknown, known}
			}
			p, err := NewPool(rows, PoolConfig{})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := p.Acquire(context.Background())
			if lease != nil || !errors.Is(err, ErrNoEligibleAccount) {
				t.Fatalf("lease=%v err=%v", lease, err)
			}
			for _, s := range p.Snapshot() {
				if s.ParkKind != "muted" || !s.ParkUntil.IsZero() {
					t.Fatalf("duplicate lost unknown quarantine: %+v", s)
				}
			}
		})
	}
}

func TestUnknownParkIsNotReplacedByNewFallback(t *testing.T) {
	a := Account{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: "invalid"}
	p, err := NewPool([]Account{a}, PoolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	am := p.accounts[0].am
	am.mu.Lock()
	am.markBan(&BizError{BizCode: 5})
	if am.ban != BanMuted || !am.parkUntil.IsZero() {
		t.Fatalf("unknown park overwritten with fallback: kind=%v until=%v", am.ban, am.parkUntil)
	}
	am.mu.Unlock()
	if err := p.AddAccount(Account{Mobile: "100", Password: "pw", ParkKind: "risk", ParkUntil: ""}); err != nil {
		t.Fatal(err)
	}
	if lease, err := p.Acquire(context.Background()); lease != nil || !errors.Is(err, ErrNoEligibleAccount) {
		t.Fatalf("hot add escaped unknown park: lease=%v err=%v", lease, err)
	}
}

func TestLateErrorCannotInventDeadlineForUnknownPark(t *testing.T) {
	p, err := NewPool([]Account{{Mobile: "100", Password: "pw"}}, PoolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	am := p.accounts[0].am
	am.mu.Lock()
	am.ban, am.parkUntil = BanMuted, time.Time{} // persisted quarantine arrives while lease is in flight
	am.mu.Unlock()
	lease.NoteError(&BizError{BizCode: 5})
	lease.NoteError(&BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"})
	if got := p.Snapshot()[0]; got.ParkKind != "muted" || !got.ParkUntil.IsZero() {
		t.Fatalf("late error invented recovery: %+v", got)
	}
	if next, err := p.Acquire(context.Background()); next != nil || !errors.Is(err, ErrNoEligibleAccount) {
		t.Fatalf("late error admitted new lease: %v %v", next, err)
	}
}

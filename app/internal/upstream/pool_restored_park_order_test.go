package upstream

import (
	"testing"
	"time"
)

// Duplicate persisted rows describe one physical identity. The later timed
// window must win even when its park kind differs from the first row's kind:
// otherwise the first window can expire and clear the still-active park.
func TestPoolRestoredDuplicateParkUsesLaterDeadlineRegardlessOfOrder(t *testing.T) {
	const identity = "13800000000"
	shortUntil := time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)
	longUntil := time.Date(2099, time.February, 1, 0, 0, 0, 0, time.UTC)
	short := Account{Mobile: identity, Password: "pw", ParkKind: "risk", ParkUntil: shortUntil.Format(time.RFC3339), ParkReason: "short risk"}
	long := Account{Mobile: identity, Password: "pw", ParkKind: "muted", ParkUntil: longUntil.Format(time.RFC3339), ParkReason: "long mute"}

	for _, tc := range []struct {
		name string
		rows []Account
	}{
		{name: "short risk then long mute", rows: []Account{short, long}},
		{name: "long mute then short risk", rows: []Account{long, short}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPool(tc.rows, PoolConfig{RandomSeed: 1})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := p.Snapshot()
			if len(snapshot) != len(tc.rows) {
				t.Fatalf("snapshot rows = %d, want %d", len(snapshot), len(tc.rows))
			}
			for i, row := range snapshot {
				if row.State != "muted" || row.ParkKind != "muted" || !row.ParkUntil.Equal(longUntil) || row.ParkReason != "long mute" {
					t.Errorf("row %d effective park = (%s, %s, %s, %q), want muted through %s: %+v", i, row.State, row.ParkKind, row.ParkUntil, row.ParkReason, longUntil, row)
				}
			}
		})
	}
}

func TestPoolRestoredDuplicatePermanentBanDominatesTimedParkRegardlessOfOrder(t *testing.T) {
	const identity = "13800000000"
	until := time.Date(2099, time.February, 1, 0, 0, 0, 0, time.UTC)
	timed := Account{Mobile: identity, Password: "pw", ParkKind: "muted", ParkUntil: until.Format(time.RFC3339), ParkReason: "timed mute"}
	banned := Account{Mobile: identity, Password: "pw", ParkKind: "banned", ParkReason: "permanent ban"}

	for _, tc := range []struct {
		name string
		rows []Account
	}{
		{name: "timed then banned", rows: []Account{timed, banned}},
		{name: "banned then timed", rows: []Account{banned, timed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPool(tc.rows, PoolConfig{RandomSeed: 1})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := p.Snapshot()
			if len(snapshot) != len(tc.rows) {
				t.Fatalf("snapshot rows = %d, want %d", len(snapshot), len(tc.rows))
			}
			for i, row := range snapshot {
				if row.State != "banned" || row.ParkKind != "banned" || !row.ParkUntil.IsZero() || row.ParkReason != "permanent ban" {
					t.Errorf("row %d effective park = (%s, %s, %s, %q), want permanent ban: %+v", i, row.State, row.ParkKind, row.ParkUntil, row.ParkReason, row)
				}
			}
		})
	}
}

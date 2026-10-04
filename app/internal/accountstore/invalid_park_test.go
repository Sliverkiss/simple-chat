package accountstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// Invalid or absent deadlines have no provable recovery time. Loading must
// neither clear the backing row nor admit a new lease for that identity.
func TestInvalidParkDeadlineLoadPreservesQuarantine(t *testing.T) {
	for _, kind := range []string{"muted", "risk"} {
		for _, until := range []string{"", "not-a-date"} {
			for _, backend := range []string{"json", "redis"} {
				t.Run(backend+"/"+kind+"/"+until, func(t *testing.T) {
					acct := upstream.Account{Mobile: "100", Password: "pw", DeviceID: "dev-100", ParkKind: kind, ParkUntil: until, ParkReason: "upstream rejected", ParkedAt: "2026-01-01T00:00:00Z"}
					var store Store
					var backing func() upstream.Account
					if backend == "json" {
						s, path, _ := storeTestEnv(t, []upstream.Account{acct})
						store = s
						backing = func() upstream.Account { return readTestFile(t, path)[0] }
					} else {
						s, _, _ := redisTestEnv(t, []upstream.Account{acct})
						store = s
						backing = func() upstream.Account {
							raw, err := s.conn.do("GET", redisKeyPrefix+acct.Identity())
							if err != nil {
								t.Fatal(err)
							}
							var saved upstream.Account
							if err := json.Unmarshal([]byte(raw.(string)), &saved); err != nil {
								t.Fatal(err)
							}
							return saved
						}
					}
					original := backing()
					for i := 0; i < 2; i++ { // simulate another process boot from the same backing
						rows, err := EnsureDeviceIDs(context.Background(), store)
						if err != nil {
							t.Fatal(err)
						}
						if len(rows) != 1 || rows[0].ParkKind != kind || rows[0].ParkUntil != until {
							t.Fatalf("boot %d loaded %+v", i, rows)
						}
						if got := backing(); got != original {
							t.Fatalf("boot %d rewrote parked row: %+v", i, got)
						}
						p, err := upstream.NewPool(rows, upstream.PoolConfig{QueueWait: time.Millisecond})
						if err != nil {
							t.Fatal(err)
						}
						lease, err := p.Acquire(context.Background())
						if lease != nil || err != upstream.ErrNoEligibleAccount {
							t.Fatalf("boot %d lease=%v err=%v", i, lease, err)
						}
						snapshot := p.Snapshot()
						if len(snapshot) != 1 || snapshot[0].ParkKind != kind || !snapshot[0].ParkUntil.IsZero() {
							t.Fatalf("boot %d snapshot=%+v", i, snapshot)
						}
					}
				})
			}
		}
	}
}

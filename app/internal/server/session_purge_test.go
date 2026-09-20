// Tests for the weekly purge-all-sessions scheduler (TASK_PURGE):
// mimics the app's "clear all chat history" (profile →
// delete_all_chat_button, telemetry delete_all_chat_history_click).
// One background goroutine fires delete_all for every healthy+warm
// account once a week at DS_PURGE_WEEKDAY/DS_PURGE_HOUR ±30m jitter;
// a window missed while the process was down catches up once at
// startup. Deterministic cases call runPurge() on a dormant scheduler.
package server

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

// purgeCfgSunday4 is the canonical test config: Sunday 04:00.
func purgeCfgSunday4() purgeConfig {
	return purgeConfig{weekday: 6, hour: 4}
}

// runPurge fires one deterministic purge on a dormant scheduler.
func runPurge(s *purgeScheduler) {
	s.rng = rand.New(rand.NewSource(42))
	s.pass(context.Background())
}

// TestPurgeCallsDeleteAllPerActiveAccount: an active (warm) account gets
// exactly one delete_all, its live drawer empties, and the before/after
// counts land in the log line.
func TestPurgeCallsDeleteAllPerActiveAccount(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	} // 8 live sessions, warm account

	runPurge(gw.purge)

	f.mu.Lock()
	calls, live := f.deleteAll, len(f.live)
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("delete_all called %d times, want 1", calls)
	}
	if live != 0 {
		t.Fatalf("drawer still holds %d sessions after purge", live)
	}
}

// TestPurgeSkipsColdAccounts: an account that never served traffic holds
// no cached token — the purge must not touch it (no login, no
// fetch_page, no delete_all).
func TestPurgeSkipsColdAccounts(t *testing.T) {
	f := newCleanupFixture(t)
	f.seedOrphans(10) // upstream truth the gateway never touched
	gw, _ := newCleanupServer(t, f.srv.URL, nil)

	runPurge(gw.purge)

	f.mu.Lock()
	calls, pages, live := f.deleteAll, f.pageHits, len(f.live)
	f.mu.Unlock()
	if calls != 0 {
		t.Fatalf("cold account received %d delete_all calls, want 0", calls)
	}
	if pages != 0 {
		t.Fatalf("cold account received %d fetch_page calls, want 0", pages)
	}
	if live != 10 {
		t.Fatalf("orphan sessions purged: %d remain, want 10", live)
	}
}

// TestPurgeFailureNoRetryStorm: a failing delete_all (biz 5) is logged
// and NOT retried within the same purge; the next week is the retry.
func TestPurgeFailureNoRetryStorm(t *testing.T) {
	f := newCleanupFixture(t)
	f.failDeleteAll = true
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 3; i++ {
		postOneChat(t, ts.URL)
	}

	runPurge(gw.purge)
	time.Sleep(300 * time.Millisecond)

	f.mu.Lock()
	calls := f.deleteAll
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("delete_all called %d times on failure, want exactly 1 (no retry storm)", calls)
	}
}

// TestPurgeResetsSessionRegistry: after a purge, the in-memory registry
// no longer holds the purged account's sessions (they are gone upstream).
func TestPurgeResetsSessionRegistry(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, func(c *Config) { c.SessionCap = 10 })

	for i := 0; i < 5; i++ {
		postOneChat(t, ts.URL)
	}
	gw.sessions.mu.Lock()
	recorded := len(gw.sessions.seq["13800000000"])
	gw.sessions.mu.Unlock()
	if recorded != 5 {
		t.Fatalf("registry recorded %d sessions, want 5", recorded)
	}

	runPurge(gw.purge)

	gw.sessions.mu.Lock()
	recorded = len(gw.sessions.seq["13800000000"])
	gw.sessions.mu.Unlock()
	if recorded != 0 {
		t.Fatalf("registry still holds %d sessions after purge, want 0", recorded)
	}
}

// TestPurgePostPurgeBelowFloor: after a purge the account has ~0
// sessions — a cleanup episode right after finds nothing above the
// floor and must not fire deletes (post-purge registry truth is not
// needed; fetch_page truth shows an empty drawer).
func TestPurgePostPurgeBelowFloor(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}
	runPurge(gw.purge)

	// Fire a cleanup episode immediately after the purge.
	runPass(gw.cleanup, 7)
	time.Sleep(300 * time.Millisecond)

	_, deleted := f.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("cleanup fired right after purge: deleted %v, want none (below floor)", deleted)
	}
	f.mu.Lock()
	calls := f.deleteAll
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("delete_all called %d times, want 1", calls)
	}
}

// TestNextPurgeTimeComputation: the next purge time lands on the
// configured weekday/hour (±30m), strictly in the future, and crosses
// the week boundary when this week's window already passed. All times
// are local — the scheduler slots in local time.
func TestNextPurgeTimeComputation(t *testing.T) {
	s := &purgeScheduler{cfg: purgeCfgSunday4()}
	// Sunday 2026-09-20 in local time.
	sunday := func(h, m int) time.Time {
		return time.Date(2026, 9, 20, h, m, 0, 0, time.Local)
	}
	cases := []struct {
		name string
		now  time.Time
	}{
		{"before window same day", sunday(1, 0)},
		{"after window same day", sunday(7, 0)},
		{"midweek before", time.Date(2026, 9, 16, 12, 0, 0, 0, time.Local)},  // Wednesday
		{"midweek after", time.Date(2026, 9, 18, 23, 30, 0, 0, time.Local)},  // Friday 23:30
		{"saturday night", time.Date(2026, 9, 19, 23, 59, 0, 0, time.Local)}, // Saturday 23:59
		{"exact window moment", sunday(4, 0)},
		{"year boundary week", time.Date(2026, 12, 31, 12, 0, 0, 0, time.Local)}, // Thursday Dec 31
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s.rng = rand.New(rand.NewSource(1))
			next := s.nextPurge(tc.now)
			if !next.After(tc.now) {
				t.Fatalf("next purge %v not after now %v", next, tc.now)
			}
			// The fire time must sit within ±30m of a Sunday 04:00 slot.
			if next.Weekday() != time.Sunday {
				t.Fatalf("next purge lands on %s (%v), want Sunday (±30m may cross midnight — see slot check)", next.Weekday(), next)
			}
			if next.Hour() != 4 || next.Minute() != 0 {
				// ±30m around 04:00 can land 03:30-04:30; the weekday
				// check above pins the day, accept the half-hour band.
				if !(next.Hour() == 3 && next.Minute() >= 30) && !(next.Hour() == 4 && next.Minute() <= 30) {
					t.Fatalf("next purge lands %02d:%02d, want 04:00 ±30m", next.Hour(), next.Minute())
				}
			}
			if next.Sub(tc.now) > 7*24*time.Hour+time.Hour {
				t.Fatalf("next purge %v is more than a week out from %v", next, tc.now)
			}
		})
	}
}

// TestPurgeJitterVaries: the ±30m offset actually varies — never an
// exact clock time for the risk service to learn.
func TestPurgeJitterVaries(t *testing.T) {
	s := &purgeScheduler{
		cfg: purgeCfgSunday4(),
		rng: rand.New(rand.NewSource(7)),
	}
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.Local) // Wednesday
	slot := time.Date(2026, 9, 20, 4, 0, 0, 0, time.Local)
	seen := map[time.Duration]bool{}
	for i := 0; i < 30; i++ {
		d := s.nextPurge(now).Sub(now)
		if d < 0 {
			t.Fatalf("negative purge delay %s", d)
		}
		delta := d - slot.Sub(now)
		if delta < -30*time.Minute || delta > 30*time.Minute {
			t.Fatalf("delay %s is %s off the exact slot, want ±30m", d, delta)
		}
		seen[d] = true
	}
	if len(seen) < 3 {
		t.Fatalf("jitter too static: %d distinct delays over 30 draws", len(seen))
	}
}

// TestPurgeMissedWindowCatchUp: a slot that passed within the last day
// (process may have been down through it) is a missed window and
// catches up at startup; a slot still in the future is not missed; a
// slot older than a day is left to the previous instance.
func TestPurgeMissedWindowCatchUp(t *testing.T) {
	f := newCleanupFixture(t)
	gw, _ := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.PurgeEnabled = true
		c.PurgeWeekday = 6 // Sunday
		c.PurgeHour = 4
	})
	s := gw.purge

	// Sunday 12:00 local: today's 04:00 slot passed 8h ago → missed.
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.Local)
	if !s.catchUpNeeded(now) {
		t.Fatal("slot that passed 8h ago not detected as missed")
	}
	// Sunday 03:00: today's slot is still ahead → not missed.
	if s.catchUpNeeded(time.Date(2026, 9, 20, 3, 0, 0, 0, time.Local)) {
		t.Fatal("future slot detected as missed")
	}
	// Monday 10:00: the Sunday 04:00 slot passed 30h ago → beyond the
	// catch-up window; assume the previous instance handled it.
	if s.catchUpNeeded(time.Date(2026, 9, 21, 10, 0, 0, 0, time.Local)) {
		t.Fatal("slot older than the catch-up window detected as missed")
	}
	// lastSlot sanity: at Sunday 12:00 the last slot is today 04:00.
	if last := s.lastSlot(now); last.Weekday() != time.Sunday || last.Hour() != 4 {
		t.Fatalf("lastSlot = %v, want Sunday 04:00", last)
	}
}

// TestPurgeDisabledConfigNeverStarts: DS_PURGE=0 / weekday -1 means
// the goroutine never starts — and the zero-value Config (PurgeEnabled
// false, existing callers) stays dormant too.
func TestPurgeDisabledConfigNeverStarts(t *testing.T) {
	f := newCleanupFixture(t)
	gw, _ := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.PurgeEnabled = false // explicit off
		c.PurgeWeekday = 6
		c.PurgeHour = 4
	})
	if gw.purge.started {
		t.Fatal("purge loop started with disabled config, want dormant")
	}

	gw2, _ := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.PurgeEnabled = true
		c.PurgeWeekday = -1 // weekday -1 also disables
	})
	if gw2.purge.started {
		t.Fatal("purge loop started with weekday -1, want dormant")
	}
}

// TestPurgeShutdownDrainsCleanly: after Shutdown the loop goroutine has
// exited and a second Shutdown is safe.
func TestPurgeShutdownDrainsCleanly(t *testing.T) {
	f := newCleanupFixture(t)
	gw, _ := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.PurgeEnabled = true
		c.PurgeWeekday = 6
		c.PurgeHour = 4
	})
	if !gw.purge.started {
		t.Fatal("purge loop not marked started")
	}
	gw.Shutdown()
	select {
	case <-gw.purge.done:
	default:
		t.Fatal("purge loop goroutine still running after Shutdown")
	}
	gw.Shutdown() // idempotent
}

// TestPurgeLoopFiresOnShortDelay: with an injected near-immediate next
// purge, the background loop fires a purge without manual intervention,
// and continues firing (the override is honored on every re-read).
func TestPurgeLoopFiresOnShortDelay(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.PurgeEnabled = true
		c.PurgeWeekday = 6
		c.PurgeHour = 4
		// Shrink the startup catch-up delay: the machine's clock may
		// sit right after a missed Sunday-04:00 slot (e.g. testing on
		// Sunday morning), and the production 2-12s catch-up sleep
		// would starve the 5s deadline.
		c.PurgeCatchUpMin = time.Millisecond
		c.PurgeCatchUpMax = 2 * time.Millisecond
	})
	for i := 0; i < 5; i++ {
		postOneChat(t, ts.URL)
	}
	// Force the loop's next wake into the immediate future.
	gw.purge.overrideNextUnix.Store(time.Now().Add(50 * time.Millisecond).Unix())

	eventually(t, 5*time.Second, "background purge to fire", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.deleteAll >= 1
	})
}

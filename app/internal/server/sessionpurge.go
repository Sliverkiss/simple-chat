// Weekly purge-all-sessions (TASK_PURGE). Design in one paragraph:
// mimics the app's "clear all chat history" feature (profile →
// delete_all_chat_button, telemetry delete_all_chat_history_click —
// r02.java case 1 wires the bodyless POST /api/v0/chat_session/
// delete_all). One background goroutine per gateway computes the next
// weekly window (DS_PURGE_WEEKDAY/DS_PURGE_HOUR, default Sunday 04:00)
// and sleeps until it, with a ±30min random offset so the fire time is
// never an exact clock the risk service can learn — the same
// anti-cadence discipline as the human-paced cleanup. A window missed
// while the process was down (the most recent slot passed within the
// last day at startup) catches up once shortly after start, with its
// own jitter, never instantly; then the weekly schedule resumes. Each
// purge walks every healthy+warm account (parked/muted/cold accounts
// are skipped and just miss that week — a purge must never cold-login
// an account), fires delete_all, verifies the envelope, logs the
// before/after session counts (fetch_page truth), and resets the
// in-memory session registry for the purged account. On failure: log,
// no retry storm — next week is the retry. Post-purge the account
// holds ~0 sessions, below the cleanup floor, so the human-paced
// episode right after naturally finds nothing to delete.
package server

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"simple-chat/internal/upstream"
)

// Weekly purge defaults.
const (
	// DefaultPurgeWeekday is the purge day when DS_PURGE_WEEKDAY is
	// unset: Sunday, in the config's Monday-based numbering
	// (0=Monday .. 6=Sunday).
	DefaultPurgeWeekday = 6
	// DefaultPurgeHour is the purge hour when DS_PURGE_HOUR is unset:
	// 04:00 local — the quiet hours, when a mass delete looks most
	// natural.
	DefaultPurgeHour = 4
	// DefaultPurgeJitter is the ± bound on the fire time: the purge
	// lands anywhere in [slot-30m, slot+30m], never on the exact slot.
	DefaultPurgeJitter = 30 * time.Minute
	// purgeCatchUpWindow: a slot that passed within this window before
	// process start is treated as missed (the process may have been
	// down through it) and catches up once. Older slots are assumed to
	// have been handled by a previous instance — restarting must not
	// turn a weekly purge into a daily one.
	purgeCatchUpWindow = 24 * time.Hour
	// purgeCatchUpDelay bounds the startup catch-up: it fires after a
	// jittered [purgeCatchUpDelayMin, purgeCatchUpDelayMax] — never
	// instantly on boot, which would be its own telltale cadence.
	purgeCatchUpDelayMin = 2 * time.Second
	purgeCatchUpDelayMax = 12 * time.Second
)

// purgeConfig sizes the weekly purge. weekday is Monday-based
// (0=Monday .. 6=Sunday); weekday < 0 disables the scheduler entirely
// (DS_PURGE=0 or DS_PURGE_WEEKDAY=-1).
type purgeConfig struct {
	weekday int // 0=Monday .. 6=Sunday, <0 = disabled
	hour    int // 0-23
	jitter  time.Duration
	// catchUpMin/Max bound the startup catch-up delay; zero values
	// take the production constants (test seam).
	catchUpMin time.Duration
	catchUpMax time.Duration
}

// goWeekday maps the config's Monday-based weekday onto Go's
// Sunday-based time.Weekday numbering.
func (c purgeConfig) goWeekday() time.Weekday {
	if c.weekday < 0 {
		return time.Sunday
	}
	return time.Weekday((c.weekday + 1) % 7)
}

// jitterBound returns the active jitter (tests construct configs
// without running fillDefaults).
func (c purgeConfig) jitterBound() time.Duration {
	if c.jitter <= 0 {
		return DefaultPurgeJitter
	}
	return c.jitter
}

func (c *purgeConfig) fillDefaults() {
	if c.jitter <= 0 {
		c.jitter = DefaultPurgeJitter
	}
}

// purgeScheduler is the weekly delete_all sweeper.
type purgeScheduler struct {
	cfg    purgeConfig
	logger *log.Logger

	// accounts yields the purge-eligible accounts (healthy + cached
	// token) — the same seam the human-paced cleanup uses. Wired to the
	// pool in NewServer.
	accounts func() []upstream.AccountRef

	// sessions is the in-memory session registry: a successful purge
	// resets the purged account's recorded sessions (they are gone
	// upstream; keeping them would poison DS_SESSION_CAP eviction).
	sessions *sessionRegistry

	// rng feeds the jitter. Owned by whichever goroutine runs the
	// loop / pass — never both.
	rng *rand.Rand

	// overrideNextUnix, when non-zero, replaces the computed next fire
	// time (test seam for driving the background loop). Read freshly
	// on every wait slice so a late override takes effect; atomic
	// because the test writes it while the loop goroutine runs.
	overrideNextUnix atomic.Int64

	// started records that the loop goroutine was launched (enabled).
	started bool
	// done closes when the loop goroutine exits (drain proof).
	done chan struct{}
	// stop cancels the loop; closeOnce makes shutdown idempotent.
	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// newPurgeScheduler builds the scheduler; the loop only starts on
// start() when the config is enabled (weekday >= 0).
func newPurgeScheduler(cfg purgeConfig, pool *upstream.Pool, sessions *sessionRegistry, logger *log.Logger) *purgeScheduler {
	cfg.fillDefaults()
	s := &purgeScheduler{
		cfg:      cfg,
		logger:   logger,
		sessions: sessions,
		done:     make(chan struct{}),
		stop:     make(chan struct{}),
	}
	if pool != nil {
		s.accounts = pool.ActiveAccountRefs
	}
	return s
}

// weekdayName renders the purge weekday for the startup log line.
func (s *purgeScheduler) weekdayName() string {
	if s.cfg.weekday < 0 || s.cfg.weekday > 6 {
		return "off"
	}
	return s.cfg.goWeekday().String()
}

// logPolicy prints the one startup line describing the active policy.
func (s *purgeScheduler) logPolicy() {
	if s.cfg.weekday < 0 {
		s.logger.Printf("weekly purge: disabled (DS_PURGE=0)")
		return
	}
	s.logger.Printf("weekly purge: %s %02d:00 ±%dm (delete_all)",
		s.weekdayName(), s.cfg.hour, int(s.cfg.jitterBound().Minutes()))
}

// start launches the background loop when enabled; otherwise the
// scheduler stays dormant (pass() remains directly callable for tests).
func (s *purgeScheduler) start() {
	if s.cfg.weekday < 0 {
		return
	}
	if s.rng == nil {
		s.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	s.started = true
	s.wg.Add(1)
	go s.loop()
}

// loop is the background cycle: compute the next weekly window (with
// jitter), sleep until it in short slices (so a late target override
// lands), fire one purge, repeat. A window missed while the process
// was down catches up once after a jittered delay — never instantly.
func (s *purgeScheduler) loop() {
	defer s.wg.Done()
	defer close(s.done)
	if s.catchUpNeeded(time.Now()) {
		select {
		case <-s.stop:
			return
		case <-time.After(s.nextCatchUpDelay()):
		}
		s.pass(context.Background())
	}
	for {
		for target := s.nextWake(time.Now()); time.Now().Before(target); target = s.nextWake(time.Now()) {
			select {
			case <-s.stop:
				return
			case <-time.After(purgeWaitSlice):
			}
		}
		s.pass(context.Background())
	}
}

// purgeWaitSlice bounds one sleep slice: the loop re-reads its wake
// target between slices, so a changed target (test override) is picked
// up without busy-waiting. 200ms is invisible next to a weekly cadence.
const purgeWaitSlice = 200 * time.Millisecond

// nextWake returns the next fire time: the test override when set,
// else the next jittered weekly slot from now.
func (s *purgeScheduler) nextWake(now time.Time) time.Time {
	if v := s.overrideNextUnix.Load(); v != 0 {
		return time.Unix(v, 0)
	}
	return s.nextPurge(now)
}

// catchUpNeeded reports whether the most recent weekly slot passed
// within the catch-up window before now — i.e. the process may have
// been down through a window it never fired.
func (s *purgeScheduler) catchUpNeeded(now time.Time) bool {
	if s.cfg.weekday < 0 {
		return false
	}
	last := s.lastSlot(now)
	return last.Before(now) && now.Sub(last) < purgeCatchUpWindow
}

// lastSlot computes the most recent (at or before now) un-jittered
// weekly slot, in local time.
func (s *purgeScheduler) lastSlot(now time.Time) time.Time {
	now = now.Local()
	today := time.Date(now.Year(), now.Month(), now.Day(), s.cfg.hour, 0, 0, 0, now.Location())
	// Shift today back to the most recent configured weekday.
	delta := (int(today.Weekday()) - int(s.cfg.goWeekday()) + 7) % 7
	slot := today.AddDate(0, 0, -delta)
	if slot.After(now) {
		slot = slot.AddDate(0, 0, -7)
	}
	return slot
}

// nextPurge computes the next fire time: the next weekly slot with a
// ±jitter offset drawn uniformly. The result is always strictly after
// now (a draw at/below now re-rolls off the following week's slot).
func (s *purgeScheduler) nextPurge(now time.Time) time.Time {
	slot := s.nextSlot(now)
	fire := slot.Add(s.nextJitter())
	for !fire.After(now) {
		slot = slot.AddDate(0, 0, 7)
		fire = slot.Add(s.nextJitter())
	}
	return fire
}

// nextSlot computes the next un-jittered weekly slot strictly after
// now: the coming configured weekday at the configured hour, local time.
func (s *purgeScheduler) nextSlot(now time.Time) time.Time {
	now = now.Local()
	today := time.Date(now.Year(), now.Month(), now.Day(), s.cfg.hour, 0, 0, 0, now.Location())
	// Days from today to the next occurrence of the configured weekday.
	delta := (int(s.cfg.goWeekday()) - int(today.Weekday()) + 7) % 7
	if today.After(now) && delta == 0 {
		return today // today's slot is still ahead
	}
	if delta == 0 {
		delta = 7 // today's slot passed; next week's
	}
	return today.AddDate(0, 0, delta)
}

// nextJitter draws one uniform offset in [-jitter, +jitter].
func (s *purgeScheduler) nextJitter() time.Duration {
	j := s.cfg.jitterBound()
	if j <= 0 {
		return 0
	}
	return time.Duration(s.rng.Int63n(int64(j)*2+1)) - j
}

// nextCatchUpDelay draws the startup catch-up delay uniformly from
// [catchUpMin, catchUpMax] — positive-only, so boot never fires the
// purge instantly.
func (s *purgeScheduler) nextCatchUpDelay() time.Duration {
	lo, hi := s.cfg.catchUpMin, s.cfg.catchUpMax
	if lo <= 0 {
		lo = purgeCatchUpDelayMin
	}
	if hi <= lo {
		hi = lo + 10*time.Second
		if lo == purgeCatchUpDelayMin {
			hi = purgeCatchUpDelayMax
		}
	}
	span := hi - lo
	return lo + time.Duration(s.rng.Int63n(int64(span)+1))
}

// pass performs one purge over every eligible account. An account is
// eligible when it is healthy and holds a cached token — a purge must
// never log a cold account in (parked accounts stay cold and just miss
// that week's purge).
func (s *purgeScheduler) pass(ctx context.Context) {
	if s.accounts == nil {
		return
	}
	for _, acct := range s.accounts() {
		if ctx.Err() != nil {
			return
		}
		s.purgeAccount(ctx, acct)
	}
}

// purgeAccount fires delete_all on one account, verifies the envelope,
// logs the before/after counts (fetch_page truth), and resets the
// in-memory registry. On failure: log and move on — next week is the
// retry, never a retry storm.
func (s *purgeScheduler) purgeAccount(ctx context.Context, acct upstream.AccountRef) {
	tok, err := acct.AM.Token(ctx)
	if err != nil {
		// Raced into a park between listing and now — skip this account.
		s.logger.Printf("purge: skipping %s (unavailable: %v)", acct.Mobile, err)
		return
	}
	// Before-count from upstream truth (best-effort; a failed list
	// doesn't block the purge — delete_all is the point).
	before := -1
	if sessions, err := acct.Client.ListSessions(ctx, tok); err == nil {
		before = len(sessions)
	}
	if err := acct.Client.DeleteAllSessions(ctx, tok); err != nil {
		s.logger.Printf("purge: %s delete_all failed (will retry next week): %v", acct.Mobile, err)
		return
	}
	after := -1
	if sessions, err := acct.Client.ListSessions(ctx, tok); err == nil {
		after = len(sessions)
	}
	// The account's recorded sessions are gone upstream — drop them so
	// DS_SESSION_CAP eviction and future episodes work off real truth.
	if s.sessions != nil {
		s.sessions.reset(acct.Mobile)
	}
	s.logger.Printf("purge: %s cleared (%d → %d sessions)", acct.Mobile, before, after)
}

// shutdown stops the loop goroutine. A purge interrupted mid-walk
// simply resumes next week — delete_all has no queue of its own.
func (s *purgeScheduler) shutdown() {
	s.closeOnce.Do(func() {
		close(s.stop)
	})
	s.wg.Wait()
}

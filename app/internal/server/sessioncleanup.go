// Human-paced session cleanup (TASK_CLEANUP). Design in one paragraph:
// pure accumulation is as non-human as burn-after-request — real users
// occasionally tidy old chats. One background goroutine per gateway wakes
// on a jittered interval (base ±50%, DS_CLEANUP_INTERVAL, 0 = disabled).
// Each wake, every account that is healthy AND already holds a cached
// token rolls an episode: with probability p it lists the account's REAL
// upstream sessions (fetch_page — the same drawer-open GET the app fires)
// and deletes a randomized 1–3 of the oldest non-pinned ones, but only
// when the upstream holds more than the floor (DS_CLEANUP_FLOOR, default
// 5). The deletes flow through the unchanged async deleter, spaced by
// jittered gaps — burst-then-idle, like a person long-pressing through
// old chats. Truth is the upstream drawer, never the in-memory registry:
// pre-existing orphans (restart leftovers) heal on the next episode, and
// DS_SESSION_CAP remains a synchronous emergency valve on create.
package server

import (
	"context"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"simple-chat/internal/upstream"
)

// Human-paced cleanup defaults.
const (
	// DefaultCleanupInterval is the base wake interval; each sleep is drawn
	// uniformly from [base/2, base*3/2] — never a fixed cadence the risk
	// service can learn. Resolved in main (DS_CLEANUP_INTERVAL).
	DefaultCleanupInterval = time.Hour
	// DefaultCleanupFloor is how many sessions stay untouched at/below it:
	// a tidy person keeps their recent chats, only trimming a backlog.
	DefaultCleanupFloor = 5
	// DefaultCleanupProbability is the per-account chance that a wake
	// becomes an episode: cleanup is the exception, not the rule.
	DefaultCleanupProbability = 0.5
	// Delete batch bounds: an episode deletes between MinCleanupBatch and
	// MaxCleanupBatch sessions — a handful in one sitting, never a purge.
	MinCleanupBatch = 1
	MaxCleanupBatch = 3
	// In-episode delete gap bounds: two long-press deletes are seconds
	// apart in human hands, never one instant.
	DefaultCleanupGapMin = time.Second
	DefaultCleanupGapMax = 6 * time.Second
	// cleanupResuppress bounds how long a freshly enqueued delete is
	// remembered: while a delete is still draining through the async
	// deleter, a following episode must not re-enqueue the same session
	// (the drawer still lists it). Long enough to cover the drain; short
	// enough that a genuinely failed delete is retried by a later episode.
	cleanupResuppress = 15 * time.Minute
)

// cleanupConfig sizes the human-paced policy. interval <= 0 disables the
// scheduler; zero values of the rest take defaults.
type cleanupConfig struct {
	interval    time.Duration
	floor       int
	probability float64
	gapMin      time.Duration
	gapMax      time.Duration
}

func (c *cleanupConfig) fillDefaults() {
	if c.floor <= 0 {
		c.floor = DefaultCleanupFloor
	}
	if c.probability <= 0 {
		c.probability = DefaultCleanupProbability
	}
	if c.gapMin <= 0 {
		c.gapMin = DefaultCleanupGapMin
	}
	if c.gapMax <= c.gapMin {
		c.gapMax = DefaultCleanupGapMin + 5*time.Second
	}
}

// cleanupScheduler is the human-paced session cleaner.
type cleanupScheduler struct {
	cfg    cleanupConfig
	logger *log.Logger

	// accounts yields the cleanup-eligible accounts (healthy + cached
	// token). Wired to the pool in NewServer; a seam for in-package tests.
	accounts func() []upstream.AccountRef

	deleter *asyncDeleter

	// rng feeds all randomness (interval jitter, episode rolls, batch
	// size, gap). Owned by whichever goroutine runs pass() — the loop, or
	// a test on a dormant scheduler; never both.
	rng *rand.Rand

	// pending remembers recently enqueued deletes ("mobile|sessionID" →
	// enqueued-at) so back-to-back episodes don't double-delete a session
	// that is still draining through the async deleter.
	pendingMu sync.Mutex
	pending   map[string]time.Time

	// started records that the loop goroutine was launched (interval > 0).
	started bool
	// done closes when the loop goroutine exits (drain proof).
	done chan struct{}
	// stop cancels the loop; closeOnce makes shutdown idempotent.
	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// newCleanupScheduler builds the scheduler; the loop only starts on
// start() when interval > 0.
func newCleanupScheduler(cfg cleanupConfig, pool *upstream.Pool, deleter *asyncDeleter, logger *log.Logger) *cleanupScheduler {
	cfg.fillDefaults()
	s := &cleanupScheduler{
		cfg:     cfg,
		logger:  logger,
		deleter: deleter,
		pending: map[string]time.Time{},
		done:    make(chan struct{}),
		stop:    make(chan struct{}),
	}
	if pool != nil {
		s.accounts = pool.ActiveAccountRefs
	}
	return s
}

// logPolicy prints the one startup line describing the active policy.
func (s *cleanupScheduler) logPolicy() {
	if s.cfg.interval <= 0 {
		s.logger.Printf("session cleanup: disabled (DS_CLEANUP_INTERVAL=0)")
		return
	}
	s.logger.Printf("session cleanup: human-paced (interval %s ±50%%, batch %d-%d, floor %d)",
		s.cfg.interval, MinCleanupBatch, MaxCleanupBatch, s.cfg.floor)
}

// start launches the background loop when interval > 0; otherwise the
// scheduler stays dormant (pass() remains directly callable for tests).
func (s *cleanupScheduler) start() {
	if s.cfg.interval <= 0 {
		return
	}
	if s.rng == nil {
		s.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	s.started = true
	s.wg.Add(1)
	go s.loop()
}

// loop is the background wake cycle: sleep a jittered interval, fire one
// pass, repeat until stopped.
func (s *cleanupScheduler) loop() {
	defer s.wg.Done()
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(s.nextInterval()):
		}
		s.pass(context.Background())
	}
}

// pass performs one cleanup wake over every eligible account. An account
// is eligible when it is healthy and holds a cached token — cleanup must
// never log a cold account in (parked accounts stay cold: zero upstream
// traffic renews nothing).
func (s *cleanupScheduler) pass(ctx context.Context) {
	if s.accounts == nil {
		return
	}
	for _, acct := range s.accounts() {
		if ctx.Err() != nil {
			return
		}
		if s.rng.Float64() >= s.cfg.probability {
			continue
		}
		s.episode(ctx, acct)
	}
}

// episode performs one tidy sitting on one account: list the REAL upstream
// sessions, and if the unpinned backlog exceeds the floor, delete a random
// bounded batch of the oldest through the async deleter with human gaps
// between enqueues.
func (s *cleanupScheduler) episode(ctx context.Context, acct upstream.AccountRef) {
	tok, err := acct.AM.Token(ctx)
	if err != nil {
		// Raced into a park between listing and now — skip this account.
		s.logger.Printf("cleanup: skipping %s (unavailable: %v)", acct.Mobile, err)
		return
	}
	sessions, err := acct.Client.ListSessions(ctx, tok)
	if err != nil {
		s.logger.Printf("cleanup: %s session list failed (skipped): %v", acct.Mobile, err)
		return
	}
	// Humans don't tidy pinned chats.
	unpinned := make([]upstream.SessionInfo, 0, len(sessions))
	for _, sess := range sessions {
		if !sess.Pinned {
			unpinned = append(unpinned, sess)
		}
	}
	if len(unpinned) <= s.cfg.floor {
		return
	}
	// The drawer is newest-first; the deletable backlog is the oldest
	// (len - floor) entries: everything past the floor-newest. Re-sort
	// ascending by updated_at so drawer order can't betray us.
	candidates := unpinned[s.cfg.floor:]
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].UpdatedAt < candidates[j].UpdatedAt
	})
	// Skip anything still draining from a recent episode.
	now := time.Now()
	victims := make([]upstream.SessionInfo, 0, len(candidates))
	s.pendingMu.Lock()
	for k, t := range s.pending {
		if now.After(t.Add(cleanupResuppress)) {
			delete(s.pending, k)
		}
	}
	for _, sess := range candidates {
		if _, draining := s.pending[acct.Mobile+"|"+sess.ID]; !draining {
			victims = append(victims, sess)
		}
	}
	s.pendingMu.Unlock()
	if len(victims) == 0 {
		return
	}
	n := MinCleanupBatch + s.rng.Intn(MaxCleanupBatch-MinCleanupBatch+1)
	if n > len(victims) {
		n = len(victims)
	}
	for i, sess := range victims[:n] {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-time.After(s.nextGap()):
			}
		}
		s.pendingMu.Lock()
		s.pending[acct.Mobile+"|"+sess.ID] = now
		s.pendingMu.Unlock()
		s.deleter.enqueue(deleteJob{client: acct.Client, token: tok, sessionID: sess.ID})
	}
	s.logger.Printf("cleanup episode: %s deleted %d, remaining %d", acct.Mobile, n, len(unpinned)-n)
}

// nextInterval draws the next wake delay uniformly from
// [interval/2, interval*3/2] — ±50% around the base, no fixed cadence.
func (s *cleanupScheduler) nextInterval() time.Duration {
	half := s.cfg.interval / 2
	return half + time.Duration(s.rng.Int63n(int64(half)*2+1))
}

// nextGap draws one in-episode delete gap uniformly from [gapMin, gapMax].
func (s *cleanupScheduler) nextGap() time.Duration {
	span := s.cfg.gapMax - s.cfg.gapMin
	if span <= 0 {
		return s.cfg.gapMin
	}
	return s.cfg.gapMin + time.Duration(s.rng.Int63n(int64(span)+1))
}

// shutdown stops the loop goroutine. Already-queued deletes drain through
// the async deleter's own shutdown (server.Shutdown calls that after this).
func (s *cleanupScheduler) shutdown() {
	s.closeOnce.Do(func() {
		close(s.stop)
	})
	s.wg.Wait()
}

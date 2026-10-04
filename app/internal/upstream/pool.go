// Multi-account pool: per-physical-identity in-flight caps, scored selection,
// and shared health states (banned / muted / risk-device) across duplicate rows.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Default in-flight cap per account when MaxInflight is unset.
const DefaultMaxInflight = 2

// ErrNoAccounts means every account is banned (permanent) — no wait would help.
var ErrNoAccounts = errors.New("upstream: pool has no available accounts (all banned)")

// ErrNoAlternativeAccount means a retry cannot select a distinct ready
// account. The caller should terminate this retry, never reuse the prior one.
var ErrNoAlternativeAccount = errors.New("upstream: no eligible alternative account")

// PoolBusyError reports all accounts busy; the pool waited QueueWait for a
// slot and gave up. Retry-After is in seconds.
type PoolBusyError struct {
	RetryAfter time.Duration
}

func (e *PoolBusyError) Error() string {
	return fmt.Sprintf("upstream: all accounts busy (waited %s; retry after %s)", e.RetryAfter, e.RetryAfter)
}

// Is makes errors.Is(err, ErrPoolBusy) match any *PoolBusyError value.
func (e *PoolBusyError) Is(target error) bool {
	_, ok := target.(*PoolBusyError)
	return ok
}

// ErrPoolBusy is the sentinel matching PoolBusyError values.
var ErrPoolBusy = &PoolBusyError{}

// PoolParkedError means no identity is ready and the nearest known local
// unpark deadline is Until. It is not a capacity/QueueWait failure.
type PoolParkedError struct{ Until time.Time }

func (e *PoolParkedError) Error() string { return "upstream: all eligible accounts parked" }

// ErrNoEligibleAccount indicates a parked pool without any known recovery
// deadline. Do not advertise a fabricated Retry-After for this case.
var ErrNoEligibleAccount = errors.New("upstream: no eligible account with known recovery")

// ErrParkPersistence indicates this process can no longer safely assign new
// leases because a park transition was not confirmed by the backing store.
var ErrParkPersistence = errors.New("pool: park persistence failed; account admission closed")

// PoolConfig configures the pool.
type PoolConfig struct {
	// ParkWriteFailed closes admission after a failed durable park write.
	// Nil preserves the behavior of memory-only pools.
	ParkWriteFailed func() bool
	// BaseURL overrides every region's base URL (tests inject httptest).
	BaseURL string
	// MaxInflight caps concurrent requests per account (default 2).
	MaxInflight int
	// QueueWait bounds how long Acquire waits for a slot when every account
	// is busy (default 30s).
	QueueWait time.Duration
	// MuteParkDefault parks a muted account when the upstream sends no
	// mute_until (default 7 days; missing/invalid/past timestamps must not
	// cause an early retry). Explicit overrides are useful for local tests.
	MuteParkDefault time.Duration
	// RiskCooldown parks a risk-device account (default 10min).
	RiskCooldown time.Duration
	// OnParkPersist receives every park-state transition (TASK_MUTE): a park
	// (ban/mute/risk) or the natural unpark (Kind=BanNone). The sink is
	// invoked synchronously under the account's lock, after the in-memory
	// state is updated. Production memory-first stores latch failed writes
	// and reject subsequent Acquire calls via ParkWriteFailed.
	OnParkPersist func(ParkRecord)
	// OnLoginPersist receives every successful login/relogin's fresh bearer
	// token (docs-spec-memory-first.md): the write-through moment for the
	// persisted SessionToken field. Same posture as the park sink —
	// synchronous, fail-soft, nil = memory-only tokens.
	OnLoginPersist func(LoginRecord)
	// PersistenceGeneration snapshots the store's identity generation when a
	// runtime account is built. Nil keeps legacy, unfenced callback records.
	PersistenceGeneration func(identity string) uint64
	Logger                func(format string, args ...any)
	// RandomSeed makes selection reproducible in tests. Zero seeds from the
	// current clock; all access to the PRNG is serialized by p.mu.
	RandomSeed int64
	// EWMAAlpha controls latency adaptation. Zero uses 0.2.
	EWMAAlpha float64
	// ParallelLimitCooldown briefly suppresses an identity after an explicit
	// per-account parallel generation limit (default 1s).
	ParallelLimitCooldown time.Duration
}

// LoginRecord is one successful login, handed to PoolConfig.OnLoginPersist.
type LoginRecord struct {
	Identity   string
	Token      string
	Generation uint64
}

// ParkRecord is one park-state transition, handed to PoolConfig.OnParkPersist.
// A zero Until with Kind=BanBanned means "forever" (manual revive only).
type ParkRecord struct {
	Mobile     string
	Generation uint64
	// Kind is the new state: BanBanned/BanMuted/BanRiskDevice on a park,
	// BanNone on a natural unpark (fields below are zero then).
	Kind   BanState
	Until  time.Time
	Reason string
}

// parkKindNames maps BanState to the persisted park_kind string and back.
// Banned/muted/risk are the only persisted kinds; BanNone renders empty and
// clears the persisted fields.
var (
	parkKindNames = map[BanState]string{
		BanBanned:     "banned",
		BanMuted:      "muted",
		BanRiskDevice: "risk",
	}
	parkKindsByString = func() map[string]BanState {
		m := make(map[string]BanState, len(parkKindNames))
		for k, v := range parkKindNames {
			m[v] = k
		}
		return m
	}()
)

// ParkKindName renders a BanState as the persisted park_kind value.
func ParkKindName(b BanState) string { return parkKindNames[b] }

// ParseParkKind parses a persisted park_kind value; unknown/empty = BanNone.
func ParseParkKind(s string) BanState { return parkKindsByString[s] }

// parsePersistedPark reads an account's park_* fields into (kind, until).
// A missing or unparseable until remains a park with unknown recovery. The
// store must not erase it, and admission must not invent a new deadline.
func parsePersistedPark(a Account) (BanState, time.Time) {
	kind := ParseParkKind(a.ParkKind)
	if kind == BanNone {
		return BanNone, time.Time{}
	}
	until, _ := time.Parse(time.RFC3339, a.ParkUntil)
	return kind, until
}

func (c *PoolConfig) fillDefaults() {
	if c.MaxInflight <= 0 {
		c.MaxInflight = DefaultMaxInflight
	}
	if c.QueueWait <= 0 {
		c.QueueWait = 30 * time.Second
	}
	if c.MuteParkDefault <= 0 {
		c.MuteParkDefault = 7 * 24 * time.Hour
	}
	if c.RiskCooldown <= 0 {
		c.RiskCooldown = 10 * time.Minute
	}
	if c.ParallelLimitCooldown <= 0 {
		c.ParallelLimitCooldown = time.Second
	}
}

// poolAccount is the runtime state of one account in the ring.
type poolAccount struct {
	account Account
	client  *Client
	am      *AccountManager
	// active is shared by duplicate rows and invalidated when this generation
	// leaves the ring. Login callbacks run under am.mu, not p.mu.
	active     *atomic.Bool
	generation uint64

	// Duplicate rows for one physical identity share this semaphore, client,
	// and manager. Rows remain separate for admin display and ring weighting.
	slots chan struct{}
	// ewmaNanos is updated under Pool.mu after a completed request. Zero means
	// no latency sample yet.
	ewmaNanos float64
	samples   uint64
	// Written only under p.mu; tracks the first NoteError persistence after
	// AccountManager.markBan sets a mute deadline without persisting it.
	parkReported time.Time
}

// health states for an account.
type health int

const (
	healthReady health = iota
	healthBanned
	healthMuted
	healthRisk
	healthCooling
)

// Pool is the account ring.
type Pool struct {
	accounts  []*poolAccount
	cfg       PoolConfig
	transport http.RoundTripper // shared tuned Transport (R5)

	mu  sync.Mutex
	rng *rand.Rand
	// nextSeq counts successful acquisitions for introspection.
	nextSeq int
}

// NewPool validates accounts and builds the ring. An empty ring is valid
// (docs-spec-memory-first.md): a fresh cloud Upstash boots with zero accounts
// and is seeded via the admin API; Acquire on the empty ring answers
// ErrNoAccounts.
func NewPool(accounts []Account, cfg PoolConfig) (*Pool, error) {
	cfg.fillDefaults()
	// One Transport per pool, sized to the pool: DefaultTransport's
	// MaxIdleConnsPerHost=2 forces TCP+TLS re-handshakes under our own
	// concurrency (accounts × in-flight all hit the same host).
	perHost := cfg.MaxInflight * len(accounts)
	if perHost < minIdleConnsPerHost {
		perHost = minIdleConnsPerHost
	}
	transport := newTransport(nil)
	transport.MaxIdleConnsPerHost = perHost
	seed := cfg.RandomSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	p := &Pool{cfg: cfg, transport: transport, rng: rand.New(rand.NewSource(seed))}
	parked := 0
	for _, a := range accounts {
		pa, err := p.buildAccount(a)
		if err != nil {
			return nil, err
		}
		if err := p.joinIdentity(pa); err != nil {
			return nil, err
		}
		if pa.am.ban != BanNone {
			parked++
		}
		p.accounts = append(p.accounts, pa)
	}
	if parked > 0 {
		p.logf("pool: %d parked at startup", parked)
	}
	return p, nil
}

// buildAccount validates one account and assembles its runtime state
// (client, manager, in-flight semaphore, persisted park restore). It is the
// single construction path: startup loading and hot-added accounts (admin
// API) get byte-identical treatment. Parked accounts come back with the
// manager's ban state already set; the park was persisted by whoever wrote
// the record, so no sink notification happens here.
func (p *Pool) buildAccount(a Account) (*poolAccount, error) {
	if err := a.Validate(); err != nil {
		return nil, fmt.Errorf("upstream: account %s: %w", a.Identity(), err)
	}
	// The caller joins duplicate rows to the existing physical identity
	// before making this slot visible to acquisition.
	client := NewClient(Config{
		BaseURL:  p.cfg.BaseURL,
		Mobile:   a.Mobile,
		Email:    a.Email,
		Password: a.Password,
		// Per-account persistent device identity (device-id-research.md):
		// explicit value verbatim, else deterministic UUIDv5. Never one
		// shared id across accounts — risk-service correlation vector.
		DeviceID: ResolveDeviceID(a),
		// Channel: "" (android default) or "web" (browser-harvested
		// Shumei id; login body os:"web"). Validate() has already
		// rejected web without an explicit device_id.
		Channel: a.Channel,
	})
	client.SetTransport(p.transport)
	client.SetLogger(p.cfg.Logger)
	client.AccountManager().muteParkDefault = p.cfg.MuteParkDefault
	client.AccountManager().riskCooldown = p.cfg.RiskCooldown
	active := &atomic.Bool{}
	active.Store(true)
	var generation uint64
	if p.cfg.PersistenceGeneration != nil {
		generation = p.cfg.PersistenceGeneration(a.Identity())
	}
	// Login write-through (docs-spec-memory-first.md): every successful
	// login/relogin hands the fresh token to the sink under the manager's
	// lock. Nil hook = memory-only tokens (current behavior).
	if p.cfg.OnLoginPersist != nil {
		hook := p.cfg.OnLoginPersist
		client.AccountManager().onLogin = func(tok string) {
			if active.Load() {
				hook(LoginRecord{Identity: a.Identity(), Token: tok, Generation: generation})
			}
		}
	}
	pa := &poolAccount{
		account:    a,
		client:     client,
		am:         client.AccountManager(),
		active:     active,
		generation: generation,
		slots:      make(chan struct{}, p.cfg.MaxInflight),
	}
	// Restore persisted park state (TASK_MUTE): an account muted/banned
	// before a restart must not re-enter rotation "clean" — re-hitting
	// the upstream renews the window (observed 6h mute → 3-day ban
	// escalation). Expired parks are ignored (ready on load); banned
	// persists forever.
	kind, until := parsePersistedPark(a)
	if kind == BanBanned {
		// A ban is permanent even if a stale persisted row has an until.
		pa.am.mu.Lock()
		pa.am.ban = BanBanned
		pa.am.banMsg = a.ParkReason
		pa.am.mu.Unlock()
		p.logf("pool: account still BANNED (restored from disk; manual revive = delete park fields from accounts.json)")
	} else if kind != BanNone && (until.IsZero() || time.Now().Before(until)) {
		pa.am.mu.Lock()
		pa.am.ban = kind
		pa.am.parkUntil = until
		pa.am.banMsg = a.ParkReason
		pa.am.mu.Unlock()
		if until.IsZero() {
			p.logf("pool: account still %s with unknown recovery (restored from disk)", banName(kind))
		} else {
			p.logf("pool: account still %s until %s (restored from disk)", banName(kind), until.Format(time.RFC3339))
		}
	}
	return pa, nil
}

// joinIdentity shares one runtime state per physical identity. Called only
// under p.mu (or during construction before the pool is published). A parked
// duplicate from persistence must not resurrect a ready copy: a permanent ban
// wins, then an unknown recovery deadline, then the later timed window.
// Equal deadlines prefer mute over risk, then lexicographically later reason.
func (p *Pool) joinIdentity(pa *poolAccount) error {
	for _, existing := range p.accounts {
		if existing.account.Identity() != pa.account.Identity() {
			continue
		}
		// A shared client must never silently select one row's login or
		// wire fingerprint. Do not include credential/device values in errors.
		if existing.account.Mobile != pa.account.Mobile ||
			existing.account.Email != pa.account.Email ||
			existing.account.Password != pa.account.Password ||
			existing.account.normalizedRegion() != pa.account.normalizedRegion() ||
			ResolveDeviceID(existing.account) != ResolveDeviceID(pa.account) ||
			existing.account.Channel != pa.account.Channel {
			return fmt.Errorf("upstream: conflicting account configuration for identity %s", pa.account.Identity())
		}
		pa.am.mu.Lock()
		kind, until, reason := pa.am.ban, pa.am.parkUntil, pa.am.banMsg
		pa.am.mu.Unlock()
		existing.am.mu.Lock()
		if kind == BanBanned && (existing.am.ban != BanBanned || reason > existing.am.banMsg) ||
			kind != BanNone && existing.am.ban == BanNone ||
			kind != BanNone && kind != BanBanned && existing.am.ban != BanBanned &&
				(until.IsZero() && !existing.am.parkUntil.IsZero() ||
					until.After(existing.am.parkUntil) && !existing.am.parkUntil.IsZero() ||
					until.Equal(existing.am.parkUntil) && (kind == BanMuted && existing.am.ban == BanRiskDevice ||
						kind == existing.am.ban && reason > existing.am.banMsg)) {
			existing.am.ban, existing.am.parkUntil, existing.am.banMsg = kind, until, reason
		}
		existing.am.mu.Unlock()
		pa.client, pa.am, pa.slots, pa.active, pa.generation = existing.client, existing.am, existing.slots, existing.active, existing.generation
		return nil
	}
	return nil
}

// AddAccount hot-adds one validated account to the ring (admin API): no
// restart, selectable by the very next Acquire. The account goes through
// buildAccount, so a hot-added account is byte-identical to a startup-loaded
// one — same device-id mint, same persisted-park restore, same one-shot
// startup sequence on first login. Ring appends are mutex-guarded; a
// concurrent Acquire either sees the old ring (this account waits one
// rotation) or the new one (it is selectable immediately).
func (p *Pool) AddAccount(a Account) error {
	pa, err := p.buildAccount(a)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.joinIdentity(pa); err != nil {
		return err
	}
	p.accounts = append(p.accounts, pa)
	// Grow the shared transport's idle pool so the new account's
	// concurrency does not force TCP+TLS re-handshakes.
	if tr, ok := p.transport.(*http.Transport); ok {
		if want := p.cfg.MaxInflight * len(p.accounts); want > tr.MaxIdleConnsPerHost {
			tr.MaxIdleConnsPerHost = want
		}
	}
	p.logf("pool: hot-added account (ring now %d)", len(p.accounts))
	return nil
}

// RemoveAccount removes every ring slot matching id (mobile, or email for
// email-only accounts) from selection, reporting whether anything was
// removed. In-flight leases on the removed account are NOT disturbed — a
// lease holds the *poolAccount reference, not a ring index — and their
// Release still works (the channel outlives the ring entry). Re-adding an
// identity after removal creates fresh runtime state; old leases cannot park
// or release capacity on the new entry. The upstream
// sessions and tokens of a removed account are left alone: removal only
// stops new acquisitions.
func (p *Pool) RemoveAccount(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.accounts[:0]
	removed := 0
	for _, pa := range p.accounts {
		if pa.account.MatchesIdentity(id) {
			pa.active.Store(false)
			removed++
			continue
		}
		kept = append(kept, pa)
	}
	if removed == 0 {
		return false
	}
	p.accounts = kept
	p.logf("pool: removed account (%d slot(s), ring now %d)", removed, len(p.accounts))
	return true
}

// logf logs through the pool's configured logger when set.
func (p *Pool) logf(format string, args ...any) {
	if p.cfg.Logger != nil {
		p.cfg.Logger(format, args...)
	}
}

// healthNow returns the account's effective health, clearing expired parks.
// Caller must hold p.mu.
func (p *Pool) healthNow(pa *poolAccount, now time.Time) health {
	pa.am.mu.Lock()
	defer pa.am.mu.Unlock()
	// Banned is permanent; nothing to clear.
	if pa.am.ban == BanBanned {
		return healthBanned
	}
	// Muted/risk parks are time-bounded by parkUntil.
	if !pa.am.parkUntil.IsZero() && !now.Before(pa.am.parkUntil) {
		pa.am.parkUntil = time.Time{}
		if pa.am.ban == BanMuted || pa.am.ban == BanRiskDevice {
			pa.am.ban = BanNone
			// Natural unpark is a persisted-state transition (TASK_MUTE):
			// clear the parked account's fields so the next restart does not
			// resurrect an expired park.
			p.notifyParkPersist(pa, ParkRecord{Mobile: pa.account.Identity(), Kind: BanNone})
		}
	}
	if pa.am.ban != BanNone {
		return health(pa.am.ban)
	}
	if now.Before(pa.am.cooldownUntil) {
		return healthCooling
	}
	return healthReady
}

// isBusy reports whether the account has free in-flight slots.
func (pa *poolAccount) isBusy() bool {
	return len(pa.slots) >= cap(pa.slots)
}

// Acquire scores ready identities with free capacity and reserves a slot.
// A random draw among near-best candidates is serialized under p.mu.
// If all candidates are busy, wait up to QueueWait; if none can recover,
// fail immediately.
func (p *Pool) Acquire(ctx context.Context) (*Lease, error) {
	lease, _, err := p.AcquireWithWait(ctx)
	return lease, err
}

// AcquireWithWait is Acquire plus the time spent waiting for a free slot.
// The duration excludes account login/session work after the lease is held.
func (p *Pool) AcquireWithWait(ctx context.Context) (*Lease, time.Duration, error) {
	return p.acquireWithWait(ctx, "")
}

// AcquireWithWaitExcluding reserves a lease for a different physical identity
// during one retry. priorIdentity is Account.Identity(), not a ring slot: all
// duplicate entries of that identity are excluded. A blank identity excludes
// nothing. If no distinct ready account exists, ErrNoAlternativeAccount is
// terminal; if one is ready but at capacity, QueueWait applies as usual.
// This never falls back to the prior account or bypasses health/park checks.
func (p *Pool) AcquireWithWaitExcluding(ctx context.Context, priorIdentity string) (*Lease, time.Duration, error) {
	return p.acquireWithWait(ctx, priorIdentity)
}

func (p *Pool) acquireWithWait(ctx context.Context, excludedIdentity string) (*Lease, time.Duration, error) {
	started := time.Now()
	deadline := time.Now().Add(p.cfg.QueueWait)
	for {
		lease, err := p.tryAcquireExcluding(ctx, excludedIdentity)
		if err == nil {
			return lease, time.Since(started), nil
		}
		var parked *PoolParkedError
		if errors.As(err, &parked) && !time.Now().Before(parked.Until) {
			// The earliest park may have expired while selection was blocked
			// on another identity lock. Re-scan before emitting a stale 429.
			continue
		}
		if !errors.Is(err, ErrPoolBusy) {
			return nil, time.Since(started), err
		}
		if time.Now().After(deadline) {
			return nil, time.Since(started), &PoolBusyError{RetryAfter: p.cfg.QueueWait.Round(time.Second) + time.Second}
		}
		select {
		case <-ctx.Done():
			return nil, time.Since(started), ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// tryAcquire does one non-blocking pass over the ring.
func (p *Pool) tryAcquire(ctx context.Context) (*Lease, error) {
	return p.tryAcquireExcluding(ctx, "")
}

func (p *Pool) tryAcquireExcluding(ctx context.Context, excludedIdentity string) (*Lease, error) {
	p.mu.Lock()
	if p.cfg.ParkWriteFailed != nil && p.cfg.ParkWriteFailed() {
		p.mu.Unlock()
		return nil, ErrParkPersistence
	}
	n := len(p.accounts)
	if n == 0 {
		// Empty ring (admin API removed every account): nothing can free a
		// slot, so waiting cannot help — fail fast with ErrNoAccounts.
		p.mu.Unlock()
		return nil, ErrNoAccounts
	}
	now := time.Now()
	var anyBanned bool
	var distinctReady bool
	var parked bool
	var earliest time.Time
	var unknownRecovery bool
	ready := make([]*poolAccount, 0, n)
	for i := 0; i < n; i++ {
		pa := p.accounts[i]
		if excludedIdentity != "" && pa.account.Identity() == excludedIdentity {
			continue
		}
		h := p.healthNow(pa, now)
		if h == healthBanned {
			anyBanned = true
			continue
		}
		if h != healthReady {
			// healthNow has reconciled expired parks. Snapshot the actual
			// eligibility deadline while holding both pool and manager locks.
			pa.am.mu.Lock()
			until := pa.am.parkUntil
			if h == healthCooling {
				until = pa.am.cooldownUntil
			}
			pa.am.mu.Unlock()
			parked = true
			if until.After(now) && (earliest.IsZero() || until.Before(earliest)) {
				earliest = until
			} else if !until.After(now) {
				unknownRecovery = true
			}
			continue
		}
		distinctReady = true
		if pa.isBusy() {
			continue
		}
		ready = append(ready, pa)
	}
	// healthNow may naturally clear an expired park and synchronously fail
	// its durable write during this scan. The entry check cannot cover that
	// transition; recheck the sticky store latch before reserving any slot.
	// ParkWriteFailed is an atomic read (no store mutex) under p.mu.
	if p.cfg.ParkWriteFailed != nil && p.cfg.ParkWriteFailed() {
		p.mu.Unlock()
		return nil, ErrParkPersistence
	}
	if len(ready) > 0 {
		best := ready[0]
		bestScore := p.accountScore(best)
		for _, pa := range ready[1:] {
			score := p.accountScore(pa)
			if score < bestScore {
				best, bestScore = pa, score
			}
		}
		near := make([]*poolAccount, 0, len(ready))
		for _, pa := range ready {
			if p.accountScore(pa) <= bestScore*1.15+1 {
				near = append(near, pa)
			}
		}
		pa := near[p.rng.Intn(len(near))]
		pa.slots <- struct{}{}
		p.nextSeq++
		p.mu.Unlock()
		return &Lease{pool: p, pa: pa}, nil
	}
	if excludedIdentity != "" && !distinctReady {
		p.mu.Unlock()
		return nil, ErrNoAlternativeAccount
	}
	// Check the terminal all-banned case while holding p.mu. AddAccount and
	// RemoveAccount mutate p.accounts, so traversing the slice after unlock
	// would race with hot administration and could observe a stale backing
	// array.
	allGone := anyBanned
	if allGone {
		for _, pa := range p.accounts {
			if p.healthNow(pa, now) != healthBanned {
				allGone = false
				break
			}
		}
	}
	// The terminal classification pass also calls healthNow; never return
	// a harmless-looking availability result after a failed clear write.
	if p.cfg.ParkWriteFailed != nil && p.cfg.ParkWriteFailed() {
		p.mu.Unlock()
		return nil, ErrParkPersistence
	}
	p.mu.Unlock()
	if allGone {
		return nil, ErrNoAccounts
	}
	if !distinctReady && parked {
		if earliest.IsZero() || unknownRecovery {
			return nil, ErrNoEligibleAccount
		}
		return nil, &PoolParkedError{Until: earliest}
	}
	return nil, ErrPoolBusy
}

// accountScore ranks ready accounts. Lower is better. In-flight dominates;
// measured latency breaks ties, while cold accounts receive a neutral prior.
func (p *Pool) accountScore(pa *poolAccount) float64 {
	inflight := float64(len(pa.slots))
	capacity := float64(cap(pa.slots))
	if capacity <= 0 {
		capacity = 1
	}
	load := inflight / capacity
	latency := 1.0
	if pa.ewmaNanos > 0 {
		latency += pa.ewmaNanos / float64(time.Second)
	}
	warmPenalty := 0.0
	pa.am.mu.Lock()
	if pa.am.token == "" {
		warmPenalty = 0.05
	}
	pa.am.mu.Unlock()
	return load*100 + latency + warmPenalty
}

// Observe records one completed request latency for future scheduling.
func (p *Pool) Observe(lease *Lease, elapsed time.Duration) {
	if lease == nil || lease.pa == nil || elapsed < 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	alpha := p.cfg.EWMAAlpha
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.2
	}
	sample := float64(elapsed)
	if lease.pa.samples == 0 {
		lease.pa.ewmaNanos = sample
	} else {
		lease.pa.ewmaNanos = alpha*sample + (1-alpha)*lease.pa.ewmaNanos
	}
	lease.pa.samples++
}

// Lease is one reserved in-flight slot on one account.
type Lease struct {
	pool *Pool
	pa   *poolAccount
}

// Account returns the account config for this lease.
func (l *Lease) Account() Account { return l.pa.account }

// ParkUntil returns the effective local deadline without exposing identity.
func (l *Lease) ParkUntil() time.Time {
	l.pa.am.mu.Lock()
	defer l.pa.am.mu.Unlock()
	return l.pa.am.parkUntil
}

// Client returns the upstream client bound to this account.
func (l *Lease) Client() *Client { return l.pa.client }

// Token returns the account's current bearer token (lazy login).
func (l *Lease) Token(ctx context.Context) (string, error) { return l.pa.am.Token(ctx) }

// CreateSession creates a chat session on this account (lazy relogin on auth
// failure, once — existing AccountManager behavior).
func (l *Lease) CreateSession(ctx context.Context) (string, error) {
	return l.pa.am.CreateSession(ctx)
}

// Completion runs the completion stream on this account with the account's
// token (PoW included, lazy relogin on auth failure — AccountManager).
func (l *Lease) Completion(ctx context.Context, req CompletionRequest) (io.ReadCloser, error) {
	return l.pa.am.Completion(ctx, req)
}

// NoteError feeds an upstream error back into pool health. Ban states are
// recorded per-account; relogin-worthy auth failures are ignored (the
// AccountManager already retried).
func (l *Lease) NoteError(err error) {
	if err == nil {
		return
	}
	if limited, ok := err.(interface{ IsParallelLimit() bool }); ok && limited.IsParallelLimit() {
		l.pool.mu.Lock()
		l.pa.am.mu.Lock()
		until := time.Now().Add(l.pool.cfg.ParallelLimitCooldown)
		if until.After(l.pa.am.cooldownUntil) {
			l.pa.am.cooldownUntil = until
		}
		l.pa.am.mu.Unlock()
		l.pool.mu.Unlock()
		return
	}
	if BanKind(err) == BanNone {
		return
	}
	be, _ := err.(*BizError)
	l.pool.mu.Lock()
	defer l.pool.mu.Unlock()
	l.pa.am.mu.Lock()
	defer l.pa.am.mu.Unlock()
	// The manager may have observed this very ban while executing the lease's
	// upstream call. Its in-memory transition still needs one durable write.
	if l.pa.am.ban == BanBanned && !l.pa.am.banParkPending {
		return
	}
	if l.pa.am.ban == BanBanned && BanKind(err) != BanBanned {
		return
	}
	// An existing unknown recovery time must not be replaced by a newly
	// computed fallback (or risk cooldown) from a late in-flight error.
	if (l.pa.am.ban == BanMuted || l.pa.am.ban == BanRiskDevice) && l.pa.am.parkUntil.IsZero() && BanKind(err) != BanBanned {
		return
	}
	switch BanKind(err) {
	case BanBanned:
		l.pa.am.ban = BanBanned
		l.pa.am.banParkPending = false
		l.pa.am.parkUntil = time.Time{}
		l.pa.am.banMsg = "account banned: " + err.Error()
	case BanMuted:
		until := time.Time{}
		if be != nil {
			until = be.MuteUntil
		}
		if be != nil && !be.localMutePark.IsZero() {
			until = be.localMutePark
		} else {
			// The manager may have parked this same biz5 before NoteError sees it.
			// A fallback is relative to observation time, so recomputing it here
			// would extend the deadline on every duplicate report.
			if l.pa.am.ban == BanMuted && (be == nil || !be.MuteUntil.After(time.Now())) && l.pa.am.parkUntil.After(time.Now()) {
				until = l.pa.am.parkUntil
			} else {
				until = localMuteDeadline(until, l.pool.cfg.MuteParkDefault)
			}
		}
		// Concurrent leases may report older/shorter mute windows out of
		// order. Never make a known mute eligible sooner.
		if l.pa.am.ban != BanNone && l.pa.am.parkUntil.After(until) {
			return
		}
		// Equal windows can still need their first durable write: markBan runs
		// before NoteError on the same error. Suppress only later duplicates.
		if l.pa.am.ban == BanMuted && l.pa.am.parkUntil.Equal(until) && l.pa.parkReported.Equal(until) {
			return
		}
		l.pa.am.ban = BanMuted
		l.pa.am.parkUntil = until
		l.pa.am.banMsg = "account muted: " + err.Error()
		l.pa.parkReported = until
	case BanRiskDevice:
		until := time.Now().Add(l.pool.cfg.RiskCooldown)
		if l.pa.am.ban != BanNone && l.pa.am.parkUntil.After(until) {
			return
		}
		l.pa.am.ban = BanRiskDevice
		l.pa.am.parkUntil = until
		l.pa.am.banMsg = "risk device detected: " + err.Error()
	}
	if l.pool.cfg.Logger != nil {
		l.pool.cfg.Logger("pool: %s → %s until %s (persisted)", l.pa.account.Identity(), banName(l.pa.am.ban), l.pa.am.parkUntil.Format(time.RFC3339))
	}
	// Persist the park (TASK_MUTE): banned survives restarts forever; muted/
	// risk windows must survive restarts so the account never re-hits the
	// upstream and renews its window.
	// A removed lease may finish after the same identity is re-added. Its
	// manager is intentionally detached; never overwrite the new record's
	// persisted health with an error from the old generation.
	for _, active := range l.pool.accounts {
		if active.am == l.pa.am {
			l.pool.notifyParkPersist(l.pa, ParkRecord{
				Mobile: l.pa.account.Identity(),
				Kind:   l.pa.am.ban,
				Until:  l.pa.am.parkUntil,
				Reason: l.pa.am.banMsg,
			})
			break
		}
	}
}

// notifyParkPersist hands a park-state transition to the persistence sink.
// Caller must hold pa.am.mu (state already updated in memory).
func (p *Pool) notifyParkPersist(pa *poolAccount, rec ParkRecord) {
	if p.cfg.OnParkPersist == nil {
		return
	}
	rec.Generation = pa.generation
	p.cfg.OnParkPersist(rec)
}

func banName(b BanState) string {
	switch b {
	case BanBanned:
		return "BANNED"
	case BanMuted:
		return "MUTED"
	case BanRiskDevice:
		return "RISK"
	}
	return "READY"
}

// Release returns the in-flight slot to the account.
func (l *Lease) Release() {
	if l == nil || l.pa == nil {
		return
	}
	select {
	case <-l.pa.slots:
	default:
	}
}

// Status returns per-account states (debug endpoint material).
func (p *Pool) Status() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	out := make([]map[string]any, 0, len(p.accounts))
	for _, pa := range p.accounts {
		h := p.healthNow(pa, now)
		state := "ready"
		switch h {
		case healthBanned:
			state = "banned"
		case healthMuted:
			state = "muted"
		case healthRisk:
			state = "risk"
		case healthCooling:
			state = "cooling"
		}
		pa.am.mu.Lock()
		parkUntil := pa.am.parkUntil.Format(time.RFC3339)
		pa.am.mu.Unlock()
		out = append(out, map[string]any{
			"mobile":     pa.account.Mobile,
			"region":     pa.account.normalizedRegion(),
			"state":      state,
			"inflight":   len(pa.slots),
			"max":        cap(pa.slots),
			"park_until": parkUntil,
		})
	}
	return out
}

// AccountStatus is one account's live runtime state — the material for the
// admin list endpoint. It carries the full stored record (credentials
// included; the admin surface is owner-only and unredacted by decision) plus
// the pool's view of it.
type AccountStatus struct {
	Account Account
	// State is "ready"/"banned"/"muted"/"risk" — the effective health with
	// expired parks already cleared.
	State string
	// ParkKind/ParkUntil/ParkReason mirror the current park state (empty
	// when ready); Until is zero for a permanent ban.
	ParkKind   string
	ParkUntil  time.Time
	ParkReason string
	// Inflight/MaxInflight are the account's live semaphore counts.
	Inflight    int
	MaxInflight int
	// TokenWarm reports whether the account holds a cached token (has
	// logged in and served at least one request).
	TokenWarm bool
}

// Snapshot returns every account's live runtime state. Reads of park fields
// are mutex-guarded; the copy is consistent per account, not across the
// whole ring.
func (p *Pool) Snapshot() []AccountStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	out := make([]AccountStatus, 0, len(p.accounts))
	for _, pa := range p.accounts {
		h := p.healthNow(pa, now)
		state := "ready"
		switch h {
		case healthBanned:
			state = "banned"
		case healthMuted:
			state = "muted"
		case healthRisk:
			state = "risk"
		case healthCooling:
			state = "cooling"
		}
		pa.am.mu.Lock()
		status := AccountStatus{
			Account:     pa.account,
			State:       state,
			ParkKind:    parkKindNames[pa.am.ban],
			ParkUntil:   pa.am.parkUntil,
			ParkReason:  pa.am.banMsg,
			Inflight:    len(pa.slots),
			MaxInflight: cap(pa.slots),
			TokenWarm:   pa.am.token != "",
		}
		pa.am.mu.Unlock()
		if status.ParkKind == "" {
			// Ready accounts: no park window/reason noise.
			status.ParkUntil = time.Time{}
			status.ParkReason = ""
		}
		out = append(out, status)
	}
	return out
}

// AccountRef is a read handle on one pool account for background work
// (human-paced cleanup): the account's client and token manager, without
// an in-flight slot. Refs are valid as long as the pool lives.
type AccountRef struct {
	Mobile string
	Client *Client
	AM     *AccountManager
}

// ActiveAccountRefs returns refs for every account that is healthy AND
// holds a cached token — i.e. has served traffic and is not parked.
// Callers must treat a nil-token ref as skip (background work must never
// trigger a login; parked accounts stay cold).
func (p *Pool) ActiveAccountRefs() []AccountRef {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var refs []AccountRef
	for _, pa := range p.accounts {
		if p.healthNow(pa, now) != healthReady {
			continue
		}
		pa.am.mu.Lock()
		hasToken := pa.am.token != ""
		pa.am.mu.Unlock()
		if !hasToken {
			continue
		}
		refs = append(refs, AccountRef{Mobile: pa.account.Identity(), Client: pa.client, AM: pa.am})
	}
	return refs
}

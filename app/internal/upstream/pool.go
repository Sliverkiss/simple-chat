// Multi-account pool: strict round-robin selection, per-account in-flight
// caps, and health states (banned / muted / risk-device). Ideas lifted from
// the ds2api account pool and snake-aabb-wtf idle/busy/error states; the bloat
// was not invited.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Default in-flight cap per account when MaxInflight is unset.
const DefaultMaxInflight = 2

// ErrNoAccounts means every account is banned (permanent) — no wait would help.
var ErrNoAccounts = errors.New("upstream: pool has no available accounts (all banned)")

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

// PoolConfig configures the pool.
type PoolConfig struct {
	// BaseURL overrides every region's base URL (tests inject httptest).
	BaseURL string
	// MaxInflight caps concurrent requests per account (default 2).
	MaxInflight int
	// QueueWait bounds how long Acquire waits for a slot when every account
	// is busy (default 30s).
	QueueWait time.Duration
	// MuteParkDefault parks a muted account when the upstream sends no
	// mute_until (default 6h; recon: mutes are "usually weeks" — this is a
	// conservative floor, not a guess at their duration).
	MuteParkDefault time.Duration
	// RiskCooldown parks a risk-device account (default 10min).
	RiskCooldown time.Duration
	// OnParkPersist receives every park-state transition (TASK_MUTE): a park
	// (ban/mute/risk) or the natural unpark (Kind=BanNone). The sink is
	// invoked synchronously under the account's lock, after the in-memory
	// state is updated — persistence never gates the fast path's correctness,
	// a failed write is the sink's problem to log.
	OnParkPersist func(ParkRecord)
	// OnLoginPersist receives every successful login/relogin's fresh bearer
	// token (docs-spec-memory-first.md): the write-through moment for the
	// persisted SessionToken field. Same posture as the park sink —
	// synchronous, fail-soft, nil = memory-only tokens.
	OnLoginPersist func(LoginRecord)
	Logger         func(format string, args ...any)
}

// LoginRecord is one successful login, handed to PoolConfig.OnLoginPersist.
type LoginRecord struct {
	Identity string
	Token    string
}

// ParkRecord is one park-state transition, handed to PoolConfig.OnParkPersist.
// A zero Until with Kind=BanBanned means "forever" (manual revive only).
type ParkRecord struct {
	Mobile string
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
// A garbage until is treated as absent — an unparseable window must not
// brick the account, and ParseParkKind already rejects garbage kinds.
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
		c.MuteParkDefault = 6 * time.Hour
	}
	if c.RiskCooldown <= 0 {
		c.RiskCooldown = 10 * time.Minute
	}
}

// poolAccount is the runtime state of one account in the ring.
type poolAccount struct {
	account Account
	client  *Client
	am      *AccountManager

	// slots is the in-flight semaphore: buffered channel of size MaxInflight.
	slots chan struct{}
}

// health states for an account.
type health int

const (
	healthReady health = iota
	healthBanned
	healthMuted
	healthRisk
)

// Pool is the account ring.
type Pool struct {
	accounts  []*poolAccount
	cfg       PoolConfig
	transport http.RoundTripper // shared tuned Transport (R5)

	mu      sync.Mutex
	cursor  int // next ring index to try
	nextSeq int // round-robin sequence counter (debug/introspection)
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
	p := &Pool{cfg: cfg, transport: transport}
	parked := 0
	for _, a := range accounts {
		pa, err := p.buildAccount(a)
		if err != nil {
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
	// Duplicate entries are intentional: each accounts.json row is its
	// own ring slot (own token, own semaphore), giving one physical
	// account more slots when repeated.
	client := NewClient(Config{
		BaseURL: p.cfg.BaseURL,
		Mobile:  a.Mobile,
		Email:   a.Email,
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
	// Login write-through (docs-spec-memory-first.md): every successful
	// login/relogin hands the fresh token to the sink under the manager's
	// lock. Nil hook = memory-only tokens (current behavior).
	if p.cfg.OnLoginPersist != nil {
		hook := p.cfg.OnLoginPersist
		client.AccountManager().onLogin = func(tok string) {
			hook(LoginRecord{Identity: a.Identity(), Token: tok})
		}
	}
	pa := &poolAccount{
		account: a,
		client:  client,
		am:      client.AccountManager(),
		slots:   make(chan struct{}, p.cfg.MaxInflight),
	}
	// Restore persisted park state (TASK_MUTE): an account muted/banned
	// before a restart must not re-enter rotation "clean" — re-hitting
	// the upstream renews the window (observed 6h mute → 3-day ban
	// escalation). Expired parks are ignored (ready on load); banned
	// persists forever.
	kind, until := parsePersistedPark(a)
	if kind != BanNone && !until.IsZero() && !time.Now().After(until) {
		pa.am.mu.Lock()
		pa.am.ban = kind
		pa.am.parkUntil = until
		pa.am.banMsg = a.ParkReason
		pa.am.mu.Unlock()
		p.logf("pool: %s still %s until %s (restored from disk)", a.Identity(), banName(kind), until.Format(time.RFC3339))
	} else if kind == BanBanned {
		// Banned has no window: zero/absent until means forever.
		pa.am.mu.Lock()
		pa.am.ban = BanBanned
		pa.am.banMsg = a.ParkReason
		pa.am.mu.Unlock()
		p.logf("pool: %s still BANNED (restored from disk; manual revive = delete park fields from accounts.json)", a.Identity())
	}
	return pa, nil
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
	p.accounts = append(p.accounts, pa)
	// Grow the shared transport's idle pool so the new account's
	// concurrency does not force TCP+TLS re-handshakes.
	if tr, ok := p.transport.(*http.Transport); ok {
		if want := p.cfg.MaxInflight * len(p.accounts); want > tr.MaxIdleConnsPerHost {
			tr.MaxIdleConnsPerHost = want
		}
	}
	// cursor stays valid: it is < len(accounts) for the old ring, and the
	// old ring is a prefix of the new one.
	p.logf("pool: hot-added %s (ring now %d)", a.Identity(), len(p.accounts))
	return nil
}

// RemoveAccount removes every ring slot matching id (mobile, or email for
// email-only accounts) from selection, reporting whether anything was
// removed. In-flight leases on the removed account are NOT disturbed — a
// lease holds the *poolAccount reference, not a ring index — and their
// Release still works (the channel outlives the ring entry). The upstream
// sessions and tokens of a removed account are left alone: removal only
// stops new acquisitions.
func (p *Pool) RemoveAccount(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.accounts[:0]
	removed := 0
	for _, pa := range p.accounts {
		if pa.account.MatchesIdentity(id) {
			removed++
			continue
		}
		kept = append(kept, pa)
	}
	if removed == 0 {
		return false
	}
	p.accounts = kept
	// Keep the cursor inside the shrunken ring. Removing an entry before
	// (or at) the cursor would otherwise skip a survivor; clamping to the
	// last index keeps the rotation fair without a restart.
	if n := len(p.accounts); n > 0 && p.cursor >= n {
		p.cursor = n - 1
	}
	p.logf("pool: removed %s (%d slot(s), ring now %d)", id, removed, len(p.accounts))
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
	if !pa.am.parkUntil.IsZero() && now.After(pa.am.parkUntil) {
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
	return healthReady
}

// isBusy reports whether the account has free in-flight slots.
func (pa *poolAccount) isBusy() bool {
	return len(pa.slots) >= cap(pa.slots)
}

// Acquire selects the next account with free capacity and reserves a slot.
// Selection: strict round-robin from the cursor; an account whose slots are
// full is skipped (its ring position is unchanged); if all candidates are
// busy, wait up to cfg.QueueWait polling for a slot; if all are banned,
// fail immediately with ErrNoAccounts.
func (p *Pool) Acquire(ctx context.Context) (*Lease, error) {
	deadline := time.Now().Add(p.cfg.QueueWait)
	for {
		lease, err := p.tryAcquire(ctx)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, ErrPoolBusy) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, &PoolBusyError{RetryAfter: p.cfg.QueueWait.Round(time.Second) + time.Second}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// tryAcquire does one non-blocking pass over the ring.
func (p *Pool) tryAcquire(ctx context.Context) (*Lease, error) {
	p.mu.Lock()
	n := len(p.accounts)
	if n == 0 {
		// Empty ring (admin API removed every account): nothing can free a
		// slot, so waiting cannot help — fail fast with ErrNoAccounts.
		p.mu.Unlock()
		return nil, ErrNoAccounts
	}
	now := time.Now()
	var anyBanned bool
	for i := 0; i < n; i++ {
		idx := (p.cursor + i) % n
		pa := p.accounts[idx]
		h := p.healthNow(pa, now)
		if h == healthBanned {
			anyBanned = true
			continue
		}
		if h != healthReady {
			continue // parked (muted/risk): skip but not permanently
		}
		if pa.isBusy() {
			continue // full: skip, ring position unchanged
		}
		// Reserve the slot under the lock so two goroutines can't both take
		// the last slot of the same account.
		pa.slots <- struct{}{}
		p.cursor = (idx + 1) % n
		p.nextSeq++
		p.mu.Unlock()
		return &Lease{pool: p, pa: pa}, nil
	}
	p.mu.Unlock()
	if !anyBanned {
		return nil, ErrPoolBusy
	}
	// Some accounts are banned, others busy or parked. If every account is
	// banned, no wait can help.
	allGone := true
	for _, pa := range p.accounts {
		if p.healthNow(pa, now) != healthBanned {
			allGone = false
			break
		}
	}
	if allGone {
		return nil, ErrNoAccounts
	}
	return nil, ErrPoolBusy
}

// Lease is one reserved in-flight slot on one account.
type Lease struct {
	pool *Pool
	pa   *poolAccount
}

// Account returns the account config for this lease.
func (l *Lease) Account() Account { return l.pa.account }

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
	if BanKind(err) == BanNone {
		return
	}
	be, _ := err.(*BizError)
	l.pa.am.mu.Lock()
	defer l.pa.am.mu.Unlock()
	switch BanKind(err) {
	case BanBanned:
		l.pa.am.ban = BanBanned
		l.pa.am.banMsg = "account banned: " + err.Error()
	case BanMuted:
		until := time.Time{}
		if be != nil {
			until = be.MuteUntil
		}
		if until.IsZero() || until.Before(time.Now()) {
			until = time.Now().Add(l.pool.cfg.MuteParkDefault)
		}
		l.pa.am.ban = BanMuted
		l.pa.am.parkUntil = until
		l.pa.am.banMsg = "account muted: " + err.Error()
	case BanRiskDevice:
		l.pa.am.ban = BanRiskDevice
		l.pa.am.parkUntil = time.Now().Add(l.pool.cfg.RiskCooldown)
		l.pa.am.banMsg = "risk device detected: " + err.Error()
	}
	if l.pool.cfg.Logger != nil {
		l.pool.cfg.Logger("pool: %s → %s until %s (persisted)", l.pa.account.Identity(), banName(l.pa.am.ban), l.pa.am.parkUntil.Format(time.RFC3339))
	}
	// Persist the park (TASK_MUTE): banned survives restarts forever; muted/
	// risk windows must survive restarts so the account never re-hits the
	// upstream and renews its window.
	l.pool.notifyParkPersist(l.pa, ParkRecord{
		Mobile: l.pa.account.Identity(),
		Kind:   l.pa.am.ban,
		Until:  l.pa.am.parkUntil,
		Reason: l.pa.am.banMsg,
	})
}

// notifyParkPersist hands a park-state transition to the persistence sink.
// Caller must hold pa.am.mu (state already updated in memory).
func (p *Pool) notifyParkPersist(pa *poolAccount, rec ParkRecord) {
	if p.cfg.OnParkPersist == nil {
		return
	}
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
		}
		out = append(out, map[string]any{
			"mobile":     pa.account.Mobile,
			"region":     pa.account.normalizedRegion(),
			"state":      state,
			"inflight":   len(pa.slots),
			"max":        cap(pa.slots),
			"park_until": pa.am.parkUntil.Format(time.RFC3339),
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

// Memory-first store wrapper (docs-spec-memory-first.md): when Upstash (or
// any backing store) is configured as the source of truth, the runtime
// operates purely on an in-memory copy. The backing is contacted exactly
// once at boot (Load) and afterwards only at write-through moments:
//
//   - admin upload / remove / update  (SaveAccount / DeleteAccount)
//   - login / relogin token refresh    (ApplyLogin)
//   - park transitions                 (ApplyPark)
//   - device-id minting at boot        (SaveAccount via EnsureDeviceIDs)
//
// The wrapper is store-agnostic: it wraps any Store (the Redis/Upstash
// store in production; counting fakes in tests).
package accountstore

import (
	"context"
	"log"
	"sync"
	"time"

	"simple-chat/internal/upstream"
)

// MemoryFirstStore caches every account record in memory and writes state
// changes through to the backing store. Backing reads happen only at
// construction; backing writes happen only at the write-through moments.
// Fail-soft writes (ApplyPark/ApplyLogin) log and leave the in-memory state
// correct — the next transition re-writes the full record, healing a missed
// write. Hard writes (SaveAccount/DeleteAccount) propagate errors with the
// cache untouched, so the admin API can answer 5xx honestly.
type MemoryFirstStore struct {
	backing Store
	logger  *log.Logger

	mu       sync.Mutex
	accounts map[string]upstream.Account // keyed by Identity()
}

// NewMemoryFirstStore performs the one boot load from the backing store and
// builds the in-memory cache. A validation failure is fatal (a corrupt
// source of truth must not serve); an empty backing is a valid state — the
// pool starts empty and accounts arrive via the admin API (fresh cloud
// Upstash deployment shape).
func NewMemoryFirstStore(ctx context.Context, backing Store, logger *log.Logger) (*MemoryFirstStore, error) {
	accounts, err := backing.Load(ctx)
	if err != nil {
		return nil, err
	}
	cache := make(map[string]upstream.Account, len(accounts))
	for _, a := range accounts {
		cache[a.Identity()] = a
	}
	if len(cache) == 0 {
		logf(logger, "memory-first store: backing is empty — starting with an empty pool (add accounts via admin API)")
	}
	return &MemoryFirstStore{backing: backing, logger: logger, accounts: cache}, nil
}

// Load returns the cached accounts. Zero backing reads — this is the
// request-path surface (admin duplicate checks, restarts of the pool ring).
func (s *MemoryFirstStore) Load(ctx context.Context) ([]upstream.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]upstream.Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		out = append(out, a)
	}
	return out, nil
}

// SaveAccount upserts one record: backing write first (an error propagates
// and the cache stays untouched — the admin API answers 5xx honestly), then
// the cache follows.
func (s *MemoryFirstStore) SaveAccount(ctx context.Context, acct upstream.Account) error {
	if err := s.backing.SaveAccount(ctx, acct); err != nil {
		return err
	}
	s.mu.Lock()
	s.accounts[acct.Identity()] = acct
	s.mu.Unlock()
	return nil
}

// DeleteAccount removes one record from the backing first
// (ErrAccountNotFound propagates), then from the cache.
func (s *MemoryFirstStore) DeleteAccount(ctx context.Context, identity string) error {
	if err := s.backing.DeleteAccount(ctx, identity); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.accounts, identity)
	s.mu.Unlock()
	return nil
}

// ApplyPark merges one park transition into the cached record and writes
// the full merged record through — no backing read (the old Redis
// GET+merge+SET cycle is gone). Unknown identity is the same no-op the
// backing stores take. Fail-soft: the in-memory pool state is already
// correct when this runs; a failed write logs and heals on the next
// transition.
func (s *MemoryFirstStore) ApplyPark(rec upstream.ParkRecord) {
	s.mu.Lock()
	acct, ok := s.accounts[rec.Mobile]
	if !ok {
		s.mu.Unlock()
		logf(s.logger, "memory-first store: park for unknown identity %s skipped", rec.Mobile)
		return
	}
	if !applyParkRecord(&acct, rec, time.Now()) {
		s.mu.Unlock()
		return // already persisted exactly this state
	}
	s.accounts[rec.Mobile] = acct
	s.mu.Unlock()
	if err := s.backing.SaveAccount(context.Background(), acct); err != nil {
		logf(s.logger, "memory-first store: cannot persist park state for %s: %v", rec.Mobile, err)
	}
}

// ApplyLogin records one fresh login token on the cached account and writes
// the full record through. Fail-soft: a missed token write costs one
// relogin after a restart, never a bad request.
func (s *MemoryFirstStore) ApplyLogin(rec upstream.LoginRecord) {
	s.mu.Lock()
	acct, ok := s.accounts[rec.Identity]
	if !ok {
		s.mu.Unlock()
		// Login for an identity not in the store (removed mid-run): nothing
		// to update — the token lives in the pool's memory only.
		return
	}
	acct.SessionToken = rec.Token
	s.accounts[rec.Identity] = acct
	s.mu.Unlock()
	if err := s.backing.SaveAccount(context.Background(), acct); err != nil {
		logf(s.logger, "memory-first store: cannot persist session token for %s: %v", rec.Identity, err)
	}
}

// Close releases the backing store's resources.
func (s *MemoryFirstStore) Close() error { return s.backing.Close() }

// logf logs through the given logger when set.
func logf(logger *log.Logger, format string, args ...any) {
	if logger != nil {
		logger.Printf(format, args...)
	}
}

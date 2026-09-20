// Package accountstore persists account state (credentials, device ids,
// park state) across restarts. Two implementations share one contract:
//
//   - JSONStore (default): the accounts.json file, semantics unchanged from
//     the pre-refactor code (atomic tmp+rename writes, 0600 enforcement,
//     park state written back on every pool transition).
//   - RedisStore (optional, DS_REDIS_HOST + DS_REDIS_TOKEN): one key per account plus an index
//     set, for hosted deployments without a writable file system.
//
// Both are persistence only — the live path is always the in-memory pool.
package accountstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"simple-chat/internal/upstream"
)

// ErrAccountNotFound reports a delete of an id no persisted record matches.
// The admin API maps it to 404.
var ErrAccountNotFound = errors.New("accountstore: account not found")

// Store persists account state across restarts.
type Store interface {
	// Load returns every persisted account (full records, credentials
	// included). Expired mute/risk parks are stripped (and the stripping
	// persisted) before the accounts are returned; banned survives forever.
	Load(ctx context.Context) ([]upstream.Account, error)
	// SaveAccount upserts one account's persisted state (device id, park
	// fields — the whole record). Duplicate rows for one identity follow the
	// credential: a JSON-file save updates every matching row.
	SaveAccount(ctx context.Context, acct upstream.Account) error
	// DeleteAccount removes the persisted record with the given identity
	// (mobile, else email). ErrAccountNotFound when no record matches.
	DeleteAccount(ctx context.Context, identity string) error
	// ApplyPark merges one park-state transition into the persisted state.
	// It is called synchronously under the account's pool lock; failures are
	// logged internally and never propagated — persistence is fail-soft, the
	// in-memory pool state is already correct when it runs.
	ApplyPark(rec upstream.ParkRecord)
	// Close releases resources (no-op for the file store).
	Close() error
}

// applyParkRecord merges one transition into acct in place and reports
// whether anything changed. A re-park of the identical state is a no-op so
// parked_at does not churn; an unpark (Kind=BanNone) clears the fields.
func applyParkRecord(acct *upstream.Account, rec upstream.ParkRecord, now time.Time) bool {
	if rec.Kind == upstream.BanNone {
		changed := acct.ParkKind != "" || acct.ParkUntil != "" || acct.ParkReason != "" || acct.ParkedAt != ""
		acct.ParkKind, acct.ParkUntil, acct.ParkReason, acct.ParkedAt = "", "", "", ""
		return changed
	}
	until := ""
	if !rec.Until.IsZero() {
		until = rec.Until.UTC().Format(time.RFC3339)
	}
	if acct.ParkKind == upstream.ParkKindName(rec.Kind) && acct.ParkUntil == until && acct.ParkReason == rec.Reason {
		return false // already persisted exactly this state
	}
	acct.ParkKind = upstream.ParkKindName(rec.Kind)
	acct.ParkUntil = until
	acct.ParkReason = rec.Reason
	acct.ParkedAt = now.UTC().Format(time.RFC3339)
	return true
}

// parkExpired reports whether acct carries a mute/risk park whose window has
// lapsed (or whose until is unparseable — fail-safe: treat as expired). A
// garbage until must not brick the account. Banned never expires.
func parkExpired(acct upstream.Account, now time.Time) bool {
	kind := upstream.ParseParkKind(acct.ParkKind)
	if kind != upstream.BanMuted && kind != upstream.BanRiskDevice {
		return false
	}
	until, err := time.Parse(time.RFC3339, acct.ParkUntil)
	return err != nil || now.After(until)
}

// EnsureDeviceIDs resolves and persists every account's device identity:
// accounts without an explicit device_id get one derived deterministically
// (upstream.ResolveDeviceID) and saved through the store so the id never
// changes across restarts. Generation is deterministic anyway — persistence
// makes the store the visible source of truth and survives future salt
// changes.
func EnsureDeviceIDs(ctx context.Context, store Store) ([]upstream.Account, error) {
	accounts, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	for i := range accounts {
		// Web-channel accounts present a browser-harvested Shumei id —
		// minting an android-format one would forge an identity the risk
		// service is guaranteed to reject (web-reverse-research.md §5/§6).
		// The empty device_id fails Validate() (with a clear message) at
		// pool load; do not paper over it here.
		if accounts[i].DeviceID == "" && accounts[i].Channel != "web" {
			accounts[i].DeviceID = upstream.ResolveDeviceID(accounts[i])
			if err := store.SaveAccount(ctx, accounts[i]); err != nil {
				return nil, fmt.Errorf("persist device id for %s: %w", accounts[i].Mobile, err)
			}
		}
	}
	return accounts, nil
}

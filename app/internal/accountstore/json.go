package accountstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"simple-chat/internal/upstream"
)

type accountsFile struct {
	Accounts []upstream.Account `json:"accounts"`
}

// JSONStore persists accounts into accounts.json — the default store. The
// file is rewritten atomically (temp file + rename) on every save; the
// format (and the 0600 permission enforcement on load) is unchanged from the
// original server-side implementation (TASK_MUTE / device-id persistence).
type JSONStore struct {
	path   string
	logger *log.Logger
	mu     sync.Mutex
}

// NewJSONStore builds the file-backed store. A nil logger still works (logs
// are dropped); an empty path yields a Load error — build it only for a real
// file.
func NewJSONStore(path string, logger *log.Logger) *JSONStore {
	return &JSONStore{path: path, logger: logger}
}

// Load reads the accounts file and enforces 0600 permissions.
func (s *JSONStore) Load(ctx context.Context) ([]upstream.Account, error) {
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		// Fresh deployment (e.g. a cloud container with no mounted volume):
		// create an empty store file so the server can start and accounts can
		// be added via the admin API. A zero-account pool is a valid state —
		// requests will fail with no_accounts until one is uploaded.
		if werr := os.WriteFile(s.path, []byte("{\"accounts\": []}"), 0600); werr != nil {
			return nil, fmt.Errorf("create %s: %w", s.path, werr)
		}
		s.logf("accounts file %s missing — created empty store (add accounts via admin API)", s.path)
		return nil, nil
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return nil, err
	}
	if mode := info.Mode().Perm(); mode != 0600 {
		return nil, fmt.Errorf("accounts file %s must have 0600 permissions (has %o): run chmod 600 %s", s.path, mode, s.path)
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}
	var af accountsFile
	if err := json.Unmarshal(raw, &af); err != nil {
		return nil, fmt.Errorf("parse %s: %w", s.path, err)
	}
	if len(af.Accounts) == 0 {
		// Empty store (fresh file or all accounts removed): valid state.
		// The server starts with a zero-account pool; requests return
		// no_accounts until accounts are added via the admin API.
		s.logf("accounts file %s is empty (add accounts via admin API)", s.path)
		return nil, nil
	}
	for _, a := range af.Accounts {
		if err := a.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", s.path, err)
		}
	}
	s.clearExpiredParks(af.Accounts)
	return af.Accounts, nil
}

// clearExpiredParks strips expired mute/risk park fields from the accounts
// in memory and persists the stripped file, so a restart after a window
// lapsed serves normally and the file does not accumulate stale park rows.
// Banned rows are never touched (they survive forever by design).
func (s *JSONStore) clearExpiredParks(accounts []upstream.Account) {
	now := time.Now()
	changed := false
	for i := range accounts {
		if !parkExpired(accounts[i], now) {
			continue
		}
		accounts[i].ParkKind, accounts[i].ParkUntil = "", ""
		accounts[i].ParkReason, accounts[i].ParkedAt = "", ""
		changed = true
	}
	if !changed {
		return
	}
	if err := s.writeFile(accounts); err != nil {
		s.logf("accounts store: cannot clear expired parks in %s: %v", s.path, err)
	}
}

// writeFile marshals and writes the whole file atomically.
func (s *JSONStore) writeFile(accounts []upstream.Account) error {
	out, err := json.MarshalIndent(accountsFile{Accounts: accounts}, "", " ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	return persistAccountsFile(s.path, out)
}

// persistAccountsFile writes raw atomically (temp file + rename) when the
// path is a regular writable location. Docker single-file bind mounts make
// rename-over-mountpoint impossible (EBUSY) and the image's /app is not
// writable by the nonroot user — there we fall back to an in-place truncate
// write of the mounted file itself. That window is not atomic, but a torn
// file is recoverable: generated device ids are deterministic, and the only
// irreplaceable values (explicit harvested device_id fields) exist in the
// operator's backup of the file.
func persistAccountsFile(path string, raw []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0600); err == nil {
		if err := os.Rename(tmp, path); err == nil {
			return nil
		}
		os.Remove(tmp)
	}
	return os.WriteFile(path, raw, 0600)
}

// SaveAccount upserts one account into the file: reads, merges (every row
// matching the mobile — duplicate rows are separate ring slots, park state
// follows the credential; a row for an unknown mobile is appended), writes.
// The whole read-merge-write is under the store mutex, so concurrent parks
// over one shared file stay consistent.
func (s *JSONStore) SaveAccount(ctx context.Context, acct upstream.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts, err := s.readFileLocked()
	if err != nil {
		return err
	}
	accounts, ok := mergeAccount(accounts, acct)
	if !ok {
		return nil // nothing changed
	}
	return s.writeFile(accounts)
}

// ApplyPark merges one ParkRecord into the file (read → merge → write under
// the store mutex). A failed read or write logs; the in-memory pool state is
// already correct, so the failure mode is "restart loses one park", never a
// bad request. A nil store is a silent no-op (memory-only park state).
func (s *JSONStore) ApplyPark(rec upstream.ParkRecord) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts, err := s.readFileLocked()
	if err != nil {
		var pe *parseError
		if errors.As(err, &pe) {
			s.logf("accounts store: cannot parse %s to persist park state: %v", s.path, err)
		} else {
			s.logf("accounts store: cannot read %s to persist park state: %v", s.path, err)
		}
		return
	}
	changed := false
	now := time.Now()
	for i := range accounts {
		if accounts[i].MatchesIdentity(rec.Mobile) {
			changed = applyParkRecord(&accounts[i], rec, now) || changed
		}
	}
	if !changed {
		return
	}
	if err := s.writeFile(accounts); err != nil {
		s.logf("accounts store: cannot write %s: %v", s.path, err)
	}
}

// DeleteAccount removes every row whose identity (mobile, else email)
// matches, rewriting the file under the store mutex. ErrAccountNotFound when
// no row matches (the file is untouched then). Rows for other accounts,
// including their park state, are preserved verbatim.
func (s *JSONStore) DeleteAccount(ctx context.Context, identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts, err := s.readFileLocked()
	if err != nil {
		return err
	}
	kept := accounts[:0:0]
	for _, a := range accounts {
		if a.MatchesIdentity(identity) {
			continue
		}
		kept = append(kept, a)
	}
	if len(kept) == len(accounts) {
		return ErrAccountNotFound
	}
	return s.writeFile(kept)
}

// readFileLocked loads the current file contents, distinguishing a read
// failure from a parse failure for the log line. Caller holds s.mu.
func (s *JSONStore) readFileLocked() ([]upstream.Account, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return nil, &readError{err}
	}
	var af accountsFile
	if err := json.Unmarshal(raw, &af); err != nil {
		return nil, &parseError{err}
	}
	return af.Accounts, nil
}

// readError / parseError tag the failure class for ApplyPark's log line.
type readError struct{ err error }

func (e *readError) Error() string { return e.err.Error() }
func (e *readError) Unwrap() error { return e.err }

type parseError struct{ err error }

func (e *parseError) Error() string { return e.err.Error() }
func (e *parseError) Unwrap() error { return e.err }

// Close is a no-op for the file store.
func (s *JSONStore) Close() error { return nil }

// logf logs through the store's logger when set.
func (s *JSONStore) logf(format string, args ...any) {
	if s != nil && s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

// mergeAccount folds acct into accounts: every row with the same mobile is
// replaced by acct (duplicate rows are separate ring slots for one
// credential — one save must update them all or the surviving slot keeps
// stale state); no match appends. Returns the (possibly grown) slice and
// whether anything changed.
func mergeAccount(accounts []upstream.Account, acct upstream.Account) ([]upstream.Account, bool) {
	changed := false
	matched := false
	for i := range accounts {
		if !accounts[i].MatchesIdentity(acct.Identity()) {
			continue
		}
		matched = true
		if accounts[i] == acct {
			continue
		}
		accounts[i] = acct
		changed = true
	}
	if !matched {
		return append(accounts, acct), true
	}
	return accounts, changed
}

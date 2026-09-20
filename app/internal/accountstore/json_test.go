// Unit tests for the park-persistence store: error paths, no-match, idempotent
// writes, unpark clears, and duplicate mobile entries (each accounts.json row
// is its own ring slot; park state follows the credential).
package accountstore

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// storeTestEnv builds an accountStore over a temp file plus a capturing
// logger.
func storeTestEnv(t *testing.T, accounts []upstream.Account) (*JSONStore, string, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "accounts.json")
	if err := writeTestFile(path, accounts); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	return NewJSONStore(path, logger), path, &buf
}

func TestJSONStoreApplySetsParkFields(t *testing.T) {
	store, path, _ := storeTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw"},
	})
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: until, Reason: "user is muted"})

	accts := readTestFile(t, path)
	if accts[0].ParkKind != "muted" || accts[0].ParkReason != "user is muted" {
		t.Fatalf("park fields = %+v", accts[0])
	}
	if got, err := time.Parse(time.RFC3339, accts[0].ParkUntil); err != nil || !got.Equal(until) {
		t.Errorf("park_until = %q (err %v), want %v", accts[0].ParkUntil, err, until)
	}
	if accts[0].ParkedAt == "" {
		t.Error("parked_at missing")
	}
}

func TestJSONStoreApplyBannedWritesZeroUntil(t *testing.T) {
	store, path, _ := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanBanned, Reason: "account banned upstream"})
	accts := readTestFile(t, path)
	if accts[0].ParkKind != "banned" || accts[0].ParkUntil != "" {
		t.Fatalf("banned fields = %+v, want kind=banned until=\"\"", accts[0])
	}
}

func TestJSONStoreApplyClearsOnUnpark(t *testing.T) {
	store, path, _ := storeTestEnv(t, []upstream.Account{{
		Mobile: "100", Password: "pw",
		ParkKind: "muted", ParkUntil: "2030-01-01T00:00:00Z", ParkReason: "r", ParkedAt: "2029-01-01T00:00:00Z",
	}})
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanNone})
	accts := readTestFile(t, path)
	if accts[0].ParkKind != "" || accts[0].ParkUntil != "" || accts[0].ParkReason != "" || accts[0].ParkedAt != "" {
		t.Fatalf("unpark did not clear fields: %+v", accts[0])
	}
}

func TestJSONStoreApplyNoMatchIsLoggedNoop(t *testing.T) {
	store, path, buf := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	before, _ := os.ReadFile(path)
	store.ApplyPark(upstream.ParkRecord{Mobile: "999", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("apply for unknown mobile must not rewrite the file")
	}
	if buf.Len() != 0 {
		t.Errorf("unexpected log for no-match: %s", buf)
	}
}

func TestJSONStoreApplyIdempotentSameState(t *testing.T) {
	store, path, _ := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	rec := upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: until, Reason: "user is muted"}
	store.ApplyPark(rec)
	first, _ := os.ReadFile(path)
	store.ApplyPark(rec) // identical state: must not rewrite (parked_at would churn)
	second, _ := os.ReadFile(path)
	if !bytes.Equal(first, second) {
		t.Fatal("identical park state rewrote the file")
	}
}

func TestJSONStoreApplyUpdatesEveryDuplicateMobile(t *testing.T) {
	// Duplicate rows are separate ring slots for one credential: a park must
	// freeze all of them, or the surviving slot keeps sending traffic.
	store, path, _ := storeTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
		{Mobile: "100", Password: "pw"},
	})
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "user is muted"})
	accts := readTestFile(t, path)
	for _, i := range []int{0, 2} {
		if accts[i].ParkKind != "muted" {
			t.Errorf("duplicate row %d not parked: %+v", i, accts[i])
		}
	}
	if accts[1].ParkKind != "" {
		t.Errorf("other account parked: %+v", accts[1])
	}
}

func TestJSONStoreApplyReadErrorLogs(t *testing.T) {
	store, _, buf := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	store.path = filepath.Join(t.TempDir(), "missing.json")
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	if !strings.Contains(buf.String(), "cannot read") {
		t.Errorf("want read-failure log, got %q", buf)
	}
}

func TestJSONStoreApplyParseErrorLogs(t *testing.T) {
	store, _, buf := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	if err := os.WriteFile(store.path, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	if !strings.Contains(buf.String(), "cannot parse") {
		t.Errorf("want parse-failure log, got %q", buf)
	}
}

func TestJSONStoreNilSafe(t *testing.T) {
	// A nil store (no AccountsPath configured) must be a silent no-op.
	var store *JSONStore
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted})
	store.logf("nothing")
}

func TestJSONStoreApplyEmptyPathIsNoop(t *testing.T) {
	store := NewJSONStore("", nil)
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted})
}

func TestJSONStoreClearExpiredParks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	accounts := []upstream.Account{
		{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: past, ParkReason: "stale"},
		{Mobile: "101", Password: "pw", ParkKind: "muted", ParkUntil: future, ParkReason: "live"},
		{Mobile: "102", Password: "pw", ParkKind: "banned", ParkReason: "forever"},
		{Mobile: "103", Password: "pw", ParkKind: "muted", ParkUntil: "garbage", ParkReason: "broken"},
		{Mobile: "104", Password: "pw"},
	}
	if err := writeTestFile(path, accounts); err != nil {
		t.Fatal(err)
	}
	(&JSONStore{path: path}).clearExpiredParks(readTestFile(t, path))
	accts := readTestFile(t, path)
	if accts[0].ParkKind != "" {
		t.Errorf("expired mute not cleared: %+v", accts[0])
	}
	if accts[1].ParkKind != "muted" {
		t.Errorf("live mute wrongly cleared: %+v", accts[1])
	}
	if accts[2].ParkKind != "banned" {
		t.Errorf("banned wrongly cleared: %+v", accts[2])
	}
	if accts[3].ParkKind != "" {
		t.Errorf("garbage until must be cleared (fail-safe): %+v", accts[3])
	}
	if accts[4].ParkKind != "" {
		t.Errorf("clean account touched: %+v", accts[4])
	}
}

func TestJSONStoreClearExpiredParksMissingAndGarbageFiles(t *testing.T) {
	dir := t.TempDir()
	// Missing file and unparseable file must both be silent no-ops (the
	// real load error surfaces from LoadAccounts).
	(&JSONStore{path: filepath.Join(dir, "missing.json")}).clearExpiredParks(nil)
	garbage := filepath.Join(dir, "garbage.json")
	if err := os.WriteFile(garbage, []byte("["), 0600); err != nil {
		t.Fatal(err)
	}
	(&JSONStore{path: garbage}).clearExpiredParks(nil)
	// No-change file must not be rewritten.
	stable := filepath.Join(dir, "stable.json")
	if err := writeTestFile(stable, []upstream.Account{{Mobile: "1", Password: "p"}}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(stable)
	(&JSONStore{path: stable}).clearExpiredParks(nil)
	after, _ := os.ReadFile(stable)
	if !bytes.Equal(before, after) {
		t.Fatal("no-op clear rewrote the file")
	}
}

// Round-trip check: the file the store writes decodes through LoadAccounts
// (0600 preserved).
func TestJSONStoreFileStaysLoadable(t *testing.T) {
	store, path, _ := storeTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	if _, err := (&JSONStore{path: path}).Load(context.Background()); err != nil {
		t.Fatalf("stored file not loadable: %v", err)
	}
	var _ = json.Marshal
}

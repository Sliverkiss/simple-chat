package accountstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"simple-chat/internal/upstream"
)

func TestEnsureDeviceIDsPersistsGeneratedIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "13800000009", Password: "pw"},
		{Mobile: "13800000007", Password: "pw"},
	})

	accounts, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil))
	if err != nil {
		t.Fatal(err)
	}

	// (a) generated ids exist and differ per account
	ids := map[string]bool{}
	for _, a := range accounts {
		if a.DeviceID == "" {
			t.Fatalf("account %s has empty device id after EnsureDeviceIDs", a.Mobile)
		}
		if ids[a.DeviceID] {
			t.Errorf("device id %q shared between accounts", a.DeviceID)
		}
		ids[a.DeviceID] = true
	}

	// (b) persisted to disk: reload and compare
	persisted := readTestFile(t, path)
	if len(persisted) != len(accounts) {
		t.Fatalf("persisted %d accounts, want %d", len(persisted), len(accounts))
	}
	for i := range accounts {
		if persisted[i].DeviceID != accounts[i].DeviceID {
			t.Errorf("account %s: file has %q, memory has %q — persistence broken",
				accounts[i].Mobile, persisted[i].DeviceID, accounts[i].DeviceID)
		}
	}

	// (c) stable across restart: second run returns the same ids unchanged
	second, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := range accounts {
		if second[i].DeviceID != accounts[i].DeviceID {
			t.Errorf("account %s: id changed across restart (%q -> %q)",
				accounts[i].Mobile, accounts[i].DeviceID, second[i].DeviceID)
		}
	}
}

func TestEnsureDeviceIDsKeepsExplicitIDsVerbatim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "13800000008", Password: "pw", DeviceID: "harvested_smsdk_value"},
	})

	accounts, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil))
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].DeviceID != "harvested_smsdk_value" {
		t.Errorf("explicit device id %q was modified to %q", "harvested_smsdk_value", accounts[0].DeviceID)
	}
	// File rewritten must also carry it verbatim.
	if got := readTestFile(t, path)[0].DeviceID; got != "harvested_smsdk_value" {
		t.Errorf("file device id = %q, want verbatim harvested value", got)
	}
}

func TestEnsureDeviceIDsPreservesFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	writeTestAccounts(t, path, []upstream.Account{{Mobile: "13800000009", Password: "pw"}})

	if _, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("permissions changed to %o, want 0600", perm)
	}
}

package accountstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"simple-chat/internal/upstream"
)

// writeTestFile writes an accounts file with 0600 perms (test helper).
func writeTestFile(path string, accounts []upstream.Account) error {
	raw, err := json.Marshal(accountsFile{Accounts: accounts})
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0600)
}

// writeTestAccounts is writeTestFile with t.Helper + Fatal semantics.
func writeTestAccounts(t *testing.T, path string, accounts []upstream.Account) {
	t.Helper()
	if err := writeTestFile(path, accounts); err != nil {
		t.Fatal(err)
	}
}

// readTestFile decodes the accounts file at path (test helper).
func readTestFile(t *testing.T, path string) []upstream.Account {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var af accountsFile
	if err := json.Unmarshal(raw, &af); err != nil {
		t.Fatal(err)
	}
	return af.Accounts
}

// tempAccountsPath creates an empty 0600 accounts file path in t.TempDir.
func tempAccountsPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "accounts.json")
}

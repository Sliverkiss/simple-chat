package server

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// writeAccountsFile writes an accounts file with the given permissions
// (test helper, kept for the server-layer tests that exercise file loading).
func writeAccountsFile(path string, accounts []upstream.Account, mode os.FileMode) error {
	raw, err := json.Marshal(struct {
		Accounts []upstream.Account `json:"accounts"`
	}{Accounts: accounts})
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, mode)
}

// jsonStoreFor wraps accountstore.NewJSONStore for server tests: the JSON
// store is the default persistence sink behind Config.ParkStore.
func jsonStoreFor(path string) accountstore.Store {
	return accountstore.NewJSONStore(path, nil)
}

// mustLoadAccounts loads a valid accounts file through the store (helper for
// NewServer configs in tests).
func mustLoadAccounts(t *testing.T, path string) []upstream.Account {
	t.Helper()
	accounts, err := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return accounts
}

// loadAccounts loads an accounts file through the JSON store (validation
// surface unchanged from the old server.LoadAccounts).
func loadAccounts(t *testing.T, path string) ([]upstream.Account, error) {
	t.Helper()
	return accountstore.NewJSONStore(path, nil).Load(context.Background())
}

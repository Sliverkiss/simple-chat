// Store-level contract for admin account management: upload persists full
// records keyed by identity (mobile, else email), delete removes the record
// from both stores. No live provider is contacted — the Redis path runs
// against the in-test RESP server.
package accountstore

import (
	"context"
	"testing"

	"simple-chat/internal/upstream"
)

// (identity keying) an email-only account roundtrips through the JSON store
// and stays distinguishable from mobile accounts.
func TestJSONStoreEmailAccountRoundtrip(t *testing.T) {
	path := tempAccountsPath(t)
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Email: "web@example.com", Password: "pw", Channel: "web", DeviceID: "B-shumei"},
	})
	store := NewJSONStore(path, nil)
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d accounts, want 2", len(loaded))
	}
	var emailAcct *upstream.Account
	for i := range loaded {
		if loaded[i].Email == "web@example.com" {
			emailAcct = &loaded[i]
		}
	}
	if emailAcct == nil || emailAcct.Password != "pw" || emailAcct.DeviceID != "B-shumei" || emailAcct.Channel != "web" {
		t.Fatalf("email account did not roundtrip: %+v", emailAcct)
	}
	if got := emailAcct.Identity(); got != "web@example.com" {
		t.Fatalf("Identity() = %q, want the email", got)
	}
}

// (identity keying) saving an email-only account upserts the matching email
// row (not the mobile="" rows) and keeps the file loadable.
func TestJSONStoreSaveEmailAccountUpserts(t *testing.T) {
	path := tempAccountsPath(t)
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Email: "web@example.com", Password: "pw", DeviceID: "B-old"},
	})
	store := NewJSONStore(path, nil)
	if err := store.SaveAccount(context.Background(), upstream.Account{Email: "web@example.com", Password: "pw2", DeviceID: "B-new"}); err != nil {
		t.Fatalf("SaveAccount: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d accounts, want 2 (upsert, not append)", len(loaded))
	}
	for _, a := range loaded {
		if a.Email == "web@example.com" && (a.Password != "pw2" || a.DeviceID != "B-new") {
			t.Fatalf("email row not upserted: %+v", a)
		}
	}
}

// (delete) DeleteAccount removes a mobile-keyed account from the file.
func TestJSONStoreDeleteAccountMobile(t *testing.T) {
	path := tempAccountsPath(t)
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
	})
	store := NewJSONStore(path, nil)
	if err := store.DeleteAccount(context.Background(), "100"); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Mobile != "101" {
		t.Fatalf("after delete: %+v", loaded)
	}
}

// (delete) DeleteAccount removes an email-keyed account, and deleting an
// unknown id reports not-found without touching the file.
func TestJSONStoreDeleteAccountEmailAndUnknown(t *testing.T) {
	path := tempAccountsPath(t)
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Email: "web@example.com", Password: "pw", Channel: "web", DeviceID: "B"},
	})
	store := NewJSONStore(path, nil)
	if err := store.DeleteAccount(context.Background(), "web@example.com"); err != nil {
		t.Fatalf("DeleteAccount(email): %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Mobile != "100" {
		t.Fatalf("after email delete: %+v", loaded)
	}
	if err := store.DeleteAccount(context.Background(), "999"); err != ErrAccountNotFound {
		t.Fatalf("DeleteAccount(unknown) = %v, want ErrAccountNotFound", err)
	}
	loaded, err = store.Load(context.Background())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("unknown delete changed the file: %v %v", loaded, err)
	}
}

// (delete) both stores agree: a deleted account is absent from Load on the
// JSON store and the Redis store alike.
func TestBothStoresDeleteRoundtrip(t *testing.T) {
	// JSON
	path := tempAccountsPath(t)
	writeTestAccounts(t, path, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
	})
	jsonStore := NewJSONStore(path, nil)
	if err := jsonStore.DeleteAccount(context.Background(), "100"); err != nil {
		t.Fatalf("json DeleteAccount: %v", err)
	}
	loaded, err := jsonStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Mobile != "101" {
		t.Fatalf("json after delete: %+v", loaded)
	}

	// Redis (in-test RESP server)
	redisStore, _, _ := redisTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
	})
	if err := redisStore.DeleteAccount(context.Background(), "100"); err != nil {
		t.Fatalf("redis DeleteAccount: %v", err)
	}
	loaded, err = redisStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Mobile != "101" {
		t.Fatalf("redis after delete: %+v", loaded)
	}
}

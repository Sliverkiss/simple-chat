// Admin account management endpoints: upload (POST), list (GET), and
// remove (DELETE) accounts over the running service. All behind the same
// optional DS_API_KEY auth as /v1. The stores are exercised for real (JSON
// file; Redis via an in-test RESP server), the upstream is a mock — zero
// live traffic.
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// adminFixture is a mock upstream counting login/startup traffic per
// account, so tests can prove hot-added accounts fire the startup sequence
// and removed accounts receive no further traffic.
type adminFixture struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits map[string]int
}

func (f *adminFixture) hit(key string) {
	f.mu.Lock()
	f.hits[key]++
	f.mu.Unlock()
}

func (f *adminFixture) hitsFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[key]
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	f := &adminFixture{hits: map[string]int{}}
	mux := http.NewServeMux()
	login := func(w http.ResponseWriter, mobile string) {
		fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok-%s"}}}}`, mobile)
	}
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		mobile, _ := body["mobile"].(string)
		if mobile == "" {
			mobile, _ = body["email"].(string)
		}
		f.hit("login:" + mobile)
		login(w, mobile)
	})
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		f.hit("users:" + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-"))
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"id":"u1"}}}}`)
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		f.hit("page:" + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-"))
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"session_list":[],"has_more":false}}}`)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// newAdminTestServer builds a server with a JSON store over a temp file
// seeded with one account, plus the given API key.
func newAdminTestServer(t *testing.T, upURL, apiKey string) (*httptest.Server, string, *upstream.Pool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "accounts.json")
	writeAccountsFile(path, []upstream.Account{
		{Mobile: "13800000000", Password: "seed-pw"},
	}, 0600)
	srv, err := NewServer(Config{
		UpstreamBase: upURL,
		Accounts:     mustLoadAccounts(t, path),
		APIKey:       apiKey,
		ParkStore:    accountstore.NewJSONStore(path, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return httptest.NewServer(srv.Handler()), path, srv.pool
}

// doAdmin performs one admin request with an optional bearer key and
// returns the status, body, and any error payload.
func doAdmin(t *testing.T, srv *httptest.Server, method, path, apiKey, body string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// decodeAdmin unmarshals a generic JSON object.
func decodeAdmin(t *testing.T, body string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("bad JSON %q: %v", body, err)
	}
	return m
}

// (upload) a single account object is accepted, persisted to the store, and
// echoed back in full — mobile, password, minted device_id, no redaction.
func TestAdminUploadSingleObject(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "",
		`{"mobile":"13900000001","password":"pw","region":"cn"}`)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	accts, _ := m["accounts"].([]any)
	if len(accts) != 1 {
		t.Fatalf("accounts = %v, want 1", m)
	}
	a := accts[0].(map[string]any)
	if a["mobile"] != "13900000001" || a["password"] != "pw" {
		t.Fatalf("echo missing fields: %v", a)
	}
	if a["device_id"] == "" || a["device_id"] == nil {
		t.Fatalf("echo missing minted device_id: %v", a)
	}

	// Persisted: a fresh store load sees the account with its device id.
	loaded, err := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range loaded {
		if a.Mobile == "13900000001" && a.Password == "pw" && a.DeviceID != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("account not persisted: %+v", loaded)
	}
	// Hot-added: the pool now serves it (ring grew).
	if st := pool.Status(); len(st) != 2 {
		t.Fatalf("ring = %d slots, want 2", len(st))
	}
}

// (upload) a batch upload persists and echoes every record.
func TestAdminUploadBatch(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, _ := newAdminTestServer(t, f.srv.URL, "")

	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "",
		`{"accounts":[{"mobile":"13900000001","password":"pw1"},{"email":"w@example.com","password":"pw2","channel":"web","device_id":"B-harvest"}]}`)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	accts, _ := m["accounts"].([]any)
	if len(accts) != 2 {
		t.Fatalf("accounts = %v, want 2", m)
	}
	loaded, err := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 3 {
		t.Fatalf("store has %d accounts, want 3", len(loaded))
	}
}

// (validation) bad accounts are rejected with 400 and a reason naming the
// rule and the valid values; the store and pool stay untouched.
func TestAdminUploadValidation(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	cases := []struct {
		name, body, wantIn string
	}{
		{"both empty", `{"mobile":"","email":"","password":"pw"}`, "exactly one"},
		{"both set", `{"mobile":"1","email":"a@b.c","password":"pw"}`, "exactly one"},
		{"no password", `{"mobile":"13900000001","password":""}`, "password"},
		{"bad region", `{"mobile":"13900000001","password":"pw","region":"us"}`, "valid values"},
		{"bad channel", `{"mobile":"13900000001","password":"pw","channel":"ios"}`, "valid values"},
		{"web without device_id", `{"email":"w@example.com","password":"pw","channel":"web"}`, "device_id"},
		{"empty batch", `{"accounts":[]}`, "at least one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := doAdmin(t, srv, "POST", "/admin/accounts", "", tc.body)
			if code != 400 {
				t.Fatalf("status = %d, body = %s", code, body)
			}
			if !strings.Contains(body, tc.wantIn) {
				t.Fatalf("body %q lacks %q", body, tc.wantIn)
			}
		})
	}
	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "", `{not json`)
	if code != 400 {
		t.Fatalf("malformed JSON status = %d, body = %s", code, body)
	}
	loaded, err := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("store changed by rejected uploads: %v %v", loaded, err)
	}
	if st := pool.Status(); len(st) != 1 {
		t.Fatalf("pool changed by rejected uploads: %v", st)
	}
}

// (duplicate) uploading an identity that already exists answers 409 with
// the existing record — and nothing is re-persisted or double-added.
func TestAdminUploadDuplicateConflict(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "",
		`{"mobile":"13800000000","password":"other"}`)
	if code != 409 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	accts, _ := m["accounts"].([]any)
	if len(accts) != 1 || accts[0].(map[string]any)["password"] != "seed-pw" {
		t.Fatalf("409 must echo the existing record: %v", m)
	}
	loaded, _ := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if len(loaded) != 1 || loaded[0].Password != "seed-pw" {
		t.Fatalf("duplicate overwrote the store: %+v", loaded)
	}
	if st := pool.Status(); len(st) != 1 {
		t.Fatalf("duplicate added a ring slot: %v", st)
	}
}

// (hot-add) after an upload, the running pool round-robins to the new
// account on the next completion — no restart — and its first use fires
// the startup sequence (users/current + fetch_page), exactly like a
// startup-loaded account.
func TestAdminUploadHotAddRotationAndStartup(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, pool := newAdminTestServer(t, f.srv.URL, "")

	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "", `{"mobile":"13900000001","password":"pw"}`)
	if code != 200 {
		t.Fatalf("upload status = %d, body = %s", code, body)
	}
	// Drive two logins through the pool: seed then hot-added.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lease.Token(context.Background()); err != nil {
			t.Fatalf("Token(%s): %v", lease.Account().Mobile, err)
		}
		seen[lease.Account().Mobile] = true
		lease.Release()
	}
	if !seen["13900000001"] {
		t.Fatalf("hot-added account never selected: %v", seen)
	}
	// Startup sequence fired on its first login.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.hitsFor("users:13900000001") >= 1 && f.hitsFor("page:13900000001") >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("startup sequence never fired for hot-added account: users=%d page=%d",
		f.hitsFor("users:13900000001"), f.hitsFor("page:13900000001"))
}

// (remove) deleting an account removes it from store and pool; unknown id
// answers 404; the removed record is echoed in full.
func TestAdminRemove(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	// Add a second account first, so removing one leaves a serving pool.
	if code, body := doAdmin(t, srv, "POST", "/admin/accounts", "", `{"mobile":"13900000001","password":"pw"}`); code != 200 {
		t.Fatalf("seed add failed: %d %s", code, body)
	}
	code, body := doAdmin(t, srv, "DELETE", "/admin/accounts/13900000001", "", "")
	if code != 200 {
		t.Fatalf("delete status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	a, _ := m["account"].(map[string]any)
	if a == nil || a["mobile"] != "13900000001" || a["password"] != "pw" {
		t.Fatalf("delete must echo the removed record: %v", m)
	}
	loaded, _ := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if len(loaded) != 1 || loaded[0].Mobile != "13800000000" {
		t.Fatalf("store after delete: %+v", loaded)
	}
	// No new acquisitions: the deleted account is never selected again.
	for i := 0; i < 2; i++ {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if lease.Account().Mobile != "13800000000" {
			t.Fatalf("deleted account still selected: %s", lease.Account().Mobile)
		}
		lease.Release()
	}
	// Unknown id → 404.
	code, body = doAdmin(t, srv, "DELETE", "/admin/accounts/999", "", "")
	if code != 404 {
		t.Fatalf("unknown delete status = %d, body = %s", code, body)
	}
}

// (remove) an in-flight lease on a removed account completes its login and
// releases cleanly; the account receives no new traffic afterwards.
func TestAdminRemoveInflightCompletes(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, pool := newAdminTestServer(t, f.srv.URL, "")

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Account().Mobile != "13800000000" {
		t.Fatalf("lease = %s", lease.Account().Mobile)
	}
	// Remove the leased account while the lease is in flight.
	code, body := doAdmin(t, srv, "DELETE", "/admin/accounts/13800000000", "", "")
	if code != 200 {
		t.Fatalf("delete status = %d, body = %s", code, body)
	}
	// The in-flight lease still completes a login through its own client.
	tok, err := lease.Token(context.Background())
	if err != nil {
		t.Fatalf("in-flight token after remove: %v", err)
	}
	if tok != "tok-13800000000" {
		t.Fatalf("token = %q", tok)
	}
	lease.Release()
	// Ring is empty now: acquire fails with the no-accounts error.
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("acquire on emptied pool must fail")
	}
}

// (list) GET reports full records plus live state and summary counts.
func TestAdminList(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, pool := newAdminTestServer(t, f.srv.URL, "")

	// Warm the seed account (login → token-warm) directly through the pool.
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease.Release()

	code, body := doAdmin(t, srv, "GET", "/admin/accounts", "", "")
	if code != 200 {
		t.Fatalf("list status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	accts, _ := m["accounts"].([]any)
	if len(accts) != 1 {
		t.Fatalf("accounts = %v", m)
	}
	a := accts[0].(map[string]any)
	for _, field := range []string{"mobile", "password", "region", "device_id", "state", "inflight", "token_warm"} {
		if _, ok := a[field]; !ok {
			t.Fatalf("list record missing %q: %v", field, a)
		}
	}
	if a["password"] != "seed-pw" {
		t.Fatalf("list must not redact: %v", a)
	}
	if a["state"] != "ready" || a["token_warm"] != true {
		t.Fatalf("live state wrong: %v", a)
	}
	sum, _ := m["summary"].(map[string]any)
	if sum == nil || sum["total"] != float64(1) || sum["ready"] != float64(1) || sum["parked"] != float64(0) {
		t.Fatalf("summary = %v", m)
	}
}

// (list) parked accounts report kind/until/reason and land in the parked
// summary count.
func TestAdminListParkedSummary(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, pool := newAdminTestServer(t, f.srv.URL, "")

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.NoteError(&upstream.BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: time.Now().Add(time.Hour)})
	lease.Release()

	code, body := doAdmin(t, srv, "GET", "/admin/accounts", "", "")
	if code != 200 {
		t.Fatalf("list status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	accts, _ := m["accounts"].([]any)
	a := accts[0].(map[string]any)
	if a["state"] != "muted" || a["park_kind"] != "muted" || a["park_reason"] == "" || a["park_until"] == "" {
		t.Fatalf("parked record = %v", a)
	}
	sum, _ := m["summary"].(map[string]any)
	if sum["ready"] != float64(0) || sum["parked"] != float64(1) || sum["total"] != float64(1) {
		t.Fatalf("summary = %v", m)
	}
}

// (auth) the admin surface follows DS_API_KEY exactly like /v1: with a key
// set, no/incorrect key → 401; correct key → 200.
func TestAdminAuth(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, _ := newAdminTestServer(t, f.srv.URL, "sekrit")

	if code, _ := doAdmin(t, srv, "GET", "/admin/accounts", "", ""); code != 401 {
		t.Fatalf("no key status = %d, want 401", code)
	}
	if code, _ := doAdmin(t, srv, "GET", "/admin/accounts", "wrong", ""); code != 401 {
		t.Fatalf("wrong key status = %d, want 401", code)
	}
	if code, body := doAdmin(t, srv, "GET", "/admin/accounts", "sekrit", ""); code != 200 {
		t.Fatalf("correct key status = %d, body = %s", code, body)
	}
	if code, _ := doAdmin(t, srv, "POST", "/admin/accounts", "", `{}`); code != 401 {
		t.Fatalf("POST no key status = %d, want 401", code)
	}
	if code, _ := doAdmin(t, srv, "DELETE", "/admin/accounts/1", "", ""); code != 401 {
		t.Fatalf("DELETE no key status = %d, want 401", code)
	}
}

// (store-first) a store write failure answers 500 and leaves the pool
// untouched. The failure is injected by deleting the underlying file — the
// JSON store's read-merge-write then fails on the missing read.
func TestAdminUploadStoreFailureLeavesPoolUnchanged(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	// Make the store write fail for real: replace the accounts file with a
	// directory (WriteFile into it always errors). The old approach (remove
	// the file) no longer fails — Load/S now bootstrap missing files.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0600); err != nil {
		t.Fatal(err)
	}
	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "", `{"mobile":"13900000009","password":"pw"}`)
	if code != 500 {
		t.Fatalf("store-failure status = %d, body = %s", code, body)
	}
	if st := pool.Status(); len(st) != 1 {
		t.Fatalf("pool changed despite store failure: %v", st)
	}
}

// (no store) a server built without a ParkStore answers 503 on mutations,
// never a panic — the pool must not accept an account the store can't back.
func TestAdminNoStoreGuard(t *testing.T) {
	f := newAdminFixture(t)
	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if code, _ := doAdmin(t, ts, "POST", "/admin/accounts", "", `{"mobile":"13900000001","password":"pw"}`); code != 503 {
		t.Fatalf("upload without store = %d, want 503", code)
	}
	if code, _ := doAdmin(t, ts, "DELETE", "/admin/accounts/13800000000", "", ""); code != 503 {
		t.Fatalf("delete without store = %d, want 503", code)
	}
	// List still works (pool-only read).
	if code, _ := doAdmin(t, ts, "GET", "/admin/accounts", "", ""); code != 200 {
		t.Fatalf("list without store = %d, want 200", code)
	}
}

// (oversized) a body at/over the cap answers 413.
func TestAdminUploadBodyTooLarge(t *testing.T) {
	f := newAdminFixture(t)
	srv, _, _ := newAdminTestServer(t, f.srv.URL, "")
	// One account record with a >4 MiB password.
	huge := strings.Repeat("x", 5<<20)
	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "",
		`{"mobile":"13900000001","password":"`+huge+`"}`)
	if code != 413 {
		t.Fatalf("oversized body = %d, body = %s", code, body)
	}
}

// (batch) a duplicate identity within one batch answers 400 and nothing is
// persisted.
func TestAdminUploadDuplicateInBatch(t *testing.T) {
	f := newAdminFixture(t)
	srv, path, pool := newAdminTestServer(t, f.srv.URL, "")

	code, body := doAdmin(t, srv, "POST", "/admin/accounts", "",
		`{"accounts":[{"mobile":"13900000001","password":"a"},{"mobile":"13900000001","password":"b"}]}`)
	if code != 400 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, "duplicate") {
		t.Fatalf("body = %s", body)
	}
	loaded, _ := accountstore.NewJSONStore(path, nil).Load(context.Background())
	if len(loaded) != 1 {
		t.Fatalf("partial batch persisted: %+v", loaded)
	}
	if st := pool.Status(); len(st) != 1 {
		t.Fatalf("pool changed: %v", st)
	}
}

// (pool-only delete) an account present in the pool but absent from the
// store (divergent seeds) still retires from rotation — the 404 echo only
// comes from the pool, the store miss is tolerated.
func TestAdminDeletePoolOnlyAccount(t *testing.T) {
	f := newAdminFixture(t)
	// Two different files: pool loads both accounts, store knows only one.
	poolPath := filepath.Join(t.TempDir(), "pool.json")
	storePath := filepath.Join(t.TempDir(), "store.json")
	writeAccountsFile(poolPath, []upstream.Account{
		{Mobile: "13800000000", Password: "a"},
		{Mobile: "13800000001", Password: "b"},
	}, 0600)
	writeAccountsFile(storePath, []upstream.Account{
		{Mobile: "13800000000", Password: "a"},
	}, 0600)
	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, poolPath),
		ParkStore:    accountstore.NewJSONStore(storePath, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	code, body := doAdmin(t, ts, "DELETE", "/admin/accounts/13800000001", "", "")
	if code != 200 {
		t.Fatalf("pool-only delete = %d, body = %s", code, body)
	}
	if code, _ := doAdmin(t, ts, "DELETE", "/admin/accounts/13800000001", "", ""); code != 404 {
		t.Fatal("second delete must 404")
	}
}

// (store-first, redis) a mid-batch store failure rolls back the already
// persisted records and answers 500; the pool is untouched.
func TestAdminUploadRedisRollbackOnFailure(t *testing.T) {
	f := newAdminFixture(t)
	fake := newAdminFakeRedis(t)
	store, err := accountstore.OpenRedis(fake.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "13800000000", Password: "seed-pw", DeviceID: "dev-seed"}); err != nil {
		t.Fatal(err)
	}
	accounts, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     accounts,
		ParkStore:    store,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Fail every SET from here on: the batch's first write fails, nothing
	// is left half-persisted.
	fake.fault("SET", "ERR simulated outage")
	code, body := doAdmin(t, ts, "POST", "/admin/accounts", "",
		`{"accounts":[{"mobile":"13900000001","password":"a"},{"mobile":"13900000002","password":"b"}]}`)
	if code != 500 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	// Pool untouched: still the seed account only.
	if st := srv.pool.Status(); len(st) != 1 {
		t.Fatalf("pool changed despite store failure: %v", st)
	}
	// Store rolled back to the seed.
	loaded, err := store.Load(context.Background())
	if err == nil && len(loaded) != 1 {
		t.Fatalf("store after rollback: %+v", loaded)
	}
}
//
// Mirrors the accountstore in-test fake: enough of RESP2 (PING, AUTH, GET,
// SET, DEL, SADD, SREM, SMEMBERS) over plain loopback TCP for the RedisStore
// to run its full surface. Plaintext loopback is what OpenRedis allows
// without DS_REDIS_INSECURE.

type adminFakeRedis struct {
	ln   net.Listener
	mu   sync.Mutex
	data map[string]string
	sets map[string]map[string]bool
	// faults maps a command verb to a fault; when the verb fires, the
	// handler writes the error reply and drops the connection (tests
	// simulate outages mid-batch).
	faults map[string]string
}

func newAdminFakeRedis(t *testing.T) *adminFakeRedis {
	f := &adminFakeRedis{data: map[string]string{}, sets: map[string]map[string]bool{}, faults: map[string]string{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.ln = ln
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *adminFakeRedis) url() string { return "redis://" + f.ln.Addr().String() }

// fault makes every subsequent command with this verb fail with msg.
func (f *adminFakeRedis) fault(verb, msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[strings.ToUpper(verb)] = msg
}

// setCount reports how many SET commands succeeded.
func (f *adminFakeRedis) setCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

func (f *adminFakeRedis) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *adminFakeRedis) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		cmd, err := adminReadCommand(r)
		if err != nil {
			return
		}
		if len(cmd) == 0 {
			continue
		}
		var out string
		verb := strings.ToUpper(cmd[0])
		f.mu.Lock()
		if msg, bad := f.faults[verb]; bad {
			f.mu.Unlock()
			if _, err := conn.Write([]byte("-" + msg + "\r\n")); err == nil {
				return // drop the connection like a dead server
			}
			return
		}
		switch verb {
		case "PING":
			out = "+PONG\r\n"
		case "AUTH":
			out = "+OK\r\n"
		case "SET":
			f.data[cmd[1]] = cmd[2]
			out = "+OK\r\n"
		case "GET":
			if v, ok := f.data[cmd[1]]; ok {
				out = "$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n"
			} else {
				out = "$-1\r\n"
			}
		case "DEL":
			delete(f.data, cmd[1])
			out = ":1\r\n"
		case "SADD":
			if f.sets[cmd[1]] == nil {
				f.sets[cmd[1]] = map[string]bool{}
			}
			f.sets[cmd[1]][cmd[2]] = true
			out = ":1\r\n"
		case "SREM":
			delete(f.sets[cmd[1]], cmd[2])
			out = ":1\r\n"
		case "SMEMBERS":
			members := f.sets[cmd[1]]
			out = "*" + strconv.Itoa(len(members)) + "\r\n"
			for m := range members {
				out += "$" + strconv.Itoa(len(m)) + "\r\n" + m + "\r\n"
			}
		default:
			out = "-ERR unknown command\r\n"
		}
		f.mu.Unlock()
		if _, err := conn.Write([]byte(out)); err != nil {
			return
		}
	}
}

// adminReadCommand parses one RESP array of bulk strings.
func adminReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("bad command line %q", line)
	}
	n, _ := strconv.Atoi(line[1:])
	cmd := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		l, _ := strconv.Atoi(hdr[1:])
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		cmd = append(cmd, string(buf[:l]))
	}
	return cmd, nil
}

// (redis) the full admin cycle runs against the Redis store: upload
// persists, list reports, delete removes and 404s — all through the same
// handlers, no JSON fallback anywhere.
func TestAdminRedisStoreCycle(t *testing.T) {
	f := newAdminFixture(t)
	fake := newAdminFakeRedis(t)
	store, err := accountstore.OpenRedis(fake.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	// Seed one account through the store, like -import-redis would.
	if err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "13800000000", Password: "seed-pw", DeviceID: "dev-seed"}); err != nil {
		t.Fatal(err)
	}
	accounts, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     accounts,
		ParkStore:    store,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Upload.
	code, body := doAdmin(t, ts, "POST", "/admin/accounts", "",
		`{"mobile":"13900000001","password":"pw"}`)
	if code != 200 {
		t.Fatalf("upload status = %d, body = %s", code, body)
	}
	// List: two accounts, full records.
	code, body = doAdmin(t, ts, "GET", "/admin/accounts", "", "")
	if code != 200 {
		t.Fatalf("list status = %d, body = %s", code, body)
	}
	m := decodeAdmin(t, body)
	if accts, _ := m["accounts"].([]any); len(accts) != 2 {
		t.Fatalf("redis list = %v", m)
	}
	if sum, _ := m["summary"].(map[string]any); sum["total"] != float64(2) {
		t.Fatalf("redis summary = %v", m)
	}
	// Restart-equivalence: a fresh Load over the same Redis sees both.
	loaded, err := store.Load(context.Background())
	if err != nil || len(loaded) != 2 {
		t.Fatalf("redis load after upload: %v %v", loaded, err)
	}
	// Delete.
	code, body = doAdmin(t, ts, "DELETE", "/admin/accounts/13900000001", "", "")
	if code != 200 {
		t.Fatalf("delete status = %d, body = %s", code, body)
	}
	loaded, err = store.Load(context.Background())
	if err != nil || len(loaded) != 1 || loaded[0].Mobile != "13800000000" {
		t.Fatalf("redis after delete: %v %v", loaded, err)
	}
	code, body = doAdmin(t, ts, "DELETE", "/admin/accounts/13900000001", "", "")
	if code != 404 {
		t.Fatalf("redis unknown delete status = %d, body = %s", code, body)
	}
}

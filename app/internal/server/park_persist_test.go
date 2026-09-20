// Park-state persistence across restarts at the server layer (TASK_MUTE):
// accounts.json is the account store; these tests pin the full round trip —
// park writes the fields, restart restores the park (zero upstream traffic),
// natural unpark clears the fields, banned survives forever.
package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// muteFixture answers login for every account and — when muteMobile is set —
// answers every completion for that one account with a biz 5 mute carrying a
// mute_until, so a test can drive a live park through the real pipeline.
// Every request is counted per mobile; completions are counted separately.
type muteFixture struct {
	srv *httptest.Server

	muteMobile string

	mu          sync.Mutex
	counts      map[string]int
	completions map[string]int
}

func (f *muteFixture) hit(mobile string) {
	f.mu.Lock()
	f.counts[mobile]++
	f.mu.Unlock()
}

func (f *muteFixture) hits(mobile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[mobile]
}

func newMuteFixture(t *testing.T, muteMobile string) *muteFixture {
	t.Helper()
	f := &muteFixture{counts: map[string]int{}, completions: map[string]int{}, muteMobile: muteMobile}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		mobile, _ := body["mobile"].(string)
		f.hit(mobile)
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"user": map[string]any{"token": "tok-" + mobile},
			},
		}})
	})

	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-")
		f.hit(token)
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"chat_session": map[string]any{"id": "sess-" + token},
			},
		}})
	})

	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": nil,
		}})
	})

	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"challenge": solvableChallenge(r),
			},
		}})
	})

	// Every completion against the mute target answers biz 5 with a
	// mute_until — a parked account must never reach here again (a correct
	// implementation parks it after the first hit); anything else serves.
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer tok-")
		f.hit(token)
		f.mu.Lock()
		f.completions[token]++
		f.mu.Unlock()
		if f.muteMobile != "" && token == f.muteMobile {
			until := time.Now().Add(30 * time.Minute).Unix()
			writeJSON(w, map[string]any{"code": 40300, "msg": "user muted", "data": map[string]any{
				"biz_code": 5, "biz_msg": "user is muted",
				"biz_data": map[string]any{"mute_until": until},
			}})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"ok\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// accountsFileAt writes a two-account file (0600) into t.TempDir and returns
// its path. Both accounts share a password; only the first is parked.
func accountsFileAt(t *testing.T, park *upstream.Account) string {
	t.Helper()
	accounts := []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw"},
	}
	if park != nil {
		accounts[0] = *park
	}
	path := filepath.Join(t.TempDir(), "accounts.json")
	if err := writeAccountsFile(path, accounts, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// readAccountsFile decodes the accounts file at path.
func readAccountsFile(t *testing.T, path string) []upstream.Account {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var af struct {
		Accounts []upstream.Account `json:"accounts"`
	}
	if err := json.Unmarshal(raw, &af); err != nil {
		t.Fatal(err)
	}
	return af.Accounts
}

// postChat sends one minimal chat completion and returns (status, body).
func postHi(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// (a → full round trip) a live park lands in accounts.json with mute_until.
func TestMuteParkPersistsToAccountsFile(t *testing.T) {
	f := newMuteFixture(t, "100")
	path := accountsFileAt(t, nil)

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// First request: round-robin lands on account "100", which gets muted.
	status, _ := postHi(t, ts.URL)
	if status != http.StatusTooManyRequests {
		t.Fatalf("first request status = %d, want 429 (muted)", status)
	}

	accts := readAccountsFile(t, path)
	if got := accts[0].ParkKind; got != "muted" {
		t.Fatalf("park_kind = %q, want muted (file: %+v)", got, accts[0])
	}
	if accts[0].ParkUntil == "" {
		t.Fatal("park_until missing from accounts file")
	}
	if _, err := time.Parse(time.RFC3339, accts[0].ParkUntil); err != nil {
		t.Errorf("park_until %q is not RFC3339: %v", accts[0].ParkUntil, err)
	}
	if !strings.Contains(accts[0].ParkReason, "muted") {
		t.Errorf("park_reason = %q, want it to mention the mute", accts[0].ParkReason)
	}
	if accts[1].ParkKind != "" {
		t.Errorf("healthy account got parked: %+v", accts[1])
	}
}

// (b) restart with a future mute_until in the file: the account is out of
// rotation AND receives zero upstream calls (no login, no session, nothing).
func TestRestartKeepsParkedAccountFrozen(t *testing.T) {
	f := newMuteFixture(t, "")
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	path := accountsFileAt(t, &upstream.Account{
		Mobile: "100", Password: "pw",
		ParkKind: "muted", ParkUntil: until, ParkReason: "user is muted",
	})

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for i := 0; i < 4; i++ {
		status, body := postHi(t, ts.URL)
		if status != http.StatusOK {
			t.Fatalf("request %d: status = %d body = %s (healthy account must serve)", i, status, body)
		}
	}
	if n := f.hits("100"); n != 0 {
		t.Fatalf("parked account received %d upstream requests after restart, want 0", n)
	}
	if n := f.hits("101"); n == 0 {
		t.Fatal("healthy account never served")
	}
}

// (c) restart with a past mute_until: normal rotation, both accounts serve.
func TestRestartExpiredParkRotatesNormally(t *testing.T) {
	f := newMuteFixture(t, "")
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	path := accountsFileAt(t, &upstream.Account{
		Mobile: "100", Password: "pw",
		ParkKind: "muted", ParkUntil: past, ParkReason: "stale mute",
	})

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for i := 0; i < 4; i++ {
		status, body := postHi(t, ts.URL)
		if status != http.StatusOK {
			t.Fatalf("request %d: status = %d body = %s", i, status, body)
		}
	}
	if f.hits("100") == 0 {
		t.Fatal("expired-park account never served")
	}
	if f.hits("101") == 0 {
		t.Fatal("healthy account never served")
	}
}

// (d) banned persists forever: restart keeps it frozen; manual revive (field
// removal) brings it back.
func TestRestartBannedStaysForever(t *testing.T) {
	f := newMuteFixture(t, "")
	path := accountsFileAt(t, &upstream.Account{
		Mobile: "100", Password: "pw",
		ParkKind: "banned", ParkReason: "account banned upstream",
	})

	for round := 0; round < 2; round++ {
		srv, err := NewServer(Config{
			UpstreamBase: f.srv.URL,
			Accounts:     mustLoadAccounts(t, path),
			ParkStore:    jsonStoreFor(path),
		})
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(srv.Handler())
		for i := 0; i < 3; i++ {
			status, body := postHi(t, ts.URL)
			if status != http.StatusOK {
				t.Fatalf("round %d request %d: status = %d body = %s", round, i, status, body)
			}
		}
		ts.Close()
		if n := f.hits("100"); n != 0 {
			t.Fatalf("round %d: banned account received %d upstream requests, want 0", round, n)
		}
	}

	// Manual revive: strip the park fields, restart, account serves again.
	accts := readAccountsFile(t, path)
	accts[0].ParkKind = ""
	accts[0].ParkUntil = ""
	accts[0].ParkReason = ""
	if err := writeAccountsFile(path, accts, 0600); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	for i := 0; i < 3; i++ {
		status, body := postHi(t, ts.URL)
		if status != http.StatusOK {
			t.Fatalf("revived round request %d: status = %d body = %s", i, status, body)
		}
	}
	if f.hits("100") == 0 {
		t.Fatal("revived account never served")
	}
}

// (e) natural unpark clears the persisted fields from the file.
func TestNaturalUnparkClearsFile(t *testing.T) {
	f := newMuteFixture(t, "")
	until := time.Now().Add(150 * time.Millisecond).UTC().Format(time.RFC3339)
	path := accountsFileAt(t, &upstream.Account{
		Mobile: "100", Password: "pw",
		ParkKind: "muted", ParkUntil: until, ParkReason: "user is muted",
	})

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// While parked, requests fall through to account 101.
	postHi(t, ts.URL)

	// The park window passes; a later request lazily unparks account 100...
	time.Sleep(300 * time.Millisecond)
	status, body := postHi(t, ts.URL)
	if status != http.StatusOK {
		t.Fatalf("post-window request: status = %d body = %s", status, body)
	}
	// ...and the natural unpark clears the persisted fields.
	deadline := time.Now().Add(2 * time.Second)
	for {
		accts := readAccountsFile(t, path)
		if accts[0].ParkKind == "" && accts[0].ParkUntil == "" && accts[0].ParkReason == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("persisted park fields never cleared: %+v", accts[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// (f) concurrent parks over one shared accounts file stay consistent (atomic
// write; run under -race).
func TestConcurrentParksDoNotCorruptFile(t *testing.T) {
	f := newMuteFixture(t, "")
	path := accountsFileAt(t, nil)

	srv, err := NewServer(Config{
		UpstreamBase: f.srv.URL,
		Accounts:     mustLoadAccounts(t, path),
		ParkStore:    jsonStoreFor(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
					strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
				if err != nil {
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// The file must still decode cleanly with both accounts present.
	accts := readAccountsFile(t, path)
	if len(accts) != 2 {
		t.Fatalf("accounts after concurrent parks = %d, want 2", len(accts))
	}
}

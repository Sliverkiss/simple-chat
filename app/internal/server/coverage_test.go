package server

import (
	"os"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

func TestLoadAccountsValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "accounts.json")

	// Valid multi-account file with region.
	writeAccountsFile(path, []upstream.Account{
		{Mobile: "1", Password: "p", Region: "cn"},
		{Mobile: "2", Password: "p"},
	}, 0600)
	accs, err := loadAccounts(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(accs) != 2 {
		t.Fatalf("loaded %d accounts, want 2", len(accs))
	}

	// Unknown region rejected at load.
	writeAccountsFile(path, []upstream.Account{{Mobile: "1", Password: "p", Region: "mars"}}, 0600)
	if _, err := loadAccounts(t, path); err == nil || !strings.Contains(err.Error(), "region") {
		t.Fatalf("want region error, got %v", err)
	}

	// Missing password rejected.
	writeAccountsFile(path, []upstream.Account{{Mobile: "1"}}, 0600)
	if _, err := loadAccounts(t, path); err == nil {
		t.Fatal("want error for missing password")
	}

	// Empty file: valid state (fresh cloud deployment; accounts come via
	// the admin API). loadAccounts returns zero accounts, no error.
	writeAccountsFile(path, nil, 0600)
	accs, err = loadAccounts(t, path)
	if err != nil {
		t.Fatalf("empty accounts should be valid, got %v", err)
	}
	if len(accs) != 0 {
		t.Fatalf("want 0 accounts, got %d", len(accs))
	}

	// Missing file: created as an empty store (cloud container, no volume).
	missing := filepath.Join(dir, "fresh.json")
	if _, err := loadAccounts(t, missing); err != nil {
		t.Fatalf("missing file should bootstrap an empty store, got %v", err)
	}
	if _, err := os.Stat(missing); err != nil {
		t.Fatalf("expected %s to be created", missing)
	}
}

// TestPoolErrorMapping proves the pool-level errors reach clients as 429/503.
func TestPoolErrorMapping(t *testing.T) {
	// Build a pool, ban the only account, drive a request through it.
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv, err := NewServer(Config{
		UpstreamBase: up.srv.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()

	// Ban the only account directly through the pool.
	lease, err := srv.pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.NoteError(&upstream.BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
	lease.Release()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 503 (all banned), body = %s", resp.StatusCode, body)
	}
	var got struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	if got.Error.Code != "no_accounts" {
		t.Errorf("code = %q, want no_accounts", got.Error.Code)
	}
}

// TestPoolBusyMapsTo429 drives the all-busy path: one account with a cap of
// one, the slot held by a first request, a second request must 429.
func TestPoolBusyMapsTo429(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv, err := NewServer(Config{
		UpstreamBase: up.srv.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
		MaxInflight:  1,
		QueueWait:    50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Hold the only slot.
	lease, err := srv.pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 429 (pool busy), body = %s", resp.StatusCode, body)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
}

// TestPoolStatusReportsStates proves the debug status shape.
func TestPoolStatusReportsStates(t *testing.T) {
	pool, err := upstream.NewPool([]upstream.Account{
		{Mobile: "13800000000", Password: "pw"},
		{Mobile: "13900000000", Password: "pw"},
	}, upstream.PoolConfig{MaxInflight: 3})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := pool.Acquire(context.Background())
	l.NoteError(&upstream.BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"})
	l.Release()
	st := pool.Status()
	if len(st) != 2 {
		t.Fatalf("status rows = %d, want 2", len(st))
	}
	found := map[string]string{}
	for _, row := range st {
		found[row["mobile"].(string)] = row["state"].(string)
	}
	if found["13800000000"] != "risk" {
		t.Errorf("first account state = %q, want risk", found["13800000000"])
	}
	if found["13900000000"] != "ready" {
		t.Errorf("second account state = %q, want ready", found["13900000000"])
	}
}

// TestUpstreamErrorMatrix covers writeUpstreamError branches end to end:
// banned → 403, muted → 429, auth → 401, plain → 502.
func TestUpstreamErrorMatrix(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()

	cases := []struct {
		name   string
		biz    int
		msg    string
		status int
		code   string
	}{
		{"banned", 10, "USER_IS_BANNED", 502, "account_banned"},
		{"muted", 5, "user is muted", 429, "account_muted"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// One-shot upstream that fails session creation with this biz code.
			ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v0/users/login":
					writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok"}}}})
				case "/api/v0/chat_session/create":
					writeJSON(w, mc{"code": 0, "data": mc{"biz_code": c.biz, "biz_msg": c.msg}})
				default:
					writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
				}
			}))
			defer ups.Close()
			srv, err := NewServer(Config{
				UpstreamBase: ups.URL,
				Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
				QueueWait:    10 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			gw := httptest.NewServer(srv.Handler())
			defer gw.Close()
			resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.status {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, c.status, body)
			}
			var got struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			json.NewDecoder(resp.Body).Decode(&got)
			if got.Error.Code != c.code {
				t.Errorf("code = %q, want %q", got.Error.Code, c.code)
			}
		})
	}
}

// TestStreamedBannedSurfacesError proves stream-mode errors map too. The
// hint error arrives before the first client byte, so the (uncommitted)
// response is a real HTTP error — strictly better than an SSE stream whose
// first frame is an error.
func TestStreamedBannedSurfacesError(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/users/login":
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok"}}}})
		case "/api/v0/chat_session/create":
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"chat_session": mc{"id": "s1"}}}})
		case "/api/v0/chat/create_pow_challenge":
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"challenge": solvableChallenge(r)}}})
		case "/api/v0/chat/completion":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: hint\ndata: {\"type\":\"error\",\"content\":\"banned midstream\",\"finish_reason\":\"other\"}\n\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		default:
			writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
		}
	}))
	defer ups.Close()
	srv, err := NewServer(Config{
		UpstreamBase: ups.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 for terminal pre-commit stream error, body = %s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "upstream_error") {
		t.Errorf("body missing upstream error shape: %s", raw)
	}
}

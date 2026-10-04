package server

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// OP-02/06 exercise the real HTTP handler against a local upstream. A held
// lease pins capacity without depending on the pool's randomized selection.
func op0206Gateway(t *testing.T, f *switchFixture, accounts []upstream.Account) (*Server, *httptest.Server) {
	t.Helper()
	s, err := NewServer(Config{UpstreamBase: f.server.URL, Accounts: accounts, MaxInflight: 1,
		QueueWait: 40 * time.Millisecond, Logger: log.New(io.Discard, "", 0), RandomSeed: 1})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(s.Handler())
	t.Cleanup(func() { gw.Close(); s.Shutdown() })
	return s, gw
}

func op0206Response(t *testing.T, gw *httptest.Server) (int, string, string, time.Duration) {
	t.Helper()
	start := time.Now()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(switchRequest))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body), resp.Header.Get("Retry-After"), time.Since(start)
}

func TestOP02HealthySpareServesWhileOtherIdentityIsAtCapacity(t *testing.T) {
	f := newSwitchFixture(t, "")
	s, gw := op0206Gateway(t, f, switchAccounts())
	lease, _, err := s.pool.AcquireWithWait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	busyIdentity := lease.Account().Identity()
	status, body, _, elapsed := op0206Response(t, gw)
	if status != http.StatusOK || !strings.Contains(body, "switched-answer") || elapsed > time.Second {
		t.Fatalf("healthy spare: status=%d body=%s elapsed=%s", status, body, elapsed)
	}
	calls := assertSwitchCalls(t, f, 1)
	if calls[0].identity == busyIdentity {
		t.Errorf("leased busy identity served request: %+v", calls)
	}
}

func TestOP02PreByteRetryWaitsForBusyAlternativeWithoutReplayingFirstIdentity(t *testing.T) {
	f := newSwitchFixture(t, "http500")
	s, gw := op0206Gateway(t, f, switchAccounts())
	// First identity is not predetermined by score. After the first failure,
	// both slots can be held deterministically only by occupying one identity
	// and making the other the unique initial candidate. The retry must wait
	// for the held alternative, not fall back to the failed identity.
	held, _, err := s.pool.AcquireWithWait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	status, body, retryAfter, elapsed := op0206Response(t, gw)
	if status != http.StatusTooManyRequests || !strings.Contains(body, `"code":"pool_busy"`) || retryAfter == "" || elapsed > time.Second {
		t.Fatalf("busy alternative: status=%d body=%s retry=%q elapsed=%s", status, body, retryAfter, elapsed)
	}
	calls, _ := f.snapshot()
	if len(calls) != 1 || calls[0].identity == held.Account().Identity() {
		t.Fatalf("expected one failed completion on unheld identity, got %+v", calls)
	}
}

// A discovered mute/ban is identity-specific: don't immediately probe another
// account in the same request. A transport failure remains safely retryable.
func TestOP06DiscoveredMuteAndBanDoNotSwitchIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		biz    int
		status int
		code   string
	}{
		{"mute", 5, http.StatusTooManyRequests, "account_muted"},
		{"ban", 10, http.StatusBadGateway, "account_banned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var completions int
			up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
				completions++
				bizEnvelope(w, tc.biz, "fixture rejection", `{}`)
			}))
			t.Cleanup(up.Close)
			s, err := NewServer(Config{UpstreamBase: up.URL, Accounts: switchAccounts(), QueueWait: 40 * time.Millisecond,
				MaxInflight: 1, Logger: log.New(io.Discard, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			gw := httptest.NewServer(s.Handler())
			t.Cleanup(func() { gw.Close(); s.Shutdown() })
			status, body, after, elapsed := op0206Response(t, gw)
			if status != tc.status || !strings.Contains(body, `"code":"`+tc.code+`"`) ||
				(tc.biz == 5 && after == "") || elapsed > time.Second || completions != 1 {
				t.Fatalf("status=%d body=%s retry=%q elapsed=%s calls=%d", status, body, after, elapsed, completions)
			}
			for _, account := range switchAccounts() {
				if strings.Contains(body, account.Mobile) || strings.Contains(body, account.Password) {
					t.Fatalf("response leaked account credentials: %s", body)
				}
			}
		})
	}
}

func TestOP06PoolClassificationsThroughHTTP(t *testing.T) {
	for _, tc := range []struct {
		name   string
		park   []upstream.BizError
		busy   bool
		status int
		code   string
		retry  bool
	}{
		{name: "all_banned", park: []upstream.BizError{{BizCode: 10}, {BizCode: 10}}, status: 503, code: "no_accounts"},
		{name: "all_muted", park: []upstream.BizError{{BizCode: 5}, {BizCode: 5}}, status: 429, code: "pool_parked", retry: true},
		{name: "all_busy", busy: true, status: 429, code: "pool_busy", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSwitchFixture(t, "")
			s, gw := op0206Gateway(t, f, switchAccounts())
			var held []*upstream.Lease
			for range switchAccounts() {
				l, _, err := s.pool.AcquireWithWait(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, l)
			}
			for i, l := range held {
				if len(tc.park) != 0 {
					l.NoteError(&tc.park[i])
				}
				if !tc.busy {
					l.Release()
				}
			}
			if tc.busy {
				defer func() {
					for _, l := range held {
						l.Release()
					}
				}()
			}
			status, body, after, elapsed := op0206Response(t, gw)
			var result struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(body), &result); err != nil {
				t.Fatal(err)
			}
			if status != tc.status || result.Error.Code != tc.code || (after != "") != tc.retry || elapsed > time.Second {
				t.Fatalf("status=%d code=%q retry=%q elapsed=%s body=%s", status, result.Error.Code, after, elapsed, body)
			}
			if tc.name == "all_muted" {
				seconds, err := strconv.Atoi(after)
				if err != nil || seconds < 7*24*3600-1 || seconds > 7*24*3600 {
					t.Fatalf("missing mute_until fallback Retry-After=%q, want local 7d", after)
				}
			}
			calls, _ := f.snapshot()
			if len(calls) != 0 {
				t.Fatalf("unavailable accounts were called: %+v", calls)
			}
		})
	}
}

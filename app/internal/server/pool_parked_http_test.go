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

	"simple-chat/internal/sse"
	"simple-chat/internal/upstream"
)

// This exercises both real handlers with virtual identities; no upstream
// request is permitted when every account is already parked.
func TestOP06ParkedPoolHTTP(t *testing.T) {
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/web_search"} {
		for _, scenario := range []string{"mixed_ban_mute_risk", "mute_and_risk", "all_risk", "all_cooling", "all_busy", "all_banned"} {
			t.Run(endpoint+"/"+scenario, func(t *testing.T) {
				f := newSwitchFixture(t, "")
				queueWait := 400 * time.Millisecond
				if scenario == "all_busy" {
					queueWait = 40 * time.Millisecond
				}
				s, err := NewServer(Config{UpstreamBase: f.server.URL, Accounts: switchAccounts(), MaxInflight: 1,
					QueueWait: queueWait, Logger: log.New(io.Discard, "", 0), RandomSeed: 1})
				if err != nil {
					t.Fatal(err)
				}
				gw := httptest.NewServer(s.Handler())
				t.Cleanup(func() { gw.Close(); s.Shutdown() })
				var leases []*upstream.Lease
				for range switchAccounts() {
					l, _, err := s.pool.AcquireWithWait(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					leases = append(leases, l)
				}
				defer func() {
					for _, l := range leases {
						l.Release()
					}
				}()
				if scenario != "all_busy" {
					for i, l := range leases {
						var e error
						switch scenario {
						case "mixed_ban_mute_risk":
							if i == 0 {
								e = &upstream.BizError{BizCode: 10}
							} else {
								e = &upstream.BizError{BizCode: 5, MuteUntil: time.Now().Add(2 * time.Second)}
							}
						case "mute_and_risk":
							if i == 0 {
								e = &upstream.BizError{BizCode: 5, MuteUntil: time.Now().Add(2 * time.Second)}
							} else {
								e = &upstream.BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}
							}
						case "all_risk":
							e = &upstream.BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}
						case "all_cooling":
							e = &sse.StreamError{FinishReason: "parallel_chat_limit"}
						case "all_banned":
							e = &upstream.BizError{BizCode: 10}
						}
						l.NoteError(e)
						l.Release()
					}
				}
				payload := switchRequest
				if endpoint == "/v1/web_search" {
					payload = `{"query":"fictional query"}`
				}
				started := time.Now()
				resp, err := http.Post(gw.URL+endpoint, "application/json", strings.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				var result struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if err := json.Unmarshal(body, &result); err != nil {
					t.Fatal(err)
				}
				want := "pool_parked"
				if scenario == "all_busy" {
					want = "pool_busy"
				}
				wantStatus := http.StatusTooManyRequests
				if scenario == "all_banned" {
					want, wantStatus = "no_accounts", http.StatusServiceUnavailable
				}
				if resp.StatusCode != wantStatus || result.Error.Code != want || time.Since(started) > time.Second {
					t.Fatalf("status=%d code=%q elapsed=%s body=%s", resp.StatusCode, result.Error.Code, time.Since(started), body)
				}
				if scenario != "all_busy" && time.Since(started) > 250*time.Millisecond {
					t.Fatalf("parked/ban pool waited on 400ms QueueWait: %s", time.Since(started))
				}
				if scenario == "all_banned" {
					if resp.Header.Get("Retry-After") != "" {
						t.Fatalf("ban retry=%q", resp.Header.Get("Retry-After"))
					}
				} else {
					seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
					if err != nil || seconds < 1 {
						t.Fatalf("retry=%q err=%v", resp.Header.Get("Retry-After"), err)
					}
					if scenario == "mixed_ban_mute_risk" && (seconds < 3600 || seconds > 3603) {
						t.Fatalf("Retry-After=%d must reflect local mute+1h", seconds)
					}
					if (scenario == "all_risk" || scenario == "mute_and_risk") && (seconds < 598 || seconds > 601) {
						t.Fatalf("risk Retry-After=%d must reflect existing 10m deadline", seconds)
					}
					if scenario == "all_cooling" && seconds > 2 {
						t.Fatalf("cooling Retry-After=%d must reflect 1s deadline", seconds)
					}
					if scenario == "all_busy" && seconds > 2 {
						t.Fatalf("capacity Retry-After=%d not QueueWait", seconds)
					}
				}
				calls, _ := f.snapshot()
				if len(calls) != 0 {
					t.Fatalf("unavailable accounts called: %+v", calls)
				}
				for _, a := range switchAccounts() {
					if strings.Contains(string(body), a.Mobile) || strings.Contains(string(body), a.Password) {
						t.Fatalf("identity leaked: %s", body)
					}
				}
			})
		}
	}
}

func TestOP06PoolParkedRetryAfterRoundsUp(t *testing.T) {
	w := httptest.NewRecorder()
	(&Server{}).writePoolError(w, &upstream.PoolParkedError{Until: time.Now().Add(1500 * time.Millisecond)})
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "2" || !strings.Contains(w.Body.String(), `"code":"pool_parked"`) {
		t.Fatalf("status=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

func TestOP06UnknownRecoveryHasNoRetryAfter(t *testing.T) {
	w := httptest.NewRecorder()
	(&Server{}).writePoolError(w, upstream.ErrNoEligibleAccount)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "" || !strings.Contains(w.Body.String(), `"code":"no_eligible_account"`) {
		t.Fatalf("status=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

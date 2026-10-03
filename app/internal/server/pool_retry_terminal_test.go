package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// The first completion is an empty, semantically finished answer. The pool
// changes while that upstream handler runs, before runAttempt selects a retry.
func TestEmptyRetryNoEligibleAccountTerminates(t *testing.T) {
	for _, state := range []string{"removed", "parked"} {
		for _, stream := range []bool{false, true} {
			t.Run(state+map[bool]string{false: "/nonstream", true: "/stream"}[stream], func(t *testing.T) {
				var logs bytes.Buffer
				var mu sync.Mutex
				calls := 0
				var gateway *Server
				const mobile = "13800000000"
				up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					calls++
					mu.Unlock()
					if state == "removed" {
						if !gateway.pool.RemoveAccount(mobile) {
							t.Error("fixture failed to remove active account")
						}
					} else {
						lease, _, err := gateway.pool.AcquireWithWait(r.Context())
						if err != nil {
							t.Errorf("fixture failed to lease active account: %v", err)
						} else {
							lease.NoteError(&upstream.BizError{BizCode: 5, BizMsg: "fixture muted", MuteUntil: time.Now().Add(time.Minute)})
							lease.Release()
						}
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
					_, _ = io.WriteString(w, "event: close\ndata: {}\n")
				}))
				t.Cleanup(up.Close)
				var err error
				gateway, err = NewServer(Config{UpstreamBase: up.URL, Accounts: []upstream.Account{{Mobile: mobile, Password: "fixture-password"}}, QueueWait: 20 * time.Millisecond, Logger: log.New(&logs, "", 0)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(gateway.Shutdown)
				gw := httptest.NewServer(gateway.Handler())
				t.Cleanup(gw.Close)
				status, body := poolBoundaryChat(t, gw, stream)
				if status != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"no_eligible_account"`) {
					t.Errorf("terminal retry = %d %s; want typed no_eligible_account 503", status, body)
				}
				mu.Lock()
				gotCalls := calls
				mu.Unlock()
				if gotCalls != 1 {
					t.Errorf("completion calls = %d, want 1 (no replay after removal/park)", gotCalls)
				}
				if strings.Contains(logs.String(), "upstream error: <nil>") || strings.Contains(logs.String(), "upstream error: nil") {
					t.Errorf("nil upstream error diagnostic: %s", logs.String())
				}
				var diagnostics []map[string]any
				for _, line := range strings.Split(logs.String(), "\n") {
					var event map[string]any
					if json.Unmarshal([]byte(line), &event) == nil && event["event"] == "chat_completion" {
						diagnostics = append(diagnostics, event)
					}
				}
				if len(diagnostics) != 2 {
					t.Fatalf("diagnostics = %v, want one per attempt; logs: %s", diagnostics, logs.String())
				}
				if diagnostics[0]["termination"] != "empty_output" || diagnostics[1]["termination"] != "no_eligible_account" {
					t.Errorf("attempt diagnostics = %v; want empty_output then no_eligible_account", diagnostics)
				}
			})
		}
	}
}

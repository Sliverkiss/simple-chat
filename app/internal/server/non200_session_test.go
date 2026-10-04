package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

func TestNon200CompletionMuteParksIdentity(t *testing.T) {
	until := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	var mu sync.Mutex
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(429)
		writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 5, "biz_msg": "muted", "biz_data": mc{"mute_until": until.Unix()}}})
	})
	up := httptest.NewServer(mux)
	defer up.Close()
	path := accountsFileAt(t, &upstream.Account{Mobile: "13800000000", Password: "fixture"})
	accounts := mustLoadAccounts(t, path)
	srv, err := NewServer(Config{UpstreamBase: up.URL, Accounts: accounts[:1], ParkStore: jsonStoreFor(path), QueueWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(switchRequest))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 || !strings.Contains(string(payload), "muted") || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status=%d retry-after=%q body=%s", resp.StatusCode, resp.Header.Get("Retry-After"), payload)
	}
	mu.Lock()
	n := hits
	mu.Unlock()
	if n != 1 {
		t.Fatalf("completion replayed %d", n)
	}
	stored := readAccountsFile(t, path)[0]
	if stored.ParkKind != "muted" {
		t.Fatalf("park=%+v, want mute until %s", stored, until)
	}
	parkUntil, err := time.Parse(time.RFC3339, stored.ParkUntil)
	if err != nil || !parkUntil.Equal(until) {
		t.Fatalf("park_until=%q, want %s (err=%v)", stored.ParkUntil, until, err)
	}
	resp, err = http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(switchRequest))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	n = hits
	mu.Unlock()
	if n != 1 {
		t.Fatalf("muted account reused: hits=%d", n)
	}
}

func TestSession503SwitchesBeforeCompletion(t *testing.T) {
	for _, web := range []bool{false, true} {
		t.Run(map[bool]string{false: "chat", true: "web_search"}[web], func(t *testing.T) {
			var mu sync.Mutex
			var sessions, completions []string
			mux := http.NewServeMux()
			mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
				var v struct {
					Mobile string `json:"mobile"`
				}
				json.NewDecoder(r.Body).Decode(&v)
				writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0, "biz_data": mc{"user": mc{"token": "tok-" + v.Mobile}}}})
			})
			mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
				id := r.Header.Get("X-Device-ID")
				mu.Lock()
				sessions = append(sessions, id)
				first := len(sessions) <= 2
				mu.Unlock()
				if first {
					w.WriteHeader(503)
					writeJSON(w, mc{"code": 0, "data": mc{"biz_code": 0}})
					return
				}
				sessOK(w, "s")
			})
			mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
			mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				completions = append(completions, r.Header.Get("X-Device-ID"))
				mu.Unlock()
				streamOK(w, "answer")
			})
			up := httptest.NewServer(mux)
			defer up.Close()
			srv, err := NewServer(Config{UpstreamBase: up.URL, Accounts: switchAccounts()})
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Shutdown()
			gw := httptest.NewServer(srv.Handler())
			defer gw.Close()
			endpoint, body := "/v1/chat/completions", switchRequest
			if web {
				endpoint, body = "/v1/web_search", `{"query":"q"}`
			}
			resp, err := http.Post(gw.URL+endpoint, "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			mu.Lock()
			ss := append([]string(nil), sessions...)
			cc := append([]string(nil), completions...)
			mu.Unlock()
			if resp.StatusCode != 200 || len(ss) != 3 || ss[0] == "" || ss[0] != ss[1] || ss[1] == ss[2] || len(cc) != 1 || cc[0] != ss[2] {
				t.Fatalf("status=%d sessions=%v completions=%v body=%s", resp.StatusCode, ss, cc, payload)
			}
		})
	}
}

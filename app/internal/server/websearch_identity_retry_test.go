package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simple-chat/internal/upstream"
)

func TestWebSearchRetryUsesDifferentPhysicalIdentity(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		// A unique request header fingerprints the account's physical device.
		id := r.Header.Get("X-Device-ID")
		mu.Lock()
		seen = append(seen, id)
		n := len(seen)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		streamOK(w, "answer")
	})
	up := httptest.NewServer(mux)
	defer up.Close()
	srv, err := NewServer(Config{UpstreamBase: up.URL, Accounts: []upstream.Account{{Mobile: "13800000000", Password: "pw"}, {Mobile: "13900000000", Password: "pw"}}})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/web_search", "application/json", strings.NewReader(`{"query":"q"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	if !json.Valid(payload) {
		t.Fatalf("bad JSON %s", payload)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] == "" || seen[0] == seen[1] {
		t.Fatalf("retry physical devices = %q; want two distinct", seen)
	}
}

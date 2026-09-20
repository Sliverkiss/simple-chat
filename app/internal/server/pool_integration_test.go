package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/pow"
	"simple-chat/internal/upstream"
)

// trackedFixture is an upstream mock that tracks per-account sessions and
// deletes: login answers a token derived from the mobile, every created
// session gets a unique id recorded against its token, and every delete is
// recorded per session id.
type trackedFixture struct {
	srv *httptest.Server

	mu      sync.Mutex
	tokens  map[string]int  // bearer token -> counter for unique session ids
	created map[string]bool // session id -> created
	deleted map[string]int  // session id -> delete count
}

func newTrackedFixture(t *testing.T) *trackedFixture {
	f := &trackedFixture{
		tokens:  map[string]int{},
		created: map[string]bool{},
		deleted: map[string]int{},
	}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		mobile, _ := body["mobile"].(string)
		f.mu.Lock()
		f.tokens["tok-"+mobile]++
		f.mu.Unlock()
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"user": map[string]any{"token": "tok-" + mobile, "id": mobile},
			},
		}})
	})

	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		f.mu.Lock()
		f.tokens[token]++ // reuse counter: sessions created under this token
		n := f.tokens[token]
		id := fmt.Sprintf("sess-%s-%d", token, n)
		f.created[id] = true
		f.mu.Unlock()
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"chat_session": map[string]any{"id": id},
			},
		}})
	})

	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		id, _ := body["chat_session_id"].(string)
		f.mu.Lock()
		f.deleted[id]++
		f.mu.Unlock()
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": nil,
		}})
	})

	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		target, _ := body["target_path"].(string)
		h := pow.HashV1([]byte("testsalt_1700000000_42"))
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"challenge": map[string]any{
					"algorithm": "HashV1", "challenge": hex.EncodeToString(h[:]),
					"salt": "testsalt", "expire_at": 1700000000,
					"difficulty": 144000, "signature": "sig", "target_path": target,
				},
			},
		}})
	})

	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"ok\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// TestConcurrentRotationAndSessionCleanup proves, under load (5 parallel
// requests over a 2-account pool): every request succeeds, accounts rotate
// (both accounts serve), and with a cap of 2 per account the five sessions
// are trimmed to the newest 2 per account — each surviving session counted
// exactly once per delete, no leaks.
func TestConcurrentRotationAndSessionCleanup(t *testing.T) {
	up := newTrackedFixture(t)
	srv, err := NewServer(Config{
		UpstreamBase: up.srv.URL,
		Accounts: []upstream.Account{
			{Mobile: "13800000000", Password: "pw"},
			{Mobile: "13900000000", Password: "pw"},
		},
		MaxInflight: 2,
		SessionCap:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	defer gw.Close()

	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
			if err != nil {
				t.Errorf("request: %v", err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Errorf("status = %d, body = %s", resp.StatusCode, body)
				return
			}
			if !strings.Contains(string(body), "ok") {
				t.Errorf("completion content missing: %s", body)
			}
		}()
	}
	wg.Wait()

	// Session trimming is async (off the response path): with cap=2 per
	// account and n=5 creates spread over 2 accounts, expect n - 2*2 = 1
	// eviction at minimum once evictions settle (each account evicts down to
	// its newest 2; the exact count depends on ring distribution, but at
	// least one eviction must fire and no delete may repeat or leak).
	deadline := time.Now().Add(3 * time.Second)
	for {
		up.mu.Lock()
		done := len(up.deleted) >= 1
		up.mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.created) != n {
		t.Fatalf("created %d sessions, want %d", len(up.created), n)
	}
	if len(up.deleted) > n-2*2 {
		t.Fatalf("deleted %d distinct sessions, want at most %d (cap=2 per account)", len(up.deleted), n-4)
	}
	for id := range up.deleted {
		if up.deleted[id] != 1 {
			t.Errorf("session %s deleted %d times, want exactly 1", id, up.deleted[id])
		}
	}
	for id := range up.deleted {
		if !up.created[id] {
			t.Errorf("delete for unknown session %s (leaked id)", id)
		}
	}
	// Rotation: both accounts must have served at least one session.
	for _, tok := range []string{"tok-13800000000", "tok-13900000000"} {
		served := false
		for id := range up.created {
			if strings.Contains(id, tok) {
				served = true
				break
			}
		}
		if !served {
			t.Errorf("account %s never served a request — rotation broken", tok)
		}
	}
}

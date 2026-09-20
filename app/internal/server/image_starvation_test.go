package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// A hanging image server must never starve the pool: the image fetch is
// bounded (request context + client timeout) and happens off-lease, so a
// follow-up request on the same single-slot account succeeds promptly
// (gap-analysis R4).
func TestHangingImageFetchNeverStarvesPool(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()

	block := make(chan struct{})
	imgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang far beyond request B's budget; release only at cleanup so a
		// stuck (old-code) handler can't wedge the test suite.
		select {
		case <-block:
		case <-time.After(30 * time.Second):
		case <-r.Context().Done(): // new code cancels the fetch — releases early
		}
	}))
	defer imgSrv.Close()
	defer close(block)

	gwSrv, err := NewServer(Config{
		UpstreamBase: up.srv.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
		MaxInflight:  1,
		QueueWait:    300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(gwSrv.Handler())
	defer gw.Close()

	// Request A carries a hanging image URL and a client-side deadline.
	ctxA, cancelA := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelA()
	reqA, _ := http.NewRequestWithContext(ctxA, http.MethodPost, gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"`+imgSrv.URL+`/photo.png"}}]}]}`))
	respA, err := http.DefaultClient.Do(reqA)
	if err == nil {
		respA.Body.Close()
	}
	// A's outcome (client timeout vs 400) is not the point. The image server
	// KEEPS hanging — with the old code, request A's handler still holds the
	// account's only slot, and request B must hit the 300ms QueueWait wall.

	// Request B, plain text, same single-slot account: must succeed promptly.
	ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelB()
	reqB, _ := http.NewRequestWithContext(ctxB, http.MethodPost, gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	start := time.Now()
	respB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		t.Fatal(err)
	}
	defer respB.Body.Close()
	body, _ := io.ReadAll(respB.Body)
	if respB.StatusCode != http.StatusOK {
		t.Fatalf("follow-up request got %d (%s) after %s — pool starved by hanging image fetch",
			respB.StatusCode, body, time.Since(start))
	}
}

package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoginMuteExpiresInsteadOfPermanentlyBlockingManager(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `{"code":0,"data":{"biz_code":5,"biz_msg":"muted","biz_data":{}}}`)
			return
		}
		fmt.Fprint(w, `{"code":0,"data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"recovered"}}}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	p, err := NewPool([]Account{{Mobile: "13800000000", Password: "pw"}}, PoolConfig{BaseURL: srv.URL, MuteParkDefault: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err := lease.Token(context.Background()); err == nil {
		t.Fatal("expected login mute")
	}
	am := lease.pa.am
	am.mu.Lock()
	if am.ban != BanMuted || am.parkUntil.IsZero() {
		am.mu.Unlock()
		t.Fatal("mute missing bounded deadline")
	}
	if remaining := time.Until(am.parkUntil); remaining < 30*time.Second || remaining > 2*time.Minute {
		am.mu.Unlock()
		t.Fatalf("mute deadline ignored configured duration: %s", remaining)
	}
	am.parkUntil = time.Now().Add(-time.Second)
	am.mu.Unlock()
	tok, err := lease.Token(context.Background())
	if err != nil || tok != "recovered" {
		t.Fatalf("expired mute should permit login, token=%q err=%v", tok, err)
	}
}

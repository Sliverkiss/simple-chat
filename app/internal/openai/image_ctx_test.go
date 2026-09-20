package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A hanging image server must not stall extraction forever: the fetch is
// bounded by the request context (and a 30s client timeout), and the failure
// surfaces as an error instead of a silent skip (gap-analysis R4).
func TestExtractImagesHangingServerFailsFast(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hangs until the test client cancels (new code) — honoring the
		// request context keeps srv.Close() from wedging on this handler.
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msgs := []Message{{
		Role: "user",
		ContentParts: []ContentPart{
			{Type: "image_url", ImageURL: srv.URL + "/photo.png"},
		},
	}}
	done := make(chan error, 1)
	var imgs []Image
	go func() {
		var err error
		imgs, err = ExtractImages(ctx, msgs)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("hanging image server must produce an error, not an empty result")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
		if len(imgs) != 0 {
			t.Errorf("imgs = %d, want 0 on failure", len(imgs))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ExtractImages did not respect the context deadline")
	}
}

// Client cancellation must abort an in-flight fetch (context threaded through
// the request, not just a blanket timeout).
func TestExtractImagesContextCancelAborts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	msgs := []Message{{
		Role:         "user",
		ContentParts: []ContentPart{{Type: "image_url", ImageURL: srv.URL + "/p.png"}},
	}}
	done := make(chan error, 1)
	go func() {
		_, err := ExtractImages(ctx, msgs)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("cancelled fetch must error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not abort the image fetch")
	}
}

// A reachable-but-wrong image (non-200, non-image content type, oversized)
// must surface as an error — silently dropping a requested image produces a
// completion the client thinks includes it.
func TestExtractImagesBadResponsesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gone":
			w.WriteHeader(http.StatusNotFound)
		case "/html":
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte("<html>"))
		case "/huge":
			w.Header().Set("Content-Type", "image/png")
			w.Write(make([]byte, 10<<20+2))
		}
	}))
	defer srv.Close()

	for _, path := range []string{"/gone", "/html", "/huge"} {
		msgs := []Message{{
			Role:         "user",
			ContentParts: []ContentPart{{Type: "image_url", ImageURL: srv.URL + path}},
		}}
		if _, err := ExtractImages(context.Background(), msgs); err == nil {
			t.Errorf("%s: expected an error", path)
		}
	}
}

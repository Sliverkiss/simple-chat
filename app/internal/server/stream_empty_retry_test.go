package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Empty stream output with no error and no client byte yet: the ladder
// re-runs once on a fresh session (mirroring the non-stream empty-retry,
// gap-analysis R1) and delivers the second attempt's output.
func TestStreamEmptyOutputRetriesBeforeFirstByte(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		if calls == 1 {
			// Healthy stream shape but zero fragments — the empty-output case.
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[],\"status\":\"FINISHED\"}}}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
			return
		}
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"second-run\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after empty-stream retry, body = %s", resp.StatusCode, raw)
	}
	if !strings.Contains(s, `"content":"second-run"`) {
		t.Errorf("stream missing re-run content: %s", s)
	}
	if calls != 2 {
		t.Errorf("completion calls = %d, want 2 (empty + re-run)", calls)
	}
}

// A THINK-only stream is NOT empty — reasoning_content counts as output.
// The empty-retry must not fire; the reasoning is delivered as-is.
func TestStreamThinkOnlyIsNotEmptyNoRetry(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"pondering\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"reasoning_content":"pondering"`) {
		t.Errorf("think-only stream must deliver reasoning_content: %s", s)
	}
	if calls != 1 {
		t.Errorf("completion calls = %d, want 1 — think-only is not empty", calls)
	}
}

// Once a byte has gone to the client, an empty finish is delivered honestly,
// never retried — a retry would duplicate frames. THINK deltas with thinking
// opted out still commit the response (the frame is suppressed but the stream
// has begun), so the empty finish lands as an honest empty 200.
func TestStreamEmptyAfterFirstByteNotRetried(t *testing.T) {
	var calls int
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		// THINK-only with the client opted out: the delta callback fires (and
		// commits) but suppresses the frame.
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"hidden\"}]}}}\n")
		io.WriteString(w, "data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n")
		io.WriteString(w, "event: close\ndata: {}\n")
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":true,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (committed stream delivered honestly)", resp.StatusCode)
	}
	if strings.Contains(s, "reasoning_content") {
		t.Errorf("opted-out stream must not leak reasoning_content: %s", s)
	}
	if !strings.Contains(s, `"finish_reason":"stop"`) || !strings.Contains(s, "data: [DONE]") {
		t.Errorf("committed empty stream must terminate cleanly: %s", s)
	}
	if calls != 1 {
		t.Errorf("completion calls = %d, want 1 — no retry after the stream committed", calls)
	}
}

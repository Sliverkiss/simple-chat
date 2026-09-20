package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// bizEnvelope writes a 200 JSON completion answer carrying a biz error.
func bizEnvelope(w http.ResponseWriter, biz int, msg string, bizData string) {
	body := `{"code":0,"msg":"","data":{"biz_code":` + strconv.Itoa(biz) +
		`,"biz_msg":"` + msg + `","biz_data":` + bizData + `}}`
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, body)
}

// risk device (biz 11) maps to 503 upstream_unavailable (spec table), not
// the old 502 risk_device.
func TestRiskDeviceMapsTo503UpstreamUnavailable(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		bizEnvelope(w, 11, "RISK_DEVICE_DETECTED", `{}`)
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"code":"upstream_unavailable"`) {
		t.Errorf("code must be upstream_unavailable: %s", body)
	}
}

// muted 429 carries Retry-After derived from the upstream mute_until.
func TestMuted429CarriesRetryAfterFromMuteUntil(t *testing.T) {
	until := time.Now().Add(2 * time.Minute).Unix()
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		bizEnvelope(w, 5, "user is muted", `{"mute_until":`+strconv.FormatInt(until, 10)+`}`)
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429, body = %s", resp.StatusCode, body)
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		t.Fatal("muted 429 must carry Retry-After")
	}
	secs, err := strconv.Atoi(ra)
	if err != nil {
		t.Fatalf("Retry-After not an integer: %q", ra)
	}
	if secs < 60 || secs > 130 {
		t.Errorf("Retry-After = %d, want ~120 (from mute_until)", secs)
	}
}

// muted without mute_until still carries a conservative Retry-After.
func TestMuted429WithoutMuteUntilHasFallbackRetryAfter(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		bizEnvelope(w, 5, "user is muted", `{}`)
	}))
	defer up.Close()
	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("muted 429 must carry a Retry-After even without mute_until")
	}
}

// A content-filter rejection before the first stream byte maps to a typed
// 400 content_filter (spec table), not a generic 502.
func TestContentFilterPreByteMapsTo400(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"code\":\"content_filter\",\"error\":\"blocked\"}\n\n")
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
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"code":"content_filter"`) {
		t.Errorf("code must be content_filter: %s", body)
	}
}

// A content-filter rejection mid-stream (after a delivered delta) emits a
// typed content_filter error frame instead of the generic stream_error one.
func TestContentFilterMidStreamEmitsTypedFrame(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\n")
		io.WriteString(w, "data: {\"error\":{\"code\":\"content_filter\"}}\n\n")
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
		t.Fatalf("status = %d, want 200 (stream committed)", resp.StatusCode)
	}
	if !strings.Contains(s, `"content":"partial"`) {
		t.Errorf("pre-filter delta must be delivered: %s", s)
	}
	if !strings.Contains(s, `"code":"content_filter"`) {
		t.Errorf("typed content_filter frame required: %s", s)
	}
	if strings.Contains(s, `"code":"stream_error"`) {
		t.Errorf("generic stream_error frame must not carry a content filter: %s", s)
	}
}

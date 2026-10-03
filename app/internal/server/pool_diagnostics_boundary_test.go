package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"simple-chat/internal/upstream"
)

// An orderly HTTP EOF is not a semantic upstream completion. The upstream
// deliberately omits both FINISHED and event: close after its visible delta.
func poolBoundaryUpstream(t *testing.T, ending string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"v":{"response":{"fragments":[{"type":"RESPONSE","content":"partial"}]}}}`+"\n\n")
		if ending != "" {
			_, _ = io.WriteString(w, ending)
		}
	}))
}

func poolBoundaryGateway(t *testing.T, upstreamURL string, logs *bytes.Buffer) *httptest.Server {
	t.Helper()
	srv, err := NewServer(Config{
		UpstreamBase: upstreamURL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "fixture-password"}},
		Logger:       log.New(logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(gw.Close)
	return gw
}

func poolBoundaryChat(t *testing.T, gw *httptest.Server, stream bool) (int, string) {
	t.Helper()
	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
	if stream {
		body = `{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

func TestPoolBoundaryStreamEOFWithoutSemanticFinishCannotStop(t *testing.T) {
	up := poolBoundaryUpstream(t, "")
	defer up.Close()
	var logs bytes.Buffer
	gw := poolBoundaryGateway(t, up.URL, &logs)
	status, body := poolBoundaryChat(t, gw, true)
	if status != http.StatusOK || !strings.Contains(body, `"content":"partial"`) {
		t.Fatalf("visible delta expected before orderly upstream EOF: status=%d body=%s", status, body)
	}
	if strings.Contains(body, `"finish_reason":"stop"`) {
		t.Errorf("unconfirmed upstream completion must not produce clean stop: %s", body)
	}
	if !strings.Contains(body, `"code":"stream_error"`) {
		t.Errorf("committed truncated stream must report an error frame: %s", body)
	}
}

func TestPoolBoundaryNonStreamEOFWithoutSemanticFinishFails(t *testing.T) {
	up := poolBoundaryUpstream(t, "")
	defer up.Close()
	var logs bytes.Buffer
	gw := poolBoundaryGateway(t, up.URL, &logs)
	status, body := poolBoundaryChat(t, gw, false)
	if status != http.StatusBadGateway || !strings.Contains(body, `"code":"stream_error"`) {
		t.Errorf("non-stream truncated upstream answer must fail, not return partial success: status=%d body=%s", status, body)
	}
}

func TestPoolBoundaryEventCloseWithoutStatusIsClean(t *testing.T) {
	up := poolBoundaryUpstream(t, "event: close\ndata: {}\n")
	defer up.Close()
	var logs bytes.Buffer
	gw := poolBoundaryGateway(t, up.URL, &logs)
	for _, stream := range []bool{true, false} {
		status, body := poolBoundaryChat(t, gw, stream)
		if status != http.StatusOK || !strings.Contains(body, `"content":"partial"`) || !strings.Contains(body, `"finish_reason":"stop"`) {
			t.Errorf("close without status is a clean terminal event (stream=%v): status=%d body=%s", stream, status, body)
		}
	}
}

func TestPoolBoundaryEOFBeforeFirstByteFailsWithoutSuccess(t *testing.T) {
	up := httptest.NewServer(ladderMux(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: ready\ndata: {}\n")
	}))
	defer up.Close()
	var logs bytes.Buffer
	gw := poolBoundaryGateway(t, up.URL, &logs)
	for _, stream := range []bool{true, false} {
		status, body := poolBoundaryChat(t, gw, stream)
		if status != http.StatusBadGateway || !strings.Contains(body, `"code":"stream_error"`) {
			t.Errorf("uncommitted EOF cannot be success (stream=%v): status=%d body=%s", stream, status, body)
		}
	}
}

func TestPoolBoundaryContentFilterAfterDeltaDiagnostic(t *testing.T) {
	up := poolBoundaryUpstream(t, "data: {\"error\":{\"code\":\"content_filter\"}}\n\n")
	defer up.Close()
	var logs bytes.Buffer
	gw := poolBoundaryGateway(t, up.URL, &logs)
	status, body := poolBoundaryChat(t, gw, true)
	if status != http.StatusOK || !strings.Contains(body, `"content":"partial"`) || !strings.Contains(body, `"code":"content_filter"`) {
		t.Fatalf("expected committed delta then typed content filter: status=%d body=%s", status, body)
	}
	var events []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		start := strings.IndexByte(line, '{')
		if start < 0 {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(line[start:]), &event) == nil && event["event"] == "chat_completion" {
			events = append(events, event)
		}
	}
	if len(events) != 1 {
		t.Fatalf("want one structured completion diagnostic, got %d", len(events))
	}
	if got := events[0]["termination"]; got != "content_filter" {
		t.Errorf("termination = %v, want content_filter (not stream_error)", got)
	}
}

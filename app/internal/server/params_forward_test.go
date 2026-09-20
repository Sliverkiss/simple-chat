package server

// TASK_UPSTREAM_PARAMS — end-to-end sampling-parameter forwarding.
//
// The upstream tolerates extra sampling keys (proven by shipped traffic and
// ds2api; see app/docs-upstream-params.md) even though the official Android
// client never sends them. The gateway forwards temperature/top_p verbatim
// and maps max_completion_tokens (OpenAI's current name) onto max_tokens.

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestChatCompletionsSamplingParamsReachUpstream(t *testing.T) {
	var gotBody atomic.Value
	up := newUpstreamFixtureWithCompletionHook(t, func(body map[string]any) {
		gotBody.Store(body)
	})
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":false,
			"messages":[{"role":"user","content":"hello"}],
			"temperature":0.7,"top_p":0.9,"max_completion_tokens":512}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	body, _ := gotBody.Load().(map[string]any)
	if temp, _ := body["temperature"].(float64); temp != 0.7 {
		t.Errorf("upstream temperature = %v, want 0.7", body["temperature"])
	}
	if topP, _ := body["top_p"].(float64); topP != 0.9 {
		t.Errorf("upstream top_p = %v, want 0.9", body["top_p"])
	}
	if maxTok, _ := body["max_tokens"].(float64); maxTok != 512 {
		t.Errorf("upstream max_tokens = %v, want 512 (from max_completion_tokens)", body["max_tokens"])
	}
	if up.completions.Load() != 1 {
		t.Errorf("completions = %d, want 1", up.completions.Load())
	}
}

// TestChatCompletionsUpstreamBodyGoldenOrder pins the raw upstream body
// through the full OpenAI path (TASK_FINGERPRINT_ALIGN): the 10 app keys
// in kotlinx descriptor order (qj1.java:16-26) with model_type "default"
// and audio_id/action as literal nulls. Raw bytes, not a decoded map —
// key order is the assertion.
func TestChatCompletionsUpstreamBodyGoldenOrder(t *testing.T) {
	var mu sync.Mutex
	var raw []byte
	up := newUpstreamFixture(t)
	up.onCompletionRaw = func(b []byte) {
		mu.Lock()
		raw = b
		mu.Unlock()
	}
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":false,
			"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(raw) == 0 {
		t.Fatal("no upstream body captured")
	}
	// The session id is server-generated ("sess1" in the fixture); assert
	// the shape around it rather than pinning the full session id.
	// Order-sensitive prefix up to the session id, then order-sensitive
	// suffix after it.
	prefix := `{"chat_session_id":"`
	if !strings.HasPrefix(string(raw), prefix) {
		t.Fatalf("body does not start with chat_session_id:\n%s", raw)
	}
	rest := string(raw)
	for _, anchor := range []string{`"parent_message_id":null`, `"prompt":"`, `"ref_file_ids":[]`,
		`"thinking_enabled":true`, `"search_enabled":false`, `"audio_id":null`,
		`"preempt":false`, `"model_type":"default"`, `"action":null}`} {
		idx := strings.Index(rest, anchor)
		if idx < 0 {
			t.Fatalf("anchor %s missing or out of order in body:\n%s", anchor, raw)
		}
		rest = rest[idx+len(anchor):]
	}
}

func TestChatCompletionsSamplingParamsAbsentWhenUnset(t *testing.T) {
	var sawBody atomic.Value
	up := newUpstreamFixtureWithCompletionHook(t, func(body map[string]any) {
		sawBody.Store(body)
	})
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","stream":false,
			"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	body, _ := sawBody.Load().(map[string]any)
	for _, k := range []string{"temperature", "top_p", "max_tokens"} {
		if _, ok := body[k]; ok {
			t.Errorf("upstream body must omit %s when unset, got %v", k, body[k])
		}
	}
}

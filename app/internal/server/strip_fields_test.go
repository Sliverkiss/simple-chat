package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// kitchenSinkBody is the agent-framework worst case at the HTTP level:
// every unsupported request field plus system and tool-call messages. If
// sanitization works, this request completes normally against the fixture
// upstream — no 400, no tool residue anywhere in the response.
const kitchenSinkBody = `{
	"model": "deepseek-flash",
	"stream": false,
	"tools": [{"type": "function", "function": {"name": "x"}}],
	"tool_choice": "auto",
	"parallel_tool_calls": true,
	"logprobs": true,
	"user": "u1",
	"store": false,
	"metadata": {"a": "b"},
	"service_tier": "auto",
	"messages": [
		{"role": "system", "content": "You are terse."},
		{"role": "user", "content": "hello"},
		{"role": "assistant", "tool_calls": [{"id": "1", "function": {"name": "x", "arguments": "{}"}}]},
		{"role": "tool", "content": "result"}
	]
}`

func TestChatCompletionsKitchenSinkNonStream(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(kitchenSinkBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	if up.completions.Load() != 1 {
		t.Errorf("upstream completion must fire exactly once, got %d", up.completions.Load())
	}
	if strings.Contains(strings.ToLower(string(body)), "tool") {
		t.Errorf("response body must not contain tool residue: %s", body)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "hello world" {
		t.Errorf("choices wrong: %+v", out.Choices)
	}
}

func TestChatCompletionsKitchenSinkStream(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(strings.Replace(kitchenSinkBody, `"stream": false`, `"stream": true`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	if up.completions.Load() != 1 {
		t.Errorf("upstream completion must fire exactly once, got %d", up.completions.Load())
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(strings.ToLower(string(body)), "tool") {
		t.Errorf("stream body must not contain tool residue: %s", body)
	}
	// Streaming delivers incremental deltas, not the joined content.
	for _, want := range []string{`"content":"hello"`, `"content":" world"`, "data: [DONE]"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("stream missing %s\nstream: %s", want, body)
		}
	}
}

// TestChatCompletionsToolsRejected asserted tools → 400. That behavior is
// inverted by design now (TASK_STRIP_FIELDS): tools are stripped and the
// request succeeds end-to-end.
func TestChatCompletionsToolsStrippedNotRejected(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"x"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("tools must be stripped, not rejected — status = %d, body = %s", resp.StatusCode, body)
	}
	if up.completions.Load() != 1 {
		t.Errorf("upstream completion must fire, got %d", up.completions.Load())
	}
}

func TestChatCompletionsSystemOnlyConversationRejected(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServer(t, up.srv.URL)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"system","content":"only system"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("system-only conversation must 400 after sanitization, got %d", resp.StatusCode)
	}
	if up.completions.Load() != 0 {
		t.Errorf("no upstream completion should fire, got %d", up.completions.Load())
	}
}

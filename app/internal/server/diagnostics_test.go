package server

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"simple-chat/internal/openai"
	"simple-chat/internal/upstream"
)

func TestChatCompletionDiagnosticsAreStructuredAndRedacted(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	var logs bytes.Buffer
	srv, err := NewServer(Config{
		UpstreamBase: up.srv.URL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "super-secret-password"}},
		Logger:       log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"private prompt"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var event map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "chat_completion") {
			start := strings.Index(line, "{")
			if start >= 0 && json.Unmarshal([]byte(line[start:]), &event) == nil {
				break
			}
		}
	}
	if event == nil {
		t.Fatalf("missing structured completion diagnostic: %q", logs.String())
	}
	for _, key := range []string{"account_id", "input_tokens", "output_tokens", "total_tokens", "elapsed_ms", "tok_per_sec", "queue_wait_ms", "attempt", "stream", "termination"} {
		if _, ok := event[key]; !ok {
			t.Errorf("diagnostic missing %q: %v", key, event)
		}
	}
	if got := event["account_id"]; got == "13800000000" || got == "super-secret-password" {
		t.Errorf("account identity was not redacted: %v", got)
	}
	for _, secret := range []string{"super-secret-password", "private prompt"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("log contains secret/content %q: %s", secret, logs.String())
		}
	}
	if event["input_tokens"] != "unknown" || event["output_tokens"] != "unknown" {
		t.Errorf("upstream token split should be explicit unknown: %v", event)
	}
}

func TestResponseFormatIsStrippedAndSystemMessageIsMerged(t *testing.T) {
	body := `{"model":"deepseek-flash","response_format":{"type":"json_object"},"messages":[{"role":"system","content":"be concise"},{"role":"user","content":"hello"}]}`
	req, err := openai.ParseRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(req.Stripped, "response_format") || !contains(req.Stripped, "system_messages=1") {
		t.Fatalf("compatibility report = %v", req.Stripped)
	}
	if got := openai.FlattenMessages(req.Messages); !strings.Contains(got, "be concise") || !strings.Contains(got, "hello") {
		t.Fatalf("system message was not merged: %q", got)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

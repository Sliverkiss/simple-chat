package openai

import (
	"reflect"
	"strings"
	"testing"
)

// msgExpect is the exported projection of a surviving Message — sanitized
// messages are compared through it because Message also carries unexported
// parse-time flags (tool_calls presence, junk part counts) that tests
// shouldn't have to know about.
type msgExpect struct {
	Role    string
	Content string
	Parts   []ContentPart
}

func assertMessages(t *testing.T, req *Request, want []msgExpect) {
	t.Helper()
	if len(req.Messages) != len(want) {
		t.Fatalf("messages = %d, want %d: %+v", len(req.Messages), len(want), req.Messages)
	}
	for i, w := range want {
		got := req.Messages[i]
		if got.Role != w.Role {
			t.Errorf("msg[%d].Role = %q, want %q", i, got.Role, w.Role)
		}
		if got.Content != w.Content {
			t.Errorf("msg[%d].Content = %q, want %q", i, got.Content, w.Content)
		}
		if !reflect.DeepEqual(got.ContentParts, w.Parts) {
			t.Errorf("msg[%d].ContentParts = %+v, want %+v", i, got.ContentParts, w.Parts)
		}
	}
}

func assertStripped(t *testing.T, req *Request, want ...string) {
	t.Helper()
	set := make(map[string]bool, len(req.Stripped))
	for _, s := range req.Stripped {
		set[s] = true
	}
	for _, w := range want {
		if !set[w] {
			t.Errorf("stripped list %v missing %q", req.Stripped, w)
		}
	}
}

// kitchenSinkBody is the worst any agent framework sends: every unsupported
// request field, system messages, tool_calls carriers, empty tool results,
// junk content parts, and a name field — wrapped around a valid request.
const kitchenSinkBody = `{
	"model": "deepseek-flash",
	"stream": true,
	"temperature": 0.7,
	"top_p": 0.9,
	"max_tokens": 512,
	"thinking": {"type": "disabled"},
	"search": {"type": "enabled"},
	"tools": [{"type": "function", "function": {"name": "x"}}],
	"tool_choice": "auto",
	"functions": [{"name": "f"}],
	"function_call": "none",
	"parallel_tool_calls": true,
	"logprobs": true,
	"top_logprobs": 5,
	"logit_bias": {"1": 2},
	"user": "u1",
	"store": false,
	"metadata": {"a": "b"},
	"service_tier": "auto",
	"response_format": {"type": "text"},
	"seed": 42,
	"stop": ["\n"],
	"n": 1,
	"stream_options": {"include_usage": true},
	"presence_penalty": 0.5,
	"frequency_penalty": 0.5,
	"messages": [
		{"role": "system", "content": "You are terse."},
		{"role": "system", "content": "Second system."},
		{"role": "user", "content": "hello", "name": "alice"},
		{"role": "assistant", "tool_calls": [{"id": "1", "function": {"name": "x", "arguments": "{}"}}]},
		{"role": "assistant", "content": "partial answer", "tool_calls": [{"id": "2", "function": {"name": "y", "arguments": "{}"}}]},
		{"role": "tool", "content": "tool result text"},
		{"role": "tool", "content": ""},
		{"role": "user", "content": [
			{"type": "text", "text": "describe"},
			{"type": "audio", "audio": "x"},
			{"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}}
		]}
	]
}`

func TestParseRequestKitchenSink(t *testing.T) {
	req, err := ParseRequest(kitchenSinkBody)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly what survives the sanitizer. The two system messages are merged
	// into the first user message as a prefix; everything else is unchanged.
	assertMessages(t, req, []msgExpect{
		{Role: "user", Content: "You are terse.\n\nSecond system." + systemMergeFormat + "hello"},
		{Role: "assistant", Content: "partial answer"},
		{Role: "tool", Content: "tool result text"},
		{Role: "user", Parts: []ContentPart{
			{Type: "text", Text: "describe"},
			{Type: "image_url", ImageURL: "data:image/png;base64,aGVsbG8="},
		}},
	})
	// Kept flags ride through untouched.
	if !req.Stream || req.Temperature != 0.7 || req.TopP != 0.9 || req.MaxTokens != 512 {
		t.Errorf("kept request fields lost: %+v", req)
	}
	if req.ThinkingEnabled {
		t.Error("thinking {type: disabled} must parse as disabled")
	}
	if !req.SearchEnabled {
		t.Error("search {type: enabled} must parse as enabled")
	}
	// Every stripped category is reported, exactly once per field.
	assertStripped(t, req,
		"tools", "tool_choice", "functions", "function_call", "parallel_tool_calls",
		"logprobs", "top_logprobs", "logit_bias", "user", "store", "metadata",
		"service_tier", "response_format", "seed", "stop", "n", "stream_options",
		"presence_penalty", "frequency_penalty",
		"system_messages=2", "tool_messages=2", "junk_content_parts=1", "message_names=1",
	)
	for _, s := range req.Stripped {
		if (strings.HasPrefix(s, "system_messages") || strings.HasPrefix(s, "tool_messages")) && !strings.Contains(s, "=") {
			t.Errorf("counter entry %q must be count-suffixed", s)
		}
	}
}

func TestParseRequestToolsAccepted(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [{"role": "user", "content": "hi"}], "tools": [{"type": "function", "function": {"name": "x"}}], "tool_choice": "auto"}`)
	if err != nil {
		t.Fatalf("tools must be stripped, not rejected: %v", err)
	}
	if len(req.Messages) != 1 || req.Messages[0].Content != "hi" {
		t.Errorf("messages wrong: %+v", req.Messages)
	}
	assertStripped(t, req, "tools", "tool_choice")
}

func TestParseRequestNullJunkFieldsNotReported(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [{"role": "user", "content": "hi"}], "tools": null, "user": null}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Stripped) != 0 {
		t.Errorf("null junk fields must not be reported: %v", req.Stripped)
	}
}

// systemMergeFormat pins the deterministic merge layout:
// system blocks joined with a blank line, then a "---" separator line,
// then the original first-user content.
const systemMergeFormat = "\n\n---\n\n"

func TestParseRequestMergesAllSystemMessagesIntoFirstUser(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [
		{"role": "system", "content": "s1"},
		{"role": "user", "content": "u1"},
		{"role": "system", "content": "s2"},
		{"role": "system", "content": "s3"},
		{"role": "assistant", "content": "a1"},
		{"role": "user", "content": "u2"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, req, []msgExpect{
		{Role: "user", Content: "s1" + "\n\n" + "s2" + "\n\n" + "s3" + systemMergeFormat + "u1"},
		{Role: "assistant", Content: "a1"},
		{Role: "user", Content: "u2"},
	})
	assertStripped(t, req, "system_messages=3")
}

// systemMergeTable covers the merge edge cases: multiple system messages,
// system-only conversations (now valid), interleaved system messages, empty
// system content, and multipart system content.
func TestParseRequestSystemMergeTable(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantMsgs  []msgExpect
		wantCount string // "" = no system_messages entry expected
		wantErr   string // "" = expect success
	}{
		{
			name: "multiple system messages merge in original order",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": "first"},
				{"role": "system", "content": "second"},
				{"role": "user", "content": "question"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "first\n\nsecond" + systemMergeFormat + "question"},
			},
			wantCount: "system_messages=2",
		},
		{
			name: "system-only conversation becomes a user message",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": "only system"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "only system"},
			},
			wantCount: "system_messages=1",
		},
		{
			name: "system between user turns still merges into the first user",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "user", "content": "u1"},
				{"role": "system", "content": "mid"},
				{"role": "assistant", "content": "a1"},
				{"role": "user", "content": "u2"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "mid" + systemMergeFormat + "u1"},
				{Role: "assistant", Content: "a1"},
				{Role: "user", Content: "u2"},
			},
			wantCount: "system_messages=1",
		},
		{
			name: "empty system content skipped without counting",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": ""},
				{"role": "system", "content": "real"},
				{"role": "user", "content": "q"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "real" + systemMergeFormat + "q"},
			},
			wantCount: "system_messages=1",
		},
		{
			name: "multipart system content concatenates text parts",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": [
					{"type": "text", "text": "part one"},
					{"type": "text", "text": "part two"}
				]},
				{"role": "user", "content": "q"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "part one\npart two" + systemMergeFormat + "q"},
			},
			wantCount: "system_messages=1",
		},
		{
			name: "system with only junk parts is skipped",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": [
					{"type": "audio", "audio": "x"}
				]},
				{"role": "user", "content": "q"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "q"},
			},
			wantCount: "",
		},
		{
			name: "all system messages empty still errors",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": ""}
			]}`,
			wantErr: "messages must not be empty",
		},
		{
			name: "no user message: system merges into new leading user message",
			body: `{"model": "deepseek-flash", "messages": [
				{"role": "system", "content": "s1"},
				{"role": "assistant", "content": "a1"},
				{"role": "system", "content": "s2"}
			]}`,
			wantMsgs: []msgExpect{
				{Role: "user", Content: "s1\n\ns2"},
				{Role: "assistant", Content: "a1"},
			},
			wantCount: "system_messages=2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseRequest(tt.body)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got success: %+v", tt.wantErr, req.Messages)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertMessages(t, req, tt.wantMsgs)
			if tt.wantCount == "" {
				for _, s := range req.Stripped {
					if strings.HasPrefix(s, "system_messages") {
						t.Errorf("unexpected stripped entry %q", s)
					}
				}
			} else {
				assertStripped(t, req, tt.wantCount)
			}
		})
	}
}

func TestParseRequestToolCallsCarrier(t *testing.T) {
	// Carrier without content → dropped; carrier with content → kept.
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [
		{"role": "user", "content": "go"},
		{"role": "assistant", "tool_calls": [{"id": "1", "function": {"name": "x", "arguments": "{}"}}]},
		{"role": "assistant", "content": "did it", "function_call": {"name": "x", "arguments": "{}"}},
		{"role": "user", "content": "thanks"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, req, []msgExpect{
		{Role: "user", Content: "go"},
		{Role: "assistant", Content: "did it"},
		{Role: "user", Content: "thanks"},
	})
	assertStripped(t, req, "tool_messages=1")
}

func TestParseRequestToolRoleMessages(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [
		{"role": "user", "content": "go"},
		{"role": "tool", "content": "result"},
		{"role": "tool", "content": ""},
		{"role": "tool", "tool_call_id": "1", "content": "another result"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, req, []msgExpect{
		{Role: "user", Content: "go"},
		{Role: "tool", Content: "result"},
		{Role: "tool", Content: "another result"},
	})
	assertStripped(t, req, "tool_messages=1")
}

func TestParseRequestJunkContentParts(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [
		{"role": "user", "content": [
			{"type": "text", "text": "keep me"},
			{"type": "audio", "audio": "x"},
			{"type": "file", "file": {"url": "http://x"}},
			{"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}}
		]},
		{"role": "user", "content": [
			{"type": "video", "video": "x"},
			{"type": "audio", "audio": "y"}
		]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, req, []msgExpect{
		{Role: "user", Parts: []ContentPart{
			{Type: "text", Text: "keep me"},
			{Type: "image_url", ImageURL: "data:image/png;base64,aGVsbG8="},
		}},
		// All-junk array → empty parts slice, message survives as empty text
		// (same shape as a literal "content": []).
		{Role: "user", Parts: []ContentPart{}},
	})
	assertStripped(t, req, "junk_content_parts=4")
}

func TestParseRequestEmptyMessagesStillErrors(t *testing.T) {
	if _, err := ParseRequest(`{"model": "deepseek-flash", "messages": []}`); err == nil {
		t.Fatal("empty messages must error")
	}
}

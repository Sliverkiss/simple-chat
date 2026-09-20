package openai

import (
	"encoding/json"
	"strings"
	"testing"
)

// The thinking switch: `"thinking": {"type": "enabled"|"disabled"}` on the
// request body. Absent (or null) = enabled — deep thinking is default ON.

func TestParseRequestThinkingDefaultOn(t *testing.T) {
	req, err := ParseRequest(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !req.ThinkingEnabled {
		t.Error("absent thinking switch must default to enabled")
	}
}

func TestParseRequestThinkingNullIsDefault(t *testing.T) {
	req, err := ParseRequest(`{"model":"deepseek-flash","thinking":null,"messages":[{"role":"user","content":"hi"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !req.ThinkingEnabled {
		t.Error("null thinking switch must default to enabled")
	}
}

func TestParseRequestThinkingExplicitEnabled(t *testing.T) {
	req, err := ParseRequest(`{"model":"deepseek-flash","thinking":{"type":"enabled"},"messages":[{"role":"user","content":"hi"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !req.ThinkingEnabled {
		t.Error("explicit enabled must parse as enabled")
	}
}

func TestParseRequestThinkingExplicitDisabled(t *testing.T) {
	req, err := ParseRequest(`{"model":"deepseek-flash","thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if req.ThinkingEnabled {
		t.Error("explicit disabled must parse as disabled")
	}
}

func TestParseRequestThinkingMalformedValue(t *testing.T) {
	for _, body := range []string{
		`{"model":"deepseek-flash","thinking":{"type":"banana"},"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"deepseek-flash","thinking":{"type":123},"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"deepseek-flash","thinking":{"level":"high"},"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"deepseek-flash","thinking":"disabled","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"deepseek-flash","thinking":true,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"deepseek-flash","thinking":[],"messages":[{"role":"user","content":"hi"}]}`,
	} {
		if _, err := ParseRequest(body); err == nil {
			t.Errorf("malformed switch must be rejected: %s", body)
		}
	}
}

// Non-stream response must carry message.reasoning_content alongside
// message.content; empty content with reasoning is a valid completion.

func TestCompletionResponseReasoningContent(t *testing.T) {
	resp := NewCompletionResponse("chatcmpl-1", "deepseek-flash", "because 2+2", "4", 42, "stop", nil)
	b, _ := json.Marshal(resp)
	s := string(b)
	for _, want := range []string{
		`"object":"chat.completion"`,
		`"content":"4"`,
		`"reasoning_content":"because 2+2"`,
		`"finish_reason":"stop"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("response missing %s in %s", want, s)
		}
	}
}

func TestCompletionResponseEmptyReasoningOmitted(t *testing.T) {
	resp := NewCompletionResponse("chatcmpl-1", "deepseek-flash", "", "4", 42, "stop", nil)
	b, _ := json.Marshal(resp)
	if strings.Contains(string(b), "reasoning_content") {
		t.Errorf("reasoning_content must be omitted when empty: %s", b)
	}
}

// Stream chunks: THINK deltas → delta.reasoning_content, before content
// deltas; both share the completion id.

func TestReasoningStreamChunkShape(t *testing.T) {
	chunk := NewReasoningStreamChunk("chatcmpl-1", "deepseek-flash", "ponder", "")
	b, _ := json.Marshal(chunk)
	s := string(b)
	for _, want := range []string{`"reasoning_content":"ponder"`, `"model":"deepseek-flash"`} {
		if !strings.Contains(s, want) {
			t.Errorf("chunk missing %s in %s", want, s)
		}
	}
	if strings.Contains(s, `"content"`) && strings.Contains(s, `"content":""`) == false {
		// content key may appear only as an explicit empty string; assert it is
		// never non-empty on a reasoning chunk.
		if strings.Contains(s, `"content":"`) && !strings.Contains(s, `"content":""`) {
			t.Errorf("reasoning chunk must not carry content: %s", s)
		}
	}
}

func TestReasoningStreamChunkRoleOnFirst(t *testing.T) {
	chunk := NewReasoningStreamChunk("chatcmpl-1", "deepseek-flash", "ponder", "assistant")
	b, _ := json.Marshal(chunk)
	if !strings.Contains(string(b), `"role":"assistant"`) {
		t.Errorf("first reasoning chunk must carry the role: %s", b)
	}
}

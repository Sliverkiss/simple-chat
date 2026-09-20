package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseRequestRejectsBadModel(t *testing.T) {
	_, err := ParseRequest(`{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]}`)
	if err == nil {
		t.Fatal("expected error for unknown model")
	}
	if !strings.Contains(err.Error(), "deepseek-flash") {
		t.Errorf("error should name the supported model: %v", err)
	}
}

// TestParseRequestRejectsTools used to assert tools → 400. Inverted by
// TASK_STRIP_FIELDS: tools are silently stripped now; the accepted case is
// covered by strip_test.go (TestParseRequestToolsAccepted).
func TestParseRequestRejectsTools(t *testing.T) {
	req, err := ParseRequest(`{"model": "deepseek-flash", "messages": [{"role": "user", "content": "hi"}], "tools": [{"type": "function"}]}`)
	if err != nil {
		t.Fatalf("tools must be stripped, not rejected: %v", err)
	}
	if len(req.Messages) != 1 {
		t.Errorf("messages = %+v", req.Messages)
	}
}

func TestParseRequestRejectsEmptyMessages(t *testing.T) {
	if _, err := ParseRequest(`{"model": "deepseek-flash", "messages": []}`); err == nil {
		t.Fatal("empty messages must error")
	}
}

func TestParseRequestAcceptsValid(t *testing.T) {
	req, err := ParseRequest(`{
		"model": "deepseek-flash",
		"stream": true,
		"temperature": 0.5,
		"messages": [
			{"role": "system", "content": "You are terse."},
			{"role": "user", "content": "hello"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if !req.Stream {
		t.Error("stream flag lost")
	}
	if req.Temperature != 0.5 {
		t.Errorf("temperature = %v", req.Temperature)
	}
}

// TestParseRequestAcceptsValid exercises the OpenAI multipart content format:
// a message whose content is an array of {type, text} and {type, image_url}
// parts must parse without error. (Regression: Message.Content used to be a
// plain string, so unmarshal failed on content arrays before decodeMultipart
// ever ran — caught live in the image smoke test.)
func TestParseRequestAcceptsMultipartContent(t *testing.T) {
	req, err := ParseRequest(`{
		"model": "deepseek-flash",
		"messages": [{
			"role": "user",
			"content": [
				{"type": "text", "text": "describe this image"},
				{"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}}
			]
		}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(req.Messages))
	}
	m := req.Messages[0]
	if m.Content != "" {
		t.Errorf("string content should be empty for array message, got %q", m.Content)
	}
	if len(m.ContentParts) != 2 {
		t.Fatalf("content parts = %d, want 2: %+v", len(m.ContentParts), m.ContentParts)
	}
	if m.ContentParts[0].Type != "text" || m.ContentParts[0].Text != "describe this image" {
		t.Errorf("text part wrong: %+v", m.ContentParts[0])
	}
	if m.ContentParts[1].Type != "image_url" || m.ContentParts[1].ImageURL != "data:image/png;base64,aGVsbG8=" {
		t.Errorf("image part wrong: %+v", m.ContentParts[1])
	}
}

func TestFlattenMessagesRoleTagged(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "be brief"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "hello"},
		{Role: "user", Content: "bye"},
	}
	got := FlattenMessages(msgs)
	for _, want := range []string{"system:", "user:", "assistant:"} {
		if !strings.Contains(got, want) {
			t.Errorf("flattened prompt missing role tag %q: %q", want, got)
		}
	}
	if !strings.Contains(got, "be brief") || !strings.Contains(got, "bye") {
		t.Errorf("content lost: %q", got)
	}
	// System message must be flattened like any other role, not injected.
	if strings.Count(got, "system:") != 1 {
		t.Errorf("system should appear exactly once as a tag: %q", got)
	}
}

func TestFlattenMessagesMultipartContent(t *testing.T) {
	msgs := []Message{{
		Role: "user",
		ContentParts: []ContentPart{
			{Type: "text", Text: "what is in this image"},
			{Type: "image_url", ImageURL: "data:image/png;base64,aGVsbG8="},
		},
	}}
	got := FlattenMessages(msgs)
	if !strings.Contains(got, "what is in this image") {
		t.Errorf("text part lost: %q", got)
	}
	if strings.Contains(got, "aGVsbG8=") {
		t.Errorf("image data must not leak into prompt: %q", got)
	}
}

func TestExtractImagesDataURL(t *testing.T) {
	msgs := []Message{{
		Role: "user",
		ContentParts: []ContentPart{
			{Type: "text", Text: "look"},
			{Type: "image_url", ImageURL: "data:image/png;base64,aGVsbG8="},
		},
	}}
	imgs, err := ExtractImages(context.Background(), msgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(imgs) != 1 {
		t.Fatalf("images = %d, want 1", len(imgs))
	}
	if string(imgs[0].Data) != "hello" {
		t.Errorf("decoded data = %q", imgs[0].Data)
	}
	if imgs[0].Ext != "png" {
		t.Errorf("ext = %q", imgs[0].Ext)
	}
}

func TestExtractImagesHTTPURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("pngbytes"))
	}))
	defer srv.Close()

	msgs := []Message{{
		Role: "user",
		ContentParts: []ContentPart{
			{Type: "image_url", ImageURL: srv.URL + "/photo.png"},
		},
	}}
	imgs, err := ExtractImages(context.Background(), msgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(imgs) != 1 {
		t.Fatalf("images = %d, want 1", len(imgs))
	}
	if string(imgs[0].Data) != "pngbytes" {
		t.Errorf("fetched data = %q", imgs[0].Data)
	}
	if imgs[0].Ext != "png" {
		t.Errorf("ext = %q", imgs[0].Ext)
	}
}

func TestExtractImagesRejectsNonImageContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("<html>"))
	}))
	defer srv.Close()
	msgs := []Message{{
		Role: "user",
		ContentParts: []ContentPart{
			{Type: "image_url", ImageURL: srv.URL + "/evil.html"},
		},
	}}
	if imgs, err := ExtractImages(context.Background(), msgs); err == nil && len(imgs) != 0 {
		t.Error("non-image content-type must be rejected")
	}
}

func TestStreamChunkShape(t *testing.T) {
	chunk := NewStreamChunk("chatcmpl-1", "deepseek-flash", "hi", "assistant")
	b, _ := json.Marshal(chunk)
	s := string(b)
	for _, want := range []string{`"object":"chat.completion.chunk"`, `"role":"assistant"`, `"model":"deepseek-flash"`} {
		if !strings.Contains(s, strings.ReplaceAll(want, " ", "")) {
			t.Errorf("chunk missing %s in %s", want, s)
		}
	}
}

func TestFinalChunkShape(t *testing.T) {
	chunk := NewFinalChunk("chatcmpl-1", "deepseek-flash", 41, "stop", nil)
	b, _ := json.Marshal(chunk)
	s := string(b)
	if !strings.Contains(s, `"finish_reason":"stop"`) {
		t.Errorf("missing finish_reason: %s", s)
	}
	if !strings.Contains(s, `"total_tokens":41`) {
		t.Errorf("missing usage: %s", s)
	}
}

func TestNonStreamResponseShape(t *testing.T) {
	resp := NewCompletionResponse("chatcmpl-1", "deepseek-flash", "", "the answer", 42, "stop", nil)
	b, _ := json.Marshal(resp)
	s := string(b)
	for _, want := range []string{`"object":"chat.completion"`, `"content":"the answer"`, `"total_tokens":42`, `"finish_reason":"stop"`} {
		if !strings.Contains(s, want) {
			t.Errorf("response missing %s in %s", want, s)
		}
	}
}

func TestModelsResponse(t *testing.T) {
	b, _ := json.Marshal(ModelsResponse())
	s := string(b)
	if !strings.Contains(s, "deepseek-flash") {
		t.Errorf("models must list deepseek-flash: %s", s)
	}
	if !strings.Contains(s, `"object":"list"`) {
		t.Errorf("models must be an object list: %s", s)
	}
}

func TestErrorBody(t *testing.T) {
	b, _ := json.Marshal(ErrorBody("boom", "invalid_request_error", "bad_thing"))
	s := string(b)
	if !strings.Contains(s, `"message":"boom"`) || !strings.Contains(s, `"type":"invalid_request_error"`) {
		t.Errorf("error shape wrong: %s", s)
	}
}

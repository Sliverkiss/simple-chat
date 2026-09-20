// Package openai implements the OpenAI-compatible surface: request parsing,
// message flattening, image extraction, and response emission.
package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ModelName is the only model this proxy serves.
const ModelName = "deepseek-flash"

// ContentPart is one element of a multipart content array.
type ContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"` // OpenAI: image_url: {url}
}

// Message is one chat message. Content may be a plain string (Content) or a
// multipart array (ContentParts) — UnmarshalJSON accepts both. Parse-time
// stats (junkParts, hasName) feed the sanitize report; tool_calls /
// function_call / name / tool_call_id fields are never parsed.
type Message struct {
	Role         string        `json:"role"`
	Content      string        `json:"content,omitempty"`
	ContentParts []ContentPart `json:"-"`
	junkParts    int
	hasName      bool
}

// UnmarshalJSON decodes a message whose content is either a plain string or
// an OpenAI multipart content array. Part types other than text/image_url
// are dropped and counted (junkParts); the name field is ignored and
// counted; tool_calls/function_call/tool_call_id are ignored outright —
// their residue shows up as a dropped empty message in sanitizeMessages.
func (m *Message) UnmarshalJSON(data []byte) error {
	var raw struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
		Name    json.RawMessage `json:"name"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.Role = raw.Role
	m.hasName = rawJunkPresent(raw.Name)
	trimmed := strings.TrimSpace(string(raw.Content))
	switch {
	case trimmed == "" || trimmed == "null":
		return nil
	case trimmed[0] == '"':
		return json.Unmarshal(raw.Content, &m.Content)
	case trimmed[0] == '[':
		var parts []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			ImageURL *struct {
				URL string `json:"url"`
			} `json:"image_url"`
		}
		if err := json.Unmarshal(raw.Content, &parts); err != nil {
			return fmt.Errorf("invalid content array: %w", err)
		}
		m.ContentParts = make([]ContentPart, 0, len(parts))
		for _, p := range parts {
			if p.Type == "image_url" && p.ImageURL != nil {
				m.ContentParts = append(m.ContentParts, ContentPart{Type: "image_url", ImageURL: p.ImageURL.URL})
				continue
			}
			if p.Type == "text" {
				m.ContentParts = append(m.ContentParts, ContentPart{Type: "text", Text: p.Text})
				continue
			}
			m.junkParts++
		}
		return nil
	default:
		return errors.New("content must be a string or an array of content parts")
	}
}

// rawJunkPresent reports whether a junk field carried an actual value
// (absent, null, and empty array all count as absent).
func rawJunkPresent(v json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(v))
	return trimmed != "" && trimmed != "null" && trimmed != "[]"
}

// Request is the parsed /v1/chat/completions body.
type Request struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	Temperature float64   `json:"temperature"`
	TopP        float64   `json:"top_p"`
	MaxTokens   int       `json:"max_tokens"`
	// ThinkingEnabled is the deep-thinking state sent upstream. Default
	// (switch absent) = true — deep thinking is ON by default.
	ThinkingEnabled bool
	// SearchEnabled is the web-search state sent upstream. Default (switch
	// absent) = false — search slows responses and is wrong for bulk text
	// processing (web-search-research.md).
	SearchEnabled bool
	// Stripped lists what sanitization removed or restructured in this
	// request — field names plus counters like "system_messages=2" (system
	// messages MERGED into the first user message, not dropped). Empty =
	// nothing stripped. The server logs it as one debug line; nothing else.
	Stripped []string
}

// parseRaw mirrors the request body: kept fields plus the known-junk fields
// (enumerated only so stripping is visible in the debug log). Anything not
// listed here is dropped by json.Unmarshal ignoring unknown keys.
type parseRaw struct {
	Model       string          `json:"model"`
	Messages    json.RawMessage `json:"messages"`
	Stream      bool            `json:"stream"`
	Temperature float64         `json:"temperature"`
	TopP        float64         `json:"top_p"`
	MaxTokens   int             `json:"max_tokens"`
	Thinking    json.RawMessage `json:"thinking"`
	Search      json.RawMessage `json:"search"`

	Tools             json.RawMessage `json:"tools"`
	ToolChoice        json.RawMessage `json:"tool_choice"`
	Functions         json.RawMessage `json:"functions"`
	FunctionCall      json.RawMessage `json:"function_call"`
	ParallelToolCalls json.RawMessage `json:"parallel_tool_calls"`
	Logprobs          json.RawMessage `json:"logprobs"`
	TopLogprobs       json.RawMessage `json:"top_logprobs"`
	LogitBias         json.RawMessage `json:"logit_bias"`
	User              json.RawMessage `json:"user"`
	Store             json.RawMessage `json:"store"`
	Metadata          json.RawMessage `json:"metadata"`
	ServiceTier       json.RawMessage `json:"service_tier"`
	ResponseFormat    json.RawMessage `json:"response_format"`
	Seed              json.RawMessage `json:"seed"`
	Stop              json.RawMessage `json:"stop"`
	N                 json.RawMessage `json:"n"`
	StreamOptions     json.RawMessage `json:"stream_options"`
	PresencePenalty   json.RawMessage `json:"presence_penalty"`
	FrequencyPenalty  json.RawMessage `json:"frequency_penalty"`
	Prediction        json.RawMessage `json:"prediction"`
}

// ParseRequest decodes, sanitizes, and validates the request body.
// Sanitization is silent by design (docs-spec-field-strip.md): the upstream
// has no tool calling and no system role, so tool-calling fields are
// stripped — never rejected — and system messages are merged into the
// first user message rather than dropped (TASK_SYSTEM_MERGE), so clients
// that ride prompt engineering on the system role keep working. What
// remains is exactly what the gateway can serve.
func ParseRequest(body string) (*Request, error) {
	var raw parseRaw
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	if raw.Model != ModelName {
		return nil, fmt.Errorf("unknown model %q: this proxy serves %q only", raw.Model, ModelName)
	}
	stripped := raw.strippedFields()
	var msgs []Message
	if err := json.Unmarshal(raw.Messages, &msgs); err != nil {
		return nil, fmt.Errorf("invalid messages: %w", err)
	}
	msgs, stripped = sanitizeMessages(msgs, stripped)
	if len(msgs) == 0 {
		if len(stripped) > 0 {
			return nil, errors.New("messages must not be empty (all messages were removed by sanitization: tool calls are not supported and system messages had no usable content)")
		}
		return nil, errors.New("messages must not be empty")
	}
	thinking, err := parseThinkingSwitch(raw.Thinking)
	if err != nil {
		return nil, err
	}
	search, err := parseSearchSwitch(raw.Search)
	if err != nil {
		return nil, err
	}
	return &Request{
		Model:           raw.Model,
		Messages:        msgs,
		Stream:          raw.Stream,
		Temperature:     raw.Temperature,
		TopP:            raw.TopP,
		MaxTokens:       raw.MaxTokens,
		ThinkingEnabled: thinking,
		SearchEnabled:   search,
		Stripped:        stripped,
	}, nil
}

// strippedFields reports the junk request-level fields present in the body.
// A field set to null or an empty array is invisible to the client anyway —
// it counts as absent.
func (raw *parseRaw) strippedFields() []string {
	present := []struct {
		name string
		val  json.RawMessage
	}{
		{"tools", raw.Tools},
		{"tool_choice", raw.ToolChoice},
		{"functions", raw.Functions},
		{"function_call", raw.FunctionCall},
		{"parallel_tool_calls", raw.ParallelToolCalls},
		{"logprobs", raw.Logprobs},
		{"top_logprobs", raw.TopLogprobs},
		{"logit_bias", raw.LogitBias},
		{"user", raw.User},
		{"store", raw.Store},
		{"metadata", raw.Metadata},
		{"service_tier", raw.ServiceTier},
		{"response_format", raw.ResponseFormat},
		{"seed", raw.Seed},
		{"stop", raw.Stop},
		{"n", raw.N},
		{"stream_options", raw.StreamOptions},
		{"presence_penalty", raw.PresencePenalty},
		{"frequency_penalty", raw.FrequencyPenalty},
		{"prediction", raw.Prediction},
	}
	var stripped []string
	for _, p := range present {
		trimmed := strings.TrimSpace(string(p.val))
		if trimmed == "" || trimmed == "null" || trimmed == "[]" || trimmed == "0" {
			continue
		}
		stripped = append(stripped, p.name)
	}
	return stripped
}

// systemMergeSeparator sits between the merged system block and the
// original first-user content: blocks are joined with a blank line, then
// one "---" divider line separates instructions from the user turn
// (docs-spec-field-strip.md).
const systemMergeSeparator = "\n\n---\n\n"

// sanitizeMessages applies the message-level rules: system messages are
// MERGED, not dropped — their contents are collected in order and
// prepended to the first user message (or a new leading user message when
// the conversation has none); messages left with no content by tool-call
// stripping are dropped; junk content-part types are removed at parse;
// name fields are ignored at parse. Returns the surviving messages and
// the appended strip report.
func sanitizeMessages(msgs []Message, stripped []string) ([]Message, []string) {
	var systemBlocks []string
	emptyToolCount, junkPartCount, nameCount := 0, 0, 0
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		switch {
		case m.Role == "system":
			if text := m.systemText(); text != "" {
				systemBlocks = append(systemBlocks, text)
			}
			continue
		case (m.Role == "assistant" || m.Role == "tool") && m.Content == "" && len(m.ContentParts) == 0:
			// An assistant carrier with only tool_calls, or an empty tool
			// result — nothing survives stripping, so neither does the
			// message. User messages with empty/junk-stripped content
			// still count as turn boundaries and stay.
			emptyToolCount++
			continue
		}
		junkPartCount += m.junkParts
		nameCount += boolToInt(m.hasName)
		out = append(out, m)
	}
	if len(systemBlocks) > 0 {
		out = mergeSystemBlocks(out, strings.Join(systemBlocks, "\n\n"))
		stripped = append(stripped, fmt.Sprintf("system_messages=%d", len(systemBlocks)))
	}
	if emptyToolCount > 0 {
		stripped = append(stripped, fmt.Sprintf("tool_messages=%d", emptyToolCount))
	}
	if junkPartCount > 0 {
		stripped = append(stripped, fmt.Sprintf("junk_content_parts=%d", junkPartCount))
	}
	if nameCount > 0 {
		stripped = append(stripped, fmt.Sprintf("message_names=%d", nameCount))
	}
	return out, stripped
}

// systemText renders a system message's usable content: the plain string
// when present, otherwise its text parts joined with newlines. Empty
// result means the message carries nothing worth merging.
func (m Message) systemText() string {
	if m.Content != "" {
		return m.Content
	}
	var texts []string
	for _, p := range m.ContentParts {
		if p.Type == "text" && p.Text != "" {
			texts = append(texts, p.Text)
		}
	}
	return strings.Join(texts, "\n")
}

// mergeSystemBlocks prepends the merged system text to the first user
// message, separated by systemMergeSeparator. A conversation without a
// user message gets a new leading user message carrying only the system
// text — a system-only transcript is valid input (TASK_SYSTEM_MERGE).
// The separator is omitted when the user message has nothing to separate
// from (empty string content and no parts).
func mergeSystemBlocks(out []Message, system string) []Message {
	for i := range out {
		if out[i].Role != "user" {
			continue
		}
		switch {
		case out[i].Content != "":
			out[i].Content = system + systemMergeSeparator + out[i].Content
		case len(out[i].ContentParts) > 0:
			out[i].Content = system + systemMergeSeparator
		default:
			out[i].Content = system
		}
		return out
	}
	merged := make([]Message, 0, len(out)+1)
	merged = append(merged, Message{Role: "user", Content: system})
	return append(merged, out...)
}

// boolToInt keeps sanitizeMessages free of an if per flag.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// parseThinkingSwitch decodes the per-request thinking switch:
//
//	"thinking": {"type": "enabled"}  → true
//	"thinking": {"type": "disabled"} → false
//	absent or null                   → true (deep thinking default ON)
//
// The switch shape mirrors the openai/anthropic reasoning convention
// (reasoning_effort / thinking.type). Anything else is a 400.
func parseThinkingSwitch(raw json.RawMessage) (bool, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return true, nil
	}
	var sw struct {
		Type json.RawMessage `json:"type"`
	}
	if err := json.Unmarshal(raw, &sw); err != nil {
		return false, errors.New(`invalid "thinking" field: expected {"type": "enabled"|"disabled"}`)
	}
	var typ string
	if err := json.Unmarshal(sw.Type, &typ); err != nil {
		return false, errors.New(`invalid "thinking" field: expected {"type": "enabled"|"disabled"}`)
	}
	switch typ {
	case "enabled":
		return true, nil
	case "disabled":
		return false, nil
	}
	return false, fmt.Errorf(`invalid "thinking.type" %q: expected "enabled" or "disabled"`, typ)
}

// parseSearchSwitch decodes the per-request web-search switch:
//
//	"search": {"type": "enabled"}  → true
//	"search": {"type": "disabled"} → false
//	absent or null                  → false (search default OFF — it slows
//	                                  responses; wrong default for bulk text)
//
// Same shape as the thinking switch. Anything else is a 400.
func parseSearchSwitch(raw json.RawMessage) (bool, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return false, nil
	}
	var sw struct {
		Type json.RawMessage `json:"type"`
	}
	if err := json.Unmarshal(raw, &sw); err != nil {
		return false, errors.New(`invalid "search" field: expected {"type": "enabled"|"disabled"}`)
	}
	var typ string
	if err := json.Unmarshal(sw.Type, &typ); err != nil {
		return false, errors.New(`invalid "search" field: expected {"type": "enabled"|"disabled"}`)
	}
	switch typ {
	case "enabled":
		return true, nil
	case "disabled":
		return false, nil
	}
	return false, fmt.Errorf(`invalid "search.type" %q: expected "enabled" or "disabled"`, typ)
}

// FlattenMessages renders the transcript as role-tagged text. System messages
// are tagged like every other role — no special-casing, no injection.
// Image parts are skipped (they travel via ref_file_ids).
func FlattenMessages(msgs []Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Role)
		b.WriteString(": ")
		if m.Content != "" {
			b.WriteString(m.Content)
		}
		for _, p := range m.ContentParts {
			if p.Type == "text" && p.Text != "" {
				if b.Len() > 0 && !strings.HasSuffix(b.String(), ": ") {
					b.WriteString("\n")
				}
				b.WriteString(p.Text)
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

// Image is one extracted image awaiting upload.
type Image struct {
	Data []byte
	Ext  string // png/jpg/jpeg/webp/gif
}

// imageExtByMIME maps common image content types to file extensions.
// The upload filename MUST carry a supported extension — the server types
// files by suffix (recon: dsh notes).
func imageExtByMIME(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	switch ct {
	case "image/png":
		return "png"
	case "image/jpeg", "image/jpg":
		return "jpg"
	case "image/webp":
		return "webp"
	case "image/gif":
		return "gif"
	}
	return ""
}

// imageFetchTimeout bounds each server-side image fetch. A hanging image
// host must fail the request cleanly, never wedge the handler (and with it a
// pool lease) indefinitely.
const imageFetchTimeout = 30 * time.Second

// imageHTTPClient fetches client-supplied image URLs. Dedicated client: the
// default one carries no timeout, and the upstream client's transport/tuning
// must not be shared with arbitrary external hosts.
var imageHTTPClient = &http.Client{Timeout: imageFetchTimeout}

// ExtractImages pulls image_url parts out of the messages: data URLs decode
// inline; http(s) URLs are fetched server-side (10 MB cap, image content type
// required). The fetch honors ctx; any failed fetch is an error — silently
// dropping a requested image would produce a completion the client believes
// includes it.
func ExtractImages(ctx context.Context, msgs []Message) ([]Image, error) {
	var imgs []Image
	for _, m := range msgs {
		for _, p := range m.ContentParts {
			if p.Type != "image_url" || p.ImageURL == "" {
				continue
			}
			if data, ext, ok := decodeDataURL(p.ImageURL); ok {
				imgs = append(imgs, Image{Data: data, Ext: ext})
				continue
			}
			data, ext, err := fetchImage(ctx, p.ImageURL)
			if err != nil {
				return nil, err
			}
			if ext != "" {
				imgs = append(imgs, Image{Data: data, Ext: ext})
			}
		}
	}
	return imgs, nil
}

// decodeDataURL handles data:image/...;base64,<payload> URLs.
func decodeDataURL(u string) ([]byte, string, bool) {
	if !strings.HasPrefix(u, "data:") {
		return nil, "", false
	}
	rest := u[len("data:"):]
	semi := strings.Index(rest, ",")
	if semi < 0 {
		return nil, "", false
	}
	meta := rest[:semi]
	payload := rest[semi+1:]
	if !strings.HasSuffix(meta, ";base64") {
		return nil, "", false
	}
	ext := imageExtByMIME(strings.TrimSuffix(meta, ";base64"))
	if ext == "" {
		return nil, "", false
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, "", false
	}
	if len(data) > 10<<20 {
		return nil, "", false
	}
	return data, ext, true
}

// fetchImage downloads an http(s) image URL, honoring ctx and bounded by
// imageFetchTimeout. ext == "" with a nil error means "not an http(s) URL" —
// the only non-error skip left.
func fetchImage(ctx context.Context, u string) ([]byte, string, error) {
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return nil, "", nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil) //nolint:gosec // client-supplied URL by design
	if err != nil {
		return nil, "", fmt.Errorf("image url %q: %w", firstSegment(u), err)
	}
	resp, err := imageHTTPClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("image fetch %q: %w", firstSegment(u), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("image fetch %q: http %d", firstSegment(u), resp.StatusCode)
	}
	ext := imageExtByMIME(resp.Header.Get("Content-Type"))
	if ext == "" {
		return nil, "", fmt.Errorf("image fetch %q: unsupported content type %q", firstSegment(u), resp.Header.Get("Content-Type"))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20+1))
	if err != nil {
		return nil, "", fmt.Errorf("image fetch %q: %w", firstSegment(u), err)
	}
	if len(data) > 10<<20 {
		return nil, "", fmt.Errorf("image fetch %q: exceeds 10MB cap", firstSegment(u))
	}
	return data, ext, nil
}

// firstSegment keeps error messages short: scheme://host/path, no query
// string — client-supplied URLs can carry tokens in query params.
func firstSegment(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	if len(u) > 128 {
		return u[:128]
	}
	return u
}

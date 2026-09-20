// Package sse interprets the upstream JSON-patch SSE stream into text deltas,
// reasoning deltas, usage, and terminal state.
//
// Stream shape (see recon.md §3.5): `data:` lines carry {p, o, v} patch ops.
// Omitted p/o repeat the previous op. The first data payload carries the full
// response object under v.response. The stream ends with `event: close`;
// `data: [DONE]` is tolerated but not expected.
package sse

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Usage mirrors the OpenAI usage object, filled from accumulated_token_usage.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Delta is one incremental output event. Text and Reasoning are mutually
// exclusive per delta.
type Delta struct {
	Text      string
	Reasoning string
}

// StreamError is an upstream mid-stream error delivered via `event: hint`
// with payload {"type":"error","content":...,"finish_reason":...}. Notable
// finish_reason: "parallel_chat_limit" (another generation is running on the
// same account — retry, ideally on another account).
type StreamError struct {
	Type          string
	Content       string
	ClearResponse bool
	FinishReason  string
}

func (e *StreamError) Error() string {
	if e.FinishReason != "" {
		return fmt.Sprintf("stream error (%s): %s", e.FinishReason, e.Content)
	}
	return "stream error: " + e.Content
}

// IsParallelLimit reports whether the error is the per-account parallel
// generation limit.
func (e *StreamError) IsParallelLimit() bool {
	return e.FinishReason == "parallel_chat_limit"
}

// SearchResult is one structured hit from the upstream SEARCH fragment's
// results array (web-search-research.md): url/title/snippet plus citation
// keying (cite_index) and provenance (query_indexes into SearchQueries).
type SearchResult struct {
	URL          string  `json:"url"`
	Title        string  `json:"title"`
	Snippet      string  `json:"snippet"`
	SiteName     string  `json:"site_name,omitempty"`
	SiteIcon     string  `json:"site_icon,omitempty"`
	CiteIndex    int     `json:"cite_index"`
	PublishedAt  float64 `json:"published_at,omitempty"`
	QueryIndexes []int   `json:"query_indexes,omitempty"`
	Provider     *string `json:"provider,omitempty"`
}

// State is the accumulated interpreter state.
type State struct {
	Text      string
	Reasoning string
	Usage     Usage
	Finished  bool
	Err       error
	// SearchResults holds the structured web-search hits when the upstream ran
	// in search mode (search_enabled:true). Empty otherwise.
	SearchResults []SearchResult
	// SearchQueries holds the queries the upstream model generated for the
	// search fan-out, in order.
	SearchQueries []string
}

// Interpreter parses the upstream SSE incrementally. Feed and State may be
// called from different goroutines.
type Interpreter struct {
	mu       sync.Mutex
	state    State
	lastPath string
	lastOp   string
	lastKind string // fragment type that the current append path targets
	pending  []byte
	// lastEvent is the most recent `event:` line (empty before the first).
	lastEvent string
	// onDelta, if set, is invoked (under the interpreter lock) for every
	// content delta, in stream order.
	onDelta func(Delta)
}

// New returns a ready Interpreter.
func New() *Interpreter {
	return &Interpreter{lastKind: "RESPONSE"}
}

// OnDelta registers a callback invoked for each content delta, in order.
// Must be called before Feed.
func (it *Interpreter) OnDelta(fn func(Delta)) { it.onDelta = fn }

// Feed consumes a raw chunk of the SSE byte stream. Lines may split across
// chunks at any byte boundary.
func (it *Interpreter) Feed(chunk string) error {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.pending = append(it.pending, chunk...)
	for {
		idx := indexByte(it.pending, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(string(it.pending[:idx]), "\r")
		it.pending = it.pending[idx+1:]
		if err := it.handleLine(line); err != nil {
			return err
		}
	}
	return nil
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// handleLine processes one SSE line. `event:` lines set sawClose when they
// announce close; `data:` lines are patch ops or payloads.
func (it *Interpreter) handleLine(line string) error {
	if it.state.Finished {
		return nil
	}
	if !strings.HasPrefix(line, "data:") {
		if strings.HasPrefix(line, "event:") {
			it.lastEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			if it.lastEvent == "close" {
				it.state.Finished = true
			}
		}
		return nil
	}
	data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if data == "" {
		return nil
	}
	if data == "[DONE]" {
		it.state.Finished = true
		return nil
	}

	var op struct {
		P     string          `json:"p"`
		O     string          `json:"o"`
		V     json.RawMessage `json:"v"`
		Error json.RawMessage `json:"error"`
		Code  json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal([]byte(data), &op); err != nil {
		// Non-JSON payloads (e.g. update_session events use data: too) — ignore.
		return nil
	}
	// `event: hint` carries error payloads with their own shape; the generic
	// error/code fields below don't exist there.
	if it.lastEvent == "hint" {
		var hint struct {
			Type          string `json:"type"`
			Content       string `json:"content"`
			ClearResponse bool   `json:"clear_response"`
			FinishReason  string `json:"finish_reason"`
		}
		if err := json.Unmarshal([]byte(data), &hint); err == nil && hint.Type == "error" {
			it.state.Err = &StreamError{
				Type:          hint.Type,
				Content:       hint.Content,
				ClearResponse: hint.ClearResponse,
				FinishReason:  hint.FinishReason,
			}
			it.state.Finished = true
		}
		return nil
	}
	if op.Error != nil || hasContentFilterCode(op.Code) {
		detail := firstNonEmpty(string(op.Error), string(op.Code))
		if hasContentFilterCode(op.Code) || strings.Contains(string(op.Error), "content_filter") {
			// Typed so the gateway can answer a clean 400 content_filter
			// instead of a generic upstream failure.
			it.state.Err = fmt.Errorf("%w: %s", ErrContentFilter, detail)
			return nil
		}
		it.state.Err = fmt.Errorf("stream error: %s", detail)
		return nil
	}
	if len(op.V) == 0 {
		return nil
	}
	it.apply(op.P, op.O, op.V)
	return nil
}

func hasContentFilterCode(code json.RawMessage) bool {
	var s string
	if err := json.Unmarshal(code, &s); err == nil {
		return strings.EqualFold(strings.TrimSpace(s), "content_filter")
	}
	return false
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return "unknown error"
}

// apply dispatches a single patch operation. path/op may be empty, meaning
// "repeat the previous ones" — except that a BATCH never carries over to a
// path-less payload: on the live wire a bare {"v":"..."} delta after a
// response BATCH is fragment content (search mode, recon-data/search_sse.txt),
// so it routes through the initial-response handler like every other bare
// delta.
func (it *Interpreter) apply(path, op string, v json.RawMessage) {
	if path != "" {
		it.lastPath = path
	}
	if op != "" {
		it.lastOp = op
	}
	switch {
	case path == "" && op == "":
		it.applyInitialResponse(v)
	case it.lastOp == "BATCH" && path == "":
		it.applyInitialResponse(v)
	case it.lastOp == "BATCH":
		it.applyBatch(v)
	case it.lastPath == "response/status":
		it.applyStatus(v)
	default:
		it.applyPathValue(it.lastPath, v)
	}
}

// applyInitialResponse handles the first full-response payload
// {"v": {"response": {"fragments": [...]}}}, or — when p/o are absent and v
// is a bare string — a delta appended to the current fragment (ds2api
// semantics for path-less chunks).
func (it *Interpreter) applyInitialResponse(v json.RawMessage) {
	// Bare string with no p/o: append to the last fragment kind.
	var bare string
	if err := json.Unmarshal(v, &bare); err == nil {
		it.emitFragment(it.lastKind, bare)
		return
	}
	var full struct {
		Response struct {
			Status         string `json:"status"`
			AccumulatedUse int    `json:"accumulated_token_usage"`
			Fragments      []struct {
				Type    string `json:"type"`
				Content string `json:"content"`
				Queries []struct {
					Query string `json:"query"`
				} `json:"queries"`
			} `json:"fragments"`
		} `json:"response"`
	}
	if err := json.Unmarshal(v, &full); err != nil {
		return
	}
	r := full.Response
	for _, f := range r.Fragments {
		// The last fragment of the initial payload is the target of subsequent
		// bare-string deltas (omitted p/o): track its kind.
		it.lastKind = strings.ToUpper(f.Type)
		it.emitFragment(f.Type, f.Content)
		if strings.EqualFold(f.Type, "SEARCH") || strings.EqualFold(f.Type, "TOOL_SEARCH") {
			for _, q := range f.Queries {
				it.state.SearchQueries = append(it.state.SearchQueries, q.Query)
			}
		}
	}
	if r.AccumulatedUse > 0 {
		it.state.Usage.TotalTokens = r.AccumulatedUse
	}
	if strings.EqualFold(r.Status, "FINISHED") {
		it.state.Finished = true
	}
}

func (it *Interpreter) emitFragment(kind, content string) {
	if content == "" {
		return
	}
	if strings.EqualFold(kind, "THINK") || strings.EqualFold(kind, "THINKING") {
		it.state.Reasoning += content
		it.fire(Delta{Reasoning: content})
		return
	}
	it.state.Text += content
	it.fire(Delta{Text: content})
}

func (it *Interpreter) fire(d Delta) {
	if it.onDelta != nil {
		it.onDelta(d)
	}
}

func (it *Interpreter) applyBatch(v json.RawMessage) {
	var items []struct {
		P string          `json:"p"`
		V json.RawMessage `json:"v"`
	}
	if err := json.Unmarshal(v, &items); err != nil {
		return
	}
	for _, item := range items {
		switch item.P {
		case "accumulated_token_usage":
			var n int
			if err := json.Unmarshal(item.V, &n); err == nil {
				it.state.Usage.TotalTokens = n
			}
		case "fragments":
			// Search mode: the first RESPONSE fragment chunk arrives nested
			// inside a response BATCH as a fragments APPEND. Route it through
			// the same fragment-append handling as the top-level path.
			it.applyPathValue("response/fragments", item.V)
		case "results":
			// Nested results APPEND (search mode): same shape as the
			// response/fragments/-1/results path.
			it.applyPathValue("response/fragments/-1/results", item.V)
		default:
			// quasi_status, has_pending_fragment and other noise: skip.
		}
	}
}

func (it *Interpreter) applyStatus(v json.RawMessage) {
	var s string
	if err := json.Unmarshal(v, &s); err == nil && strings.EqualFold(strings.TrimSpace(s), "FINISHED") {
		it.state.Finished = true
	}
}

// applyPathValue handles APPEND/SET against concrete paths.
func (it *Interpreter) applyPathValue(path string, v json.RawMessage) {
	if isNoisePath(path) {
		return
	}
	if path == "response/fragments/-1/results" {
		// Structured search hits from the upstream SEARCH fragment
		// (web-search-research.md). Append: search+thinking mode emits one
		// array per TOOL_SEARCH round.
		var results []SearchResult
		if err := json.Unmarshal(v, &results); err == nil && len(results) > 0 {
			it.state.SearchResults = append(it.state.SearchResults, results...)
		}
		return
	}
	if path == "response/fragments/-1/content" || path == "response/content" {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return
		}
		it.emitFragment(it.lastKind, s)
		return
	}
	if path == "response/thinking_content" {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			it.state.Reasoning += s
			it.fire(Delta{Reasoning: s})
		}
		return
	}
	if path == "response/fragments" {
		// APPEND of a new fragment object (e.g. THINK fragment starting).
		var frags []struct {
			Type    string `json:"type"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(v, &frags); err == nil {
			for _, f := range frags {
				it.lastKind = strings.ToUpper(f.Type)
				it.emitFragment(f.Type, f.Content)
			}
			return
		}
		// Bare-string payload on the fragments path (p/o omitted after a
		// fragment APPEND): append to the current fragment's kind.
		var bare string
		if err := json.Unmarshal(v, &bare); err == nil {
			it.emitFragment(it.lastKind, bare)
		}
		return
	}
	// Unknown path with a string value: only a real fragment-content carryover
	// can reach here because noise paths were filtered. Ignore anything else.
}

func isNoisePath(path string) bool {
	switch {
	case path == "response/search_status":
		return true
	case strings.HasPrefix(path, "response/fragments/") && strings.HasSuffix(path, "/status"):
		return true
	}
	for _, noise := range []string{
		"quasi_status", "elapsed_secs", "token_usage",
		"pending_fragment", "conversation_mode",
	} {
		if strings.Contains(path, noise) {
			return true
		}
	}
	return false
}

// ErrStreamTerminated is returned when the stream ended with an error payload.
var ErrStreamTerminated = errors.New("stream terminated by error")

// ErrContentFilter marks an upstream content-filter rejection delivered as a
// data payload ({"code":"content_filter"} or an error object naming it).
// Match with errors.Is; the gateway maps it to a typed 400 content_filter.
var ErrContentFilter = errors.New("content_filter")

// Snapshot returns a copy of the current accumulated state.
func (it *Interpreter) Snapshot() State {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.state
}

// SetOnDelta registers a callback invoked for each content delta, in order.
// Must be called before the first Feed.
func (it *Interpreter) SetOnDelta(fn func(Delta)) { it.onDelta = fn }

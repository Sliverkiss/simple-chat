// Package server wires the HTTP surface: /v1/chat/completions, /v1/models,
// /healthz, and the accounts.json config loading.
package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/openai"
	"simple-chat/internal/sse"
	"simple-chat/internal/upstream"
)

// Config configures one server instance.
type Config struct {
	// UpstreamBase overrides the production upstream; tests inject an
	// httptest URL. Empty = production default.
	UpstreamBase string
	Accounts     []upstream.Account
	// MaxInflight caps concurrent requests per account (default 2).
	MaxInflight int
	// QueueWait bounds how long a request waits when every account is busy
	// (default 30s); exceeded → 429 + Retry-After.
	QueueWait time.Duration
	APIKey    string // optional static key; empty = auth disabled (open access)
	Logger    *log.Logger
	// DeleteQueueSize bounds pending async session deletes (default 256).
	DeleteQueueSize int
	// DeleteWorkers is the async delete worker count (default 2).
	DeleteWorkers int
	// MaxPromptChars caps the flattened prompt; over-cap answers 400 before
	// any upstream call. 0 = DefaultMaxPromptChars (DS_MAX_PROMPT_CHARS=0
	// semantics are resolved in main; a negative value disables the guard).
	MaxPromptChars int
	// ParkStore is the persistence sink for park transitions and device ids
	// (accountstore.Store). Nil = park state stays memory-only (the
	// pre-persistence behavior). Ownership stays with the caller (main
	// closes it after shutdown).
	ParkStore accountstore.Store
	// SessionCap bounds sessions kept per account (app-like accumulation by
	// default). 0 = keep everything; N = oldest sessions beyond N are
	// deleted through the async deleter (apk-behavior.md §8 D1).
	SessionCap int
	// CleanupInterval is the base wake interval of the human-paced session
	// cleanup (TASK_CLEANUP); each sleep is jittered ±50%. 0 (default) =
	// scheduler dormant. Resolved from DS_CLEANUP_INTERVAL in main.
	CleanupInterval time.Duration
	// CleanupFloor is the session count at/below which a cleanup episode
	// deletes nothing (humans keep their recent chats). 0 = default (5).
	// Resolved from DS_CLEANUP_FLOOR in main.
	CleanupFloor int
	// PurgeEnabled turns on the weekly purge-all-sessions scheduler
	// (TASK_PURGE): one background goroutine fires delete_all for every
	// healthy+warm account once a week, ±30m jitter. The zero value
	// (false) keeps the scheduler dormant — main resolves the DS_PURGE*
	// defaults, exactly like CleanupInterval.
	PurgeEnabled bool
	// PurgeWeekday is the weekly purge day, Monday-based (0=Monday ..
	// 6=Sunday — the DS_PURGE_WEEKDAY convention). Only meaningful with
	// PurgeEnabled; -1 disables.
	PurgeWeekday int
	// PurgeHour is the weekly purge hour (0-23). Only meaningful with
	// PurgeEnabled; resolved from DS_PURGE_HOUR in main (default 4).
	PurgeHour int
	// PurgeCatchUpMin/Max bound the startup catch-up delay of a missed
	// weekly window. Zero values take the production constants
	// (2-12s); shrunk by loop tests so a missed-window catch-up
	// doesn't starve the test deadline.
	PurgeCatchUpMin time.Duration
	PurgeCatchUpMax time.Duration
}

// DefaultMaxPromptChars is the conservative prompt cap. The upstream's
// observed hard limit is ~2,621,440 characters (audit §1.2); this sits below
// it with headroom so a legit-but-long request never trips the upstream's
// opaque biz rejection before our clean 400 fires.
const DefaultMaxPromptChars = 2_000_000

// Server holds the account pool and routes.
type Server struct {
	pool      *upstream.Pool
	logger    *log.Logger
	apiKey    string
	deleter   *asyncDeleter
	maxPrompt int
	sessions  *sessionRegistry
	cleanup   *cleanupScheduler
	purge     *purgeScheduler
	// store persists accounts for the admin API (upload/delete); nil when
	// the server runs store-less (admin mutations then answer 500 with a
	// clear message instead of mutating a pool that forgets on restart).
	store accountstore.Store
}

// NewServer builds the server from config.
func NewServer(cfg Config) (*Server, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[simple-chat] ", log.LstdFlags)
	}
	// Park persistence (TASK_MUTE): the store's Load has already stripped
	// expired parks; hand the pool a sink that writes every transition back.
	// Nil ParkStore = memory-only park state.
	var onParkPersist func(upstream.ParkRecord)
	if cfg.ParkStore != nil {
		onParkPersist = cfg.ParkStore.ApplyPark
	}
	// Login write-through (docs-spec-memory-first.md): a store that
	// implements ApplyLogin receives every fresh login token. Stores that
	// don't (JSON file) keep memory-only tokens — zero behavior change.
	var onLoginPersist func(upstream.LoginRecord)
	if cfg.ParkStore != nil {
		if al, ok := cfg.ParkStore.(interface {
			ApplyLogin(upstream.LoginRecord)
		}); ok {
			onLoginPersist = al.ApplyLogin
		}
	}
	pool, err := upstream.NewPool(cfg.Accounts, upstream.PoolConfig{
		BaseURL:         cfg.UpstreamBase,
		MaxInflight:     cfg.MaxInflight,
		QueueWait:       cfg.QueueWait,
		OnParkPersist:   onParkPersist,
		OnLoginPersist:  onLoginPersist,
		Logger: func(format string, args ...any) {
			logger.Printf(format, args...)
		},
	})
	if err != nil {
		return nil, err
	}
	deleter := newAsyncDeleter(deleterConfig{
		queueSize: cfg.DeleteQueueSize,
		workers:   cfg.DeleteWorkers,
	}, logger)
	cleanup := newCleanupScheduler(cleanupConfig{
		interval: cfg.CleanupInterval,
		floor:    cfg.CleanupFloor,
	}, pool, deleter, logger)
	cleanup.start()
	cleanup.logPolicy()
	sessions := newSessionRegistry(cfg.SessionCap, deleter)
	// Gate the purge: enabled + a valid weekday/hour, else dormant.
	purgeWeekday := -1
	if cfg.PurgeEnabled && cfg.PurgeWeekday >= 0 && cfg.PurgeWeekday <= 6 && cfg.PurgeHour >= 0 && cfg.PurgeHour <= 23 {
		purgeWeekday = cfg.PurgeWeekday
	}
	purge := newPurgeScheduler(purgeConfig{
		weekday:    purgeWeekday,
		hour:       cfg.PurgeHour,
		catchUpMin: cfg.PurgeCatchUpMin,
		catchUpMax: cfg.PurgeCatchUpMax,
	}, pool, sessions, logger)
	purge.start()
	purge.logPolicy()
	return &Server{
		pool:      pool,
		logger:    logger,
		apiKey:    cfg.APIKey,
		maxPrompt: cfg.MaxPromptChars,
		deleter:   deleter,
		sessions:  sessions,
		cleanup:   cleanup,
		purge:     purge,
		store:     cfg.ParkStore,
	}, nil
}

// Shutdown stops the cleanup and purge loops and drains pending async
// session deletes (bounded wait) after the HTTP listener has stopped.
// Call once, after the listener is closed.
func (s *Server) Shutdown() {
	s.cleanup.shutdown()
	s.purge.shutdown()
	s.deleter.shutdown()
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", s.requireAPIKey(s.handleChatCompletions))
	mux.Handle("POST /v1/web_search", s.requireAPIKey(s.handleWebSearch))
	mux.Handle("GET /v1/models", s.requireAPIKey(s.handleModels))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	mux.Handle("POST /admin/accounts", s.requireAPIKey(s.handleAdminUpload))
	mux.Handle("GET /admin/accounts", s.requireAPIKey(s.handleAdminList))
	mux.Handle("DELETE /admin/accounts/{id}", s.requireAPIKey(s.handleAdminDelete))
	return mux
}

// requireAPIKey enforces the optional static key. Empty key = open access.
func (s *Server) requireAPIKey(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" && !s.keyMatches(r) {
			writeError(w, http.StatusUnauthorized, "Invalid API key", "invalid_api_key", "invalid_api_key")
			return
		}
		next(w, r)
	})
}

// keyMatches checks Authorization: Bearer <key> or X-Api-Key: <key> in
// constant time.
func (s *Server) keyMatches(r *http.Request) bool {
	present := func(val string) bool {
		return subtle.ConstantTimeCompare([]byte(val), []byte(s.apiKey)) == 1
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if present(strings.TrimPrefix(h, "Bearer ")) {
			return true
		}
	}
	return present(r.Header.Get("X-Api-Key"))
}

func writeError(w http.ResponseWriter, status int, message, typ, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(openai.ErrorBody(message, typ, code))
}

// maxPromptChars resolves the effective prompt cap: 0 → default, negative →
// disabled.
func (s *Server) maxPromptChars() int {
	if s.maxPrompt == 0 {
		return DefaultMaxPromptChars
	}
	return s.maxPrompt
}

// randomID generates a chatcmpl-style id.
func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "chatcmpl-fallback"
	}
	return "chatcmpl-" + hex.EncodeToString(b[:])
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(openai.ModelsResponse())
}

// webSearchRequest is the POST /v1/web_search body.
type webSearchRequest struct {
	Query string `json:"query"`
}

// handleWebSearch runs a standalone web search: one upstream completion with
// search on and thinking OFF (thinking + search composes into the upstream's
// slower DEEP_SEARCH tool pipeline — web-search-research.md), returning the
// structured results collected from the SEARCH fragment. The model's answer
// text is discarded; the results array is the product.
func (s *Server) handleWebSearch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body", "invalid_request_error", "bad_request")
		return
	}
	var req webSearchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, `invalid JSON body: expected {"query": "..."}`, "invalid_request_error", "bad_request")
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeError(w, http.StatusBadRequest, `field "query" must be a non-empty string`, "invalid_request_error", "bad_request")
		return
	}

	ctx := r.Context()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if !s.runWebSearchAttempt(ctx, w, req.Query, attempt) {
			return
		}
	}
}

// runWebSearchAttempt executes one lease→session→completion→collect cycle.
// Returns true when the caller should re-attempt on a fresh account (retryable
// failure with nothing written to the client).
func (s *Server) runWebSearchAttempt(ctx context.Context, w http.ResponseWriter, query string, attempt int) bool {
	lease, err := s.pool.Acquire(ctx)
	if err != nil {
		s.writePoolError(w, err)
		return false
	}
	defer lease.Release()

	tok, err := lease.Token(ctx)
	if err != nil {
		lease.NoteError(err)
		s.writeUpstreamError(w, err)
		return false
	}

	sessionID, err := lease.CreateSession(ctx)
	if err != nil {
		lease.NoteError(err)
		s.writeUpstreamError(w, err)
		return false
	}
	// App-like lifecycle (apk-behavior.md §8 D1): the session persists — the
	// app never auto-deletes. The registry only evicts oldest-beyond-cap when
	// a cap is configured, through the async deleter.
	s.sessions.record(lease.Account().Mobile, sessionID, lease.Client(), tok)

	stream, err := lease.Completion(ctx, upstream.CompletionRequest{
		SessionID:        sessionID,
		Prompt:           query,
		ThinkingDisabled: true, // stay on the simple SEARCH path
		SearchEnabled:    true,
	})
	if err != nil {
		lease.NoteError(err)
		s.logger.Printf("web search completion failed (attempt %d): %v", attempt, err)
		if attempt < maxAttempts && ctx.Err() == nil && upstream.IsRetryable(err) {
			return true
		}
		s.writeUpstreamError(w, err)
		return false
	}
	defer stream.Close()

	interp := sse.New()
	buf := make([]byte, 8192)
	var readErr error
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			if ferr := interp.Feed(string(buf[:n])); ferr != nil {
				s.logger.Printf("sse feed: %v", ferr)
				break
			}
		}
		st := interp.Snapshot()
		if st.Err != nil || st.Finished {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	st := interp.Snapshot()
	switch {
	case st.Err != nil:
		lease.NoteError(st.Err)
		s.logger.Printf("web search failed (attempt %d): %v", attempt, st.Err)
		if attempt < maxAttempts && ctx.Err() == nil && isClientRetryableStreamError(st.Err) {
			return true
		}
		s.writeUpstreamError(w, st.Err)
		return false
	case readErr != nil:
		s.logger.Printf("web search transport error (attempt %d): %v", attempt, readErr)
		if attempt < maxAttempts && ctx.Err() == nil && upstream.IsRetryable(readErr) {
			return true
		}
		writeError(w, http.StatusBadGateway, "upstream stream terminated early", "upstream_error", "stream_error")
		return false
	}
	// Empty results with a clean finish is an honest empty search — no retry.
	resp := openai.NewWebSearchResponse(st.SearchQueries, st.SearchResults)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	return false
}

// maxAttempts bounds the completion retry ladder (gap-analysis R1): the
// original attempt plus one switch-account retry. More than that multiplies
// load against an upstream that is already struggling.
const maxAttempts = 2

// handleChatCompletions runs the full pipeline: parse → images (off-lease) →
// per-attempt {acquire → upload → session → completion → deliver}, retrying
// the whole attempt only while nothing has been written to the client.
func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 50<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body", "invalid_request_error", "bad_request")
		return
	}
	req, err := openai.ParseRequest(string(body))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "bad_request")
		return
	}
	if len(req.Stripped) > 0 {
		s.logger.Printf("stripped unsupported request fields: %s", strings.Join(req.Stripped, ", "))
	}

	ctx := r.Context()
	prompt := openai.FlattenMessages(req.Messages)

	if cap := s.maxPromptChars(); cap > 0 && len(prompt) > cap {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("prompt is too long: %d characters exceeds the %d-character limit", len(prompt), cap),
			"invalid_request_error", "context_length_exceeded")
		return
	}

	// Resolve images (fetch/decode) BEFORE taking a lease: an unbounded
	// external fetch must never hold a pool slot (gap-analysis R4). The
	// fetch honors the request context and a 30s client timeout.
	images, err := openai.ExtractImages(ctx, req.Messages)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "image_fetch_failed")
		return
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if !s.runAttempt(ctx, w, req, prompt, images, attempt) {
			return
		}
	}
}

// runAttempt executes one full attempt (lease → upload → session →
// completion → delivery). It returns true when the caller should re-attempt
// on a fresh account: only ever when nothing has been written to the client.
func (s *Server) runAttempt(ctx context.Context, w http.ResponseWriter, req *openai.Request, prompt string, images []openai.Image, attempt int) bool {
	lease, err := s.pool.Acquire(ctx)
	if err != nil {
		// Pool-level failures are terminal: all busy / all banned — a retry
		// cannot conjure capacity.
		s.writePoolError(w, err)
		return false
	}
	defer lease.Release()

	tok, err := lease.Token(ctx)
	if err != nil {
		lease.NoteError(err)
		s.writeUpstreamError(w, err)
		return false
	}

	refFileIDs := make([]string, 0, len(images))
	for _, img := range images {
		fileID, err := lease.Client().UploadImageAndWait(ctx, tok, img.Data, "image."+img.Ext)
		if err != nil {
			s.logger.Printf("image upload failed: %v", err)
			lease.NoteError(err)
			writeError(w, http.StatusBadGateway, "image upload failed", "upstream_error", "upload_failed")
			return false
		}
		refFileIDs = append(refFileIDs, fileID)
	}

	sessionID, err := lease.CreateSession(ctx)
	if err != nil {
		lease.NoteError(err)
		s.writeUpstreamError(w, err)
		return false
	}

	// App-like lifecycle (apk-behavior.md §8 D1): the session persists — the
	// app keeps sessions whose exchange failed, too. The registry evicts
	// oldest-beyond-cap only when a cap is configured, via the async deleter.
	s.sessions.record(lease.Account().Mobile, sessionID, lease.Client(), tok)

	stream, err := lease.Completion(ctx, upstream.CompletionRequest{
		SessionID:        sessionID,
		Prompt:           prompt,
		RefFileIDs:       refFileIDs,
		ThinkingDisabled: !req.ThinkingEnabled,
		SearchEnabled:    req.SearchEnabled,
		Temperature:      req.Temperature,
		TopP:             req.TopP,
		MaxTokens:        req.MaxTokens,
	})
	if err != nil {
		lease.NoteError(err)
		s.logger.Printf("completion call failed (attempt %d): %v", attempt, err)
		if attempt < maxAttempts && ctx.Err() == nil && upstream.IsRetryable(err) {
			s.logger.Printf("retry ladder: switching account for attempt %d", attempt+1)
			return true
		}
		s.writeUpstreamError(w, err)
		return false
	}

	if req.Stream {
		return s.deliverStream(ctx, w, lease, tok, sessionID, stream, attempt, req.ThinkingEnabled)
	}
	return s.deliverNonStream(ctx, w, lease, tok, sessionID, stream, attempt, req.ThinkingEnabled)
}

// isClientRetryableStreamError reports whether a mid-stream error that
// arrived before the first client byte warrants a retry (gap-analysis R1):
// the per-account parallel generation limit does (a fresh account can serve
// it); content-filter and other upstream rejections do not — a new session
// would answer the same.
func isClientRetryableStreamError(err error) bool {
	var se *sse.StreamError
	if errors.As(err, &se) {
		return se.IsParallelLimit()
	}
	return false
}

// deliverStream bridges the upstream SSE into OpenAI chunk frames. The SSE
// headers are NOT written until the first delta frame is emitted, which is
// exactly what makes "retry before the first client byte" real: a stream
// that fails (parallel_chat_limit, transport cut) before any output returns
// retry=true and the ladder resends on a fresh account. After the first byte
// the delivery is final — retrying would duplicate data.
func (s *Server) deliverStream(ctx context.Context, w http.ResponseWriter, lease *upstream.Lease, tok, sessionID string, stream io.ReadCloser, attempt int, thinkingEnabled bool) bool {
	defer stream.Close()

	flusher, _ := w.(http.Flusher)
	committed := false
	commit := func() {
		if committed {
			return
		}
		committed = true
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
	}
	writeChunk := func(v any) {
		b, _ := json.Marshal(v)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if flusher != nil {
			flusher.Flush()
		}
	}

	interp := sse.New()
	id := randomID() // one id for the whole completion (all chunks share it)
	first := true
	interp.SetOnDelta(func(d sse.Delta) {
		commit()
		role := ""
		if first {
			role = "assistant"
			first = false
		}
		if d.Reasoning != "" {
			if !thinkingEnabled {
				// Client opted out: thinking was never requested. Suppress any
				// stray THINK deltas rather than leak reasoning_content.
				return
			}
			// THINK fragment: deep-thinking delta, emitted before content
			// deltas as the fragments arrive.
			writeChunk(openai.NewReasoningStreamChunk(id, openai.ModelName, d.Reasoning, role))
			return
		}
		if d.Text == "" {
			return
		}
		writeChunk(openai.NewStreamChunk(id, openai.ModelName, d.Text, role))
	})

	buf := make([]byte, 8192)
	var readErr error
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			if ferr := interp.Feed(string(buf[:n])); ferr != nil {
				s.logger.Printf("sse feed: %v", ferr)
				break
			}
		}
		// Check state BEFORE the EOF break: when the final data chunk and
		// io.EOF arrive in one Read, an error payload must still surface.
		st := interp.Snapshot()
		if st.Err != nil {
			if !committed {
				// Nothing written to the client yet — the ladder may retry.
				lease.NoteError(st.Err)
				s.logger.Printf("stream failed before first client byte (attempt %d): %v", attempt, st.Err)
				if attempt < maxAttempts && ctx.Err() == nil && isClientRetryableStreamError(st.Err) {
					return true
				}
				// Terminal pre-commit failure: the client connection is still
				// clean — answer with a real HTTP error, not an empty 200.
				s.writeUpstreamError(w, st.Err)
				return false
			}
			if errors.Is(st.Err, sse.ErrContentFilter) {
				writeChunk(openai.ErrorBody("content filtered by upstream", "invalid_request_error", "content_filter"))
			} else {
				writeChunk(openai.ErrorBody("upstream stream error", "upstream_error", "stream_error"))
			}
			break
		}
		if st.Finished {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	st := interp.Snapshot()
	switch {
	case st.Err != nil:
		// A clean final chunk here would tell standard OpenAI clients the
		// completion finished normally — silently truncated output that looks
		// right (gap-analysis R2). The error frame already went out; the only
		// acceptable extra is a final chunk carrying the upstream's own
		// non-clean finish_reason (e.g. "parallel_chat_limit"), so clients
		// that read finish_reason still notice. Never "stop".
		var se *sse.StreamError
		if errors.As(st.Err, &se) && se.FinishReason != "" {
			writeChunk(openai.NewFinalChunk(id, openai.ModelName, st.Usage.TotalTokens, se.FinishReason, st.SearchResults))
		}
	case readErr != nil:
		// Transport cut mid-stream after the first byte: not a clean finish
		// either — say so instead of lying with finish_reason:"stop".
		s.logger.Printf("stream transport error: %v", readErr)
		writeChunk(openai.ErrorBody("upstream stream terminated early", "upstream_error", "stream_error"))
	case st.Text == "" && st.Reasoning == "" && !committed && attempt < maxAttempts && ctx.Err() == nil:
		// Empty-but-clean stream before any client byte: one re-run on a
		// fresh attempt, mirroring the non-stream empty-retry (gap-analysis
		// R1). THINK-only output is NOT empty — reasoning counts. Once the
		// stream committed (first frame out, including a suppressed THINK
		// delta), an empty finish is delivered honestly, never retried.
		s.logger.Printf("empty stream output before first byte (attempt %d), re-running", attempt)
		return true
	default:
		if first && st.Text != "" {
			// Stream ended without deltas (shouldn't happen) — emit accumulated text.
			commit()
			writeChunk(openai.NewStreamChunk(id, openai.ModelName, st.Text, "assistant"))
		}
		commit()
		writeChunk(openai.NewFinalChunk(id, openai.ModelName, st.Usage.TotalTokens, "stop", st.SearchResults))
	}
	io.WriteString(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
	return false
}

// deliverNonStream consumes the whole upstream stream, then decides: a
// retryable failure (parallel_chat_limit, transport cut) or an empty output
// re-runs once on a fresh account; anything else is delivered or mapped to
// an error response. Nothing is written to the client before the decision,
// so non-stream requests are always retry-safe.
func (s *Server) deliverNonStream(ctx context.Context, w http.ResponseWriter, lease *upstream.Lease, tok, sessionID string, stream io.ReadCloser, attempt int, thinkingEnabled bool) bool {
	defer stream.Close()

	interp := sse.New()
	id := randomID()
	var readErr error
	buf := make([]byte, 8192)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			if ferr := interp.Feed(string(buf[:n])); ferr != nil {
				s.logger.Printf("sse feed: %v", ferr)
				break
			}
		}
		st := interp.Snapshot()
		if st.Err != nil || st.Finished {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	st := interp.Snapshot()
	switch {
	case st.Err != nil:
		lease.NoteError(st.Err)
		s.logger.Printf("non-stream failed (attempt %d): %v", attempt, st.Err)
		if attempt < maxAttempts && ctx.Err() == nil && isClientRetryableStreamError(st.Err) {
			s.logger.Printf("retry ladder: switching account for attempt %d", attempt+1)
			return true
		}
		s.writeUpstreamError(w, st.Err)
		return false
	case readErr != nil:
		// Transport cut mid-stream: partial output would look like a
		// successful (truncated) completion — retry instead.
		s.logger.Printf("non-stream transport error (attempt %d): %v", attempt, readErr)
		if attempt < maxAttempts && ctx.Err() == nil && upstream.IsRetryable(readErr) {
			s.logger.Printf("retry ladder: switching account for attempt %d", attempt+1)
			return true
		}
		writeError(w, http.StatusBadGateway, "upstream stream terminated early", "upstream_error", "stream_error")
		return false
	case st.Text == "" && st.Reasoning == "" && attempt < maxAttempts && ctx.Err() == nil:
		// Empty output with no error: one re-run before returning empty
		// (gap-analysis R1) — the detectable subset of mid-flight blips.
		// A reasoning-only completion (empty content, non-empty THINK) is a
		// valid answer, not a blip.
		s.logger.Printf("empty completion output (attempt %d), re-running", attempt)
		return true
	}
	reasoning := st.Reasoning
	if !thinkingEnabled {
		// Client opted out: never surface reasoning_content, even if the
		// upstream sent THINK fragments anyway.
		reasoning = ""
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(openai.NewCompletionResponse(id, openai.ModelName, reasoning, st.Text, st.Usage.TotalTokens, "stop", st.SearchResults))
	return false
}

// writePoolError maps pool-level failures to client-facing responses.
func (s *Server) writePoolError(w http.ResponseWriter, err error) {
	var busy *upstream.PoolBusyError
	if errors.As(err, &busy) {
		retry := int(busy.RetryAfter / time.Second)
		if retry < 1 {
			retry = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		writeError(w, http.StatusTooManyRequests, "all accounts busy, retry later", "rate_limit_error", "pool_busy")
		return
	}
	if errors.Is(err, upstream.ErrNoAccounts) {
		writeError(w, http.StatusServiceUnavailable, "no accounts available (all banned)", "upstream_error", "no_accounts")
		return
	}
	writeError(w, http.StatusBadGateway, err.Error(), "upstream_error", "pool_failure")
}

// writeUpstreamError maps upstream failures to OpenAI error shapes.
// Client-facing text is stable and minimal (gap-analysis §4.6): raw err.Error()
// can leak upstream biz_msg internals and pool detail. Details live in logs.
func (s *Server) writeUpstreamError(w http.ResponseWriter, err error) {
	s.logger.Printf("upstream error: %v", err)
	status := http.StatusBadGateway
	typ := "upstream_error"
	code := "upstream_failure"
	message := "upstream request failed"
	switch upstream.BanKind(err) {
	case upstream.BanBanned:
		code = "account_banned"
		message = "account banned upstream"
	case upstream.BanMuted:
		status = http.StatusTooManyRequests
		code = "account_muted"
		message = "account muted upstream"
	case upstream.BanRiskDevice:
		// Spec table: risk device → 503 upstream_unavailable (the account is
		// parked for a cooldown, but the gateway keeps serving — the client
		// should retry, not conclude its request was bad).
		status = http.StatusServiceUnavailable
		code = "upstream_unavailable"
		message = "account flagged as risk device upstream"
	}
	if upstream.IsAuthFailure(err) {
		status = http.StatusUnauthorized
		typ = "invalid_api_key"
		code = "invalid_api_key"
		message = "upstream authentication failed"
	}
	// Content filter: the request content is the problem — 400, typed.
	if errors.Is(err, sse.ErrContentFilter) {
		writeError(w, http.StatusBadRequest, "content filtered by upstream", "invalid_request_error", "content_filter")
		return
	}
	// Muted: surface the parking window as Retry-After (spec table).
	var muteBE *upstream.BizError
	if errors.As(err, &muteBE) && muteBE.BizCode == 5 {
		s.setMuteRetryAfter(w, muteBE)
	}
	// Mid-stream parallel-generation limit: 429, retryable.
	var se *sse.StreamError
	if errors.As(err, &se) && se.IsParallelLimit() {
		w.Header().Set("Retry-After", "3")
		writeError(w, http.StatusTooManyRequests, "another generation is running on this account; retry shortly", "rate_limit_error", "parallel_chat_limit")
		return
	}
	var he *upstream.HTTPStatusError
	if errors.As(err, &he) && he.Status >= 500 {
		message = "upstream unavailable"
	}
	writeError(w, status, message, typ, code)
}

// mutedRetryAfterFallback bounds Retry-After for a muted account when the
// upstream sends no usable mute_until: conservative minutes, not hours.
const mutedRetryAfterFallback = 60

// setMuteRetryAfter derives the 429 Retry-After header from the upstream
// mute_until (already parsed for parking). A missing, past, or absurd
// (longer than a day) window falls back to a conservative constant.
func (s *Server) setMuteRetryAfter(w http.ResponseWriter, be *upstream.BizError) {
	until := be.MuteUntil
	if until.IsZero() || until.Before(time.Now()) || time.Until(until) > 24*time.Hour {
		w.Header().Set("Retry-After", strconv.Itoa(mutedRetryAfterFallback))
		return
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(time.Until(until).Round(time.Second)/time.Second)))
}

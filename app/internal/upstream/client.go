// Package upstream is the upstream client.
// android-app API: login, chat sessions, PoW, image upload, completion.
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"simple-chat/internal/pow"
)

// DefaultBaseURL is the production domestic upstream host. It is a constant:
// the base URL is not user configuration.
const DefaultBaseURL = "https://chat.deepseek.com"

// preflightTimeout bounds every pre-flight JSON call (login, session, PoW,
// fetch_files, delete): whole request including body. Generous for a slow
// upstream, fatal for a hung one.
const preflightTimeout = 60 * time.Second

// StreamIdleTimeout is the completion-stream idle watchdog: no bytes for this
// long → the body is closed and reads surface ErrStreamIdleTimeout. It is NOT
// a whole-request cap — a slow but progressing generation runs as long as it
// keeps producing bytes.
const StreamIdleTimeout = 180 * time.Second

// ErrStreamIdleTimeout is the typed error surfaced when the completion stream
// stalls for longer than the idle window. Wrapped in http body errors, so
// match with errors.Is.
var ErrStreamIdleTimeout = errors.New("upstream: completion stream idle timeout")

// transportTuning carries the connection-pool sizing for the upstream host
// (gap-analysis R5): DefaultTransport's MaxIdleConnsPerHost=2 re-handshakes
// TLS on the hot path under our own concurrency numbers.
const (
	maxIdleConns        = 64
	minIdleConnsPerHost = 8
	idleConnTimeout     = 90 * time.Second
)

// defaultDeviceID is the last-resort device id for a Client built without an
// explicit one (tests, single-account use): a fixed app-format mint over a
// fixed synthetic ANDROID_ID. The pool always resolves per-account ids
// instead.
var defaultDeviceID = func() string {
	id, err := mintAppDeviceID(syntheticAndroidID("simple-chat-default"), appDeviceBrand)
	if err != nil {
		return "simple_chat_client"
	}
	return id
}()

// Impersonated-device wire values (apk-alignment.md §2/§11): the exact
// fingerprint the DeepSeek Android 2.5.3 client emits for our device profile
// (Pixel 8 / Android 15 / en_US build / UTC+8). These are functional
// wire-protocol constants — the upstream risk service scores them.
const (
	appClientVersion = "2.5.3"
	appUserAgent     = "DeepSeek/2.5.3 Android/35"
	appClientLocale  = "en_US"
	appBundleID      = "com.deepseek.chat"
	appTimezoneOff   = "28800" // +08:00, seconds (dj.java:440-446)
	appDeviceModel   = "Pixel 8"
)

// newTransport builds the shared transport. base (optional) allows callers to
// derive from an existing transport's dialer settings.
func newTransport(base http.RoundTripper) *http.Transport {
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	if bt, ok := base.(*http.Transport); ok && bt.DialContext != nil {
		dialer = nil // inherit the base dialer
	}
	tr := &http.Transport{
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   minIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	if dialer != nil {
		tr.DialContext = dialer.DialContext
	} else if bt, ok := base.(*http.Transport); ok {
		tr.DialContext = bt.DialContext
	}
	return tr
}

// Config carries the single account credentials and endpoint options.
type Config struct {
	// BaseURL defaults to DefaultBaseURL; tests inject an httptest URL.
	BaseURL  string
	Mobile   string
	Email    string
	Password string
	// DeviceID is the per-account device identity sent on the login wire.
	// The pool resolves it via ResolveDeviceID; an empty value falls back
	// to a static string (tests, single-client use).
	DeviceID string
	// Channel mirrors Account.Channel: "" (android, default) or "web".
	// Web channel sends os:"web" in the login body with the harvested
	// Shumei device_id, and a minted UUID in the x-device-id header —
	// mirroring the browser, which sends its own UUID there, distinct from
	// the Shumei id in the body (web-reverse-research.md §1/§6).
	Channel string
}

// Client talks to one upstream with one account.
type Client struct {
	cfg Config
	// http serves pre-flight JSON calls (login, session, PoW, upload,
	// fetch_files, delete) with a whole-request timeout.
	http *http.Client
	// httpStream serves the completion stream with NO whole-request timeout —
	// the body reader's idle watchdog governs it instead (gap-analysis R3).
	httpStream        *http.Client
	logger            func(format string, args ...any)
	streamIdleTimeout time.Duration

	// deviceProfile is the per-account impersonated device (apk-alignment.md
	// §11): x-device-id and x-rangers-id values, minted deterministically.
	deviceProfile deviceProfile

	powMu sync.Mutex // guards concurrent fresh-challenge fetches

	amOnce sync.Once
	am     *AccountManager
}

// deviceProfile carries the per-account device identity headers (dj.java
// case 18). deviceID is the app-format mint (or explicit accounts.json
// value); rangersID is the APM install id (qqb.java bd_did — opaque, stable
// per install; UUID shape is plausible for it).
//
// On the web channel, deviceID is the browser-harvested Shumei SMSdk id
// (login body only), while headerDeviceID is a minted UUIDv5 sent in
// x-device-id — mirroring the browser, which sends its own UUID there,
// distinct from the Shumei id in the body (web-reverse-research.md §1).
type deviceProfile struct {
	deviceID       string
	rangersID      string
	headerDeviceID string
	channel        string
}

// channelOS reports the login body "os" value for this channel: the web
// channel presents os:"web" (the Shumei id was minted in the web collector's
// namespace; web-reverse-research.md §3), android stays "android".
func (p deviceProfile) channelOS() string {
	if p.channel == "web" {
		return "web"
	}
	return "android"
}

// resolveDeviceProfile mints the per-account device profile. The rangers id
// derives from the account identity the same way the device id does —
// deterministic, never shared between accounts.
func resolveDeviceProfile(cfg Config) deviceProfile {
	deviceID := strings.TrimSpace(cfg.DeviceID)
	if deviceID == "" {
		deviceID = defaultDeviceID
	}
	identity := strings.TrimSpace(cfg.Mobile)
	if identity == "" {
		identity = strings.TrimSpace(cfg.Email)
	}
	if identity == "" {
		identity = deviceID
	}
	channel := strings.TrimSpace(cfg.Channel)
	if channel == "web" {
		return deviceProfile{
			deviceID:       deviceID,
			rangersID:      formatUUID(uuidv5(deviceIDNamespace, "simple-chat/rangers-id/"+identity)),
			headerDeviceID: formatUUID(uuidv5(deviceIDNamespace, "simple-chat/web-header-device-id/"+identity)),
			channel:        "web",
		}
	}
	return deviceProfile{
		deviceID:       deviceID,
		rangersID:      formatUUID(uuidv5(deviceIDNamespace, "simple-chat/rangers-id/"+identity)),
		headerDeviceID: deviceID,
		channel:        "",
	}
}

// NewClient builds a client; an empty BaseURL means the production default.
func NewClient(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.DeviceID == "" {
		cfg.DeviceID = defaultDeviceID
	}
	transport := newTransport(nil)
	return &Client{
		cfg:               cfg,
		http:              &http.Client{Timeout: preflightTimeout, Transport: transport},
		httpStream:        &http.Client{Transport: transport},
		streamIdleTimeout: StreamIdleTimeout,
		deviceProfile:     resolveDeviceProfile(cfg),
	}
}

// SetLogger installs a printf-style logger (nil disables logging).
func (c *Client) SetLogger(fn func(format string, args ...any)) { c.logger = fn }

// SetTransport overrides the transport for both the pre-flight and the stream
// client (the pool injects one shared, pool-sized Transport).
func (c *Client) SetTransport(rt http.RoundTripper) {
	c.http.Transport = rt
	c.httpStream.Transport = rt
}

func (c *Client) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger(format, args...)
	}
}

// baseHeaders are the android-client impersonation headers (recon §3.1,
// apk-alignment.md §2): the exact dj.java case-18 block the DeepSeek Android
// 2.5.3 client attaches to every chat.deepseek.com request, for our
// impersonated device profile. The User-Agent and friends are functional
// wire-protocol values: the upstream WAF rejects a neutral UA with HTTP 202 +
// empty body (live-verified 2026-09-19). Do not rebrand them.
func (c *Client) baseHeaders() map[string]string {
	return map[string]string{
		"Accept":                   "application/json",
		"Referer":                  "https://chat.deepseek.com",
		"User-Agent":               appUserAgent,
		"x-client-platform":        "android",
		"x-client-version":         appClientVersion,
		"x-client-locale":          appClientLocale,
		"x-client-bundle-id":       appBundleID,
		"x-client-timezone-offset": appTimezoneOff,
		"x-device-model":           appDeviceModel,
		"x-device-id":              c.deviceProfile.headerDeviceID,
		"x-rangers-id":             c.deviceProfile.rangersID,
	}
}

// envelope is the outer response wrapper used by every JSON endpoint.
type envelope struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		BizCode int             `json:"biz_code"`
		BizMsg  string          `json:"biz_msg"`
		BizData json.RawMessage `json:"biz_data"`
	} `json:"data"`
}

// BizError marks a nonzero biz_code (or outer code) response.
type BizError struct {
	HTTPStatus int
	Code       int
	BizCode    int
	Msg        string
	BizMsg     string
	// MuteUntil carries the upstream mute_until for biz 5 (muted), when sent.
	MuteUntil time.Time
}

func (e *BizError) Error() string {
	if e.BizMsg != "" {
		return fmt.Sprintf("upstream: biz_code %d: %s", e.BizCode, e.BizMsg)
	}
	if e.Msg != "" {
		return fmt.Sprintf("upstream: code %d: %s", e.Code, e.Msg)
	}
	return fmt.Sprintf("upstream: http %d", e.HTTPStatus)
}

func asBizError(err error, target **BizError) bool {
	var be *BizError
	if errors.As(err, &be) {
		*target = be
		return true
	}
	return false
}

// IsAuthFailure reports whether an error warrants a token refresh (recon §5:
// 401/403, outer codes 40001-40003, or auth-indicative biz messages).
func IsAuthFailure(err error) bool {
	be, ok := err.(*BizError)
	if !ok {
		return false
	}
	if be.HTTPStatus == http.StatusUnauthorized || be.HTTPStatus == http.StatusForbidden {
		return true
	}
	if be.Code >= 40001 && be.Code <= 40003 || be.BizCode >= 40001 && be.BizCode <= 40003 {
		return true
	}
	combined := strings.ToLower(be.Msg + " " + be.BizMsg)
	for _, kw := range []string{"token", "unauthorized", "expired", "not login", "login required", "invalid jwt"} {
		if strings.Contains(combined, kw) {
			return true
		}
	}
	return false
}

// BanState classifies account-level failure codes (recon §5: 10 banned,
// 5 muted, 11 risk device).
type BanState int

const (
	BanNone BanState = iota
	BanBanned
	BanMuted
	BanRiskDevice
)

// BanKind classifies a BizError into an account ban/mute state.
func BanKind(err error) BanState {
	be, ok := err.(*BizError)
	if !ok {
		return BanNone
	}
	switch be.BizCode {
	case 10:
		return BanBanned
	case 5:
		return BanMuted
	case 11:
		return BanRiskDevice
	}
	return BanNone
}

// postJSON sends an authenticated JSON POST and decodes the envelope.
func (c *Client) postJSON(ctx context.Context, path string, token string, body any, extraHeaders map[string]string) (envelope, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return envelope{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return envelope{}, err
	}
	h := c.baseHeaders()
	h["Content-Type"] = "application/json"
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	for k, v := range extraHeaders {
		h[k] = v
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return envelope{}, err
	}
	defer resp.Body.Close()
	var env envelope
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return envelope{}, err
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("upstream: bad JSON from %s (http %d): %.200s", path, resp.StatusCode, data)
	}
	if resp.StatusCode != http.StatusOK {
		return env, &BizError{HTTPStatus: resp.StatusCode, Code: env.Code, Msg: env.Msg, BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg}
	}
	return env, nil
}

// checkEnv validates the envelope for success.
func checkEnv(env envelope) error {
	if env.Code != 0 {
		return &BizError{Code: env.Code, Msg: env.Msg, BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg, MuteUntil: parseMuteUntil(env.Data.BizData)}
	}
	if env.Data.BizCode != 0 {
		return &BizError{BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg, MuteUntil: parseMuteUntil(env.Data.BizData)}
	}
	return nil
}

// parseMuteUntil extracts a mute_until timestamp from raw biz_data. The
// upstream has been observed sending unix seconds as number or string and
// RFC3339; null/garbage yields the zero time.
func parseMuteUntil(raw json.RawMessage) time.Time {
	var probe struct {
		MuteUntil json.RawMessage `json:"mute_until"`
		Chat      struct {
			MuteUntil json.RawMessage `json:"mute_until"`
		} `json:"chat"`
		User struct {
			Chat struct {
				MuteUntil json.RawMessage `json:"mute_until"`
			} `json:"chat"`
		} `json:"user"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return time.Time{}
	}
	for _, c := range []json.RawMessage{probe.MuteUntil, probe.Chat.MuteUntil, probe.User.Chat.MuteUntil} {
		if t := parseMuteUntilValue(c); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

// parseMuteUntilValue decodes one mute_until candidate.
func parseMuteUntilValue(raw json.RawMessage) time.Time {
	s := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	if s == "" || s == "null" {
		return time.Time{}
	}
	// Unix seconds, number or string.
	if sec, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(sec, 0)
	}
	// RFC3339.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

// Login performs POST /api/v0/users/login and returns the bearer token.
// Body shape mirrors the app's kotlinx.serialization encoding (mta/ota):
// the app's Json has encodeDefaults=true and explicitNulls=true (ak5/bi5),
// so os:"android" is always sent and area_code is an explicit JSON null on
// mobile logins — the encoder guard (zyb.A0() || value != default) is
// always true because A0() returns true (apk-alignment.md I1, revised).
func (c *Client) Login(ctx context.Context) (string, error) {
	payload := map[string]any{
		"password":  c.cfg.Password,
		"device_id": c.deviceProfile.deviceID,
		"os":        c.deviceProfile.channelOS(),
	}
	if email := strings.TrimSpace(c.cfg.Email); email != "" {
		payload["email"] = email
	} else if mobile := normalizeMobile(c.cfg.Mobile); mobile != "" {
		payload["mobile"] = mobile
		payload["area_code"] = nil
	} else {
		return "", errors.New("upstream: account needs mobile or email")
	}
	env, err := c.postJSON(ctx, "/api/v0/users/login", "", payload, nil)
	if err != nil {
		return "", err
	}
	if err := checkEnv(env); err != nil {
		return "", err
	}
	var bizData struct {
		User struct {
			Token string `json:"token"`
		} `json:"user"`
	}
	if err := json.Unmarshal(env.Data.BizData, &bizData); err != nil {
		return "", err
	}
	if bizData.User.Token == "" {
		return "", errors.New("upstream: login response missing token")
	}
	return bizData.User.Token, nil
}

// normalizeMobile strips +86 prefixes from mainland numbers (recon §3.2).
func normalizeMobile(raw string) string {
	s := strings.TrimSpace(raw)
	var b strings.Builder
	hasPlus := strings.HasPrefix(s, "+")
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	digits := b.String()
	if (hasPlus || strings.HasPrefix(digits, "86")) && strings.HasPrefix(digits, "86") && len(digits) == 13 {
		return digits[2:]
	}
	return digits
}

// HTTPStatusError marks a non-200 completion response.
type HTTPStatusError struct {
	Status  int
	Snippet string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("upstream: completion http %d: %.200s", e.Status, e.Snippet)
}

// IsRetryable reports whether a failed call is worth another attempt:
// transport errors and HTTP >= 500 yes; upstream rejections (BizError) and
// client-side HTTP statuses no.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var be *BizError
	if errors.As(err, &be) {
		return false
	}
	var he *HTTPStatusError
	if errors.As(err, &he) {
		return he.Status >= 500
	}
	return true
}

// preflightRetryBackoff spaces the one pre-flight retry attempt.
const preflightRetryBackoff = 250 * time.Millisecond

// retryTransport runs fn once, then once more (after a short backoff) if the
// first attempt failed with a transport error. Upstream answers (BizError)
// are not retried — the upstream spoke; retrying is wasted load.
func (c *Client) retryTransport(ctx context.Context, fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}
	var be *BizError
	if errors.As(err, &be) {
		return err
	}
	select {
	case <-ctx.Done():
		return err
	case <-time.After(preflightRetryBackoff):
	}
	return fn()
}

// CreateSession performs POST /api/v0/chat_session/create. One transport-error
// retry with 250ms backoff (gap-analysis R1).
func (c *Client) CreateSession(ctx context.Context, token string) (string, error) {
	var env envelope
	err := c.retryTransport(ctx, func() error {
		var e error
		env, e = c.postJSON(ctx, "/api/v0/chat_session/create", token, map[string]string{"agent": "chat"}, nil)
		return e
	})
	if err != nil {
		return "", err
	}
	if err := checkEnv(env); err != nil {
		return "", err
	}
	var bizData struct {
		ID          string `json:"id"`
		ChatSession struct {
			ID string `json:"id"`
		} `json:"chat_session"`
	}
	if err := json.Unmarshal(env.Data.BizData, &bizData); err != nil {
		return "", err
	}
	if bizData.ChatSession.ID != "" {
		return bizData.ChatSession.ID, nil
	}
	if bizData.ID != "" {
		return bizData.ID, nil
	}
	return "", errors.New("upstream: create_session response missing id")
}

// PowHeader fetches and solves a fresh PoW challenge for the given target
// path — one per completion, exactly like the app's no-prefetch path
// (dz1.f → dz1.b → n8: challenge → solve → use, no reuse; gj0's prefetch
// deque is consumable, and yc0's reuse cache is upload-only). The server
// rejects reused solutions with 40301 INVALID_POW_RESPONSE (live-verified
// 2026-09-19), so solutions are single-use (apk-alignment.md I2, revised).
// One transport-error retry on the challenge fetch (gap-analysis R1).
func (c *Client) PowHeader(ctx context.Context, token, targetPath string) (string, error) {
	var env envelope
	err := c.retryTransport(ctx, func() error {
		var e error
		env, e = c.postJSON(ctx, "/api/v0/chat/create_pow_challenge", token, map[string]string{"target_path": targetPath}, nil)
		return e
	})
	if err != nil {
		return "", err
	}
	if err := checkEnv(env); err != nil {
		return "", err
	}
	var bizData struct {
		Challenge pow.Challenge `json:"challenge"`
	}
	if err := json.Unmarshal(env.Data.BizData, &bizData); err != nil {
		return "", err
	}
	return pow.SolveAndBuildHeader(ctx, &bizData.Challenge)
}

// CompletionRequest is the body for POST /api/v0/chat/completion.
type CompletionRequest struct {
	SessionID  string
	Prompt     string
	RefFileIDs []string
	// ThinkingDisabled inverts the upstream thinking_enabled flag: the zero
	// value means thinking ON, so the default-on contract holds without every
	// caller having to remember the flag.
	ThinkingDisabled bool
	// SearchEnabled turns on upstream web search (search_enabled:true).
	// Zero value = off, the documented default (web-search-research.md).
	SearchEnabled bool
	// Temperature, TopP, MaxTokens pass through when nonzero.
	Temperature float64
	TopP        float64
	MaxTokens   int
}

// Completion opens the SSE completion stream. The caller must Close the body.
func (c *Client) Completion(ctx context.Context, token string, req CompletionRequest) (io.ReadCloser, error) {
	powHeader, err := c.PowHeader(ctx, token, "/api/v0/chat/completion")
	if err != nil {
		return nil, err
	}
	// Body shape mirrors the app's ChatFullCompletionRequest encoding
	// (qj1/sj1, kotlinx.serialization): the app's Json has
	// encodeDefaults=true (ak5/bi5), so every field serializes every time —
	// ref_file_ids: [] (empty array, not JSON null) when no files,
	// thinking_enabled/search_enabled with their literal boolean values, and
	// preempt unconditionally (apk-alignment.md I3, revised).
	refFileIDs := req.RefFileIDs
	if refFileIDs == nil {
		refFileIDs = []string{}
	}
	payload := map[string]any{
		"chat_session_id":   req.SessionID,
		"parent_message_id": nil,
		"prompt":            req.Prompt,
		"ref_file_ids":      refFileIDs,
		"thinking_enabled":  !req.ThinkingDisabled,
		"search_enabled":    req.SearchEnabled,
		"preempt":           false,
	}
	if req.Temperature != 0 {
		payload["temperature"] = req.Temperature
	}
	if req.TopP != 0 {
		payload["top_p"] = req.TopP
	}
	if req.MaxTokens != 0 {
		payload["max_tokens"] = req.MaxTokens
	}
	body, _ := json.Marshal(payload)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/api/v0/chat/completion", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	h := c.baseHeaders()
	h["Content-Type"] = "application/json"
	h["Authorization"] = "Bearer " + token
	h["X-DS-PoW-Response"] = powHeader // app casing (qf3.java:346)
	for k, v := range h {
		httpReq.Header.Set(k, v)
	}
	resp, err := c.httpStream.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &HTTPStatusError{Status: resp.StatusCode, Snippet: string(raw)}
	}
	// The upstream may answer errors as JSON with HTTP 200 on this endpoint;
	// peek at the content type to distinguish SSE from an error envelope.
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "text/event-stream") {
		return c.watchIdle(resp.Body), nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("upstream: unexpected completion response: %.200s", raw)
	}
	if err := checkEnv(env); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("upstream: completion returned JSON without stream")
}

// idleWatchdogBody wraps a completion body: every successful Read bumps a
// sliding idle deadline, and a watchdog goroutine closes the body once the
// deadline passes without bytes. Reads unblocked by the cut surface
// ErrStreamIdleTimeout instead of hanging (or silently EOF-ing) on a
// dead-silent upstream.
type idleWatchdogBody struct {
	body     io.ReadCloser
	idle     time.Duration
	cut      atomic.Bool
	lastRead atomic.Int64 // unix nanos of the last successful Read
	done     chan struct{}
}

// watchIdle arms the idle watchdog on a completion body.
func (c *Client) watchIdle(body io.ReadCloser) io.ReadCloser {
	w := &idleWatchdogBody{
		body: body,
		idle: c.streamIdleTimeout,
		done: make(chan struct{}),
	}
	w.lastRead.Store(time.Now().UnixNano())
	go func() {
		tick := w.idle / 4
		if tick < 10*time.Millisecond {
			tick = 10 * time.Millisecond
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, w.lastRead.Load())) >= w.idle {
					// Idle window fully elapsed with no bytes: cut the stream.
					w.cut.Store(true)
					body.Close()
					return
				}
			}
		}
	}()
	return w
}

func (w *idleWatchdogBody) Read(p []byte) (int, error) {
	n, err := w.body.Read(p)
	if n > 0 {
		w.lastRead.Store(time.Now().UnixNano())
	}
	if w.cut.Load() && err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("%w (no bytes for %s)", ErrStreamIdleTimeout, w.idle)
	}
	return n, err
}

func (w *idleWatchdogBody) Close() error {
	close(w.done)
	return w.body.Close()
}

// DeleteSession performs POST /api/v0/chat_session/delete (best-effort).
func (c *Client) DeleteSession(ctx context.Context, token, sessionID string) error {
	env, err := c.postJSON(ctx, "/api/v0/chat_session/delete", token, map[string]string{"chat_session_id": sessionID}, nil)
	if err != nil {
		return err
	}
	return checkEnv(env)
}

// DeleteAllSessions performs POST /api/v0/chat_session/delete_all — the
// app's "clear all chat history" (profile → delete_all_chat_button). Wire
// shape confirmed from the decompiled call site (r02.java case 1): a POST
// with NO request serializer attached (chat_session/create's case 0 calls
// rl2.n0 to set the JSON body; delete_all never does), answered by the
// standard envelope. We send an empty body — matching the app's
// bodyless POST — so the wire is byte-identical on our side.
func (c *Client) DeleteAllSessions(ctx context.Context, token string) error {
	env, err := c.postEmpty(ctx, "/api/v0/chat_session/delete_all", token)
	if err != nil {
		return err
	}
	return checkEnv(env)
}

// postEmpty sends an authenticated bodyless POST (no Content-Type, no
// body) and decodes the envelope — the wire form of APK request builders
// that attach no serializer (r02.java case 1).
func (c *Client) postEmpty(ctx context.Context, path, token string) (envelope, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+path, nil)
	if err != nil {
		return envelope{}, err
	}
	h := c.baseHeaders()
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return envelope{}, err
	}
	defer resp.Body.Close()
	var env envelope
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return envelope{}, err
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("upstream: bad JSON from %s (http %d): %.200s", path, resp.StatusCode, data)
	}
	if resp.StatusCode != http.StatusOK {
		return env, &BizError{HTTPStatus: resp.StatusCode, Code: env.Code, Msg: env.Msg, BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg}
	}
	return env, nil
}

// getJSON sends an authenticated parameterless GET and decodes the envelope.
// Mirrors postJSON's header handling (bearer + app base headers).
func (c *Client) getJSON(ctx context.Context, path, token string) (envelope, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.BaseURL+path, nil)
	if err != nil {
		return envelope{}, err
	}
	h := c.baseHeaders()
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return envelope{}, err
	}
	defer resp.Body.Close()
	var env envelope
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return envelope{}, err
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("upstream: bad JSON from %s (http %d): %.200s", path, resp.StatusCode, data)
	}
	if resp.StatusCode != http.StatusOK {
		return env, &BizError{HTTPStatus: resp.StatusCode, Code: env.Code, Msg: env.Msg, BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg}
	}
	return env, nil
}

// UsersCurrent performs GET /api/v0/users/current (app fires it ~1s after
// launch for the update check; we fire it after first login — same position
// relative to the conversation lifecycle). The result is discarded; the call
// itself is the mimicry target.
func (c *Client) UsersCurrent(ctx context.Context, token string) error {
	env, err := c.getJSON(ctx, "/api/v0/users/current", token)
	if err != nil {
		return err
	}
	return checkEnv(env)
}

// FetchSessionPage performs GET /api/v0/chat_session/fetch_page — the
// parameterless first page, exactly as the app's first drawer open (cursor
// params only appear when a t72 cursor exists; od1:820-838). Result
// discarded.
func (c *Client) FetchSessionPage(ctx context.Context, token string) error {
	env, err := c.getJSON(ctx, "/api/v0/chat_session/fetch_page", token)
	if err != nil {
		return err
	}
	return checkEnv(env)
}

// SessionInfo is one entry of the session drawer (fetch_page biz_data).
type SessionInfo struct {
	ID        string  `json:"id"`
	Pinned    bool    `json:"pinned"`
	UpdatedAt float64 `json:"updated_at"` // epoch seconds
}

// sessionPage is the parsed fetch_page biz_data payload.
type sessionPage struct {
	Sessions []SessionInfo `json:"chat_sessions"`
	HasMore  bool          `json:"has_more"`
}

// maxSessionPages bounds fetch_page pagination: a human-paced cleanup
// pass never needs more than the drawer's depth; 50 pages × page size is
// far beyond any account the gateway serves.
const maxSessionPages = 50

// ListSessions walks the account's session drawer (fetch_page, newest
// first, cursor-paginated via lte_cursor.pinned / lte_cursor.updated_at —
// the app's t72 cursor semantics) and returns every live session. The
// cursor repeats the OLDEST listed entry's (pinned, updated_at); the
// upstream returns strictly older sessions from there. First page is
// parameterless — same shape as the app's drawer open.
func (c *Client) ListSessions(ctx context.Context, token string) ([]SessionInfo, error) {
	var all []SessionInfo
	cursor := ""
	for page := 0; page < maxSessionPages; page++ {
		p, err := c.listSessionPage(ctx, token, cursor, &all)
		if err != nil {
			return nil, err
		}
		if !p.HasMore || len(p.Sessions) == 0 {
			break
		}
		oldest := p.Sessions[len(p.Sessions)-1]
		cursor = fmt.Sprintf("lte_cursor.pinned=%t&lte_cursor.updated_at=%s",
			oldest.Pinned, strconv.FormatFloat(oldest.UpdatedAt, 'f', -1, 64))
	}
	return all, nil
}

// listSessionPage fetches one drawer page, appending its sessions (in
// arrival order, newest-first) and returning the page descriptor.
func (c *Client) listSessionPage(ctx context.Context, token string, cursor string, out *[]SessionInfo) (sessionPage, error) {
	path := "/api/v0/chat_session/fetch_page"
	if cursor != "" {
		path += "?" + cursor
	}
	env, err := c.getJSON(ctx, path, token)
	if err != nil {
		return sessionPage{}, err
	}
	if err := checkEnv(env); err != nil {
		return sessionPage{}, err
	}
	var page sessionPage
	if err := json.Unmarshal(env.Data.BizData, &page); err != nil {
		return sessionPage{}, fmt.Errorf("upstream: bad fetch_page biz_data: %.200s", env.Data.BizData)
	}
	*out = append(*out, page.Sessions...)
	return page, nil
}

// UploadImageAndWait uploads an image (multipart, PoW-authenticated) and
// polls fetch_files until the file reaches SUCCESS (recon: probe 2026-09-19).
// The filename MUST end in an image extension — the server types by suffix.
func (c *Client) UploadImageAndWait(ctx context.Context, token string, data []byte, filename string) (string, error) {
	fileID, err := c.uploadImage(ctx, token, data, filename)
	if err != nil {
		return "", err
	}
	// Poll for parse completion.
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, err := c.fileStatus(ctx, token, fileID)
		if err != nil {
			return "", err
		}
		switch strings.ToUpper(status) {
		case "SUCCESS", "COMPLETED":
			return fileID, nil
		case "FAILED", "ERROR", "PARSE_FAILED", "CONTENT_EMPTY":
			return "", fmt.Errorf("upstream: file %s failed to parse (status %s)", fileID, status)
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("upstream: file %s stuck in %s", fileID, status)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (c *Client) uploadImage(ctx context.Context, token string, data []byte, filename string) (string, error) {
	powHeader, err := c.PowHeader(ctx, token, "/api/v0/file/upload_file")
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return "", err
	}
	if _, err := fw.Write(data); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/api/v0/file/upload_file", &buf)
	if err != nil {
		return "", err
	}
	h := c.baseHeaders()
	h["Authorization"] = "Bearer " + token
	h["Content-Type"] = mw.FormDataContentType()
	// Upload header set (qy1.java:174-189): X-DS-PoW-Response,
	// X-Thinking-Enabled, x-file-size. The app's mixed-case names are kept.
	h["X-DS-PoW-Response"] = powHeader
	h["X-Thinking-Enabled"] = "0"
	h["x-file-size"] = strconv.Itoa(len(data))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("upstream: bad upload response: %.200s", raw)
	}
	if resp.StatusCode != http.StatusOK {
		return "", &BizError{HTTPStatus: resp.StatusCode, Code: env.Code, Msg: env.Msg, BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg}
	}
	if err := checkEnv(env); err != nil {
		return "", err
	}
	var bizData struct {
		ID     string `json:"id"`
		FileID string `json:"file_id"`
	}
	if err := json.Unmarshal(env.Data.BizData, &bizData); err != nil {
		return "", err
	}
	if bizData.ID == "" {
		return "", fmt.Errorf("upstream: upload response missing id: %.200s", raw)
	}
	return bizData.ID, nil
}

// fileStatus fetches the parse status of one uploaded file.
func (c *Client) fileStatus(ctx context.Context, token, fileID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.cfg.BaseURL+"/api/v0/file/fetch_files?file_ids="+fileID, nil)
	if err != nil {
		return "", err
	}
	h := c.baseHeaders()
	h["Authorization"] = "Bearer " + token
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env struct {
		Code int `json:"code"`
		Data struct {
			BizCode int    `json:"biz_code"`
			BizMsg  string `json:"biz_msg"`
			BizData struct {
				Files []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"files"`
			} `json:"biz_data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("upstream: bad fetch_files response: %.200s", raw)
	}
	if env.Code != 0 || env.Data.BizCode != 0 {
		return "", &BizError{Code: env.Code, Msg: "", BizCode: env.Data.BizCode, BizMsg: env.Data.BizMsg}
	}
	for _, f := range env.Data.BizData.Files {
		if f.ID == fileID {
			return f.Status, nil
		}
	}
	return "", fmt.Errorf("upstream: file %s missing from fetch_files", fileID)
}

// AccountManager wraps the client with token lifecycle: lazy relogin on auth
// failures, ban/mute marking. It is the per-account seam the pool leases out.
type AccountManager struct {
	client *Client

	mu        sync.Mutex
	token     string
	ban       BanState
	banMsg    string
	parkUntil time.Time
	// startupFired guards the one-shot app-launch sequence (users/current
	// then fetch_page) that fires on the empty→token transition — once per
	// process per account (apk-behavior.md §8 D2).
	startupFired bool
	// onLogin, when set, receives every successful login/relogin's fresh
	// token synchronously under mu (docs-spec-memory-first.md) — the pool
	// wires it to the store's login write-through. Nil = memory-only tokens.
	onLogin func(token string)
}

// AccountManager returns (creating if needed) the token manager.
func (c *Client) AccountManager() *AccountManager {
	// one manager per client is enough for our single-account scope; store it.
	c.amOnce.Do(func() {
		c.am = &AccountManager{client: c}
	})
	return c.am
}

// Token returns the current bearer token, logging in if none yet.
func (am *AccountManager) Token(ctx context.Context) (string, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if am.ban != BanNone {
		return "", fmt.Errorf("upstream: account unavailable: %s", am.banMsg)
	}
	if am.token != "" {
		return am.token, nil
	}
	tok, err := am.client.Login(ctx)
	if err != nil {
		am.markBan(err)
		return "", err
	}
	am.token = tok
	if am.onLogin != nil {
		am.onLogin(tok)
	}
	if !am.startupFired {
		am.startupFired = true
		am.fireStartupSequence(tok)
	}
	return tok, nil
}

// fireStartupSequence mimics the app's launch traffic around first login
// (apk-behavior.md §8 D2): GET users/current (app fires it ~1s after launch
// for the update check), then GET chat_session/fetch_page (first drawer
// open, parameterless first page). Best-effort: background goroutine, never
// blocks or gates the request, no retries, failures only logged.
func (am *AccountManager) fireStartupSequence(tok string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := am.client.UsersCurrent(ctx, tok); err != nil {
			am.client.logf("startup users/current failed (ignored): %v", err)
			return
		}
		if err := am.client.FetchSessionPage(ctx, tok); err != nil {
			am.client.logf("startup fetch_page failed (ignored): %v", err)
		}
	}()
}

// markBan records ban/mute states from an upstream error.
func (am *AccountManager) markBan(err error) {
	switch BanKind(err) {
	case BanBanned:
		am.ban = BanBanned
		am.banMsg = "account banned: " + err.Error()
	case BanMuted:
		am.ban = BanMuted
		am.banMsg = "account muted: " + err.Error()
	case BanRiskDevice:
		am.ban = BanRiskDevice
		am.banMsg = "risk device detected: " + err.Error()
	}
}

// relogin drops the cached token and logs in again.
func (am *AccountManager) relogin(ctx context.Context) (string, error) {
	am.token = ""
	tok, err := am.client.Login(ctx)
	if err != nil {
		am.markBan(err)
		return "", err
	}
	am.token = tok
	if am.onLogin != nil {
		am.onLogin(tok)
	}
	return tok, nil
}

// CreateSession creates a chat session, lazily logging in and retrying once
// with a fresh token on auth failure.
func (am *AccountManager) CreateSession(ctx context.Context) (string, error) {
	tok, err := am.Token(ctx)
	if err != nil {
		return "", err
	}
	id, err := am.client.CreateSession(ctx, tok)
	if err != nil && IsAuthFailure(err) {
		am.mu.Lock()
		tok, aerr := am.relogin(ctx)
		am.mu.Unlock()
		if aerr != nil {
			return "", aerr
		}
		id, err = am.client.CreateSession(ctx, tok)
	}
	if err != nil {
		am.mu.Lock()
		am.markBan(err)
		am.mu.Unlock()
	}
	return id, err
}

// Completion runs the completion stream with the account's token, retrying
// once with a fresh token on an auth failure during the PoW pre-flight —
// the same lazy-relogin pattern CreateSession has always had.
func (am *AccountManager) Completion(ctx context.Context, req CompletionRequest) (io.ReadCloser, error) {
	tok, err := am.Token(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := am.client.Completion(ctx, tok, req)
	if err != nil && IsAuthFailure(err) {
		am.mu.Lock()
		tok, aerr := am.relogin(ctx)
		am.mu.Unlock()
		if aerr != nil {
			am.markBan(aerr)
			return nil, aerr
		}
		stream, err = am.client.Completion(ctx, tok, req)
	}
	if err != nil {
		am.mu.Lock()
		am.markBan(err)
		am.mu.Unlock()
	}
	return stream, err
}

// Status returns a human-readable account state.
func (am *AccountManager) Status() string {
	am.mu.Lock()
	defer am.mu.Unlock()
	switch am.ban {
	case BanBanned:
		return "banned"
	case BanMuted:
		return "muted"
	case BanRiskDevice:
		return "risk device"
	}
	if am.token != "" {
		return "ready"
	}
	return "no token"
}

// setTokenForTest seeds a token without network access (tests only).
func (am *AccountManager) setTokenForTest(tok string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.token = tok
}

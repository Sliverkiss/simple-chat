package upstream

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simple-chat/internal/pow"
)

// mockUpstream spins an httptest server with per-path handlers.
type mockUpstream struct {
	mu       sync.Mutex
	served   map[string]int
	handlers map[string]func(w http.ResponseWriter, r *http.Request)
	srv      *httptest.Server
}

func newMock(handlers map[string]func(w http.ResponseWriter, r *http.Request)) *mockUpstream {
	m := &mockUpstream{handlers: handlers, served: map[string]int{}}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.served[r.URL.Path]++
		h, ok := handlers[r.URL.Path]
		m.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		h(w, r)
	}))
	return m
}

func (m *mockUpstream) count(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.served[path]
}

func (m *mockUpstream) client() *Client {
	return NewClient(Config{BaseURL: m.srv.URL, Mobile: "13800000000", Password: "pw"})
}

// solvableChallenge builds a real HashV1 challenge whose answer is 42.
func solvableChallenge(targetPath string) map[string]any {
	h := pow.HashV1([]byte("testsalt_1700000000_42"))
	return map[string]any{
		"algorithm":   "HashV1",
		"challenge":   hex.EncodeToString(h[:]),
		"salt":        "testsalt",
		"expire_at":   1700000000,
		"difficulty":  144000,
		"signature":   "sig",
		"target_path": targetPath,
	}
}

func writeEnvelope(w http.ResponseWriter, bizCode int, bizMsg string, bizData any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "msg": "",
		"data": map[string]any{
			"biz_code": bizCode, "biz_msg": bizMsg, "biz_data": bizData,
		},
	})
}

func TestLoginSuccess(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["mobile"] != "13800000000" || body["password"] != "pw" {
				t.Errorf("unexpected login body: %v", body)
			}
			if body["device_id"] == nil {
				t.Errorf("missing device_id: %v", body)
			}
			// App encoding (apk-alignment.md I1, revised): encodeDefaults
			// always sends os:"android" and an explicit area_code null.
			if body["os"] != "android" {
				t.Errorf("os = %v, want \"android\"", body["os"])
			}
			if v, ok := body["area_code"]; !ok || v != nil {
				t.Errorf("area_code = %v (%t), want explicit JSON null", v, ok)
			}
			// The WAF rejects neutral UAs with HTTP 202 + empty body (live-verified
			// 2026-09-19). The exact android-client UA is a functional requirement
			// (now the 2.5.3 fingerprint — apk-alignment.md C1).
			if ua := r.Header.Get("User-Agent"); ua != "DeepSeek/2.5.3 Android/35" {
				t.Errorf("User-Agent = %q, want authentic android client UA", ua)
			}
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1", "id": "u1"}})
		},
	})
	defer m.srv.Close()

	c := m.client()
	tok, err := c.Login(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "tok1" {
		t.Errorf("token = %q", tok)
	}
}

func TestLoginBizError(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 5, "user is muted", nil)
		},
	})
	defer m.srv.Close()
	if _, err := m.client().Login(context.Background()); err == nil {
		t.Fatal("expected error for muted account")
	}
}

func TestCreateSession(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer tok1" {
				t.Errorf("missing bearer")
			}
			writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "sess1"}})
		},
	})
	defer m.srv.Close()
	id, err := m.client().CreateSession(context.Background(), "tok1")
	if err != nil {
		t.Fatal(err)
	}
	if id != "sess1" {
		t.Errorf("session id = %q", id)
	}
}

func TestCreateSessionAcceptsLegacyIDField(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"id": "legacy1"})
		},
	})
	defer m.srv.Close()
	id, err := m.client().CreateSession(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if id != "legacy1" {
		t.Errorf("id = %q, want legacy1", id)
	}
}

func TestPowChallengeAndSolve(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["target_path"] != "/api/v0/chat/completion" {
				t.Errorf("wrong target_path: %v", body)
			}
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
	})
	defer m.srv.Close()
	header, err := m.client().PowHeader(context.Background(), "tok1", "/api/v0/chat/completion")
	if err != nil {
		t.Fatal(err)
	}
	if header == "" {
		t.Fatal("empty pow header")
	}
}

func TestCompletionStreams(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("x-ds-pow-response") == "" {
				t.Error("completion must carry x-ds-pow-response")
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			// App encoding (apk-alignment.md I3, revised):
			// encodeDefaults always sends both flags as literal booleans.
			if body["thinking_enabled"] != true || body["search_enabled"] != false {
				t.Errorf("thinking must default on, search off: %v", body)
			}
			if v, ok := body["preempt"]; !ok || v != false {
				t.Errorf("preempt = %v (%t), want false present (always serialized)", v, ok)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"hi\"}]}}}\n")
			io.WriteString(w, "data: {\"v\":\" there\"}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	stream, err := m.client().Completion(context.Background(), "tok1", CompletionRequest{
		SessionID: "sess1", Prompt: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	raw, _ := io.ReadAll(stream)
	if !strings.Contains(string(raw), "\"hi\"") || !strings.Contains(string(raw), "there") {
		t.Errorf("stream content wrong: %q", raw)
	}
}

func TestCompletionBizError(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 10, "USER_IS_BANNED", nil)
		},
	})
	defer m.srv.Close()
	_, err := m.client().Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
	if err == nil {
		t.Fatal("expected ban error")
	}
	var be *BizError
	if !asBizError(err, &be) || be.BizCode != 10 {
		t.Fatalf("want BizError code 10, got %v", err)
	}
}

func TestDeleteSessionBestEffort(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/delete": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["chat_session_id"] != "sess1" {
				t.Errorf("wrong session id in delete: %v", body)
			}
			writeEnvelope(w, 0, "", nil)
		},
	})
	defer m.srv.Close()
	if err := m.client().DeleteSession(context.Background(), "tok1", "sess1"); err != nil {
		t.Errorf("delete should succeed: %v", err)
	}
}

func TestAccountManagerLazyReloginOn401(t *testing.T) {
	var mu sync.Mutex
	cur := "fresh" // token the upstream currently considers valid
	loginCount := 0
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			loginCount++
			mu.Unlock()
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "fresh"}})
		},
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			mu.Lock()
			valid := auth == "Bearer "+cur
			mu.Unlock()
			if !valid {
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]any{"code": 40001, "msg": "token expired"})
				return
			}
			writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "sess-after-relogin"}})
		},
	})
	defer m.srv.Close()

	c := m.client()
	am := c.AccountManager()
	// Seed a stale token without going through the network.
	am.setTokenForTest("stale")
	id, err := am.CreateSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != "sess-after-relogin" {
		t.Errorf("session id = %q, want relogin path", id)
	}
	if loginCount != 1 {
		t.Errorf("login count = %d, want 1 (lazy relogin)", loginCount)
	}
}

func TestAccountManagerMarksBanned(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "t1"}})
		},
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 10, "USER_IS_BANNED", nil)
		},
	})
	defer m.srv.Close()
	am := m.client().AccountManager()
	_, err := am.CreateSession(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	st := am.Status()
	if !strings.Contains(strings.ToLower(st), "ban") {
		t.Errorf("status = %q, want banned", st)
	}
}

func TestUploadFile(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/file/upload_file")})
		},
		"/api/v0/file/upload_file": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("x-ds-pow-response") == "" {
				t.Error("upload must carry its own pow header")
			}
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				t.Errorf("content type = %s", r.Header.Get("Content-Type"))
			}
			file, hdr, err := r.FormFile("file")
			if err != nil {
				t.Fatalf("form file: %v", err)
			}
			defer file.Close()
			if !strings.HasSuffix(hdr.Filename, ".png") {
				t.Errorf("filename must end with image ext, got %q", hdr.Filename)
			}
			writeEnvelope(w, 0, "", map[string]any{
				"id": "file-1", "status": "PENDING", "model_kind": "VISION", "is_image": true,
			})
		},
		"/api/v0/file/fetch_files": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"files": []map[string]any{
				{"id": "file-1", "status": "SUCCESS"},
			}})
		},
	})
	defer m.srv.Close()

	id, err := m.client().UploadImageAndWait(context.Background(), "tok1", []byte("pngdata"), "image.png")
	if err != nil {
		t.Fatal(err)
	}
	if id != "file-1" {
		t.Errorf("file id = %q", id)
	}
	if m.count("/api/v0/file/fetch_files") < 1 {
		t.Error("should have polled fetch_files")
	}
}

func TestUploadWaitsForSuccess(t *testing.T) {
	var polls int
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/file/upload_file")})
		},
		"/api/v0/file/upload_file": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"id": "file-2", "status": "PENDING"})
		},
		"/api/v0/file/fetch_files": func(w http.ResponseWriter, r *http.Request) {
			polls++
			status := "PENDING"
			if polls >= 2 {
				status = "SUCCESS"
			}
			writeEnvelope(w, 0, "", map[string]any{"files": []map[string]any{
				{"id": "file-2", "status": status},
			}})
		},
	})
	defer m.srv.Close()
	_, err := m.client().UploadImageAndWait(context.Background(), "tok1", []byte("png"), "image.png")
	if err != nil {
		t.Fatal(err)
	}
	if polls < 2 {
		t.Errorf("polls = %d, want >= 2", polls)
	}
}

func TestEnvelopeRejectsNonZeroOuterCode(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(map[string]any{"code": 40300, "msg": "MISSING_HEADER", "data": nil})
		},
	})
	defer m.srv.Close()
	if _, err := m.client().CreateSession(context.Background(), "tok"); err == nil {
		t.Fatal("expected error for outer code 40300")
	}
}

func TestAuthKeywordTriggersRelogin(t *testing.T) {
	var logins int
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			logins++
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": fmt.Sprintf("tok%d", logins)}})
		},
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "Bearer stale2" {
				// HTTP 200 + code 0 + nonzero biz_code with auth-ish message:
				// must still trigger a lazy relogin.
				writeEnvelope(w, 3, "not login", nil)
				return
			}
			writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "ok"}})
		},
	})
	defer m.srv.Close()
	c := m.client()
	am := c.AccountManager()
	am.setTokenForTest("stale2")
	id, err := am.CreateSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != "ok" {
		t.Errorf("id = %q", id)
	}
	if logins != 1 {
		t.Errorf("logins = %d, want 1", logins)
	}
}

func TestDefaultBaseURLIsProduction(t *testing.T) {
	// The domestic base URL is a constant in this package, not configuration.
	// NewClient must resolve an empty BaseURL to the production host.
	c := NewClient(Config{Mobile: "13800000000", Password: "pw"})
	if c.cfg.BaseURL != DefaultBaseURL {
		t.Errorf("BaseURL = %q, want %q", c.cfg.BaseURL, DefaultBaseURL)
	}
	if DefaultBaseURL != "https://chat.deepseek.com" {
		t.Errorf("DefaultBaseURL = %q, want https://chat.deepseek.com", DefaultBaseURL)
	}
}

func TestRegionField(t *testing.T) {
	// Only the "cn" region exists today (see spec.md region evidence). An
	// empty region means "cn"; anything else must be rejected at load time,
	// not discovered mid-request.
	if got := normalizeRegion(""); got != "cn" {
		t.Errorf(`normalizeRegion("") = %q, want "cn"`, got)
	}
	if got := normalizeRegion("cn"); got != "cn" {
		t.Errorf(`normalizeRegion("cn") = %q, want "cn"`, got)
	}
	if got := normalizeRegion("intl"); got != "" {
		t.Errorf(`normalizeRegion("intl") = %q, want "" (unsupported)`, got)
	}
}

func TestBizErrorMessageVariants(t *testing.T) {
	cases := []struct {
		err  BizError
		want string
	}{
		{BizError{BizCode: 5, BizMsg: "muted"}, "biz_code 5: muted"},
		{BizError{Code: 40001, Msg: "token expired"}, "code 40001: token expired"},
		{BizError{HTTPStatus: 502}, "http 502"},
	}
	for _, c := range cases {
		if got := c.err.Error(); got != "upstream: "+c.want {
			t.Errorf("Error() = %q, want %q", got, "upstream: "+c.want)
		}
	}
}

func TestLoginEmailAccount(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["email"] != "a@b.c" || body["mobile"] != nil {
				t.Errorf("login body = %v, want email only", body)
			}
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "etok"}})
		},
	})
	defer m.srv.Close()
	c := NewClient(Config{BaseURL: m.srv.URL, Email: "a@b.c", Password: "pw"})
	tok, err := c.Login(context.Background())
	if err != nil || tok != "etok" {
		t.Fatalf("tok = %q, err = %v", tok, err)
	}
}

func TestLoginNeedsCredentials(t *testing.T) {
	c := NewClient(Config{Password: "pw"})
	if _, err := c.Login(context.Background()); err == nil {
		t.Fatal("want error with no mobile/email")
	}
}

func TestAccountManagerStatusVariants(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "t"}})
		},
	})
	defer m.srv.Close()
	am := m.client().AccountManager()
	if st := am.Status(); st != "no token" {
		t.Errorf("fresh status = %q, want no token", st)
	}
	am.setTokenForTest("t")
	if st := am.Status(); st != "ready" {
		t.Errorf("ready status = %q", st)
	}
}

func TestRegionBaseURLFallback(t *testing.T) {
	if got := regionBaseURL("cn"); got != DefaultBaseURL {
		t.Errorf("cn base = %q", got)
	}
	if got := regionBaseURL("nonexistent"); got != DefaultBaseURL {
		t.Errorf("fallback base = %q", got)
	}
}

func TestAccountValidateVariants(t *testing.T) {
	if err := (Account{Mobile: "1", Password: "p", Region: "cn"}).Validate(); err != nil {
		t.Errorf("valid account: %v", err)
	}
	if err := (Account{Email: "a@b.c", Password: "p"}).Validate(); err != nil {
		t.Errorf("email account: %v", err)
	}
	if err := (Account{Password: "p", Region: "intl"}).Validate(); err == nil {
		t.Error("unknown region must be rejected")
	}
	if err := (Account{Mobile: "1", Email: "a@b.c"}).Validate(); err == nil {
		t.Error("missing password must be rejected")
	}
}

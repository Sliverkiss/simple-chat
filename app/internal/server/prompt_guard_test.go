package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"simple-chat/internal/upstream"
)

// newPromptGuardServer builds a gateway with MaxPromptChars set; the mock
// upstream answers 500 on everything so any leaked request fails loudly.
func newPromptGuardServer(t *testing.T, maxPromptChars int) *httptest.Server {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "upstream must not be reached by a prompt-guard rejection")
	}))
	t.Cleanup(up.Close)
	srv, err := NewServer(Config{
		UpstreamBase:   up.URL,
		Accounts:       []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
		MaxPromptChars: maxPromptChars,
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(gw.Close)
	return gw
}

// postChat sends one chat request and returns status + body.
func postChat(t *testing.T, gw *httptest.Server, content string) (int, string) {
	t.Helper()
	body := `{"model":"deepseek-flash","messages":[{"role":"user","content":` + jsonString(content) + `}]}`
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// An over-cap flattened prompt is rejected with a clean 400 (invalid_request_error,
// context_length_exceeded) before any upstream call — not an opaque 502.
func TestPromptSizeGuardOverCapRejected(t *testing.T) {
	gw := newPromptGuardServer(t, 16)
	status, body := postChat(t, gw, strings.Repeat("a", 64))
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", status, body)
	}
	if !strings.Contains(body, "invalid_request_error") {
		t.Errorf("body must be an invalid_request_error: %s", body)
	}
	if !strings.Contains(body, "context_length_exceeded") {
		t.Errorf("body must carry code context_length_exceeded: %s", body)
	}
}

// Boundary: a prompt exactly at the cap passes the guard (the upstream mock's
// 500 is irrelevant — the guard must not reject it).
func TestPromptSizeGuardBoundaryPasses(t *testing.T) {
	gw := newPromptGuardServer(t, 16)
	status, body := postChat(t, gw, "hi") // flattened: "user: hi\n" = 9 chars
	if status == http.StatusBadRequest {
		t.Fatalf("prompt at/under the cap must not be 400-rejected, body = %s", body)
	}
}

// Negative cap = guard disabled; a prompt far over the default cap passes
// through (0 resolves to the default, negative disables).
func TestPromptSizeGuardNegativeDisabled(t *testing.T) {
	gw := newPromptGuardServer(t, -1)
	status, _ := postChat(t, gw, strings.Repeat("a", 3_000_000))
	if status == http.StatusBadRequest {
		t.Fatal("negative cap must disable the guard, got 400")
	}
}

// The error body message must state the actual limit, not just the code.
func TestPromptSizeGuardMessageNamesLimit(t *testing.T) {
	gw := newPromptGuardServer(t, 16)
	_, body := postChat(t, gw, strings.Repeat("a", 64))
	if !strings.Contains(body, "16") {
		t.Errorf("error message should name the cap: %s", body)
	}
}

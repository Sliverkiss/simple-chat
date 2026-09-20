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

// newTestServerWithKey builds a server with a static API key configured.
func newTestServerWithKey(t *testing.T, upstreamURL, apiKey string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(Config{
		UpstreamBase: upstreamURL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
		APIKey:       apiKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(srv.Handler())
}

// wantInvalidKey asserts an OpenAI-style 401 invalid_api_key error body.
func wantInvalidKey(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 401, body = %s", resp.StatusCode, body)
	}
	var got struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if got.Error.Message != "Invalid API key" || got.Error.Type != "invalid_api_key" || got.Error.Code != "invalid_api_key" {
		t.Errorf("error body = %+v", got.Error)
	}
}

func chatBody() io.Reader {
	return strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`)
}

func TestAuthDisabledWhenNoKeyConfigured(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "")
	defer srv.Close()

	for _, methodURL := range []struct {
		method string
		url    string
		body   io.Reader
	}{
		{http.MethodPost, "/v1/chat/completions", chatBody()},
		{http.MethodGet, "/v1/models", nil},
	} {
		resp, err := http.NewRequest(methodURL.method, srv.URL+methodURL.url, methodURL.body)
		if err != nil {
			t.Fatal(err)
		}
		req := resp
		req.Header.Set("Content-Type", "application/json")
		do, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if do.StatusCode != 200 {
			body, _ := io.ReadAll(do.Body)
			t.Errorf("%s %s: status = %d, want 200 (open access), body = %s", methodURL.method, methodURL.url, do.StatusCode, body)
		}
		do.Body.Close()
	}
}

func TestAuthValidBearer(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "sk-test-key")
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", chatBody())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	if up.completions.Load() != 1 {
		t.Errorf("upstream completions = %d, want 1", up.completions.Load())
	}
}

func TestAuthWrongKey(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "sk-test-key")
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", chatBody())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-wrong-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	wantInvalidKey(t, resp)
	if up.completions.Load() != 0 {
		t.Errorf("upstream completions = %d, want 0 (rejected before upstream)", up.completions.Load())
	}
}

func TestAuthMissingHeader(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "sk-test-key")
	defer srv.Close()

	for _, target := range []string{"/v1/chat/completions", "/v1/models"} {
		var resp *http.Response
		var err error
		if target == "/v1/chat/completions" {
			resp, err = http.Post(srv.URL+target, "application/json", chatBody())
		} else {
			resp, err = http.Get(srv.URL + target)
		}
		if err != nil {
			t.Fatal(err)
		}
		wantInvalidKey(t, resp)
		resp.Body.Close()
	}
}

func TestAuthXApiKeyAccepted(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "sk-test-key")
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	req.Header.Set("X-Api-Key", "sk-test-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
}

func TestAuthHealthzOpenEvenWithKey(t *testing.T) {
	up := newUpstreamFixture(t)
	defer up.srv.Close()
	srv := newTestServerWithKey(t, up.srv.URL, "sk-test-key")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (healthz unauthenticated)", resp.StatusCode)
	}
}

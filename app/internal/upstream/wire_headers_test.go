package upstream

// Wire alignment — request headers (apk-alignment.md C1/C2/C4).
//
// dj.java case 18 attaches this block to every chat.deepseek.com request:
// Referer, UA DeepSeek/2.5.3 Android/<sdk>, x-client-platform/version/locale/
// bundle-id, x-rangers-id, x-client-timezone-offset, x-device-model,
// x-device-id (plus nullable x-hif-*). accept-charset is never sent.

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
)

// appHeaderSet is the exact dj.java case-18 block with stock-install values
// for our impersonated device (Pixel 8, Android 15, en_US build, +08:00).
var appHeaderSet = map[string]string{
	"Referer":                  "https://chat.deepseek.com",
	"User-Agent":               "DeepSeek/2.5.3 Android/35",
	"x-client-platform":        "android",
	"x-client-version":         "2.5.3",
	"x-client-locale":          "en_US",
	"x-client-bundle-id":       "com.deepseek.chat",
	"x-client-timezone-offset": "28800",
	"x-device-model":           "Pixel 8",
}

// assertAppHeaders verifies the full app fingerprint on one request.
func assertAppHeaders(t *testing.T, r *http.Request) {
	t.Helper()
	for name, want := range appHeaderSet {
		if got := r.Header.Get(name); got != want {
			t.Errorf("header %s = %q, want %q", name, got, want)
		}
	}
	if r.Header.Get("x-device-id") == "" {
		t.Error("x-device-id must be present on every request")
	} else if !appDeviceIDPattern.MatchString(r.Header.Get("x-device-id")) {
		t.Errorf("x-device-id = %q, want app-format Base64 (not a UUID)", r.Header.Get("x-device-id"))
	}
	if r.Header.Get("x-rangers-id") == "" {
		t.Error("x-rangers-id must be present on every request")
	}
	if v := r.Header.Get("accept-charset"); v != "" {
		t.Errorf("accept-charset = %q, app never sends it", v)
	}
}

func TestLoginHeadersMatchAndroidApp(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			assertAppHeaders(t, r)
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1"}})
		},
	})
	defer m.srv.Close()
	if _, err := m.client().Login(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestAllEndpointsCarryAppHeaders: the header block is host-wide in the app
// (dj.java interceptor), so every endpoint we call must carry it.
func TestAllEndpointsCarryAppHeaders(t *testing.T) {
	var mu sync.Mutex
	bad := map[string]string{}
	check := func(path string) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			for name, want := range appHeaderSet {
				if got := r.Header.Get(name); got != want {
					mu.Lock()
					bad[path+" "+name] = got
					mu.Unlock()
				}
			}
			if r.Header.Get("x-device-id") == "" || r.Header.Get("x-rangers-id") == "" {
				mu.Lock()
				bad[path+" x-device-id/x-rangers-id"] = "missing"
				mu.Unlock()
			}
		}
	}
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			check("/login")(w, r)
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1"}})
		},
		"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
			check("/session")(w, r)
			writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "s1"}})
		},
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			check("/pow")(w, r)
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			check("/completion")(w, r)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":\"ok\"}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
		"/api/v0/chat_session/delete": func(w http.ResponseWriter, r *http.Request) {
			check("/delete")(w, r)
			writeEnvelope(w, 0, "", nil)
		},
	})
	defer m.srv.Close()

	c := m.client()
	tok, err := c.Login(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sess, err := c.CreateSession(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := c.Completion(context.Background(), tok, CompletionRequest{SessionID: sess, Prompt: "p"})
	if err != nil {
		t.Fatal(err)
	}
	stream.Close()
	if err := c.DeleteSession(context.Background(), tok, sess); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for k, v := range bad {
		t.Errorf("%s: header mismatch, got %q", k, v)
	}
}

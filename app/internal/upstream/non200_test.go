package upstream

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestNon200CompletionMutePreservesWindow(t *testing.T) {
	until := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	m := newMock(map[string]func(http.ResponseWriter, *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			writeEnvelope(w, 5, "user is muted", map[string]any{"mute_until": until.Unix()})
		},
	})
	defer m.srv.Close()
	_, err := m.client().Completion(context.Background(), "tok", CompletionRequest{SessionID: "s", Prompt: "p"})
	var be *BizError
	if !errors.As(err, &be) || be.HTTPStatus != 429 || be.BizCode != 5 || !be.MuteUntil.Equal(until) || BanKind(err) != BanMuted || IsRetryable(err) {
		t.Fatalf("completion error=%v, want nonretryable mute until %s", err, until)
	}
	if got := m.count("/api/v0/chat/completion"); got != 1 {
		t.Fatalf("completion calls=%d, want 1", got)
	}
}

func TestNon200SessionMutePreservesWindow(t *testing.T) {
	until := time.Now().Add(time.Hour).Truncate(time.Second)
	m := newMock(map[string]func(http.ResponseWriter, *http.Request){"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		writeEnvelope(w, 5, "muted", map[string]any{"mute_until": until.Unix()})
	}})
	defer m.srv.Close()
	_, err := m.client().CreateSession(context.Background(), "tok")
	var be *BizError
	if !errors.As(err, &be) || be.BizCode != 5 || !be.MuteUntil.Equal(until) || IsRetryable(err) {
		t.Fatalf("session error=%v, want mute until %s", err, until)
	}
	if n := m.count("/api/v0/chat_session/create"); n != 1 {
		t.Fatalf("calls=%d, want 1", n)
	}
}

func TestNon200SessionEmpty503IsRetryable(t *testing.T) {
	m := newMock(map[string]func(http.ResponseWriter, *http.Request){"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503); writeEnvelope(w, 0, "", nil) }})
	defer m.srv.Close()
	_, err := m.client().CreateSession(context.Background(), "tok")
	if !IsRetryable(err) {
		t.Fatalf("503 empty JSON error=%v, want retryable", err)
	}
}

func TestNon200AuthStillRefreshes(t *testing.T) {
	for _, endpoint := range []string{"session", "completion"} {
		t.Run(endpoint, func(t *testing.T) {
			m := newMock(map[string]func(http.ResponseWriter, *http.Request){
				"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
					writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "fresh"}})
				},
				"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "Bearer fresh" {
						w.WriteHeader(401)
						writeEnvelope(w, 0, "", nil)
						return
					}
					writeEnvelope(w, 0, "", map[string]any{"id": "s"})
				},
				"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
					writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
				},
				"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "Bearer fresh" {
						w.WriteHeader(401)
						writeEnvelope(w, 0, "", nil)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
				},
			})
			defer m.srv.Close()
			am := m.client().AccountManager()
			am.token = "stale"
			if endpoint == "session" {
				if _, err := am.CreateSession(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				stream, err := am.Completion(context.Background(), CompletionRequest{SessionID: "s", Prompt: "p"})
				if err != nil {
					t.Fatal(err)
				}
				stream.Close()
			}
			if n := m.count("/api/v0/users/login"); n != 1 {
				t.Fatalf("refreshes=%d, want 1", n)
			}
		})
	}
}

func TestNon200ExplicitBiz503NotRetryable(t *testing.T) {
	m := newMock(map[string]func(http.ResponseWriter, *http.Request){"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503); writeEnvelope(w, 7, "rejected", nil) }})
	defer m.srv.Close()
	_, err := m.client().CreateSession(context.Background(), "tok")
	if IsRetryable(err) {
		t.Fatalf("explicit business rejection retried: %v", err)
	}
	if n := m.count("/api/v0/chat_session/create"); n != 1 {
		t.Fatalf("calls=%d, want 1", n)
	}
}

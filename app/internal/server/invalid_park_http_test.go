package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

func TestInvalidPersistedParkWithHealthyAccountSkipsIdentity(t *testing.T) {
	f := newMuteFixture(t, "")
	acct := upstream.Account{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: "bad-time"}
	path := accountsFileAt(t, &acct)
	rows := mustLoadAccounts(t, path)
	s, err := NewServer(Config{UpstreamBase: f.srv.URL, Accounts: rows, ParkStore: jsonStoreFor(path), QueueWait: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()
	gw := httptest.NewServer(s.Handler())
	defer gw.Close()
	for _, ep := range []struct{ path, body string }{
		{"/v1/chat/completions", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`},
		{"/v1/web_search", `{"query":"hi"}`},
	} {
		resp, err := http.Post(gw.URL+ep.path, "application/json", strings.NewReader(ep.body))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status=%d body=%s", ep.path, resp.StatusCode, body)
		}
	}
	if got := f.hits("100"); got != 0 {
		t.Fatalf("parked identity reached upstream %d times", got)
	}
	if got := f.hits("101"); got == 0 {
		t.Fatal("healthy identity never served")
	}
}

// Both real handlers must reject an already parked sole identity before any
// login/session/search call, and must not invent a Retry-After deadline.
func TestInvalidPersistedParkHTTPFailsClosed(t *testing.T) {
	for _, kind := range []string{"muted", "risk"} {
		for _, until := range []string{"", "invalid-date"} {
			t.Run(kind+"/"+until, func(t *testing.T) {
				f := newMuteFixture(t, "")
				acct := upstream.Account{Mobile: "100", Password: "pw", ParkKind: kind, ParkUntil: until, ParkReason: "upstream rejected"}
				path := accountsFileAt(t, &acct)
				rows := mustLoadAccounts(t, path)
				if len(rows) != 2 || rows[0].ParkKind != kind {
					t.Fatalf("loaded rows=%+v", rows)
				}
				// Force the corrupted identity to be the sole selectable account;
				// otherwise a healthy second identity correctly serves the request.
				rows = rows[:1]
				s, err := NewServer(Config{UpstreamBase: f.srv.URL, Accounts: rows, ParkStore: jsonStoreFor(path), QueueWait: 20 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(s.Shutdown)
				gw := httptest.NewServer(s.Handler())
				defer gw.Close()
				if lease, err := s.pool.Acquire(context.Background()); lease != nil || !errors.Is(err, upstream.ErrNoEligibleAccount) {
					t.Fatalf("lease=%v err=%v", lease, err)
				}
				for _, ep := range []struct{ path, body string }{
					{"/v1/chat/completions", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`},
					{"/v1/web_search", `{"query":"hi"}`},
				} {
					resp, err := http.Post(gw.URL+ep.path, "application/json", strings.NewReader(ep.body))
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "" || !strings.Contains(string(body), `"code":"no_eligible_account"`) {
						t.Fatalf("%s: status=%d retry=%q body=%s", ep.path, resp.StatusCode, resp.Header.Get("Retry-After"), body)
					}
				}
				if got := f.hits("100"); got != 0 {
					t.Fatalf("parked identity reached upstream %d times", got)
				}
			})
		}
	}
}

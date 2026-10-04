package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// An initially ready identity may fail on first probe; once marked, neither
// this request nor later requests may send it a second upstream call.
func TestFirstDiscoveredMuteExcludedFromLaterSwitches(t *testing.T) {
	f := newMuteFixture(t, "100")
	path := accountsFileAt(t, &upstream.Account{Mobile: "100", Password: "pw"})
	accounts := mustLoadAccounts(t, path)
	// Only A is selectable for the first request; add the healthy identity
	// after the mute to avoid depending on probabilistic pool selection.
	srv, err := NewServer(Config{UpstreamBase: f.srv.URL, Accounts: accounts[:1], ParkStore: jsonStoreFor(path), QueueWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(gw.Close)
	status, body := postHi(t, gw.URL)
	if status != http.StatusTooManyRequests || !strings.Contains(body, "muted") {
		t.Fatalf("first mute status=%d body=%s", status, body)
	}
	firstHits := f.hits("100")
	if firstHits == 0 {
		t.Fatal("first probe never reached A")
	}
	f.mu.Lock()
	completions := f.completions["100"]
	f.mu.Unlock()
	if completions != 1 {
		t.Fatalf("mute completion replayed: %d", completions)
	}
	if got := readAccountsFile(t, path)[0].ParkKind; got != "muted" {
		t.Fatalf("mute not persisted: %q", got)
	}
	if err := srv.pool.AddAccount(accounts[1]); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		status, body = postHi(t, gw.URL)
		if status != http.StatusOK {
			t.Fatalf("later request %d: %d %s", i, status, body)
		}
	}
	if got := f.hits("100"); got != firstHits {
		t.Fatalf("marked mute selected again: before=%d after=%d", firstHits, got)
	}
	if f.hits("101") == 0 {
		t.Fatal("healthy B never served")
	}
}

// Tests for the human-paced session cleanup scheduler (TASK_CLEANUP):
// episodes fire on a jittered interval, delete a bounded random batch of
// the OLDEST sessions only above a floor, never touch inactive accounts,
// and shut down cleanly. Deterministic cases call pass() directly with a
// seeded RNG on a dormant (interval 0) scheduler.
package server

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// liveSession is one entry of the mock upstream's session drawer.
type liveSession struct {
	id        string
	pinned    bool
	updatedAt float64
}

// cleanupFixture is an upstream mock whose fetch_page drawer is LIVE truth:
// sessions appear on create (with monotonic updated_at) and disappear on
// delete, newest-first like the real drawer.
type cleanupFixture struct {
	srv *httptest.Server

	mu        sync.Mutex
	created   int            // chat_session/create calls
	deleted   map[string]int // session id → delete count
	live      []liveSession  // upstream session truth (append = newest last)
	pageHits  int
	deleteAll int  // chat_session/delete_all calls
	// failDeleteAll makes delete_all answer biz 5 (muted) — the purge
	// failure path.
	failDeleteAll bool
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	f := &cleanupFixture{deleted: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.pageHits++
		// Newest-first, exactly like the app's drawer.
		drawer := make([]mc, 0, len(f.live))
		for i := len(f.live) - 1; i >= 0; i-- {
			s := f.live[i]
			drawer = append(drawer, mc{"id": s.id, "pinned": s.pinned, "updated_at": s.updatedAt})
		}
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "",
			"biz_data": mc{"chat_sessions": drawer, "has_more": false}}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.created++
		id := "s" + itoa(f.created)
		f.live = append(f.live, liveSession{id: id, updatedAt: 1700000000 + float64(f.created)})
		f.mu.Unlock()
		sessOK(w, id)
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) { streamOK(w, "ok") })
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID string `json:"chat_session_id"`
		}
		jsonDecode(r, &body)
		f.mu.Lock()
		f.deleted[body.ID]++
		for i, s := range f.live {
			if s.id == body.ID {
				f.live = append(f.live[:i], f.live[i+1:]...)
				break
			}
		}
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete_all", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deleteAll++
		failed := f.failDeleteAll
		f.mu.Unlock()
		if failed {
			writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 5, "biz_msg": "user is muted",
				"biz_data": mc{"mute_until": time.Now().Add(time.Hour).Format("2006-01-02 15:04:05")}}})
			return
		}
		f.mu.Lock()
		f.live = nil // the whole drawer is gone
		f.mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// seedOrphans pre-populates the upstream drawer with sessions the gateway
// never created (restart leftovers) — oldest first in time.
func (f *cleanupFixture) seedOrphans(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 1; i <= n; i++ {
		f.live = append(f.live, liveSession{id: "o" + itoa(i), updatedAt: 1600000000 + float64(i)})
	}
}

// pin marks a live session pinned (humans don't tidy pinned chats).
func (f *cleanupFixture) pin(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.live {
		if s.id == id {
			f.live[i].pinned = true
		}
	}
}

func (f *cleanupFixture) snapshot() (created int, deleted map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	deleted = make(map[string]int, len(f.deleted))
	for k, v := range f.deleted {
		deleted[k] = v
	}
	return f.created, deleted
}

// newCleanupServer builds a gateway over one account; the optional tweak
// adjusts the Config (cleanup interval, floor, ...). The returned *Server
// gives in-package tests direct access to the (dormant by default)
// cleanup scheduler.
func newCleanupServer(t *testing.T, upstreamURL string, tweak func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := Config{
		UpstreamBase: upstreamURL,
		Accounts:     []upstream.Account{{Mobile: "13800000000", Password: "pw"}},
	}
	if tweak != nil {
		tweak(&cfg)
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Shutdown()
	})
	return srv, ts
}

// postOneChat sends one minimal chat completion through the gateway.
func postOneChat(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("chat request status = %d, want 200", resp.StatusCode)
	}
}

// runPass fires one deterministic cleanup wake on a dormant scheduler:
// probability 1, millisecond gaps, seeded RNG.
func runPass(s *cleanupScheduler, seed int64) {
	s.cfg.probability = 1
	s.cfg.gapMin = time.Millisecond
	s.cfg.gapMax = 2 * time.Millisecond
	s.rng = rand.New(rand.NewSource(seed))
	s.pass(context.Background())
}

// TestCleanupPassDeletesBoundedOldestBatch: 8 recorded sessions, floor 5 —
// one pass deletes between 1 and 3 sessions, all from the oldest end
// (s1..s3), newest kept, each exactly once.
func TestCleanupPassDeletesBoundedOldestBatch(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}

	runPass(gw.cleanup, 42)

	eventually(t, 3*time.Second, "cleanup deletes to drain", func() bool {
		_, deleted := f.snapshot()
		return len(deleted) >= 1
	})
	_, deleted := f.snapshot()
	if len(deleted) < 1 || len(deleted) > 3 {
		t.Fatalf("episode deleted %d sessions, want 1-3", len(deleted))
	}
	for id, n := range deleted {
		if n != 1 {
			t.Fatalf("session %s deleted %d times, want exactly 1", id, n)
		}
		num := id[1:] // "sN" → "N"
		if num != "1" && num != "2" && num != "3" {
			t.Fatalf("episode deleted non-oldest session %s (deleted set %v)", id, deleted)
		}
	}
}

// TestCleanupPassNoopAtOrBelowFloor: exactly floor sessions → an episode
// roll that fires still deletes nothing.
func TestCleanupPassNoopAtOrBelowFloor(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 5; i++ { // == DefaultCleanupFloor
		postOneChat(t, ts.URL)
	}

	runPass(gw.cleanup, 7)

	time.Sleep(300 * time.Millisecond)
	_, deleted := f.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("cleanup fired at floor: deleted %v, want none", deleted)
	}
}

// TestCleanupProbabilityZeroNeverFires: p=0 skips every account even well
// above the floor.
func TestCleanupProbabilityZeroNeverFires(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}

	gw.cleanup.cfg.probability = 0
	gw.cleanup.rng = rand.New(rand.NewSource(1))
	gw.cleanup.pass(context.Background())

	time.Sleep(300 * time.Millisecond)
	_, deleted := f.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("cleanup fired with probability 0: deleted %v", deleted)
	}
}

// TestCleanupIntervalJitterVaries: nextInterval stays within base ±50% and
// actually varies — no fixed cadence for the risk service to learn.
func TestCleanupIntervalJitterVaries(t *testing.T) {
	s := &cleanupScheduler{
		cfg: cleanupConfig{interval: time.Hour},
		rng: rand.New(rand.NewSource(7)),
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 30; i++ {
		d := s.nextInterval()
		if d < 30*time.Minute || d > 90*time.Minute {
			t.Fatalf("interval %s outside base ±50%% bounds", d)
		}
		seen[d] = true
	}
	if len(seen) < 3 {
		t.Fatalf("jitter too static: %d distinct intervals over 30 draws", len(seen))
	}
}

// TestCleanupGapJitterVaries: the in-episode delete gap is randomized within
// its bounds.
func TestCleanupGapJitterVaries(t *testing.T) {
	s := &cleanupScheduler{
		cfg: cleanupConfig{gapMin: time.Second, gapMax: 6 * time.Second},
		rng: rand.New(rand.NewSource(11)),
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 30; i++ {
		d := s.nextGap()
		if d < time.Second || d > 6*time.Second {
			t.Fatalf("gap %s outside [1s, 6s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 3 {
		t.Fatalf("gap jitter too static: %d distinct gaps over 30 draws", len(seen))
	}
}

// TestCleanupFiresOnConfiguredInterval: a short configured interval makes
// the background loop fire episodes without any further chat traffic.
func TestCleanupFiresOnConfiguredInterval(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.CleanupInterval = 40 * time.Millisecond
	})

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}

	// The loop wakes every ~20-60ms (jitter), each wake a 50% episode roll;
	// a delete must appear without any new request.
	eventually(t, 10*time.Second, "background cleanup episode to fire", func() bool {
		_, deleted := f.snapshot()
		return len(deleted) >= 1
	})
	_, deleted := f.snapshot()
	for id, n := range deleted {
		if n != 1 {
			t.Fatalf("session %s deleted %d times, want exactly 1 (victims must not be re-issued)", id, n)
		}
	}
	if !gw.cleanup.started {
		t.Fatal("cleanup loop not marked started")
	}
}

// TestCleanupDisabledWhenIntervalZero: interval 0 = no loop goroutine, no
// deletes ever.
func TestCleanupDisabledWhenIntervalZero(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	if gw.cleanup.started {
		t.Fatal("cleanup loop started with interval 0, want disabled")
	}
	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}
	time.Sleep(300 * time.Millisecond)
	_, deleted := f.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("cleanup fired while disabled: deleted %v", deleted)
	}
}

// TestCleanupSkipsInactiveAccounts: an account that never served a request
// holds no cached token — cleanup must not touch it (no login, no
// fetch_page, nothing).
func TestCleanupSkipsInactiveAccounts(t *testing.T) {
	f := newCleanupFixture(t)
	f.seedOrphans(10) // upstream truth the gateway never created
	gw, _ := newCleanupServer(t, f.srv.URL, nil)

	runPass(gw.cleanup, 3)

	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	pages := f.pageHits
	f.mu.Unlock()
	if pages != 0 {
		t.Fatalf("inactive account received %d fetch_page hits, want 0", pages)
	}
	_, deleted := f.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("inactive account had sessions deleted: %v", deleted)
	}
}

// TestCleanupShutdownDrainsCleanly: after Shutdown the loop goroutine has
// exited (done closed) and a second Shutdown is safe.
func TestCleanupShutdownDrainsCleanly(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, func(c *Config) {
		c.CleanupInterval = 20 * time.Millisecond
	})

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}
	eventually(t, 10*time.Second, "cleanup episode to fire", func() bool {
		_, deleted := f.snapshot()
		return len(deleted) >= 1
	})

	gw.Shutdown()
	select {
	case <-gw.cleanup.done:
	default:
		t.Fatal("cleanup loop goroutine still running after Shutdown")
	}
	gw.Shutdown() // idempotent
}

// TestCleanupHealsPreExistingOrphans: sessions that predate this gateway
// process (restart leftovers, visible only in the upstream drawer) are
// cleaned by an episode once the account is active — fetch_page is the
// truth, not the in-memory registry.
func TestCleanupHealsPreExistingOrphans(t *testing.T) {
	f := newCleanupFixture(t)
	f.seedOrphans(8) // o1..o8, older than anything the gateway creates
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	postOneChat(t, ts.URL) // activates the account (login + cached token)

	runPass(gw.cleanup, 99)

	eventually(t, 3*time.Second, "orphan cleanup to drain", func() bool {
		_, deleted := f.snapshot()
		return len(deleted) >= 1
	})
	_, deleted := f.snapshot()
	if len(deleted) > 3 {
		t.Fatalf("episode deleted %d orphans, want ≤3 (bounded batch)", len(deleted))
	}
	for id := range deleted {
		if !strings.HasPrefix(id, "o") {
			t.Fatalf("episode deleted non-orphan %s (deleted %v)", id, deleted)
		}
	}
}

// TestCleanupKeepsPinnedSessions: pinned chats are never tidied — humans
// don't delete what they pinned. 8 unpinned + 2 pinned (oldest); the
// pinned oldest two must survive every episode.
func TestCleanupKeepsPinnedSessions(t *testing.T) {
	f := newCleanupFixture(t)
	f.seedOrphans(2) // o1, o2 — oldest of all
	f.pin("o1")
	f.pin("o2")
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}

	// Several episodes with different seeds; none may touch o1/o2.
	for seed := int64(1); seed <= 5; seed++ {
		gw.cleanup.rng = rand.New(rand.NewSource(seed))
		gw.cleanup.pass(context.Background())
		time.Sleep(100 * time.Millisecond)
	}
	_, deleted := f.snapshot()
	for id := range deleted {
		if id == "o1" || id == "o2" {
			t.Fatalf("pinned session %s deleted (deleted %v)", id, deleted)
		}
	}
}

// TestCleanupNoDoubleDeleteAcrossEpisodes: two passes back-to-back must
// not re-delete a session still draining through the async deleter.
func TestCleanupNoDoubleDeleteAcrossEpisodes(t *testing.T) {
	f := newCleanupFixture(t)
	gw, ts := newCleanupServer(t, f.srv.URL, nil)

	for i := 0; i < 8; i++ {
		postOneChat(t, ts.URL)
	}

	runPass(gw.cleanup, 5)
	runPass(gw.cleanup, 5) // same seed, same victims — must not re-enqueue

	eventually(t, 3*time.Second, "deletes to drain", func() bool {
		_, deleted := f.snapshot()
		return len(deleted) >= 1
	})
	time.Sleep(300 * time.Millisecond)
	_, deleted := f.snapshot()
	for id, n := range deleted {
		if n > 1 {
			t.Fatalf("session %s deleted %d times across episodes (draining re-issue)", id, n)
		}
	}
}

// TestCleanupSkipsParkedAccounts: an account muted upstream mid-flight
// (biz 5 on the completion call → pool parks it) must receive zero
// cleanup traffic — parked accounts stay cold. The only fetch_page that
// may land is the one-shot app-launch startup sequence after first login.
func TestCleanupSkipsParkedAccounts(t *testing.T) {
	mux := http.NewServeMux()
	var mu sync.Mutex
	var pageHits int
	var muted bool
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("GET /api/v0/users/current", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	mux.HandleFunc("GET /api/v0/chat_session/fetch_page", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		pageHits++
		mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "",
			"biz_data": mc{"chat_sessions": []mc{}, "has_more": false}}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s1") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		// Mute the account: the pool parks it via NoteError on the error.
		mu.Lock()
		muted = true
		mu.Unlock()
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 5, "biz_msg": "user is muted",
			"biz_data": mc{"mute_until": time.Now().Add(time.Hour).Format("2006-01-02 15:04:05")}}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	gw, ts := newCleanupServer(t, srv.URL, func(c *Config) {
		c.CleanupInterval = 20 * time.Millisecond // loop wakes ~25x/500ms
	})
	_ = gw

	// One request: login OK, completion mutes → account parked.
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Wait for the mute to register and the startup fetch_page to land.
	eventually(t, 3*time.Second, "account parked", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return muted
	})
	eventually(t, 3*time.Second, "startup fetch_page fired", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return pageHits >= 1
	})

	// Let the cleanup loop wake many times against the parked account.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	pages := pageHits
	mu.Unlock()
	if pages != 1 {
		t.Fatalf("parked account received %d fetch_page hits total, want exactly 1 (startup only)", pages)
	}
}

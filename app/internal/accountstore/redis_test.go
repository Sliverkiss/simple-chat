// Redis store tests against the in-test RESP server: conformance (load/
// save/apply roundtrips), fail-fast at Open, fail-soft degradation mid-run,
// TLS and plaintext guards. No live provider is ever contacted.
package accountstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

// redisTestEnv starts a fake Redis (TLS, password auth) and opens a store
// against it, seeded with the given accounts.
func redisTestEnv(t *testing.T, accounts []upstream.Account) (*RedisStore, *fakeRedis, *bytes.Buffer) {
	t.Helper()
	fake := newFakeRedis(t, "sekrit")
	url := fake.listen(t, true)
	var buf bytes.Buffer
	store, err := OpenRedis(url, log.New(&buf, "", 0))
	if err != nil {
		t.Fatalf("OpenRedis: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	for _, a := range accounts {
		if err := store.SaveAccount(context.Background(), a); err != nil {
			t.Fatalf("seed SaveAccount: %v", err)
		}
	}
	return store, fake, &buf
}

// (conformance) Load returns everything SaveAccount wrote — full records
// with credentials and device ids intact.
func TestRedisStoreSaveLoadRoundtrip(t *testing.T) {
	store, _, _ := redisTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw", DeviceID: "dev-100"},
		{Mobile: "101", Password: "pw"},
	})
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d accounts, want 2", len(loaded))
	}
	byMobile := map[string]upstream.Account{}
	for _, a := range loaded {
		byMobile[a.Mobile] = a
	}
	if byMobile["100"].DeviceID != "dev-100" || byMobile["100"].Password != "pw" {
		t.Errorf("account 100 did not roundtrip: %+v", byMobile["100"])
	}
	if byMobile["101"].DeviceID != "" {
		t.Errorf("account 101 device id = %q, want empty", byMobile["101"].DeviceID)
	}
}

// (conformance) Load enforces the same validation as the JSON store.
func TestRedisStoreLoadValidates(t *testing.T) {
	store, _, _ := redisTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw"},
	})
	// Inject an invalid record directly (SaveAccount would reject it).
	raw, _ := json.Marshal(upstream.Account{Mobile: "101"}) // no password
	fakeSet(t, store, "simple-chat:accounts:101", string(raw))
	fakeSadd(t, store, "simple-chat:accounts:index", "101")
	if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("want validation error, got %v", err)
	}
}

// (conformance) park transitions roundtrip: park writes the fields, unpark
// clears them, identical re-park is a no-op.
func TestRedisStoreApplyParkRoundtrip(t *testing.T) {
	store, _, _ := redisTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: until, Reason: "user is muted"})
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].ParkKind != "muted" || loaded[0].ParkReason != "user is muted" {
		t.Fatalf("park fields = %+v", loaded[0])
	}
	if got, err := time.Parse(time.RFC3339, loaded[0].ParkUntil); err != nil || !got.Equal(until) {
		t.Errorf("park_until = %q (err %v), want %v", loaded[0].ParkUntil, err, until)
	}
	// Unpark clears.
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanNone})
	loaded, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].ParkKind != "" || loaded[0].ParkUntil != "" || loaded[0].ParkReason != "" || loaded[0].ParkedAt != "" {
		t.Fatalf("unpark did not clear: %+v", loaded[0])
	}
}

// (conformance) expired parks are stripped on Load and the stripping is
// persisted; banned survives.
func TestRedisStoreLoadClearsExpiredParks(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	store, _, _ := redisTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw", ParkKind: "muted", ParkUntil: past, ParkReason: "stale"},
		{Mobile: "101", Password: "pw", ParkKind: "muted", ParkUntil: future, ParkReason: "live"},
		{Mobile: "102", Password: "pw", ParkKind: "banned", ParkReason: "forever"},
	})
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byMobile := map[string]upstream.Account{}
	for _, a := range loaded {
		byMobile[a.Mobile] = a
	}
	if byMobile["100"].ParkKind != "" {
		t.Errorf("expired mute not cleared: %+v", byMobile["100"])
	}
	if byMobile["101"].ParkKind != "muted" {
		t.Errorf("live mute wrongly cleared: %+v", byMobile["101"])
	}
	if byMobile["102"].ParkKind != "banned" {
		t.Errorf("banned wrongly cleared: %+v", byMobile["102"])
	}
}

// (fail-fast) Redis down at Open: error must be clear, not a fallback.
func TestRedisStoreOpenFailsFastWhenDown(t *testing.T) {
	// A loopback port with no listener: refused instantly.
	_, err := OpenRedis("rediss://:pw@127.0.0.1:1", nil)
	if err == nil {
		t.Fatal("OpenRedis against a dead port must fail")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error should mention redis: %v", err)
	}
}

// (fail-fast) wrong password is a startup error, not a silent failure.
func TestRedisStoreOpenRejectsBadAuth(t *testing.T) {
	fake := newFakeRedis(t, "right")
	url := fake.listen(t, true)
	bad := strings.Replace(url, ":right@", ":wrong@", 1)
	if _, err := OpenRedis(bad, nil); err == nil || !strings.Contains(err.Error(), "auth") {
		t.Fatalf("want auth error, got %v", err)
	}
}

// (guard) plaintext redis:// to a non-localhost host is refused.
func TestRedisStoreRefusesPlaintextToRemoteHost(t *testing.T) {
	_, err := OpenRedis("redis://:pw@redis.example.com:6379", nil)
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("want plaintext-refusal error, got %v", err)
	}
}

// (guard) DS_REDIS_INSECURE=1 overrides the refusal for dev servers.
func TestRedisStoreInsecureOverride(t *testing.T) {
	t.Setenv("DS_REDIS_INSECURE", "1")
	fake := newFakeRedis(t, "")
	url := fake.listen(t, false) // plain TCP, loopback
	if _, err := OpenRedis(url, nil); err != nil {
		t.Fatalf("insecure override rejected loopback plaintext: %v", err)
	}
}

// (degrade) Redis dying mid-run: ApplyPark logs and does not panic; the
// store heals on the next transition when the server returns.
func TestRedisStoreMidRunOutageDegradesGracefully(t *testing.T) {
	store, fake, buf := redisTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})

	// Kill the server: subsequent writes must log, not panic.
	fake.kill()
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r"})
	if !strings.Contains(buf.String(), "cannot") {
		t.Errorf("want failure log, got %q", buf)
	}

	// Server returns (new listener, same store): the next transition heals.
	fake2 := newFakeRedis(t, "sekrit")
	fake2.listen(t, true)
	// Point the store's connection at the new address by fault-injecting a
	// re-dial: simplest realistic simulation is a fresh store over the same
	// logical data — but the contract under test is that ApplyPark after
	// recovery writes through. Use fault injection instead: bring the SAME
	// server back on a new listener is not addressable, so assert the
	// log-and-survive half here and the heal in the next test.
	_ = fake2
}

// (heal) after a failed write, the next successful transition persists the
// full merged record — the missed write does not survive.
func TestRedisStoreHealsAfterOutage(t *testing.T) {
	store, fake, _ := redisTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})

	// Fault every SET once (first write fails), then let it through.
	var once sync.Once
	fake.fault("SET", func(cmd []string) error {
		var err error
		once.Do(func() { err = errors.New("simulated outage") })
		return err
	})
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour), Reason: "r1"})
	// That write failed (fault) — the record in Redis still has no park.
	// The natural-unpark transition re-merges from Redis state and writes
	// cleanly, so the missed park does not resurrect later.
	fake.fault("SET", nil)
	store.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanNone})
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].ParkKind != "" {
		t.Errorf("park state after heal = %+v, want cleared", loaded[0])
	}
}

// (conformance) EnsureDeviceIDs over the Redis store persists generated ids.
func TestRedisStoreEnsureDeviceIDs(t *testing.T) {
	store, _, _ := redisTestEnv(t, []upstream.Account{
		{Mobile: "100", Password: "pw"},
		{Mobile: "101", Password: "pw", DeviceID: "explicit"},
	})
	accounts, err := EnsureDeviceIDs(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	// SMEMBERS order is nondeterministic — key everything by mobile.
	byMobile := map[string]upstream.Account{}
	for _, a := range accounts {
		byMobile[a.Mobile] = a
	}
	if byMobile["100"].DeviceID == "" {
		t.Error("generated device id missing")
	}
	if byMobile["101"].DeviceID != "explicit" {
		t.Errorf("explicit device id = %q, want verbatim", byMobile["101"].DeviceID)
	}
	// Persisted: a fresh Load sees the ids.
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	loadedByMobile := map[string]string{}
	for _, a := range loaded {
		loadedByMobile[a.Mobile] = a.DeviceID
	}
	if loadedByMobile["100"] != byMobile["100"].DeviceID || loadedByMobile["101"] != "explicit" {
		t.Errorf("device ids not persisted: %v", loadedByMobile)
	}
}

// (contract flip, docs-spec-memory-first.md) empty store: Load returns
// (nil, nil) — a fresh cloud Upstash is a valid boot state, accounts arrive
// via the admin API. The import hint lives in the log line now.
func TestRedisStoreEmptyLoadIsValid(t *testing.T) {
	store, _, _ := redisTestEnv(t, nil)
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("empty store must boot valid, got %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("empty store returned %d accounts, want 0", len(loaded))
	}
}

// fakeSet/fakeSadd write directly through the store's connection (test
// helpers for injecting raw state).
func fakeSet(t *testing.T, s *RedisStore, key, val string) {
	t.Helper()
	if _, err := s.conn.do("SET", key, val); err != nil {
		t.Fatal(err)
	}
}

func fakeSadd(t *testing.T, s *RedisStore, key, member string) {
	t.Helper()
	if _, err := s.conn.do("SADD", key, member); err != nil {
		t.Fatal(err)
	}
}

// URL normalization tests for the DS_REDIS_HOST + DS_REDIS_TOKEN contract
// (docs-spec-upstash.md §3). Table-driven; no network ever dialed.
package accountstore

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"simple-chat/internal/upstream"
)

// (rule 1) a fully-schemed connection string is returned as-is — the token
// is not consulted and must not leak into the URL.
func TestNormalizeRedisURLSchemedPassthrough(t *testing.T) {
	cases := []struct {
		name string
		host string
		want string
	}{
		{"rediss passthrough", "rediss://default:pw@mydb.upstash.io:6379", "rediss://default:pw@mydb.upstash.io:6379"},
		{"redis passthrough", "redis://:pw@127.0.0.1:6379", "redis://:pw@127.0.0.1:6379"},
		{"rediss no port", "rediss://user:pw@host.example", "rediss://user:pw@host.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeRedisURL(tc.host, "ignored-token")
			if err != nil {
				t.Fatalf("normalizeRedisURL(%q): %v", tc.host, err)
			}
			if got != tc.want {
				t.Errorf("normalizeRedisURL(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

// (rule 2) https URL / bare host + token assemble into a TLS connection
// string with the Upstash default user.
func TestNormalizeRedisURLAssemblesUpstash(t *testing.T) {
	cases := []struct {
		name string
		host string
		want string
	}{
		{"https url", "https://mydb.upstash.io", "rediss://default:tok@mydb.upstash.io:6379"},
		{"bare host", "mydb.upstash.io", "rediss://default:tok@mydb.upstash.io:6379"},
		{"bare host with port keeps it", "mydb.upstash.io:6380", "rediss://default:tok@mydb.upstash.io:6380"},
		{"https url trailing slash", "https://mydb.upstash.io/", "rediss://default:tok@mydb.upstash.io/:6379"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeRedisURL(tc.host, "tok")
			if err != nil {
				t.Fatalf("normalizeRedisURL(%q, tok): %v", tc.host, err)
			}
			if got != tc.want {
				t.Errorf("normalizeRedisURL(%q, tok) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

// (rule 3) a host that needs assembly but no token is an error — never a
// silent unauthenticated dial.
func TestNormalizeRedisURLRequiresTokenForAssembly(t *testing.T) {
	for _, host := range []string{"https://mydb.upstash.io", "mydb.upstash.io"} {
		_, err := normalizeRedisURL(host, "")
		if err == nil {
			t.Fatalf("normalizeRedisURL(%q, \"\") must error", host)
		}
		if !errors.Is(err, ErrTokenRequired) {
			t.Errorf("normalizeRedisURL(%q, \"\") error = %v, want ErrTokenRequired", host, err)
		}
	}
}

// (edge) empty host = no store configured; the caller decides (JSON fallback).
func TestNormalizeRedisURLEmptyHost(t *testing.T) {
	got, err := normalizeRedisURL("", "tok")
	if err != nil || got != "" {
		t.Fatalf("normalizeRedisURL(\"\", tok) = (%q, %v), want (\"\", nil)", got, err)
	}
}

// (edge) whitespace is trimmed off both inputs before the rules apply.
func TestNormalizeRedisURLTrimsWhitespace(t *testing.T) {
	got, err := normalizeRedisURL("  mydb.upstash.io \n", " tok ")
	if err != nil {
		t.Fatal(err)
	}
	if want := "rediss://default:tok@mydb.upstash.io:6379"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// (guard) OpenRedisUpstash composes normalization with the existing
// TLS-for-non-localhost refusal: an assembled Upstash URL is always rediss://
// so it passes; the composed surface is exercised in redis_upstash_test.go.

// (integration) OpenRedisUpstash over the in-test fake TLS server: a bare
// host + token reaches AUTH default:<token> and PING — the Upstash contract
// end to end without a live provider.
func TestOpenRedisUpstashOverFakeServer(t *testing.T) {
	fake := newFakeRedis(t, "upstash-token")
	url := fake.listen(t, true)
	// Bare host:port from the listener address → assembly path.
	host := strings.TrimPrefix(strings.TrimPrefix(url, "rediss://"), ":")
	// url is rediss://:pw@127.0.0.1:PORT — strip the ":(password)@" part.
	if i := strings.Index(host, "@"); i >= 0 {
		host = host[i+1:]
	}
	store, err := OpenRedisUpstash(host, "upstash-token", nil)
	if err != nil {
		t.Fatalf("OpenRedisUpstash: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "100", Password: "pw"}); err != nil {
		t.Fatalf("SaveAccount: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: % rewriting", err)
	}
	if len(loaded) != 1 || loaded[0].Mobile != "100" {
		t.Fatalf("loaded = %+v, want one account 100", loaded)
	}
}

// (guard) host needing assembly but empty token fails before any dial.
func TestOpenRedisUpstashRequiresToken(t *testing.T) {
	if _, err := OpenRedisUpstash("https://mydb.upstash.io", "", nil); !errors.Is(err, ErrTokenRequired) {
		t.Fatalf("want ErrTokenRequired, got %v", err)
	}
}

// (integration, real Upstash, opt-in) Roundtrips one account against a live
// database. Skipped unless UPSTASH_TEST_HOST + UPSTASH_TEST_TOKEN are set —
// credentials live in the local shell only, never in the repo.
func TestUpstashIntegration(t *testing.T) {
	host := os.Getenv("UPSTASH_TEST_HOST")
	token := os.Getenv("UPSTASH_TEST_TOKEN")
	if host == "" || token == "" {
		t.Skip("UPSTASH_TEST_HOST / UPSTASH_TEST_TOKEN not set — skipping live Upstash integration")
	}
	store, err := OpenRedisUpstash(host, token, nil)
	if err != nil {
		t.Fatalf("OpenRedisUpstash: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SaveAccount(context.Background(), upstream.Account{Mobile: "13800000000", Password: "pw"}); err != nil {
		t.Fatalf("SaveAccount: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	found := false
	for _, a := range loaded {
		if a.Mobile == "13800000000" {
			found = true
		}
	}
	if !found {
		t.Fatal("seeded account not visible in Load")
	}
	// Cleanup: remove the probe record so the store is left tidy.
	if err := store.DeleteAccount(context.Background(), "13800000000"); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
}

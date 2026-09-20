package accountstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"simple-chat/internal/upstream"
)

// Redis key schema:
//
//	simple-chat:accounts:<mobile>   → JSON-encoded upstream.Account (full
//	                                  record: credentials, device id, park)
//	simple-chat:accounts:index      → SET of every <mobile> in the store
//
// One key per account keeps SaveAccount O(1) (no read-modify-write of a
// whole blob); the index set is the enumeration surface for Load.
const (
	redisKeyPrefix = "simple-chat:accounts:"
	redisIndexKey  = "simple-chat:accounts:index"
)

// redisRootCAs is an optional extra CA pool for the TLS handshake — a test
// hook (injected by redis_test.go for the in-test server's self-signed
// cert); nil in production, where the system roots verify the provider's
// certificate.
var redisRootCAs *x509.CertPool

// RedisStore persists accounts into Redis (e.g. a managed Upstash instance).
// It is chosen by setting DS_REDIS_HOST (+ DS_REDIS_TOKEN); the JSON file
// store remains the default when the host variable is unset.
//
// Passwords live in Redis in plaintext — the store therefore refuses
// non-TLS (redis://) connections to non-localhost hosts at Open time. Set
// DS_REDIS_INSECURE=1 only for a loopback dev server.
type RedisStore struct {
	conn   *respConn
	logger *log.Logger

	mu sync.Mutex // serializes read-modify-write cycles (ApplyPark)
}

// ErrTokenRequired reports a DS_REDIS_HOST that needs assembly (bare host
// or https:// URL) but no DS_REDIS_TOKEN — dialing unauthenticated would
// silently fail auth, so refuse up front.
var ErrTokenRequired = errors.New("upstash host requires DS_REDIS_TOKEN (set the token env var or use a full rediss:// connection string in DS_REDIS_HOST)")

// normalizeRedisURL composes DS_REDIS_HOST + DS_REDIS_TOKEN into a
// redis/rediss connection string (docs-spec-upstash.md §3):
//
//   - "" → "" (no store configured; caller falls back to the JSON file)
//   - already "rediss://" or "redis://" → returned as-is (token unused)
//   - otherwise (https://… or bare host) → strip any scheme, assemble
//     "rediss://default:<token>@<host>:6379"
//
// Assembly without a token is ErrTokenRequired — never a silent
// unauthenticated dial.
func normalizeRedisURL(host, token string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", nil
	}
	if strings.HasPrefix(host, "rediss://") || strings.HasPrefix(host, "redis://") {
		return host, nil
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", ErrTokenRequired
	}
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	// A host that already carries an explicit port keeps it; only a bare
	// host gets the Upstash default :6379 (blind appending would produce
	// "host:6380:6379", which url.Parse cannot dial).
	port := ":6379"
	if u, err := url.Parse("//" + host); err == nil && u.Port() != "" {
		port = ""
	}
	return "rediss://default:" + token + "@" + host + port, nil
}

// OpenRedisUpstash opens the Redis store from the two-env-var Upstash
// contract (DS_REDIS_HOST + DS_REDIS_TOKEN): normalize, then hand the
// resulting connection string to OpenRedis (fail-fast, TLS rules and all).
func OpenRedisUpstash(host, token string, logger *log.Logger) (*RedisStore, error) {
	rawURL, err := normalizeRedisURL(host, token)
	if err != nil {
		return nil, err
	}
	return OpenRedis(rawURL, logger)
}

// OpenRedis dials, authenticates, and pings; any failure is fatal — when
// a Redis host is configured, Redis is the configured source of truth and
// starting without it would silently serve zero accounts.
func OpenRedis(rawURL string, logger *log.Logger) (*RedisStore, error) {
	_, _, _, useTLS, err := parseRedisURL(rawURL)
	if err != nil {
		return nil, err
	}
	if !useTLS && !isLoopbackURL(rawURL) && os.Getenv("DS_REDIS_INSECURE") != "1" {
		return nil, errors.New("redis account store: refusing plaintext redis:// to a non-localhost host — passwords would transit unencrypted; use rediss:// (TLS) or set DS_REDIS_INSECURE=1 for a local dev server")
	}
	tlsCfg := &tls.Config{ServerName: serverName(rawURL), RootCAs: redisRootCAs}
	conn, err := newRespConn(rawURL, tlsCfg)
	if err != nil {
		return nil, err
	}
	s := &RedisStore{conn: conn, logger: logger}
	// Fail fast: dial + AUTH + PING (managed providers expire credentials;
	// better a clear startup error than a store that cannot load).
	if err := s.conn.doPing(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("redis account store: %w", err)
	}
	return s, nil
}

// urlHost extracts the bare hostname from a redis/rediss URL.
func urlHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// isLoopbackURL reports whether the URL's host is 127.0.0.0/8, ::1, or
// "localhost".
func isLoopbackURL(rawURL string) bool {
	host := strings.Trim(urlHost(rawURL), "[]")
	if host == "localhost" || host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// serverName extracts the SNI hostname from the URL.
func serverName(rawURL string) string {
	return strings.Trim(urlHost(rawURL), "[]")
}

// Load returns every account in the index. An account key that is missing
// (deleted out-of-band) or unparseable is skipped with a log line — one bad
// row must not take the pool down.
func (s *RedisStore) Load(ctx context.Context) ([]upstream.Account, error) {
	reply, err := s.conn.do("SMEMBERS", redisIndexKey)
	if err != nil {
		return nil, err
	}
	members, _ := reply.([]any)
	var accounts []upstream.Account
	for _, m := range members {
		mobile, _ := m.(string)
		raw, err := s.get(ctx, redisKeyPrefix+mobile)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue // index ahead of data: skip, don't brick the pool
		}
		var acct upstream.Account
		if err := json.Unmarshal([]byte(raw.(string)), &acct); err != nil {
			s.logf("redis store: skipping unparseable account key for %s: %v", mobile, err)
			continue
		}
		if parkExpired(acct, time.Now()) {
			acct.ParkKind, acct.ParkUntil = "", ""
			acct.ParkReason, acct.ParkedAt = "", ""
			if err := s.SaveAccount(ctx, acct); err != nil {
				s.logf("redis store: cannot clear expired park for %s: %v", mobile, err)
			}
		}
		accounts = append(accounts, acct)
	}
	if len(accounts) == 0 {
		// Empty store is a valid state (docs-spec-memory-first.md): a fresh
		// cloud Upstash boots an empty pool; accounts arrive via the admin
		// API (or -import-redis for bulk seeding).
		s.logf("redis account store: no accounts under %s — starting empty (seed via admin API or -import-redis)", redisIndexKey)
		return nil, nil
	}
	for _, a := range accounts {
		if err := a.Validate(); err != nil {
			return nil, fmt.Errorf("redis account %s: %w", a.Mobile, err)
		}
	}
	return accounts, nil
}

// SaveAccount upserts one account: SET the per-identity key (whole record
// as JSON; identity = mobile, else email) and SADD the identity to the
// index. Redis is single-threaded per command, so there is no
// read-modify-write to race.
func (s *RedisStore) SaveAccount(ctx context.Context, acct upstream.Account) error {
	if err := acct.Validate(); err != nil {
		return fmt.Errorf("save account %s: %w", acct.Identity(), err)
	}
	raw, err := json.Marshal(acct)
	if err != nil {
		return err
	}
	id := acct.Identity()
	if _, err := s.conn.do("SET", redisKeyPrefix+id, string(raw)); err != nil {
		return err
	}
	if _, err := s.conn.do("SADD", redisIndexKey, id); err != nil {
		return err
	}
	return nil
}

// DeleteAccount removes the account's key from the index set and drops its
// data key. ErrAccountNotFound when the identity is not in the index — the
// admin API maps that to 404. The removal is plain Redis deletion: no
// upstream logout, no token invalidation.
func (s *RedisStore) DeleteAccount(ctx context.Context, identity string) error {
	// The index stores the account's identity (mobile, else email).
	reply, err := s.conn.do("SREM", redisIndexKey, identity)
	if err != nil {
		return err
	}
	if n, _ := reply.(int64); n == 0 {
		return ErrAccountNotFound
	}
	_, err = s.conn.do("DEL", redisKeyPrefix+identity)
	return err
}

// ApplyPark merges one park transition: GET the record, merge in memory,
// SET it back. Under s.mu so concurrent transitions serialize. Failures are
// logged only (fail-soft): the in-memory pool is the live truth, and the
// next transition re-writes the full record, healing any missed write.
func (s *RedisStore) ApplyPark(rec upstream.ParkRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := s.get(context.Background(), redisKeyPrefix+rec.Mobile)
	if err != nil {
		s.logf("redis store: cannot load %s to persist park state: %v", rec.Mobile, err)
		return
	}
	if raw == nil {
		// Park for a mobile not in the store (e.g. an imported account
		// removed mid-run): nothing to update — same no-op the JSON store
		// takes for an unknown mobile.
		s.logf("redis store: park for unknown mobile %s skipped", rec.Mobile)
		return
	}
	var acct upstream.Account
	if err := json.Unmarshal([]byte(raw.(string)), &acct); err != nil {
		s.logf("redis store: cannot parse %s to persist park state: %v", rec.Mobile, err)
		return
	}
	if !applyParkRecord(&acct, rec, time.Now()) {
		return
	}
	out, err := json.Marshal(acct)
	if err != nil {
		s.logf("redis store: cannot marshal park state: %v", err)
		return
	}
	if _, err := s.conn.do("SET", redisKeyPrefix+rec.Mobile, string(out)); err != nil {
		s.logf("redis store: cannot persist park state for %s: %v", rec.Mobile, err)
	}
}

// get wraps conn.do("GET", key) with nil-typing for the missing-key case.
func (s *RedisStore) get(ctx context.Context, key string) (any, error) {
	return s.conn.do("GET", key)
}

// Close closes the underlying connection.
func (s *RedisStore) Close() error { return s.conn.Close() }

// logf logs through the store's logger when set.
func (s *RedisStore) logf(format string, args ...any) {
	if s != nil && s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

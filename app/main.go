// simple-chat: minimal OpenAI-compatible chat gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/server"
)

// openAccountStore builds the persistence store from the environment:
// DS_REDIS_HOST set → Upstash as the sole source of truth, memory-first at
// runtime (one boot load, then write-through only; unreachable at startup
// is fatal), unset → the accounts.json file (unchanged default). The second
// return is the human-readable name for the startup log line.
func openAccountStore(ctx context.Context, host, token, accountsPath string, logger *log.Logger) (accountstore.Store, string, error) {
	if host != "" {
		redis, err := accountstore.OpenRedisUpstash(host, token, logger)
		if err != nil {
			return nil, "", fmt.Errorf("DS_REDIS_HOST set but store unusable — refusing to start on a stale/empty fallback: %w", err)
		}
		store, err := accountstore.NewMemoryFirstStore(ctx, redis, logger)
		if err != nil {
			redis.Close()
			return nil, "", fmt.Errorf("upstash load: %w", err)
		}
		return store, "upstash (memory-first, write-through)", nil
	}
	return accountstore.NewJSONStore(accountsPath, logger), "json file", nil
}

// redisHost renders a credential-free description of the configured Redis
// for log lines: a schemed connection string becomes "<scheme>://<host>",
// a bare/https Upstash host becomes "rediss://<host>".
func redisHost(host string) string {
	if !strings.HasPrefix(host, "rediss://") && !strings.HasPrefix(host, "redis://") {
		h := strings.TrimSpace(host)
		if i := strings.Index(h, "://"); i >= 0 {
			h = h[i+3:]
		}
		return "rediss://" + h
	}
	u, err := url.Parse(host)
	if err != nil || u.Host == "" {
		return "unparsable-url"
	}
	return u.Scheme + "://" + u.Host
}

func main() {
	addr := os.Getenv("DS_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	accountsPath := os.Getenv("DS_ACCOUNTS")
	if accountsPath == "" {
		accountsPath = "accounts.json"
	}
	host := os.Getenv("DS_REDIS_HOST")
	token := os.Getenv("DS_REDIS_TOKEN")
	maxInflight := 2
	if v := os.Getenv("DS_MAX_INFLIGHT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Fatalf("DS_MAX_INFLIGHT must be a positive integer, got %q", v)
		}
		maxInflight = n
	}
	// DS_MAX_PROMPT_CHARS: flattened-prompt cap. Unset or 0 = default
	// (2,000,000 chars); negative = guard disabled.
	maxPromptChars := 0
	if v := os.Getenv("DS_MAX_PROMPT_CHARS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("DS_MAX_PROMPT_CHARS must be an integer, got %q", v)
		}
		maxPromptChars = n
	}
	// DS_SESSION_CAP: hard safety ceiling on sessions kept per account
	// (apk-behavior.md §8 D1). Unset or 0 = no ceiling — the human-paced
	// cleanup policy (DS_CLEANUP_INTERVAL) does the real deleting. N > 0 =
	// oldest sessions beyond N evicted synchronously on create through the
	// async deleter (emergency valve).
	sessionCap := 0
	if v := os.Getenv("DS_SESSION_CAP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			log.Fatalf("DS_SESSION_CAP must be a non-negative integer, got %q", v)
		}
		sessionCap = n
	}
	// DS_CLEANUP_INTERVAL: base interval of the human-paced session
	// cleanup; each wake sleeps a uniformly jittered [interval/2,
	// interval*3/2]. Unset or 0 = default 1h; "0" (explicit) = cleanup
	// disabled entirely. Accepted as seconds ("90m"/"1h30m" style also
	// parses via time.ParseDuration).
	cleanupInterval := time.Hour
	if v := os.Getenv("DS_CLEANUP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cleanupInterval = d
		} else if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cleanupInterval = time.Duration(n) * time.Second
		} else {
			log.Fatalf("DS_CLEANUP_INTERVAL must be a duration or non-negative integer (seconds), got %q", v)
		}
	}
	// DS_CLEANUP_FLOOR: at/below this many upstream sessions a cleanup
	// episode deletes nothing. Unset or 0 = default 5.
	cleanupFloor := 0
	if v := os.Getenv("DS_CLEANUP_FLOOR"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			log.Fatalf("DS_CLEANUP_FLOOR must be a non-negative integer, got %q", v)
		}
		cleanupFloor = n
	}
	// DS_PURGE: master switch for the weekly purge-all-sessions. "0"
	// (explicit) disables it entirely; anything else leaves it on.
	// Unset = on (the weekly default).
	purgeEnabled := true
	if v := os.Getenv("DS_PURGE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n == 0 {
			purgeEnabled = false
		}
	}
	// DS_PURGE_WEEKDAY: weekly purge day (0=Monday .. 6=Sunday).
	// Unset = 6 (Sunday); -1 = disabled (with DS_PURGE=0).
	purgeWeekday := 6
	if v := os.Getenv("DS_PURGE_WEEKDAY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < -1 || n > 6 {
			log.Fatalf("DS_PURGE_WEEKDAY must be -1..6 (0=Monday, 6=Sunday), got %q", v)
		}
		purgeWeekday = n
	}
	// DS_PURGE_HOUR: weekly purge hour (0-23). Unset = 4 (04:00 local).
	purgeHour := 4
	if v := os.Getenv("DS_PURGE_HOUR"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 23 {
			log.Fatalf("DS_PURGE_HOUR must be 0-23, got %q", v)
		}
		purgeHour = n
	}
	if !purgeEnabled {
		purgeWeekday = -1
	}
	var importRedis string
	flag.StringVar(&addr, "addr", addr, "listen address")
	flag.StringVar(&accountsPath, "accounts", accountsPath, "path to accounts.json")
	flag.StringVar(&importRedis, "import-redis", "", "import accounts.json into the Redis store (DS_REDIS_HOST), then exit")
	flag.Parse()

	logger := log.New(os.Stderr, "[simple-chat] ", log.LstdFlags)

	// One-shot migration: load accounts.json, upsert every record into the
	// Redis store (DS_REDIS_HOST), exit.
	if importRedis != "" {
		if host == "" {
			log.Fatalf("-import-redis requires DS_REDIS_HOST to be set")
		}
		if err := importAccountsToRedis(importRedis, host, token, logger); err != nil {
			log.Fatalf("import: %v", err)
		}
		return
	}

	ctx := context.Background()
	store, storeName, err := openAccountStore(ctx, host, token, accountsPath, logger)
	if err != nil {
		log.Fatalf("account store: %v", err)
	}
	defer store.Close()

	accounts, err := accountstore.EnsureDeviceIDs(ctx, store)
	if err != nil {
		log.Fatalf("accounts: %v", err)
	}

	srv, err := server.NewServer(server.Config{
		Accounts:        accounts,
		ParkStore:       store,
		APIKey:          os.Getenv("DS_API_KEY"),
		MaxInflight:     maxInflight,
		MaxPromptChars:  maxPromptChars,
		SessionCap:      sessionCap,
		CleanupInterval: cleanupInterval,
		CleanupFloor:    cleanupFloor,
		PurgeEnabled:    purgeWeekday >= 0,
		PurgeWeekday:    purgeWeekday,
		PurgeHour:       purgeHour,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	authMode := "auth: open (DS_API_KEY unset)"
	if os.Getenv("DS_API_KEY") != "" {
		authMode = "auth: static API key required on /v1 routes"
	}
	sessionMode := "sessions: app-like accumulation (no auto-delete)"
	if sessionCap > 0 {
		sessionMode = "sessions: cap " + strconv.Itoa(sessionCap) + "/account (oldest evicted async)"
	}
	log.Printf("simple-chat listening on %s (model: deepseek-flash, account store: %s, accounts: %d, max in-flight/account: %d, reasoning: on (default), per-request opt-out, %s, %s)",
		addr, storeName, len(accounts), maxInflight, authMode, sessionMode)

	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	// Graceful shutdown: SIGTERM/SIGINT stops accepting, in-flight responses
	// finish (bounded), then pending async session deletes drain (bounded).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("shutdown signal received; draining")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(ctx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// Drain the async delete queue so no upstream session is left behind.
	srv.Shutdown()
	log.Printf("shutdown complete")
}

// importAccountsToRedis seeds a Redis store from an accounts.json file:
// every account is upserted (full record: credentials, device id, park
// state). Idempotent — re-running overwrites with the file's contents.
func importAccountsToRedis(path, host, token string, logger *log.Logger) error {
	jsonStore := accountstore.NewJSONStore(path, logger)
	accounts, err := jsonStore.Load(context.Background())
	if err != nil {
		return err
	}
	redisStore, err := accountstore.OpenRedisUpstash(host, token, logger)
	if err != nil {
		return err
	}
	defer redisStore.Close()
	for _, a := range accounts {
		if err := redisStore.SaveAccount(context.Background(), a); err != nil {
			return fmt.Errorf("account %s: %w", a.Mobile, err)
		}
	}
	log.Printf("imported %d accounts from %s into redis (%s)", len(accounts), path, redisHost(host))
	return nil
}

package server

import (
	"sync"

	"simple-chat/internal/upstream"
)

// sessionRegistry tracks created sessions per account for the app-like
// session lifecycle (apk-behavior.md §8 D1). With no cap configured it only
// records; with a cap it evicts the oldest sessions beyond N through the
// async deleter — genuine policy deletes, never per-request auto-deletes.
type sessionRegistry struct {
	mu    sync.Mutex
	cap   int // 0 = keep everything (app-like default)
	seq   map[string][]string // account key → sessions in creation order
	deleter *asyncDeleter
}

func newSessionRegistry(cap int, deleter *asyncDeleter) *sessionRegistry {
	return &sessionRegistry{
		cap:     cap,
		seq:     map[string][]string{},
		deleter: deleter,
	}
}

// record registers a created session and, when the cap is exceeded, enqueues
// deletes for the oldest surplus sessions through the async deleter. The
// evictions are computed under the lock so concurrent creates can't double-
// evict.
func (r *sessionRegistry) record(accountKey, sessionID string, client *upstream.Client, token string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq[accountKey] = append(r.seq[accountKey], sessionID)
	if r.cap <= 0 {
		return
	}
	sessions := r.seq[accountKey]
	if len(sessions) <= r.cap {
		return
	}
	// Evict the oldest (len - cap) sessions; drop them from the registry
	// immediately so a later record doesn't re-enqueue them.
	evict := sessions[:len(sessions)-r.cap]
	r.seq[accountKey] = sessions[len(sessions)-r.cap:]
	for _, id := range evict {
		r.deleter.enqueue(deleteJob{client: client, token: token, sessionID: id})
	}
}

// reset drops every recorded session for one account — used after a
// weekly delete_all purge, whose sessions are gone upstream (keeping
// them would poison DS_SESSION_CAP eviction and cleanup episodes).
func (r *sessionRegistry) reset(accountKey string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seq, accountKey)
}

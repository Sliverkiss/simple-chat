package upstream

import (
	"context"
	"testing"
	"time"

	"simple-chat/internal/sse"
)

// An explicit per-account parallel limit must not immediately feed the same
// physical identity to the next independent request (including duplicate rows).
func TestParallelLimitTemporarilyCoolsPhysicalIdentity(t *testing.T) {
	p := newTestPool(t, 2, 1, func(c PoolConfig) PoolConfig { c.RandomSeed = 1; return c })
	p.mu.Lock()
	p.accounts[1].ewmaNanos = float64(10 * time.Second)
	p.mu.Unlock()
	first, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Account().Identity() != p.accounts[0].account.Identity() {
		t.Fatalf("unexpected first identity %s", first.Account().Identity())
	}
	first.NoteError(&sse.StreamError{FinishReason: "parallel_chat_limit"})
	first.Release()
	next, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Account().Identity() == first.Account().Identity() {
		t.Fatalf("parallel-limited account immediately reselected")
	}
	next.Release()
	time.Sleep(1100 * time.Millisecond)
	restored, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Release()
	if restored.Account().Identity() != first.Account().Identity() {
		t.Fatalf("expired cooldown did not restore account: %s", restored.Account().Identity())
	}
}

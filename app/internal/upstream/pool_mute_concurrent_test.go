package upstream

import (
	"context"
	"testing"
	"time"
)

// Two already-issued leases can finish out of order. A late mute must not
// shorten the deadline already learned from another in-flight lease.
func TestConcurrentLeaseLateMuteDoesNotShortenKnownPark(t *testing.T) {
	var records []ParkRecord
	p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{MaxInflight: 2, QueueWait: time.Millisecond, OnParkPersist: func(r ParkRecord) { records = append(records, r) }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	late, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer late.Release()
	long := time.Now().Add(2 * time.Hour)
	first.NoteError(&BizError{BizCode: 5, BizMsg: "muted", MuteUntil: long})
	short := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(time.Minute)}
	// AccountManager.Completion marks the same shared manager before the
	// server calls Lease.NoteError; exercise that ordering explicitly.
	late.pa.am.mu.Lock()
	late.pa.am.markBan(short)
	late.pa.am.mu.Unlock()
	late.NoteError(short)
	if len(records) != 1 || records[0].Until.Before(long) {
		t.Fatalf("persisted mute shortened or rewritten: %+v", records)
	}
	snap := p.Snapshot()
	if snap[0].State != "muted" || snap[0].ParkUntil.Before(long) {
		t.Fatalf("late in-flight error shortened known mute: state=%s until=%s want >=%s", snap[0].State, snap[0].ParkUntil, long)
	}
}

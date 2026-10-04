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

// A raw timestamp can expire between the manager and lease observations.
// An intervening longer risk park must not turn that formerly valid mute
// into a fresh seven-day fallback.
func TestMuteExpiryBetweenManagerAndLeaseDoesNotExtendExistingRisk(t *testing.T) {
	var records []ParkRecord
	p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{
		MaxInflight: 2, RiskCooldown: 2 * time.Hour,
		OnParkPersist: func(r ParkRecord) { records = append(records, r) },
	})
	if err != nil {
		t.Fatal(err)
	}
	l, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	mute := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(50 * time.Millisecond)}
	l.pa.am.mu.Lock()
	l.pa.am.markBan(mute)
	first := l.pa.am.parkUntil
	l.pa.am.mu.Unlock()
	if !first.Equal(mute.MuteUntil.Add(time.Hour)) {
		t.Fatalf("manager park = %s", first)
	}
	risk := &BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}
	l.NoteError(risk)
	riskUntil := p.Snapshot()[0].ParkUntil
	time.Sleep(time.Until(mute.MuteUntil) + 10*time.Millisecond)
	l.NoteError(mute)
	snap := p.Snapshot()[0]
	if snap.State != "risk" || !snap.ParkUntil.Equal(riskUntil) || len(records) != 1 {
		t.Fatalf("expired raw mute extended existing park: state=%s until=%s records=%+v", snap.State, snap.ParkUntil, records)
	}
}

func TestMuteExpiryBetweenManagerAndLeasePersistsOriginalGrace(t *testing.T) {
	var records []ParkRecord
	p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{OnParkPersist: func(r ParkRecord) { records = append(records, r) }})
	if err != nil {
		t.Fatal(err)
	}
	l, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	mute := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(50 * time.Millisecond)}
	l.pa.am.mu.Lock()
	l.pa.am.markBan(mute)
	l.pa.am.mu.Unlock()
	time.Sleep(time.Until(mute.MuteUntil) + 10*time.Millisecond)
	l.NoteError(mute)
	if len(records) != 1 || !records[0].Until.Equal(mute.MuteUntil.Add(time.Hour)) || !p.Snapshot()[0].ParkUntil.Equal(records[0].Until) {
		t.Fatalf("original mute grace not persisted: records=%+v snapshot=%+v", records, p.Snapshot())
	}
}

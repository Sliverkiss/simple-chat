package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestManagerBanIsPersistedExactlyOnceByLease(t *testing.T) {
	const id = "13800000000"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/users/login" {
			fmt.Fprint(w, `{"code":0,"data":{"biz_code":10,"biz_msg":"USER_IS_BANNED"}}`)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	rec := &persistRecorder{}
	p, err := NewPool([]Account{{Mobile: id, Password: "fixture"}}, PoolConfig{BaseURL: srv.URL, OnParkPersist: rec.sink})
	if err != nil {
		t.Fatal(err)
	}
	l, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Token(context.Background())
	if BanKind(err) != BanBanned {
		t.Fatalf("login error = %v", err)
	}
	l.NoteError(err)
	l.NoteError(err)
	l.Release()
	records := rec.all()
	if len(records) != 1 || records[0].Kind != BanBanned || !records[0].Until.IsZero() {
		t.Fatalf("ban persistence = %+v", records)
	}
	restored, err := NewPool([]Account{{Mobile: id, Password: "fixture", ParkKind: ParkKindName(records[0].Kind), ParkReason: records[0].Reason}}, PoolConfig{BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Acquire(context.Background()); err == nil {
		t.Fatal("restarted banned identity became selectable")
	}
}

func TestManagerBanLateResponsesDoNotDowngradePersistedBan(t *testing.T) {
	rec := &persistRecorder{}
	p := newTestPool(t, 1, 2, func(c PoolConfig) PoolConfig { c.OnParkPersist = rec.sink; return c })
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
	ban := &BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"}
	first.pa.am.mu.Lock()
	first.pa.am.markBan(ban)
	first.pa.am.mu.Unlock()
	first.NoteError(ban)
	for _, delayed := range []*BizError{{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(48 * time.Hour)}, {BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}} {
		late.pa.am.mu.Lock()
		late.pa.am.markBan(delayed)
		late.pa.am.mu.Unlock()
		late.NoteError(delayed)
	}
	if got := p.Snapshot()[0]; got.ParkKind != "banned" || !got.ParkUntil.IsZero() {
		t.Fatalf("late response downgraded ban: %+v", got)
	}
	if records := rec.all(); len(records) != 1 || records[0].Kind != BanBanned {
		t.Fatalf("durable ban overwritten: %+v", records)
	}
}

func TestParkDeadlineMonotonicAcrossKinds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, late *BizError
		want        BanState
	}{
		{"mute then risk", &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(48 * time.Hour)}, &BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}, BanMuted},
		{"risk then shorter mute", &BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}, &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: time.Now().Add(time.Minute)}, BanRiskDevice},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &persistRecorder{}
			p := newTestPool(t, 1, 2, func(c PoolConfig) PoolConfig { c.OnParkPersist = rec.sink; c.RiskCooldown = 24 * time.Hour; return c })
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
			first.NoteError(tc.first)
			before := p.Snapshot()[0]
			late.pa.am.mu.Lock()
			late.pa.am.markBan(tc.late)
			afterManagerKind, afterManagerUntil := late.pa.am.ban, late.pa.am.parkUntil
			late.pa.am.mu.Unlock()
			if afterManagerKind != tc.want || !afterManagerUntil.Equal(before.ParkUntil) {
				t.Fatalf("manager shortened park: kind=%v until=%v, before=%+v", afterManagerKind, afterManagerUntil, before)
			}
			late.NoteError(tc.late)
			after := p.Snapshot()[0]
			if after.ParkKind != before.ParkKind || !after.ParkUntil.Equal(before.ParkUntil) {
				t.Fatalf("lease shortened park: before=%+v after=%+v", before, after)
			}
			if records := rec.all(); len(records) != 1 {
				t.Fatalf("shorter park persisted: %+v", records)
			}
		})
	}
}

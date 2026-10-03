package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Two rows may weight a physical identity, but cannot silently choose one
// row's credentials or wire fingerprint for the shared client.
func TestPoolRejectsConflictingDuplicateIdentityConfiguration(t *testing.T) {
	first := Account{Mobile: "13800000000", Password: "original", DeviceID: "device-original"}
	for _, tc := range []struct {
		name   string
		second Account
	}{
		{name: "password", second: Account{Mobile: first.Mobile, Password: "different", DeviceID: first.DeviceID}},
		{name: "device ID", second: Account{Mobile: first.Mobile, Password: first.Password, DeviceID: "device-different"}},
		{name: "channel", second: Account{Mobile: first.Mobile, Password: first.Password, DeviceID: first.DeviceID, Channel: "web"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("NewPool", func(t *testing.T) {
				if p, err := NewPool([]Account{first, tc.second}, PoolConfig{}); err == nil {
					t.Fatalf("NewPool accepted conflicting %s for identity %s (ring size %d)", tc.name, first.Identity(), len(p.Snapshot()))
				}
			})
			t.Run("AddAccount", func(t *testing.T) {
				p, err := NewPool([]Account{first}, PoolConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.AddAccount(tc.second); err == nil {
					t.Errorf("AddAccount accepted conflicting %s for identity %s", tc.name, first.Identity())
				}
				if got := len(p.Snapshot()); got != 1 {
					t.Errorf("rejected add changed ring size: got %d, want 1", got)
				}
			})
		})
	}
}

// Both leases predate the ban: a delayed response from the second request
// must not downgrade the permanent state or the persisted ban record.
func TestPoolLateRiskOrMuteCannotDowngradeBan(t *testing.T) {
	for _, tc := range []struct {
		name string
		late *BizError
	}{
		{name: "mute", late: &BizError{BizCode: 5, BizMsg: "user is muted", MuteUntil: time.Now().Add(time.Hour)}},
		{name: "risk", late: &BizError{BizCode: 11, BizMsg: "RISK_DEVICE_DETECTED"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var records []ParkRecord
			p, err := NewPool([]Account{{Mobile: "13800000000", Password: "pw"}}, PoolConfig{
				MaxInflight:   2,
				OnParkPersist: func(rec ParkRecord) { records = append(records, rec) },
			})
			if err != nil {
				t.Fatal(err)
			}
			first, err := p.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			second, err := p.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			first.NoteError(&BizError{BizCode: 10, BizMsg: "USER_IS_BANNED"})
			if len(records) != 1 || records[0].Kind != BanBanned {
				t.Fatalf("initial ban persistence = %+v", records)
			}
			second.NoteError(tc.late)
			rows := p.Snapshot()
			if len(rows) != 1 || rows[0].State != "banned" || rows[0].ParkKind != "banned" || !rows[0].ParkUntil.IsZero() {
				t.Fatalf("late %s downgraded permanent ban: %+v", tc.name, rows)
			}
			if len(records) != 1 || records[0].Kind != BanBanned {
				t.Fatalf("late %s overwrote persisted ban: %+v", tc.name, records)
			}
		})
	}
}

// A login started on an old lease can return after removal and re-addition.
// Its callback must not write an obsolete token over the new generation.
func TestPoolLateLoginCallbackCannotPersistAfterRemoveAndReAdd(t *testing.T) {
	const id = "13800000000"
	entered := make(chan struct{})
	releaseOld := make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(releaseOld) }) }
	defer unblock() // unblock server even on an assertion failure
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/users/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		ordinal := requests.Add(1)
		if ordinal == 1 {
			close(entered)
			select {
			case <-releaseOld:
			case <-r.Context().Done():
				return
			}
		}
		fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"token-%d"}}}}`, ordinal)
	}))
	defer srv.Close()
	var mu sync.Mutex
	var persisted []LoginRecord
	p, err := NewPool([]Account{{Mobile: id, Password: "pw"}}, PoolConfig{
		BaseURL: srv.URL, MaxInflight: 1,
		OnLoginPersist: func(rec LoginRecord) {
			mu.Lock()
			persisted = append(persisted, rec)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	old, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		token string
		err   error
	}
	oldResult := make(chan result, 1)
	go func() { token, err := old.Token(ctx); oldResult <- result{token, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("old login never reached local server: %v", ctx.Err())
	}
	if !p.RemoveAccount(id) {
		t.Fatal("old identity was not removed")
	}
	if err := p.AddAccount(Account{Mobile: id, Password: "pw"}); err != nil {
		t.Fatal(err)
	}
	fresh, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if token, err := fresh.Token(ctx); err != nil || token != "token-2" {
		t.Fatalf("new generation login = %q, %v; want token-2", token, err)
	}
	unblock()
	select {
	case got := <-oldResult:
		if got.err != nil || got.token != "token-1" {
			t.Fatalf("old in-flight login = %+v; want token-1", got)
		}
	case <-ctx.Done():
		t.Fatalf("old login never completed: %v", ctx.Err())
	}
	mu.Lock()
	records := append([]LoginRecord(nil), persisted...)
	mu.Unlock()
	if len(records) != 1 || records[0] != (LoginRecord{Identity: id, Token: "token-2"}) {
		t.Fatalf("stale login overwrote new generation's persisted token: %+v", records)
	}
}

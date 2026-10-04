package upstream

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestMuteGraceBothEntrancesAndLateResponse(t *testing.T) {
	until := time.Now().Add(25 * time.Minute).Truncate(time.Second)
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
	be := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: until}
	first.pa.am.mu.Lock()
	first.pa.am.markBan(be)
	first.pa.am.mu.Unlock()
	first.NoteError(be)
	first.NoteError(be)
	want := until.Add(time.Hour)
	if got := p.Snapshot()[0].ParkUntil; !got.Equal(want) {
		t.Fatalf("park=%s want=%s", got, want)
	}
	if len(records) != 1 || !records[0].Until.Equal(want) {
		t.Fatalf("persist=%+v want=%s", records, want)
	}
	short := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: until.Add(-time.Minute)}
	late.pa.am.mu.Lock()
	late.pa.am.markBan(short)
	late.pa.am.mu.Unlock()
	late.NoteError(short)
	if len(records) != 1 || !p.Snapshot()[0].ParkUntil.Equal(want) {
		t.Fatalf("late shortened deadline: records=%+v snapshot=%+v", records, p.Snapshot())
	}
}

func TestMuteGraceFromEnvelopeOnEveryEndpoint(t *testing.T) {
	for _, endpoint := range []string{"login", "session", "completion"} {
		for _, status := range []int{200, 429} {
			t.Run(endpoint+"/"+http.StatusText(status), func(t *testing.T) {
				until := time.Now().Add(25 * time.Minute).Truncate(time.Second)
				routes := map[string]func(http.ResponseWriter, *http.Request){
					"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
						if endpoint == "login" {
							w.WriteHeader(status)
							writeEnvelope(w, 5, "muted", map[string]any{"mute_until": until.Unix()})
						} else {
							writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok"}})
						}
					},
					"/api/v0/chat_session/create": func(w http.ResponseWriter, r *http.Request) {
						if endpoint == "session" {
							w.WriteHeader(status)
							writeEnvelope(w, 5, "muted", map[string]any{"mute_until": until.Unix()})
						} else {
							writeEnvelope(w, 0, "", map[string]any{"chat_session": map[string]any{"id": "s"}})
						}
					},
					"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
						writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
					},
					"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(status)
						writeEnvelope(w, 5, "muted", map[string]any{"mute_until": until.Unix()})
					},
				}
				m := newMock(routes)
				defer m.srv.Close()
				var recs []ParkRecord
				p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{BaseURL: m.srv.URL, QueueWait: time.Millisecond, OnParkPersist: func(r ParkRecord) { recs = append(recs, r) }})
				if err != nil {
					t.Fatal(err)
				}
				l, err := p.Acquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if endpoint == "login" {
					_, err = l.CreateSession(context.Background())
				} else if endpoint == "session" {
					_, err = l.CreateSession(context.Background())
				} else {
					_, err = l.Completion(context.Background(), CompletionRequest{SessionID: "s", Prompt: "hi"})
				}
				var be *BizError
				if !errors.As(err, &be) || !be.MuteUntil.Equal(until) {
					t.Fatalf("error=%v want mute %s", err, until)
				}
				l.NoteError(err)
				l.Release()
				want := until.Add(time.Hour)
				if len(recs) != 1 || !recs[0].Until.Equal(want) || !p.Snapshot()[0].ParkUntil.Equal(want) {
					t.Fatalf("park=%+v snapshot=%+v want=%s", recs, p.Snapshot(), want)
				}
				if _, err := p.Acquire(context.Background()); !errors.Is(err, ErrPoolBusy) {
					t.Fatalf("muted identity selectable: %v", err)
				}
			})
		}
	}
}

func TestMuteFallbackBothEntrancesDoNotExtend(t *testing.T) {
	var records []ParkRecord
	p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{QueueWait: time.Millisecond, OnParkPersist: func(r ParkRecord) { records = append(records, r) }})
	if err != nil {
		t.Fatal(err)
	}
	l, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	be := &BizError{BizCode: 5, BizMsg: "muted"}
	l.pa.am.mu.Lock()
	l.pa.am.markBan(be)
	want := l.pa.am.parkUntil
	l.pa.am.mu.Unlock()
	time.Sleep(time.Millisecond)
	l.NoteError(be)
	if len(records) != 1 || !records[0].Until.Equal(want) || !p.Snapshot()[0].ParkUntil.Equal(want) {
		t.Fatalf("duplicate fallback extended park: records=%+v want=%s got=%s", records, want, p.Snapshot()[0].ParkUntil)
	}
}

func TestMuteGraceFallbackInvalidOrExpired(t *testing.T) {
	for _, raw := range []string{"null", "\"bad\"", "0"} {
		t.Run(raw, func(t *testing.T) {
			be := &BizError{BizCode: 5, BizMsg: "muted", MuteUntil: parseMuteUntil([]byte(`{"mute_until":` + raw + `}`))}
			p, err := NewPool([]Account{{Mobile: "13800000000", Password: "fixture"}}, PoolConfig{QueueWait: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			l, err := p.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			l.NoteError(be)
			l.Release()
			got := p.Snapshot()[0].ParkUntil
			if got.Before(before.Add(7*24*time.Hour-time.Second)) || got.After(before.Add(7*24*time.Hour+time.Second)) {
				t.Fatalf("fallback=%s", got)
			}
		})
	}
}

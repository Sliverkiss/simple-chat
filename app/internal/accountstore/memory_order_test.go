package accountstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/upstream"
)

type gatedMemFake struct {
	*memFake
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedMemFake) SaveAccount(ctx context.Context, acct upstream.Account) error {
	f.once.Do(func() { close(f.started); <-f.release })
	return f.memFake.SaveAccount(ctx, acct)
}

func TestMemoryFirstInflightTransitionPrecedesDeleteAndReadd(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transition func(*MemoryFirstStore)
	}{
		{"login", func(s *MemoryFirstStore) { s.ApplyLogin(upstream.LoginRecord{Identity: "100", Token: "old-token"}) }},
		{"park", func(s *MemoryFirstStore) {
			s.ApplyPark(upstream.ParkRecord{Mobile: "100", Kind: upstream.BanMuted, Until: time.Now().Add(time.Hour)})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = "100"
			backing := &gatedMemFake{memFake: newMemFake(upstream.Account{Mobile: id, Password: "old"}), started: make(chan struct{}), release: make(chan struct{})}
			store, err := NewMemoryFirstStore(context.Background(), backing, nil)
			if err != nil {
				t.Fatal(err)
			}
			transitionDone := make(chan struct{})
			go func() { tc.transition(store); close(transitionDone) }()
			select {
			case <-backing.started:
			case <-time.After(time.Second):
				t.Fatal("old write did not start")
			}
			deleteDone := make(chan error, 1)
			go func() { deleteDone <- store.DeleteAccount(context.Background(), id) }()
			select {
			case err := <-deleteDone:
				t.Fatalf("delete overtook old backing write: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(backing.release)
			select {
			case <-transitionDone:
			case <-time.After(time.Second):
				t.Fatal("old write stuck")
			}
			select {
			case err := <-deleteDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("delete stuck")
			}
			fresh := upstream.Account{Mobile: id, Password: "new"}
			if err := store.SaveAccount(context.Background(), fresh); err != nil {
				t.Fatal(err)
			}
			for _, target := range []Store{store, backing} {
				rows, err := target.Load(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 1 || rows[0] != fresh {
					t.Fatalf("stale transition survived replacement: %+v", rows)
				}
			}
		})
	}
}

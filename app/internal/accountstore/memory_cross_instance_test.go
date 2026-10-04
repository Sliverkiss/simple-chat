package accountstore

import (
 "context"
 "testing"
 "time"

 "simple-chat/internal/upstream"
)

// Two separately booted wrappers must not overwrite each other's park state
// with a stale full record, or recreate a deleted identity on Redis.
func TestMemoryFirstCrossInstanceStaleWrites(t *testing.T) {
 ctx := context.Background()
 redisA, fake, _ := redisTestEnv(t, []upstream.Account{{Mobile: "100", Password: "pw"}})
 redisB, err := OpenRedis("rediss://:sekrit@"+fake.ln.Addr().String(), nil)
 if err != nil { t.Fatal(err) }
 defer redisB.Close()
 a, err := NewMemoryFirstStore(ctx, redisA, nil)
 if err != nil { t.Fatal(err) }
 b, err := NewMemoryFirstStore(ctx, redisB, nil)
 if err != nil { t.Fatal(err) }
 until := time.Now().Add(time.Hour)
 a.ApplyPark(upstream.ParkRecord{Mobile:"100", Kind:upstream.BanMuted, Until:until})
 b.ApplyLogin(upstream.LoginRecord{Identity:"100", Token:"token"})
 got, err := redisA.Load(ctx)
 if err != nil { t.Fatal(err) }
 if len(got)!=1 || got[0].ParkKind!="muted" { t.Fatalf("stale login cleared remote mute: %+v", got) }
 if err:=a.DeleteAccount(ctx,"100"); err!=nil { t.Fatal(err) }
 b.ApplyPark(upstream.ParkRecord{Mobile:"100", Kind:upstream.BanBanned})
 got, err = redisA.Load(ctx)
 if err != nil { t.Fatal(err) }
 if len(got)!=0 { t.Fatalf("stale park resurrected removed identity: %+v", got) }
}

func TestMemoryFirstCrossInstanceReaddDoesNotAcceptOldPark(t *testing.T) {
 ctx:=context.Background()
 redisA, fake, _ := redisTestEnv(t, []upstream.Account{{Mobile:"100", Password:"pw"}})
 redisB, err:=OpenRedis("rediss://:sekrit@"+fake.ln.Addr().String(), nil)
 if err!=nil { t.Fatal(err) }; defer redisB.Close()
 a, err:=NewMemoryFirstStore(ctx, redisA,nil); if err!=nil { t.Fatal(err) }
 b, err:=NewMemoryFirstStore(ctx, redisB,nil); if err!=nil { t.Fatal(err) }
 if err:=a.DeleteAccount(ctx,"100"); err!=nil { t.Fatal(err) }
 if err:=a.SaveAccount(ctx, upstream.Account{Mobile:"100", Password:"pw"}); err!=nil { t.Fatal(err) }
 b.ApplyPark(upstream.ParkRecord{Mobile:"100", Kind:upstream.BanBanned})
 got,err:=redisA.Load(ctx); if err!=nil { t.Fatal(err) }
 if len(got)!=1 || got[0].ParkKind!="" { t.Fatalf("old incarnation parked re-added account: %+v",got) }
}

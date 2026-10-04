package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// A local RESP peer accepts the park SET but withholds its reply. This
// exercises the actual RedisStore -> memory-first -> pool/HTTP wiring.
func TestOP05StalledRedisParkClosesAdmission(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	parkSeen := make(chan struct{}, 1)
	var mu sync.Mutex
	var conns []net.Conn
	row, _ := json.Marshal(upstream.Account{Mobile: "100", Password: "fictional"})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					cmd, err := readStallCommand(r)
					if err != nil {
						return
					}
					switch cmd[0] {
					case "PING":
						_, err = io.WriteString(conn, "+PONG\r\n")
					case "SMEMBERS":
						_, err = io.WriteString(conn, "*1\r\n$3\r\n100\r\n")
					case "GET":
						_, err = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(row), row)
					case "SET":
						parkSeen <- struct{}{}
						// No response, even though Redis may already have applied SET.
						_, err = r.ReadByte() // client drops ambiguous connection
					default:
						return
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		for _, conn := range conns {
			conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	redis, err := accountstore.OpenRedis("redis://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer redis.Close()
	store, err := accountstore.NewMemoryFirstStore(context.Background(), redis, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewServer(Config{Accounts: rows, ParkStore: store})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Shutdown()
	lease, err := gateway.pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		lease.NoteError(&upstream.BizError{BizCode: 5})
		lease.Release()
		close(done)
	}()
	select {
	case <-parkSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("park SET never reached Redis")
	}
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("park callback blocked without bound")
	}
	if !store.ParkWriteFailed() {
		t.Fatal("ambiguous park SET reply must latch failure")
	}
	// Make a healthy-looking account available; the latch must still win.
	if !gateway.pool.RemoveAccount("100") {
		t.Fatal("expected account")
	}
	if err := gateway.pool.AddAccount(upstream.Account{Mobile: "101", Password: "fictional"}); err != nil {
		t.Fatal(err)
	}
	if fresh, err := gateway.pool.Acquire(context.Background()); err == nil {
		fresh.Release()
		t.Fatal("new lease admitted after uncertain park write")
	}
	gw := httptest.NewServer(gateway.Handler())
	defer gw.Close()
	for _, path := range []string{"/v1/chat/completions", "/v1/web_search"} {
		body := `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`
		if path == "/v1/web_search" {
			body = `{"query":"hi"}`
		}
		resp, err := gw.Client().Post(gw.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s: HTTP %d, want 503", path, resp.StatusCode)
		}
	}
	resp, err := gw.Client().Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz: HTTP %d, want 503", resp.StatusCode)
	}
}

func readStallCommand(r *bufio.Reader) ([]string, error) {
	var n int
	if _, err := fmt.Fscanf(r, "*%d\r\n", &n); err != nil {
		return nil, err
	}
	cmd := make([]string, n)
	for i := range cmd {
		var size int
		if _, err := fmt.Fscanf(r, "$%d\r\n", &size); err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		cmd[i] = string(buf[:size])
	}
	return cmd, nil
}

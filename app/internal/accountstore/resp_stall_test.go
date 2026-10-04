package accountstore

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"
)

// A command may have executed even if its reply is lost. Never replay SET.
func TestRESPStalledWriteReplyTimesOutWithoutReplay(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := &respConn{conn: client, r: bufio.NewReader(client), w: bufio.NewWriter(client)}
	defer c.closeForTest()
	seen := make(chan []string, 1)
	go func() {
		cmd, err := readCommand(bufio.NewReader(server))
		if err == nil {
			seen <- cmd
		}
		// A non-answering Redis still accepts the command; wait until the
		// client discards the ambiguous connection.
		var b [1]byte
		server.Read(b[:])
	}()
	result := make(chan error, 1)
	go func() { _, err := c.do("SET", "park", "muted"); result <- err }()
	select {
	case cmd := <-seen:
		if len(cmd) != 3 || cmd[0] != "SET" {
			t.Fatalf("unexpected command: %q", cmd)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("command was not sent")
	}
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, net.ErrClosed) && !isTimeout(err) {
			t.Fatalf("expected connection failure/timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		server.Close() // unblock the peer before taking the client's mutex
		<-result
		t.Fatal("SET waited without bound for Redis reply")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		t.Fatal("uncertain write kept a reusable connection")
	}
}

func (c *respConn) closeForTest() { c.mu.Lock(); c.closeLocked(); c.mu.Unlock() }
func isTimeout(err error) bool {
	var n net.Error
	return errors.As(err, &n) && n.Timeout()
}

// The peer accepts a connection but stops consuming command bytes. The
// command must not remain in bufio.Flush forever either.
func TestRESPStalledCommandWriteTimesOut(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := &respConn{conn: client, r: bufio.NewReader(client), w: bufio.NewWriter(client)}
	defer c.closeForTest()
	result := make(chan error, 1)
	go func() {
		_, err := c.do("SET", "park", string(make([]byte, 8192)))
		result <- err
	}()
	select {
	case err := <-result:
		if !isTimeout(err) {
			t.Fatalf("expected bounded write timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		server.Close()
		<-result
		t.Fatal("command write blocked without bound")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		t.Fatal("timed-out write kept a reusable connection")
	}
}

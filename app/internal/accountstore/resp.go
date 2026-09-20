package accountstore

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// respConn is a minimal RESP (RESP2) client: one TCP (optionally TLS)
// connection, AUTH, and the handful of commands the store needs. It is the
// whole reason the Redis store exists without a dependency: the binary keeps
// its stdlib-only build (spec: "stdlib only. YAGNI is law") while hosted
// deployments get a managed-Redis account store.
//
// Supported commands: AUTH, PING, QUIT, GET, SET, DEL, HSET (reserved for
// the key schema's future), SADD, SREM, SMEMBERS. No pipelining: each
// command is written and its reply read in order, under the connection
// mutex.
type respConn struct {
	addr     string
	username string
	password string
	useTLS   bool
	tlsCfg   *tls.Config

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// errClosed marks a broken connection so the next command re-dials.
var errClosed = errors.New("redis: connection closed")

// parseRedisURL parses redis:// and rediss:// connection strings into dial
// parameters. A password in the URL is the AUTH password (a "user:pass"
// form sends AUTH <user> <pass>; user "default" degrades to AUTH <pass>).
func parseRedisURL(raw string) (addr, password, username string, useTLS bool, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", false, fmt.Errorf("parse redis url: %w", err)
	}
	switch u.Scheme {
	case "rediss":
		useTLS = true
	case "redis":
		useTLS = false
	default:
		return "", "", "", false, fmt.Errorf("redis url scheme must be redis:// or rediss://, got %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return "", "", "", false, errors.New("redis url has no host")
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	return net.JoinHostPort(host, port), password, username, useTLS, nil
}

// newRespConn builds a lazy client; nothing is dialed until the first
// command.
func newRespConn(rawURL string, tlsCfg *tls.Config) (*respConn, error) {
	addr, password, username, useTLS, err := parseRedisURL(rawURL)
	if err != nil {
		return nil, err
	}
	if username == "default" {
		username = "" // default user: plain AUTH <password>
	}
	return &respConn{
		addr:     addr,
		username: username,
		password: password,
		useTLS:   useTLS,
		tlsCfg:   tlsCfg,
	}, nil
}

// do writes one command and reads one reply, dialing (and authenticating)
// if needed. Safe for concurrent use.
func (c *respConn) do(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		if err := c.dialLocked(); err != nil {
			return nil, err
		}
	}
	reply, err := c.roundTripLocked(args)
	if errors.Is(err, errClosed) {
		// One re-dial attempt per command: a transient broken pipe heals on
		// the next call instead of bricking the store.
		if err := c.dialLocked(); err != nil {
			return nil, err
		}
		return c.roundTripLocked(args)
	}
	return reply, err
}

// dialLocked establishes (or re-establishes) the connection and runs the
// AUTH/PING handshake. Caller holds c.mu.
func (c *respConn) dialLocked() error {
	c.closeLocked()
	d := &net.Dialer{Timeout: 5 * time.Second}
	var conn net.Conn
	var err error
	if c.useTLS {
		conn, err = tls.DialWithDialer(d, "tcp", c.addr, c.tlsCfg)
	} else {
		conn, err = d.Dial("tcp", c.addr)
	}
	if err != nil {
		return fmt.Errorf("redis: dial %s: %w", c.addr, err)
	}
	c.conn = conn
	c.r = bufio.NewReader(conn)
	c.w = bufio.NewWriter(conn)
	if c.password != "" {
		authArgs := []string{"AUTH", c.password}
		if c.username != "" {
			authArgs = []string{"AUTH", c.username, c.password}
		}
		if _, err := c.roundTripLocked(authArgs); err != nil {
			c.closeLocked()
			return fmt.Errorf("redis: auth against %s: %w", c.addr, err)
		}
	}
	if _, err := c.roundTripLocked([]string{"PING"}); err != nil {
		c.closeLocked()
		return fmt.Errorf("redis: ping %s: %w", c.addr, err)
	}
	return nil
}

// doPing forces the lazy connection up with one PING round trip so Open
// fails fast on a dead/unauthenticated Redis.
func (c *respConn) doPing() error {
	if _, err := c.do("PING"); err != nil {
		return err
	}
	return nil
}

// roundTripLocked writes args as one RESP array and reads one reply.
// Caller holds c.mu.
func (c *respConn) roundTripLocked(args []string) (any, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.w.WriteString(b.String()); err != nil {
		return nil, errClosed
	}
	if err := c.w.Flush(); err != nil {
		return nil, errClosed
	}
	return readReply(c.r)
}

// closeLocked tears down the connection. Caller holds c.mu.
func (c *respConn) closeLocked() {
	if c.conn != nil {
		c.conn.Close() // best-effort; a failed close just drops the socket
		c.conn = nil
		c.r, c.w = nil, nil
	}
}

// Close closes the connection.
func (c *respConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	c.roundTripLocked([]string{"QUIT"}) // best-effort
	c.closeLocked()
	return nil
}

// readReply parses one RESP2 reply: simple string (+), error (-), integer
// (:), bulk string ($), array (*). Nil bulk ($-1) and nil array (*-1)
// decode to nil, nil error — GET on a missing key.
func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, errClosed
	}
	if len(line) < 1 {
		return nil, errors.New("redis: empty reply line")
	}
	typ, rest := line[0], line[1:]
	switch typ {
	case '+':
		return string(rest), nil
	case '-':
		return nil, errors.New("redis: " + string(rest))
	case ':':
		n, err := strconv.ParseInt(string(rest), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis: bad integer reply %q: %w", line, err)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(string(rest))
		if err != nil {
			return nil, fmt.Errorf("redis: bad bulk length %q", line)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2) // payload + \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, errClosed
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(string(rest))
		if err != nil {
			return nil, fmt.Errorf("redis: bad array length %q", line)
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			item, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("redis: unknown reply type %q", line)
	}
}

// readLine reads one \r\n-terminated line without the terminator.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(line, []byte("\r\n")), nil
}

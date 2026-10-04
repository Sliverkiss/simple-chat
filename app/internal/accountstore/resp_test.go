package accountstore

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// testRootCAs trusts the in-test server's self-signed cert (see
// selfSignedCert): each cert minted gets added to the package-level pool
// that OpenRedis picks up.
var testRootCAs = x509.NewCertPool()

// fakeRedis is an in-test RESP2 server: enough command surface to exercise
// the store (PING, AUTH, GET, SET, DEL, SADD, SREM, SMEMBERS, QUIT) over a
// plain TCP or TLS listener. It exists because httptest cannot speak Redis
// and the task forbids live-provider testing; a real redis-server in docker
// covers the same surface in the smoke stage.
type fakeRedis struct {
	t          *testing.T
	ln         net.Listener
	password   string
	requireTLS bool

	mu    sync.Mutex
	data  map[string]string                   // GET/SET keys
	sets  map[string]map[string]bool          // SET members
	conds map[string]func(cmd []string) error // per-command fault injection
	conns map[net.Conn]bool                   // live client connections (for kill)
}

func newFakeRedis(t *testing.T, password string) *fakeRedis {
	return &fakeRedis{
		t:        t,
		password: password,
		data:     map[string]string{},
		sets:     map[string]map[string]bool{},
		conds:    map[string]func(cmd []string) error{},
		conns:    map[net.Conn]bool{},
	}
}

// listen starts the server on a loopback port; returns the URL.
func (f *fakeRedis) listen(t *testing.T, useTLS bool) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if useTLS {
		cert, err := selfSignedCert(t)
		if err != nil {
			t.Fatal(err)
		}
		if l, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
			cert.Leaf = l
			testRootCAs.AddCert(l)
		}
		redisRootCAs = testRootCAs
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
	}
	f.ln = ln
	go f.serve()
	scheme := "redis"
	if useTLS {
		scheme = "rediss"
	}
	return fmt.Sprintf("%s://:%s@%s", scheme, f.password, ln.Addr().String())
}

// listenInsecureURL returns the URL with no password.
func (f *fakeRedis) listenPlain(t *testing.T, useTLS bool) string {
	f.listen(t, useTLS)
	scheme := "redis"
	if useTLS {
		scheme = "rediss"
	}
	return scheme + "://" + f.ln.Addr().String()
}

// fault installs a per-command error injector (name is the command verb).
func (f *fakeRedis) fault(cmd string, fn func(cmd []string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conds[cmd] = fn
}

// kill simulates a Redis outage: stops accepting AND tears down every live
// client connection (a listener Close alone leaves accepted conns serving).
func (f *fakeRedis) kill() {
	f.ln.Close()
	f.mu.Lock()
	for c := range f.conns {
		c.Close()
	}
	f.mu.Unlock()
}

func (f *fakeRedis) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	f.mu.Lock()
	f.conns[conn] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.conns, conn)
		f.mu.Unlock()
		conn.Close()
	}()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	authenticated := f.password == ""
	for {
		cmd, err := readCommand(r)
		if err != nil {
			return
		}
		if len(cmd) == 0 {
			continue
		}
		verb := strings.ToUpper(cmd[0])
		f.mu.Lock()
		if cond := f.conds[verb]; cond != nil {
			if err := cond(cmd); err != nil {
				f.mu.Unlock()
				fmt.Fprintf(w, "-%s\r\n", err.Error())
				w.Flush()
				return
			}
		}
		f.mu.Unlock()
		if !authenticated && verb != "AUTH" && verb != "QUIT" {
			fmt.Fprint(w, "-NOAUTH Authentication required.\r\n")
			w.Flush()
			continue
		}
		switch verb {
		case "AUTH":
			if len(cmd) >= 2 && cmd[len(cmd)-1] == f.password {
				authenticated = true
				fmt.Fprint(w, "+OK\r\n")
			} else {
				fmt.Fprint(w, "-ERR invalid password\r\n")
			}
		case "PING":
			fmt.Fprint(w, "+PONG\r\n")
		case "QUIT":
			fmt.Fprint(w, "+OK\r\n")
			w.Flush()
			return
		case "SET":
			if len(cmd) != 3 {
				fmt.Fprint(w, "-ERR wrong number of arguments for SET\r\n")
				break
			}
			f.mu.Lock()
			f.data[cmd[1]] = cmd[2]
			f.mu.Unlock()
			fmt.Fprint(w, "+OK\r\n")
		case "GET":
			if len(cmd) != 2 {
				fmt.Fprint(w, "-ERR wrong number of arguments for GET\r\n")
				break
			}
			f.mu.Lock()
			v, ok := f.data[cmd[1]]
			f.mu.Unlock()
			if !ok {
				fmt.Fprint(w, "$-1\r\n")
			} else {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v), v)
			}
		case "DEL":
			f.mu.Lock()
			delete(f.data, cmd[1])
			f.mu.Unlock()
			fmt.Fprint(w, ":1\r\n")
		case "SADD":
			if len(cmd) != 3 {
				fmt.Fprint(w, "-ERR wrong number of arguments for SADD\r\n")
				break
			}
			f.mu.Lock()
			if f.sets[cmd[1]] == nil {
				f.sets[cmd[1]] = map[string]bool{}
			}
			if f.sets[cmd[1]][cmd[2]] {
				f.mu.Unlock()
				fmt.Fprint(w, ":0\r\n")
			} else {
				f.sets[cmd[1]][cmd[2]] = true
				f.mu.Unlock()
				fmt.Fprint(w, ":1\r\n")
			}
		case "SREM":
			f.mu.Lock()
			if f.sets[cmd[1]] != nil && f.sets[cmd[1]][cmd[2]] {
				delete(f.sets[cmd[1]], cmd[2])
				f.mu.Unlock()
				fmt.Fprint(w, ":1\r\n")
			} else {
				f.mu.Unlock()
				fmt.Fprint(w, ":0\r\n")
			}
		case "SMEMBERS":
			f.mu.Lock()
			members := f.sets[cmd[1]]
			fmt.Fprintf(w, "*%d\r\n", len(members))
			for m := range members {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(m), m)
			}
			f.mu.Unlock()
		default:
			fmt.Fprintf(w, "-ERR unknown command '%s'\r\n", verb)
		}
		w.Flush()
	}
}

// readCommand parses one RESP array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("bad command line %q", line)
	}
	var n int
	if _, err := fmt.Sscanf(line, "*%d", &n); err != nil {
		return nil, err
	}
	cmd := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		var l int
		if _, err := fmt.Sscanf(hdr, "$%d", &l); err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		cmd = append(cmd, string(buf[:l]))
	}
	return cmd, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// selfSignedCert mints a throwaway in-memory TLS cert for the loopback
// listener (SANs: localhost, 127.0.0.1) — no filesystem, no external tool.
func selfSignedCert(t *testing.T) (tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, nil
}

package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"
)

// uuidPattern accepts the canonical 8-4-4-4-12 hex layout.
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestResolveDeviceIDUsesExplicitValueVerbatim(t *testing.T) {
	a := Account{Mobile: "13800000000", Password: "pw", DeviceID: "harvested_smsdk_value"}
	if got := ResolveDeviceID(a); got != "harvested_smsdk_value" {
		t.Errorf("ResolveDeviceID = %q, want explicit value verbatim", got)
	}
}

func TestResolveDeviceIDGeneratesAppFormatForMissing(t *testing.T) {
	// Contract changed (apk-alignment.md C3): the mint is the app's
	// AES-CBC Base64 format, never a UUID.
	a := Account{Mobile: "13800000000", Password: "pw"}
	got := ResolveDeviceID(a)
	if uuidPattern.MatchString(got) {
		t.Errorf("ResolveDeviceID = %q, UUIDs are a device-fingerprint tell", got)
	}
	if got == "" || got == "simple_chat_client" {
		t.Errorf("ResolveDeviceID = %q, want app-format mint", got)
	}
}

func TestResolveDeviceIDStableAcrossCalls(t *testing.T) {
	a := Account{Mobile: "13800000009", Password: "pw"}
	first := ResolveDeviceID(a)
	for i := 0; i < 3; i++ {
		if got := ResolveDeviceID(a); got != first {
			t.Fatalf("ResolveDeviceID call %d = %q, want stable %q", i, got, first)
		}
	}
}

func TestResolveDeviceIDDifferentAccountsDiffer(t *testing.T) {
	a := Account{Mobile: "13800000009", Password: "pw"}
	b := Account{Mobile: "13800000007", Password: "pw"}
	if ResolveDeviceID(a) == ResolveDeviceID(b) {
		t.Error("two different accounts resolved to the same device id")
	}
}

func TestResolveDeviceIDEmailIdentity(t *testing.T) {
	a := Account{Email: "one@example.com", Password: "pw"}
	b := Account{Email: "two@example.com", Password: "pw"}
	if ResolveDeviceID(a) == "" || ResolveDeviceID(b) == "" {
		t.Fatal("email accounts must resolve a device id")
	}
	if ResolveDeviceID(a) == ResolveDeviceID(b) {
		t.Error("two different email accounts resolved to the same device id")
	}
}

// TestPoolLoginCarriesPerAccountDeviceID verifies the login wire payload:
// each pool account presents its own resolved device id (never the static
// default, never shared between accounts).
func TestPoolLoginCarriesPerAccountDeviceID(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{} // mobile -> device_id

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("bad login body: %v", err)
		}
		mu.Lock()
		seen[fmt.Sprint(body["mobile"])] = fmt.Sprint(body["device_id"])
		mu.Unlock()
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"user":{"token":"tok"}}}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	accounts := []Account{
		{Mobile: "13800000009", Password: "pw"},
		{Mobile: "13800000007", Password: "pw"},
	}
	pool, err := NewPool(accounts, PoolConfig{BaseURL: srv.URL, MaxInflight: 1, QueueWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for range accounts {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lease.Token(context.Background()); err != nil {
			t.Fatal(err)
		}
		lease.Release()
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("login seen for %d accounts, want 2 (%v)", len(seen), seen)
	}
	for _, acc := range accounts {
		got, ok := seen[acc.Mobile]
		if !ok {
			t.Fatalf("no login recorded for %s", acc.Mobile)
		}
		if got == "" || got == "simple_chat_client" || got == "deepseek_to_api" {
			t.Errorf("account %s sent device_id %q, want its own generated id", acc.Mobile, got)
		}
		if want := ResolveDeviceID(acc); got != want {
			t.Errorf("account %s sent device_id %q, want %q", acc.Mobile, got, want)
		}
	}
	if seen["13800000009"] == seen["13800000007"] {
		t.Error("both accounts shared one device id on the wire")
	}
}

package upstream

// Wire alignment — per-account web channel (web-reverse-research.md §6).
//
// Evidence: NIyueeE/ds-free-api's field-proven hybrid is android-shaped wire
// headers + login body {os:"web", device_id:<browser-harvested Shumei id>};
// our 2026-09-20 live probe confirms android headers pass the AWS WAF from
// plain Go TLS. The browser sends its own UUID in x-device-id (distinct from
// the Shumei id in the body), so the web channel mints a UUID there too.

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestWebChannelLoginWire(t *testing.T) {
	var mu sync.Mutex
	var body map[string]any
	var headerDeviceID, headerPlatform string

	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			var b map[string]any
			json.NewDecoder(r.Body).Decode(&b)
			mu.Lock()
			body = b
			headerDeviceID = r.Header.Get("x-device-id")
			headerPlatform = r.Header.Get("x-client-platform")
			mu.Unlock()
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok-web"}})
		},
	})
	defer m.srv.Close()

	c := NewClient(Config{
		BaseURL:  m.srv.URL,
		Email:    "user@example.com",
		Password: "pw",
		Channel:  "web",
		// Harvested Shumei id, verbatim (web-reverse-research.md §5).
		DeviceID: "BV7K3ps2Q7hph9kfcFDJfQwtNx9sOVSSrZeq54EnVtnktW+9+Q26WV3AJ4WHRHmw4VFqZv1gFBl8Wq9XHM5U6Hw==",
	})
	if _, err := c.Login(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if body == nil {
		t.Fatal("login body not captured")
	}
	if os, _ := body["os"].(string); os != "web" {
		t.Errorf("web channel login os = %v, want \"web\"", body["os"])
	}
	if did, _ := body["device_id"].(string); did != "BV7K3ps2Q7hph9kfcFDJfQwtNx9sOVSSrZeq54EnVtnktW+9+Q26WV3AJ4WHRHmw4VFqZv1gFBl8Wq9XHM5U6Hw==" {
		t.Errorf("web channel device_id = %q, want harvested value verbatim", did)
	}
	if headerPlatform != "android" {
		t.Errorf("x-client-platform = %q, want \"android\" (probe-verified through WAF)", headerPlatform)
	}
	if !uuidRe.MatchString(headerDeviceID) {
		t.Errorf("web channel x-device-id = %q, want minted UUID (browser sends its own UUID there)", headerDeviceID)
	}
	if strings.HasPrefix(headerDeviceID, "B") {
		t.Errorf("web channel x-device-id must not leak the Shumei body id")
	}
}

func TestDefaultChannelUnchanged(t *testing.T) {
	var mu sync.Mutex
	var body map[string]any
	var headerDeviceID string

	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			var b map[string]any
			json.NewDecoder(r.Body).Decode(&b)
			mu.Lock()
			body = b
			headerDeviceID = r.Header.Get("x-device-id")
			mu.Unlock()
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1"}})
		},
	})
	defer m.srv.Close()

	// No Channel, no DeviceID: the historical mint path.
	c := NewClient(Config{BaseURL: m.srv.URL, Mobile: "13800000000", Password: "pw"})
	if _, err := c.Login(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if os, _ := body["os"].(string); os != "android" {
		t.Errorf("default channel login os = %v, want \"android\" (byte-identical regression)", body["os"])
	}
	if did, _ := body["device_id"].(string); did == "" || did == headerDeviceID[:0] {
		t.Errorf("default channel device_id = %q, want android mint", did)
	}
	if headerDeviceID == "" {
		t.Error("default channel x-device-id header missing")
	}
	if did, _ := body["device_id"].(string); did != headerDeviceID {
		t.Errorf("default channel: body device_id (%q) and x-device-id (%q) must stay equal (android app behavior)", did, headerDeviceID)
	}
}

func TestWebChannelStableHeaderDeviceID(t *testing.T) {
	// The minted x-device-id UUID is deterministic per account (survives
	// restarts, never reshuffled into a "new device" from the risk
	// service's perspective).
	a := Account{Email: "a@b.c", Password: "pw", Channel: "web", DeviceID: "Bxxx=="}
	c1 := NewClient(Config{BaseURL: "http://x", Email: a.Email, Password: a.Password, Channel: a.Channel, DeviceID: a.DeviceID})
	c2 := NewClient(Config{BaseURL: "http://x", Email: a.Email, Password: a.Password, Channel: a.Channel, DeviceID: a.DeviceID})
	if c1.deviceProfile.headerDeviceID != c2.deviceProfile.headerDeviceID {
		t.Errorf("web channel x-device-id not deterministic: %q vs %q", c1.deviceProfile.headerDeviceID, c2.deviceProfile.headerDeviceID)
	}
	b := Account{Email: "other@b.c", Password: "pw", Channel: "web", DeviceID: "Byyy=="}
	c3 := NewClient(Config{BaseURL: "http://x", Email: b.Email, Password: b.Password, Channel: b.Channel, DeviceID: b.DeviceID})
	if c1.deviceProfile.headerDeviceID == c3.deviceProfile.headerDeviceID {
		t.Error("web channel x-device-id shared across accounts — risk correlation vector")
	}
}

func TestWebChannelRequiresExplicitDeviceID(t *testing.T) {
	a := Account{Email: "a@b.c", Password: "pw", Channel: "web"}
	if err := a.Validate(); err == nil {
		t.Fatal("web channel without explicit device_id must fail validation (forged ids are risk-rejected)")
	}
	a.DeviceID = "Bharvested=="
	if err := a.Validate(); err != nil {
		t.Fatalf("web channel with harvested device_id rejected: %v", err)
	}
}

func TestUnknownChannelRejected(t *testing.T) {
	a := Account{Email: "a@b.c", Password: "pw", Channel: "ios"}
	if err := a.Validate(); err != ErrUnknownChannel {
		t.Errorf("unknown channel error = %v, want ErrUnknownChannel", err)
	}
}

func TestPoolRejectsWebChannelWithoutDeviceID(t *testing.T) {
	accounts := []Account{
		{Email: "a@b.c", Password: "pw", Channel: "web"}, // no device_id
	}
	if _, err := NewPool(accounts, PoolConfig{BaseURL: "http://x"}); err == nil {
		t.Fatal("pool must reject web-channel account without harvested device_id at load time")
	}
}

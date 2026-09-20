package upstream

// Wire alignment — login body encoding (apk-alignment.md I1, revised).
//
// The app's Json config has encodeDefaults=true and explicitNulls=true
// (ak5 constructs bi5(true, true, true, ...) and zyb.A0() returns true), so
// UsersLoginWithPhoneAndPwdRequest (mta/ota) serializes os:"android" always
// and area_code as an explicit JSON null on mobile logins.

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
)

func TestLoginBodyEncodesDefaults(t *testing.T) {
	var mu sync.Mutex
	var mobileBody, emailBody map[string]any

	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/users/login": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			if body["mobile"] != nil {
				mobileBody = body
			} else {
				emailBody = body
			}
			mu.Unlock()
			writeEnvelope(w, 0, "", map[string]any{"user": map[string]any{"token": "tok1"}})
		},
	})
	defer m.srv.Close()

	c := m.client()
	if _, err := c.Login(context.Background()); err != nil {
		t.Fatal(err)
	}
	ce := NewClient(Config{BaseURL: m.srv.URL, Email: "a@b.c", Password: "pw"})
	if _, err := ce.Login(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for name, body := range map[string]map[string]any{"mobile": mobileBody, "email": emailBody} {
		if body == nil {
			t.Fatalf("%s login body not captured", name)
		}
		if body["os"] != "android" {
			t.Errorf("%s login body os = %v, want \"android\" (encodeDefaults=true)", name, body["os"])
		}
		if body["device_id"] == nil {
			t.Errorf("%s login body missing device_id: %v", name, body)
		}
	}
	if v, ok := mobileBody["area_code"]; !ok || v != nil {
		t.Errorf("mobile login area_code = %v (%t), want explicit JSON null", v, ok)
	}
	if _, ok := emailBody["area_code"]; ok {
		t.Errorf("email login body carries area_code; the DTO has no such field: %v", emailBody)
	}
}

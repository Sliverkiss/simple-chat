package accountstore

// Channel semantics at the store layer (web-reverse-research.md §6):
// EnsureDeviceIDs mints android-format ids for android-channel accounts,
// but a web-channel account's device_id is a browser-harvested Shumei
// fingerprint — minting one would present a forged id the risk service
// is guaranteed to reject (biz_code 11). The store must leave web-channel
// device_ids alone (harvest once in the browser, paste into accounts.json).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"simple-chat/internal/upstream"
)

func TestEnsureDeviceIDsDoesNotMintForWebChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	harvested := "BV7K3ps2Q7hph9kfcFDJfQwtNx9sOVSSrZeq54EnVtnktW+9+Q26WV3AJ4WHRHmw4VFqZv1gFBl8Wq9XHM5U6Hw=="
	writeTestAccounts(t, path, []upstream.Account{
		{Email: "web@example.com", Password: "pw", Channel: "web", DeviceID: harvested},
		{Mobile: "13800000000", Password: "pw"},
	})

	accounts, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil))
	if err != nil {
		t.Fatal(err)
	}

	for _, a := range accounts {
		if a.Channel == "web" {
			if a.DeviceID != harvested {
				t.Errorf("web-channel device_id = %q, want harvested value untouched", a.DeviceID)
			}
		} else if a.DeviceID == "" {
			t.Errorf("android-channel account %s still has no device id after EnsureDeviceIDs", a.Mobile)
		}
	}

	// Persisted state must not have grown a device_id for the web account.
	persisted := readTestFile(t, path)
	for _, a := range persisted {
		if a.Channel == "web" && a.DeviceID != harvested {
			t.Errorf("persisted web-channel device_id = %q, want %q (untouched)", a.DeviceID, harvested)
		}
	}
	_ = os.Remove(path)
}

func TestEnsureDeviceIDsLeavesEmptyWebChannelUnminted(t *testing.T) {
	// A web-channel account with a MISSING device_id must surface as a
	// validation error at pool load (ErrWebChannelNeedsDeviceID), not be
	// silently handed a forged android-format mint. writeTestFile validates
	// (and correctly rejects) this entry, so the raw file is written
	// directly to simulate an operator hand-editing accounts.json.
	path := filepath.Join(t.TempDir(), "accounts.json")
	raw := `{"accounts":[{"email":"web@example.com","password":"pw","channel":"web"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := EnsureDeviceIDs(context.Background(), NewJSONStore(path, nil))
	if err == nil {
		t.Fatal("web-channel account with no device_id must fail, not mint a forged id")
	}
	if !errors.Is(err, upstream.ErrWebChannelNeedsDeviceID) {
		t.Errorf("error = %v, want ErrWebChannelNeedsDeviceID (clear actionable message)", err)
	}
}

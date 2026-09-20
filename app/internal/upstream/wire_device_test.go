package upstream

// Wire alignment — device identity (apk-alignment.md C2/C3).
//
// The app's x-device-id / login device_id is Base64(AES-128-CBC/PKCS5(
// ANDROID_ID + "_" + Build.BRAND, key=MD5(packageName), IV=zeros)) — nf3.java.
// A bare UUID is a device-fingerprint tell no Android client produces.

import (
	"regexp"
	"strings"
	"testing"
)

// appDeviceIDPattern matches the app's mint: 32-byte padded plaintext →
// 48-byte ciphertext → 44-char Base64 (43 body chars + padding '=').
var appDeviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9+/]{43}=$`)

func TestDeviceIDMintMatchesAppFormat(t *testing.T) {
	explicit := Account{Mobile: "13800000000", Password: "pw", DeviceID: "harvested_smsdk_value"}
	if got := ResolveDeviceID(explicit); got != "harvested_smsdk_value" {
		t.Errorf("explicit device id = %q, want verbatim", got)
	}

	a := Account{Mobile: "13800000009", Password: "pw"}
	b := Account{Mobile: "13800000007", Password: "pw"}
	for _, acc := range []Account{a, b} {
		got := ResolveDeviceID(acc)
		if !appDeviceIDPattern.MatchString(got) {
			t.Errorf("account %s device id = %q, want app-format Base64", acc.Mobile, got)
		}
		if strings.Contains(got, "-") {
			t.Errorf("device id %q looks like a UUID; the app never emits UUIDs here", got)
		}
	}
	if ResolveDeviceID(a) == ResolveDeviceID(b) {
		t.Error("two accounts minted the same device id")
	}
	if ResolveDeviceID(a) != ResolveDeviceID(a) {
		t.Error("device id not deterministic")
	}
}

func TestDeviceIDMintIsAppDecryptableShape(t *testing.T) {
	// The mint is the app's exact construction: decrypting with the app's key
	// (MD5 of the package name) and zero IV must yield ANDROID_ID_BRAND
	// plaintext with PKCS7 padding and a 16-hex-char ANDROID_ID.
	id := ResolveDeviceID(Account{Mobile: "13800000007", Password: "pw"})
	if !appDeviceIDPattern.MatchString(id) {
		t.Fatalf("device id = %q, want app format", id)
	}
	plain, ok := decryptAppDeviceID(id)
	if !ok {
		t.Fatal("minted id is not decryptable in the app's scheme")
	}
	parts := strings.SplitN(plain, "_", 2)
	if len(parts) != 2 || len(parts[0]) != 16 {
		t.Fatalf("plaintext = %q, want 16-hex ANDROID_ID + _ + BRAND", plain)
	}
	for _, c := range parts[0] {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("ANDROID_ID %q is not lowercase hex", parts[0])
		}
	}
	if parts[1] != "google" {
		t.Errorf("BRAND = %q, want google", parts[1])
	}
}

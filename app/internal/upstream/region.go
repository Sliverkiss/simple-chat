// Region semantics for the android-app upstream.
//
// Evidence (2026-09-19, live probe + recon survey):
//   - The login response carries `is_mainland` as a USER attribute inside
//     biz_data.user — it describes the account, it is not a routing directive.
//   - No separate overseas host exists: chat-overseas/overseas/intl/chat-intl
//     .deepseek.com are all NXDOMAIN; every reference implementation (ds2api,
//     NIyueeE, snake-aabb, Fly143) talks to chat.deepseek.com exclusively.
//   - `REGISTER_FROM_MAINLAND` (Fly143) is a registration-time mainland-IP
//     restriction, not an API variant.
//
// Only the "cn" region is therefore real. The map below is the single place a
// future region would plug into; until one exists, anything else is rejected
// at load time rather than discovered mid-request.
package upstream

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// regionWire is the wire-protocol configuration for one region.
type regionWire struct {
	BaseURL string
}

// regions maps a normalized region name to its wire configuration.
var regions = map[string]regionWire{
	"cn": {BaseURL: DefaultBaseURL},
}

// normalizeRegion maps a raw accounts.json region value to a known region
// name. Empty means "cn" (the only region); unknown values return "".
func normalizeRegion(raw string) string {
	switch raw {
	case "", "cn":
		return "cn"
	default:
		return ""
	}
}

// ErrUnknownRegion rejects an accounts.json entry with an unsupported region.
var ErrUnknownRegion = errors.New("upstream: unknown region (only \"cn\" is supported)")

// ErrUnknownChannel rejects an accounts.json entry with an unsupported channel.
var ErrUnknownChannel = errors.New("upstream: unknown channel (only \"\" (android) and \"web\" are supported)")

// ErrWebChannelNeedsDeviceID rejects a web-channel account without an
// explicit device_id: the web channel presents a browser-harvested Shumei
// SMSdk fingerprint, and a minted/synthetic id is guaranteed risk-rejected
// (NIyueeE field evidence: forged device_ids → biz_code 11). Fail at load
// time with a clear message instead of a guaranteed RISK_DEVICE_DETECTED.
var ErrWebChannelNeedsDeviceID = errors.New("upstream: channel \"web\" requires an explicit device_id (browser-harvested Shumei SMSdk id — see web-reverse-research.md §5)")

// regionBaseURL returns the upstream base URL for a normalized region.
func regionBaseURL(region string) string {
	if rw, ok := regions[region]; ok && rw.BaseURL != "" {
		return rw.BaseURL
	}
	return DefaultBaseURL
}

// Account is one entry in accounts.json.
type Account struct {
	Mobile   string `json:"mobile"`
	Email    string `json:"email"`
	Password string `json:"password"`
	// Region selects the upstream wire configuration; empty means "cn".
	Region string `json:"region"`
	// DeviceID is this account's persistent device identity on the login
	// wire. Empty means "resolve one deterministically" (see ResolveDeviceID);
	// an explicit value (e.g. a browser-harvested Shumei SMSdk fingerprint)
	// is used verbatim. Never shared between accounts — the upstream risk
	// service correlates device ids across accounts (device-id-research.md).
	DeviceID string `json:"device_id,omitempty"`
	// Channel selects which client channel this account's device identity
	// was minted in (web-reverse-research.md §6). "" means android (default,
	// historical behavior). "web" means the device_id is a browser-harvested
	// Shumei SMSdk id ("B…" base64) for a web-bound account: the login body
	// then sends os:"web" and the id verbatim, while wire headers stay
	// android-profile (probe-verified through the AWS WAF from Go TLS).
	Channel string `json:"channel,omitempty"`
	// ParkKind/ParkUntil/ParkReason/ParkedAt persist the pool's park state
	// (TASK_MUTE) so a restart keeps a muted/banned account out of rotation
	// instead of re-hitting the upstream and renewing the window. kind is
	// "banned"/"muted"/"risk"; until is RFC3339. BANNED persists forever —
	// manual revive means deleting these fields from accounts.json.
	ParkKind   string `json:"park_kind,omitempty"`
	ParkUntil  string `json:"park_until,omitempty"`
	ParkReason string `json:"park_reason,omitempty"`
	ParkedAt   string `json:"parked_at,omitempty"`
	// SessionToken is the account's last successful upstream login token,
	// written through by the memory-first store on every login/relogin
	// (docs-spec-memory-first.md). It rides the persisted record so the
	// store always holds a complete one; it is NOT replayed into the client
	// on boot (the wire exposes no expiry) — the lazy-relogin-on-auth-failure
	// path handles staleness naturally.
	SessionToken string `json:"session_token,omitempty"`
}

// deviceIDNamespace is the fixed UUIDv5 namespace for account device ids.
// It namespaces generation so ids from this deployment never collide with
// UUIDv5s minted under standard namespaces by accident.
var deviceIDNamespace = [16]byte{
	0xa5, 0xc3, 0xf7, 0xe1, 0x2b, 0x4d, 0x4f, 0x6a,
	0x8c, 0x91, 0x0d, 0x3e, 0x5f, 0x7a, 0x9b, 0x2c,
}

// uuidv5 returns the RFC 4122 version-5 (SHA-1 name-based) UUID of name
// under namespace ns.
func uuidv5(ns [16]byte, name string) [16]byte {
	h := sha1.New()
	h.Write(ns[:])
	h.Write([]byte(name))
	var u [16]byte
	copy(u[:], h.Sum(nil)[:16])
	u[6] = (u[6] & 0x0f) | 0x50 // version 5
	u[8] = (u[8] & 0x3f) | 0x80 // RFC 4122 variant
	return u
}

// formatUUID renders b in the canonical 8-4-4-4-12 layout, lowercase.
func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Device-id mint (apk-alignment.md C3): the app derives
//
//	device_id = Base64(AES-128-CBC/PKCS5(ANDROID_ID + "_" + Build.BRAND,
//	                                  key = MD5(packageName), IV = zeros(16)))
//
// (nf3.java, package com.deepseek.chat). A bare UUID there is a
// device-fingerprint tell no Android client produces, so our deterministic
// per-account mint reproduces the app's exact construction over a synthetic
// ANDROID_ID (16 lowercase hex chars — the shape Settings.Secure emits)
// derived from the account identity. Same account → same inputs → same id,
// forever; no persistence needed.
const (
	appPackageName = "com.deepseek.chat"
	appDeviceBrand = "google"
)

// appDeviceKey is the AES key the app uses: MD5(packageName).
func appDeviceKey() []byte {
	sum := md5.Sum([]byte(appPackageName))
	return sum[:]
}

// syntheticAndroidID derives a stable 16-hex-char ANDROID_ID from the
// account identity (mobile or email) — shape-identical to Settings.Secure.
func syntheticAndroidID(identity string) string {
	sum := sha256.Sum256([]byte("simple-chat/device-id/" + identity))
	return hex.EncodeToString(sum[:8])
}

// mintAppDeviceID runs the app's mint: AES-CBC/PKCS5 encrypt of
// ANDROID_ID + "_" + BRAND under MD5(pkg) with a zero IV, Base64-encoded.
func mintAppDeviceID(androidID, brand string) (string, error) {
	block, err := aes.NewCipher(appDeviceKey())
	if err != nil {
		return "", err
	}
	plain := []byte(androidID + "_" + brand)
	// PKCS7 pad to the block size.
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	plain = append(plain, make([]byte, pad)...)
	for i := len(plain) - pad; i < len(plain); i++ {
		plain[i] = byte(pad)
	}
	enc := cipher.NewCBCEncrypter(block, make([]byte, aes.BlockSize))
	out := make([]byte, len(plain))
	enc.CryptBlocks(out, plain)
	return base64.StdEncoding.EncodeToString(out), nil
}

// decryptAppDeviceID inverts mintAppDeviceID (test seam: proves the mint is
// the app's construction, not just a Base64 blob).
func decryptAppDeviceID(id string) (string, bool) {
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil || len(raw)%aes.BlockSize != 0 || len(raw) == 0 {
		return "", false
	}
	block, err := aes.NewCipher(appDeviceKey())
	if err != nil {
		return "", false
	}
	dec := cipher.NewCBCDecrypter(block, make([]byte, aes.BlockSize))
	out := make([]byte, len(raw))
	dec.CryptBlocks(out, raw)
	pad := int(out[len(out)-1])
	if pad == 0 || pad > aes.BlockSize || pad > len(out) {
		return "", false
	}
	for _, b := range out[len(out)-pad:] {
		if int(b) != pad {
			return "", false
		}
	}
	return string(out[:len(out)-pad]), true
}

// ResolveDeviceID returns the account's device id: the explicit value
// verbatim, or the app-format deterministic mint derived from the account
// identity (mobile, else email). The same account always resolves to the
// same id — persistence to accounts.json is a file-format nicety;
// determinism is the guarantee.
func ResolveDeviceID(a Account) string {
	if id := strings.TrimSpace(a.DeviceID); id != "" {
		return id
	}
	identity := strings.TrimSpace(a.Mobile)
	if identity == "" {
		identity = strings.TrimSpace(a.Email)
	}
	if identity == "" {
		// Validate() rejects such accounts upstream of here; fall back to
		// a random app-format id rather than an empty (risk-scored) one.
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return ""
		}
		id, err := mintAppDeviceID(hex.EncodeToString(b[:]), appDeviceBrand)
		if err != nil {
			return ""
		}
		return id
	}
	id, err := mintAppDeviceID(syntheticAndroidID(identity), appDeviceBrand)
	if err != nil {
		return ""
	}
	return id
}

// Identity returns the account's primary identity: mobile, else email
// (email-only accounts are the one case where mobile is empty). Used as the
// dedup/delete key by the admin API and the store key prefix on Redis.
func (a Account) Identity() string {
	if id := strings.TrimSpace(a.Mobile); id != "" {
		return id
	}
	return strings.TrimSpace(a.Email)
}

// MatchesIdentity reports whether id (mobile or email) identifies this
// account: the id equals the mobile, or — for email-only accounts — the
// email. Used by the admin delete path against URL path segments.
func (a Account) MatchesIdentity(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	if a.Mobile != "" {
		return a.Mobile == id
	}
	return a.Email != "" && a.Email == id
}

// Validate checks one account entry at load time.
func (a Account) Validate() error {
	if a.Password == "" {
		return errors.New("upstream: account entry missing password")
	}
	if a.Mobile == "" && a.Email == "" {
		return errors.New("upstream: account entry needs mobile or email")
	}
	if normalizeRegion(a.Region) == "" {
		return ErrUnknownRegion
	}
	if !normalizeChannel(a.Channel) {
		return ErrUnknownChannel
	}
	if a.Channel == "web" && strings.TrimSpace(a.DeviceID) == "" {
		return ErrWebChannelNeedsDeviceID
	}
	return nil
}

// normalizeChannel reports whether the raw accounts.json channel value is
// supported ("" = android default, "web").
func normalizeChannel(raw string) bool {
	switch raw {
	case "", "web":
		return true
	default:
		return false
	}
}

// normalizedRegion returns the account's region name after normalization
// (guaranteed nonempty for a validated account).
func (a Account) normalizedRegion() string {
	return normalizeRegion(a.Region)
}

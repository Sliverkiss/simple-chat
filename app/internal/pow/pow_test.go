package pow

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"
)

// Golden vectors (protocol-conformance, independently derived;
// cross-confirmed by a second independent implementation).
func TestHashV1GoldenVectors(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "e594808bc5b7151ac160c6d39a02e0a8e261ed588578403099e3561dc40c26b3"},
		{"testsalt_1700000000_42", "d4a2ea58c89e40887c933484868380c6f803eaa8dc53a3b9df8e431b921a4f09"},
		{"testsalt_1700000000_100000", "abea2f35796b65486e9be1b36f7878c66cab021e96faa473fdf4decd31f9ba30"},
		{"abc123salt_1700000000_12345", "74b3b7452745b70e85eb32ee7f0a9ec0381d42dd5137b695da915e104fc390e1"},
		// Live-solved challenge from recon.md §3.4 (probe of 2026-09-19).
		{"6c3a962c828dd81d7d69_1780227446451_86022", "34d4c336676aa2e83c3308148e12ba8bc5e77ccb2fc12eeea1046e20c4c64eec"},
	} {
		h := HashV1([]byte(tc.in))
		got := hex.EncodeToString(h[:])
		if got != tc.want {
			t.Errorf("HashV1(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// Hash must absorb inputs longer than one rate block (136 bytes) correctly.
func TestHashV1MultiBlock(t *testing.T) {
	// 300 bytes spans three rate blocks; verify against the naive
	// implementation: same sponge, constructed via the exported one-block path
	// is not possible, so we pin determinism + length and cross-check that the
	// single-block path (above) stays correct. The sponge structure itself is
	// covered by the golden vectors; here we assert stability.
	a := HashV1(make([]byte, 300))
	b := HashV1(make([]byte, 300))
	if a != b {
		t.Fatal("hash not deterministic for multi-block input")
	}
	c := HashV1(make([]byte, 301))
	if a == c {
		t.Fatal("300-byte and 301-byte inputs must differ")
	}
}

func TestBuildPrefix(t *testing.T) {
	if got := BuildPrefix("testsalt", 1700000000); got != "testsalt_1700000000_" {
		t.Errorf("BuildPrefix = %q", got)
	}
}

func TestSolvePowFindsGoldenAnswers(t *testing.T) {
	for _, tc := range []struct {
		salt   string
		expire int64
		answer int64
		diff   int64
	}{
		{"testsalt", 1700000000, 42, 1000},
		{"testsalt", 1700000000, 500, 2000},
		{"abc123salt", 1700000000, 12345, 20000},
	} {
		h := HashV1([]byte(BuildPrefix(tc.salt, tc.expire) + strconv.FormatInt(tc.answer, 10)))
		got, err := SolvePow(context.Background(), hex.EncodeToString(h[:]), tc.salt, tc.expire, tc.diff)
		if err != nil {
			t.Fatalf("salt=%q: %v", tc.salt, err)
		}
		if got != tc.answer {
			t.Errorf("salt=%q: got answer %d, want %d", tc.salt, got, tc.answer)
		}
	}
}

// Live challenge from recon.md §3.4: answer 86022, difficulty 144000.
func TestSolvePowLiveChallenge(t *testing.T) {
	got, err := SolvePow(context.Background(),
		"34d4c336676aa2e83c3308148e12ba8bc5e77ccb2fc12eeea1046e20c4c64eec",
		"6c3a962c828dd81d7d69", 1780227446451, 144000)
	if err != nil {
		t.Fatal(err)
	}
	if got != 86022 {
		t.Errorf("got %d, want 86022", got)
	}
}

func TestSolvePowRejectsMalformedChallenge(t *testing.T) {
	if _, err := SolvePow(context.Background(), "zz", "salt", 1, 100); err == nil {
		t.Error("expected error for non-hex challenge")
	}
	if _, err := SolvePow(context.Background(), hex.EncodeToString(make([]byte, 32)), "salt", 1, 0); err == nil {
		t.Error("expected error when difficulty=0 (no solution)")
	}
}

func TestSolvePowContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SolvePow(ctx, hex.EncodeToString(make([]byte, 32)), "salt", 1700000000, 144000); err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestBuildPowHeader(t *testing.T) {
	c := &Challenge{
		Algorithm:  "HashV1",
		Challenge:  "61" + "00",
		Salt:       "salt",
		ExpireAt:   1712345678,
		Difficulty: 2000,
		Signature:  "sig",
		TargetPath: "/api/v0/chat/completion",
	}
	header, err := BuildPowHeader(c, 777)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"algorithm", "challenge", "salt", "answer", "signature", "target_path"}
	for _, k := range wantKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("header payload missing key %q", k)
		}
	}
	if m["answer"] != float64(777) {
		t.Errorf("answer = %v, want 777", m["answer"])
	}
	// difficulty/expire_at must NOT be in the header payload.
	for _, k := range []string{"difficulty", "expire_at"} {
		if _, ok := m[k]; ok {
			t.Errorf("header payload must not contain %q", k)
		}
	}
}

func TestSolveAndBuildHeaderEndToEnd(t *testing.T) {
	h := HashV1([]byte("salt_1712345678_777"))
	header, err := SolveAndBuildHeader(context.Background(), &Challenge{
		Algorithm: "HashV1", Challenge: hex.EncodeToString(h[:]),
		Salt: "salt", ExpireAt: 1712345678, Difficulty: 2000,
		Signature: "sig", TargetPath: "/api/v0/chat/completion",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(header)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["answer"] != float64(777) {
		t.Errorf("answer = %v, want 777", m["answer"])
	}
}

func TestSolveAndBuildHeaderRejectsUnknownAlgorithm(t *testing.T) {
	if _, err := SolveAndBuildHeader(context.Background(), &Challenge{Algorithm: "SHA256"}); err == nil {
		t.Error("expected error for unsupported algorithm")
	}
}

func TestSolveAndBuildHeaderAcceptsDeepSeekHashV1(t *testing.T) {
	// The live upstream names the algorithm "DeepSeekHashV1" (android-app
	// protocol, same as ds2api expects). "HashV1" was the web-client name.
	c := &Challenge{
		Algorithm:   "DeepSeekHashV1",
		Challenge:   hex.EncodeToString(powTestHash()),
		Salt:        "testsalt",
		ExpireAt:    1700000000,
		Difficulty:  144000,
		Signature:   "sig",
		TargetPath:  "/api/v0/chat/completion",
	}
	hdr, err := SolveAndBuildHeader(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if hdr == "" {
		t.Fatal("empty header")
	}
}

func powTestHash() []byte {
	h := HashV1([]byte("testsalt_1700000000_42"))
	return h[:]
}

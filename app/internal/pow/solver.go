package pow

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Challenge mirrors data.biz_data.challenge from
// POST /api/v0/chat/create_pow_challenge.
type Challenge struct {
	Algorithm   string `json:"algorithm"`
	Challenge   string `json:"challenge"`
	Salt        string `json:"salt"`
	ExpireAt    int64  `json:"expire_at"`
	Difficulty  int64  `json:"difficulty"`
	ExpireAfter int64  `json:"expire_after"`
	Signature   string `json:"signature"`
	TargetPath  string `json:"target_path"`
}

// BuildPrefix returns "<salt>_<expire_at>_", the pre-image prefix for nonces.
func BuildPrefix(salt string, expireAt int64) string {
	return salt + "_" + strconv.FormatInt(expireAt, 10) + "_"
}

// SolvePow searches nonce ∈ [0, difficulty) such that
// HashV1("<salt>_<expire_at>_<nonce>") == challenge (exact match).
// The prefix is pre-absorbed into the keccak state so the hot loop is
// allocation-free; ctx is checked every 1024 nonces.
func SolvePow(ctx context.Context, challengeHex, salt string, expireAt, difficulty int64) (int64, error) {
	if len(challengeHex) != 64 {
		return 0, errors.New("pow: challenge must be 64 hex chars")
	}
	target, err := hex.DecodeString(challengeHex)
	if err != nil {
		return 0, err
	}
	t0 := binary.LittleEndian.Uint64(target[0:])
	t1 := binary.LittleEndian.Uint64(target[8:])
	t2 := binary.LittleEndian.Uint64(target[16:])
	t3 := binary.LittleEndian.Uint64(target[24:])

	prefix := []byte(BuildPrefix(salt, expireAt))
	const rate = 136
	var baseState [25]uint64
	off := 0
	for off+rate <= len(prefix) {
		for i := 0; i < rate/8; i++ {
			baseState[i] ^= binary.LittleEndian.Uint64(prefix[off+i*8:])
		}
		keccakF23(&baseState)
		off += rate
	}
	tailLen := len(prefix) - off
	var tail [rate]byte
	copy(tail[:], prefix[off:])

	var numBuf [20]byte
	for n := int64(0); n < difficulty; n++ {
		if n&0x3FF == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}

		v := uint64(n)
		pos := 20
		if v == 0 {
			pos--
			numBuf[pos] = '0'
		} else {
			for v > 0 {
				pos--
				numBuf[pos] = byte('0' + v%10)
				v /= 10
			}
		}
		numLen := 20 - pos
		s := baseState
		totalTail := tailLen + numLen
		if totalTail < rate {
			var buf [rate]byte
			copy(buf[:tailLen], tail[:tailLen])
			copy(buf[tailLen:totalTail], numBuf[pos:])
			buf[totalTail] = 0x06
			buf[rate-1] |= 0x80
			for i := 0; i < rate/8; i++ {
				s[i] ^= binary.LittleEndian.Uint64(buf[i*8:])
			}
			keccakF23(&s)
		} else {
			// Prefix tail plus nonce span a full rate block; absorb in two blocks.
			var buf [rate]byte
			copy(buf[:tailLen], tail[:tailLen])
			copy(buf[tailLen:rate], numBuf[pos:pos+(rate-tailLen)])
			for i := 0; i < rate/8; i++ {
				s[i] ^= binary.LittleEndian.Uint64(buf[i*8:])
			}
			keccakF23(&s)
			var buf2 [rate]byte
			rem := totalTail - rate
			copy(buf2[:rem], numBuf[pos+(rate-tailLen):pos+(rate-tailLen)+rem])
			buf2[rem] = 0x06
			buf2[rate-1] |= 0x80
			for i := 0; i < rate/8; i++ {
				s[i] ^= binary.LittleEndian.Uint64(buf2[i*8:])
			}
			keccakF23(&s)
		}
		if s[0] == t0 && s[1] == t1 && s[2] == t2 && s[3] == t3 {
			return n, nil
		}
	}
	return 0, errors.New("pow: no solution within difficulty")
}

// BuildPowHeader serializes {algorithm, challenge, salt, answer, signature,
// target_path} as base64(JSON) for the x-ds-pow-response header.
// difficulty and expire_at are deliberately excluded.
func BuildPowHeader(c *Challenge, answer int64) (string, error) {
	b, err := json.Marshal(map[string]any{
		"algorithm":   c.Algorithm,
		"challenge":   c.Challenge,
		"salt":        c.Salt,
		"answer":      answer,
		"signature":   c.Signature,
		"target_path": c.TargetPath,
	})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// SolveAndBuildHeader goes end to end: Challenge → x-ds-pow-response value.
func SolveAndBuildHeader(ctx context.Context, c *Challenge) (string, error) {
	// The live android-app upstream names the algorithm "DeepSeekHashV1"
	// (ds2api expects exactly that); "HashV1" is the web client's name for
	// the same scheme. The response header echoes the challenge's name.
	switch c.Algorithm {
	case "HashV1", "DeepSeekHashV1":
	default:
		return "", fmt.Errorf("pow: unsupported algorithm %q", c.Algorithm)
	}
	d := c.Difficulty
	if d == 0 {
		d = 144000
	}
	answer, err := SolvePow(ctx, c.Challenge, c.Salt, c.ExpireAt, d)
	if err != nil {
		return "", err
	}
	return BuildPowHeader(c, answer)
}

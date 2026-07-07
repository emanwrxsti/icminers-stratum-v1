package bitcoinbase

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
)

// BitsToTarget expands a compact nBits value (hex, as delivered by
// getblocktemplate, e.g. "1d00ffff") into the 256-bit big-endian target.
func BitsToTarget(bitsHex string) (*big.Int, error) {
	raw, err := hex.DecodeString(bitsHex)
	if err != nil {
		return nil, fmt.Errorf("decode bits %q: %w", bitsHex, err)
	}
	if len(raw) != 4 {
		return nil, fmt.Errorf("bits %q is %d bytes, want 4", bitsHex, len(raw))
	}
	compact := binary.BigEndian.Uint32(raw)
	return CompactToTarget(compact), nil
}

// CompactToTarget expands a compact uint32 nBits into the target big.Int,
// following Bitcoin's arith_uint256 SetCompact semantics (without the sign
// bit, which never appears in valid targets).
func CompactToTarget(compact uint32) *big.Int {
	exponent := int(compact >> 24)
	mantissa := int64(compact & 0x007fffff)
	target := big.NewInt(mantissa)
	if exponent <= 3 {
		target.Rsh(target, uint(8*(3-exponent)))
	} else {
		target.Lsh(target, uint(8*(exponent-3)))
	}
	return target
}

// TargetToBytes renders a target as a fixed 32-byte big-endian slice.
func TargetToBytes(target *big.Int) []byte {
	out := make([]byte, 32)
	target.FillBytes(out)
	return out
}

// Uint32LEHex encodes v as 4 little-endian bytes in hex. Used for the nonce,
// nTime, and version fields when reconstructing headers from stratum submits.
func Uint32LEHex(v uint32) string {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return hex.EncodeToString(b[:])
}

// PrevHashToStratum converts a previousblockhash from display hex (big-endian)
// to the stratum mining.notify encoding: the hash's internal little-endian
// byte order with every 4-byte word swapped, which is equivalent to reversing
// the order of the 4-byte words of the display hex while leaving each word's
// bytes untouched.
func PrevHashToStratum(displayHex string) (string, error) {
	raw, err := hex.DecodeString(displayHex)
	if err != nil {
		return "", fmt.Errorf("decode prevhash %q: %w", displayHex, err)
	}
	if len(raw) != 32 {
		return "", fmt.Errorf("prevhash %q is %d bytes, want 32", displayHex, len(raw))
	}
	out := make([]byte, 32)
	for i := 0; i < 32; i += 4 {
		copy(out[i:i+4], raw[28-i:32-i])
	}
	return hex.EncodeToString(out), nil
}

// StratumPrevHashToLE converts the stratum notify prevhash encoding back to
// internal little-endian bytes (each 4-byte word byte-swapped). Needed on the
// submit path when reconstructing the 80-byte header.
func StratumPrevHashToLE(stratumHex string) ([]byte, error) {
	raw, err := hex.DecodeString(stratumHex)
	if err != nil {
		return nil, fmt.Errorf("decode stratum prevhash: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("stratum prevhash is %d bytes, want 32", len(raw))
	}
	out := make([]byte, 32)
	for i := 0; i < 32; i += 4 {
		out[i+0] = raw[i+3]
		out[i+1] = raw[i+2]
		out[i+2] = raw[i+1]
		out[i+3] = raw[i+0]
	}
	return out, nil
}

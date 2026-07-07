package bitcoinbase

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// DoubleSHA256 is Bitcoin's SHA256d.
func DoubleSHA256(b []byte) []byte {
	first := sha256.Sum256(b)
	second := sha256.Sum256(first[:])
	return second[:]
}

// ReverseBytes returns a reversed copy of b. Used to convert between display
// (big-endian) hex and internal (little-endian) byte order.
func ReverseBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, v := range b {
		out[len(b)-1-i] = v
	}
	return out
}

// HashLEFromDisplayHex decodes a display-order (big-endian) 32-byte hash hex
// string into internal little-endian bytes.
func HashLEFromDisplayHex(displayHex string) ([]byte, error) {
	raw, err := hex.DecodeString(displayHex)
	if err != nil {
		return nil, fmt.Errorf("decode hash %q: %w", displayHex, err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("hash %q is %d bytes, want 32", displayHex, len(raw))
	}
	return ReverseBytes(raw), nil
}

// MerkleBranch computes the stratum merkle branch for the given transaction
// hashes (internal little-endian byte order, coinbase EXCLUDED). Miners fold
// their coinbase hash through these steps to reach the merkle root:
//
//	root = fold(coinbaseHash, steps) where fold(h, s) = SHA256d(h || s)
//
// The algorithm mirrors the classic stratum merkletree "steps" construction:
// at each level the element immediately after the (unknown) coinbase slot is
// emitted as a step, then the level is reduced pairwise.
func MerkleBranch(txHashesLE [][]byte) [][]byte {
	steps := make([][]byte, 0)
	// Level with a placeholder (nil) in the coinbase position.
	level := make([][]byte, 0, len(txHashesLE)+1)
	level = append(level, nil)
	level = append(level, txHashesLE...)

	for len(level) > 1 {
		steps = append(steps, level[1])
		if len(level)%2 == 1 {
			level = append(level, level[len(level)-1])
		}
		next := [][]byte{nil}
		for i := 2; i < len(level); i += 2 {
			next = append(next, DoubleSHA256(append(append([]byte{}, level[i]...), level[i+1]...)))
		}
		level = next
	}
	return steps
}

// FoldBranch computes the merkle root (little-endian) from a coinbase hash and
// a branch produced by MerkleBranch.
func FoldBranch(coinbaseHashLE []byte, steps [][]byte) []byte {
	h := append([]byte{}, coinbaseHashLE...)
	for _, s := range steps {
		h = DoubleSHA256(append(append([]byte{}, h...), s...))
	}
	return h
}

// MerkleRoot computes the full merkle root (little-endian) over all
// transaction hashes including the coinbase, using standard Bitcoin pairwise
// duplication of the trailing element on odd levels. Used by tests to verify
// MerkleBranch/FoldBranch and by submit-side block reconstruction.
func MerkleRoot(allHashesLE [][]byte) []byte {
	if len(allHashesLE) == 0 {
		return nil
	}
	level := make([][]byte, len(allHashesLE))
	for i, h := range allHashesLE {
		level[i] = append([]byte{}, h...)
	}
	for len(level) > 1 {
		if len(level)%2 == 1 {
			level = append(level, level[len(level)-1])
		}
		next := make([][]byte, 0, len(level)/2)
		for i := 0; i < len(level); i += 2 {
			next = append(next, DoubleSHA256(append(append([]byte{}, level[i]...), level[i+1]...)))
		}
		level = next
	}
	return level[0]
}

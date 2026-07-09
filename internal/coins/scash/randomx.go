// Package scash implements the CoinAdapter for Scash (SatoshiCash), a Bitcoin
// fork whose proof-of-work is RandomX rather than SHA256d.
//
// RandomX is a memory-hard VM-based PoW: computing the inner hash requires a
// ~2 GiB dataset seeded by a per-epoch key and hundreds of MiB of scratchpad.
// That inner hash cannot be reimplemented in pure Go here, and there is no
// pure-Go RandomX in the module set, so it is provided through a pluggable
// RandomXHasher interface: production wires a cgo binding to the reference
// librandomx, while tests inject a deterministic double. Everything ELSE that
// makes Scash work — the per-epoch seed-hash derivation, the RandomX
// commitment transform, the extended block header, difficulty comparison, and
// address handling — is implemented and tested here in pure Go.
package scash

import (
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/blake2b"
)

// RandomXHashSize is RandomX's fixed hash/commitment output size (32 bytes).
const RandomXHashSize = 32

// RandomXHasher computes the RandomX VM hash of an input under a given seed
// key (the epoch seed hash). Implementations MUST be deterministic for a given
// (seed, input) and MUST return a 32-byte little-endian digest. The production
// implementation delegates to librandomx; a fake is used in tests.
//
// seed is the 32-byte epoch seed hash (see EpochSeedHash). input is the 80-byte
// Scash header serialized with an all-zero RandomX field.
type RandomXHasher interface {
	// RandomXHash returns the RandomX hash of input under seed.
	RandomXHash(seed, input []byte) ([]byte, error)
	// Algo identifies the backing implementation (e.g. "randomx-cgo",
	// "randomx-fake") for logging and health output.
	Algo() string
}

// Commitment computes the RandomX commitment exactly as the reference
// implementation's randomx_calculate_commitment:
//
//	blake2b(input || rxHash, outLen = 32)
//
// The commitment — not the raw RandomX hash — is what Scash compares against
// the share and network targets. This is pure Go and KAT-verified.
func Commitment(input, rxHash []byte) ([]byte, error) {
	if len(rxHash) != RandomXHashSize {
		return nil, fmt.Errorf("scash: rx hash is %d bytes, want %d", len(rxHash), RandomXHashSize)
	}
	h, err := blake2b.New(RandomXHashSize, nil)
	if err != nil {
		return nil, fmt.Errorf("scash: blake2b: %w", err)
	}
	if _, err := h.Write(input); err != nil {
		return nil, err
	}
	if _, err := h.Write(rxHash); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// unavailableHasher is the default hasher when no RandomX backend is wired. It
// fails loudly rather than silently accepting or rejecting shares, so a
// misconfigured deployment cannot mint bad blocks.
type unavailableHasher struct{}

func (unavailableHasher) RandomXHash(seed, input []byte) ([]byte, error) {
	return nil, fmt.Errorf("scash: no RandomX backend configured " +
		"(wire a RandomXHasher backed by librandomx, or inject a test hasher)")
}
func (unavailableHasher) Algo() string { return "randomx-unavailable" }

// hexSeed is a small helper for logging seed hashes.
func hexSeed(seed []byte) string { return hex.EncodeToString(seed) }

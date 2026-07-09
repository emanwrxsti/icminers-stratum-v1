package bitcoinbase

import (
	"crypto/sha512"
	"encoding/hex"
	"testing"
)

// TestSHA512256DKnownAnswer pins the primitive against a hand-computed value:
// SHA512_256D(x) = SHA-512/256( SHA-512/256(x) ). We verify the inner hash
// against the published SHA-512/256("abc") vector, then confirm the double
// hash matches an independent computation.
func TestSHA512256DKnownAnswer(t *testing.T) {
	// Published SHA-512/256("abc").
	inner := sha512.Sum512_256([]byte("abc"))
	const want = "53048e2681941ef99b2e29b76b4c7dabe4c2d0c634fc6d46e0e2f13107e7af23"
	if hex.EncodeToString(inner[:]) != want {
		t.Fatalf("SHA-512/256(\"abc\") = %s, want %s", hex.EncodeToString(inner[:]), want)
	}
	// Double hash matches an independent recomputation.
	outer := sha512.Sum512_256(inner[:])
	got, err := SHA512_256D([]byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(got) != hex.EncodeToString(outer[:]) {
		t.Fatalf("SHA512_256D mismatch: %x vs %x", got, outer[:])
	}
	if len(got) != 32 {
		t.Fatalf("digest length = %d, want 32", len(got))
	}
}

package bitcoinbase

import (
	"encoding/hex"
	"math/big"
	"testing"
)

// TestScryptHashLitecoinGenesis pins ScryptHash to the Litecoin genesis
// header and, more importantly, verifies that the scrypt proof-of-work hash
// satisfies the genesis difficulty target (bits 0x1e0ffff0). This proves the
// PoW hash is computed correctly: unlike SHA256d (which yields the block
// IDENTITY hash 12a765e3...), the scrypt hash is what the network target is
// compared against.
func TestScryptHashLitecoinGenesis(t *testing.T) {
	headerHex := "010000000000000000000000000000000000000000000000000000000000000000000000" +
		"d9ced4ed1130f7b7faad9be25323ffafa33232a17c3edf6cfd97bee6bafbdd97" +
		"b9aa8e4ef0ff0f1ecd513f7c"
	header, err := hex.DecodeString(headerHex)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 80 {
		t.Fatalf("header len = %d, want 80", len(header))
	}

	h, err := ScryptHash(header)
	if err != nil {
		t.Fatal(err)
	}
	// Exact scrypt(N=1024,r=1,p=1) digest of the genesis header (display form).
	got := hex.EncodeToString(ReverseBytes(h))
	want := "0000050c34a64b415b6b15b37f2216634b5b1669cb9a2e38d76f7213b0671e00"
	if got != want {
		t.Fatalf("scrypt(genesis) = %s, want %s", got, want)
	}

	// The scrypt hash (little-endian, as a big int) must meet the genesis
	// target derived from bits 0x1e0ffff0.
	target, err := BitsToTarget("1e0ffff0")
	if err != nil {
		t.Fatal(err)
	}
	hashBig := new(big.Int).SetBytes(ReverseBytes(h))
	if hashBig.Cmp(target) > 0 {
		t.Fatalf("scrypt hash does not meet genesis target:\n hash=%x\n tgt =%x", hashBig, target)
	}

	// Sanity: SHA256d of the same header is the block IDENTITY, a different
	// value entirely, confirming PoW and identity are distinct hashes.
	id := hex.EncodeToString(ReverseBytes(DoubleSHA256(header)))
	if id == got {
		t.Fatal("scrypt PoW and SHA256d identity must differ")
	}
	if id != "12a765e31ffd4059bada1e25190f6e98c99d9714d334efa41a195a7e7e04bfe2" {
		t.Fatalf("genesis block identity (SHA256d) = %s", id)
	}
}

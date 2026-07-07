package bitcoinbase

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"
)

// Bitcoin mainnet block 170 (the first block with a non-coinbase transaction:
// the Satoshi -> Hal Finney 10 BTC payment). Two txids and the block's merkle
// root, all extremely well-documented constants.
var block170TxIDs = []string{
	"b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220bfa78111c5082", // coinbase
	"f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16",
}

const block170MerkleRoot = "7dac2c5666815c17a3b36427de37bb9d2e2c5ccec3f8633eb91a4205cb4c10ff"

// Genesis block constants (single transaction: merkle root == coinbase txid).
const (
	genesisTxID      = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
	genesisBlockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	genesisTime      = 1231006505
	genesisNonce     = 2083236893
)

func hashesLE(t *testing.T, displayHex []string) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(displayHex))
	for _, h := range displayHex {
		le, err := HashLEFromDisplayHex(h)
		if err != nil {
			t.Fatalf("HashLEFromDisplayHex(%q): %v", h, err)
		}
		out = append(out, le)
	}
	return out
}

// TestMerkleRootBlock170 verifies the full merkle-root computation against the
// known root of a real Bitcoin block (the first 2-transaction block).
func TestMerkleRootBlock170(t *testing.T) {
	all := hashesLE(t, block170TxIDs)
	rootLE := MerkleRoot(all)
	got := hex.EncodeToString(ReverseBytes(rootLE))
	if got != block170MerkleRoot {
		t.Fatalf("merkle root = %s, want %s", got, block170MerkleRoot)
	}
}

// TestMerkleRootGenesis: a single-transaction block's merkle root is its txid.
func TestMerkleRootGenesis(t *testing.T) {
	all := hashesLE(t, []string{genesisTxID})
	got := hex.EncodeToString(ReverseBytes(MerkleRoot(all)))
	if got != genesisTxID {
		t.Fatalf("genesis merkle root = %s, want %s", got, genesisTxID)
	}
}

// TestGenesisHeaderHash reconstructs the 80-byte genesis header from documented
// constants using this package's field encoders and checks DoubleSHA256
// produces the genesis block hash. This pins down byte ordering end to end.
func TestGenesisHeaderHash(t *testing.T) {
	header := make([]byte, 0, 80)
	header = appendUint32LE(header, 1)           // version
	header = append(header, make([]byte, 32)...) // prev hash (null)
	merkleLE, err := HashLEFromDisplayHex(genesisTxID)
	if err != nil {
		t.Fatal(err)
	}
	header = append(header, merkleLE...)
	header = appendUint32LE(header, genesisTime)
	header = appendUint32LE(header, 0x1d00ffff) // bits
	header = appendUint32LE(header, genesisNonce)
	if len(header) != 80 {
		t.Fatalf("header is %d bytes, want 80", len(header))
	}
	got := hex.EncodeToString(ReverseBytes(DoubleSHA256(header)))
	if got != genesisBlockHash {
		t.Fatalf("genesis header hash = %s, want %s", got, genesisBlockHash)
	}
}

// TestBranchFoldMatchesFullRoot verifies that folding the coinbase hash
// through MerkleBranch steps produces the same root as the full computation,
// for the real block and for randomized transaction sets of varying sizes.
func TestBranchFoldMatchesFullRoot(t *testing.T) {
	// Real block: branch excludes coinbase.
	all := hashesLE(t, block170TxIDs)
	branch := MerkleBranch(all[1:])
	folded := FoldBranch(all[0], branch)
	if !bytes.Equal(folded, MerkleRoot(all)) {
		t.Fatal("block 170: folded branch root != full merkle root")
	}

	// Randomized sets: 0..12 non-coinbase txs.
	for n := 0; n <= 12; n++ {
		set := make([][]byte, n+1)
		for i := range set {
			set[i] = make([]byte, 32)
			if _, err := rand.Read(set[i]); err != nil {
				t.Fatal(err)
			}
		}
		branch := MerkleBranch(set[1:])
		folded := FoldBranch(set[0], branch)
		full := MerkleRoot(set)
		if !bytes.Equal(folded, full) {
			t.Fatalf("n=%d: folded root %x != full root %x", n, folded, full)
		}
	}
}

// TestMerkleBranchEmptyTemplate ensures a template with only the coinbase
// yields an empty branch and the root equals the coinbase hash.
func TestMerkleBranchEmptyTemplate(t *testing.T) {
	branch := MerkleBranch(nil)
	if len(branch) != 0 {
		t.Fatalf("branch len = %d, want 0", len(branch))
	}
	cb := make([]byte, 32)
	if !bytes.Equal(FoldBranch(cb, branch), cb) {
		t.Fatal("root of single-tx block must equal the coinbase hash")
	}
}

// TestBitsToTargetKAT verifies nBits expansion against two documented vectors:
// diff-1 (0x1d00ffff) and the classic 0x1b0404cb example.
func TestBitsToTargetKAT(t *testing.T) {
	cases := []struct {
		bits string
		want string
	}{
		{"1d00ffff", "00000000ffff0000000000000000000000000000000000000000000000000000"},
		{"1b0404cb", "00000000000404cb000000000000000000000000000000000000000000000000"},
	}
	for _, c := range cases {
		target, err := BitsToTarget(c.bits)
		if err != nil {
			t.Fatalf("BitsToTarget(%s): %v", c.bits, err)
		}
		got := hex.EncodeToString(TargetToBytes(target))
		if got != c.want {
			t.Fatalf("BitsToTarget(%s) = %s, want %s", c.bits, got, c.want)
		}
	}
}

// TestCompactToTargetSmallExponent covers the exponent<=3 right-shift path.
func TestCompactToTargetSmallExponent(t *testing.T) {
	// exponent=3 keeps the mantissa as-is.
	if got := CompactToTarget(0x03123456); got.Cmp(big.NewInt(0x123456)) != 0 {
		t.Fatalf("0x03123456 -> %s, want 0x123456", got.Text(16))
	}
	// exponent=1 shifts right by 16 bits.
	if got := CompactToTarget(0x01123456); got.Cmp(big.NewInt(0x12)) != 0 {
		t.Fatalf("0x01123456 -> %s, want 0x12", got.Text(16))
	}
}

// TestPrevHashStratumRoundTrip verifies the notify prevhash encoding is the
// documented word-order reversal of the display hex and that the submit-path
// decoder inverts it back to little-endian bytes.
func TestPrevHashStratumRoundTrip(t *testing.T) {
	display := "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
	stratum, err := PrevHashToStratum(display)
	if err != nil {
		t.Fatal(err)
	}
	// Word-order reversal of the display hex, each 8-hex-char word intact.
	want := "fd33e5063986da1a346c3f1fd2b00432ecaad478aa200b1c0003ba2700000000"
	if stratum != want {
		t.Fatalf("stratum prevhash = %s, want %s", stratum, want)
	}

	le, err := StratumPrevHashToLE(stratum)
	if err != nil {
		t.Fatal(err)
	}
	displayRaw, _ := hex.DecodeString(display)
	if !bytes.Equal(le, ReverseBytes(displayRaw)) {
		t.Fatal("StratumPrevHashToLE did not recover little-endian bytes")
	}
}

// TestParseTemplate covers valid parsing and each rejection path.
func TestParseTemplate(t *testing.T) {
	valid := map[string]any{
		"version":           536870912,
		"rules":             []string{"segwit"},
		"previousblockhash": "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506",
		"transactions": []map[string]any{
			{"data": "aa", "txid": block170TxIDs[1], "hash": block170TxIDs[1], "fee": 100},
		},
		"coinbasevalue":              312500000,
		"target":                     "00000000ffff0000000000000000000000000000000000000000000000000000",
		"mintime":                    1700000000,
		"curtime":                    1700000600,
		"bits":                       "1d00ffff",
		"height":                     100001,
		"default_witness_commitment": "6a24aa21a9ed" + "e2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf9",
	}
	mustJSON := func(v any) json.RawMessage {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	tpl, err := ParseTemplate(mustJSON(valid))
	if err != nil {
		t.Fatalf("ParseTemplate(valid): %v", err)
	}
	if tpl.Height != 100001 || tpl.Bits != "1d00ffff" || len(tpl.Transactions) != 1 {
		t.Fatalf("unexpected parse result: %+v", tpl)
	}
	if got := tpl.TxIDs(); len(got) != 1 || got[0] != block170TxIDs[1] {
		t.Fatalf("TxIDs = %v", got)
	}

	broken := func(mutate func(m map[string]any)) json.RawMessage {
		m := map[string]any{}
		b, _ := json.Marshal(valid)
		_ = json.Unmarshal(b, &m)
		mutate(m)
		return mustJSON(m)
	}
	rejects := map[string]json.RawMessage{
		"missing height":  broken(func(m map[string]any) { delete(m, "height") }),
		"bad prevhash":    broken(func(m map[string]any) { m["previousblockhash"] = "zz" }),
		"bad bits":        broken(func(m map[string]any) { m["bits"] = "xyz" }),
		"zero curtime":    broken(func(m map[string]any) { m["curtime"] = 0 }),
		"zero cb value":   broken(func(m map[string]any) { m["coinbasevalue"] = 0 }),
		"tx without txid": broken(func(m map[string]any) { m["transactions"] = []map[string]any{{"data": "aa"}} }),
	}
	for name, raw := range rejects {
		if _, err := ParseTemplate(raw); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
}

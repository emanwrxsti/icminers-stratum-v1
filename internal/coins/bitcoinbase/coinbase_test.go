package bitcoinbase

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func testSpec() CoinbaseSpec {
	script, _ := AddressToScript("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", BTCMainNet)
	return CoinbaseSpec{
		Height:          840000,
		CoinbaseValue:   312500000,
		PayoutScript:    script,
		Tag:             "/ICMINERS/",
		ExtraNonce1Size: 4,
		ExtraNonce2Size: 4,
		TxVersion:       2,
	}
}

// TestBuildCoinbaseStructure assembles the split coinbase with concrete
// extranonces and verifies the serialized transaction field-by-field.
func TestBuildCoinbaseStructure(t *testing.T) {
	spec := testSpec()
	parts, err := BuildCoinbase(spec)
	if err != nil {
		t.Fatal(err)
	}
	en1 := []byte{0xde, 0xad, 0xbe, 0xef}
	en2 := []byte{0x01, 0x02, 0x03, 0x04}
	tx, err := parts.Assemble(en1, en2)
	if err != nil {
		t.Fatal(err)
	}

	// version
	if v := binary.LittleEndian.Uint32(tx[0:4]); v != 2 {
		t.Fatalf("tx version = %d, want 2", v)
	}
	// input count
	if tx[4] != 0x01 {
		t.Fatalf("input count = %d, want 1", tx[4])
	}
	// null prevout
	if !bytes.Equal(tx[5:37], make([]byte, 32)) {
		t.Fatal("prevout hash not null")
	}
	if binary.LittleEndian.Uint32(tx[37:41]) != 0xffffffff {
		t.Fatal("prevout index != 0xffffffff")
	}
	// scriptSig
	scriptLen := int(tx[41])
	script := tx[42 : 42+scriptLen]
	height, consumed, err := DecodeHeightBIP34(script)
	if err != nil {
		t.Fatal(err)
	}
	if height != spec.Height {
		t.Fatalf("BIP34 height = %d, want %d", height, spec.Height)
	}
	rest := script[consumed:]
	if !bytes.HasPrefix(rest, []byte(spec.Tag)) {
		t.Fatal("tag missing from scriptSig")
	}
	enInScript := rest[len(spec.Tag):]
	if !bytes.Equal(enInScript, append(append([]byte{}, en1...), en2...)) {
		t.Fatal("extranonce bytes not at expected scriptSig position")
	}
	// sequence
	pos := 42 + scriptLen
	if binary.LittleEndian.Uint32(tx[pos:pos+4]) != 0xffffffff {
		t.Fatal("sequence != 0xffffffff")
	}
	pos += 4
	// outputs
	if tx[pos] != 0x01 {
		t.Fatalf("output count = %d, want 1", tx[pos])
	}
	pos++
	if v := binary.LittleEndian.Uint64(tx[pos : pos+8]); v != uint64(spec.CoinbaseValue) {
		t.Fatalf("output value = %d, want %d", v, spec.CoinbaseValue)
	}
	pos += 8
	outScriptLen := int(tx[pos])
	pos++
	if !bytes.Equal(tx[pos:pos+outScriptLen], spec.PayoutScript) {
		t.Fatal("payout script mismatch")
	}
	pos += outScriptLen
	// locktime
	if binary.LittleEndian.Uint32(tx[pos:pos+4]) != 0 {
		t.Fatal("locktime != 0")
	}
	if pos+4 != len(tx) {
		t.Fatalf("trailing bytes: parsed %d of %d", pos+4, len(tx))
	}
}

// TestBuildCoinbaseWitnessCommitment verifies the zero-value commitment output
// is appended when the template provides one.
func TestBuildCoinbaseWitnessCommitment(t *testing.T) {
	spec := testSpec()
	wc, _ := hex.DecodeString("6a24aa21a9ed" + strings.Repeat("ab", 32))
	spec.WitnessCommitmentScript = wc
	parts, err := BuildCoinbase(spec)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := parts.Assemble(make([]byte, 4), make([]byte, 4))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(tx, wc) {
		t.Fatal("witness commitment script not present")
	}
	// Output count must be 2 (payout + commitment).
	scriptLen := int(tx[41])
	pos := 42 + scriptLen + 4
	if tx[pos] != 0x02 {
		t.Fatalf("output count = %d, want 2", tx[pos])
	}
	// The commitment output value must be zero.
	// Skip payout output: value(8) + varint(1) + script.
	pos++
	pos += 8
	l := int(tx[pos])
	pos += 1 + l
	if v := binary.LittleEndian.Uint64(tx[pos : pos+8]); v != 0 {
		t.Fatalf("commitment output value = %d, want 0", v)
	}
}

// TestBuildCoinbaseRejects covers the guard rails.
func TestBuildCoinbaseRejects(t *testing.T) {
	base := testSpec()

	bad := base
	bad.Height = 0
	if _, err := BuildCoinbase(bad); err == nil {
		t.Fatal("height 0: expected error")
	}
	bad = base
	bad.CoinbaseValue = 0
	if _, err := BuildCoinbase(bad); err == nil {
		t.Fatal("value 0: expected error")
	}
	bad = base
	bad.PayoutScript = nil
	if _, err := BuildCoinbase(bad); err == nil {
		t.Fatal("empty payout script: expected error")
	}
	bad = base
	bad.Tag = strings.Repeat("x", 101)
	if _, err := BuildCoinbase(bad); err == nil {
		t.Fatal("oversized scriptSig: expected error")
	}
	parts, _ := BuildCoinbase(base)
	if _, err := parts.Assemble([]byte{0x00}, []byte{0x00}); err == nil {
		t.Fatal("wrong extranonce width: expected error")
	}
}

// TestEncodeHeightBIP34 round-trips heights including the signed-padding edge.
func TestEncodeHeightBIP34(t *testing.T) {
	for _, h := range []int64{1, 127, 128, 255, 256, 65535, 100000, 840000, 16777215, 16777216} {
		enc := EncodeHeightBIP34(h)
		got, consumed, err := DecodeHeightBIP34(enc)
		if err != nil {
			t.Fatalf("height %d: %v", h, err)
		}
		if got != h || consumed != len(enc) {
			t.Fatalf("height %d round-trip = %d (consumed %d/%d)", h, got, consumed, len(enc))
		}
		// Script numbers are signed: the top byte must not have the high bit.
		if enc[len(enc)-1]&0x80 != 0 {
			t.Fatalf("height %d: encoding has sign bit set", h)
		}
	}
}

// TestAddressToScriptKAT verifies address decoding against known vectors:
// the genesis P2PKH address, a well-known P2SH address, and the BIP173
// reference P2WPKH vector.
func TestAddressToScriptKAT(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		// Genesis block payout address (P2PKH).
		{"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
			"76a914" + "62e907b15cbf27d5425399ebf6f0fb50ebb88f18" + "88ac"},
		// BIP173 reference vector: P2WPKH for the all-known pubkey.
		{"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
			"0014" + "751e76e8199196d454941c45d1b3a323f1433bd6"},
		// BIP173 reference vector: P2WSH.
		{"bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3",
			"0020" + "1863143c14c5166804bd19203356da136c985678cd4d27a1b8c6329604903262"},
	}
	for _, c := range cases {
		script, err := AddressToScript(c.addr, BTCMainNet)
		if err != nil {
			t.Fatalf("AddressToScript(%s): %v", c.addr, err)
		}
		if got := hex.EncodeToString(script); got != c.want {
			t.Fatalf("AddressToScript(%s) = %s, want %s", c.addr, got, c.want)
		}
	}
}

// TestAddressToScriptRejects covers malformed inputs.
func TestAddressToScriptRejects(t *testing.T) {
	for _, addr := range []string{
		"",
		"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfN0", // bad base58 char '0'
		"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNb", // checksum mismatch
		"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t5", // bech32 checksum mismatch
		"3J98t1WpEZ73CNmQviecrnyiWrnqRhWNL",          // truncated P2SH (checksum fails)
	} {
		if _, err := AddressToScript(addr, BTCMainNet); err == nil {
			t.Fatalf("AddressToScript(%q): expected error, got nil", addr)
		}
	}
}

// TestNotifyParamsAssembly builds a job from a realistic template and checks
// the mining.notify positional parameters element by element.
func TestNotifyParamsAssembly(t *testing.T) {
	txids := []string{
		strings.Repeat("11", 32),
		strings.Repeat("22", 32),
		strings.Repeat("33", 32),
	}
	raw, _ := json.Marshal(map[string]any{
		"version":           536870912,
		"rules":             []string{"segwit"},
		"previousblockhash": "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506",
		"transactions": []map[string]any{
			{"data": "aa", "txid": txids[0]},
			{"data": "bb", "txid": txids[1]},
			{"data": "cc", "txid": txids[2]},
		},
		"coinbasevalue": 312500000,
		"mintime":       1700000000,
		"curtime":       1700000600,
		"bits":          "1d00ffff",
		"height":        100001,
	})
	tpl, err := ParseTemplate(raw)
	if err != nil {
		t.Fatal(err)
	}
	job, err := NewJob("1a", tpl, testSpec(), true)
	if err != nil {
		t.Fatal(err)
	}
	params := job.NotifyParams()
	if len(params) != 9 {
		t.Fatalf("notify params len = %d, want 9", len(params))
	}
	if params[0] != "1a" {
		t.Fatalf("jobId = %v", params[0])
	}
	if params[1] != "fd33e5063986da1a346c3f1fd2b00432ecaad478aa200b1c0003ba2700000000" {
		t.Fatalf("prevhash = %v", params[1])
	}
	if params[2] != job.Coinb1Hex || params[3] != job.Coinb2Hex {
		t.Fatal("coinb1/coinb2 mismatch")
	}
	branch, ok := params[4].([]any)
	if !ok || len(branch) != 2 {
		// 3 txs -> branch has 2 steps (level sizes 4 -> 2 -> 1).
		t.Fatalf("branch = %v", params[4])
	}
	if params[5] != "20000000" {
		t.Fatalf("version = %v, want 20000000", params[5])
	}
	if params[6] != "1d00ffff" {
		t.Fatalf("nbits = %v", params[6])
	}
	if params[7] != "6553f358" {
		// 1700000600 = 0x6553f358
		t.Fatalf("ntime = %v, want 6553f358", params[7])
	}
	if params[8] != true {
		t.Fatalf("cleanJobs = %v, want true", params[8])
	}

	// The branch must fold a real coinbase hash to the same root as the full
	// merkle computation over coinbase + template txs.
	cbTx, err := job.CoinbaseParts.Assemble([]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8})
	if err != nil {
		t.Fatal(err)
	}
	cbHash := DoubleSHA256(cbTx)
	all := [][]byte{cbHash}
	all = append(all, hashesLE(t, txids)...)
	if !bytes.Equal(FoldBranch(cbHash, job.MerkleBranchLE), MerkleRoot(all)) {
		t.Fatal("notify branch does not fold to the full merkle root")
	}

	// The whole parameter array must survive JSON marshaling (what actually
	// goes on the wire).
	if _, err := json.Marshal(params); err != nil {
		t.Fatalf("notify params not marshalable: %v", err)
	}
}

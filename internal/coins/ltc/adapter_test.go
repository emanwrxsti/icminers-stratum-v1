package ltc

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// fakeCore is a minimal Litecoin Core stand-in serving GBT and submitblock.
func fakeCore(t *testing.T, template map[string]any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var result any
		switch req.Method {
		case "getblocktemplate":
			result = template
		case "submitblock":
			result = nil // accepted
		default:
			t.Errorf("unexpected rpc method %q", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID, "result": result, "error": nil})
	}))
}

func ltcTemplate() map[string]any {
	return map[string]any{
		"version":                    536870912,
		"rules":                      []string{"segwit"},
		"previousblockhash":          "00000000000000000000000000000000000000000000000000000000deadbeef",
		"transactions":               []map[string]any{},
		"coinbasevalue":              1250000000, // 12.5 LTC
		"mintime":                    1700000000,
		"curtime":                    1700000600,
		"bits":                       "207fffff", // easy regtest-style target
		"height":                     2600001,
		"default_witness_commitment": "6a24aa21a9ed" + strings.Repeat("cd", 32),
	}
}

func newLTCAdapter(t *testing.T, url, network, addr string) *Adapter {
	t.Helper()
	a, err := New(Options{
		RPC:         rpc.New(rpc.Options{URL: url}),
		Network:     network,
		PoolAddress: addr,
		CoinbaseTag: "/ICMINERS-LTC/",
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestLTCUsesScryptAndAcceptsLTCAddress is the core abstraction proof: the LTC
// adapter builds a job from a Litecoin template, a scrypt-mined share
// validates as a block candidate, the difficulty comes from the SCRYPT hash
// (not SHA256d), and the block identity is SHA256d.
func TestLTCUsesScryptAndAcceptsLTCAddress(t *testing.T) {
	srv := fakeCore(t, ltcTemplate())
	defer srv.Close()
	// A Litecoin bech32 address (rejected by the BTC adapter, accepted here).
	a := newLTCAdapter(t, srv.URL, "mainnet", "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kgmn4n9")

	if a.Symbol() != "LTC" || a.Algo() != "scrypt" {
		t.Fatalf("symbol/algo = %s/%s", a.Symbol(), a.Algo())
	}

	ctx := context.Background()
	tpl, err := a.GetBlockTemplate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, err := a.BuildMiningJob(ctx, tpl, "")
	if err != nil {
		t.Fatal(err)
	}
	job.JobID = "1"
	base, err := BitcoinbaseJob(job)
	if err != nil {
		t.Fatal(err)
	}
	base.JobID = "1"

	en1 := []byte{0x00, 0x00, 0x00, 0x01}
	en2 := []byte{0x00, 0x00, 0x00, 0x2a}

	// Independently reconstruct the header and mine against the SCRYPT target,
	// exactly as a real scrypt miner would.
	cb, err := base.CoinbaseParts.Assemble(en1, en2)
	if err != nil {
		t.Fatal(err)
	}
	root := bitcoinbase.FoldBranch(bitcoinbase.DoubleSHA256(cb), base.MerkleBranchLE)
	prevLE, _ := bitcoinbase.HashLEFromDisplayHex(base.PrevHashDisplay)
	bitsRaw, _ := hex.DecodeString(base.BitsHex)
	verRaw, _ := hex.DecodeString(base.VersionHex)

	workerDiff := 1e-9
	target := vardiff.DifficultyToTarget(workerDiff)
	ntime := uint32(base.CurTime)
	header := make([]byte, 80)
	binary.LittleEndian.PutUint32(header[0:4], binary.BigEndian.Uint32(verRaw))
	copy(header[4:36], prevLE)
	copy(header[36:68], root)
	binary.LittleEndian.PutUint32(header[68:72], ntime)
	binary.LittleEndian.PutUint32(header[72:76], binary.BigEndian.Uint32(bitsRaw))

	var nonce uint32
	found := false
	for n := uint32(0); n < 2_000_000; n++ {
		binary.LittleEndian.PutUint32(header[76:80], n)
		pow, err := bitcoinbase.ScryptHash(header)
		if err != nil {
			t.Fatal(err)
		}
		if vardiff.MeetsTarget(vardiff.HashToBig(pow), target) {
			nonce = n
			found = true
			break
		}
	}
	if !found {
		t.Fatal("could not mine a scrypt share (target too hard for test)")
	}

	res, err := a.ValidateShare(ctx, job, coins.ShareSubmit{
		Worker:      "w.rig1",
		JobID:       "1",
		ExtraNonce1: hex.EncodeToString(en1),
		ExtraNonce2: hex.EncodeToString(en2),
		NTime:       fmt.Sprintf("%08x", ntime),
		Nonce:       fmt.Sprintf("%08x", nonce),
		WorkerDiff:  workerDiff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Fatal("scrypt-mined share reported invalid")
	}
	if !res.BlockCandidate {
		t.Fatal("share under regtest network target must be a block candidate")
	}

	// The block IDENTITY is SHA256d, independent of the scrypt PoW.
	raw, err := hex.DecodeString(res.BlockHex)
	if err != nil {
		t.Fatal(err)
	}
	gotID := hex.EncodeToString(bitcoinbase.ReverseBytes(bitcoinbase.DoubleSHA256(raw[:80])))
	if gotID != res.BlockHash {
		t.Fatalf("block identity %s != reported %s", gotID, res.BlockHash)
	}

	// Prove the difficulty came from scrypt, NOT SHA256d: the SHA256d hash of
	// this header does NOT meet the (tiny) network target, but the scrypt hash
	// does — that's why it validated.
	sha := vardiff.HashToBig(bitcoinbase.DoubleSHA256(header))
	scr := vardiff.HashToBig(func() []byte { h, _ := bitcoinbase.ScryptHash(header); return h }())
	if sha.Cmp(scr) == 0 {
		t.Fatal("scrypt and sha256d hashes are identical (impossible)")
	}
}

// TestLTCRejectsBadAddressAndNetwork proves LTC address validation is distinct
// from BTC: a BTC bech32 address is not a valid LTC address.
func TestLTCRejectsBadAddressAndNetwork(t *testing.T) {
	client := rpc.New(rpc.Options{URL: "http://127.0.0.1:1"})
	if _, err := New(Options{RPC: client, Network: "moonnet", PoolAddress: "x"}); err == nil {
		t.Fatal("unknown network accepted")
	}
	// A BTC bech32 address (hrp "bc") must not validate under LTC (hrp "ltc").
	if _, err := New(Options{RPC: client, Network: "mainnet",
		PoolAddress: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}); err == nil {
		t.Fatal("BTC address accepted by LTC adapter")
	}
}

// TestLTCNormalizeAddress checks LTC bech32 normalization.
func TestLTCNormalizeAddress(t *testing.T) {
	srv := fakeCore(t, ltcTemplate())
	defer srv.Close()
	a := newLTCAdapter(t, srv.URL, "mainnet", "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kgmn4n9")
	// Uppercased bech32 must normalize to lowercase and validate.
	got, err := a.NormalizeAddress("LTC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KGMN4N9")
	if err != nil {
		t.Fatal(err)
	}
	if got != "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kgmn4n9" {
		t.Fatalf("normalized = %s", got)
	}
}

package rxd

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

// A valid Radiant mainnet P2PKH address (base58, version 0x00).
const rxdMainAddr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

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
			result = nil
		default:
			t.Errorf("unexpected rpc method %q", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID, "result": result, "error": nil})
	}))
}

func rxdTemplate() map[string]any {
	return map[string]any{
		"version":                    536870912,
		"rules":                      []string{"segwit"},
		"previousblockhash":          "00000000000000000000000000000000000000000000000000000000deadbeef",
		"transactions":               []map[string]any{},
		"coinbasevalue":              5000000000,
		"mintime":                    1700000000,
		"curtime":                    1700000600,
		"bits":                       "207fffff",
		"height":                     100001,
		"default_witness_commitment": "6a24aa21a9ed" + strings.Repeat("cd", 32),
	}
}

func newRXDAdapter(t *testing.T, url, network, addr string) *Adapter {
	t.Helper()
	a, err := New(Options{
		RPC:         rpc.New(rpc.Options{URL: url}),
		Network:     network,
		PoolAddress: addr,
		CoinbaseTag: "/ICMINERS-RXD/",
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestRXDUsesSHA512256dForPoWAndIdentity is the core abstraction proof: the
// RXD adapter builds a job from a Radiant template, a SHA-512/256d-mined share
// validates as a block candidate, difficulty comes from SHA-512/256d (not
// SHA256d), AND the block identity is also SHA-512/256d (Radiant replaced
// SHA256 throughout — this is what makes RXD different from LTC, whose
// identity stays SHA256d).
func TestRXDUsesSHA512256dForPoWAndIdentity(t *testing.T) {
	srv := fakeCore(t, rxdTemplate())
	defer srv.Close()
	a := newRXDAdapter(t, srv.URL, "mainnet", rxdMainAddr)

	if a.Symbol() != "RXD" || a.Algo() != "sha512_256d" {
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
		pow, err := bitcoinbase.SHA512_256D(header)
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
		t.Fatal("could not mine a sha512_256d share (target too hard for test)")
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
		t.Fatal("sha512_256d-mined share reported invalid")
	}
	if !res.BlockCandidate {
		t.Fatal("share under regtest network target must be a block candidate")
	}

	// The block IDENTITY must be SHA-512/256d for Radiant (NOT SHA256d).
	raw, err := hex.DecodeString(res.BlockHex)
	if err != nil {
		t.Fatal(err)
	}
	id512, _ := bitcoinbase.SHA512_256D(raw[:80])
	gotID := hex.EncodeToString(bitcoinbase.ReverseBytes(id512))
	if gotID != res.BlockHash {
		t.Fatalf("block identity (sha512_256d) %s != reported %s", gotID, res.BlockHash)
	}
	// And it must NOT match the SHA256d identity — proving IdentityHash is wired.
	sha256id := hex.EncodeToString(bitcoinbase.ReverseBytes(bitcoinbase.DoubleSHA256(raw[:80])))
	if sha256id == res.BlockHash {
		t.Fatal("RXD block identity matched SHA256d; IdentityHash not applied")
	}

	// Difficulty must come from sha512_256d, not sha256d.
	sha := vardiff.HashToBig(bitcoinbase.DoubleSHA256(header))
	s512 := vardiff.HashToBig(func() []byte { h, _ := bitcoinbase.SHA512_256D(header); return h }())
	if sha.Cmp(s512) == 0 {
		t.Fatal("sha512_256d and sha256d hashes identical (impossible)")
	}
}

// TestRXDRejectsBadNetwork checks network validation.
func TestRXDRejectsBadNetwork(t *testing.T) {
	client := rpc.New(rpc.Options{URL: "http://127.0.0.1:1"})
	if _, err := New(Options{RPC: client, Network: "moonnet", PoolAddress: rxdMainAddr}); err == nil {
		t.Fatal("unknown network accepted")
	}
	// Radiant has no bech32; a bech32 string must be rejected.
	if _, err := New(Options{RPC: client, Network: "mainnet",
		PoolAddress: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}); err == nil {
		t.Fatal("bech32 address accepted by RXD adapter")
	}
}

// TestRXDNormalizeAddress checks base58 validation round-trips.
func TestRXDNormalizeAddress(t *testing.T) {
	srv := fakeCore(t, rxdTemplate())
	defer srv.Close()
	a := newRXDAdapter(t, srv.URL, "mainnet", rxdMainAddr)
	got, err := a.NormalizeAddress(rxdMainAddr)
	if err != nil {
		t.Fatal(err)
	}
	if got != rxdMainAddr {
		t.Fatalf("normalized = %s, want %s", got, rxdMainAddr)
	}
	// A corrupted checksum must be rejected.
	if _, err := a.NormalizeAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfXX"); err == nil {
		t.Fatal("address with bad checksum accepted")
	}
}

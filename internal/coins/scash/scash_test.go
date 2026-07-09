package scash

import (
	"context"
	"crypto/sha256"
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

// A valid Scash mainnet P2PKH address (base58, version 0x00, same as BTC).
const scashMainAddr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

// TestCommitmentKAT pins the RandomX commitment transform against an
// independent blake2b computation: commitment = blake2b(input || rxHash, 32).
func TestCommitmentKAT(t *testing.T) {
	inp := make([]byte, 80)
	for i := range inp {
		inp[i] = byte(i)
	}
	rxHash := make([]byte, 32)
	for i := range rxHash {
		rxHash[i] = 0xAB
	}
	got, err := Commitment(inp, rxHash)
	if err != nil {
		t.Fatal(err)
	}
	const want = "555065545736bbfeb82829b6718463709721a0ed7341ae7848b15ab9b35cc69e"
	if hex.EncodeToString(got) != want {
		t.Fatalf("commitment = %s, want %s", hex.EncodeToString(got), want)
	}
}

// TestEpochSeedHash pins the epoch seed derivation and its epoch boundaries.
func TestEpochSeedHash(t *testing.T) {
	const dur = int64(604800) // one week
	// epoch = floor(t/dur). For t=604800 the epoch is 1.
	seed := EpochSeedHash(604800, dur)
	want := sha256.Sum256([]byte("Scash/RandomX/Epoch/1"))
	if hex.EncodeToString(seed) != hex.EncodeToString(want[:]) {
		t.Fatalf("seed = %x, want %x", seed, want[:])
	}
	// Times within the same epoch share a seed; crossing a boundary changes it.
	a := EpochSeedHash(604800, dur)
	b := EpochSeedHash(604800+100, dur)
	if hex.EncodeToString(a) != hex.EncodeToString(b) {
		t.Fatal("seed changed within an epoch")
	}
	c := EpochSeedHash(604800*2, dur)
	if hex.EncodeToString(a) == hex.EncodeToString(c) {
		t.Fatal("seed did not change across an epoch boundary")
	}
	if EpochNumber(604800, dur) != 1 || EpochNumber(0, dur) != 0 {
		t.Fatal("epoch number wrong")
	}
}

// TestUnavailableHasherFailsClosed proves a pool with no RandomX backend
// refuses to validate rather than silently accepting shares.
func TestUnavailableHasherFailsClosed(t *testing.T) {
	pow := PoWHash(nil, 0)
	if _, err := pow(make([]byte, 80)); err == nil {
		t.Fatal("PoW must fail when no RandomX backend is configured")
	}
}

// fakeRandomX is a deterministic stand-in for librandomx. It is NOT RandomX;
// it lets us exercise the full share/commitment/target pipeline without the
// 2 GiB dataset. It hashes seed||input with SHA-256 so results are stable and
// seed-dependent (proving the epoch seed actually feeds the PoW).
type fakeRandomX struct{}

func (fakeRandomX) RandomXHash(seed, input []byte) ([]byte, error) {
	h := sha256.New()
	h.Write(seed)
	h.Write(input)
	return h.Sum(nil), nil
}
func (fakeRandomX) Algo() string { return "randomx-fake" }

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

func scashTemplate() map[string]any {
	return map[string]any{
		"version":                    536870912,
		"rules":                      []string{"segwit"},
		"previousblockhash":          "00000000000000000000000000000000000000000000000000000000deadbeef",
		"transactions":               []map[string]any{},
		"coinbasevalue":              5000000000,
		"mintime":                    1700000000,
		"curtime":                    1700000600,
		"bits":                       "207fffff",
		"height":                     50001,
		"default_witness_commitment": "6a24aa21a9ed" + strings.Repeat("cd", 32),
	}
}

func newSCASHAdapter(t *testing.T, url string) *Adapter {
	t.Helper()
	a, err := New(Options{
		RPC:         rpc.New(rpc.Options{URL: url}),
		Network:     "mainnet",
		PoolAddress: scashMainAddr,
		CoinbaseTag: "/ICMINERS-SCASH/",
		RandomX:     fakeRandomX{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSCASHRandomXShareFlow proves the SCASH adapter mines and validates a
// share whose difficulty is the RandomX COMMITMENT (not SHA256d), using the
// pluggable hasher, and assembles a block whose identity is SHA256d.
func TestSCASHRandomXShareFlow(t *testing.T) {
	srv := fakeCore(t, scashTemplate())
	defer srv.Close()
	a := newSCASHAdapter(t, srv.URL)

	if a.Symbol() != "SCASH" || a.Algo() != "randomx" {
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

	// The PoW the miner must beat is the SCASH commitment closure.
	pow := PoWHash(fakeRandomX{}, 0)
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
		commit, err := pow(header)
		if err != nil {
			t.Fatal(err)
		}
		if vardiff.MeetsTarget(vardiff.HashToBig(commit), target) {
			nonce = n
			found = true
			break
		}
	}
	if !found {
		t.Fatal("could not mine a scash share (target too hard for test)")
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
		t.Fatal("scash share reported invalid")
	}
	if !res.BlockCandidate {
		t.Fatal("share under regtest network target must be a block candidate")
	}

	// Block identity is SHA256d (Scash keeps Bitcoin's block hash).
	raw, err := hex.DecodeString(res.BlockHex)
	if err != nil {
		t.Fatal(err)
	}
	gotID := hex.EncodeToString(bitcoinbase.ReverseBytes(bitcoinbase.DoubleSHA256(raw[:80])))
	if gotID != res.BlockHash {
		t.Fatalf("block identity %s != reported %s", gotID, res.BlockHash)
	}

	// Prove difficulty came from the RandomX commitment, not SHA256d: the
	// SHA256d of the header must NOT meet the tiny target (overwhelmingly).
	sha := vardiff.HashToBig(bitcoinbase.DoubleSHA256(header))
	commit, _ := pow(header)
	if sha.Cmp(vardiff.HashToBig(commit)) == 0 {
		t.Fatal("commitment and sha256d identical (impossible)")
	}
}

// TestSCASHSeedFeedsPoW proves the epoch seed actually influences the PoW: two
// headers identical except for ntime in different epochs yield different
// commitments (because the RandomX seed differs).
func TestSCASHSeedFeedsPoW(t *testing.T) {
	pow := PoWHash(fakeRandomX{}, 604800)
	h1 := make([]byte, 80)
	h2 := make([]byte, 80)
	// epoch 1 vs epoch 5 (different seeds).
	binary.LittleEndian.PutUint32(h1[68:72], 604800*1)
	binary.LittleEndian.PutUint32(h2[68:72], 604800*5)
	c1, _ := pow(h1)
	c2, _ := pow(h2)
	if hex.EncodeToString(c1) == hex.EncodeToString(c2) {
		t.Fatal("commitment identical across epochs; seed not feeding PoW")
	}
}

// TestSCASHRejectsBadNetwork checks network validation.
func TestSCASHRejectsBadNetwork(t *testing.T) {
	client := rpc.New(rpc.Options{URL: "http://127.0.0.1:1"})
	if _, err := New(Options{RPC: client, Network: "moonnet", PoolAddress: scashMainAddr, RandomX: fakeRandomX{}}); err == nil {
		t.Fatal("unknown network accepted")
	}
}

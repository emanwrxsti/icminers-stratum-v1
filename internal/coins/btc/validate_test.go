package btc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// easyTemplate uses the regtest-style compact target 207fffff, under which
// almost any hash qualifies — letting tests "mine" real block candidates in a
// handful of nonce attempts.
func easyTemplate() map[string]any {
	tpl := validTemplate()
	tpl["bits"] = "207fffff"
	return tpl
}

// buildJob fetches a template from the fake daemon and derives a stamped job.
func buildJob(t *testing.T, a *Adapter, f *fakeCore) *coins.MiningJob {
	t.Helper()
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
	return job
}

// mineShare brute-forces a nonce whose header hash meets the given target,
// using an INDEPENDENT (test-local) header construction so the adapter's
// reconstruction is checked against a second implementation.
func mineShare(t *testing.T, job *coins.MiningJob, en1, en2 []byte, workerDiff float64) (nonce uint32, ntimeHex string) {
	t.Helper()
	base, err := BitcoinbaseJob(job)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := base.CoinbaseParts.Assemble(en1, en2)
	if err != nil {
		t.Fatal(err)
	}
	root := bitcoinbase.FoldBranch(bitcoinbase.DoubleSHA256(cb), base.MerkleBranchLE)
	prevLE, _ := bitcoinbase.HashLEFromDisplayHex(base.PrevHashDisplay)
	bitsRaw, _ := hex.DecodeString(base.BitsHex)
	verRaw, _ := hex.DecodeString(base.VersionHex)

	target := vardiff.DifficultyToTarget(workerDiff)
	ntime := uint32(base.CurTime)

	header := make([]byte, 80)
	binary.LittleEndian.PutUint32(header[0:4], binary.BigEndian.Uint32(verRaw))
	copy(header[4:36], prevLE)
	copy(header[36:68], root)
	binary.LittleEndian.PutUint32(header[68:72], ntime)
	binary.LittleEndian.PutUint32(header[72:76], binary.BigEndian.Uint32(bitsRaw))

	for n := uint32(0); n < 5_000_000; n++ {
		binary.LittleEndian.PutUint32(header[76:80], n)
		h := vardiff.HashToBig(bitcoinbase.DoubleSHA256(header))
		if vardiff.MeetsTarget(h, target) {
			return n, fmt.Sprintf("%08x", ntime)
		}
	}
	t.Fatal("could not mine a share (target too hard for test)")
	return 0, ""
}

func TestValidateShareBlockCandidate(t *testing.T) {
	f := &fakeCore{t: t, template: easyTemplate()}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	job := buildJob(t, a, f)

	en1 := []byte{0x00, 0x00, 0x00, 0x01}
	en2 := []byte{0x00, 0x00, 0x00, 0x2a}
	// Worker diff so low that the mined share is also under the (huge) regtest
	// network target -> block candidate.
	nonce, ntime := mineShare(t, job, en1, en2, 1e-9)

	res, err := a.ValidateShare(context.Background(), job, coins.ShareSubmit{
		Worker:      "w.rig1",
		JobID:       "1",
		ExtraNonce1: hex.EncodeToString(en1),
		ExtraNonce2: hex.EncodeToString(en2),
		NTime:       ntime,
		Nonce:       fmt.Sprintf("%08x", nonce),
		WorkerDiff:  1e-9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Fatal("mined share reported invalid")
	}
	if !res.BlockCandidate {
		t.Fatal("share under regtest network target must be a block candidate")
	}
	if res.ShareDiff <= 0 {
		t.Fatalf("shareDiff = %g", res.ShareDiff)
	}

	// --- verify the assembled block ---
	raw, err := hex.DecodeString(res.BlockHex)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 81 {
		t.Fatalf("block is %d bytes", len(raw))
	}
	header := raw[:80]
	// Header hash must equal the reported block hash.
	gotHash := hex.EncodeToString(bitcoinbase.ReverseBytes(bitcoinbase.DoubleSHA256(header)))
	if gotHash != res.BlockHash {
		t.Fatalf("header hash %s != reported %s", gotHash, res.BlockHash)
	}
	// tx count: coinbase + 2 template txs.
	if raw[80] != 0x03 {
		t.Fatalf("tx count = %d, want 3", raw[80])
	}
	// Template has a witness commitment -> coinbase must be witness-serialized
	// (marker 00, flag 01 right after the version).
	if raw[81+4] != 0x00 || raw[81+5] != 0x01 {
		t.Fatal("coinbase in block missing segwit marker/flag")
	}
	// Template tx data ("aa", "bb") must trail the block.
	if !bytes.HasSuffix(raw, []byte{0xaa, 0xbb}) {
		t.Fatal("template transactions not appended in order")
	}
}

func TestValidateShareLowDifficulty(t *testing.T) {
	f := &fakeCore{t: t, template: validTemplate()} // bits 1d00ffff (diff 1)
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	job := buildJob(t, a, f)

	// An arbitrary nonce will essentially never meet a diff-1e12 share target.
	res, err := a.ValidateShare(context.Background(), job, coins.ShareSubmit{
		JobID:       "1",
		ExtraNonce1: "00000001",
		ExtraNonce2: "00000000",
		NTime:       fmt.Sprintf("%08x", 1700000600),
		Nonce:       "00000000",
		WorkerDiff:  1e12,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid || res.BlockCandidate {
		t.Fatal("garbage share validated")
	}
}

func TestValidateShareVersionRolling(t *testing.T) {
	f := &fakeCore{t: t, template: easyTemplate()}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	job := buildJob(t, a, f)
	ctx := context.Background()

	// Bits outside the advertised mask must be rejected outright.
	_, err := a.ValidateShare(ctx, job, coins.ShareSubmit{
		JobID:       "1",
		ExtraNonce1: "00000001",
		ExtraNonce2: "00000000",
		NTime:       fmt.Sprintf("%08x", 1700000600),
		Nonce:       "00000000",
		VersionBits: "00000001", // bit 0 is outside 1fffe000
		WorkerDiff:  1e-9,
	})
	if err == nil {
		t.Fatal("version bits outside mask accepted")
	}

	// Bits inside the mask must change the header version: validate the same
	// nonce with and without rolling and confirm the share diffs differ
	// (different headers hash differently).
	sub := coins.ShareSubmit{
		JobID:       "1",
		ExtraNonce1: "00000001",
		ExtraNonce2: "00000000",
		NTime:       fmt.Sprintf("%08x", 1700000600),
		Nonce:       "00000000",
		WorkerDiff:  1e-12, // accept anything; we only compare hashes
	}
	plain, err := a.ValidateShare(ctx, job, sub)
	if err != nil {
		t.Fatal(err)
	}
	sub.VersionBits = "1fffe000"
	rolled, err := a.ValidateShare(ctx, job, sub)
	if err != nil {
		t.Fatal(err)
	}
	if plain.ShareDiff == rolled.ShareDiff {
		t.Fatal("version rolling did not change the header")
	}
}

func TestValidateShareGuards(t *testing.T) {
	f := &fakeCore{t: t, template: validTemplate()}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	job := buildJob(t, a, f)
	ctx := context.Background()

	good := coins.ShareSubmit{
		JobID:       "1",
		ExtraNonce1: "00000001",
		ExtraNonce2: "00000000",
		NTime:       fmt.Sprintf("%08x", 1700000600),
		Nonce:       "00000000",
		WorkerDiff:  1e-12,
	}

	cases := map[string]func(s coins.ShareSubmit) coins.ShareSubmit{
		"short extranonce2": func(s coins.ShareSubmit) coins.ShareSubmit { s.ExtraNonce2 = "00"; return s },
		"bad extranonce2":   func(s coins.ShareSubmit) coins.ShareSubmit { s.ExtraNonce2 = "zzzzzzzz"; return s },
		"short ntime":       func(s coins.ShareSubmit) coins.ShareSubmit { s.NTime = "ff"; return s },
		"ntime before mintime": func(s coins.ShareSubmit) coins.ShareSubmit {
			s.NTime = fmt.Sprintf("%08x", 1699999999)
			return s
		},
		"ntime too future": func(s coins.ShareSubmit) coins.ShareSubmit {
			s.NTime = fmt.Sprintf("%08x", 1700000600+7201)
			return s
		},
		"bad nonce": func(s coins.ShareSubmit) coins.ShareSubmit { s.Nonce = "xyz"; return s },
	}
	for name, mutate := range cases {
		if _, err := a.ValidateShare(ctx, job, mutate(good)); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
	// The unmutated submit must pass the guards (validity aside).
	if _, err := a.ValidateShare(ctx, job, good); err != nil {
		t.Fatalf("control submit errored: %v", err)
	}
}

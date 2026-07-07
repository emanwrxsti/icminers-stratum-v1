package jobs

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/btc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// fakeSubmitDaemon serves an easy (regtest-bits) template and records
// submitblock calls.
type fakeSubmitDaemon struct {
	mu        sync.Mutex
	template  map[string]any
	submitted []string
}

func (f *fakeSubmitDaemon) submittedBlocks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.submitted...)
}

func (f *fakeSubmitDaemon) server() *httptest.Server {
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
			f.mu.Lock()
			tpl := make(map[string]any, len(f.template))
			for k, v := range f.template {
				tpl[k] = v
			}
			f.mu.Unlock()
			result = tpl
		case "submitblock":
			var blockHex string
			if len(req.Params) > 0 {
				_ = json.Unmarshal(req.Params[0], &blockHex)
			}
			f.mu.Lock()
			f.submitted = append(f.submitted, blockHex)
			f.mu.Unlock()
			result = nil
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID, "result": result, "error": nil})
	}))
}

func easyBaseTemplate() map[string]any {
	tpl := baseTemplate()
	tpl["bits"] = "207fffff" // regtest-style: nearly any hash is a block
	return tpl
}

// mineForJob brute-forces a nonce for the manager's current job meeting the
// worker's share target, computed with a test-local header construction.
func mineForJob(t *testing.T, m *Manager, jobID string, en1, en2 []byte, workerDiff float64) (nonceHex, ntimeHex string) {
	t.Helper()
	job, ok := m.Get(jobID)
	if !ok {
		t.Fatalf("job %q not found", jobID)
	}
	base, err := btc.BitcoinbaseJob(job)
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
		if vardiff.MeetsTarget(vardiff.HashToBig(bitcoinbase.DoubleSHA256(header)), target) {
			return fmt.Sprintf("%08x", n), fmt.Sprintf("%08x", ntime)
		}
	}
	t.Fatal("could not mine a test share")
	return "", ""
}

func currentJobID(t *testing.T, m *Manager) string {
	t.Helper()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil {
		t.Fatal("no current job")
	}
	return m.current.JobID
}

func TestSubmitShareAcceptAndBlockSubmit(t *testing.T) {
	f := &fakeSubmitDaemon{template: easyBaseTemplate()}
	srv := f.server()
	defer srv.Close()
	m := newTestManager(t, srv.URL)
	ctx := context.Background()
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := currentJobID(t, m)

	en1 := []byte{0, 0, 0, 1}
	en2 := []byte{0, 0, 0, 9}
	nonce, ntime := mineForJob(t, m, jobID, en1, en2, 1e-9)

	res, err := m.SubmitShare(ctx, coins.ShareSubmit{
		Worker:      "addr.rig1",
		JobID:       jobID,
		ExtraNonce1: hex.EncodeToString(en1),
		ExtraNonce2: hex.EncodeToString(en2),
		NTime:       ntime,
		Nonce:       nonce,
		WorkerDiff:  1e-9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid || !res.BlockCandidate {
		t.Fatalf("result = %+v", res)
	}

	// The block candidate must reach the daemon (async, so poll briefly).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if blocks := f.submittedBlocks(); len(blocks) == 1 {
			if blocks[0] != res.BlockHex {
				t.Fatal("submitted block differs from validated block")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("submitblock never reached the daemon")
}

func TestSubmitShareDuplicate(t *testing.T) {
	f := &fakeSubmitDaemon{template: easyBaseTemplate()}
	srv := f.server()
	defer srv.Close()
	m := newTestManager(t, srv.URL)
	ctx := context.Background()
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := currentJobID(t, m)
	en1 := []byte{0, 0, 0, 1}
	en2 := []byte{0, 0, 0, 7}
	nonce, ntime := mineForJob(t, m, jobID, en1, en2, 1e-9)
	sub := coins.ShareSubmit{
		JobID:       jobID,
		ExtraNonce1: hex.EncodeToString(en1),
		ExtraNonce2: hex.EncodeToString(en2),
		NTime:       ntime,
		Nonce:       nonce,
		WorkerDiff:  1e-9,
	}
	if _, err := m.SubmitShare(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SubmitShare(ctx, sub); !errors.Is(err, ErrDuplicateShare) {
		t.Fatalf("second submit err = %v, want ErrDuplicateShare", err)
	}
	// A different extranonce2 is NOT a duplicate.
	sub.ExtraNonce2 = "0000002a"
	if _, err := m.SubmitShare(ctx, sub); errors.Is(err, ErrDuplicateShare) {
		t.Fatal("distinct extranonce2 flagged as duplicate")
	}
}

func TestSubmitShareStaleJob(t *testing.T) {
	f := &fakeSubmitDaemon{template: easyBaseTemplate()}
	srv := f.server()
	defer srv.Close()
	m := newTestManager(t, srv.URL)
	ctx := context.Background()
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := m.SubmitShare(ctx, coins.ShareSubmit{JobID: "no-such-job"})
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("err = %v, want ErrJobNotFound", err)
	}
}

func TestSubmitShareLowDifficulty(t *testing.T) {
	tpl := baseTemplate() // real diff-1 bits: garbage nonce cannot meet diff 1e12
	f := &fakeSubmitDaemon{template: tpl}
	srv := f.server()
	defer srv.Close()
	m := newTestManager(t, srv.URL)
	ctx := context.Background()
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := currentJobID(t, m)
	_, err := m.SubmitShare(ctx, coins.ShareSubmit{
		JobID:       jobID,
		ExtraNonce1: "00000001",
		ExtraNonce2: "00000000",
		NTime:       fmt.Sprintf("%08x", 1700000600),
		Nonce:       "00000000",
		WorkerDiff:  1e12,
	})
	if !errors.Is(err, ErrLowDifficulty) {
		t.Fatalf("err = %v, want ErrLowDifficulty", err)
	}
}

func TestRegistrySubmitUnknownPool(t *testing.T) {
	r := NewRegistry()
	_, err := r.SubmitShare(context.Background(), "ghost", coins.ShareSubmit{JobID: "1"})
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("err = %v, want ErrJobNotFound", err)
	}
}

func TestDupCacheEvictedWithJob(t *testing.T) {
	f := &fakeSubmitDaemon{template: easyBaseTemplate()}
	srv := f.server()
	defer srv.Close()
	m := newTestManager(t, srv.URL)
	ctx := context.Background()
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	firstJob := currentJobID(t, m)
	en1 := []byte{0, 0, 0, 1}
	en2 := []byte{0, 0, 0, 3}
	nonce, ntime := mineForJob(t, m, firstJob, en1, en2, 1e-9)
	if _, err := m.SubmitShare(ctx, coins.ShareSubmit{
		JobID: firstJob, ExtraNonce1: hex.EncodeToString(en1), ExtraNonce2: hex.EncodeToString(en2),
		NTime: ntime, Nonce: nonce, WorkerDiff: 1e-9,
	}); err != nil {
		t.Fatal(err)
	}
	// Roll enough new jobs to evict the first.
	for i := 0; i < maxRememberedJobs+1; i++ {
		f.mu.Lock()
		f.template["curtime"] = 1700000600 + i + 1
		f.mu.Unlock()
		if err := m.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	m.mu.RLock()
	_, dupAlive := m.dupByJob[firstJob]
	m.mu.RUnlock()
	if dupAlive {
		t.Fatal("dup cache for evicted job not released")
	}
}

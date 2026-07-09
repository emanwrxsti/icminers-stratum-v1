package payouts

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

func testLogger() *logging.Logger { return logging.New(logging.Options{Level: "error"}) }

func TestSatsToAmountString(t *testing.T) {
	cases := map[int64]string{
		0:         "0.00000000",
		1:         "0.00000001",
		100000000: "1.00000000",
		312500000: "3.12500000",
		100000:    "0.00100000",
		123456789: "1.23456789",
		-5:        "-0.00000005",
	}
	for sats, want := range cases {
		if got := SatsToAmountString(sats); got != want {
			t.Errorf("SatsToAmountString(%d) = %q, want %q", sats, got, want)
		}
	}
}

// fakeStore records what the processor drives.
type fakeStore struct {
	balances map[string]int64 // miner -> sats
	addrs    map[string]string
	batches  map[string][]Payment // batchID -> payments
	sent     map[string]string    // batchID -> txid
	refunded map[string]bool
	stuck    []string
	failMark bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		balances: map[string]int64{},
		addrs:    map[string]string{},
		batches:  map[string][]Payment{},
		sent:     map[string]string{},
		refunded: map[string]bool{},
	}
}

func (f *fakeStore) BeginPayout(_ context.Context, poolID string, minSats int64, batchID string, validate func(string) (string, bool)) ([]Payment, error) {
	var batch []Payment
	// deterministic order
	miners := []string{}
	for m := range f.balances {
		miners = append(miners, m)
	}
	for i := 0; i < len(miners); i++ {
		for j := i + 1; j < len(miners); j++ {
			if miners[j] < miners[i] {
				miners[i], miners[j] = miners[j], miners[i]
			}
		}
	}
	for _, m := range miners {
		sats := f.balances[m]
		if sats < minSats {
			continue
		}
		addr, ok := validate(m)
		if !ok {
			continue
		}
		f.balances[m] = 0
		batch = append(batch, Payment{Miner: m, Address: addr, AmountSats: sats, BatchID: batchID})
	}
	if len(batch) > 0 {
		f.batches[batchID] = batch
	}
	return batch, nil
}

func (f *fakeStore) MarkPaymentsSent(_ context.Context, batchID, txid string) error {
	if f.failMark {
		return errors.New("mark failed")
	}
	f.sent[batchID] = txid
	return nil
}

func (f *fakeStore) RefundBatch(_ context.Context, batchID string) error {
	for _, p := range f.batches[batchID] {
		f.balances[p.Miner] += p.AmountSats
	}
	f.refunded[batchID] = true
	return nil
}

func (f *fakeStore) StuckBatches(_ context.Context, _ string) ([]string, error) {
	return f.stuck, nil
}

// fakeWallet captures the last SendMany call.
type fakeWallet struct {
	txid        string
	err         error
	lastOutputs map[string]int64
	lastSubFee  bool
	calls       int
}

func (w *fakeWallet) SendMany(_ context.Context, outputs map[string]int64, subFee bool) (string, error) {
	w.calls++
	w.lastOutputs = outputs
	w.lastSubFee = subFee
	return w.txid, w.err
}

type fakeValidator struct{ bad map[string]bool }

func (v fakeValidator) NormalizeAddress(a string) (string, error) {
	if v.bad[a] {
		return "", fmt.Errorf("invalid address %q", a)
	}
	return "addr:" + a, nil
}

func newProc(store *fakeStore, wallet *fakeWallet, val AddressValidator, subFee bool) *Processor {
	return NewProcessor(ProcessorOptions{
		PoolID:      "p1",
		MinSats:     100000,
		SubtractFee: subFee,
		Wallet:      wallet,
		Store:       store,
		Validator:   val,
		Log:         testLogger(),
		NewBatchID:  func() string { return "batch-1" },
	})
}

func TestPassSuccess(t *testing.T) {
	store := newFakeStore()
	store.balances["alice"] = 500000
	store.balances["bob"] = 200000
	store.balances["carol"] = 50000 // under threshold
	wallet := &fakeWallet{txid: "txabc"}
	proc := newProc(store, wallet, fakeValidator{}, true)

	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wallet.calls != 1 {
		t.Fatalf("wallet calls = %d", wallet.calls)
	}
	if store.sent["batch-1"] != "txabc" {
		t.Fatalf("batch not marked sent: %v", store.sent)
	}
	// alice+bob paid to normalized addresses; carol retained.
	if wallet.lastOutputs["addr:alice"] != 500000 || wallet.lastOutputs["addr:bob"] != 200000 {
		t.Fatalf("outputs = %v", wallet.lastOutputs)
	}
	if _, ok := wallet.lastOutputs["addr:carol"]; ok {
		t.Fatal("under-threshold miner paid")
	}
	if store.balances["alice"] != 0 || store.balances["bob"] != 0 {
		t.Fatal("paid balances not zeroed")
	}
	if store.balances["carol"] != 50000 {
		t.Fatal("carol balance changed")
	}
	if !wallet.lastSubFee {
		t.Fatal("subtractFee not passed")
	}
}

func TestPassRefundsOnSendFailure(t *testing.T) {
	store := newFakeStore()
	store.balances["alice"] = 500000
	wallet := &fakeWallet{err: errors.New("daemon down")}
	proc := newProc(store, wallet, fakeValidator{}, true)

	err := proc.Pass(context.Background())
	if err == nil {
		t.Fatal("expected error on send failure")
	}
	if !store.refunded["batch-1"] {
		t.Fatal("batch not refunded")
	}
	if store.balances["alice"] != 500000 {
		t.Fatalf("alice not made whole: %d", store.balances["alice"])
	}
}

func TestPassInvalidAddressSkipped(t *testing.T) {
	store := newFakeStore()
	store.balances["alice"] = 500000
	store.balances["baddr"] = 500000
	wallet := &fakeWallet{txid: "tx1"}
	proc := newProc(store, wallet, fakeValidator{bad: map[string]bool{"baddr": true}}, true)

	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := wallet.lastOutputs["addr:alice"]; !ok {
		t.Fatal("alice not paid")
	}
	if len(wallet.lastOutputs) != 1 {
		t.Fatalf("outputs = %v (bad address should be skipped)", wallet.lastOutputs)
	}
	if store.balances["baddr"] != 500000 {
		t.Fatal("invalid-address balance not retained")
	}
}

func TestPassNoPayableIsNoop(t *testing.T) {
	store := newFakeStore()
	store.balances["alice"] = 50000 // under threshold
	wallet := &fakeWallet{txid: "tx1"}
	proc := newProc(store, wallet, fakeValidator{}, true)
	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wallet.calls != 0 {
		t.Fatal("wallet called with nothing payable")
	}
}

func TestPassMarkFailureSurfaces(t *testing.T) {
	store := newFakeStore()
	store.balances["alice"] = 500000
	store.failMark = true
	wallet := &fakeWallet{txid: "txsent"}
	proc := newProc(store, wallet, fakeValidator{}, true)
	err := proc.Pass(context.Background())
	if err == nil {
		t.Fatal("mark failure must surface (money moved, rows not finalized)")
	}
	// NOT refunded: the tx went out.
	if store.refunded["batch-1"] {
		t.Fatal("batch refunded despite successful send")
	}
}

func TestStuckBatchesSurfaced(t *testing.T) {
	store := newFakeStore()
	store.stuck = []string{"old-batch-1", "old-batch-2"}
	wallet := &fakeWallet{txid: "tx1"}
	proc := newProc(store, wallet, fakeValidator{}, true)
	// No payable balances, but the pass must still succeed and surface stuck.
	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestBitcoindWalletParams verifies the exact sendmany param shape.
type captureRPC struct {
	method string
	params any
	txid   string
}

func (c *captureRPC) Call(_ context.Context, method string, params any, result any) error {
	c.method = method
	c.params = params
	if s, ok := result.(*string); ok {
		*s = c.txid
	}
	return nil
}

func TestBitcoindWalletSendMany(t *testing.T) {
	rpc := &captureRPC{txid: "deadbeef"}
	w := &BitcoindWallet{RPC: rpc}
	txid, err := w.SendMany(context.Background(), map[string]int64{
		"bc1qalice": 500000, "bc1qbob": 312500000,
	}, true)
	if err != nil || txid != "deadbeef" {
		t.Fatalf("txid=%q err=%v", txid, err)
	}
	if rpc.method != "sendmany" {
		t.Fatalf("method = %s", rpc.method)
	}
	params := rpc.params.([]any)
	if len(params) != 5 {
		t.Fatalf("params len = %d, want 5 (with subtractfeefrom)", len(params))
	}
	if params[0] != "" || params[2] != 1 || params[3] != "gostratum payout" {
		t.Fatalf("params = %v", params)
	}
	amounts := params[1].(map[string]string)
	if amounts["bc1qalice"] != "0.00500000" || amounts["bc1qbob"] != "3.12500000" {
		t.Fatalf("amounts = %v (must be exact strings)", amounts)
	}
	subtract := params[4].([]string)
	if len(subtract) != 2 {
		t.Fatalf("subtractfeefrom = %v", subtract)
	}

	// Without subtractFee: only 4 params.
	rpc2 := &captureRPC{txid: "beef"}
	w2 := &BitcoindWallet{RPC: rpc2}
	if _, err := w2.SendMany(context.Background(), map[string]int64{"a": 100}, false); err != nil {
		t.Fatal(err)
	}
	if len(rpc2.params.([]any)) != 4 {
		t.Fatalf("no-subfee params = %v", rpc2.params)
	}

	// Empty outputs error.
	if _, err := w.SendMany(context.Background(), nil, true); err == nil {
		t.Fatal("empty outputs must error")
	}
	// Empty txid from daemon is an error.
	rpc3 := &captureRPC{txid: ""}
	w3 := &BitcoindWallet{RPC: rpc3}
	if _, err := w3.SendMany(context.Background(), map[string]int64{"a": 100}, false); err == nil {
		t.Fatal("empty txid must error")
	}
}

package postgres

import (
	"context"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/payouts"
)

// setBalance directly seeds a miner balance for payout tests.
func setBalance(t *testing.T, s *Store, poolID, miner string, sats int64) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), `
		INSERT INTO balances (poolid, miner, amount_sats, updated)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (poolid, miner)
		DO UPDATE SET amount_sats = EXCLUDED.amount_sats`, poolID, miner, sats); err != nil {
		t.Fatal(err)
	}
}

func passAll(miner string) (string, bool) { return "addr:" + miner, true }

func TestBeginPayoutThresholdAndDeduction(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "alice", 500000)
	setBalance(t, s, "pp", "bob", 200000)
	setBalance(t, s, "pp", "carol", 50000) // under threshold

	batch, err := s.BeginPayout(ctx, "pp", 100000, "b1", passAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch size = %d, want 2", len(batch))
	}
	byMiner := map[string]payouts.Payment{}
	for _, p := range batch {
		byMiner[p.Miner] = p
	}
	if byMiner["alice"].AmountSats != 500000 || byMiner["alice"].Address != "addr:alice" {
		t.Fatalf("alice payment = %+v", byMiner["alice"])
	}
	// Paid balances are zeroed; carol retained.
	if sats, _ := s.MinerBalance(ctx, "pp", "alice"); sats != 0 {
		t.Fatalf("alice balance = %d", sats)
	}
	if sats, _ := s.MinerBalance(ctx, "pp", "carol"); sats != 50000 {
		t.Fatalf("carol balance = %d", sats)
	}
	// balance_changes has 'payout' rows (negative).
	var neg int64
	if err := s.Pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_sats),0) FROM balance_changes WHERE poolid='pp' AND usage='payout'`).Scan(&neg); err != nil {
		t.Fatal(err)
	}
	if neg != -700000 {
		t.Fatalf("payout changes sum = %d, want -700000", neg)
	}
	// payments rows are 'sending'.
	pays, _ := s.ListPayments(ctx, "pp", "", 10, 0)
	if len(pays) != 2 || pays[0].Status != "sending" {
		t.Fatalf("payments = %+v", pays)
	}
}

func TestBeginPayoutSkipsInvalidAddress(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "alice", 500000)
	setBalance(t, s, "pp", "baddr", 500000)

	validate := func(miner string) (string, bool) {
		if miner == "baddr" {
			return "", false
		}
		return "addr:" + miner, true
	}
	batch, err := s.BeginPayout(ctx, "pp", 100000, "b1", validate)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].Miner != "alice" {
		t.Fatalf("batch = %+v", batch)
	}
	// baddr keeps its balance and gets no payment row.
	if sats, _ := s.MinerBalance(ctx, "pp", "baddr"); sats != 500000 {
		t.Fatalf("baddr balance = %d", sats)
	}
}

func TestBeginPayoutEmptyRollsBack(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "carol", 50000) // under threshold
	batch, err := s.BeginPayout(ctx, "pp", 100000, "b1", passAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 0 {
		t.Fatalf("batch = %+v", batch)
	}
	pays, _ := s.ListPayments(ctx, "pp", "", 10, 0)
	if len(pays) != 0 {
		t.Fatal("payment rows created for empty batch")
	}
}

func TestMarkPaymentsSent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "alice", 500000)
	if _, err := s.BeginPayout(ctx, "pp", 100000, "b1", passAll); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPaymentsSent(ctx, "b1", "txhash123"); err != nil {
		t.Fatal(err)
	}
	pays, _ := s.ListPayments(ctx, "pp", "alice", 10, 0)
	if len(pays) != 1 || pays[0].Status != "sent" || pays[0].TxID != "txhash123" {
		t.Fatalf("payment = %+v", pays)
	}
	// Marking a batch with no sending rows errors.
	if err := s.MarkPaymentsSent(ctx, "nonexistent", "x"); err == nil {
		t.Fatal("marking empty batch should error")
	}
}

func TestRefundBatchReCreditsExactly(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "alice", 500000)
	setBalance(t, s, "pp", "bob", 200000)
	if _, err := s.BeginPayout(ctx, "pp", 100000, "b1", passAll); err != nil {
		t.Fatal(err)
	}
	// Both deducted to zero.
	if sats, _ := s.MinerBalance(ctx, "pp", "alice"); sats != 0 {
		t.Fatalf("alice pre-refund = %d", sats)
	}
	if err := s.RefundBatch(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if sats, _ := s.MinerBalance(ctx, "pp", "alice"); sats != 500000 {
		t.Fatalf("alice refunded = %d", sats)
	}
	if sats, _ := s.MinerBalance(ctx, "pp", "bob"); sats != 200000 {
		t.Fatalf("bob refunded = %d", sats)
	}
	pays, _ := s.ListPayments(ctx, "pp", "", 10, 0)
	for _, p := range pays {
		if p.Status != "failed" {
			t.Fatalf("payment status = %s, want failed", p.Status)
		}
	}
	// Conservation: payout + payout-refund changes net to zero.
	var net int64
	if err := s.Pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_sats),0) FROM balance_changes WHERE poolid='pp' AND usage LIKE 'payout%'`).Scan(&net); err != nil {
		t.Fatal(err)
	}
	if net != 0 {
		t.Fatalf("payout+refund net = %d, want 0", net)
	}
	// Refunding again is a no-op (no sending rows left).
	if err := s.RefundBatch(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if sats, _ := s.MinerBalance(ctx, "pp", "alice"); sats != 500000 {
		t.Fatalf("double refund: alice = %d", sats)
	}
}

func TestStuckBatchesAgeFilter(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	setBalance(t, s, "pp", "alice", 500000)
	if _, err := s.BeginPayout(ctx, "pp", 100000, "fresh", passAll); err != nil {
		t.Fatal(err)
	}
	// A fresh batch is NOT stuck (created just now, inside the grace window).
	stuck, err := s.StuckBatches(ctx, "pp")
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 0 {
		t.Fatalf("fresh batch reported stuck: %v", stuck)
	}
	// Backdate it past the grace window.
	if _, err := s.Pool.Exec(ctx,
		`UPDATE payments SET created = now() - interval '10 minutes' WHERE batch_id='fresh'`); err != nil {
		t.Fatal(err)
	}
	stuck, err = s.StuckBatches(ctx, "pp")
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 1 || stuck[0] != "fresh" {
		t.Fatalf("stuck = %v, want [fresh]", stuck)
	}
	// Once marked sent, it is no longer stuck.
	if err := s.MarkPaymentsSent(ctx, "fresh", "tx"); err != nil {
		t.Fatal(err)
	}
	stuck, _ = s.StuckBatches(ctx, "pp")
	if len(stuck) != 0 {
		t.Fatalf("sent batch still stuck: %v", stuck)
	}
}

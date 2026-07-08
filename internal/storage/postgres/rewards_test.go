package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/rewards"
)

func seedRewardData(t *testing.T, s *Store, poolID string, now time.Time) rewards.Block {
	t.Helper()
	ctx := context.Background()
	if err := s.EnsureSharePartition(ctx, now); err != nil {
		t.Fatal(err)
	}
	w := &ShareWriter{store: s}
	recs := []ShareRecord{
		{PoolID: poolID, BlockHeight: 99, Difficulty: 60, NetworkDifficulty: 100, Miner: "alice", Worker: "r1", Created: now.Add(-9 * time.Minute)},
		{PoolID: poolID, BlockHeight: 99, Difficulty: 40, NetworkDifficulty: 100, Miner: "bob", Worker: "r1", Created: now.Add(-8 * time.Minute)},
		{PoolID: poolID, BlockHeight: 100, Difficulty: 100, NetworkDifficulty: 100, Miner: "carol", Worker: "r1", Created: now.Add(-2 * time.Minute)},
	}
	if err := w.copySharesCtx(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertBlock(ctx, BlockRecord{
		PoolID: poolID, BlockHeight: 100, NetworkDifficulty: 100,
		Miner: "carol", Worker: "r1", Hash: "cafe00", RewardSats: 312500000,
		Source: "us", Created: now.Add(-1 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	blocks, err := s.UnconfirmedBlocks(ctx, poolID)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("unconfirmed = %d", len(blocks))
	}
	b := blocks[0]
	if b.RewardSats != 312500000 || b.Miner != "carol" || b.Hash != "cafe00" {
		t.Fatalf("block = %+v", b)
	}
	return b
}

func TestRewardStoreQueries(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	b := seedRewardData(t, s, "r-pool", now)

	// WorkBetween isolates the round.
	work, err := s.WorkBetween(ctx, "r-pool", now.Add(-10*time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]float64{}
	for _, w := range work {
		sums[w.Miner] = w.DiffSum
	}
	if sums["alice"] != 60 || sums["bob"] != 40 || sums["carol"] != 100 {
		t.Fatalf("work = %v", sums)
	}

	// WorkBackward stops once the target is collected (target 100 -> carol
	// alone fills it; alice/bob excluded).
	work, err = s.WorkBackward(ctx, "r-pool", now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].Miner != "carol" || work[0].DiffSum != 100 {
		t.Fatalf("backward = %v", work)
	}
	// Larger target reaches further back.
	work, err = s.WorkBackward(ctx, "r-pool", now, 150)
	if err != nil {
		t.Fatal(err)
	}
	sums = map[string]float64{}
	for _, w := range work {
		sums[w.Miner] = w.DiffSum
	}
	if sums["carol"] != 100 || sums["bob"] != 40 {
		t.Fatalf("backward-150 = %v", sums)
	}

	// PreviousBlockTime: none before this block.
	if _, ok, err := s.PreviousBlockTime(ctx, "r-pool", b.Created); err != nil || ok {
		t.Fatalf("prev = ok=%v err=%v", ok, err)
	}

	// Confirmation update flows.
	if err := s.UpdateBlockConfirmation(ctx, b.ID, "pending", 40, 0.4); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateBlockConfirmation(ctx, b.ID, "confirmed", 100, 1); err != nil {
		t.Fatal(err)
	}
	unrewarded, err := s.ConfirmedUnrewarded(ctx, "r-pool")
	if err != nil || len(unrewarded) != 1 {
		t.Fatalf("unrewarded = %v err=%v", unrewarded, err)
	}
}

func TestCreditBlockRewardsAtomicAndIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	b := seedRewardData(t, s, "c-pool", now)
	if err := s.UpdateBlockConfirmation(ctx, b.ID, "confirmed", 100, 1); err != nil {
		t.Fatal(err)
	}

	credits := []rewards.Credit{
		{Miner: "alice", AmountSats: 200000000},
		{Miner: "bob", AmountSats: 109375000},
	}
	if err := s.CreditBlockRewards(ctx, b, credits, 3125000); err != nil {
		t.Fatal(err)
	}
	// Balances written.
	if sats, _ := s.MinerBalance(ctx, "c-pool", "alice"); sats != 200000000 {
		t.Fatalf("alice = %d", sats)
	}
	if sats, _ := s.MinerBalance(ctx, "c-pool", "bob"); sats != 109375000 {
		t.Fatalf("bob = %d", sats)
	}
	if sats, _ := s.MinerBalance(ctx, "c-pool", "ghost"); sats != 0 {
		t.Fatalf("ghost = %d", sats)
	}
	// Block no longer unrewarded.
	if list, _ := s.ConfirmedUnrewarded(ctx, "c-pool"); len(list) != 0 {
		t.Fatalf("still unrewarded: %v", list)
	}
	// Idempotence: a second credit call is a no-op.
	if err := s.CreditBlockRewards(ctx, b, credits, 3125000); err != nil {
		t.Fatal(err)
	}
	if sats, _ := s.MinerBalance(ctx, "c-pool", "alice"); sats != 200000000 {
		t.Fatalf("double credit: alice = %d", sats)
	}
	// Audit trail: 2 reward rows + 1 fee row.
	var changes int
	if err := s.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM balance_changes WHERE poolid = 'c-pool'`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 3 {
		t.Fatalf("balance_changes = %d, want 3", changes)
	}
	// Accumulation: credit a second block to alice.
	if err := s.InsertBlock(ctx, BlockRecord{
		PoolID: "c-pool", BlockHeight: 101, Miner: "alice", Hash: "cafe01",
		RewardSats: 100, NetworkDifficulty: 100, Created: now,
	}); err != nil {
		t.Fatal(err)
	}
	blocks, _ := s.UnconfirmedBlocks(ctx, "c-pool")
	if len(blocks) != 1 {
		t.Fatalf("blocks = %v", blocks)
	}
	b2 := blocks[0]
	_ = s.UpdateBlockConfirmation(ctx, b2.ID, "confirmed", 100, 1)
	if err := s.CreditBlockRewards(ctx, b2, []rewards.Credit{{Miner: "alice", AmountSats: 100}}, 0); err != nil {
		t.Fatal(err)
	}
	if sats, _ := s.MinerBalance(ctx, "c-pool", "alice"); sats != 200000100 {
		t.Fatalf("accumulated alice = %d", sats)
	}
}

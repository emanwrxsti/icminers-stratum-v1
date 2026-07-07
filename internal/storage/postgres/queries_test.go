package postgres

import (
	"context"
	"testing"
	"time"
)

func seedShares(t *testing.T, s *Store, poolID string, now time.Time) {
	t.Helper()
	if err := s.EnsureSharePartition(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	recs := []ShareRecord{
		{PoolID: poolID, BlockHeight: 1, Difficulty: 100, NetworkDifficulty: 1, Miner: "alice", Worker: "r1", Created: now.Add(-10 * time.Minute)},
		{PoolID: poolID, BlockHeight: 1, Difficulty: 300, NetworkDifficulty: 1, Miner: "alice", Worker: "r2", Created: now.Add(-5 * time.Minute)},
		{PoolID: poolID, BlockHeight: 1, Difficulty: 50, NetworkDifficulty: 1, Miner: "bob", Worker: "x", Created: now.Add(-1 * time.Minute)},
		// Outside a 30m window:
		{PoolID: poolID, BlockHeight: 1, Difficulty: 9999, NetworkDifficulty: 1, Miner: "carol", Worker: "old", Created: now.Add(-2 * time.Hour)},
	}
	if err := s.EnsureSharePartition(context.Background(), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	w := &ShareWriter{store: s}
	if err := w.copySharesCtx(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
}

func TestTopMinersAndWorkers(t *testing.T) {
	s := testStore(t)
	now := time.Now().UTC()
	seedShares(t, s, "q-pool", now)
	ctx := context.Background()

	miners, err := s.TopMiners(ctx, "q-pool", 30*time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(miners) != 2 {
		t.Fatalf("miners = %d, want 2 (carol outside window)", len(miners))
	}
	if miners[0].Miner != "alice" || miners[0].DiffSum != 400 || miners[0].ShareCount != 2 {
		t.Fatalf("top miner = %+v", miners[0])
	}
	if miners[1].Miner != "bob" {
		t.Fatalf("second miner = %+v", miners[1])
	}
	// Hashrate = diffSum * 2^32 / windowSeconds.
	want := 400 * 4294967296.0 / (30 * 60.0)
	if miners[0].Hashrate != want {
		t.Fatalf("hashrate = %g, want %g", miners[0].Hashrate, want)
	}

	workers, err := s.MinerWorkers(ctx, "q-pool", "alice", 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 || workers[0].Worker != "r2" || workers[0].DiffSum != 300 {
		t.Fatalf("workers = %+v", workers)
	}
}

func TestListBlocksPagination(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	base := time.Now().UTC()
	for i := 0; i < 5; i++ {
		if err := s.InsertBlock(ctx, BlockRecord{
			PoolID: "q-pool", BlockHeight: int64(100 + i), Miner: "alice",
			Hash: string(rune('a'+i)) + "hash", Created: base.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	page1, err := s.ListBlocks(ctx, "q-pool", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].BlockHeight != 104 {
		t.Fatalf("page1 = %+v", page1)
	}
	page3, err := s.ListBlocks(ctx, "q-pool", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 || page3[0].BlockHeight != 100 {
		t.Fatalf("page3 = %+v", page3)
	}
	empty, err := s.ListBlocks(ctx, "no-such-pool", 10, 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty pool: %v %v", empty, err)
	}
}

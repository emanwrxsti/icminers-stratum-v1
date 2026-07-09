package postgres

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// countShares returns how many shares exist for a pool.
func countShares(t *testing.T, s *Store, poolID string) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM shares WHERE poolid = $1`, poolID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func mkShare(poolID, miner string, i int) ShareRecord {
	return ShareRecord{
		PoolID: poolID, BlockHeight: 1, Difficulty: 1, NetworkDifficulty: 1,
		Miner: miner, Worker: "w", Source: "us",
		Created: time.Now().UTC().Add(time.Duration(i) * time.Millisecond),
	}
}

// TestWALAppendDrainRoundtrip proves a share written to the WAL replays into
// the database via the recovery drain.
func TestWALAppendDrainRoundtrip(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "shares.wal")

	wal, err := openShareWAL(walPath, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSharePartition(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := wal.append(mkShare("wal-pool", "alice", i)); err != nil {
			t.Fatal(err)
		}
	}
	if wal.len() == 0 {
		t.Fatal("WAL empty after appends")
	}

	// Drain into the database.
	if err := wal.drain(func(recs []ShareRecord) error {
		return (&ShareWriter{store: s}).copySharesCtx(context.Background(), recs)
	}); err != nil {
		t.Fatal(err)
	}
	if got := countShares(t, s, "wal-pool"); got != 5 {
		t.Fatalf("shares in DB = %d, want 5", got)
	}
	if wal.len() != 0 {
		t.Fatalf("WAL not truncated after drain: %d bytes", wal.len())
	}
	_ = wal.close()
}

// TestWALSurvivesReopen proves records written before a crash are recovered
// when the WAL is reopened (the file persists).
func TestWALSurvivesReopen(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "shares.wal")
	if err := s.EnsureSharePartition(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	wal1, err := openShareWAL(walPath, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := wal1.append(mkShare("reopen-pool", "bob", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a crash: close without draining.
	_ = wal1.close()

	// Reopen and drain — the records must still be there.
	wal2, err := openShareWAL(walPath, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.close()
	if wal2.len() == 0 {
		t.Fatal("reopened WAL lost its records")
	}
	if err := wal2.drain(func(recs []ShareRecord) error {
		return (&ShareWriter{store: s}).copySharesCtx(context.Background(), recs)
	}); err != nil {
		t.Fatal(err)
	}
	if got := countShares(t, s, "reopen-pool"); got != 3 {
		t.Fatalf("recovered shares = %d, want 3", got)
	}
}

// TestWriterDivertsToWALInsteadOfDropping proves the integrated writer sends
// shares to the WAL (not the drop counter) when the in-memory queue is full,
// and that they end up in the database.
func TestWriterDivertsToWALInsteadOfDropping(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "shares.wal")

	// A tiny queue + a batch size larger than the queue makes the writer's
	// flush path lag, so Enqueue overflows and diverts to the WAL.
	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:        1,
		BatchSize:        1000,
		FlushInterval:    time.Hour, // never flush on timer during the burst
		WALPath:          walPath,
		WALDrainInterval: 100 * time.Millisecond,
	})

	const total = 2000
	for i := 0; i < total; i++ {
		w.Enqueue(mkShare("divert-pool", "carol", i))
	}
	diverted, _ := w.WALStats()
	if diverted == 0 {
		t.Fatal("no shares diverted to WAL despite an overflowing queue")
	}
	_, dropped := w.Stats()
	if dropped != 0 {
		t.Fatalf("shares dropped despite WAL: %d", dropped)
	}

	// The recovery loop + final Close drain must land every diverted share.
	// Wait for recovery to make progress, then Close for the final drain.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := w.WALStats(); b == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.Close()

	got := countShares(t, s, "divert-pool")
	if got != total {
		t.Fatalf("shares persisted = %d, want %d (durability hole!)", got, total)
	}
}

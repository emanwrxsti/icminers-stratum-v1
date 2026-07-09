package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// testStore connects using POOL_TEST_PG_DSN; tests are skipped without it so
// the suite stays green in databaseless environments.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("POOL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("POOL_TEST_PG_DSN not set; skipping postgres integration tests")
	}
	log := logging.New(logging.Options{Level: "error"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := New(ctx, dsn, log)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		// Isolate runs: drop everything this suite creates.
		_, _ = s.Pool.Exec(context.Background(),
			`DROP TABLE IF EXISTS shares, blocks, balances, balance_changes, payments, schema_migrations CASCADE`)
		s.Close()
	})
	return s
}

func TestMigrationsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	// Second application must be a no-op.
	if err := s.applyMigrations(ctx); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	var version int
	if err := s.Pool.QueryRow(ctx,
		`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != len(migrations) {
		t.Fatalf("version = %d, want %d", version, len(migrations))
	}
}

func TestEnsureSharePartition(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	at := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if err := s.EnsureSharePartition(ctx, at); err != nil {
		t.Fatal(err)
	}
	// Idempotent.
	if err := s.EnsureSharePartition(ctx, at); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	var exists bool
	if err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'shares_2026_07')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("partition shares_2026_07 not created")
	}
}

func TestShareWriterBatchesAndFlushes(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:     1024,
		BatchSize:     50,
		FlushInterval: 100 * time.Millisecond,
	})

	now := time.Now().UTC()
	const n = 137 // deliberately not a multiple of the batch size
	for i := 0; i < n; i++ {
		w.Enqueue(ShareRecord{
			PoolID:            "btc-test",
			BlockHeight:       int64(900000 + i),
			Difficulty:        1024,
			NetworkDifficulty: 1e12,
			Miner:             "bc1qminer",
			Worker:            fmt.Sprintf("rig%d", i%3),
			UserAgent:         "test/1.0",
			IPAddress:         "10.0.0.1",
			Source:            "us",
			Created:           now,
		})
	}

	// Wait for interval flushes to catch the tail.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		written, _ := w.Stats()
		if written == n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	w.Close()

	written, dropped := w.Stats()
	if written != n || dropped != 0 {
		t.Fatalf("written=%d dropped=%d, want %d/0", written, dropped, n)
	}
	var count int
	if err := s.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM shares WHERE poolid = 'btc-test'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("rows = %d, want %d", count, n)
	}
	// Spot-check a row round-trips.
	var miner, source string
	var diff float64
	if err := s.Pool.QueryRow(context.Background(),
		`SELECT miner, source, difficulty FROM shares WHERE poolid = 'btc-test' LIMIT 1`).
		Scan(&miner, &source, &diff); err != nil {
		t.Fatal(err)
	}
	if miner != "bc1qminer" || source != "us" || diff != 1024 {
		t.Fatalf("row = %s/%s/%g", miner, source, diff)
	}
}

func TestShareWriterCloseFlushesTail(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	// Long interval + big batch: nothing would flush without Close.
	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:     64,
		BatchSize:     1000,
		FlushInterval: time.Hour,
	})
	now := time.Now().UTC()
	for i := 0; i < 7; i++ {
		w.Enqueue(ShareRecord{PoolID: "tail-test", BlockHeight: 1, Difficulty: 1,
			NetworkDifficulty: 1, Miner: "m", Created: now})
	}
	w.Close()
	var count int
	if err := s.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM shares WHERE poolid = 'tail-test'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Fatalf("rows after Close = %d, want 7", count)
	}
}

func TestShareWriterDropsWhenFull(t *testing.T) {
	s := testStore(t)
	log := logging.New(logging.Options{Level: "error"})
	// A paused loop cannot drain: use an enormous flush interval and a queue of
	// 8, then enqueue 20 synchronously before any flush can happen.
	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:     8,
		BatchSize:     1000,
		FlushInterval: time.Hour,
	})
	now := time.Now().UTC()
	for i := 0; i < 20; i++ {
		w.Enqueue(ShareRecord{PoolID: "drop-test", BlockHeight: 1, Difficulty: 1,
			NetworkDifficulty: 1, Miner: "m", Created: now})
	}
	_, dropped := w.Stats()
	w.Close()
	if dropped == 0 {
		t.Fatal("full queue never dropped (Enqueue must not block)")
	}
	written, _ := w.Stats()
	if written+dropped < 20 {
		t.Fatalf("written(%d) + dropped(%d) < 20", written, dropped)
	}
}

func TestInsertBlockIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	b := BlockRecord{
		PoolID:            "btc-test",
		BlockHeight:       900123,
		NetworkDifficulty: 1e12,
		Miner:             "bc1qminer",
		Worker:            "rig1",
		Hash:              "00000000000000000001aa",
		Source:            "us",
		Created:           time.Now().UTC(),
	}
	if err := s.InsertBlock(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Duplicate submitblock race: second insert is a silent no-op.
	if err := s.InsertBlock(ctx, b); err != nil {
		t.Fatalf("duplicate insert: %v", err)
	}
	var count int
	var status string
	if err := s.Pool.QueryRow(ctx,
		`SELECT COUNT(*), MIN(status) FROM blocks WHERE poolid = 'btc-test' AND blockheight = 900123`).
		Scan(&count, &status); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != "pending" {
		t.Fatalf("count=%d status=%s, want 1/pending", count, status)
	}
}

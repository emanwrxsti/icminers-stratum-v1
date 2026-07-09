//go:build stress

// Stress tests for durable share persistence. These are guarded behind the
// 'stress' build tag so they do not slow the normal suite; CI runs them in a
// dedicated job (go test -tags stress -run TestStress ...). They require
// POOL_TEST_PG_DSN.
//
// The theme is the same durability guarantee throughout: a share accepted from
// a miner must reach the database eventually, even across a database outage or
// a queue overflow, and must never be silently dropped.
package postgres

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// beginOutage simulates a database outage for share writes by renaming the
// shares table out of the way. COPY INTO shares then fails with "relation does
// not exist", exercising the writer's retain -> WAL path exactly as a real
// outage would — but, unlike a DROP, this PRESERVES already-committed rows, so
// tests can assert no previously-persisted share is lost.
func beginOutage(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(),
		`ALTER TABLE shares RENAME TO shares_outage`); err != nil {
		t.Fatalf("begin outage: %v", err)
	}
}

// endOutage restores the shares table (with all prior rows intact),
// simulating the database coming back.
func endOutage(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(),
		`ALTER TABLE shares_outage RENAME TO shares`); err != nil {
		t.Fatalf("end outage: %v", err)
	}
}

// TestStressDBOutageNoShareLoss floods shares while the database is "down"
// (shares table dropped), then restores it and asserts every accepted share
// eventually lands in the database — none dropped. This is the core outage
// durability guarantee.
func TestStressDBOutageNoShareLoss(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.EnsureSharePartition(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "stress-outage.wal")

	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:        1024,
		BatchSize:        200,
		FlushInterval:    50 * time.Millisecond,
		WALPath:          walPath,
		WALDrainInterval: 200 * time.Millisecond,
	})

	const total = 20000
	poolID := "stress-outage"

	// Take the database "down" for shares mid-flood.
	beginOutage(t, s)

	// Flood shares faster than anything can absorb them; with the DB down they
	// must divert to the WAL rather than drop.
	for i := 0; i < total; i++ {
		w.Enqueue(mkShare(poolID, "miner", i))
		if i%2000 == 0 {
			time.Sleep(2 * time.Millisecond) // let the flush loop churn/fail
		}
	}

	// Give the writer time to attempt flushes (all failing) and divert to WAL.
	time.Sleep(1 * time.Second)

	_, dropped := w.Stats()
	if dropped != 0 {
		t.Fatalf("shares dropped during outage: %d (durability violated)", dropped)
	}
	diverted, walBytes := w.WALStats()
	if diverted == 0 || walBytes == 0 {
		t.Fatalf("expected shares diverted to WAL during outage; diverted=%d walBytes=%d", diverted, walBytes)
	}
	t.Logf("during outage: diverted=%d walBytes=%d", diverted, walBytes)

	// Database comes back.
	endOutage(t, s)

	// Wait for the WAL recovery loop to drain everything.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := w.WALStats(); b == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	w.Close() // final drain + flush of any in-memory tail

	got := countShares(t, s, poolID)
	// Each share has a unique id, so duplicates are detectable: total rows
	// must equal distinct ids.
	var distinct int
	if err := s.Pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT id) FROM shares WHERE poolid=$1`, poolID).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if got != distinct {
		t.Fatalf("duplicate shares persisted: rows=%d distinct=%d (WAL replayed shares already written)", got, distinct)
	}
	if got != total {
		t.Fatalf("shares persisted = %d, want %d (%d lost across outage)", got, total, total-got)
	}
	_, dropped = w.Stats()
	if dropped != 0 {
		t.Fatalf("shares dropped overall: %d", dropped)
	}
}

// TestStressQueueOverflowNoShareLoss floods shares far faster than the writer
// can flush to a healthy database, forcing the in-memory queue to overflow.
// With the WAL enabled, the overflow must divert to disk and ultimately land
// in the database with zero drops.
func TestStressQueueOverflowNoShareLoss(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.EnsureSharePartition(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "stress-overflow.wal")

	// A deliberately tiny queue + large batch so the flusher lags well behind
	// a fast producer, forcing Enqueue overflow into the WAL.
	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:        16,
		BatchSize:        5000,
		FlushInterval:    time.Second,
		WALPath:          walPath,
		WALDrainInterval: 200 * time.Millisecond,
	})

	const total = 50000
	poolID := "stress-overflow"

	// Multiple producers hammering the writer concurrently (realistic: many
	// miner connections submitting at once).
	var wg sync.WaitGroup
	const producers = 8
	per := total / producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				w.Enqueue(mkShare(poolID, fmt.Sprintf("miner%d", base), base+i))
			}
		}(p * per)
	}
	wg.Wait()

	diverted, _ := w.WALStats()
	if diverted == 0 {
		t.Fatal("expected queue overflow to divert to WAL, but none diverted")
	}
	_, dropped := w.Stats()
	if dropped != 0 {
		t.Fatalf("shares dropped on overflow: %d (durability violated)", dropped)
	}
	t.Logf("overflow diverted %d shares to WAL", diverted)

	// Drain to completion.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := w.WALStats(); b == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	w.Close()

	got := countShares(t, s, poolID)
	want := per * producers
	var distinct int
	if err := s.Pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT id) FROM shares WHERE poolid=$1`, poolID).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if got != distinct {
		t.Fatalf("duplicate shares: rows=%d distinct=%d", got, distinct)
	}
	if got != want {
		t.Fatalf("shares persisted = %d, want %d (%d lost)", got, want, want-got)
	}
}

// TestStressOutageRecoveryInterleaved alternates outage and recovery windows
// while continuously flooding, proving the WAL correctly accumulates and
// drains across repeated failures without loss or duplication.
func TestStressOutageRecoveryInterleaved(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.EnsureSharePartition(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	log := logging.New(logging.Options{Level: "error"})
	walPath := filepath.Join(t.TempDir(), "stress-interleaved.wal")

	w := NewShareWriter(s, log, WriterOptions{
		QueueSize:        512,
		BatchSize:        100,
		FlushInterval:    30 * time.Millisecond,
		WALPath:          walPath,
		WALDrainInterval: 150 * time.Millisecond,
	})

	poolID := "stress-interleaved"
	const rounds = 4
	const perRound = 5000
	sent := 0

	for r := 0; r < rounds; r++ {
		// Outage half of each round.
		beginOutage(t, s)
		for i := 0; i < perRound/2; i++ {
			w.Enqueue(mkShare(poolID, "m", sent))
			sent++
		}
		time.Sleep(300 * time.Millisecond)
		// Recovery half.
		endOutage(t, s)
		for i := 0; i < perRound/2; i++ {
			w.Enqueue(mkShare(poolID, "m", sent))
			sent++
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Final drain.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := w.WALStats(); b == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	w.Close()

	got := countShares(t, s, poolID)
	var distinct int
	if err := s.Pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT id) FROM shares WHERE poolid=$1`, poolID).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if got != distinct {
		t.Fatalf("duplicate shares: rows=%d distinct=%d", got, distinct)
	}
	if got != sent {
		t.Fatalf("shares persisted = %d, want %d (%d lost across interleaved outages)", got, sent, sent-got)
	}
	_, dropped := w.Stats()
	if dropped != 0 {
		t.Fatalf("shares dropped: %d", dropped)
	}
}

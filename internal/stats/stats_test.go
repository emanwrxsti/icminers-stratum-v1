package stats

import (
	"fmt"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
)

func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

func TestPoolAndMinerAccumulation(t *testing.T) {
	c := New()
	now := time.Date(2026, 7, 7, 12, 0, 30, 0, time.UTC)
	c.now = fixedNow(now)

	for i := 0; i < 10; i++ {
		c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "m1", Worker: "rig1", WorkerDiff: 1024})
	}
	c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "m2", Worker: "rig9", WorkerDiff: 2048})
	c.RecordBlock(jobs.BlockEvent{PoolID: "p1"})

	ps := c.Pool("p1")
	if ps.Shares != 11 || ps.Blocks != 1 || ps.Miners != 2 {
		t.Fatalf("pool stats = %+v", ps)
	}
	// 1-minute hashrate: (10*1024 + 2048) * 2^32 / 60
	want := (10*1024 + 2048) * pow2x32 / 60
	if ps.Hashrate1m != want {
		t.Fatalf("hashrate1m = %g, want %g", ps.Hashrate1m, want)
	}

	ms, ok := c.Miner("p1", "m1")
	if !ok {
		t.Fatal("miner m1 not tracked")
	}
	if ms.Shares != 10 || len(ms.Workers) != 1 || ms.Workers[0].Worker != "rig1" {
		t.Fatalf("miner stats = %+v", ms)
	}
	if _, ok := c.Miner("p1", "ghost"); ok {
		t.Fatal("unknown miner resolved")
	}
	if got := c.Pool("ghost-pool"); got.Shares != 0 {
		t.Fatal("unknown pool non-zero")
	}
}

func TestWindowExpiry(t *testing.T) {
	c := New()
	base := time.Date(2026, 7, 7, 12, 0, 30, 0, time.UTC)
	c.now = fixedNow(base)
	c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "m1", Worker: "w", WorkerDiff: 1000})

	// 1 minute later: out of the 1m window, still inside 15m.
	c.now = fixedNow(base.Add(1 * time.Minute))
	if hr := c.Pool("p1").Hashrate1m; hr != 0 {
		t.Fatalf("hashrate1m after 1m = %g, want 0", hr)
	}
	if hr := c.Pool("p1").Hashrate15m; hr == 0 {
		t.Fatal("hashrate15m lost the share too early")
	}
	// 16 minutes later: fully expired.
	c.now = fixedNow(base.Add(16 * time.Minute))
	if hr := c.Pool("p1").Hashrate15m; hr != 0 {
		t.Fatalf("hashrate15m after 16m = %g, want 0", hr)
	}
	// Ring reuse: a new share in a recycled bucket must not double-count.
	c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "m1", Worker: "w", WorkerDiff: 500})
	want := 500 * pow2x32 / 60
	if hr := c.Pool("p1").Hashrate1m; hr != want {
		t.Fatalf("recycled bucket hashrate = %g, want %g", hr, want)
	}
}

func TestMinerCapAndCleanup(t *testing.T) {
	c := New()
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	c.now = fixedNow(base)
	for i := 0; i < maxTrackedMiners; i++ {
		c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: fmt.Sprintf("m%d", i), Worker: "w", WorkerDiff: 1})
	}
	// Cap reached: a brand-new miner is not tracked individually...
	c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "overflow", Worker: "w", WorkerDiff: 1})
	if _, ok := c.Miner("p1", "overflow"); ok {
		t.Fatal("overflow miner tracked past cap")
	}
	// ...but pool totals still count it.
	if got := c.Pool("p1").Shares; got != maxTrackedMiners+1 {
		t.Fatalf("pool shares = %d", got)
	}
	// After the stale window, cleanup frees slots for new miners.
	c.now = fixedNow(base.Add(2*windowMinutes*time.Minute + time.Minute))
	c.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "fresh", Worker: "w", WorkerDiff: 1})
	if _, ok := c.Miner("p1", "fresh"); !ok {
		t.Fatal("cleanup did not free tracking slots")
	}
}

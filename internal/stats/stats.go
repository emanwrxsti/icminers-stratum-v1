// Package stats maintains live, in-memory mining statistics fed by the jobs
// recorder seam: per-pool and per-miner share counters and sliding-window
// hashrate estimates. It implements jobs.Recorder so it plugs into the same
// fan-out as persistence, and it is deliberately bounded: fixed-size minute
// rings and a capped miner map so a busy pool cannot grow memory without end.
package stats

import (
	"sync"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
)

// windowMinutes is the ring size: hashrate windows up to this length.
const windowMinutes = 15

// maxTrackedMiners bounds the per-pool miner map; beyond it, new miners are
// still counted in pool totals but not tracked individually until cleanup
// frees slots (stale miners are evicted after 2*windowMinutes without shares).
const maxTrackedMiners = 10000

// minuteRing accumulates share difficulty per minute bucket.
type minuteRing struct {
	buckets [windowMinutes]float64
	stamps  [windowMinutes]int64 // unix minute each bucket belongs to
}

func (r *minuteRing) add(diff float64, now time.Time) {
	minute := now.Unix() / 60
	idx := int(minute % windowMinutes)
	if r.stamps[idx] != minute {
		r.buckets[idx] = 0
		r.stamps[idx] = minute
	}
	r.buckets[idx] += diff
}

// sum returns the accumulated difficulty over the last n whole minutes.
func (r *minuteRing) sum(n int, now time.Time) float64 {
	if n > windowMinutes {
		n = windowMinutes
	}
	minute := now.Unix() / 60
	var total float64
	for i := 0; i < n; i++ {
		m := minute - int64(i)
		idx := int(m % windowMinutes)
		if idx < 0 {
			idx += windowMinutes
		}
		if r.stamps[idx] == m {
			total += r.buckets[idx]
		}
	}
	return total
}

// pow2x32 converts summed share difficulty to expected hashes (SHA256d
// difficulty-1 convention).
const pow2x32 = 4294967296.0

// hashrate converts a difficulty sum over a window into H/s.
func hashrate(diffSum float64, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return diffSum * pow2x32 / window.Seconds()
}

// minerEntry tracks one miner on one pool.
type minerEntry struct {
	ring      minuteRing
	shares    uint64
	lastShare time.Time
	workers   map[string]time.Time // worker -> last share
}

// poolEntry tracks one pool.
type poolEntry struct {
	ring   minuteRing
	shares uint64
	blocks uint64
	miners map[string]*minerEntry
}

// Collector implements jobs.Recorder.
type Collector struct {
	mu    sync.RWMutex
	pools map[string]*poolEntry
	now   func() time.Time // test seam
}

// New builds a Collector.
func New() *Collector {
	return &Collector{pools: make(map[string]*poolEntry), now: time.Now}
}

func (c *Collector) pool(poolID string) *poolEntry {
	p, ok := c.pools[poolID]
	if !ok {
		p = &poolEntry{miners: make(map[string]*minerEntry)}
		c.pools[poolID] = p
	}
	return p
}

// RecordShare implements jobs.Recorder.
func (c *Collector) RecordShare(ev jobs.ShareEvent) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pool(ev.PoolID)
	p.shares++
	p.ring.add(ev.WorkerDiff, now)

	m, ok := p.miners[ev.Miner]
	if !ok {
		if len(p.miners) >= maxTrackedMiners {
			c.cleanupLocked(p, now)
		}
		if len(p.miners) >= maxTrackedMiners {
			return // pool totals still counted; miner detail dropped
		}
		m = &minerEntry{workers: make(map[string]time.Time)}
		p.miners[ev.Miner] = m
	}
	m.shares++
	m.lastShare = now
	m.ring.add(ev.WorkerDiff, now)
	m.workers[ev.Worker] = now
}

// RecordBlock implements jobs.Recorder.
func (c *Collector) RecordBlock(ev jobs.BlockEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pool(ev.PoolID).blocks++
}

// cleanupLocked evicts miners with no shares for 2*windowMinutes.
func (c *Collector) cleanupLocked(p *poolEntry, now time.Time) {
	cutoff := now.Add(-2 * windowMinutes * time.Minute)
	for miner, m := range p.miners {
		if m.lastShare.Before(cutoff) {
			delete(p.miners, miner)
		}
	}
}

// PoolStats is a live pool snapshot.
type PoolStats struct {
	Shares      uint64  `json:"shares"`
	Blocks      uint64  `json:"blocks"`
	Miners      int     `json:"miners"`
	Hashrate1m  float64 `json:"hashrate1m"`
	Hashrate15m float64 `json:"hashrate15m"`
}

// Pool returns live stats for one pool (zero value when unseen).
func (c *Collector) Pool(poolID string) PoolStats {
	now := c.now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.pools[poolID]
	if !ok {
		return PoolStats{}
	}
	return PoolStats{
		Shares:      p.shares,
		Blocks:      p.blocks,
		Miners:      len(p.miners),
		Hashrate1m:  hashrate(p.ring.sum(1, now), time.Minute),
		Hashrate15m: hashrate(p.ring.sum(windowMinutes, now), windowMinutes*time.Minute),
	}
}

// WorkerStats is a live per-worker snapshot.
type WorkerStats struct {
	Worker    string    `json:"worker"`
	LastShare time.Time `json:"lastShare"`
}

// MinerStats is a live per-miner snapshot.
type MinerStats struct {
	Shares      uint64        `json:"shares"`
	Hashrate1m  float64       `json:"hashrate1m"`
	Hashrate15m float64       `json:"hashrate15m"`
	LastShare   time.Time     `json:"lastShare"`
	Workers     []WorkerStats `json:"workers"`
}

// Miner returns live stats for one miner on one pool.
func (c *Collector) Miner(poolID, miner string) (MinerStats, bool) {
	now := c.now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.pools[poolID]
	if !ok {
		return MinerStats{}, false
	}
	m, ok := p.miners[miner]
	if !ok {
		return MinerStats{}, false
	}
	out := MinerStats{
		Shares:      m.shares,
		Hashrate1m:  hashrate(m.ring.sum(1, now), time.Minute),
		Hashrate15m: hashrate(m.ring.sum(windowMinutes, now), windowMinutes*time.Minute),
		LastShare:   m.lastShare,
	}
	for w, last := range m.workers {
		out.Workers = append(out.Workers, WorkerStats{Worker: w, LastShare: last})
	}
	return out, true
}

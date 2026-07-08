package vardiff

import (
	"sync"
	"time"
)

// ControllerConfig tunes one session's variable difficulty.
type ControllerConfig struct {
	// MinDiff / MaxDiff bound the difficulty (from the port config).
	MinDiff float64
	MaxDiff float64
	// TargetInterval is the desired seconds-per-share (default 10s).
	TargetInterval time.Duration
	// RetargetInterval is how often adjustments are considered (default 60s).
	RetargetInterval time.Duration
	// VariancePercent skips adjustments inside this tolerance band around the
	// target rate (default 30).
	VariancePercent float64
	// MaxAdjustFactor clamps a single retarget step (default 4: at most 4x up
	// or 4x down per step), so misestimates cannot whipsaw miners.
	MaxAdjustFactor float64
}

func (c *ControllerConfig) defaults() {
	if c.TargetInterval <= 0 {
		c.TargetInterval = 10 * time.Second
	}
	if c.RetargetInterval <= 0 {
		c.RetargetInterval = 60 * time.Second
	}
	if c.VariancePercent <= 0 {
		c.VariancePercent = 30
	}
	if c.MaxAdjustFactor <= 1 {
		c.MaxAdjustFactor = 4
	}
	if c.MinDiff <= 0 {
		c.MinDiff = 0.001
	}
	if c.MaxDiff <= 0 || c.MaxDiff < c.MinDiff {
		c.MaxDiff = 1 << 40
	}
}

// Controller retargets one session's difficulty from its observed share rate.
// Safe for concurrent use (shares arrive from the connection goroutine while
// retargeting runs from the server loop).
type Controller struct {
	cfg ControllerConfig

	mu           sync.Mutex
	windowStart  time.Time
	shares       int
	lastRetarget time.Time
}

// NewController builds a controller; now anchors the first window.
func NewController(cfg ControllerConfig, now time.Time) *Controller {
	cfg.defaults()
	return &Controller{cfg: cfg, windowStart: now, lastRetarget: now}
}

// OnShare records one accepted share.
func (c *Controller) OnShare(now time.Time) {
	c.mu.Lock()
	c.shares++
	c.mu.Unlock()
}

// Retarget computes the new difficulty from the share rate since the last
// retarget. Returns (newDiff, true) when the difficulty should change; the
// caller applies it to the session and notifies the miner. Calling before
// RetargetInterval has elapsed is a no-op.
func (c *Controller) Retarget(now time.Time, currentDiff float64) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elapsed := now.Sub(c.lastRetarget)
	if elapsed < c.cfg.RetargetInterval {
		return currentDiff, false
	}
	shares := c.shares
	c.shares = 0
	c.lastRetarget = now
	c.windowStart = now

	target := c.cfg.TargetInterval.Seconds()
	var observed float64
	if shares == 0 {
		// Idle: pretend one share took the whole window, pushing diff down so
		// slow miners start contributing again.
		observed = elapsed.Seconds()
	} else {
		observed = elapsed.Seconds() / float64(shares)
	}

	// Inside the tolerance band: leave it alone.
	deviation := (observed - target) / target * 100
	if deviation < 0 {
		deviation = -deviation
	}
	if deviation <= c.cfg.VariancePercent {
		return currentDiff, false
	}

	// New difficulty proportional to the observed rate: shares arriving too
	// fast (observed < target) raise difficulty; too slow lowers it.
	factor := target / observed
	if factor > c.cfg.MaxAdjustFactor {
		factor = c.cfg.MaxAdjustFactor
	}
	if factor < 1/c.cfg.MaxAdjustFactor {
		factor = 1 / c.cfg.MaxAdjustFactor
	}
	newDiff := currentDiff * factor
	if newDiff < c.cfg.MinDiff {
		newDiff = c.cfg.MinDiff
	}
	if newDiff > c.cfg.MaxDiff {
		newDiff = c.cfg.MaxDiff
	}
	if newDiff == currentDiff {
		return currentDiff, false
	}
	return newDiff, true
}

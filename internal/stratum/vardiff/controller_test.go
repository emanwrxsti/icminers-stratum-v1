package vardiff

import (
	"testing"
	"time"
)

func testCfg() ControllerConfig {
	return ControllerConfig{
		MinDiff:          64,
		MaxDiff:          1 << 20,
		TargetInterval:   10 * time.Second,
		RetargetInterval: 60 * time.Second,
		VariancePercent:  30,
		MaxAdjustFactor:  4,
	}
}

func TestRetargetTooEarlyIsNoop(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	for i := 0; i < 100; i++ {
		c.OnShare(base)
	}
	if d, changed := c.Retarget(base.Add(30*time.Second), 1024); changed || d != 1024 {
		t.Fatalf("early retarget: d=%g changed=%v", d, changed)
	}
}

func TestRaiseOnFastShares(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	// 60 shares in 60s = 1s/share, target 10s -> factor 10 clamped to 4.
	for i := 0; i < 60; i++ {
		c.OnShare(base)
	}
	d, changed := c.Retarget(base.Add(60*time.Second), 1024)
	if !changed || d != 4096 {
		t.Fatalf("fast retarget: d=%g changed=%v, want 4096", d, changed)
	}
}

func TestLowerOnSlowShares(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	// 2 shares in 60s = 30s/share, target 10s -> factor 1/3.
	c.OnShare(base)
	c.OnShare(base)
	d, changed := c.Retarget(base.Add(60*time.Second), 3000)
	if !changed || d != 1000 {
		t.Fatalf("slow retarget: d=%g changed=%v, want 1000", d, changed)
	}
}

func TestIdleLowersTowardMin(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	// Zero shares in 60s: observed=60s, factor 10/60 clamped to 1/4.
	d, changed := c.Retarget(base.Add(60*time.Second), 1024)
	if !changed || d != 256 {
		t.Fatalf("idle retarget: d=%g changed=%v, want 256", d, changed)
	}
	// Repeated idling floors at MinDiff.
	cur := d
	now := base.Add(60 * time.Second)
	for i := 0; i < 10; i++ {
		now = now.Add(60 * time.Second)
		if nd, ch := c.Retarget(now, cur); ch {
			cur = nd
		}
	}
	if cur != 64 {
		t.Fatalf("floor = %g, want MinDiff 64", cur)
	}
	// At the floor, further idling reports no change.
	now = now.Add(60 * time.Second)
	if _, ch := c.Retarget(now, cur); ch {
		t.Fatal("change reported at MinDiff floor")
	}
}

func TestVarianceBandHolds(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	// 5 shares in 60s = 12s/share: 20% off a 10s target, inside 30% band.
	for i := 0; i < 5; i++ {
		c.OnShare(base)
	}
	if _, changed := c.Retarget(base.Add(60*time.Second), 1024); changed {
		t.Fatal("retarget inside variance band")
	}
}

func TestMaxDiffCeiling(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	cfg := testCfg()
	cfg.MaxDiff = 2000
	c := NewController(cfg, base)
	for i := 0; i < 600; i++ {
		c.OnShare(base)
	}
	d, changed := c.Retarget(base.Add(60*time.Second), 1024)
	if !changed || d != 2000 {
		t.Fatalf("ceiling: d=%g changed=%v, want 2000", d, changed)
	}
}

func TestWindowResetsBetweenRetargets(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c := NewController(testCfg(), base)
	for i := 0; i < 60; i++ {
		c.OnShare(base)
	}
	t1 := base.Add(60 * time.Second)
	if _, changed := c.Retarget(t1, 1024); !changed {
		t.Fatal("first retarget expected")
	}
	// The next window saw NO shares: idle behavior, not leftover counts.
	t2 := t1.Add(60 * time.Second)
	d, changed := c.Retarget(t2, 4096)
	if !changed || d >= 4096 {
		t.Fatalf("second retarget: d=%g changed=%v, want a decrease", d, changed)
	}
}

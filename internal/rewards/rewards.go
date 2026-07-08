// Package rewards implements Stage 7: block confirmation tracking and the
// PPLNS / PROP / SOLO reward calculators. All amounts are exact integers in
// base units (satoshis) — no floating-point money anywhere. Calculators are
// pure functions over an abstract ShareSource so they unit-test without a
// database.
package rewards

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Credit is one miner's cut of a block reward, in base units.
type Credit struct {
	Miner      string
	AmountSats int64
}

// Block is the subset of a persisted block the calculators need.
type Block struct {
	ID                int64
	PoolID            string
	Height            int64
	Miner             string // finder ("address" part)
	Hash              string
	RewardSats        int64
	NetworkDifficulty float64
	Created           time.Time
}

// MinerWork is one miner's accumulated share difficulty.
type MinerWork struct {
	Miner   string
	DiffSum float64
}

// ShareSource abstracts the share queries the calculators need. Implemented
// by the postgres store; tests use in-memory fakes.
type ShareSource interface {
	// WorkBetween returns per-miner share difficulty sums for shares with
	// from < created <= to.
	WorkBetween(ctx context.Context, poolID string, from, to time.Time) ([]MinerWork, error)
	// WorkBackward walks shares with created <= before, newest first, and
	// accumulates per-miner difficulty until targetDiff is collected (or
	// shares run out). Returns the per-miner sums actually counted.
	WorkBackward(ctx context.Context, poolID string, before time.Time, targetDiff float64) ([]MinerWork, error)
	// PreviousBlockTime returns the created time of the pool's most recent
	// block strictly before the given time; ok=false when none exists.
	PreviousBlockTime(ctx context.Context, poolID string, before time.Time) (time.Time, bool, error)
}

// ApplyFee subtracts the pool fee from the block reward, flooring in the
// pool's favor being avoided: the fee is floored, so miners never lose a
// satoshi to rounding. Returns (distributable, feeSats).
func ApplyFee(rewardSats int64, feePercent float64) (int64, int64) {
	if feePercent <= 0 || rewardSats <= 0 {
		return rewardSats, 0
	}
	if feePercent >= 100 {
		return 0, rewardSats
	}
	fee := int64(float64(rewardSats) * feePercent / 100.0) // floor
	return rewardSats - fee, fee
}

// distribute splits amountSats across miners proportionally to their diff
// sums using integer floor division, then hands every remainder satoshi to
// the largest contributors (deterministic order: diff desc, then miner asc).
// The returned credits sum EXACTLY to amountSats.
func distribute(amountSats int64, work []MinerWork) []Credit {
	if amountSats <= 0 || len(work) == 0 {
		return nil
	}
	var total float64
	for _, w := range work {
		if w.DiffSum > 0 {
			total += w.DiffSum
		}
	}
	if total <= 0 {
		return nil
	}
	sorted := make([]MinerWork, 0, len(work))
	for _, w := range work {
		if w.DiffSum > 0 {
			sorted = append(sorted, w)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].DiffSum != sorted[j].DiffSum {
			return sorted[i].DiffSum > sorted[j].DiffSum
		}
		return sorted[i].Miner < sorted[j].Miner
	})

	credits := make([]Credit, 0, len(sorted))
	var assigned int64
	for _, w := range sorted {
		amt := int64(float64(amountSats) * (w.DiffSum / total)) // floor
		credits = append(credits, Credit{Miner: w.Miner, AmountSats: amt})
		assigned += amt
	}
	// Remainder satoshis (at most len(credits)-1 plus float slack) go one by
	// one to the largest contributors.
	for i := 0; assigned < amountSats; i = (i + 1) % len(credits) {
		credits[i].AmountSats++
		assigned++
	}
	// Drop zero-amount credits (tiny contributors under 1 sat).
	out := credits[:0]
	for _, c := range credits {
		if c.AmountSats > 0 {
			out = append(out, c)
		}
	}
	return out
}

// Calculator computes the credits for one confirmed block.
type Calculator interface {
	Name() string
	Calculate(ctx context.Context, src ShareSource, block Block, distributableSats int64) ([]Credit, error)
}

// --- SOLO ---

// Solo pays the entire distributable reward to the block finder.
type Solo struct{}

// Name implements Calculator.
func (Solo) Name() string { return "solo" }

// Calculate implements Calculator.
func (Solo) Calculate(_ context.Context, _ ShareSource, block Block, distributableSats int64) ([]Credit, error) {
	if block.Miner == "" {
		return nil, fmt.Errorf("solo: block %d has no finder miner", block.Height)
	}
	if distributableSats <= 0 {
		return nil, nil
	}
	return []Credit{{Miner: block.Miner, AmountSats: distributableSats}}, nil
}

// --- PROP ---

// Prop pays proportionally to share work between the pool's previous block
// and this one. A pool's first block counts all shares up to it.
type Prop struct{}

// Name implements Calculator.
func (Prop) Name() string { return "prop" }

// Calculate implements Calculator.
func (Prop) Calculate(ctx context.Context, src ShareSource, block Block, distributableSats int64) ([]Credit, error) {
	from := time.Time{} // beginning of time for the first block
	if prev, ok, err := src.PreviousBlockTime(ctx, block.PoolID, block.Created); err != nil {
		return nil, fmt.Errorf("prop: previous block time: %w", err)
	} else if ok {
		from = prev
	}
	work, err := src.WorkBetween(ctx, block.PoolID, from, block.Created)
	if err != nil {
		return nil, fmt.Errorf("prop: work between: %w", err)
	}
	credits := distribute(distributableSats, work)
	if len(credits) == 0 && distributableSats > 0 {
		// No shares in the round (shouldn't happen for a real block, which
		// itself required a share): fall back to the finder.
		return Solo{}.Calculate(ctx, src, block, distributableSats)
	}
	return credits, nil
}

// --- PPLNS ---

// PPLNS pays the last N of share difficulty before the block, where
// N = Factor × the block's network difficulty. Miners are credited in
// proportion to the difficulty actually counted inside the window.
type PPLNS struct {
	// Factor is the window multiplier (classic default 2.0).
	Factor float64
}

// Name implements Calculator.
func (p PPLNS) Name() string { return "pplns" }

// Calculate implements Calculator.
func (p PPLNS) Calculate(ctx context.Context, src ShareSource, block Block, distributableSats int64) ([]Credit, error) {
	factor := p.Factor
	if factor <= 0 {
		factor = 2.0
	}
	target := factor * block.NetworkDifficulty
	if target <= 0 {
		return nil, fmt.Errorf("pplns: block %d has no network difficulty", block.Height)
	}
	work, err := src.WorkBackward(ctx, block.PoolID, block.Created, target)
	if err != nil {
		return nil, fmt.Errorf("pplns: work backward: %w", err)
	}
	credits := distribute(distributableSats, work)
	if len(credits) == 0 && distributableSats > 0 {
		return Solo{}.Calculate(ctx, src, block, distributableSats)
	}
	return credits, nil
}

// ForPaymentMode returns the calculator for a pool's configured payment mode.
func ForPaymentMode(mode string, pplnsFactor float64) (Calculator, error) {
	switch mode {
	case "solo":
		return Solo{}, nil
	case "prop":
		return Prop{}, nil
	case "pplns":
		return PPLNS{Factor: pplnsFactor}, nil
	default:
		return nil, fmt.Errorf("unknown payment mode %q", mode)
	}
}

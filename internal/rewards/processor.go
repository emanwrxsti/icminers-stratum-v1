package rewards

import (
	"context"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// BlockStore abstracts the persistence the processor drives. Implemented by
// the postgres store.
type BlockStore interface {
	ShareSource
	UnconfirmedBlocks(ctx context.Context, poolID string) ([]Block, error)
	ConfirmedUnrewarded(ctx context.Context, poolID string) ([]Block, error)
	UpdateBlockConfirmation(ctx context.Context, blockID int64, status string, confirmations int64, progress float64) error
	CreditBlockRewards(ctx context.Context, block Block, credits []Credit, feeSats int64) error
}

// ProcessorOptions configure one pool's reward processor.
type ProcessorOptions struct {
	PoolID     string
	Calculator Calculator
	FeePercent float64
	Chain      ChainView
	Store      BlockStore
	Confirm    ConfirmOptions
	// Interval between processing passes (default 30s).
	Interval time.Duration
	Log      *logging.Logger
}

// Processor confirms and rewards one pool's blocks. One Processor per pool,
// panic-isolated, exactly like every other per-pool worker in this codebase.
type Processor struct {
	opts ProcessorOptions
	log  *logging.Logger
}

// NewProcessor builds a processor.
func NewProcessor(opts ProcessorOptions) *Processor {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	return &Processor{
		opts: opts,
		log:  logging.Component(logging.ForPool(opts.Log, opts.PoolID), "rewards"),
	}
}

// Run loops until ctx ends. Panics in a pass are recovered and backed off;
// one pool's failure never touches another (each pool has its own Run).
func (p *Processor) Run(ctx context.Context) {
	for {
		p.runOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.opts.Interval):
		}
	}
}

func (p *Processor) runOnce(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("panic in reward pass; continuing next interval", "recover", r)
			backoffSleep(ctx, 5*time.Second)
		}
	}()
	if err := p.Pass(ctx); err != nil {
		p.log.Warn("reward pass failed; retrying next interval", "err", err)
	}
}

// Pass performs one confirmation + reward sweep. Exported for tests and for
// one-shot invocations.
func (p *Processor) Pass(ctx context.Context) error {
	if err := p.confirmPass(ctx); err != nil {
		return err
	}
	return p.rewardPass(ctx)
}

func (p *Processor) confirmPass(ctx context.Context) error {
	blocks, err := p.opts.Store.UnconfirmedBlocks(ctx, p.opts.PoolID)
	if err != nil {
		return err
	}
	for _, b := range blocks {
		status, err := CheckBlock(ctx, p.opts.Chain, b.Height, b.Hash, p.opts.Confirm)
		if err != nil {
			// Daemon trouble: stop this pass; the block stays pending.
			return err
		}
		switch status.Status {
		case "pending":
			if status.Confirmations > 0 {
				if err := p.opts.Store.UpdateBlockConfirmation(ctx, b.ID, "pending",
					status.Confirmations, status.Progress); err != nil {
					return err
				}
			}
		case "confirmed":
			if err := p.opts.Store.UpdateBlockConfirmation(ctx, b.ID, "confirmed",
				status.Confirmations, 1); err != nil {
				return err
			}
			p.log.Info("block CONFIRMED", "height", b.Height, "hash", b.Hash,
				"confirmations", status.Confirmations, "rewardSats", b.RewardSats)
		case "orphaned":
			if err := p.opts.Store.UpdateBlockConfirmation(ctx, b.ID, "orphaned", 0, 0); err != nil {
				return err
			}
			p.log.Warn("block ORPHANED (no reward)", "height", b.Height, "hash", b.Hash)
		}
	}
	return nil
}

func (p *Processor) rewardPass(ctx context.Context) error {
	blocks, err := p.opts.Store.ConfirmedUnrewarded(ctx, p.opts.PoolID)
	if err != nil {
		return err
	}
	for _, b := range blocks {
		distributable, feeSats := ApplyFee(b.RewardSats, p.opts.FeePercent)
		credits, err := p.opts.Calculator.Calculate(ctx, p.opts.Store, b, distributable)
		if err != nil {
			return err
		}
		if err := p.opts.Store.CreditBlockRewards(ctx, b, credits, feeSats); err != nil {
			return err
		}
		var total int64
		for _, c := range credits {
			total += c.AmountSats
		}
		p.log.Info("block REWARDED",
			"height", b.Height, "scheme", p.opts.Calculator.Name(),
			"miners", len(credits), "distributedSats", total, "feeSats", feeSats)
	}
	return nil
}

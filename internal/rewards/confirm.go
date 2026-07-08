package rewards

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ChainView abstracts the daemon calls confirmation tracking needs.
// Implemented over internal/coins/rpc for any bitcoinlike daemon; tests use
// fakes.
type ChainView interface {
	// TipHeight returns the current best-chain height.
	TipHeight(ctx context.Context) (int64, error)
	// BlockHashAt returns the best-chain block hash at a height. Returns
	// ok=false when the height is beyond the tip.
	BlockHashAt(ctx context.Context, height int64) (string, bool, error)
}

// ConfirmOptions tune confirmation tracking.
type ConfirmOptions struct {
	// MaturityDepth is how many confirmations make a coinbase spendable
	// (Bitcoin: 100).
	MaturityDepth int64
	// OrphanDepth: when our hash is absent from the best chain and the chain
	// has advanced this far past the block height, the block is orphaned.
	OrphanDepth int64
}

func (o *ConfirmOptions) defaults() {
	if o.MaturityDepth <= 0 {
		o.MaturityDepth = 100
	}
	if o.OrphanDepth <= 0 {
		o.OrphanDepth = 12
	}
}

// ConfirmationStatus is the outcome of checking one block.
type ConfirmationStatus struct {
	// Status: pending | confirmed | orphaned.
	Status        string
	Confirmations int64
	// Progress in [0,1] toward maturity.
	Progress float64
}

// CheckBlock classifies one of our blocks against the current chain.
func CheckBlock(ctx context.Context, chain ChainView, height int64, ourHash string, opts ConfirmOptions) (ConfirmationStatus, error) {
	opts.defaults()
	tip, err := chain.TipHeight(ctx)
	if err != nil {
		return ConfirmationStatus{}, fmt.Errorf("confirm: tip: %w", err)
	}
	if tip < height {
		// Chain has not reached our height (view lag): still pending.
		return ConfirmationStatus{Status: "pending"}, nil
	}
	chainHash, ok, err := chain.BlockHashAt(ctx, height)
	if err != nil {
		return ConfirmationStatus{}, fmt.Errorf("confirm: hash at %d: %w", height, err)
	}
	if !ok {
		return ConfirmationStatus{Status: "pending"}, nil
	}
	if !strings.EqualFold(chainHash, ourHash) {
		// Not on the best chain. Orphan only once the chain is convincingly
		// past us; a shallow mismatch could still reorg back.
		if tip-height >= opts.OrphanDepth {
			return ConfirmationStatus{Status: "orphaned"}, nil
		}
		return ConfirmationStatus{Status: "pending"}, nil
	}
	conf := tip - height + 1
	progress := float64(conf) / float64(opts.MaturityDepth)
	if progress > 1 {
		progress = 1
	}
	if conf >= opts.MaturityDepth {
		return ConfirmationStatus{Status: "confirmed", Confirmations: conf, Progress: 1}, nil
	}
	return ConfirmationStatus{Status: "pending", Confirmations: conf, Progress: progress}, nil
}

// RPCCaller is the minimal JSON-RPC surface (satisfied by *rpc.Client).
type RPCCaller interface {
	Call(ctx context.Context, method string, params any, result any) error
}

// BitcoinlikeChain implements ChainView over getblockcount/getblockhash.
type BitcoinlikeChain struct {
	RPC RPCCaller
}

// TipHeight implements ChainView.
func (c *BitcoinlikeChain) TipHeight(ctx context.Context) (int64, error) {
	var h int64
	if err := c.RPC.Call(ctx, "getblockcount", nil, &h); err != nil {
		return 0, err
	}
	return h, nil
}

// BlockHashAt implements ChainView. bitcoind returns error -8 ("Block height
// out of range") past the tip; treated as ok=false.
func (c *BitcoinlikeChain) BlockHashAt(ctx context.Context, height int64) (string, bool, error) {
	var hash string
	if err := c.RPC.Call(ctx, "getblockhash", []any{height}, &hash); err != nil {
		if strings.Contains(err.Error(), "out of range") {
			return "", false, nil
		}
		return "", false, err
	}
	return hash, true, nil
}

// backoffSleep sleeps respecting ctx.
func backoffSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

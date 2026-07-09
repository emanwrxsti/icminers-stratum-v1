// Package payouts implements Stage 9: moving credited miner balances
// on-chain. The safety order is deduct-then-send: balances are deducted and
// payment rows created atomically BEFORE the wallet RPC, so a crash can never
// pay twice; a failed send refunds atomically. Payments stuck in 'sending'
// after a crash are surfaced loudly for operator reconciliation instead of
// being auto-refunded (the transaction may have gone out).
package payouts

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// Payment is one miner payout row.
type Payment struct {
	ID         int64
	PoolID     string
	Miner      string
	Address    string
	AmountSats int64
	BatchID    string
}

// SatsToAmountString renders base units as an exact 8-decimal coin amount
// string (bitcoind accepts string amounts, avoiding JSON float loss).
func SatsToAmountString(sats int64) string {
	if sats < 0 {
		return "-" + SatsToAmountString(-sats)
	}
	return fmt.Sprintf("%d.%08d", sats/100000000, sats%100000000)
}

// AddressValidator checks/normalizes a payout address. Implemented by the
// coin adapter; invalid addresses are skipped (balance retained) rather than
// burning funds on a doomed sendmany.
type AddressValidator interface {
	NormalizeAddress(address string) (string, error)
}

// Wallet abstracts the daemon wallet call. Implemented over the rpc client;
// tests use fakes.
type Wallet interface {
	// SendMany sends amounts (base units) to addresses in ONE transaction and
	// returns the txid. subtractFee subtracts the tx fee from recipients.
	SendMany(ctx context.Context, outputs map[string]int64, subtractFee bool) (string, error)
}

// PayoutStore abstracts the persistence side. Implemented by postgres.Store.
type PayoutStore interface {
	// BeginPayout atomically selects balances >= minSats, deducts them, and
	// creates 'sending' payment rows tagged with batchID. Returns the batch
	// (empty when nothing is payable). validate maps a miner name to its
	// payout address; miners rejected by validate are skipped and keep their
	// balance.
	BeginPayout(ctx context.Context, poolID string, minSats int64, batchID string, validate func(miner string) (string, bool)) ([]Payment, error)
	// MarkPaymentsSent finalizes a batch with its txid.
	MarkPaymentsSent(ctx context.Context, batchID, txid string) error
	// RefundBatch atomically refunds every payment in the batch and marks the
	// rows 'failed'.
	RefundBatch(ctx context.Context, batchID string) error
	// StuckBatches returns batch ids still 'sending' (crash recovery surface),
	// excluding very recent ones that may legitimately be in flight.
	StuckBatches(ctx context.Context, poolID string) ([]string, error)
}

// ProcessorOptions configure one pool's payout processor.
type ProcessorOptions struct {
	PoolID      string
	MinSats     int64
	SubtractFee bool
	Wallet      Wallet
	Store       PayoutStore
	Validator   AddressValidator
	Interval    time.Duration
	Log         *logging.Logger
	// NewBatchID generates batch ids (test seam; default time-based).
	NewBatchID func() string
}

// Processor pays one pool's miners. One per pool, panic-isolated.
type Processor struct {
	opts ProcessorOptions
	log  *logging.Logger
}

// NewProcessor builds a processor.
func NewProcessor(opts ProcessorOptions) *Processor {
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Minute
	}
	if opts.MinSats <= 0 {
		opts.MinSats = 100000
	}
	if opts.NewBatchID == nil {
		opts.NewBatchID = func() string {
			return fmt.Sprintf("%s-%d", opts.PoolID, time.Now().UnixNano())
		}
	}
	return &Processor{
		opts: opts,
		log:  logging.Component(logging.ForPool(opts.Log, opts.PoolID), "payouts"),
	}
}

// Run loops until ctx ends.
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
			p.log.Error("panic in payout pass; continuing next interval", "recover", r)
		}
	}()
	if err := p.Pass(ctx); err != nil {
		p.log.Warn("payout pass failed; retrying next interval", "err", err)
	}
}

// Pass performs one payout sweep. Exported for -once and tests.
func (p *Processor) Pass(ctx context.Context) error {
	// Crash recovery surface: batches deducted but with no send outcome are
	// NEVER auto-refunded (the tx may have broadcast). Scream for an operator.
	stuck, err := p.opts.Store.StuckBatches(ctx, p.opts.PoolID)
	if err != nil {
		return fmt.Errorf("payout: stuck batches: %w", err)
	}
	for _, b := range stuck {
		p.log.Error("STUCK payout batch requires operator reconciliation "+
			"(check wallet transactions; then mark sent with the txid or refund manually)",
			"batch", b)
	}

	batchID := p.opts.NewBatchID()
	validate := func(miner string) (string, bool) {
		if p.opts.Validator == nil {
			return miner, true
		}
		addr, err := p.opts.Validator.NormalizeAddress(miner)
		if err != nil {
			p.log.Warn("unpayable miner address; balance retained", "miner", miner, "err", err)
			return "", false
		}
		return addr, true
	}
	batch, err := p.opts.Store.BeginPayout(ctx, p.opts.PoolID, p.opts.MinSats, batchID, validate)
	if err != nil {
		return fmt.Errorf("payout: begin: %w", err)
	}
	if len(batch) == 0 {
		return nil
	}

	outputs := make(map[string]int64, len(batch))
	var total int64
	for _, pay := range batch {
		outputs[pay.Address] += pay.AmountSats
		total += pay.AmountSats
	}

	txid, err := p.opts.Wallet.SendMany(ctx, outputs, p.opts.SubtractFee)
	if err != nil {
		p.log.Error("sendmany FAILED; refunding batch", "batch", batchID, "err", err)
		if rerr := p.opts.Store.RefundBatch(ctx, batchID); rerr != nil {
			// Deducted, unsent, and unrefunded: the stuck-batch scan will keep
			// surfacing it every pass until the database recovers.
			return fmt.Errorf("payout: REFUND FAILED for batch %s (funds held in 'sending'): %v (send error: %w)", batchID, rerr, err)
		}
		return fmt.Errorf("payout: send failed (refunded): %w", err)
	}
	if err := p.opts.Store.MarkPaymentsSent(ctx, batchID, txid); err != nil {
		// Money moved but rows not finalized: stuck-batch scan surfaces it.
		return fmt.Errorf("payout: SENT (txid %s) but marking failed for batch %s: %w", txid, batchID, err)
	}
	p.log.Info("payout batch SENT",
		"batch", batchID, "txid", txid, "payments", len(batch),
		"totalSats", total, "subtractFee", p.opts.SubtractFee)
	return nil
}

// RPCCaller is the minimal JSON-RPC surface (satisfied by *rpc.Client).
type RPCCaller interface {
	Call(ctx context.Context, method string, params any, result any) error
}

// BitcoindWallet implements Wallet over bitcoind sendmany.
type BitcoindWallet struct {
	RPC RPCCaller
}

// SendMany implements Wallet. Amounts are exact 8-decimal strings.
// sendmany positional params:
//
//	1 dummy "" | 2 amounts | 3 minconf | 4 comment | 5 subtractfeefrom
func (w *BitcoindWallet) SendMany(ctx context.Context, outputs map[string]int64, subtractFee bool) (string, error) {
	if len(outputs) == 0 {
		return "", fmt.Errorf("sendmany: empty outputs")
	}
	amounts := make(map[string]string, len(outputs))
	subtract := make([]string, 0, len(outputs))
	for addr, sats := range outputs {
		if sats <= 0 {
			return "", fmt.Errorf("sendmany: non-positive amount for %s", addr)
		}
		amounts[addr] = SatsToAmountString(sats)
		subtract = append(subtract, addr)
	}
	params := []any{"", amounts, 1, "gostratum payout"}
	if subtractFee {
		params = append(params, subtract)
	}
	var txid string
	if err := w.RPC.Call(ctx, "sendmany", params, &txid); err != nil {
		return "", err
	}
	if strings.TrimSpace(txid) == "" {
		return "", fmt.Errorf("sendmany: daemon returned empty txid")
	}
	return txid, nil
}

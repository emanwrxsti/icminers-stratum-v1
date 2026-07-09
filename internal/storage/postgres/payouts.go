package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/payouts"
)

// BeginPayout implements payouts.PayoutStore: in one transaction it locks
// every balance row at/above minSats, validates the payout address, deducts
// the balance, writes a 'payout' balance_change, and creates a 'sending'
// payment row tagged with batchID. Nothing leaves this function half-done.
func (s *Store) BeginPayout(ctx context.Context, poolID string, minSats int64, batchID string, validate func(miner string) (string, bool)) ([]payouts.Payment, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("payout begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT miner, amount_sats FROM balances
		WHERE poolid = $1 AND amount_sats >= $2
		ORDER BY miner
		FOR UPDATE`, poolID, minSats)
	if err != nil {
		return nil, fmt.Errorf("payout select: %w", err)
	}
	type payable struct {
		miner string
		sats  int64
	}
	var candidates []payable
	for rows.Next() {
		var p payable
		if err := rows.Scan(&p.miner, &p.sats); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	batch := []payouts.Payment{}
	for _, c := range candidates {
		address, ok := validate(c.miner)
		if !ok {
			continue // balance retained
		}
		if _, err := tx.Exec(ctx, `
			UPDATE balances SET amount_sats = amount_sats - $3, updated = now()
			WHERE poolid = $1 AND miner = $2`, poolID, c.miner, c.sats); err != nil {
			return nil, fmt.Errorf("payout deduct %s: %w", c.miner, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO balance_changes (poolid, miner, amount_sats, usage)
			VALUES ($1, $2, $3, 'payout')`, poolID, c.miner, -c.sats); err != nil {
			return nil, fmt.Errorf("payout change %s: %w", c.miner, err)
		}
		var id int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO payments (poolid, miner, address, amount_sats, status, batch_id)
			VALUES ($1, $2, $3, $4, 'sending', $5)
			RETURNING id`, poolID, c.miner, address, c.sats, batchID).Scan(&id); err != nil {
			return nil, fmt.Errorf("payout row %s: %w", c.miner, err)
		}
		batch = append(batch, payouts.Payment{
			ID: id, PoolID: poolID, Miner: c.miner, Address: address,
			AmountSats: c.sats, BatchID: batchID,
		})
	}
	if len(batch) == 0 {
		return nil, tx.Rollback(ctx)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("payout commit: %w", err)
	}
	return batch, nil
}

// MarkPaymentsSent implements payouts.PayoutStore.
func (s *Store) MarkPaymentsSent(ctx context.Context, batchID, txid string) error {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE payments SET status = 'sent', txid = $2, updated = now()
		WHERE batch_id = $1 AND status = 'sending'`, batchID, txid)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark sent: batch %s has no sending rows", batchID)
	}
	return nil
}

// RefundBatch implements payouts.PayoutStore: atomically re-credits every
// 'sending' payment in the batch and marks the rows 'failed'.
func (s *Store) RefundBatch(ctx context.Context, batchID string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("refund begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT poolid, miner, amount_sats FROM payments
		WHERE batch_id = $1 AND status = 'sending'
		FOR UPDATE`, batchID)
	if err != nil {
		return fmt.Errorf("refund select: %w", err)
	}
	type row struct {
		poolID, miner string
		sats          int64
	}
	var toRefund []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.poolID, &r.miner, &r.sats); err != nil {
			rows.Close()
			return err
		}
		toRefund = append(toRefund, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(toRefund) == 0 {
		return nil
	}
	for _, r := range toRefund {
		if _, err := tx.Exec(ctx, `
			INSERT INTO balances (poolid, miner, amount_sats, updated)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (poolid, miner)
			DO UPDATE SET amount_sats = balances.amount_sats + EXCLUDED.amount_sats,
			              updated = now()`, r.poolID, r.miner, r.sats); err != nil {
			return fmt.Errorf("refund credit %s: %w", r.miner, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO balance_changes (poolid, miner, amount_sats, usage)
			VALUES ($1, $2, $3, 'payout-refund')`, r.poolID, r.miner, r.sats); err != nil {
			return fmt.Errorf("refund change %s: %w", r.miner, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE payments SET status = 'failed', updated = now()
		WHERE batch_id = $1 AND status = 'sending'`, batchID); err != nil {
		return fmt.Errorf("refund mark: %w", err)
	}
	return tx.Commit(ctx)
}

// StuckBatches implements payouts.PayoutStore: 'sending' batches older than a
// grace period (a crash happened between deduct and outcome).
func (s *Store) StuckBatches(ctx context.Context, poolID string) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT DISTINCT batch_id FROM payments
		WHERE poolid = $1 AND status = 'sending' AND created < $2
		ORDER BY batch_id`, poolID, time.Now().UTC().Add(-5*time.Minute))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// PaymentRow is a persisted payment as served by the API.
type PaymentRow struct {
	ID         int64     `json:"id"`
	Miner      string    `json:"miner"`
	Address    string    `json:"address"`
	AmountSats int64     `json:"amountSats"`
	TxID       string    `json:"txid"`
	Status     string    `json:"status"`
	Created    time.Time `json:"created"`
}

// ListPayments returns a pool's payments, newest first, optionally filtered
// by miner.
func (s *Store) ListPayments(ctx context.Context, poolID, miner string, limit, offset int) ([]PaymentRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var (
		q    string
		args []any
	)
	if miner != "" {
		q = `SELECT id, miner, address, amount_sats, txid, status, created
			FROM payments WHERE poolid = $1 AND miner = $2
			ORDER BY created DESC LIMIT $3 OFFSET $4`
		args = []any{poolID, miner, limit, offset}
	} else {
		q = `SELECT id, miner, address, amount_sats, txid, status, created
			FROM payments WHERE poolid = $1
			ORDER BY created DESC LIMIT $2 OFFSET $3`
		args = []any{poolID, limit, offset}
	}
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PaymentRow{}
	for rows.Next() {
		var p PaymentRow
		if err := rows.Scan(&p.ID, &p.Miner, &p.Address, &p.AmountSats,
			&p.TxID, &p.Status, &p.Created); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

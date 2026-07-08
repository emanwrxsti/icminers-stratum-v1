package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/rewards"
)

// UnconfirmedBlocks returns a pool's blocks still awaiting confirmation.
func (s *Store) UnconfirmedBlocks(ctx context.Context, poolID string) ([]rewards.Block, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, poolid, blockheight, miner, hash, reward_sats, networkdifficulty, created
		FROM blocks
		WHERE poolid = $1 AND status = 'pending'
		ORDER BY blockheight ASC`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRewardBlocks(rows)
}

// ConfirmedUnrewarded returns confirmed blocks whose rewards are not yet
// credited.
func (s *Store) ConfirmedUnrewarded(ctx context.Context, poolID string) ([]rewards.Block, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, poolid, blockheight, miner, hash, reward_sats, networkdifficulty, created
		FROM blocks
		WHERE poolid = $1 AND status = 'confirmed' AND rewarded = false
		ORDER BY blockheight ASC`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRewardBlocks(rows)
}

type pgRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanRewardBlocks(rows pgRows) ([]rewards.Block, error) {
	out := []rewards.Block{}
	for rows.Next() {
		var b rewards.Block
		if err := rows.Scan(&b.ID, &b.PoolID, &b.Height, &b.Miner, &b.Hash,
			&b.RewardSats, &b.NetworkDifficulty, &b.Created); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UpdateBlockConfirmation persists a confirmation check outcome.
func (s *Store) UpdateBlockConfirmation(ctx context.Context, blockID int64, status string, confirmations int64, progress float64) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE blocks
		SET status = $2, confirmations = $3, confirmationprogress = $4
		WHERE id = $1`, blockID, status, confirmations, progress)
	return err
}

// --- rewards.ShareSource implementation ---

// WorkBetween implements rewards.ShareSource.
func (s *Store) WorkBetween(ctx context.Context, poolID string, from, to time.Time) ([]rewards.MinerWork, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT miner, SUM(difficulty)
		FROM shares
		WHERE poolid = $1 AND created > $2 AND created <= $3
		GROUP BY miner`, poolID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []rewards.MinerWork{}
	for rows.Next() {
		var w rewards.MinerWork
		if err := rows.Scan(&w.Miner, &w.DiffSum); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// workBackwardPage is the page size for the PPLNS backward walk.
const workBackwardPage = 10000

// WorkBackward implements rewards.ShareSource: pages newest-first from
// `before` and accumulates per-miner difficulty until targetDiff is counted.
// The share that crosses the target is counted in full (classic PPLNS).
func (s *Store) WorkBackward(ctx context.Context, poolID string, before time.Time, targetDiff float64) ([]rewards.MinerWork, error) {
	sums := map[string]float64{}
	var collected float64
	cursor := before
	cursorSet := false
	for collected < targetDiff {
		var rows interface {
			Next() bool
			Scan(...any) error
			Err() error
			Close()
		}
		var err error
		if !cursorSet {
			rows, err = s.Pool.Query(ctx, `
				SELECT miner, difficulty, created FROM shares
				WHERE poolid = $1 AND created <= $2
				ORDER BY created DESC LIMIT $3`, poolID, cursor, workBackwardPage)
		} else {
			rows, err = s.Pool.Query(ctx, `
				SELECT miner, difficulty, created FROM shares
				WHERE poolid = $1 AND created < $2
				ORDER BY created DESC LIMIT $3`, poolID, cursor, workBackwardPage)
		}
		if err != nil {
			return nil, err
		}
		n := 0
		var lastCreated time.Time
		for rows.Next() {
			var miner string
			var diff float64
			var created time.Time
			if err := rows.Scan(&miner, &diff, &created); err != nil {
				rows.Close()
				return nil, err
			}
			n++
			lastCreated = created
			if collected >= targetDiff {
				continue // drain the page but stop counting
			}
			sums[miner] += diff
			collected += diff
		}
		closeErr := rows.Err()
		rows.Close()
		if closeErr != nil {
			return nil, closeErr
		}
		if n == 0 {
			break // shares exhausted
		}
		cursor = lastCreated
		cursorSet = true
		if n < workBackwardPage && collected < targetDiff {
			// This page was the last available; done even if under target.
			break
		}
	}
	out := make([]rewards.MinerWork, 0, len(sums))
	for miner, diff := range sums {
		out = append(out, rewards.MinerWork{Miner: miner, DiffSum: diff})
	}
	return out, nil
}

// PreviousBlockTime implements rewards.ShareSource.
func (s *Store) PreviousBlockTime(ctx context.Context, poolID string, before time.Time) (time.Time, bool, error) {
	var t time.Time
	err := s.Pool.QueryRow(ctx, `
		SELECT created FROM blocks
		WHERE poolid = $1 AND created < $2
		ORDER BY created DESC LIMIT 1`, poolID, before).Scan(&t)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	return t, true, nil
}

// CreditBlockRewards atomically writes a block's credits: one balance_changes
// row per credit, balance upserts, and the block's rewarded flag — all in a
// single transaction guarded by a row lock on the block, so re-running the
// processor can never double-credit.
func (s *Store) CreditBlockRewards(ctx context.Context, block rewards.Block, credits []rewards.Credit, feeSats int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("credit: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the block row and re-check the flag inside the transaction.
	var rewarded bool
	if err := tx.QueryRow(ctx,
		`SELECT rewarded FROM blocks WHERE id = $1 FOR UPDATE`, block.ID).Scan(&rewarded); err != nil {
		return fmt.Errorf("credit: lock block %d: %w", block.ID, err)
	}
	if rewarded {
		return nil // another run got here first
	}

	for _, c := range credits {
		if _, err := tx.Exec(ctx, `
			INSERT INTO balance_changes (poolid, miner, amount_sats, usage, blockheight)
			VALUES ($1, $2, $3, 'block-reward', $4)`,
			block.PoolID, c.Miner, c.AmountSats, block.Height); err != nil {
			return fmt.Errorf("credit: balance change: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO balances (poolid, miner, amount_sats, updated)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (poolid, miner)
			DO UPDATE SET amount_sats = balances.amount_sats + EXCLUDED.amount_sats,
			              updated = now()`,
			block.PoolID, c.Miner, c.AmountSats); err != nil {
			return fmt.Errorf("credit: balance upsert: %w", err)
		}
	}
	if feeSats > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO balance_changes (poolid, miner, amount_sats, usage, blockheight)
			VALUES ($1, '', $2, 'pool-fee', $3)`,
			block.PoolID, feeSats, block.Height); err != nil {
			return fmt.Errorf("credit: fee change: %w", err)
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE blocks SET rewarded = true WHERE id = $1`, block.ID); err != nil {
		return fmt.Errorf("credit: mark rewarded: %w", err)
	}
	return tx.Commit(ctx)
}

// MinerBalance returns a miner's balance in base units.
func (s *Store) MinerBalance(ctx context.Context, poolID, miner string) (int64, error) {
	var sats int64
	err := s.Pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT amount_sats FROM balances WHERE poolid = $1 AND miner = $2), 0)`,
		poolID, miner).Scan(&sats)
	return sats, err
}

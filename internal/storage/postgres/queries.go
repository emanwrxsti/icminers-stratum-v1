package postgres

import (
	"context"
	"time"
)

// BlockRow is a persisted block as served by the API.
type BlockRow struct {
	ID                int64     `json:"id"`
	PoolID            string    `json:"poolId"`
	BlockHeight       int64     `json:"blockHeight"`
	NetworkDifficulty float64   `json:"networkDifficulty"`
	Status            string    `json:"status"`
	Miner             string    `json:"miner"`
	Worker            string    `json:"worker"`
	Hash              string    `json:"hash"`
	Source            string    `json:"source"`
	Created           time.Time `json:"created"`
}

// ListBlocks returns a pool's blocks, newest first.
func (s *Store) ListBlocks(ctx context.Context, poolID string, limit, offset int) ([]BlockRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT id, poolid, blockheight, networkdifficulty, status,
		       miner, worker, hash, source, created
		FROM blocks
		WHERE poolid = $1
		ORDER BY created DESC
		LIMIT $2 OFFSET $3`, poolID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BlockRow{}
	for rows.Next() {
		var b BlockRow
		if err := rows.Scan(&b.ID, &b.PoolID, &b.BlockHeight, &b.NetworkDifficulty,
			&b.Status, &b.Miner, &b.Worker, &b.Hash, &b.Source, &b.Created); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// MinerRow is one miner's aggregated share work over a window.
type MinerRow struct {
	Miner      string    `json:"miner"`
	ShareCount int64     `json:"shareCount"`
	DiffSum    float64   `json:"diffSum"`
	Hashrate   float64   `json:"hashrate"` // H/s over the window
	LastShare  time.Time `json:"lastShare"`
}

// TopMiners aggregates share work per miner over the window, largest first.
func (s *Store) TopMiners(ctx context.Context, poolID string, window time.Duration, limit int) ([]MinerRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if window <= 0 {
		window = time.Hour
	}
	since := time.Now().UTC().Add(-window)
	rows, err := s.Pool.Query(ctx, `
		SELECT miner, COUNT(*), SUM(difficulty), MAX(created)
		FROM shares
		WHERE poolid = $1 AND created >= $2
		GROUP BY miner
		ORDER BY SUM(difficulty) DESC
		LIMIT $3`, poolID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MinerRow{}
	for rows.Next() {
		var m MinerRow
		if err := rows.Scan(&m.Miner, &m.ShareCount, &m.DiffSum, &m.LastShare); err != nil {
			return nil, err
		}
		m.Hashrate = m.DiffSum * 4294967296.0 / window.Seconds()
		out = append(out, m)
	}
	return out, rows.Err()
}

// WorkerRow is one worker's aggregated share work over a window.
type WorkerRow struct {
	Worker     string    `json:"worker"`
	ShareCount int64     `json:"shareCount"`
	DiffSum    float64   `json:"diffSum"`
	Hashrate   float64   `json:"hashrate"`
	LastShare  time.Time `json:"lastShare"`
}

// MinerWorkers aggregates one miner's share work per worker over the window.
func (s *Store) MinerWorkers(ctx context.Context, poolID, miner string, window time.Duration) ([]WorkerRow, error) {
	if window <= 0 {
		window = time.Hour
	}
	since := time.Now().UTC().Add(-window)
	rows, err := s.Pool.Query(ctx, `
		SELECT worker, COUNT(*), SUM(difficulty), MAX(created)
		FROM shares
		WHERE poolid = $1 AND miner = $2 AND created >= $3
		GROUP BY worker
		ORDER BY SUM(difficulty) DESC`, poolID, miner, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WorkerRow{}
	for rows.Next() {
		var w WorkerRow
		if err := rows.Scan(&w.Worker, &w.ShareCount, &w.DiffSum, &w.LastShare); err != nil {
			return nil, err
		}
		w.Hashrate = w.DiffSum * 4294967296.0 / window.Seconds()
		out = append(out, w)
	}
	return out, rows.Err()
}

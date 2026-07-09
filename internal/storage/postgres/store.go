// Package postgres implements Stage 4 persistence: the connection pool,
// embedded schema migrations, the partitioned shares table, the blocks table,
// and an async batched share writer that keeps every database write off the
// stratum submit hot path.
//
// Schema layout follows the architecture spec: shares are range-partitioned
// by creation time (monthly) so retention becomes cheap partition drops, and
// the writer creates upcoming partitions on demand.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// Store owns the pgx connection pool.
type Store struct {
	Pool *pgxpool.Pool
	log  *logging.Logger
}

// New connects, verifies the connection, and applies migrations.
func New(ctx context.Context, dsn string, log *logging.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	s := &Store{Pool: pool, log: logging.Component(log, "postgres")}
	if err := s.applyMigrations(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// migrations run in order inside one transaction each; the applied version is
// recorded in schema_migrations. Additive-only by policy.
var migrations = []string{
	// 001: shares, partitioned monthly by created.
	`CREATE TABLE IF NOT EXISTS schema_migrations (
		version    int PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS shares (
		poolid            text        NOT NULL,
		blockheight       bigint      NOT NULL,
		difficulty        double precision NOT NULL,
		networkdifficulty double precision NOT NULL,
		miner             text        NOT NULL,
		worker            text        NOT NULL DEFAULT '',
		useragent         text        NOT NULL DEFAULT '',
		ipaddress         text        NOT NULL DEFAULT '',
		source            text        NOT NULL DEFAULT '',
		created           timestamptz NOT NULL
	) PARTITION BY RANGE (created)`,
	`CREATE INDEX IF NOT EXISTS idx_shares_pool_miner_created
		ON shares (poolid, miner, created)`,
	`CREATE INDEX IF NOT EXISTS idx_shares_pool_created
		ON shares (poolid, created)`,
	// 002: blocks.
	`CREATE TABLE IF NOT EXISTS blocks (
		id                      bigserial   PRIMARY KEY,
		poolid                  text        NOT NULL,
		blockheight             bigint      NOT NULL,
		networkdifficulty       double precision NOT NULL DEFAULT 0,
		status                  text        NOT NULL DEFAULT 'pending',
		confirmationprogress    double precision NOT NULL DEFAULT 0,
		effort                  double precision,
		minereffort             double precision,
		transactionconfirmationdata text    NOT NULL DEFAULT '',
		miner                   text        NOT NULL DEFAULT '',
		worker                  text        NOT NULL DEFAULT '',
		reward                  numeric(28,8) NOT NULL DEFAULT 0,
		hash                    text        NOT NULL DEFAULT '',
		source                  text        NOT NULL DEFAULT '',
		created                 timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_pool_height_hash
		ON blocks (poolid, blockheight, hash)`,
	`CREATE INDEX IF NOT EXISTS idx_blocks_pool_status
		ON blocks (poolid, status)`,
	// 003: Stage 7 rewards — exact-integer accounting in base units (sats).
	`ALTER TABLE blocks
		ADD COLUMN IF NOT EXISTS reward_sats bigint NOT NULL DEFAULT 0,
		ADD COLUMN IF NOT EXISTS rewarded boolean NOT NULL DEFAULT false,
		ADD COLUMN IF NOT EXISTS confirmations bigint NOT NULL DEFAULT 0`,
	`CREATE TABLE IF NOT EXISTS balances (
		poolid      text   NOT NULL,
		miner       text   NOT NULL,
		amount_sats bigint NOT NULL DEFAULT 0,
		updated     timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (poolid, miner)
	)`,
	`CREATE TABLE IF NOT EXISTS balance_changes (
		id          bigserial PRIMARY KEY,
		poolid      text   NOT NULL,
		miner       text   NOT NULL,
		amount_sats bigint NOT NULL,
		usage       text   NOT NULL DEFAULT '',
		blockheight bigint NOT NULL DEFAULT 0,
		created     timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS idx_balance_changes_pool_miner
		ON balance_changes (poolid, miner, created)`,
	// 004: Stage 9 payouts.
	`CREATE TABLE IF NOT EXISTS payments (
		id          bigserial PRIMARY KEY,
		poolid      text   NOT NULL,
		miner       text   NOT NULL,
		address     text   NOT NULL,
		amount_sats bigint NOT NULL,
		txid        text   NOT NULL DEFAULT '',
		status      text   NOT NULL DEFAULT 'pending',
		batch_id    text   NOT NULL DEFAULT '',
		created     timestamptz NOT NULL DEFAULT now(),
		updated     timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS idx_payments_pool_status
		ON payments (poolid, status)`,
	`CREATE INDEX IF NOT EXISTS idx_payments_pool_miner
		ON payments (poolid, miner, created)`,

	// 005: share id for idempotent persistence. A stable id assigned when the
	// share is accepted lets the durable WAL replay records after a crash or a
	// commit-then-error without double-counting.
	`ALTER TABLE shares ADD COLUMN IF NOT EXISTS id text NOT NULL DEFAULT ''`,
	// 006: unique index on (id, created). On a partitioned table a unique
	// index must include the partition key (created). Empty ids (legacy rows)
	// are excluded via a partial index so they don't collide.
	`CREATE UNIQUE INDEX IF NOT EXISTS uq_shares_id_created
		ON shares (id, created) WHERE id <> ''`,
}

// applyMigrations records progress in schema_migrations and only runs pending
// statements, so restarts are idempotent.
func (s *Store) applyMigrations(ctx context.Context) error {
	// Bootstrap: statement 0 creates schema_migrations itself.
	if _, err := s.Pool.Exec(ctx, migrations[0]); err != nil {
		return fmt.Errorf("postgres: bootstrap migrations table: %w", err)
	}
	var current int
	if err := s.Pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("postgres: read migration version: %w", err)
	}
	for v := current + 1; v <= len(migrations); v++ {
		stmt := migrations[v-1]
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("postgres: begin migration %d: %w", v, err)
		}
		if _, err := tx.Exec(ctx, stmt); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("postgres: migration %d: %w", v, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, v); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("postgres: record migration %d: %w", v, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("postgres: commit migration %d: %w", v, err)
		}
		s.log.Info("applied migration", "version", v)
	}
	return nil
}

// EnsureSharePartition creates the monthly partition covering t (and is a
// no-op when it already exists). Called by the writer for the current and next
// month so month rollover never drops writes.
func (s *Store) EnsureSharePartition(ctx context.Context, t time.Time) error {
	t = t.UTC()
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("shares_%04d_%02d", start.Year(), int(start.Month()))
	stmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF shares FOR VALUES FROM ('%s') TO ('%s')`,
		name, start.Format("2006-01-02"), end.Format("2006-01-02"))
	if _, err := s.Pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: ensure partition %s: %w", name, err)
	}
	return nil
}

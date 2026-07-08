package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// ShareRecord is one accepted share bound for the shares table.
type ShareRecord struct {
	PoolID            string
	BlockHeight       int64
	Difficulty        float64 // share difficulty credited (worker difficulty)
	NetworkDifficulty float64
	Miner             string
	Worker            string
	UserAgent         string
	IPAddress         string
	Source            string // region
	Created           time.Time
}

// BlockRecord is one found block candidate bound for the blocks table.
type BlockRecord struct {
	PoolID            string
	BlockHeight       int64
	NetworkDifficulty float64
	Miner             string
	Worker            string
	Hash              string
	// RewardSats is the exact coinbase value in base units.
	RewardSats int64
	Source     string
	Created    time.Time
}

// WriterOptions tune the async share writer.
type WriterOptions struct {
	// QueueSize is the buffered channel capacity. When full, new shares are
	// DROPPED (with a counter) rather than blocking the submit path.
	QueueSize int
	// BatchSize triggers a flush when this many shares are queued.
	BatchSize int
	// FlushInterval triggers a flush even when the batch is small.
	FlushInterval time.Duration
}

func (o *WriterOptions) defaults() {
	if o.QueueSize <= 0 {
		o.QueueSize = 65536
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 500
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 2 * time.Second
	}
}

// ShareWriter batches shares and bulk-inserts them with COPY. Enqueue never
// blocks and never returns an error: the stratum hot path must not feel the
// database. Failed flushes are retried once immediately, then the batch is
// re-queued at the front (up to a bounded number of retained batches) so a
// short database outage loses nothing.
type ShareWriter struct {
	store *Store
	log   *logging.Logger
	opts  WriterOptions

	ch      chan ShareRecord
	dropped atomic.Uint64
	written atomic.Uint64

	mu      sync.Mutex
	pending []ShareRecord // retained after failed flushes (bounded)

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// maxRetainedShares bounds memory during a database outage; beyond it, oldest
// retained shares are dropped (counted).
const maxRetainedShares = 250_000

// NewShareWriter starts the background flusher.
func NewShareWriter(store *Store, log *logging.Logger, opts WriterOptions) *ShareWriter {
	opts.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	w := &ShareWriter{
		store:  store,
		log:    logging.Component(log, "sharewriter"),
		opts:   opts,
		ch:     make(chan ShareRecord, opts.QueueSize),
		cancel: cancel,
	}
	// Make sure the current and next month partitions exist before accepting.
	now := time.Now().UTC()
	for _, t := range []time.Time{now, now.AddDate(0, 1, 0)} {
		if err := store.EnsureSharePartition(ctx, t); err != nil {
			w.log.Error("partition bootstrap failed", "err", err)
		}
	}
	w.wg.Add(1)
	go w.loop(ctx)
	return w
}

// Enqueue hands a share to the writer. NEVER blocks: on a full queue the share
// is dropped and counted, keeping the submit path latency flat.
func (w *ShareWriter) Enqueue(rec ShareRecord) {
	select {
	case w.ch <- rec:
	default:
		w.dropped.Add(1)
	}
}

// Stats returns (written, dropped) counters.
func (w *ShareWriter) Stats() (written, dropped uint64) {
	return w.written.Load(), w.dropped.Load()
}

// Close flushes what it can and stops the loop.
func (w *ShareWriter) Close() {
	w.cancel()
	w.wg.Wait()
}

func (w *ShareWriter) loop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()

	batch := make([]ShareRecord, 0, w.opts.BatchSize)
	lastMonth := time.Now().UTC().Month()

	flush := func() {
		if len(batch) == 0 && len(w.pending) == 0 {
			return
		}
		w.mu.Lock()
		toWrite := append(w.pending, batch...)
		w.pending = nil
		w.mu.Unlock()
		batch = batch[:0]

		if err := w.copyShares(ctx, toWrite); err != nil {
			// One immediate retry (transient errors), then retain.
			if err2 := w.copyShares(ctx, toWrite); err2 != nil {
				w.retain(toWrite)
				w.log.Error("share flush failed; batch retained",
					"shares", len(toWrite), "err", err2)
				return
			}
		}
		w.written.Add(uint64(len(toWrite)))
	}

	for {
		select {
		case <-ctx.Done():
			// Drain the channel, then final flush with a fresh timeout context.
			for {
				select {
				case rec := <-w.ch:
					batch = append(batch, rec)
				default:
					fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					w.mu.Lock()
					toWrite := append(w.pending, batch...)
					w.pending = nil
					w.mu.Unlock()
					if len(toWrite) > 0 {
						if err := w.copySharesCtx(fctx, toWrite); err != nil {
							w.log.Error("final share flush failed", "shares", len(toWrite), "err", err)
						} else {
							w.written.Add(uint64(len(toWrite)))
						}
					}
					cancel()
					return
				}
			}
		case rec := <-w.ch:
			batch = append(batch, rec)
			if len(batch) >= w.opts.BatchSize {
				flush()
			}
		case <-ticker.C:
			// Month rollover: pre-create the next partition.
			if m := time.Now().UTC().Month(); m != lastMonth {
				lastMonth = m
				now := time.Now().UTC()
				for _, t := range []time.Time{now, now.AddDate(0, 1, 0)} {
					if err := w.store.EnsureSharePartition(ctx, t); err != nil {
						w.log.Error("partition rollover failed", "err", err)
					}
				}
			}
			flush()
		}
	}
}

func (w *ShareWriter) retain(recs []ShareRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = append(w.pending, recs...)
	if over := len(w.pending) - maxRetainedShares; over > 0 {
		w.dropped.Add(uint64(over))
		w.pending = w.pending[over:]
	}
}

func (w *ShareWriter) copyShares(ctx context.Context, recs []ShareRecord) error {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return w.copySharesCtx(cctx, recs)
}

func (w *ShareWriter) copySharesCtx(ctx context.Context, recs []ShareRecord) error {
	if len(recs) == 0 {
		return nil
	}
	_, err := w.store.Pool.CopyFrom(ctx,
		pgx.Identifier{"shares"},
		[]string{"poolid", "blockheight", "difficulty", "networkdifficulty",
			"miner", "worker", "useragent", "ipaddress", "source", "created"},
		pgx.CopyFromSlice(len(recs), func(i int) ([]any, error) {
			r := recs[i]
			return []any{r.PoolID, r.BlockHeight, r.Difficulty, r.NetworkDifficulty,
				r.Miner, r.Worker, r.UserAgent, r.IPAddress, r.Source, r.Created}, nil
		}))
	return err
}

// InsertBlock records a found block candidate with status "pending".
// Idempotent on (poolid, blockheight, hash).
func (s *Store) InsertBlock(ctx context.Context, b BlockRecord) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO blocks
			(poolid, blockheight, networkdifficulty, status, miner, worker, hash, reward_sats, source, created)
		VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8, $9)
		ON CONFLICT (poolid, blockheight, hash) DO NOTHING`,
		b.PoolID, b.BlockHeight, b.NetworkDifficulty, b.Miner, b.Worker, b.Hash, b.RewardSats, b.Source, b.Created)
	return err
}

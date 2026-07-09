package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/spool"
)

// shareWALSubject tags share records in the spool (the spool is generic over a
// subject/payload pair; the writer only ever stores shares here).
const shareWALSubject = "share"

// shareWAL is a durable write-ahead log for accepted shares. When the
// in-memory writer cannot absorb a share (queue full, or the retention bound
// is hit during a database outage), the share is appended here instead of
// being dropped. A recovery loop drains the WAL back into the database once it
// is healthy again. This is what makes accepted-share persistence durable:
// nothing that was acknowledged to a miner is silently lost.
type shareWAL struct {
	sp  *spool.Spool
	log *logging.Logger
}

// openShareWAL opens (or creates) the WAL at path with a size bound.
func openShareWAL(path string, maxBytes int64, log *logging.Logger) (*shareWAL, error) {
	sp, err := spool.Open(path, maxBytes)
	if err != nil {
		return nil, err
	}
	return &shareWAL{sp: sp, log: logging.Component(log, "share-wal")}, nil
}

// append durably records one share. Returns an error only when even the WAL
// could not accept it (disk full / size bound), which is the sole remaining
// path to genuine loss and is loudly logged by the caller.
func (w *shareWAL) append(rec ShareRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return w.sp.Append(shareWALSubject, payload)
}

// len returns the WAL's on-disk byte size (0 = empty).
func (w *shareWAL) len() int64 { return w.sp.Len() }

// walDrainBatchSize is how many WAL records are inserted per DB round-trip
// during recovery.
const walDrainBatchSize = 1000

// drain replays WAL records into the database in batches. Crucially, a batch
// is only removed from the WAL AFTER insert returns nil for it: if the
// database is still down, the failing batch and everything after it stay on
// disk for the next attempt. This is the fix for the original bug, where
// records were truncated from the WAL before the (possibly failing) database
// insert, losing them in exactly the outage scenario the WAL exists for.
func (w *shareWAL) drain(insert func(recs []ShareRecord) error) error {
	return w.sp.DrainBatch(walDrainBatchSize, func(records []spool.Record) error {
		recs := make([]ShareRecord, 0, len(records))
		for _, r := range records {
			var rec ShareRecord
			if err := json.Unmarshal(r.Payload, &rec); err != nil {
				// A corrupt line is skipped rather than wedging recovery.
				w.log.Error("skipping corrupt WAL record", "err", err)
				continue
			}
			recs = append(recs, rec)
		}
		if len(recs) == 0 {
			return nil
		}
		return insert(recs)
	})
}

// close releases the WAL file.
func (w *shareWAL) close() error { return w.sp.Close() }

// recoveryLoop periodically drains the WAL into the database while the writer
// runs. It only attempts a drain when the WAL is non-empty.
func (w *ShareWriter) recoveryLoop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.opts.WALDrainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.wal == nil || w.wal.len() == 0 {
				continue
			}
			err := w.wal.drain(func(recs []ShareRecord) error {
				dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				return w.copySharesIdempotent(dctx, recs)
			})
			if err != nil {
				w.log.Warn("WAL drain incomplete; will retry", "err", err)
			} else {
				w.log.Info("WAL drained into database")
			}
		}
	}
}

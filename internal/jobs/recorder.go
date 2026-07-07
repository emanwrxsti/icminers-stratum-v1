package jobs

import (
	"strings"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
)

// ShareEvent describes an accepted share for downstream persistence/stats.
// The jobs package deliberately knows nothing about PostgreSQL; main adapts
// these events to the storage layer (and, in Stage 6, to NATS).
type ShareEvent struct {
	PoolID            string
	BlockHeight       int64
	ShareDiff         float64 // achieved difficulty
	WorkerDiff        float64 // difficulty credited
	NetworkDifficulty float64
	Miner             string
	Worker            string
	UserAgent         string
	IPAddress         string
	Created           time.Time
	IsBlockCandidate  bool
}

// BlockEvent describes a found block candidate.
type BlockEvent struct {
	PoolID            string
	BlockHeight       int64
	NetworkDifficulty float64
	Miner             string
	Worker            string
	Hash              string
	Created           time.Time
}

// Recorder receives share/block events off the validation path. Implementations
// MUST be non-blocking (enqueue-and-return); they are invoked synchronously
// after a share is accepted.
type Recorder interface {
	RecordShare(ev ShareEvent)
	RecordBlock(ev BlockEvent)
}

// SetRecorder wires persistence. May be nil (no-op).
func (m *Manager) SetRecorder(r Recorder) {
	m.mu.Lock()
	m.recorder = r
	m.mu.Unlock()
}

// SetRecorder wires the recorder into every registered manager.
func (r *Registry) SetRecorder(rec Recorder) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, m := range r.managers {
		m.SetRecorder(rec)
	}
}

// splitWorker separates "address.rig" into miner and worker parts.
func splitWorker(full string) (miner, worker string) {
	if i := strings.LastIndex(full, "."); i > 0 {
		return full[:i], full[i+1:]
	}
	return full, ""
}

// record emits events for an accepted share (and its block, when a candidate).
func (m *Manager) record(job *coins.MiningJob, submit coins.ShareSubmit, result *coins.ShareResult, networkDiff float64) {
	m.mu.RLock()
	rec := m.recorder
	m.mu.RUnlock()
	if rec == nil {
		return
	}
	miner, worker := splitWorker(submit.Worker)
	now := time.Now().UTC()
	rec.RecordShare(ShareEvent{
		PoolID:            m.poolID,
		BlockHeight:       job.Height,
		ShareDiff:         result.ShareDiff,
		WorkerDiff:        submit.WorkerDiff,
		NetworkDifficulty: networkDiff,
		Miner:             miner,
		Worker:            worker,
		UserAgent:         submit.UserAgent,
		IPAddress:         submit.RemoteIP,
		Created:           now,
		IsBlockCandidate:  result.BlockCandidate,
	})
	if result.BlockCandidate {
		rec.RecordBlock(BlockEvent{
			PoolID:            m.poolID,
			BlockHeight:       job.Height,
			NetworkDifficulty: networkDiff,
			Miner:             miner,
			Worker:            worker,
			Hash:              result.BlockHash,
			Created:           now,
		})
	}
}

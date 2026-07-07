package jobs

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// networkDifficulty derives the network difficulty from the job's target.
func networkDifficulty(job *coins.MiningJob) float64 {
	if len(job.NetworkTarget) == 0 {
		return 0
	}
	return vardiff.TargetToDifficulty(new(big.Int).SetBytes(job.NetworkTarget))
}

// Submit-path errors, mapped to Stratum error codes by the handler.
var (
	// ErrJobNotFound: unknown or evicted (stale) job id.
	ErrJobNotFound = errors.New("job not found")
	// ErrDuplicateShare: identical share already submitted for this job.
	ErrDuplicateShare = errors.New("duplicate share")
	// ErrLowDifficulty: share does not meet the worker's share target.
	ErrLowDifficulty = errors.New("low difficulty share")
)

// blockSubmitTimeout bounds the async submitblock RPC.
const blockSubmitTimeout = 30 * time.Second

// SubmitShare validates one mining.submit against a remembered job. It is the
// hot path: pure in-memory work (duplicate check + hash validation); the only
// I/O — submitblock for a block candidate — runs in a detached goroutine so
// the miner's reply is never delayed. Persistence lands in Stage 4 behind an
// async writer, per the architecture spec.
func (m *Manager) SubmitShare(ctx context.Context, submit coins.ShareSubmit) (*coins.ShareResult, error) {
	m.mu.RLock()
	job, ok := m.byID[submit.JobID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrJobNotFound, submit.JobID)
	}

	// Duplicate detection BEFORE the (more expensive) hash validation. The key
	// covers every field that changes the header; extranonce1 is included so
	// two miners cannot collide.
	dupKey := strings.Join([]string{
		submit.ExtraNonce1, submit.ExtraNonce2, submit.NTime, submit.Nonce, submit.VersionBits,
	}, ":")
	m.mu.Lock()
	seen := m.dupByJob[submit.JobID]
	if seen == nil {
		seen = make(map[string]struct{})
		m.dupByJob[submit.JobID] = seen
	}
	if _, dup := seen[dupKey]; dup {
		m.mu.Unlock()
		return nil, ErrDuplicateShare
	}
	seen[dupKey] = struct{}{}
	m.mu.Unlock()

	result, err := m.adapter.ValidateShare(ctx, job, submit)
	if err != nil {
		return nil, err
	}
	if result.BlockCandidate {
		m.log.Info("BLOCK CANDIDATE",
			"jobId", submit.JobID, "height", job.Height,
			"hash", result.BlockHash, "worker", submit.Worker, "shareDiff", result.ShareDiff)
		m.submitBlockAsync(result.BlockHash, result.BlockHex, job.Height)
	}
	if !result.Valid && !result.BlockCandidate {
		return result, fmt.Errorf("%w: %g < %g", ErrLowDifficulty, result.ShareDiff, submit.WorkerDiff)
	}
	m.log.Debug("share accepted",
		"jobId", submit.JobID, "worker", submit.Worker,
		"shareDiff", result.ShareDiff, "workerDiff", submit.WorkerDiff)
	m.record(job, submit, result, networkDifficulty(job))
	return result, nil
}

// submitBlockAsync pushes a candidate to the daemon off the reply path. A
// failed submit is loud in the logs but cannot take the pool down.
func (m *Manager) submitBlockAsync(blockHash, blockHex string, height int64) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.log.Error("panic in block submit", "recover", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), blockSubmitTimeout)
		defer cancel()
		if err := m.adapter.SubmitBlock(ctx, blockHex); err != nil {
			m.log.Error("block submit FAILED", "height", height, "hash", blockHash, "err", err)
			return
		}
		m.log.Info("block submit ACCEPTED", "height", height, "hash", blockHash)
	}()
}

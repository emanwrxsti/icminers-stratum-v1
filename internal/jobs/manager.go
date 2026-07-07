// Package jobs owns per-pool mining jobs: it polls the coin daemon for block
// templates, derives stratum jobs, stamps job ids, remembers recent jobs for
// the Stage 3 submit path, and broadcasts mining.notify to the pool's miners.
// One Manager exists per pool; a Registry maps pool ids to managers so the
// stratum handler can fetch the current job for newly-authorized sessions.
package jobs

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/btc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// Broadcaster delivers a notification to every subscribed miner on a pool.
// Implemented by the stratum server.
type Broadcaster interface {
	BroadcastNotify(poolID string, params []any)
}

// maxRememberedJobs bounds the per-pool job history kept for submit lookups.
const maxRememberedJobs = 8

// Manager owns jobs for exactly one pool.
type Manager struct {
	poolID  string
	adapter coins.CoinAdapter
	log     *logging.Logger

	mu          sync.RWMutex
	broadcaster Broadcaster
	recorder    Recorder
	current     *coins.MiningJob
	byID        map[string]*coins.MiningJob
	dupByJob    map[string]map[string]struct{} // per-job duplicate-share sets
	order       []string                       // insertion order for eviction
	jobCounter  uint64
	lastPrev    string
	lastCurTime int64
	lastTxCount int
	lastValue   int64
}

// NewManager builds a job manager for one pool.
func NewManager(poolID string, adapter coins.CoinAdapter, log *logging.Logger) *Manager {
	return &Manager{
		poolID:   poolID,
		adapter:  adapter,
		log:      logging.Component(logging.ForPool(log, poolID), "jobs"),
		byID:     make(map[string]*coins.MiningJob),
		dupByJob: make(map[string]map[string]struct{}),
	}
}

// SetBroadcaster wires the notify fan-out. Called once at startup after the
// stratum server exists (the server depends on the registry, so this breaks
// the construction cycle).
func (m *Manager) SetBroadcaster(b Broadcaster) {
	m.mu.Lock()
	m.broadcaster = b
	m.mu.Unlock()
}

// Poll implements the pool.Poller contract: fetch a template, and when it
// meaningfully changed, derive + broadcast a new job. Errors are returned to
// the pool supervisor, which moves only this pool into the error state.
func (m *Manager) Poll(ctx context.Context) error {
	tpl, err := m.adapter.GetBlockTemplate(ctx)
	if err != nil {
		return fmt.Errorf("pool %s: %w", m.poolID, err)
	}

	txCount := 0
	if txs, ok := tpl.Raw["transactions"].([]any); ok {
		txCount = len(txs)
	}

	m.mu.RLock()
	prevChanged := tpl.PreviousBlockHash != m.lastPrev
	updated := prevChanged ||
		tpl.CurTime != m.lastCurTime ||
		txCount != m.lastTxCount ||
		tpl.CoinbaseValue != m.lastValue
	m.mu.RUnlock()

	if !updated {
		return nil
	}

	job, err := m.adapter.BuildMiningJob(ctx, tpl, "")
	if err != nil {
		return fmt.Errorf("pool %s: build job: %w", m.poolID, err)
	}

	m.mu.Lock()
	m.jobCounter++
	jobID := strconv.FormatUint(m.jobCounter, 16)
	stampJob(job, m.poolID, jobID, prevChanged)

	m.current = job
	m.byID[jobID] = job
	m.order = append(m.order, jobID)
	for len(m.order) > maxRememberedJobs {
		delete(m.byID, m.order[0])
		delete(m.dupByJob, m.order[0]) // dup entries die with their job
		m.order = m.order[1:]
	}
	m.lastPrev = tpl.PreviousBlockHash
	m.lastCurTime = tpl.CurTime
	m.lastTxCount = txCount
	m.lastValue = tpl.CoinbaseValue
	b := m.broadcaster
	params := job.NotifyParams
	m.mu.Unlock()

	m.log.Info("new job", "jobId", jobID, "height", job.Height, "clean", prevChanged, "txs", txCount)
	if b != nil {
		b.BroadcastNotify(m.poolID, params)
	}
	return nil
}

// stampJob assigns the job id and (for bitcoinbase-backed jobs) regenerates
// the notify parameters with the final id and clean flag.
func stampJob(job *coins.MiningJob, poolID, jobID string, clean bool) {
	job.JobID = jobID
	job.PoolID = poolID
	job.CleanJobs = clean
	if base, err := btc.BitcoinbaseJob(job); err == nil {
		base.JobID = jobID
		base.CleanJob = clean
		job.NotifyParams = base.NotifyParams()
	}
}

// CurrentNotify returns the notify params of the current job, with cleanJobs
// forced true so a newly-(re)connected miner always starts fresh work.
func (m *Manager) CurrentNotify() ([]any, bool) {
	m.mu.RLock()
	job := m.current
	m.mu.RUnlock()
	if job == nil {
		return nil, false
	}
	if base, err := btc.BitcoinbaseJob(job); err == nil {
		fresh := *base
		fresh.CleanJob = true
		return fresh.NotifyParams(), true
	}
	if len(job.NotifyParams) > 0 {
		return job.NotifyParams, true
	}
	return nil, false
}

// Get returns a remembered job by id (Stage 3 submit path).
func (m *Manager) Get(jobID string) (*coins.MiningJob, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	j, ok := m.byID[jobID]
	return j, ok
}

// Registry maps pool ids to job managers.
type Registry struct {
	mu       sync.RWMutex
	managers map[string]*Manager
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{managers: make(map[string]*Manager)}
}

// Add registers a pool's manager.
func (r *Registry) Add(m *Manager) {
	r.mu.Lock()
	r.managers[m.poolID] = m
	r.mu.Unlock()
}

// Get returns the manager for a pool.
func (r *Registry) Get(poolID string) (*Manager, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.managers[poolID]
	return m, ok
}

// CurrentNotify implements the stratum handler's JobSource: the current job's
// notify params for a pool, if any.
func (r *Registry) CurrentNotify(poolID string) ([]any, bool) {
	m, ok := r.Get(poolID)
	if !ok {
		return nil, false
	}
	return m.CurrentNotify()
}

// SubmitShare routes a mining.submit to the owning pool's manager.
func (r *Registry) SubmitShare(ctx context.Context, poolID string, submit coins.ShareSubmit) (*coins.ShareResult, error) {
	m, ok := r.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q has no job manager", ErrJobNotFound, poolID)
	}
	return m.SubmitShare(ctx, submit)
}

// SetBroadcaster wires the broadcaster into every registered manager.
func (r *Registry) SetBroadcaster(b Broadcaster) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, m := range r.managers {
		m.SetBroadcaster(b)
	}
}

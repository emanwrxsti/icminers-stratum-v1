package pool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/icminers/gostratumpool/internal/config"
	"github.com/icminers/gostratumpool/internal/logging"
)

// ErrUnknownPool is returned for operations on a pool id that is not loaded.
type ErrUnknownPool struct{ PoolID string }

func (e ErrUnknownPool) Error() string { return fmt.Sprintf("unknown pool %q", e.PoolID) }

// PoolLifecycleManager owns every pool Service and mediates lifecycle actions.
// It is the single place where pool state is mutated, which keeps the state
// machine coherent across the admin API, the NATS consumer (regional nodes),
// and internal supervision.
//
// Crucially, an action on one pool only ever touches that pool. There is no
// global stop/restart path — by design.
type PoolLifecycleManager struct {
	log  *logging.Logger
	hook StateHook

	mu    sync.RWMutex
	pools map[string]*Service
	ctx   context.Context
}

// NewManager builds a manager from pool configs. Pools are constructed in their
// configured initial state but not started until Start is called.
func NewManager(log *logging.Logger, hook StateHook, pools []config.PoolConfig) *PoolLifecycleManager {
	m := &PoolLifecycleManager{
		log:   logging.Component(log, "lifecycle"),
		hook:  hook,
		pools: make(map[string]*Service, len(pools)),
	}
	for _, pc := range pools {
		if !pc.Enabled {
			// Load it but force disabled so ports mapped to it refuse traffic.
			pc.InitialState = config.StateDisabled
		}
		m.pools[pc.ID] = newService(pc, log, hook)
	}
	return m
}

// Start launches loops for all non-disabled pools under ctx.
func (m *PoolLifecycleManager) Start(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	services := make([]*Service, 0, len(m.pools))
	for _, s := range m.pools {
		services = append(services, s)
	}
	m.mu.Unlock()

	for _, s := range services {
		s.start(ctx)
	}
	m.log.Info("pool lifecycle manager started", "pools", len(services))
}

// Stop tears down every pool loop. This is process shutdown, not a per-pool op.
func (m *PoolLifecycleManager) Stop() {
	m.mu.RLock()
	services := make([]*Service, 0, len(m.pools))
	for _, s := range m.pools {
		services = append(services, s)
	}
	m.mu.RUnlock()
	for _, s := range services {
		s.stop()
	}
}

func (m *PoolLifecycleManager) get(poolID string) (*Service, error) {
	m.mu.RLock()
	s, ok := m.pools[poolID]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrUnknownPool{PoolID: poolID}
	}
	return s, nil
}

// ---- Public lifecycle operations (mirror the spec's PoolLifecycleManager) ----

// StartPool starts (or re-starts into active) a pool's loop. Used to bring a
// disabled pool online.
func (m *PoolLifecycleManager) StartPool(poolID string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	if s.State() == StateDisabled {
		if err := s.setState(StateActive, "StartPool"); err != nil {
			return err
		}
		if m.ctx != nil {
			s.start(m.ctx)
		}
		return nil
	}
	return s.setState(StateActive, "StartPool")
}

// StopPool cancels a pool's loop and marks it disabled. Only this pool stops.
func (m *PoolLifecycleManager) StopPool(poolID string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	if err := s.setState(StateDisabled, "StopPool"); err != nil {
		return err
	}
	s.stop()
	return nil
}

// PausePool temporarily stops a pool. Its miner sessions should be rejected;
// other pools continue.
func (m *PoolLifecycleManager) PausePool(poolID string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	return s.setState(StatePaused, "PausePool")
}

// ResumePool returns a paused/maintenance/error pool to active.
func (m *PoolLifecycleManager) ResumePool(poolID string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	return s.setState(StateActive, "ResumePool")
}

// DrainPool stops new jobs but keeps accepting shares for existing jobs for
// grace, then auto-advances to maintenance.
func (m *PoolLifecycleManager) DrainPool(poolID string, grace time.Duration) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	return s.beginDrain(grace, "DrainPool")
}

// PutPoolInMaintenance stops template polling and job notifications for this
// pool and rejects new authorizations, while other pools keep running.
func (m *PoolLifecycleManager) PutPoolInMaintenance(poolID, reason string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	if reason != "" {
		s.mu.Lock()
		s.maintenanceMessage = reason
		s.mu.Unlock()
	}
	return s.setState(StateMaintenance, reasonOr(reason, "PutPoolInMaintenance"))
}

// DisablePool marks a pool disabled and stops its loop. Ports mapped only to it
// will refuse connections.
func (m *PoolLifecycleManager) DisablePool(poolID string) error {
	return m.StopPool(poolID)
}

// RestartPool drains briefly then brings the pool back to active. Only this pool
// is affected; the global stratum server keeps running throughout.
func (m *PoolLifecycleManager) RestartPool(poolID string) error {
	s, err := m.get(poolID)
	if err != nil {
		return err
	}
	// Cycle the loop: stop then start again in active state.
	s.stop()
	if err := s.setState(StateActive, "RestartPool"); err != nil {
		// Force from disabled if needed.
		s.mu.Lock()
		s.state = StateActive
		s.stateChangedAt = time.Now()
		s.mu.Unlock()
	}
	if m.ctx != nil {
		s.start(m.ctx)
	}
	return nil
}

// GetPoolState returns a pool's current lifecycle state.
func (m *PoolLifecycleManager) GetPoolState(poolID string) (State, error) {
	s, err := m.get(poolID)
	if err != nil {
		return "", err
	}
	return s.State(), nil
}

// ---- Query helpers used by the stratum server and API ----

// AcceptsNewAuthorization reports whether a pool will accept new worker logins.
// Unknown pools return (false, error).
func (m *PoolLifecycleManager) AcceptsNewAuthorization(poolID string) (bool, error) {
	s, err := m.get(poolID)
	if err != nil {
		return false, err
	}
	return s.State().AcceptsNewAuthorization(), nil
}

// MaintenanceMessage returns the miner-facing maintenance message for a pool.
func (m *PoolLifecycleManager) MaintenanceMessage(poolID string) string {
	s, err := m.get(poolID)
	if err != nil {
		return ""
	}
	return s.MaintenanceMessage()
}

// ApplyRemoteState applies a state change received from the master over NATS.
// Regional nodes call this; it only ever touches the matching pool.
func (m *PoolLifecycleManager) ApplyRemoteState(poolID string, to State, reason string) error {
	switch to {
	case StatePaused:
		return m.PausePool(poolID)
	case StateActive:
		return m.ResumePool(poolID)
	case StateMaintenance:
		return m.PutPoolInMaintenance(poolID, reason)
	case StateDisabled:
		return m.DisablePool(poolID)
	case StateDraining:
		return m.DrainPool(poolID, 60*time.Second)
	default:
		return fmt.Errorf("cannot apply remote state %q", to)
	}
}

// Snapshot returns a single pool's status.
func (m *PoolLifecycleManager) Snapshot(poolID string) (Snapshot, error) {
	s, err := m.get(poolID)
	if err != nil {
		return Snapshot{}, err
	}
	return s.snapshot(), nil
}

// SnapshotAll returns every pool's status, sorted by id for stable output.
func (m *PoolLifecycleManager) SnapshotAll() []Snapshot {
	m.mu.RLock()
	out := make([]Snapshot, 0, len(m.pools))
	for _, s := range m.pools {
		out = append(out, s.snapshot())
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })
	return out
}

func reasonOr(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}

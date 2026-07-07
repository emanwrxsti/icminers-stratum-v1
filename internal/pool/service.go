package pool

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// StateHook is notified whenever a pool changes state. The master uses this to
// publish state changes over NATS; regional nodes and the API use it to react.
// Hooks must not block for long and must never panic the caller.
type StateHook interface {
	OnPoolStateChange(poolID string, from, to State, reason string)
}

// Poller is the per-pool work body invoked on every template-poll tick while
// the pool's state allows polling. Implemented by the job manager (Stage 2+).
// A returned error moves ONLY this pool into the error state; the supervisor
// backs off and retries. Implementations must never call os.Exit.
type Poller interface {
	Poll(ctx context.Context) error
}

// Service is a single pool/coin running in isolation. It owns its own context,
// goroutine, and (in later stages) job manager, template poller, share
// validator, duplicate cache, vardiff state, and daemon RPC client. All of that
// work happens under a panic-recovered loop so a bug in one coin cannot crash
// the process.
type Service struct {
	cfg  config.PoolConfig
	log  *logging.Logger
	hook StateHook

	mu                 sync.RWMutex
	state              State
	maintenanceMessage string
	lastError          string
	stateChangedAt     time.Time

	// loop lifecycle
	loopCtx    context.Context
	loopCancel context.CancelFunc
	loopWG     sync.WaitGroup

	// drainTimer transitions draining -> maintenance after the grace period.
	drainTimer *time.Timer

	// poller is the pool's work body (job manager). Nil pollers idle.
	poller Poller
}

// newService builds a Service in its configured initial state without starting
// its loop.
func newService(cfg config.PoolConfig, log *logging.Logger, hook StateHook) *Service {
	initial := State(cfg.InitialState)
	if !initial.Valid() {
		initial = StateActive
	}
	return &Service{
		cfg:                cfg,
		log:                logging.ForPool(log, cfg.ID),
		hook:               hook,
		state:              initial,
		maintenanceMessage: cfg.MaintenanceMessage,
		stateChangedAt:     time.Now(),
	}
}

// SetPoller wires the pool's work body. Must be called before Start.
func (s *Service) SetPoller(p Poller) {
	s.mu.Lock()
	s.poller = p
	s.mu.Unlock()
}

// State returns the current lifecycle state.
func (s *Service) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// MaintenanceMessage returns the message shown to miners during maintenance.
func (s *Service) MaintenanceMessage() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.maintenanceMessage != "" {
		return s.maintenanceMessage
	}
	return fmt.Sprintf("Pool %s is under maintenance. Please reconnect later.", s.cfg.ID)
}

// setState performs a guarded transition and fires the hook. It returns an error
// if the transition is illegal. Callers hold no lock.
func (s *Service) setState(to State, reason string) error {
	if !to.Valid() {
		return fmt.Errorf("invalid target state %q", to)
	}
	s.mu.Lock()
	from := s.state
	if !canTransition(from, to) {
		s.mu.Unlock()
		return fmt.Errorf("illegal transition %s -> %s", from, to)
	}
	s.state = to
	s.stateChangedAt = time.Now()
	if to == StateError {
		s.lastError = reason
	}
	s.mu.Unlock()

	if from != to {
		s.log.Info("pool state change", "from", from, "to", to, "reason", reason)
		if s.hook != nil {
			// Fire hooks without holding the lock; guard against a panicking hook.
			func() {
				defer func() { _ = recover() }()
				s.hook.OnPoolStateChange(s.cfg.ID, from, to, reason)
			}()
		}
	}
	return nil
}

// start launches the pool's supervised loop under the parent context. It is a
// no-op if the pool is disabled.
func (s *Service) start(parent context.Context) {
	if s.State() == StateDisabled {
		s.log.Info("pool is disabled; not starting loop")
		return
	}
	s.loopCtx, s.loopCancel = context.WithCancel(parent)
	s.loopWG.Add(1)
	go s.superviseLoop()
}

// stop cancels the loop and waits for it to exit.
func (s *Service) stop() {
	s.mu.Lock()
	if s.drainTimer != nil {
		s.drainTimer.Stop()
		s.drainTimer = nil
	}
	cancel := s.loopCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.loopWG.Wait()
}

// superviseLoop runs the pool's work loop and restarts it on panic, moving the
// pool into the error state and backing off. This is the guarantee that one
// coin's bug cannot crash the whole stratum.
func (s *Service) superviseLoop() {
	defer s.loopWG.Done()
	backoff := s.cfg.ErrorBackoff.D()
	if backoff <= 0 {
		backoff = 15 * time.Second
	}

	for {
		if s.loopCtx.Err() != nil {
			return
		}
		err := s.runOnce(s.loopCtx)
		if s.loopCtx.Err() != nil {
			return
		}
		if err != nil {
			_ = s.setState(StateError, err.Error())
			s.log.Warn("pool loop errored; backing off", "err", err, "backoff", backoff)
			select {
			case <-s.loopCtx.Done():
				return
			case <-time.After(backoff):
			}
			// Attempt recovery back to active before re-running.
			if s.State() == StateError {
				_ = s.setState(StateActive, "retry after backoff")
			}
		}
	}
}

// runOnce is the pool's actual work body. It recovers from panics and converts
// them into errors so superviseLoop can back off rather than crash. In Stage 1
// this is a heartbeat placeholder; Stage 2+ wires in the template poller and job
// manager here, all still under this recovery boundary.
func (s *Service) runOnce(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pool %s panicked: %v\n%s", s.cfg.ID, r, debug.Stack())
		}
	}()

	interval := s.cfg.TemplatePollInterval.D()
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.log.Debug("pool loop started")
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Only poll templates / build jobs when the state allows it. Other
			// states keep the loop alive but idle so operators can resume
			// without restarting the goroutine.
			if s.State().RunsTemplatePolling() {
				if err := s.pollOnce(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// pollOnce invokes the pool's poller (the job manager). Pools without a
// poller idle; a poller error propagates so the supervisor moves this pool
// (only) into the error state and backs off.
func (s *Service) pollOnce(ctx context.Context) error {
	s.mu.RLock()
	p := s.poller
	s.mu.RUnlock()
	if p == nil {
		return nil
	}
	return p.Poll(ctx)
}

// beginDrain moves the pool to draining and schedules the transition to
// maintenance after grace. Safe to call repeatedly.
func (s *Service) beginDrain(grace time.Duration, reason string) error {
	if err := s.setState(StateDraining, reason); err != nil {
		return err
	}
	s.mu.Lock()
	if s.drainTimer != nil {
		s.drainTimer.Stop()
	}
	s.drainTimer = time.AfterFunc(grace, func() {
		// Only auto-advance if we are still draining (operator may have acted).
		if s.State() == StateDraining {
			_ = s.setState(StateMaintenance, "drain grace period elapsed")
		}
	})
	s.mu.Unlock()
	return nil
}

// Snapshot is a read-only view of a pool's status for the API.
type Snapshot struct {
	PoolID             string    `json:"poolId"`
	CoinSymbol         string    `json:"coinSymbol"`
	State              State     `json:"state"`
	MaintenanceMessage string    `json:"maintenanceMessage,omitempty"`
	LastError          string    `json:"lastError,omitempty"`
	StateChangedAt     time.Time `json:"stateChangedAt"`
	PaymentMode        string    `json:"paymentMode,omitempty"`
}

// snapshot builds a status snapshot.
func (s *Service) snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{
		PoolID:             s.cfg.ID,
		CoinSymbol:         s.cfg.CoinSymbol,
		State:              s.state,
		MaintenanceMessage: s.maintenanceMessage,
		LastError:          s.lastError,
		StateChangedAt:     s.stateChangedAt,
		PaymentMode:        s.cfg.PaymentMode,
	}
}

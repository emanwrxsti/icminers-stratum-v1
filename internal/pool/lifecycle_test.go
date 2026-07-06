package pool

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/icminers/gostratumpool/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testPools() []config.PoolConfig {
	return []config.PoolConfig{
		{ID: "alph-shared", Enabled: true, PaymentMode: "pplns", InitialState: config.StateActive,
			MaintenanceMessage: "ALPH shared under maintenance", ErrorBackoff: config.Duration(10 * time.Millisecond)},
		{ID: "alph-solo", Enabled: true, PaymentMode: "solo", InitialState: config.StateActive},
		{ID: "btc-solo", Enabled: true, PaymentMode: "solo", InitialState: config.StateActive},
		{ID: "rxd-shared", Enabled: false},
	}
}

func newTestManager(t *testing.T) (*PoolLifecycleManager, context.CancelFunc) {
	t.Helper()
	m := NewManager(testLogger(), nil, testPools())
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	return m, cancel
}

func TestInitialStates(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if st, _ := m.GetPoolState("alph-shared"); st != StateActive {
		t.Errorf("alph-shared = %q", st)
	}
	// enabled:false must load as disabled so its ports refuse traffic.
	if st, _ := m.GetPoolState("rxd-shared"); st != StateDisabled {
		t.Errorf("rxd-shared = %q, want disabled", st)
	}
}

func TestMaintenanceIsolatedToOnePool(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.PutPoolInMaintenance("alph-shared", ""); err != nil {
		t.Fatalf("maintenance: %v", err)
	}

	// The target pool no longer accepts new authorizations.
	if ok, _ := m.AcceptsNewAuthorization("alph-shared"); ok {
		t.Error("alph-shared should reject new auth in maintenance")
	}
	if msg := m.MaintenanceMessage("alph-shared"); msg != "ALPH shared under maintenance" {
		t.Errorf("maintenance message = %q", msg)
	}

	// Every other pool is completely unaffected.
	for _, id := range []string{"alph-solo", "btc-solo"} {
		if ok, _ := m.AcceptsNewAuthorization(id); !ok {
			t.Errorf("%s should still accept auth", id)
		}
		if st, _ := m.GetPoolState(id); st != StateActive {
			t.Errorf("%s state = %q, want active", id, st)
		}
	}
}

func TestPauseResumeCycle(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.PausePool("alph-solo"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("alph-solo"); st != StatePaused {
		t.Errorf("state = %q, want paused", st)
	}
	if err := m.ResumePool("alph-solo"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("alph-solo"); st != StateActive {
		t.Errorf("state = %q, want active", st)
	}
}

func TestDrainAdvancesToMaintenance(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.DrainPool("alph-shared", 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("alph-shared"); st != StateDraining {
		t.Fatalf("state = %q, want draining", st)
	}
	// Draining still accepts shares for existing jobs but no new auth.
	if !StateDraining.AcceptsShares() {
		t.Error("draining should accept shares")
	}
	if StateDraining.AcceptsNewAuthorization() {
		t.Error("draining should not accept new auth")
	}

	time.Sleep(90 * time.Millisecond)
	if st, _ := m.GetPoolState("alph-shared"); st != StateMaintenance {
		t.Errorf("after grace state = %q, want maintenance", st)
	}
}

func TestDisableAndStart(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.DisablePool("btc-solo"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("btc-solo"); st != StateDisabled {
		t.Errorf("state = %q, want disabled", st)
	}
	if err := m.StartPool("btc-solo"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("btc-solo"); st != StateActive {
		t.Errorf("state = %q, want active", st)
	}
}

func TestUnknownPool(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.PausePool("does-not-exist"); err == nil {
		t.Fatal("expected error for unknown pool")
	}
}

func TestIllegalTransitionRejected(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	// disabled -> draining is not a legal transition.
	_ = m.DisablePool("btc-solo")
	if err := m.DrainPool("btc-solo", time.Second); err == nil {
		t.Fatal("expected illegal transition error")
	}
}

// recordingHook captures state changes; one variant panics to prove hook panics
// cannot break the manager.
type recordingHook struct {
	mu      sync.Mutex
	changes int
	panic   bool
}

func (h *recordingHook) OnPoolStateChange(_ string, _, _ State, _ string) {
	if h.panic {
		panic("hook boom")
	}
	h.mu.Lock()
	h.changes++
	h.mu.Unlock()
}

func TestHookFiresOnChange(t *testing.T) {
	h := &recordingHook{}
	m := NewManager(testLogger(), h, testPools())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	defer m.Stop()

	_ = m.PausePool("alph-solo")
	h.mu.Lock()
	got := h.changes
	h.mu.Unlock()
	if got == 0 {
		t.Error("expected hook to fire on state change")
	}
}

func TestPanickingHookDoesNotBreakManager(t *testing.T) {
	h := &recordingHook{panic: true}
	m := NewManager(testLogger(), h, testPools())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	defer m.Stop()

	// Must not panic the caller.
	if err := m.PausePool("alph-solo"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("alph-solo"); st != StatePaused {
		t.Errorf("state = %q, want paused despite panicking hook", st)
	}
}

func TestApplyRemoteStateRoutes(t *testing.T) {
	m, cancel := newTestManager(t)
	defer cancel()
	defer m.Stop()

	if err := m.ApplyRemoteState("alph-solo", StateMaintenance, "remote"); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.GetPoolState("alph-solo"); st != StateMaintenance {
		t.Errorf("state = %q, want maintenance", st)
	}
}

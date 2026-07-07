package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/btc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// fakeDaemon serves getblocktemplate with a mutable template.
type fakeDaemon struct {
	mu       sync.Mutex
	template map[string]any
}

func (f *fakeDaemon) set(mutate func(m map[string]any)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mutate(f.template)
}

func (f *fakeDaemon) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		tpl := make(map[string]any, len(f.template))
		for k, v := range f.template {
			tpl[k] = v
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID, "result": tpl, "error": nil})
	}))
}

func baseTemplate() map[string]any {
	return map[string]any{
		"version":           536870912,
		"rules":             []string{"segwit"},
		"previousblockhash": strings.Repeat("ab", 32),
		"transactions": []map[string]any{
			{"data": "aa", "txid": strings.Repeat("11", 32)},
		},
		"coinbasevalue": 312500000,
		"mintime":       1700000000,
		"curtime":       1700000600,
		"bits":          "1d00ffff",
		"height":        840001,
	}
}

type captureBroadcaster struct {
	mu    sync.Mutex
	calls []struct {
		poolID string
		params []any
	}
}

func (c *captureBroadcaster) BroadcastNotify(poolID string, params []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, struct {
		poolID string
		params []any
	}{poolID, params})
}

func (c *captureBroadcaster) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *captureBroadcaster) last(t *testing.T) []any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		t.Fatal("no broadcasts captured")
	}
	return c.calls[len(c.calls)-1].params
}

func newTestManager(t *testing.T, url string) *Manager {
	t.Helper()
	adapter, err := btc.New(btc.Options{
		RPC:         rpc.New(rpc.Options{URL: url}),
		PoolAddress: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
		CoinbaseTag: "/TEST/",
	})
	if err != nil {
		t.Fatal(err)
	}
	log := logging.New(logging.Options{Level: "error"})
	return NewManager("btc-test", adapter, log)
}

// TestPollBuildsAndBroadcastsJob covers the happy path: a poll produces a
// stamped job with valid notify params and fans it out.
func TestPollBuildsAndBroadcastsJob(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	cb := &captureBroadcaster{}
	m.SetBroadcaster(cb)

	if err := m.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cb.count() != 1 {
		t.Fatalf("broadcasts = %d, want 1", cb.count())
	}
	params := cb.last(t)
	if len(params) != 9 {
		t.Fatalf("notify params len = %d, want 9", len(params))
	}
	jobID, ok := params[0].(string)
	if !ok || jobID == "" {
		t.Fatalf("jobId = %v", params[0])
	}
	// First job after startup has a prevhash "change" -> cleanJobs true.
	if params[8] != true {
		t.Fatalf("cleanJobs = %v, want true on first job", params[8])
	}
	// Job must be retrievable by id for the Stage 3 submit path.
	if _, ok := m.Get(jobID); !ok {
		t.Fatalf("job %q not remembered", jobID)
	}
	// The whole array must marshal (wire format).
	if _, err := json.Marshal(params); err != nil {
		t.Fatal(err)
	}
}

// TestPollDeduplicatesUnchangedTemplate: identical templates produce no new
// job and no broadcast.
func TestPollDeduplicatesUnchangedTemplate(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	cb := &captureBroadcaster{}
	m.SetBroadcaster(cb)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := m.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if cb.count() != 1 {
		t.Fatalf("broadcasts = %d, want 1 (dedup failed)", cb.count())
	}
}

// TestPollCleanFlagSemantics: a curtime-only refresh is NOT clean; a prevhash
// change (new chain tip) IS clean.
func TestPollCleanFlagSemantics(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	cb := &captureBroadcaster{}
	m.SetBroadcaster(cb)
	ctx := context.Background()

	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	// Refresh: same tip, newer curtime.
	f.set(func(tpl map[string]any) { tpl["curtime"] = 1700000660 })
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := cb.last(t)[8]; got != false {
		t.Fatalf("curtime refresh cleanJobs = %v, want false", got)
	}
	// New tip.
	f.set(func(tpl map[string]any) {
		tpl["previousblockhash"] = strings.Repeat("cd", 32)
		tpl["height"] = 840002
	})
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	last := cb.last(t)
	if last[8] != true {
		t.Fatalf("new tip cleanJobs = %v, want true", last[8])
	}
	if cb.count() != 3 {
		t.Fatalf("broadcasts = %d, want 3", cb.count())
	}
}

// TestCurrentNotifyForcesClean: a miner authorizing mid-job must get the
// current job with cleanJobs forced true.
func TestCurrentNotifyForcesClean(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	ctx := context.Background()

	if _, ok := m.CurrentNotify(); ok {
		t.Fatal("CurrentNotify before any poll must report no job")
	}
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	// Force a non-clean refresh so the stored job's flag is false.
	f.set(func(tpl map[string]any) { tpl["curtime"] = 1700000700 })
	if err := m.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	params, ok := m.CurrentNotify()
	if !ok {
		t.Fatal("no current notify")
	}
	if params[8] != true {
		t.Fatalf("CurrentNotify cleanJobs = %v, want forced true", params[8])
	}
}

// TestJobHistoryEviction: only the most recent maxRememberedJobs survive.
func TestJobHistoryEviction(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	m := newTestManager(t, srv.URL)
	ctx := context.Background()

	firstID := ""
	for i := 0; i < maxRememberedJobs+2; i++ {
		f.set(func(tpl map[string]any) { tpl["curtime"] = 1700000600 + i })
		if err := m.Poll(ctx); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			m.mu.RLock()
			firstID = m.order[0]
			m.mu.RUnlock()
		}
	}
	if _, ok := m.Get(firstID); ok {
		t.Fatal("oldest job should have been evicted")
	}
	m.mu.RLock()
	n := len(m.byID)
	m.mu.RUnlock()
	if n != maxRememberedJobs {
		t.Fatalf("remembered jobs = %d, want %d", n, maxRememberedJobs)
	}
}

// TestPollErrorPropagates: a dead daemon must surface as an error (the pool
// supervisor turns this into the error state for that pool only).
func TestPollErrorPropagates(t *testing.T) {
	m := newTestManager(t, "http://127.0.0.1:1")
	if err := m.Poll(context.Background()); err == nil {
		t.Fatal("expected error from unreachable daemon")
	}
}

// TestRegistry covers lookup and broadcaster fan-in.
func TestRegistry(t *testing.T) {
	f := &fakeDaemon{template: baseTemplate()}
	srv := f.server()
	defer srv.Close()

	r := NewRegistry()
	m := newTestManager(t, srv.URL)
	r.Add(m)

	if _, ok := r.Get("nope"); ok {
		t.Fatal("unknown pool must not resolve")
	}
	if _, ok := r.CurrentNotify("btc-test"); ok {
		t.Fatal("no job yet")
	}
	cb := &captureBroadcaster{}
	r.SetBroadcaster(cb)
	if err := m.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cb.count() != 1 {
		t.Fatal("registry-wired broadcaster not invoked")
	}
	if _, ok := r.CurrentNotify("btc-test"); !ok {
		t.Fatal("registry CurrentNotify failed after poll")
	}
}

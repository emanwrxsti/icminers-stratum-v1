package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stats"
)

func testConfig() *config.Config {
	return &config.Config{
		Mode:   "all-in-one",
		Region: "us",
		NodeID: "api-test",
		Stratum: config.StratumConfig{
			Ports: []config.PortConfig{
				{Port: 3032, PoolID: "p1", Difficulty: 1024},
				{Port: 3033, PoolID: "p2", VarDiff: true, Difficulty: 512},
			},
		},
		Pools: []config.PoolConfig{
			{ID: "p1", Enabled: true, CoinSymbol: "BTC", PaymentMode: "solo",
				MaintenanceMessage: "p1 down for maintenance"},
			{ID: "p2", Enabled: true, CoinSymbol: "BTC", PaymentMode: "pplns"},
		},
	}
}

// newTestServer builds a Server with a real, started lifecycle manager.
func newTestServer(t *testing.T, adminToken string) (*Server, *pool.PoolLifecycleManager, *stats.Collector) {
	t.Helper()
	cfg := testConfig()
	log := logging.New(logging.Options{Level: "error"})
	lc := pool.NewManager(log, nil, cfg.Pools)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	lc.Start(ctx)
	t.Cleanup(func() { lc.Stop() })

	col := stats.New()
	srv := New(Options{
		Config:       cfg,
		Lifecycle:    lc,
		Stats:        col,
		Store:        nil,
		SessionCount: func() int { return 7 },
		AdminToken:   adminToken,
		Log:          log,
	})
	return srv, lc, col
}

func doReq(t *testing.T, h http.Handler, method, path, token string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func TestHealth(t *testing.T) {
	srv, _, _ := newTestServer(t, "secret")
	rec, out := doReq(t, srv.Handler(), "GET", "/api/health", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if out["status"] != "ok" || out["sessions"] != float64(7) || out["pools"] != float64(2) {
		t.Fatalf("health = %v", out)
	}
	if out["database"] != false {
		t.Fatal("database must report false without a store")
	}
}

func TestPoolsPublic(t *testing.T) {
	srv, _, col := newTestServer(t, "")
	col.RecordShare(jobs.ShareEvent{PoolID: "p1", Miner: "m1", Worker: "w1", WorkerDiff: 100})

	req := httptest.NewRequest("GET", "/api/pools", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var pools []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools) != 2 {
		t.Fatalf("pools = %d", len(pools))
	}
	byID := map[string]map[string]any{}
	for _, p := range pools {
		byID[p["id"].(string)] = p
	}
	if byID["p1"]["state"] != "active" {
		t.Fatalf("p1 state = %v", byID["p1"]["state"])
	}
	live := byID["p1"]["live"].(map[string]any)
	if live["shares"] != float64(1) {
		t.Fatalf("p1 live = %v", live)
	}
	ports := byID["p2"]["ports"].([]any)
	if len(ports) != 1 || ports[0].(map[string]any)["varDiff"] != true {
		t.Fatalf("p2 ports = %v", ports)
	}

	// Single pool + 404.
	rec2, out := doReq(t, srv.Handler(), "GET", "/api/pools/p1", "", nil)
	if rec2.Code != http.StatusOK || out["id"] != "p1" {
		t.Fatalf("pool p1: %d %v", rec2.Code, out)
	}
	rec3, _ := doReq(t, srv.Handler(), "GET", "/api/pools/ghost", "", nil)
	if rec3.Code != http.StatusNotFound {
		t.Fatalf("ghost pool status = %d", rec3.Code)
	}
}

func TestDBEndpointsWithoutStore(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	for _, path := range []string{"/api/pools/p1/blocks", "/api/pools/p1/miners"} {
		rec, _ := doReq(t, srv.Handler(), "GET", path, "", nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status = %d, want 503", path, rec.Code)
		}
	}
	// Miner endpoint degrades gracefully: live-only, no workers key.
	rec, out := doReq(t, srv.Handler(), "GET", "/api/pools/p1/miners/bc1qx", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("miner status = %d", rec.Code)
	}
	if _, has := out["workers"]; has {
		t.Fatal("workers present without a store")
	}
}

func TestAdminAuth(t *testing.T) {
	srv, _, _ := newTestServer(t, "secret")
	// No token.
	rec, _ := doReq(t, srv.Handler(), "POST", "/api/admin/pools/p1/pause", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token status = %d", rec.Code)
	}
	// Wrong token.
	rec, _ = doReq(t, srv.Handler(), "POST", "/api/admin/pools/p1/pause", "wrong", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d", rec.Code)
	}
	// Right token.
	rec, out := doReq(t, srv.Handler(), "POST", "/api/admin/pools/p1/pause", "secret", nil)
	if rec.Code != http.StatusOK || out["state"] != "paused" {
		t.Fatalf("pause: %d %v", rec.Code, out)
	}
}

func TestAdminRoutesAbsentWithoutToken(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	rec, _ := doReq(t, srv.Handler(), "POST", "/api/admin/pools/p1/pause", "anything", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("admin without configured token: status = %d, want 404", rec.Code)
	}
}

func TestAdminLifecycleFlowAndIsolation(t *testing.T) {
	srv, lc, _ := newTestServer(t, "secret")
	h := srv.Handler()

	// Maintenance with message on p1 only.
	rec, out := doReq(t, h, "POST", "/api/admin/pools/p1/maintenance", "secret",
		map[string]any{"message": "upgrading daemon"})
	if rec.Code != http.StatusOK || out["state"] != "maintenance" || out["maintenanceMessage"] != "upgrading daemon" {
		t.Fatalf("maintenance: %d %v", rec.Code, out)
	}
	// p2 untouched (isolation).
	if st, _ := lc.GetPoolState("p2"); st != pool.StateActive {
		t.Fatalf("p2 state = %s, want active", st)
	}
	// Resume p1.
	rec, out = doReq(t, h, "POST", "/api/admin/pools/p1/resume", "secret", nil)
	if rec.Code != http.StatusOK || out["state"] != "active" {
		t.Fatalf("resume: %d %v", rec.Code, out)
	}

	// Drain with a short grace, then observe maintenance.
	rec, out = doReq(t, h, "POST", "/api/admin/pools/p1/drain", "secret",
		map[string]any{"gracePeriodSeconds": 1})
	if rec.Code != http.StatusOK || out["state"] != "draining" {
		t.Fatalf("drain: %d %v", rec.Code, out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := lc.GetPoolState("p1")
		if st == pool.StateMaintenance {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain never reached maintenance (state %s)", st)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Restart back to active.
	rec, out = doReq(t, h, "POST", "/api/admin/pools/p1/restart", "secret", nil)
	if rec.Code != http.StatusOK || out["state"] != "active" {
		t.Fatalf("restart: %d %v", rec.Code, out)
	}

	// Disable, then an illegal pause is a 409.
	rec, _ = doReq(t, h, "POST", "/api/admin/pools/p1/disable", "secret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("disable: %d", rec.Code)
	}
	rec, _ = doReq(t, h, "POST", "/api/admin/pools/p1/pause", "secret", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("pause disabled pool: %d, want 409", rec.Code)
	}

	// Unknown pool is a 404.
	rec, _ = doReq(t, h, "POST", "/api/admin/pools/ghost/pause", "secret", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown pool: %d, want 404", rec.Code)
	}
	// State endpoint.
	rec, out = doReq(t, h, "GET", "/api/admin/pools/p2/state", "secret", nil)
	if rec.Code != http.StatusOK || out["state"] != "active" {
		t.Fatalf("state: %d %v", rec.Code, out)
	}
}

func TestAdminPublishFirstAuthority(t *testing.T) {
	srv, lc, _ := newTestServer(t, "secret")
	type captured struct {
		poolID, action, message string
		grace                   int
	}
	var cmds []captured
	srv.opts.PublishCommand = func(poolID, action, message string, graceSeconds int) error {
		cmds = append(cmds, captured{poolID, action, message, graceSeconds})
		return nil
	}
	h := srv.Handler()

	// Local apply works: 200 + published.
	rec, out := doReq(t, h, "POST", "/api/admin/pools/p1/pause", "secret", nil)
	if rec.Code != http.StatusOK || out["state"] != "paused" {
		t.Fatalf("pause: %d %v", rec.Code, out)
	}
	// A pool this node does not host: still published, 202.
	rec, out = doReq(t, h, "POST", "/api/admin/pools/remote-only/maintenance", "secret",
		map[string]any{"message": "upgrade"})
	if rec.Code != http.StatusAccepted || out["published"] != true {
		t.Fatalf("remote-only: %d %v", rec.Code, out)
	}
	// Illegal local transition (disable p2 then maintenance): still published, 202.
	if _, err := doReq(t, h, "POST", "/api/admin/pools/p2/disable", "secret", nil); false {
		_ = err
	}
	rec, out = doReq(t, h, "POST", "/api/admin/pools/p2/maintenance", "secret",
		map[string]any{"message": "x"})
	if rec.Code != http.StatusAccepted || out["published"] != true {
		t.Fatalf("illegal local: %d %v", rec.Code, out)
	}
	if len(cmds) != 4 {
		t.Fatalf("published commands = %d, want 4", len(cmds))
	}
	if cmds[1].poolID != "remote-only" || cmds[1].action != "maintenance" || cmds[1].message != "upgrade" {
		t.Fatalf("cmd[1] = %+v", cmds[1])
	}
	_ = lc
}

package nats

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/spool"
)

// natsURL returns a NATS URL for tests: POOL_TEST_NATS_URL if set, otherwise a
// throwaway `nats-server -js` spawned on a free port. Skips when neither is
// available.
func natsURL(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("POOL_TEST_NATS_URL"); url != "" {
		return url
	}
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not in PATH and POOL_TEST_NATS_URL unset; skipping")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.Command(bin, "-js", "-p", fmt.Sprint(port), "-sd", t.TempDir())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return url
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("nats-server did not come up")
	return ""
}

func testClient(t *testing.T, url, region string) *Client {
	t.Helper()
	log := logging.New(logging.Options{Level: "error"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Connect(ctx, Options{
		URLs: []string{url}, Name: "test-" + region, Region: region,
		Log: log, RequireStreams: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// memSink collects consumed events.
type memSink struct {
	mu     sync.Mutex
	shares []ShareEventMsg
	blocks []BlockEventMsg
	states []StateChangeMsg
}

func (m *memSink) SinkShare(ev ShareEventMsg) {
	m.mu.Lock()
	m.shares = append(m.shares, ev)
	m.mu.Unlock()
}
func (m *memSink) SinkBlock(ev BlockEventMsg) {
	m.mu.Lock()
	m.blocks = append(m.blocks, ev)
	m.mu.Unlock()
}
func (m *memSink) SinkStateChange(ev StateChangeMsg) {
	m.mu.Lock()
	m.states = append(m.states, ev)
	m.mu.Unlock()
}

func (m *memSink) counts() (int, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.shares), len(m.blocks), len(m.states)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestPublishConsumeRoundtrip: regional publisher -> master consumer, all
// three event kinds, master sees the regional's region tag.
func TestPublishConsumeRoundtrip(t *testing.T) {
	url := natsURL(t)
	regional := testClient(t, url, "eu")
	master := testClient(t, url, "master")
	log := logging.New(logging.Options{Level: "error"})

	sink := &memSink{}
	cons, err := StartConsumer(context.Background(), master, sink, log)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Stop()

	pub := NewPublisher(regional, nil, "eu-node-1", log)
	defer pub.Close()

	created := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 25; i++ {
		pub.RecordShare(jobs.ShareEvent{
			PoolID: "btc-shared", BlockHeight: 900001, WorkerDiff: 1024,
			ShareDiff: 2048, NetworkDifficulty: 1e12,
			Miner: "bc1qminer", Worker: "rig1", UserAgent: "t/1", IPAddress: "10.1.1.1",
			Created: created,
		})
	}
	pub.RecordBlock(jobs.BlockEvent{
		PoolID: "btc-shared", BlockHeight: 900001, Hash: "00aa", Miner: "bc1qminer",
		NetworkDifficulty: 1e12, Created: created,
	})
	pub.OnPoolStateChange("btc-shared", pool.StateActive, pool.StateMaintenance, "test")

	waitFor(t, 10*time.Second, func() bool {
		s, b, st := sink.counts()
		return s == 25 && b == 1 && st == 1
	}, "all events consumed")

	sink.mu.Lock()
	defer sink.mu.Unlock()
	sh := sink.shares[0]
	if sh.Region != "eu" || sh.PoolID != "btc-shared" || sh.WorkerDiff != 1024 ||
		sh.Miner != "bc1qminer" || !sh.Created.Equal(created) {
		t.Fatalf("share = %+v", sh)
	}
	if sink.blocks[0].Hash != "00aa" || sink.blocks[0].Region != "eu" {
		t.Fatalf("block = %+v", sink.blocks[0])
	}
	if sink.states[0].From != "active" || sink.states[0].To != "maintenance" || sink.states[0].NodeID != "eu-node-1" {
		t.Fatalf("state = %+v", sink.states[0])
	}
}

// TestConsumerResumesFromDurable: events published while no consumer runs are
// delivered once the durable consumer starts (regional -> master with the
// master briefly down).
func TestConsumerResumesFromDurable(t *testing.T) {
	url := natsURL(t)
	regional := testClient(t, url, "us")
	log := logging.New(logging.Options{Level: "error"})
	pub := NewPublisher(regional, nil, "us-node-1", log)
	defer pub.Close()

	pub.RecordShare(jobs.ShareEvent{PoolID: "p", WorkerDiff: 1, Created: time.Now()})
	pub.RecordShare(jobs.ShareEvent{PoolID: "p", WorkerDiff: 2, Created: time.Now()})
	time.Sleep(300 * time.Millisecond) // let async publishes land in the stream

	master := testClient(t, url, "master")
	sink := &memSink{}
	cons, err := StartConsumer(context.Background(), master, sink, log)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Stop()

	waitFor(t, 10*time.Second, func() bool { s, _, _ := sink.counts(); return s == 2 }, "backlog delivered")
}

// TestSpoolReplayReachesConsumer: records stuck in the spool (simulated
// outage) reach the master after FlushSpool.
func TestSpoolReplayReachesConsumer(t *testing.T) {
	url := natsURL(t)
	regional := testClient(t, url, "asia")
	master := testClient(t, url, "master")
	log := logging.New(logging.Options{Level: "error"})

	sp, err := spool.Open(filepath.Join(t.TempDir(), "events.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	// Simulate an outage backlog: append directly (what the publisher does
	// when publishes fail).
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf(
			`{"poolId":"p1","region":"asia","workerDiff":%d,"miner":"m","created":"2026-07-07T00:00:00Z"}`, i+1))
		if err := sp.Append(ShareSubject("asia", "p1"), payload); err != nil {
			t.Fatal(err)
		}
	}

	sink := &memSink{}
	cons, err := StartConsumer(context.Background(), master, sink, log)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Stop()

	pub := NewPublisher(regional, sp, "asia-node-1", log)
	defer pub.Close()
	if err := pub.FlushSpool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sp.Len() != 0 {
		t.Fatal("spool not empty after flush")
	}
	waitFor(t, 10*time.Second, func() bool { s, _, _ := sink.counts(); return s == 3 }, "spooled events consumed")
}

// TestCommandRoundtripAppliesToOnePool: a command published by the master
// changes exactly the targeted pool on a regional's live lifecycle manager.
func TestCommandRoundtripAppliesToOnePool(t *testing.T) {
	url := natsURL(t)
	master := testClient(t, url, "master")
	regional := testClient(t, url, "eu")
	log := logging.New(logging.Options{Level: "error"})

	pools := []config.PoolConfig{
		{ID: "p1", Enabled: true, CoinSymbol: "BTC", PaymentMode: "solo"},
		{ID: "p2", Enabled: true, CoinSymbol: "BTC", PaymentMode: "solo"},
	}
	lm := pool.NewManager(log, nil, pools)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lm.Start(ctx)
	defer lm.Stop()

	sub, err := StartCommandSubscriber(context.Background(), regional, LifecycleApplier(lm, log), log)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()
	time.Sleep(200 * time.Millisecond) // subscriber must be live before publish (DeliverNew)

	if err := master.PublishCommand(context.Background(), CommandMsg{
		PoolID: "p1", Action: "maintenance", Message: "rolling upgrade", Origin: "master-1",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		st, _ := lm.GetPoolState("p1")
		return st == pool.StateMaintenance
	}, "p1 in maintenance")
	if st, _ := lm.GetPoolState("p2"); st != pool.StateActive {
		t.Fatalf("p2 state = %s, want active (isolation)", st)
	}
	if msg := lm.MaintenanceMessage("p1"); msg != "rolling upgrade" {
		t.Fatalf("maintenance message = %q", msg)
	}

	// Command for a pool this node does not host: applier ignores it cleanly.
	if err := master.PublishCommand(context.Background(), CommandMsg{
		PoolID: "not-here", Action: "pause",
	}); err != nil {
		t.Fatal(err)
	}
	// Resume p1 remotely.
	if err := master.PublishCommand(context.Background(), CommandMsg{
		PoolID: "p1", Action: "resume",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		st, _ := lm.GetPoolState("p1")
		return st == pool.StateActive
	}, "p1 resumed")
}

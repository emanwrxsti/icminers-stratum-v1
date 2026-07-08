// Command stratumd runs the GoStratumPool stratum front-end. In all-in-one
// mode it is the only process you need for local development; in regional mode
// it accepts miners and (from Stage 6) forwards shares to a master over NATS.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/api"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/btc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	natsmsg "github.com/emanwrxsti/icminers-stratum-v1/internal/messaging/nats"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/spool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stats"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/storage/postgres"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum"
)

// multiRecorder fans recorder events out to every backend (live stats,
// postgres, and NATS in Stage 6).
type multiRecorder []jobs.Recorder

func (m multiRecorder) RecordShare(ev jobs.ShareEvent) {
	for _, r := range m {
		r.RecordShare(ev)
	}
}

func (m multiRecorder) RecordBlock(ev jobs.BlockEvent) {
	for _, r := range m {
		r.RecordBlock(ev)
	}
}

// natsSink persists consumed NATS events on the master: shares through the
// async writer, blocks + state changes directly (blocks are rare).
type natsSink struct {
	writer *postgres.ShareWriter
	store  *postgres.Store
	log    *logging.Logger
}

func (s *natsSink) SinkShare(ev natsmsg.ShareEventMsg) {
	s.writer.Enqueue(postgres.ShareRecord{
		PoolID:            ev.PoolID,
		BlockHeight:       ev.BlockHeight,
		Difficulty:        ev.WorkerDiff,
		NetworkDifficulty: ev.NetworkDifficulty,
		Miner:             ev.Miner,
		Worker:            ev.Worker,
		UserAgent:         ev.UserAgent,
		IPAddress:         ev.IPAddress,
		Source:            ev.Region,
		Created:           ev.Created,
	})
}

func (s *natsSink) SinkBlock(ev natsmsg.BlockEventMsg) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := s.store.InsertBlock(ctx, postgres.BlockRecord{
		PoolID:            ev.PoolID,
		BlockHeight:       ev.BlockHeight,
		NetworkDifficulty: ev.NetworkDifficulty,
		Miner:             ev.Miner,
		Worker:            ev.Worker,
		Hash:              ev.Hash,
		RewardSats:        ev.RewardSats,
		Source:            ev.Region,
		Created:           ev.Created,
	}); err != nil {
		s.log.Error("consumed block insert failed", "pool", ev.PoolID, "err", err)
	}
}

func (s *natsSink) SinkStateChange(ev natsmsg.StateChangeMsg) {
	s.log.Info("pool state change (remote)",
		"pool", ev.PoolID, "region", ev.Region, "node", ev.NodeID,
		"from", ev.From, "to", ev.To, "reason", ev.Reason)
}

// storageRecorder adapts jobs events to the postgres layer. RecordShare is a
// non-blocking enqueue; RecordBlock does one INSERT in a detached goroutine
// (blocks are rare; the submit reply has already been computed).
type storageRecorder struct {
	writer *postgres.ShareWriter
	store  *postgres.Store
	region string
	log    *logging.Logger
}

func (r *storageRecorder) RecordShare(ev jobs.ShareEvent) {
	r.writer.Enqueue(postgres.ShareRecord{
		PoolID:            ev.PoolID,
		BlockHeight:       ev.BlockHeight,
		Difficulty:        ev.WorkerDiff,
		NetworkDifficulty: ev.NetworkDifficulty,
		Miner:             ev.Miner,
		Worker:            ev.Worker,
		UserAgent:         ev.UserAgent,
		IPAddress:         ev.IPAddress,
		Source:            r.region,
		Created:           ev.Created,
	})
}

func (r *storageRecorder) RecordBlock(ev jobs.BlockEvent) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := r.store.InsertBlock(ctx, postgres.BlockRecord{
			PoolID:            ev.PoolID,
			BlockHeight:       ev.BlockHeight,
			NetworkDifficulty: ev.NetworkDifficulty,
			Miner:             ev.Miner,
			Worker:            ev.Worker,
			Hash:              ev.Hash,
			RewardSats:        ev.RewardSats,
			Source:            r.region,
			Created:           ev.Created,
		}); err != nil {
			r.log.Error("block insert failed", "pool", ev.PoolID, "height", ev.BlockHeight, "err", err)
		}
	}()
}

func main() {
	cfgPath := flag.String("config", "configs/config.example.json", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		// No logger yet; fail fast and loudly. This is startup, not the hot path,
		// so exiting here is safe and does not violate the "never os.Exit from a
		// pool/adapter" rule.
		panic(err)
	}

	log := logging.New(logging.Options{Level: cfg.Logging.Level, JSON: cfg.Logging.JSON})
	log = log.With("mode", string(cfg.Mode), "node", cfg.NodeID, "region", cfg.Region)
	log.Info("starting stratumd", "version", stratum.Version, "pools", len(cfg.Pools), "ports", len(cfg.Stratum.Ports))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Stage 6 NATS: connect first so the publisher can serve as the lifecycle
	// StateHook. Regional nodes tolerate NATS being down at boot (spool +
	// background stream retry); master fails fast (it cannot do its job).
	var natsClient *natsmsg.Client
	var natsPub *natsmsg.Publisher
	var natsSpool *spool.Spool
	if cfg.NATS.Enabled {
		spoolDir := cfg.NATS.SpoolDir
		if spoolDir == "" {
			spoolDir = "spool"
		}
		sp, err := spool.Open(filepath.Join(spoolDir, "events.jsonl"), cfg.NATS.SpoolMaxBytes)
		if err != nil {
			log.Error("spool unavailable", "err", err)
			os.Exit(1)
		}
		natsSpool = sp
		client, err := natsmsg.Connect(ctx, natsmsg.Options{
			URLs:           cfg.NATS.URLs,
			Name:           cfg.NodeID,
			Region:         cfg.Region,
			Log:            log,
			RequireStreams: cfg.Mode == config.ModeMaster,
			AsyncFailure: func(subject string, data []byte) {
				if err := sp.Append(subject, data); err != nil {
					log.Error("spool append failed; event lost", "subject", subject, "err", err)
				}
			},
		})
		if err != nil {
			log.Error("nats unavailable", "err", err)
			os.Exit(1)
		}
		natsClient = client
		natsPub = natsmsg.NewPublisher(client, sp, cfg.NodeID, log)
	}

	// The lifecycle manager supervises every pool independently. When NATS is
	// on, every state change is published cluster-wide via the StateHook.
	var stateHook pool.StateHook
	if natsPub != nil {
		stateHook = natsPub
	}
	lifecycle := pool.NewManager(log, stateHook, cfg.Pools)

	// Wire coin adapters + job managers per pool BEFORE starting the pools. A
	// pool whose adapter cannot be built is placed into maintenance with the
	// reason; every other pool proceeds untouched (isolation spec).
	registry := jobs.NewRegistry()
	adapterFailures := map[string]string{}
	for _, p := range cfg.Pools {
		if !p.Enabled {
			continue
		}
		adapter, err := buildAdapter(cfg, p)
		if err != nil {
			adapterFailures[p.ID] = err.Error()
			log.Warn("pool adapter unavailable; pool will enter maintenance",
				"pool", p.ID, "err", err)
			continue
		}
		jm := jobs.NewManager(p.ID, adapter, log)
		registry.Add(jm)
		if err := lifecycle.SetPoller(p.ID, jm); err != nil {
			log.Warn("failed to wire poller", "pool", p.ID, "err", err)
		}
	}

	lifecycle.Start(ctx)
	for poolID, reason := range adapterFailures {
		if err := lifecycle.PutPoolInMaintenance(poolID, "adapter unavailable: "+reason); err != nil {
			log.Warn("failed to put pool in maintenance", "pool", poolID, "err", err)
		}
	}

	// Seed the extranonce1 allocator with a stable per-node prefix so nodes do
	// not collide on shared coins.
	// Stage 4 persistence: connect, migrate, and hook the async share writer
	// into every job manager. Databaseless mode stays fully supported.
	collector := stats.New()
	recorders := multiRecorder{collector}

	var store *postgres.Store
	var writer *postgres.ShareWriter
	if cfg.Postgres.Enabled {
		s, err := postgres.New(ctx, cfg.Postgres.DSN, log)
		if err != nil {
			log.Error("postgres unavailable", "err", err)
			os.Exit(1)
		}
		store = s
		writer = postgres.NewShareWriter(store, log, postgres.WriterOptions{
			QueueSize:     cfg.Postgres.ShareQueueSize,
			BatchSize:     cfg.Postgres.ShareBatchSize,
			FlushInterval: time.Duration(cfg.Postgres.ShareFlushInterval),
		})
		recorders = append(recorders, &storageRecorder{writer: writer, store: store, region: cfg.Region, log: log})
		log.Info("postgres persistence enabled")
	}
	if natsPub != nil && (cfg.Mode == config.ModeRegional ||
		(cfg.Mode == config.ModeAllInOne && !cfg.Postgres.Enabled)) {
		recorders = append(recorders, natsPub)
		log.Info("nats share/block publishing enabled")
	}
	registry.SetRecorder(recorders)

	// Master consumes regional events into PostgreSQL; non-masters obey
	// lifecycle commands published over NATS (per-pool only).
	var natsConsumer *natsmsg.Consumer
	var cmdSub *natsmsg.CommandSubscriber
	if natsClient != nil {
		if cfg.Mode == config.ModeMaster {
			sink := &natsSink{writer: writer, store: store, log: log}
			nc, err := natsmsg.StartConsumer(ctx, natsClient, sink, log)
			if err != nil {
				log.Error("nats consumer failed", "err", err)
				os.Exit(1)
			}
			natsConsumer = nc
		} else {
			cs, err := natsmsg.StartCommandSubscriber(ctx, natsClient,
				natsmsg.LifecycleApplier(lifecycle, log), log)
			if err != nil {
				log.Warn("command subscriber unavailable (will not follow remote commands)", "err", err)
			} else {
				cmdSub = cs
			}
		}
	}

	prefix := nodePrefix(cfg.NodeID)
	srv := stratum.NewServer(cfg.Stratum, log, lifecycle, prefix, registry)
	// The server fans mining.notify out to sessions; give it to job managers.
	registry.SetBroadcaster(srv)

	// Stage 5 HTTP API: public stats + admin lifecycle control.
	if cfg.API.Enabled {
		var publishCmd func(poolID, action, message string, graceSeconds int) error
		if natsClient != nil && cfg.Mode != config.ModeRegional {
			nc := natsClient
			nodeID := cfg.NodeID
			publishCmd = func(poolID, action, message string, graceSeconds int) error {
				return nc.PublishCommand(context.Background(), natsmsg.CommandMsg{
					PoolID: poolID, Action: action, Message: message,
					GracePeriodSeconds: graceSeconds, Origin: nodeID,
				})
			}
		}
		apiSrv := api.New(api.Options{
			Config:         cfg,
			Lifecycle:      lifecycle,
			Stats:          collector,
			Store:          store,
			SessionCount:   srv.SessionCount,
			AdminToken:     cfg.API.AdminToken,
			PublishCommand: publishCmd,
			Log:            log,
		})
		if err := apiSrv.Start(ctx, cfg.API.Bind); err != nil {
			log.Error("api failed to start", "err", err)
			os.Exit(1)
		}
	}
	if err := srv.Start(ctx); err != nil {
		log.Error("failed to start stratum server", "err", err)
		lifecycle.Stop()
		os.Exit(1)
	}

	log.Info("stratumd ready")

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// Graceful shutdown: stop accepting, then tear down pools.
	srv.Wait()
	lifecycle.Stop()
	if natsConsumer != nil {
		natsConsumer.Stop()
	}
	if cmdSub != nil {
		cmdSub.Stop()
	}
	if natsPub != nil {
		natsPub.Close()
	}
	if natsClient != nil {
		natsClient.Close()
	}
	if natsSpool != nil {
		_ = natsSpool.Close()
	}
	if writer != nil {
		writer.Close() // final flush
	}
	if store != nil {
		store.Close()
	}
	log.Info("stratumd stopped")
}

// buildAdapter constructs the coin adapter for a pool from its coin config.
// Only BTC (sha256d via Bitcoin Core) is implemented in Stage 2; further coins
// (RXD, SCASH, ALPH, ...) land in later stages behind the same seam.
func buildAdapter(cfg *config.Config, p config.PoolConfig) (*btc.Adapter, error) {
	coin, ok := cfg.CoinBySymbol(p.CoinSymbol)
	if !ok {
		return nil, fmt.Errorf("pool %s: no coin config for symbol %q", p.ID, p.CoinSymbol)
	}
	if !strings.EqualFold(coin.Symbol, "BTC") {
		return nil, fmt.Errorf("pool %s: coin %s not implemented yet (stage 2 is BTC only)", p.ID, coin.Symbol)
	}
	if coin.RPCURL == "" {
		return nil, fmt.Errorf("pool %s: coin %s has no rpcUrl", p.ID, coin.Symbol)
	}
	client := rpc.New(rpc.Options{URL: coin.RPCURL, User: coin.RPCUser, Password: coin.RPCPassword})
	return btc.New(btc.Options{
		RPC:         client,
		Network:     coin.Network,
		PoolAddress: p.Address,
		CoinbaseTag: p.CoinbaseTag,
	})
}

// nodePrefix derives a 2-byte extranonce1 prefix from the node id.
func nodePrefix(nodeID string) []byte {
	if nodeID == "" {
		return []byte{0x00, 0x00}
	}
	sum := sha256.Sum256([]byte(nodeID))
	return sum[:2]
}

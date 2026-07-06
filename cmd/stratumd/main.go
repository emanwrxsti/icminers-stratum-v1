// Command stratumd runs the GoStratumPool stratum front-end. In all-in-one
// mode it is the only process you need for local development; in regional mode
// it accepts miners and (from Stage 6) forwards shares to a master over NATS.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/icminers/gostratumpool/internal/config"
	"github.com/icminers/gostratumpool/internal/logging"
	"github.com/icminers/gostratumpool/internal/pool"
	"github.com/icminers/gostratumpool/internal/stratum"
)

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

	// The lifecycle manager supervises every pool independently.
	lifecycle := pool.NewManager(log, nil /* StateHook wired in Stage 6 */, cfg.Pools)
	lifecycle.Start(ctx)

	// Seed the extranonce1 allocator with a stable per-node prefix so nodes do
	// not collide on shared coins.
	prefix := nodePrefix(cfg.NodeID)
	srv := stratum.NewServer(cfg.Stratum, log, lifecycle, prefix)
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
	log.Info("stratumd stopped")
}

// nodePrefix derives a 2-byte extranonce1 prefix from the node id.
func nodePrefix(nodeID string) []byte {
	if nodeID == "" {
		return []byte{0x00, 0x00}
	}
	sum := sha256.Sum256([]byte(nodeID))
	return sum[:2]
}

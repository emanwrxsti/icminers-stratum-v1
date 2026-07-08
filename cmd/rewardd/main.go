// Command rewardd runs Stage 7 reward processing: it confirms found blocks
// against each coin daemon (maturity + orphan handling) and credits miner
// balances by the pool's payment scheme (pplns / prop / solo) with exact
// integer satoshi accounting. One panic-isolated processor per pool; a broken
// pool or daemon never touches the others.
//
// rewardd runs where the database lives (master or all-in-one) and reads the
// same JSON config as stratumd.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/rewards"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/storage/postgres"
)

func main() {
	configPath := flag.String("config", "configs/config.json", "path to JSON config")
	once := flag.Bool("once", false, "run a single pass per pool, then exit")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	log := logging.New(logging.Options{Level: cfg.Logging.Level, JSON: cfg.Logging.JSON}).
		With("app", "rewardd", "node", cfg.NodeID)

	if !cfg.Postgres.Enabled {
		log.Error("rewardd requires postgres (it credits balances)")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		log.Info("signal received; shutting down", "signal", s.String())
		cancel()
	}()

	store, err := postgres.New(ctx, cfg.Postgres.DSN, log)
	if err != nil {
		log.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	var wg sync.WaitGroup
	started := 0
	for _, p := range cfg.Pools {
		if !p.Enabled {
			continue
		}
		proc, err := buildProcessor(cfg, p, store, log)
		if err != nil {
			log.Warn("pool skipped by rewardd", "pool", p.ID, "err", err)
			continue
		}
		started++
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *once {
				if err := proc.Pass(ctx); err != nil {
					log.Warn("one-shot pass failed", "err", err)
				}
				return
			}
			proc.Run(ctx)
		}()
	}
	if started == 0 {
		log.Error("no processable pools; exiting")
		os.Exit(1)
	}
	log.Info("rewardd running", "pools", started, "once", *once)
	wg.Wait()
	log.Info("rewardd stopped")
}

// buildProcessor wires one pool's calculator, daemon view, and options.
func buildProcessor(cfg *config.Config, p config.PoolConfig, store *postgres.Store, log *logging.Logger) (*rewards.Processor, error) {
	coin, ok := cfg.CoinBySymbol(p.CoinSymbol)
	if !ok {
		return nil, fmt.Errorf("no coin config for symbol %q", p.CoinSymbol)
	}
	if !strings.EqualFold(coin.Symbol, "BTC") {
		return nil, fmt.Errorf("coin %s not implemented (BTC only until further stages)", coin.Symbol)
	}
	if coin.RPCURL == "" {
		return nil, fmt.Errorf("coin %s has no rpcUrl", coin.Symbol)
	}
	calc, err := rewards.ForPaymentMode(p.PaymentMode, p.PPLNSFactor)
	if err != nil {
		return nil, err
	}
	client := rpc.New(rpc.Options{URL: coin.RPCURL, User: coin.RPCUser, Password: coin.RPCPassword})
	return rewards.NewProcessor(rewards.ProcessorOptions{
		PoolID:     p.ID,
		Calculator: calc,
		FeePercent: p.PoolFeePercent,
		Chain:      &rewards.BitcoinlikeChain{RPC: client},
		Store:      store,
		Confirm: rewards.ConfirmOptions{
			MaturityDepth: coin.MaturityDepth,
			OrphanDepth:   coin.OrphanDepth,
		},
		Interval: time.Duration(p.RewardInterval),
		Log:      log,
	}), nil
}

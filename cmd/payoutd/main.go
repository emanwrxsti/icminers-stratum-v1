// Command payoutd runs Stage 9 payouts: it moves credited miner balances
// on-chain per pool, batching everything at/above the pool's minimum payment
// into one sendmany transaction. Safety order is deduct-then-send with atomic
// refund on failure; batches interrupted between deduct and outcome are
// surfaced loudly for operator reconciliation, never auto-refunded.
//
// payoutd runs where the database and the coin WALLETS live. It reads the
// same JSON config as stratumd/rewardd.
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

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/btc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/ltc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rxd"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/scash"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/payouts"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/storage/postgres"
)

func main() {
	configPath := flag.String("config", "configs/config.json", "path to JSON config")
	once := flag.Bool("once", false, "run a single payout pass per pool, then exit")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	log := logging.New(logging.Options{Level: cfg.Logging.Level, JSON: cfg.Logging.JSON}).
		With("app", "payoutd", "node", cfg.NodeID)

	if !cfg.Postgres.Enabled {
		log.Error("payoutd requires postgres (it moves balances)")
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
			log.Warn("pool skipped by payoutd", "pool", p.ID, "err", err)
			continue
		}
		started++
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *once {
				if err := proc.Pass(ctx); err != nil {
					log.Warn("one-shot payout pass failed", "err", err)
				}
				return
			}
			proc.Run(ctx)
		}()
	}
	if started == 0 {
		log.Error("no payable pools; exiting")
		os.Exit(1)
	}
	log.Info("payoutd running", "pools", started, "once", *once)
	wg.Wait()
	log.Info("payoutd stopped")
}

// buildProcessor wires one pool's wallet, validator, and options.
func buildProcessor(cfg *config.Config, p config.PoolConfig, store *postgres.Store, log *logging.Logger) (*payouts.Processor, error) {
	coin, ok := cfg.CoinBySymbol(p.CoinSymbol)
	if !ok {
		return nil, fmt.Errorf("no coin config for symbol %q", p.CoinSymbol)
	}
	if coin.RPCURL == "" {
		return nil, fmt.Errorf("coin %s has no rpcUrl", coin.Symbol)
	}
	client := rpc.New(rpc.Options{URL: coin.RPCURL, User: coin.RPCUser, Password: coin.RPCPassword})
	// The address validator must match the coin so payout addresses are
	// checked against the right network parameters.
	var adapter payouts.AddressValidator
	switch strings.ToUpper(coin.Symbol) {
	case "BTC":
		a, err := btc.New(btc.Options{
			RPC: client, Network: coin.Network,
			PoolAddress: p.Address, CoinbaseTag: p.CoinbaseTag,
		})
		if err != nil {
			return nil, err
		}
		adapter = a
	case "LTC":
		a, err := ltc.New(ltc.Options{
			RPC: client, Network: coin.Network,
			PoolAddress: p.Address, CoinbaseTag: p.CoinbaseTag,
		})
		if err != nil {
			return nil, err
		}
		adapter = a
	case "RXD":
		a, err := rxd.New(rxd.Options{
			RPC: client, Network: coin.Network,
			PoolAddress: p.Address, CoinbaseTag: p.CoinbaseTag,
		})
		if err != nil {
			return nil, err
		}
		adapter = a
	case "SCASH":
		// Payouts only need address validation, which is independent of the
		// RandomX PoW, so no RandomX backend is required here.
		a, err := scash.New(scash.Options{
			RPC: client, Network: coin.Network,
			PoolAddress: p.Address, CoinbaseTag: p.CoinbaseTag,
		})
		if err != nil {
			return nil, err
		}
		adapter = a
	default:
		return nil, fmt.Errorf("coin %s not implemented (supported: BTC, LTC, RXD, SCASH)", coin.Symbol)
	}
	subtractFee := true
	if p.SubtractFeeFromMiners != nil {
		subtractFee = *p.SubtractFeeFromMiners
	}
	return payouts.NewProcessor(payouts.ProcessorOptions{
		PoolID:      p.ID,
		MinSats:     p.MinimumPaymentSats,
		SubtractFee: subtractFee,
		Wallet:      &payouts.BitcoindWallet{RPC: client},
		Store:       store,
		Validator:   adapter,
		Interval:    time.Duration(p.PayoutInterval),
		Log:         log,
	}), nil
}

// Package ltc implements the CoinAdapter for Litecoin and its scrypt-family
// relatives (testnet/regtest). It is a thin constructor over
// internal/coins/bitcoinlike: Litecoin supplies scrypt(N=1024,r=1,p=1)
// proof-of-work and the LTC address parameters; every other part of block
// production is the shared bitcoin-like implementation. This is the second
// coin that proves the CoinAdapter abstraction: no forked block logic, only a
// different PoW hash and address set.
package ltc

import (
	"fmt"
	"strings"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinlike"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
)

// Adapter is the Litecoin CoinAdapter (an alias of the shared implementation).
type Adapter = bitcoinlike.Adapter

// Options configures the LTC adapter.
type Options struct {
	// RPC is the daemon client (Litecoin Core).
	RPC *rpc.Client
	// Network selects address parameters: "mainnet" (default), "testnet",
	// or "regtest".
	Network string
	// PoolAddress receives block rewards; validated at construction.
	PoolAddress string
	// CoinbaseTag is embedded in the coinbase scriptSig.
	CoinbaseTag string
	// ExtraNonce1Size/ExtraNonce2Size are the stratum extranonce widths.
	ExtraNonce1Size int
	ExtraNonce2Size int
}

// New builds and validates an LTC adapter.
func New(opts Options) (*Adapter, error) {
	if opts.RPC == nil {
		return nil, fmt.Errorf("ltc: nil rpc client")
	}
	var params bitcoinbase.AddressParams
	switch strings.ToLower(opts.Network) {
	case "", "mainnet":
		params = bitcoinbase.LTCMainNet
	case "testnet", "signet":
		params = bitcoinbase.LTCTestNet
	case "regtest":
		params = bitcoinbase.LTCRegTest
	default:
		return nil, fmt.Errorf("ltc: unknown network %q", opts.Network)
	}
	return bitcoinlike.New(bitcoinlike.Spec{
		CoinName:   "Litecoin",
		CoinSymbol: "LTC",
		AlgoName:   "scrypt",
		Params:     params,
		PoW:        bitcoinbase.ScryptHash,
		// Litecoin does not use ASICBoost version rolling.
		VersionRollingMask: 0,
	}, bitcoinlike.Options{
		RPC:             opts.RPC,
		PoolAddress:     opts.PoolAddress,
		CoinbaseTag:     opts.CoinbaseTag,
		ExtraNonce1Size: opts.ExtraNonce1Size,
		ExtraNonce2Size: opts.ExtraNonce2Size,
	})
}

// BitcoinbaseJob extracts the underlying bitcoinbase.Job from a MiningJob.
func BitcoinbaseJob(job *coins.MiningJob) (*bitcoinbase.Job, error) {
	return bitcoinlike.BitcoinbaseJob(job)
}

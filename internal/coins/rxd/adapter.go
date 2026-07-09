// Package rxd implements the CoinAdapter for Radiant (RXD). It is a thin
// constructor over internal/coins/bitcoinlike: Radiant supplies SHA-512/256d
// proof-of-work AND SHA-512/256d block identity (Radiant replaced SHA256 with
// SHA-512/256 throughout), plus legacy base58 address parameters. Every other
// part of block production is the shared bitcoin-like implementation. This is
// the third coin proving the CoinAdapter abstraction: a different PoW hash, a
// different identity hash, and a different address set, with no forked block
// logic.
package rxd

import (
	"fmt"
	"strings"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinlike"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
)

// Adapter is the Radiant CoinAdapter (an alias of the shared implementation).
type Adapter = bitcoinlike.Adapter

// Options configures the RXD adapter.
type Options struct {
	// RPC is the daemon client (Radiant Node).
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

// New builds and validates an RXD adapter.
func New(opts Options) (*Adapter, error) {
	if opts.RPC == nil {
		return nil, fmt.Errorf("rxd: nil rpc client")
	}
	var params bitcoinbase.AddressParams
	switch strings.ToLower(opts.Network) {
	case "", "mainnet":
		params = bitcoinbase.RXDMainNet
	case "testnet":
		params = bitcoinbase.RXDTestNet
	case "regtest":
		params = bitcoinbase.RXDRegTest
	default:
		return nil, fmt.Errorf("rxd: unknown network %q", opts.Network)
	}
	return bitcoinlike.New(bitcoinlike.Spec{
		CoinName:     "Radiant",
		CoinSymbol:   "RXD",
		AlgoName:     "sha512_256d",
		Params:       params,
		PoW:          bitcoinbase.SHA512_256D,
		IdentityHash: bitcoinbase.SHA512_256D,
		// Radiant does not use ASICBoost version rolling.
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

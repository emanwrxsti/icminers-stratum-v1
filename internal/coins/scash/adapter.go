package scash

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinlike"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
)

// Adapter is the Scash CoinAdapter (an alias of the shared implementation).
// Scash reuses the entire bitcoin-like block-production pipeline; only the PoW
// hash differs, which is supplied as a closure that computes the RandomX
// commitment over the 80-byte header.
type Adapter = bitcoinlike.Adapter

// Options configures the SCASH adapter.
type Options struct {
	// RPC is the daemon client (Scash Core).
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

	// RandomX provides the memory-hard RandomX VM hash. Required in
	// production (wire a librandomx binding); tests inject a deterministic
	// double. When nil, share validation fails loudly rather than silently.
	RandomX RandomXHasher
	// EpochDuration overrides the RandomX epoch length in seconds (default:
	// one week). Must match the daemon's consensus value.
	EpochDuration int64
}

// PoWHash builds the Scash proof-of-work closure: given the 80-byte header, it
// derives the epoch seed from the header's ntime, runs the RandomX VM hash
// under that seed, and returns the RandomX commitment (blake2b(header||rxHash)).
// The commitment is what the share/network targets are compared against.
func PoWHash(rx RandomXHasher, epochDuration int64) bitcoinlike.PoWHash {
	if rx == nil {
		rx = unavailableHasher{}
	}
	return func(header []byte) ([]byte, error) {
		if len(header) != 80 {
			return nil, fmt.Errorf("scash: header is %d bytes, want 80", len(header))
		}
		// ntime is a little-endian uint32 at bytes 68:72 of the header.
		ntime := int64(binary.LittleEndian.Uint32(header[68:72]))
		seed := EpochSeedHash(ntime, epochDuration)
		rxHash, err := rx.RandomXHash(seed, header)
		if err != nil {
			return nil, err
		}
		if len(rxHash) != RandomXHashSize {
			return nil, fmt.Errorf("scash: randomx hash is %d bytes, want %d", len(rxHash), RandomXHashSize)
		}
		return Commitment(header, rxHash)
	}
}

// New builds and validates a SCASH adapter.
func New(opts Options) (*Adapter, error) {
	if opts.RPC == nil {
		return nil, fmt.Errorf("scash: nil rpc client")
	}
	var params bitcoinbase.AddressParams
	switch strings.ToLower(opts.Network) {
	case "", "mainnet":
		params = bitcoinbase.SCASHMainNet
	case "testnet":
		params = bitcoinbase.SCASHTestNet
	case "regtest":
		params = bitcoinbase.SCASHRegTest
	default:
		return nil, fmt.Errorf("scash: unknown network %q", opts.Network)
	}
	pow := PoWHash(opts.RandomX, opts.EpochDuration)
	return bitcoinlike.New(bitcoinlike.Spec{
		CoinName:   "Scash",
		CoinSymbol: "SCASH",
		AlgoName:   "randomx",
		Params:     params,
		PoW:        pow,
		// Block identity is SHA256d (Scash keeps Bitcoin's block hash).
		IdentityHash: nil,
		// Scash does not use ASICBoost version rolling.
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

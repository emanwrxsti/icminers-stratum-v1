// Package bitcoinlike is the parameterized CoinAdapter shared by every
// Bitcoin-derived coin: the getblocktemplate model, coinbase construction,
// merkle branch, 80-byte header, block assembly, submitblock, and address
// handling are all identical across the family. A specific coin supplies only
// what actually differs — its name/symbol, address parameters, and its
// proof-of-work hash function. This is the concrete proof that the
// coins.CoinAdapter abstraction holds: btc and ltc are ~40-line constructors
// over this one implementation.
package bitcoinlike

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// PoWHash computes a coin's proof-of-work hash of the 80-byte header. It must
// return a little-endian 32-byte digest (same orientation as SHA256d), which
// is what the network/share targets are compared against. Note that the block
// IDENTITY hash is always SHA256d regardless of the PoW algorithm (Litecoin
// behavior), so this function affects difficulty only.
type PoWHash func(header []byte) ([]byte, error)

// Spec is the per-coin configuration for a bitcoin-like adapter.
type Spec struct {
	CoinName   string // e.g. "Bitcoin", "Litecoin"
	CoinSymbol string // e.g. "BTC", "LTC"
	AlgoName   string // e.g. "sha256d", "scrypt"
	// Params selects address encoding for the coin+network.
	Params bitcoinbase.AddressParams
	// PoW is the coin's proof-of-work hash (defaults to SHA256d when nil).
	PoW PoWHash
	// IdentityHash computes the block identity hash from the 80-byte header.
	// Most bitcoin-like coins use SHA256d (the default when nil), but some
	// (e.g. Radiant, which replaced SHA256 with SHA-512/256 throughout) use a
	// different hash for the block id as well as the PoW. This affects only
	// the reported BlockHash, never difficulty.
	IdentityHash PoWHash
	// VersionRollingMask advertised in mining.configure and enforced on
	// submit (0 disables version rolling).
	VersionRollingMask uint32
}

// Adapter is the shared bitcoin-like CoinAdapter.
type Adapter struct {
	spec         Spec
	rpc          *rpc.Client
	payoutScript []byte
	tag          string
	en1Size      int
	en2Size      int
}

// compile-time interface check.
var _ coins.CoinAdapter = (*Adapter)(nil)

// Options configures a bitcoin-like adapter.
type Options struct {
	RPC             *rpc.Client
	PoolAddress     string
	CoinbaseTag     string
	ExtraNonce1Size int
	ExtraNonce2Size int
}

// New builds and validates an adapter for the given coin spec. The pool
// address is resolved to its scriptPubKey immediately so a misconfigured pool
// fails fast (into that pool's error state, not the process).
func New(spec Spec, opts Options) (*Adapter, error) {
	if opts.RPC == nil {
		return nil, fmt.Errorf("%s: nil rpc client", spec.CoinSymbol)
	}
	if spec.PoW == nil {
		spec.PoW = func(header []byte) ([]byte, error) {
			return bitcoinbase.DoubleSHA256(header), nil
		}
	}
	if spec.IdentityHash == nil {
		spec.IdentityHash = func(header []byte) ([]byte, error) {
			return bitcoinbase.DoubleSHA256(header), nil
		}
	}
	script, err := bitcoinbase.AddressToScript(opts.PoolAddress, spec.Params)
	if err != nil {
		return nil, fmt.Errorf("%s: pool address: %w", spec.CoinSymbol, err)
	}
	en1, en2 := opts.ExtraNonce1Size, opts.ExtraNonce2Size
	if en1 <= 0 {
		en1 = 4
	}
	if en2 <= 0 {
		en2 = 4
	}
	return &Adapter{
		spec:         spec,
		rpc:          opts.RPC,
		payoutScript: script,
		tag:          opts.CoinbaseTag,
		en1Size:      en1,
		en2Size:      en2,
	}, nil
}

// Name implements CoinAdapter.
func (a *Adapter) Name() string { return a.spec.CoinName }

// Symbol implements CoinAdapter.
func (a *Adapter) Symbol() string { return a.spec.CoinSymbol }

// Algo implements CoinAdapter.
func (a *Adapter) Algo() string { return a.spec.AlgoName }

// BlockchainInfo is the subset of getblockchaininfo the pool uses for health.
type BlockchainInfo struct {
	Chain                string  `json:"chain"`
	Blocks               int64   `json:"blocks"`
	Headers              int64   `json:"headers"`
	InitialBlockDownload bool    `json:"initialblockdownload"`
	VerificationProgress float64 `json:"verificationprogress"`
}

// GetBlockchainInfo calls getblockchaininfo.
func (a *Adapter) GetBlockchainInfo(ctx context.Context) (*BlockchainInfo, error) {
	var info BlockchainInfo
	if err := a.rpc.Call(ctx, "getblockchaininfo", nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Health checks the daemon is reachable and usable for mining.
func (a *Adapter) Health(ctx context.Context) error {
	info, err := a.GetBlockchainInfo(ctx)
	if err != nil {
		return fmt.Errorf("%s health: %w", a.spec.CoinSymbol, err)
	}
	if info.InitialBlockDownload {
		return fmt.Errorf("%s health: daemon is in initial block download (%d/%d blocks)",
			a.spec.CoinSymbol, info.Blocks, info.Headers)
	}
	return nil
}

// GetBlockTemplate implements CoinAdapter using segwit GBT.
func (a *Adapter) GetBlockTemplate(ctx context.Context) (*coins.BlockTemplate, error) {
	var raw json.RawMessage
	params := []any{map[string]any{"rules": []string{"segwit"}}}
	if err := a.rpc.Call(ctx, "getblocktemplate", params, &raw); err != nil {
		return nil, fmt.Errorf("%s getblocktemplate: %w", a.spec.CoinSymbol, err)
	}
	tpl, err := bitcoinbase.ParseTemplate(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", a.spec.CoinSymbol, err)
	}
	var rawMap map[string]any
	_ = json.Unmarshal(raw, &rawMap)
	return &coins.BlockTemplate{
		Height:            tpl.Height,
		PreviousBlockHash: tpl.PreviousBlockHash,
		Bits:              tpl.Bits,
		CurTime:           tpl.CurTime,
		CoinbaseValue:     tpl.CoinbaseValue,
		Raw:               rawMap,
	}, nil
}

func (a *Adapter) parseTemplateFromRaw(tpl *coins.BlockTemplate) (*bitcoinbase.Template, error) {
	raw, err := json.Marshal(tpl.Raw)
	if err != nil {
		return nil, fmt.Errorf("%s: re-marshal template: %w", a.spec.CoinSymbol, err)
	}
	return bitcoinbase.ParseTemplate(raw)
}

// BuildMiningJob implements CoinAdapter.
func (a *Adapter) BuildMiningJob(ctx context.Context, tpl *coins.BlockTemplate, extraNonce1 string) (*coins.MiningJob, error) {
	_ = extraNonce1
	base, err := a.parseTemplateFromRaw(tpl)
	if err != nil {
		return nil, err
	}
	spec := bitcoinbase.CoinbaseSpec{
		Height:          base.Height,
		CoinbaseValue:   base.CoinbaseValue,
		PayoutScript:    a.payoutScript,
		Tag:             a.tag,
		ExtraNonce1Size: a.en1Size,
		ExtraNonce2Size: a.en2Size,
		TxVersion:       2,
	}
	if base.DefaultWitnessCommitment != "" {
		wc, err := hex.DecodeString(base.DefaultWitnessCommitment)
		if err != nil {
			return nil, fmt.Errorf("%s: default_witness_commitment: %w", a.spec.CoinSymbol, err)
		}
		spec.WitnessCommitmentScript = wc
	}
	job, err := bitcoinbase.NewJob("", base, spec, false)
	if err != nil {
		return nil, err
	}
	target, err := bitcoinbase.BitsToTarget(base.Bits)
	if err != nil {
		return nil, err
	}
	return &coins.MiningJob{
		JobID:         "",
		Height:        base.Height,
		NetworkTarget: bitcoinbase.TargetToBytes(target),
		CoinbaseValue: base.CoinbaseValue,
		NotifyParams:  nil,
		Raw:           map[string]any{"bitcoinbase": job},
	}, nil
}

// BitcoinbaseJob extracts the underlying bitcoinbase.Job from a MiningJob
// produced by this adapter.
func BitcoinbaseJob(job *coins.MiningJob) (*bitcoinbase.Job, error) {
	v, ok := job.Raw["bitcoinbase"]
	if !ok {
		return nil, fmt.Errorf("mining job missing bitcoinbase payload")
	}
	b, ok := v.(*bitcoinbase.Job)
	if !ok {
		return nil, fmt.Errorf("unexpected bitcoinbase payload type %T", v)
	}
	return b, nil
}

// ValidateShare implements CoinAdapter: it reconstructs the coinbase, merkle
// root, and 80-byte header from the remembered job plus the miner's submit
// fields, hashes with the coin's PoW, and checks the result against the
// worker's share target and the network target. The block IDENTITY hash is
// SHA256d regardless of the PoW. No I/O happens here.
func (a *Adapter) ValidateShare(ctx context.Context, job *coins.MiningJob, submit coins.ShareSubmit) (*coins.ShareResult, error) {
	base, err := BitcoinbaseJob(job)
	if err != nil {
		return nil, err
	}
	fields, err := bitcoinbase.ParseSubmitHex(
		submit.ExtraNonce1, submit.ExtraNonce2,
		submit.NTime, submit.Nonce, submit.VersionBits, a.en2Size)
	if err != nil {
		return nil, fmt.Errorf("%s submit: %w", a.spec.CoinSymbol, err)
	}
	if len(fields.ExtraNonce1) != a.en1Size {
		return nil, fmt.Errorf("%s submit: extranonce1 is %d bytes, want %d",
			a.spec.CoinSymbol, len(fields.ExtraNonce1), a.en1Size)
	}
	if base.MinTime > 0 && int64(fields.NTime) < base.MinTime {
		return nil, fmt.Errorf("%s submit: ntime %d before template mintime %d",
			a.spec.CoinSymbol, fields.NTime, base.MinTime)
	}
	if int64(fields.NTime) > base.CurTime+7200 {
		return nil, fmt.Errorf("%s submit: ntime %d too far in the future (curtime %d)",
			a.spec.CoinSymbol, fields.NTime, base.CurTime)
	}
	if fields.HasVersion && a.spec.VersionRollingMask != 0 &&
		fields.VersionBits&^a.spec.VersionRollingMask != 0 {
		return nil, fmt.Errorf("%s submit: version bits %08x outside mask %08x",
			a.spec.CoinSymbol, fields.VersionBits, a.spec.VersionRollingMask)
	}

	coinbaseTx, err := base.CoinbaseParts.Assemble(fields.ExtraNonce1, fields.ExtraNonce2)
	if err != nil {
		return nil, fmt.Errorf("%s submit: %w", a.spec.CoinSymbol, err)
	}
	coinbaseHash := bitcoinbase.DoubleSHA256(coinbaseTx)
	merkleRootLE := bitcoinbase.FoldBranch(coinbaseHash, base.MerkleBranchLE)

	header, err := bitcoinbase.BuildHeader(base, merkleRootLE, fields, a.spec.VersionRollingMask)
	if err != nil {
		return nil, fmt.Errorf("%s submit: %w", a.spec.CoinSymbol, err)
	}

	// PoW hash drives difficulty; the identity hash (SHA256d for most coins)
	// produces the reported block hash.
	powLE, err := a.spec.PoW(header)
	if err != nil {
		return nil, fmt.Errorf("%s submit: pow hash: %w", a.spec.CoinSymbol, err)
	}
	powBig := vardiff.HashToBig(powLE)
	idLE, err := a.spec.IdentityHash(header)
	if err != nil {
		return nil, fmt.Errorf("%s submit: identity hash: %w", a.spec.CoinSymbol, err)
	}

	result := &coins.ShareResult{ShareDiff: vardiff.ShareDifficulty(powBig)}
	shareTarget := vardiff.DifficultyToTarget(submit.WorkerDiff)
	result.Valid = vardiff.MeetsTarget(powBig, shareTarget)

	networkTarget := new(big.Int).SetBytes(job.NetworkTarget)
	if vardiff.MeetsTarget(powBig, networkTarget) {
		result.BlockCandidate = true
		result.BlockHash = hex.EncodeToString(bitcoinbase.ReverseBytes(idLE))
		blockHex, err := bitcoinbase.AssembleBlock(header, coinbaseTx, base)
		if err != nil {
			return nil, fmt.Errorf("%s submit: assemble block: %w", a.spec.CoinSymbol, err)
		}
		result.BlockHex = blockHex
	}
	return result, nil
}

// SubmitBlock implements CoinAdapter via submitblock.
func (a *Adapter) SubmitBlock(ctx context.Context, blockHex string) error {
	var result *string
	if err := a.rpc.Call(ctx, "submitblock", []any{blockHex}, &result); err != nil {
		return fmt.Errorf("%s submitblock: %w", a.spec.CoinSymbol, err)
	}
	if result != nil && *result != "" {
		return fmt.Errorf("%s submitblock rejected: %s", a.spec.CoinSymbol, *result)
	}
	return nil
}

// HashHeader implements CoinAdapter with the coin's PoW hash.
func (a *Adapter) HashHeader(header []byte) ([]byte, error) {
	if len(header) != 80 {
		return nil, fmt.Errorf("%s: header is %d bytes, want 80", a.spec.CoinSymbol, len(header))
	}
	return a.spec.PoW(header)
}

// AddressToScript implements CoinAdapter.
func (a *Adapter) AddressToScript(address string) ([]byte, error) {
	return bitcoinbase.AddressToScript(address, a.spec.Params)
}

// NormalizeAddress implements CoinAdapter.
func (a *Adapter) NormalizeAddress(address string) (string, error) {
	addr := strings.TrimSpace(address)
	if a.spec.Params.Bech32HRP != "" && strings.HasPrefix(strings.ToLower(addr), a.spec.Params.Bech32HRP+"1") {
		addr = strings.ToLower(addr)
	}
	if _, err := bitcoinbase.AddressToScript(addr, a.spec.Params); err != nil {
		return "", err
	}
	return addr, nil
}

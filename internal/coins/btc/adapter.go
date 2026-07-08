// Package btc implements the CoinAdapter for Bitcoin (and BTC testnet/regtest)
// on top of internal/coins/rpc and internal/coins/bitcoinbase. Share
// validation and block assembly land in Stage 3; this stage delivers real
// getblocktemplate polling and mining.notify jobs.
package btc

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

// Options configures the BTC adapter.
type Options struct {
	// RPC is the daemon client (Bitcoin Core).
	RPC *rpc.Client
	// Network selects address parameters: "mainnet" (default), "testnet",
	// or "regtest".
	Network string
	// PoolAddress receives block rewards; validated at construction.
	PoolAddress string
	// CoinbaseTag is embedded in the coinbase scriptSig, e.g. "/ICMINERS/".
	CoinbaseTag string
	// ExtraNonce1Size/ExtraNonce2Size are the stratum extranonce widths.
	ExtraNonce1Size int
	ExtraNonce2Size int
}

// Adapter is the Bitcoin CoinAdapter.
type Adapter struct {
	rpc          *rpc.Client
	params       bitcoinbase.AddressParams
	payoutScript []byte
	tag          string
	en1Size      int
	en2Size      int
}

// compile-time interface check.
var _ coins.CoinAdapter = (*Adapter)(nil)

// New builds and validates a BTC adapter. The pool address is resolved to its
// scriptPubKey immediately so a misconfigured pool fails fast (into that
// pool's error state, not the process).
func New(opts Options) (*Adapter, error) {
	if opts.RPC == nil {
		return nil, fmt.Errorf("btc: nil rpc client")
	}
	var params bitcoinbase.AddressParams
	switch strings.ToLower(opts.Network) {
	case "", "mainnet":
		params = bitcoinbase.BTCMainNet
	case "testnet", "signet":
		params = bitcoinbase.BTCTestNet
	case "regtest":
		params = bitcoinbase.BTCRegTest
	default:
		return nil, fmt.Errorf("btc: unknown network %q", opts.Network)
	}
	script, err := bitcoinbase.AddressToScript(opts.PoolAddress, params)
	if err != nil {
		return nil, fmt.Errorf("btc: pool address: %w", err)
	}
	en1, en2 := opts.ExtraNonce1Size, opts.ExtraNonce2Size
	if en1 <= 0 {
		en1 = 4
	}
	if en2 <= 0 {
		en2 = 4
	}
	return &Adapter{
		rpc:          opts.RPC,
		params:       params,
		payoutScript: script,
		tag:          opts.CoinbaseTag,
		en1Size:      en1,
		en2Size:      en2,
	}, nil
}

// Name implements CoinAdapter.
func (a *Adapter) Name() string { return "Bitcoin" }

// Symbol implements CoinAdapter.
func (a *Adapter) Symbol() string { return "BTC" }

// Algo implements CoinAdapter.
func (a *Adapter) Algo() string { return "sha256d" }

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

// Health checks the daemon is reachable and usable for mining: it must answer
// getblockchaininfo and must not be in initial block download.
func (a *Adapter) Health(ctx context.Context) error {
	info, err := a.GetBlockchainInfo(ctx)
	if err != nil {
		return fmt.Errorf("btc health: %w", err)
	}
	if info.InitialBlockDownload {
		return fmt.Errorf("btc health: daemon is in initial block download (%d/%d blocks)",
			info.Blocks, info.Headers)
	}
	return nil
}

// GetBlockTemplate implements CoinAdapter using Bitcoin Core's segwit GBT.
func (a *Adapter) GetBlockTemplate(ctx context.Context) (*coins.BlockTemplate, error) {
	var raw json.RawMessage
	params := []any{map[string]any{"rules": []string{"segwit"}}}
	if err := a.rpc.Call(ctx, "getblocktemplate", params, &raw); err != nil {
		return nil, fmt.Errorf("btc getblocktemplate: %w", err)
	}
	tpl, err := bitcoinbase.ParseTemplate(raw)
	if err != nil {
		return nil, fmt.Errorf("btc: %w", err)
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

// parseTemplateFromRaw rebuilds the typed template from BlockTemplate.Raw.
func parseTemplateFromRaw(tpl *coins.BlockTemplate) (*bitcoinbase.Template, error) {
	raw, err := json.Marshal(tpl.Raw)
	if err != nil {
		return nil, fmt.Errorf("btc: re-marshal template: %w", err)
	}
	return bitcoinbase.ParseTemplate(raw)
}

// BuildMiningJob implements CoinAdapter. Jobs are session-agnostic in Stratum
// V1 (the extranonce placeholder covers every miner), so extraNonce1 is
// intentionally unused; it exists on the interface for coins whose jobs are
// per-connection.
func (a *Adapter) BuildMiningJob(ctx context.Context, tpl *coins.BlockTemplate, extraNonce1 string) (*coins.MiningJob, error) {
	_ = extraNonce1
	base, err := parseTemplateFromRaw(tpl)
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
		wc, err := hexDecode(base.DefaultWitnessCommitment)
		if err != nil {
			return nil, fmt.Errorf("btc: default_witness_commitment: %w", err)
		}
		spec.WitnessCommitmentScript = wc
	}

	// The JobID is assigned by the job manager; build with a placeholder and
	// let the manager stamp it (see internal/jobs).
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
		NotifyParams:  nil, // stamped by the job manager together with JobID
		Raw:           map[string]any{"bitcoinbase": job},
	}, nil
}

// BitcoinbaseJob extracts the underlying bitcoinbase.Job from a MiningJob
// produced by this adapter.
func BitcoinbaseJob(job *coins.MiningJob) (*bitcoinbase.Job, error) {
	v, ok := job.Raw["bitcoinbase"]
	if !ok {
		return nil, fmt.Errorf("btc: mining job missing bitcoinbase payload")
	}
	b, ok := v.(*bitcoinbase.Job)
	if !ok {
		return nil, fmt.Errorf("btc: unexpected bitcoinbase payload type %T", v)
	}
	return b, nil
}

// VersionRollingMask is the version-rolling (ASICBoost) mask the pool
// advertises in mining.configure and enforces on submit.
const VersionRollingMask uint32 = 0x1fffe000

// ValidateShare implements CoinAdapter: it reconstructs the coinbase, merkle
// root, and 80-byte header from the remembered job plus the miner's submit
// fields, hashes with SHA256d, and checks the result against the worker's
// share target and the network target. No I/O happens here; block submission
// is the caller's job when BlockCandidate is true.
func (a *Adapter) ValidateShare(ctx context.Context, job *coins.MiningJob, submit coins.ShareSubmit) (*coins.ShareResult, error) {
	base, err := BitcoinbaseJob(job)
	if err != nil {
		return nil, err
	}

	fields, err := bitcoinbase.ParseSubmitHex(
		submit.ExtraNonce1, submit.ExtraNonce2,
		submit.NTime, submit.Nonce, submit.VersionBits, a.en2Size)
	if err != nil {
		return nil, fmt.Errorf("btc submit: %w", err)
	}
	if len(fields.ExtraNonce1) != a.en1Size {
		return nil, fmt.Errorf("btc submit: extranonce1 is %d bytes, want %d", len(fields.ExtraNonce1), a.en1Size)
	}

	// nTime sanity: not before the template's mintime, not more than 2 hours
	// past the template's curtime (standard future-drift allowance).
	if base.MinTime > 0 && int64(fields.NTime) < base.MinTime {
		return nil, fmt.Errorf("btc submit: ntime %d before template mintime %d", fields.NTime, base.MinTime)
	}
	if int64(fields.NTime) > base.CurTime+7200 {
		return nil, fmt.Errorf("btc submit: ntime %d too far in the future (curtime %d)", fields.NTime, base.CurTime)
	}
	// Version rolling must stay inside the advertised mask.
	if fields.HasVersion && fields.VersionBits&^VersionRollingMask != 0 {
		return nil, fmt.Errorf("btc submit: version bits %08x outside mask %08x", fields.VersionBits, VersionRollingMask)
	}

	// Rebuild coinbase -> merkle root -> header.
	coinbaseTx, err := base.CoinbaseParts.Assemble(fields.ExtraNonce1, fields.ExtraNonce2)
	if err != nil {
		return nil, fmt.Errorf("btc submit: %w", err)
	}
	coinbaseHash := bitcoinbase.DoubleSHA256(coinbaseTx)
	merkleRootLE := bitcoinbase.FoldBranch(coinbaseHash, base.MerkleBranchLE)

	header, err := bitcoinbase.BuildHeader(base, merkleRootLE, fields, VersionRollingMask)
	if err != nil {
		return nil, fmt.Errorf("btc submit: %w", err)
	}
	hashLE := bitcoinbase.DoubleSHA256(header)
	hashBig := vardiff.HashToBig(hashLE)

	result := &coins.ShareResult{ShareDiff: vardiff.ShareDifficulty(hashBig)}

	shareTarget := vardiff.DifficultyToTarget(submit.WorkerDiff)
	result.Valid = vardiff.MeetsTarget(hashBig, shareTarget)

	networkTarget := new(big.Int).SetBytes(job.NetworkTarget)
	if vardiff.MeetsTarget(hashBig, networkTarget) {
		result.BlockCandidate = true
		result.BlockHash = hex.EncodeToString(bitcoinbase.ReverseBytes(hashLE))
		blockHex, err := bitcoinbase.AssembleBlock(header, coinbaseTx, base)
		if err != nil {
			return nil, fmt.Errorf("btc submit: assemble block: %w", err)
		}
		result.BlockHex = blockHex
	}
	return result, nil
}

// SubmitBlock implements CoinAdapter via Bitcoin Core submitblock. A nil/empty
// result means acceptance; any string result is a rejection reason.
func (a *Adapter) SubmitBlock(ctx context.Context, blockHex string) error {
	var result *string
	if err := a.rpc.Call(ctx, "submitblock", []any{blockHex}, &result); err != nil {
		return fmt.Errorf("btc submitblock: %w", err)
	}
	if result != nil && *result != "" {
		return fmt.Errorf("btc submitblock rejected: %s", *result)
	}
	return nil
}

// HashHeader implements CoinAdapter with SHA256d.
func (a *Adapter) HashHeader(header []byte) ([]byte, error) {
	if len(header) != 80 {
		return nil, fmt.Errorf("btc: header is %d bytes, want 80", len(header))
	}
	return bitcoinbase.DoubleSHA256(header), nil
}

// AddressToScript implements CoinAdapter.
func (a *Adapter) AddressToScript(address string) ([]byte, error) {
	return bitcoinbase.AddressToScript(address, a.params)
}

// NormalizeAddress implements CoinAdapter: trims whitespace and verifies the
// address decodes; bech32 is lowercased.
func (a *Adapter) NormalizeAddress(address string) (string, error) {
	addr := strings.TrimSpace(address)
	if a.params.Bech32HRP != "" && strings.HasPrefix(strings.ToLower(addr), a.params.Bech32HRP+"1") {
		addr = strings.ToLower(addr)
	}
	if _, err := bitcoinbase.AddressToScript(addr, a.params); err != nil {
		return "", err
	}
	return addr, nil
}

func hexDecode(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

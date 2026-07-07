// Package coins defines the coin-agnostic adapter contract. The stratum core
// never contains coin-specific logic; every chain is added by implementing
// CoinAdapter. Stage 1 defines the contract and shared types. The bitcoinlike
// SHA256d implementation lands in Stage 2 under internal/coins/bitcoinlike.
package coins

import "context"

// BlockTemplate is the coin-agnostic view of a getblocktemplate result. Adapter
// implementations populate the fields they need; the stratum core treats it as
// opaque beyond Height/Bits.
type BlockTemplate struct {
	Height            int64
	PreviousBlockHash string
	// Bits is the compact network target (nBits) as a hex string.
	Bits string
	// CurTime is the template timestamp.
	CurTime int64
	// CoinbaseValue is the total reward (including fees) in the coin's smallest
	// unit.
	CoinbaseValue int64
	// Raw preserves the full daemon response for adapters that need extra
	// fields not modeled here.
	Raw map[string]any
}

// MiningJob is a ready-to-mine job derived from a template. The stratum core
// caches these in memory and references them by JobID.
type MiningJob struct {
	JobID  string
	PoolID string
	Height int64
	// CleanJobs signals miners to drop stale work (sent as the last
	// mining.notify parameter).
	CleanJobs bool
	// NetworkTarget is the big-endian network target the block must beat.
	NetworkTarget []byte
	// NotifyParams is the exact positional parameter array for mining.notify,
	// built by the adapter so coin-specific fields stay out of the core.
	NotifyParams []any
	// Raw preserves adapter-internal job data (coinbase parts, merkle branch)
	// needed to reconstruct the block on submit.
	Raw map[string]any
}

// ShareSubmit carries the fields from a mining.submit request.
type ShareSubmit struct {
	Worker      string
	JobID       string
	ExtraNonce2 string
	NTime       string
	Nonce       string
	VersionBits string // ASICBoost version rolling, may be empty
	ExtraNonce1 string // assigned to the session at subscribe time
	WorkerDiff  float64

	// Connection metadata for persistence/stats (not used in validation).
	UserAgent string
	RemoteIP  string
}

// ShareResult is the outcome of validating a submitted share.
type ShareResult struct {
	// Valid is true if the share meets the worker's share target.
	Valid bool
	// BlockCandidate is true if the share also meets the network target.
	BlockCandidate bool
	// BlockHash is populated when BlockCandidate is true (big-endian hex).
	BlockHash string
	// BlockHex is the serialized block ready for submitblock, when a candidate.
	BlockHex string
	// ShareDiff is the effective difficulty achieved by this share.
	ShareDiff float64
}

// CoinAdapter is the contract every coin must implement. Implementations must
// never call os.Exit and must return pool-scoped errors on failure so a single
// broken coin cannot take down the process.
type CoinAdapter interface {
	Name() string
	Symbol() string
	Algo() string

	GetBlockTemplate(ctx context.Context) (*BlockTemplate, error)
	BuildMiningJob(ctx context.Context, tpl *BlockTemplate, extraNonce1 string) (*MiningJob, error)
	ValidateShare(ctx context.Context, job *MiningJob, submit ShareSubmit) (*ShareResult, error)
	SubmitBlock(ctx context.Context, blockHex string) error

	HashHeader(header []byte) ([]byte, error)
	AddressToScript(address string) ([]byte, error)
	NormalizeAddress(address string) (string, error)
}

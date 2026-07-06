# Adding a new coin

Coins are added by implementing the `coins.CoinAdapter` interface. The stratum
core never contains coin-specific logic, so a new chain touches only its own
adapter package plus config. The initial target coins are BTC, RXD, SCASH, and ALPH; do not add extra coins until those adapters are stable.

## 1. Create the adapter package

Add `internal/coins/<yourcoin>/adapter.go` with a type that satisfies
`coins.CoinAdapter`:

```go
type Adapter struct { /* rpc client, algo, network params */ }

func (a *Adapter) Name() string   { return "Bitcoin" }
func (a *Adapter) Symbol() string { return "BTC" }
func (a *Adapter) Algo() string   { return "sha256d" }

func (a *Adapter) GetBlockTemplate(ctx context.Context) (*coins.BlockTemplate, error) { ... }
func (a *Adapter) BuildMiningJob(ctx context.Context, tpl *coins.BlockTemplate, en1 string) (*coins.MiningJob, error) { ... }
func (a *Adapter) ValidateShare(ctx context.Context, job *coins.MiningJob, s coins.ShareSubmit) (*coins.ShareResult, error) { ... }
func (a *Adapter) SubmitBlock(ctx context.Context, blockHex string) error { ... }

func (a *Adapter) HashHeader(header []byte) ([]byte, error) { ... }
func (a *Adapter) AddressToScript(address string) ([]byte, error) { ... }
func (a *Adapter) NormalizeAddress(address string) (string, error) { ... }
```

## 2. Reuse the shared building blocks

- **Difficulty/target math** lives in `internal/stratum/vardiff`. Use
  `DifficultyToTarget`, `MeetsTarget`, and `ShareDifficulty` rather than
  re-deriving them. They are covered by known-answer tests.
- If your coin is a Bitcoin-like `getblocktemplate`/`submitblock` chain, build on
  the `bitcoinlike` adapter (Stage 2) and only override the hashing function.
  Hashing is pluggable, so a new algorithm is usually a single function.

## 3. Register the coin in config

```json
{
  "coins": [
    { "symbol": "BTC", "name": "Bitcoin", "algo": "sha256d",
      "rpcUrl": "http://127.0.0.1:8332", "rpcUser": "u", "rpcPassword": "p" }
  ],
  "pools": [
    { "id": "btc-shared", "enabled": true, "coinSymbol": "BTC", "paymentMode": "pplns" }
  ],
  "stratum": { "ports": [ { "port": 3040, "poolId": "btc-shared", "difficulty": 1024 } ] }
}
```

## Safety rules (enforced by the supervisor)

- **Never call `os.Exit`** anywhere in an adapter, RPC client, template poller,
  job manager, or share validator. Return a pool-scoped error instead.
- Adapter failures are caught by the per-pool panic-recovery boundary in
  `internal/pool`. A broken daemon moves only that pool to the `error` state and
  retries on its configured backoff; every other pool keeps running.
- Consensus-critical code (hashing, target comparison, block serialization) must
  be verified against known-answer vectors before shipping.

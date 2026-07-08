// Package config loads and validates the GoStratumPool configuration.
//
// Stage 1 uses JSON (like Miningcore) so the loader is pure standard library
// and compiles with zero external dependencies. When the yaml module is
// whitelisted in a later stage the loader can gain a YAML front-end without
// touching any of the consuming code; everything downstream depends only on
// the typed structs below.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// DeploymentMode selects how this process behaves. See the architecture doc.
type DeploymentMode string

const (
	ModeAllInOne DeploymentMode = "all-in-one" // dev: everything in one process
	ModeMaster   DeploymentMode = "master"     // owns PostgreSQL, coin configs, rewards
	ModeRegional DeploymentMode = "regional"   // accepts miners, validates, forwards shares
)

// Config is the root configuration object.
type Config struct {
	// Mode is the deployment mode for this process.
	Mode DeploymentMode `json:"mode"`
	// Region is a free-form label (e.g. "us", "eu", "asia") used for stats and
	// share provenance. Required in regional mode.
	Region string `json:"region"`
	// NodeID uniquely identifies this process within a region.
	NodeID string `json:"nodeId"`

	Logging LoggingConfig `json:"logging"`
	Stratum StratumConfig `json:"stratum"`

	// Pools declares every pool/coin this process is aware of. Each is an
	// isolated service with its own lifecycle (see the isolation spec).
	Pools []PoolConfig `json:"pools"`
	// Coins declares coin-level daemon connectivity, keyed by symbol.
	Coins []CoinConfig `json:"coins"`

	// Postgres configures Stage 4 persistence. Disabled by default so the
	// stratum core keeps running databaseless (dev, regionals in Stage 6).
	Postgres PostgresConfig `json:"postgres"`

	// API configures the HTTP interface (public stats + admin lifecycle).
	API APIConfig `json:"api"`

	// NATS configures Stage 6 master/regional messaging.
	NATS NATSConfig `json:"nats"`

	// Banning configures per-IP misbehavior bans.
	Banning BanningConfig `json:"banning"`
}

// BanningConfig tunes per-IP banning.
type BanningConfig struct {
	Enabled bool `json:"enabled"`
	// InvalidPercent bans once this invalid-share percentage is reached over
	// CheckThreshold shares (default 50).
	InvalidPercent float64 `json:"invalidPercent"`
	// CheckThreshold is the minimum shares before the ratio is judged
	// (default 50).
	CheckThreshold int `json:"checkThreshold"`
	// MalformedThreshold bans after this many malformed lines (default 5).
	MalformedThreshold int `json:"malformedThreshold"`
	// FailedAuthThreshold bans after this many failed authorizations
	// (default 10).
	FailedAuthThreshold int `json:"failedAuthThreshold"`
	// BanDuration is how long bans last (default 10m).
	BanDuration Duration `json:"banDuration"`
}

// NATSConfig configures JetStream messaging.
type NATSConfig struct {
	Enabled bool     `json:"enabled"`
	URLs    []string `json:"urls"`
	// SpoolDir holds the on-disk spool for events that could not be published
	// (regional resilience). Default: ./spool
	SpoolDir string `json:"spoolDir"`
	// SpoolMaxBytes bounds the spool file (default 256 MiB).
	SpoolMaxBytes int64 `json:"spoolMaxBytes"`
}

// APIConfig configures the HTTP API server.
type APIConfig struct {
	Enabled bool   `json:"enabled"`
	Bind    string `json:"bind"`
	// AdminToken guards the /api/admin routes (Bearer auth). Empty disables
	// the admin routes while keeping the public API up.
	AdminToken string `json:"adminToken"`
}

// LoggingConfig controls the root logger.
type LoggingConfig struct {
	Level string `json:"level"`
	JSON  bool   `json:"json"`
}

// StratumConfig groups the stratum-facing listener settings.
type StratumConfig struct {
	// BindAddress is the interface stratum ports listen on, e.g. "0.0.0.0".
	BindAddress string `json:"bindAddress"`
	// Ports is the set of TCP ports miners can connect to. Each maps to exactly
	// one pool.
	Ports []PortConfig `json:"ports"`
	// MaxConnPerIP caps simultaneous connections from a single IP (0 = no cap).
	MaxConnPerIP int `json:"maxConnPerIp"`
	// ReadTimeout bounds how long a socket may be idle before being dropped.
	ReadTimeout Duration `json:"readTimeout"`
	// MaxLineBytes caps a single JSON-RPC line to defend against memory-abuse
	// spam. 0 falls back to a safe default.
	MaxLineBytes int `json:"maxLineBytes"`

	// VarDiffTargetInterval is the desired time between shares per worker
	// (default 10s).
	VarDiffTargetInterval Duration `json:"varDiffTargetInterval"`
	// VarDiffRetargetInterval is how often difficulty is reconsidered
	// (default 60s).
	VarDiffRetargetInterval Duration `json:"varDiffRetargetInterval"`
	// VarDiffVariancePercent is the no-adjust tolerance band (default 30).
	VarDiffVariancePercent float64 `json:"varDiffVariancePercent"`
}

// PortConfig describes one stratum listening port. Each port maps to one pool.
type PortConfig struct {
	Port   int    `json:"port"`
	PoolID string `json:"poolId"`

	// VarDiff toggles per-worker variable difficulty. When false, Difficulty is
	// a fixed starting/only difficulty.
	VarDiff bool `json:"varDiff"`

	Difficulty float64 `json:"difficulty"`
	MinDiff    float64 `json:"minDiff"`
	MaxDiff    float64 `json:"maxDiff"`

	// TLS is reserved: Stage 1 designs for it but does not terminate TLS yet.
	TLS bool `json:"tls"`
}

// PoolLifecycleState mirrors pool.State but lives here so config can declare an
// initial state without importing the pool package (avoids an import cycle).
type PoolLifecycleState string

const (
	StateActive      PoolLifecycleState = "active"
	StateDraining    PoolLifecycleState = "draining"
	StateMaintenance PoolLifecycleState = "maintenance"
	StatePaused      PoolLifecycleState = "paused"
	StateDisabled    PoolLifecycleState = "disabled"
	StateError       PoolLifecycleState = "error"
)

// PoolConfig declares one pool/coin service.
type PoolConfig struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
	// CoinSymbol links this pool to a CoinConfig.
	CoinSymbol string `json:"coinSymbol"`
	// PaymentMode is one of "pplns", "prop", "solo".
	PaymentMode string `json:"paymentMode"`
	// InitialState is the lifecycle state to boot into (default active).
	InitialState PoolLifecycleState `json:"initialState"`
	// MaintenanceMessage is returned to miners that try to authorize while the
	// pool is in maintenance.
	MaintenanceMessage string `json:"maintenanceMessage"`
	// PoolFeePercent is subtracted from block rewards.
	PoolFeePercent float64 `json:"poolFeePercent"`

	// Address is the pool wallet address receiving coinbase rewards for this
	// pool. Required for pools with a wired coin adapter.
	Address string `json:"address"`

	// PPLNSFactor sizes the PPLNS window: N = factor x network difficulty
	// (default 2.0). Only used when paymentMode is "pplns".
	PPLNSFactor float64 `json:"pplnsFactor"`
	// RewardInterval is rewardd's per-pool processing cadence (default 30s).
	RewardInterval Duration `json:"rewardInterval"`
	// CoinbaseTag is embedded in the coinbase scriptSig (e.g. "/ICMINERS/").
	CoinbaseTag string `json:"coinbaseTag"`

	// TemplatePollInterval controls how often the template poller runs (later
	// stages). Kept here so the field is stable from the start.
	TemplatePollInterval Duration `json:"templatePollInterval"`
	// ErrorBackoff controls retry delay when a pool is in the error state.
	ErrorBackoff Duration `json:"errorBackoff"`
}

// PostgresConfig configures persistence.
type PostgresConfig struct {
	Enabled bool   `json:"enabled"`
	DSN     string `json:"dsn"`
	// ShareQueueSize is the writer's buffered queue (default 65536).
	ShareQueueSize int `json:"shareQueueSize"`
	// ShareBatchSize triggers a flush at this many shares (default 500).
	ShareBatchSize int `json:"shareBatchSize"`
	// ShareFlushInterval flushes small batches at this cadence (default 2s).
	ShareFlushInterval Duration `json:"shareFlushInterval"`
}

// CoinConfig describes a coin daemon.
type CoinConfig struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
	Algo   string `json:"algo"`

	RPCURL      string `json:"rpcUrl"`
	RPCUser     string `json:"rpcUser"`
	RPCPassword string `json:"rpcPassword"`

	// Network selects chain parameters where relevant: "mainnet" (default),
	// "testnet", or "regtest".
	Network string `json:"network"`

	// MaturityDepth is the confirmations required before a coinbase is
	// spendable and rewards are credited (Bitcoin: 100).
	MaturityDepth int64 `json:"maturityDepth"`
	// OrphanDepth: a block absent from the best chain is declared orphaned
	// once the chain is this far past its height (default 12).
	OrphanDepth int64 `json:"orphanDepth"`
}

// CoinBySymbol returns the coin config with the given symbol (case-insensitive).
func (c *Config) CoinBySymbol(symbol string) (CoinConfig, bool) {
	for _, coin := range c.Coins {
		if strings.EqualFold(coin.Symbol, symbol) {
			return coin, true
		}
	}
	return CoinConfig{}, false
}

// Load reads, parses, and validates a JSON config file, applying defaults.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Stratum.VarDiffTargetInterval <= 0 {
		c.Stratum.VarDiffTargetInterval = Duration(10 * time.Second)
	}
	if c.Stratum.VarDiffRetargetInterval <= 0 {
		c.Stratum.VarDiffRetargetInterval = Duration(60 * time.Second)
	}
	if c.Stratum.VarDiffVariancePercent <= 0 {
		c.Stratum.VarDiffVariancePercent = 30
	}
	for i := range c.Pools {
		if c.Pools[i].PPLNSFactor <= 0 {
			c.Pools[i].PPLNSFactor = 2.0
		}
		if c.Pools[i].RewardInterval <= 0 {
			c.Pools[i].RewardInterval = Duration(30 * time.Second)
		}
	}
	for i := range c.Coins {
		if c.Coins[i].MaturityDepth <= 0 {
			c.Coins[i].MaturityDepth = 100
		}
		if c.Coins[i].OrphanDepth <= 0 {
			c.Coins[i].OrphanDepth = 12
		}
	}
	if c.Mode == "" {
		c.Mode = ModeAllInOne
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Stratum.BindAddress == "" {
		c.Stratum.BindAddress = "0.0.0.0"
	}
	if c.Stratum.MaxLineBytes == 0 {
		c.Stratum.MaxLineBytes = 64 * 1024
	}
	if c.Stratum.ReadTimeout == 0 {
		c.Stratum.ReadTimeout = Duration(10 * time.Minute)
	}
	for i := range c.Pools {
		if c.Pools[i].InitialState == "" {
			c.Pools[i].InitialState = StateActive
		}
		if c.Pools[i].TemplatePollInterval == 0 {
			c.Pools[i].TemplatePollInterval = Duration(500 * time.Millisecond)
		}
		if c.Pools[i].ErrorBackoff == 0 {
			c.Pools[i].ErrorBackoff = Duration(15 * time.Second)
		}
	}
}

// Validate checks structural invariants that would otherwise fail confusingly
// at runtime.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeAllInOne, ModeMaster, ModeRegional:
	default:
		return fmt.Errorf("invalid mode %q", c.Mode)
	}
	if c.Mode == ModeRegional && c.Region == "" {
		return fmt.Errorf("region is required in regional mode")
	}

	poolIDs := map[string]bool{}
	for _, p := range c.Pools {
		if p.ID == "" {
			return fmt.Errorf("pool with empty id")
		}
		if poolIDs[p.ID] {
			return fmt.Errorf("duplicate pool id %q", p.ID)
		}
		poolIDs[p.ID] = true
		switch strings.ToLower(p.PaymentMode) {
		case "", "pplns", "prop", "solo":
		default:
			return fmt.Errorf("pool %q: invalid paymentMode %q", p.ID, p.PaymentMode)
		}
	}

	coinSymbols := map[string]bool{}
	for _, coin := range c.Coins {
		if coin.Symbol == "" {
			return fmt.Errorf("coin with empty symbol")
		}
		coinSymbols[strings.ToUpper(coin.Symbol)] = true
	}

	seenPorts := map[int]bool{}
	for _, prt := range c.Stratum.Ports {
		if prt.Port <= 0 || prt.Port > 65535 {
			return fmt.Errorf("invalid stratum port %d", prt.Port)
		}
		if seenPorts[prt.Port] {
			return fmt.Errorf("duplicate stratum port %d", prt.Port)
		}
		seenPorts[prt.Port] = true
		if prt.PoolID == "" {
			return fmt.Errorf("stratum port %d has no poolId", prt.Port)
		}
		if !poolIDs[prt.PoolID] {
			return fmt.Errorf("stratum port %d maps to unknown poolId %q", prt.Port, prt.PoolID)
		}
		if prt.VarDiff {
			if prt.MinDiff <= 0 || prt.MaxDiff <= 0 || prt.MinDiff > prt.MaxDiff {
				return fmt.Errorf("stratum port %d: invalid vardiff min/max", prt.Port)
			}
		} else if prt.Difficulty <= 0 {
			return fmt.Errorf("stratum port %d: fixed difficulty must be > 0", prt.Port)
		}
	}

	// Pools referencing a coin must reference one that exists (when any coins
	// are declared at all; Stage 1 configs may omit coins entirely).
	if len(c.Coins) > 0 {
		for _, p := range c.Pools {
			if p.CoinSymbol != "" && !coinSymbols[strings.ToUpper(p.CoinSymbol)] {
				return fmt.Errorf("pool %q references unknown coin %q", p.ID, p.CoinSymbol)
			}
		}
	}
	if c.Postgres.Enabled && strings.TrimSpace(c.Postgres.DSN) == "" {
		return fmt.Errorf("postgres.enabled requires postgres.dsn")
	}
	if c.API.Enabled && strings.TrimSpace(c.API.Bind) == "" {
		return fmt.Errorf("api.enabled requires api.bind")
	}
	if c.NATS.Enabled && len(c.NATS.URLs) == 0 {
		return fmt.Errorf("nats.enabled requires nats.urls")
	}
	if c.Mode == ModeMaster && !c.Postgres.Enabled {
		return fmt.Errorf("master mode requires postgres (it persists consumed events)")
	}
	if c.Mode == ModeMaster && !c.NATS.Enabled {
		return fmt.Errorf("master mode requires nats (it consumes regional events)")
	}
	if c.Mode == ModeRegional && !c.NATS.Enabled {
		return fmt.Errorf("regional mode requires nats (it publishes events to the master)")
	}
	return nil
}

// PoolByID returns the pool config with the given id, if present.
func (c *Config) PoolByID(id string) (PoolConfig, bool) {
	for _, p := range c.Pools {
		if p.ID == id {
			return p, true
		}
	}
	return PoolConfig{}, false
}

// Duration is a time.Duration that (de)serializes from a human string such as
// "500ms" or "15s" in JSON, while remaining a plain duration in Go code.
type Duration time.Duration

// MarshalJSON renders the duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts either a duration string ("15s") or a number of
// nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case float64:
		*d = Duration(time.Duration(val))
	case string:
		parsed, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", val, err)
		}
		*d = Duration(parsed)
	default:
		return fmt.Errorf("invalid duration value %v", v)
	}
	return nil
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

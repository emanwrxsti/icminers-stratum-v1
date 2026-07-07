# GoStratumPool

A production-oriented Stratum V1 mining pool server in Go, built as a **multi-pool
supervisor**: every coin/pool runs as an isolated service with its own lifecycle,
context, and goroutines, so maintaining, restarting, or breaking one coin never
affects any other pool or the global stratum server.

This repository is being built in clean stages. **Stages 1–6 are complete and
this is the current milestone**: a compiling, tested, zero-dependency BTC pool
core. Miners subscribe/authorize, receive real Bitcoin Core
`getblocktemplate`-derived `mining.notify` jobs (coinbase split, merkle
branch, cleanJobs semantics), and submit shares that are fully validated in
memory — header reconstruction, share/network target checks, version rolling,
ntime window, duplicate detection — with block candidates assembled
(witness-aware) and pushed to the daemon via `submitblock` off the reply path.
Accepted shares and found blocks now persist to PostgreSQL through an async
batched writer that keeps every database write off the stratum submit path
(monthly-partitioned `shares`, idempotent `blocks`). An in-process HTTP API serves live stats
(hashrate, shares, blocks per pool and miner), DB-backed block/miner history,
and token-guarded per-pool admin lifecycle control
(pause/resume/drain/maintenance/restart/disable). Master/regional deployment is live: regional stratum
nodes publish shares, blocks, and pool-state over NATS JetStream (with a
bounded disk spool that survives full NATS outages), the master consumes them
into PostgreSQL, and the master's admin API issues per-pool lifecycle commands
that every regional follows. The remaining stages add the PPLNS/PROP/SOLO
reward calculators and hardening (vardiff controller, banning, metrics). RXD, SCASH, and ALPH
are intentionally not implemented yet. See [docs/roadmap.md](docs/roadmap.md)
for exactly what is done and what is pending. CI runs gofmt/vet/test/race/build
on every push (`.github/workflows/go.yml`).

## Design guarantees baked in from Stage 1

- **No global shutdown path.** There is deliberately no "restart the whole
  stratum" operation. Pool actions are per-pool only (`internal/pool`).
- **Panic isolation.** Each pool's work loop runs under a `recover()` boundary;
  a panic moves only that pool to the `error` state and backs off. Verified by
  `TestPanickingHookDoesNotBreakManager` and the supervision loop.
- **Hot path never blocks on I/O.** The stratum reply path touches only in-memory
  state. PostgreSQL and master round-trips are kept off the submit path by
  design (async writers/queues arrive in Stages 4 and 6).
- **Coin-agnostic core.** All coin logic lives behind `coins.CoinAdapter`; the
  stratum server has zero coin-specific code.

## Dependency note

`go.sum` was generated in an offline environment where `golang.org/x/*` and
`gopkg.in/*` modules were mirrored from their GitHub sources (the module
proxy was unreachable). The zip hashes should match the official proxy; if CI
ever reports a checksum mismatch on those modules, regenerate with:

```
go clean -modcache && rm go.sum && go mod tidy
```

## Requirements

- Go 1.22 or newer. External dependencies: `github.com/jackc/pgx/v5` (Stage 4)
  and `github.com/nats-io/nats.go` (Stage 6); everything else is the standard
  library), so it builds fully offline. `pgx`, NATS, and a YAML front-end are
  introduced in the stages that need them.

## Run (all-in-one, for development)

```bash
go build -o bin/stratumd ./cmd/stratumd
./bin/stratumd -config configs/config.example.json
# or:
./scripts/run-dev.sh
```

You should see each configured port bind to its pool. Point a miner (or a plain
TCP client) at `127.0.0.1:3032` and send `mining.subscribe` then
`mining.authorize`; you will receive an extranonce1, a subscription reply, and a
`mining.set_difficulty` notification. Share submission returns an honest
"not enabled yet (stage 3)" error until the job manager lands.

## Deployment modes

`mode` in the config selects behavior:

- `all-in-one` — everything in one process (development). Fully functional today
  for the stratum handshake and lifecycle.
- `master` — owns the central PostgreSQL connection, coin configs, template
  polling, reward calculation, and the main API. *Scaffolded; completed in
  Stages 4–7.*
- `regional` — accepts miner connections, validates shares locally, replies
  immediately, and forwards accepted shares to the master over NATS with local
  spooling during disconnects. *Scaffolded; completed in Stage 6.*

Master and regional processes are the same binary family with different configs;
the extranonce1 allocator is seeded from `nodeId` so nodes never collide on a
shared coin.

## Per-pool maintenance (the core operational workflow)

Every pool has an independent lifecycle: `active`, `draining`, `maintenance`,
`paused`, `disabled`, `error`. To work on only Flowcoin while everything else
keeps mining:

1. Drain `flowcoin-shared` (stops new jobs, keeps accepting in-flight shares).
2. Wait out the grace period; it auto-advances to `maintenance`.
3. Restart/edit the Flowcoin daemon or adapter.
4. Resume `flowcoin-shared`.

Bitcoin, Radiant, and every other pool stay online the entire time. Once the
admin API lands (Stage 5) these map to:

```
POST /api/admin/pools/{poolId}/drain
POST /api/admin/pools/{poolId}/maintenance
POST /api/admin/pools/{poolId}/resume
POST /api/admin/pools/{poolId}/pause
POST /api/admin/pools/{poolId}/restart
POST /api/admin/pools/{poolId}/disable
GET  /api/admin/pools/{poolId}/state
```

The underlying `pool.PoolLifecycleManager` (with `StartPool`, `StopPool`,
`PausePool`, `ResumePool`, `DrainPool`, `PutPoolInMaintenance`, `DisablePool`,
`RestartPool`, `GetPoolState`) is already implemented and tested. In master mode
these actions publish over NATS; regional nodes apply them to the matching pool
id only via `ApplyRemoteState`.

## Configuration

Stage 1 uses JSON (unambiguous to hand-edit, like Miningcore). A YAML front-end
can be added later without changing any consuming code, since everything
downstream depends only on the typed structs in `internal/config`. See
`configs/config.example.json` for a full annotated example. Validation is strict:
unknown fields, duplicate ports, ports mapping to unknown pools, bad vardiff
ranges, and invalid payment modes are all rejected at load time.

## Adding a coin

See [docs/adding-a-coin.md](docs/adding-a-coin.md). In short: implement
`coins.CoinAdapter`, reuse `internal/stratum/vardiff` for target math, and
register the coin/pool/port in config.

## Testing

```bash
go test -race ./...
```

Stage 1 covers JSON-RPC parsing (including malformed-spam and line-cap defense),
difficulty/target conversion (with known-answer vectors), extranonce uniqueness
(including concurrent allocation), config load/validation, and the full pool
lifecycle state machine (isolation, drain→maintenance, panic-safe hooks).

## Project layout

```
cmd/stratumd/            stratum node entrypoint (all-in-one / regional)
internal/config/         typed config + strict validation
internal/logging/        slog wrapper with per-pool/per-component labels
internal/stratum/        TCP server + JSON-RPC handler
  protocol/              message types + newline-delimited codec
  session/               per-connection state, session manager, extranonce
  vardiff/               difficulty/target math (vardiff controller: Stage 8)
internal/pool/           multi-pool supervisor + PoolLifecycleManager
internal/coins/          CoinAdapter interface + shared coin types
configs/                 example config
scripts/                 dev run script
deploy/                  systemd unit
docs/                    roadmap, adding-a-coin guide
```

Directories for later stages (`storage/postgres`, `messaging/nats`,
`rewards/{pplns,prop,solo}`, `api`, `spool`, `bans`, `metrics`, and the
`apid`/`rewardd`/`payoutd` commands) are introduced as those stages are built,
to avoid shipping empty scaffolding that does not compile to anything useful yet.

## License

Zero dev fee. Free for the mining community.

# Build roadmap

The first working end-to-end milestone is BTC mining through Stratum V1 with accepted shares recorded in PostgreSQL. After BTC works, implement RXD, then SCASH, then ALPH. Stages are built in order; each is kept compiling and tested before the next begins.

## Stage 1 — DONE (this deliverable)

- [x] Project layout and Go module (no external deps).
- [x] Strict JSON config loader with validation (`internal/config`).
- [x] Structured logger with per-pool/per-component labels (`internal/logging`).
- [x] TCP stratum server with per-port listeners mapped to pools.
- [x] Newline-delimited JSON-RPC codec with line-size cap and malformed handling.
- [x] Miner session manager with per-IP connection caps and extranonce1 allocator.
- [x] `mining.subscribe`, `mining.authorize`, `mining.configure`,
      `client.get_version`; static per-port difficulty via `mining.set_difficulty`.
- [x] `coins.CoinAdapter` interface and shared coin types.
- [x] **Multi-pool supervisor**: `pool.PoolLifecycleManager` with all six
      lifecycle states, panic-recovered per-pool loops, drain→maintenance grace,
      and `ApplyRemoteState` for NATS-driven changes (from the isolation spec).
- [x] Unit tests: JSON-RPC, difficulty/target (KAT vectors), extranonce
      (incl. concurrent), config validation, lifecycle isolation. All pass with
      `-race`.

## Stage 2 — daemon RPC + job manager

- [ ] `internal/coins/rpc`: JSON-RPC-over-HTTP daemon client.
- [ ] `internal/coins/bitcoinbase`: shared Bitcoin-like template/coinbase/merkle code.
- [ ] `internal/coins/btc`: BTC adapter using SHA256d and `getblocktemplate`/`submitblock`.
- [ ] `internal/coins/radiant`: RXD adapter using shared Bitcoin-like code plus SHA512/256d hashing.
- [ ] `internal/coins/scash`: SCASH adapter using shared Bitcoin-like code plus RandomX behind a hash engine.
- [ ] `internal/coins/alephium`: ALPH adapter after BTC/RXD/SCASH are stable; do not force it into the Bitcoin-like model.
- [ ] Per-pool template poller wired into the existing `runOnce` recovery seam.
- [ ] extranonce1/extranonce2 job wiring.
- [ ] Tests: extranonce handling, notify param assembly.

## Stage 3 — share validation

- [ ] `mining.submit` handling, header reconstruction, target/difficulty check.
- [ ] Duplicate-share cache (in memory, per pool).
- [ ] Block-candidate detection and immediate `submitblock`.
- [ ] Tests: difficulty, target, share validation, duplicate detection.

## Stage 4 — PostgreSQL

- [ ] `internal/storage/postgres` with pgx + migrations (partitioned `shares`).
- [ ] Async batched share writer (off the submit hot path).
- [ ] Block persistence, worker/pool stats storage.

## Stage 5 — REST API + admin lifecycle endpoints

- [ ] `cmd/apid`, `internal/api`, `internal/stats`.
- [ ] Public: health, pools, miners, workers, blocks, payments, regions.
- [ ] Admin: pause/resume/drain/maintenance/restart/disable/state (wired to the
      already-built `PoolLifecycleManager`).

## Stage 6 — NATS master/regional

- [ ] `internal/messaging/nats`, master + regional modes.
- [ ] Regional publishes shares/events; master consumes and persists.
- [ ] Master publishes config/job/pool-state updates; `StateHook` implemented.
- [ ] `internal/spool`: local spool + replay during disconnects.

## Stage 7 — rewards

- [ ] `cmd/rewardd`, `internal/rewards/{pplns,prop,solo}`.
- [ ] Credit balances only after block maturity; handle orphans.

## Stage 8 — hardening

- [ ] Per-worker vardiff controller (`internal/stratum/vardiff`).
- [ ] Banning (`internal/bans`), region health, metrics, graceful shutdown polish.


## Initial coin implementation order

See `docs/coin-plan.md` for details. The order is BTC -> RXD -> SCASH -> ALPH.

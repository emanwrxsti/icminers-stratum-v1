# Build roadmap

The first working end-to-end milestone is one Bitcoin-like SHA256d coin mining
through Stratum V1 with accepted shares recorded in PostgreSQL. Stages are built
in order; each is kept compiling and tested before the next begins.

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

## Stage 2 — DONE: daemon RPC + BTC job manager

- [x] `internal/coins/rpc`: JSON-RPC-over-HTTP daemon client (basic auth,
      bitcoind 500-with-body error handling, context cancellation).
- [x] `internal/coins/bitcoinbase`: shared Bitcoin-family building blocks —
      getblocktemplate model/parsing, coinbase construction with the stratum
      coinb1/coinb2 extranonce split (BIP34 height, tag, witness commitment),
      merkle branch/root math, nBits→target, prevhash stratum encoding,
      base58check + bech32/bech32m address→script, mining.notify assembly.
- [x] `internal/coins/btc`: BTC adapter (SHA256d) over Bitcoin Core RPC:
      getblocktemplate (segwit rules), submitblock, getblockchaininfo health
      (rejects IBD). mainnet/testnet/regtest address params.
- [x] `internal/jobs`: per-pool job manager + registry — template change
      detection, cleanJobs semantics (true on new tip, false on refresh),
      job-id stamping, bounded job history for the Stage 3 submit path,
      mining.notify broadcast fan-out.
- [x] Per-pool template poller wired into the pool supervisor's `runOnce`
      recovery seam via the `pool.Poller` interface; a daemon failure moves
      only that pool into the error state.
- [x] `mining.notify` on new jobs and immediately after authorize (cleanJobs
      forced true for fresh sessions). `mining.submit` still returns an honest
      "stage 3" error.
- [x] Adapter-init failures put only that pool into maintenance at startup.
- [x] Tests: fake HTTP RPC daemon, GBT parsing, coinbase KATs, merkle KATs
      (genesis, block 170, genesis header hash), notify assembly, BTC health,
      job dedup/clean-flag/eviction. All pass with `-race`.
- [x] GitHub Actions CI (`.github/workflows/go.yml`): gofmt check, vet, test,
      test -race, build.

RXD, SCASH, and ALPH are intentionally NOT implemented yet; they land behind
the same `CoinAdapter` + `bitcoinbase` seams in later stages.

## Stage 3 — DONE: share validation + submitblock

- [x] Real `mining.submit`: param parsing, worker-authorization check,
      lifecycle share-gating, error codes 20/21/22/23/24/25.
- [x] Header reconstruction (`bitcoinbase.BuildHeader`): coinbase reassembly
      from coinb1‖en1‖en2‖coinb2, merkle fold, ntime/nonce/version LE encoding,
      version-rolling under the advertised 1fffe000 mask.
- [x] Share target (worker difficulty) vs network target (nBits) checks;
      per-share achieved difficulty reported.
- [x] ntime window enforcement (>= template mintime, <= curtime + 2h);
      extranonce width and hex validation.
- [x] Per-pool, per-job duplicate-share cache; entries evicted with their job.
- [x] Block-candidate detection, full block assembly (witness-serialized
      coinbase with the BIP141 reserved value when the template carries a
      witness commitment, template txs appended in order), async `submitblock`
      off the reply path with panic isolation.
- [x] Tests: independently-mined block candidates (second header
      implementation in the tests), block hex verification, low-difficulty,
      version-rolling accept/reject, ntime/extranonce guards, duplicate/stale
      handling, submitblock capture on a fake daemon. All pass with `-race`.
- [x] Live smoke test: a Python miner mined a share client-side from the
      mining.notify fields alone, the pool accepted it, rejected the duplicate
      (22) and a stale job (21), and submitted the block to the daemon with a
      byte-identical header.

Persistence of accepted shares is intentionally deferred to Stage 4's async
writer; the submit reply path performs no I/O.

## Stage 4 — DONE: PostgreSQL persistence

- [x] `internal/storage/postgres` on pgx v5 (pooled via pgxpool).
- [x] Embedded, versioned, idempotent migrations (`schema_migrations`).
- [x] `shares` range-partitioned monthly by `created`; partitions created on
      demand for the current + next month and re-checked on month rollover, so
      retention is a cheap partition drop.
- [x] Async batched share writer: non-blocking `Enqueue` (drop-and-count on a
      full queue — the submit path NEVER waits on the database), COPY-based
      bulk insert, size/interval flush triggers, one immediate retry then
      bounded retention across database outages, final flush on shutdown.
- [x] `blocks` table with pending status; idempotent insert on
      (poolid, blockheight, hash).
- [x] Recorder seam: `jobs.Recorder` events (share + block) adapted to storage
      in `cmd/stratumd`; the jobs package has zero database knowledge, which
      keeps the same events reusable for NATS in Stage 6.
- [x] Share rows carry miner/worker split, useragent, IP, and region source.
- [x] Integration tests against a real PostgreSQL 16 (skipped cleanly when
      `POOL_TEST_PG_DSN` is unset): migration idempotence, partition creation,
      batch + interval + close-flush behavior, full-queue drop accounting,
      duplicate block insert. CI now runs a postgres:16 service.
- [x] Databaseless mode still fully supported (`postgres.enabled: false`).

Worker/pool live stats aggregation moves to Stage 5 with the API that serves it.

## Stage 5 — DONE: HTTP API + admin lifecycle endpoints

- [x] `internal/api`: HTTP server (stdlib mux, method+path patterns) served
      in-process by `stratumd` (`api.enabled` / `api.bind` in config).
- [x] Public endpoints: `GET /api/health`, `GET /api/pools`,
      `GET /api/pools/{id}`, `GET /api/pools/{id}/blocks` (DB, paginated),
      `GET /api/pools/{id}/miners` (DB, windowed top miners),
      `GET /api/pools/{id}/miners/{miner}` (live + DB worker breakdown).
      DB-backed endpoints return 503 in databaseless mode.
- [x] Admin endpoints (Bearer token, constant-time compare; routes not even
      registered without a configured token):
      `POST /api/admin/pools/{id}/{pause,resume,drain,maintenance,restart,disable}`
      and `GET /api/admin/pools/{id}/state`. Drain takes
      `{"gracePeriodSeconds"}`, maintenance takes `{"message"}`. Illegal
      transitions map to 409, unknown pools to 404 — and every action touches
      exactly ONE pool.
- [x] `internal/stats`: live in-memory collector implementing `jobs.Recorder`
      — per-pool and per-miner share/block counters and 1m/15m sliding-window
      hashrate (bounded minute rings, capped miner map with stale eviction).
- [x] `internal/storage/postgres` read queries: `ListBlocks`, `TopMiners`,
      `MinerWorkers` (windowed aggregation with hashrate derivation).
- [x] Recorder fan-out in `cmd/stratumd` (live stats + postgres together).
- [x] Tests: full admin auth/lifecycle/isolation flows over httptest with a
      real lifecycle manager, stats window/cap/eviction math, DB query
      integration tests. Live smoke: miner mined; API served live stats, DB
      blocks/miners; maintenance via API rejected new miners on that pool with
      the message while a second pool kept authorizing.

`cmd/apid` as a separate binary is deferred to Stage 6: a standalone API can
only control remote pools once NATS carries lifecycle commands. In-process API
is the correct shape for all-in-one and regional deployments today.

## Stage 6 — DONE: NATS master/regional

- [x] `internal/messaging/nats` on nats.go v1.37 (JetStream API): streams
      `POOLEVENTS` (shares/blocks/poolstate, 72h limit) and `POOLCMD`
      (lifecycle commands, 1h limit); subjects
      `shares.<region>.<poolId>`, `blocks.<region>.<poolId>`,
      `poolstate.<region>.<poolId>`, `commands.pool.<poolId>`.
- [x] Regional publisher implements `jobs.Recorder` AND `pool.StateHook`:
      async JetStream publishes on the hot path; every failed publish lands in
      the disk spool via the JetStream async-error handler (no per-share
      goroutines, no silent loss).
- [x] `internal/spool`: bounded JSONL spool with atomic rewrite, partial-drain
      resume (a failed replay keeps the remainder), crash-restart recovery.
      Background drain loop republishes whenever connectivity returns.
- [x] Master durable consumer (`master-persist`, explicit ack, at-least-once)
      feeds the Stage 4 async share writer and idempotent block insert; state
      changes logged. Poison messages are acked away, never redelivered
      forever.
- [x] Lifecycle commands: master admin API is the command authority —
      publish-first (202 when not applicable locally), regionals subscribe
      with DeliverNew (no stale-order replay on restart) and apply each
      command to exactly ONE pool via the existing lifecycle manager;
      commands for unhosted pools are ignored cleanly.
- [x] Mode wiring: master requires postgres+nats and consumes; regional
      requires nats, publishes, and follows commands; all-in-one stays fully
      local (NATS optional). Regional starts fine with NATS down (spool +
      background stream retry); master fails fast.
- [x] Tests (auto-spawn a throwaway `nats-server -js`, or use
      `POOL_TEST_NATS_URL`; skip when neither exists): publish→consume
      roundtrip for all three event kinds, durable backlog delivery, spool
      replay into the consumer, command roundtrip with single-pool isolation
      on a live lifecycle manager. Spool unit tests cover truncate,
      partial-failure remainder, size bound, and crash-reopen. CI starts a
      `nats:2.10-alpine -js` container.
- [x] Live smoke: true master/regional split — miner mined on the regional
      (no local database), the share and block appeared in the MASTER's
      PostgreSQL via NATS, and a master admin command put the regional's pool
      into maintenance (miners saw the master's message) and resumed it.

`cmd/apid` remains folded into `stratumd`'s in-process API: master mode IS the
standalone command-authority API (it needs no stratum ports). A separate
binary would duplicate the same wiring.

## Stage 7 — DONE: rewards (`rewardd`)

- [x] Exact-integer accounting end to end: the template's `coinbasevalue`
      (base units / satoshis) travels on the mining job → block event → NATS →
      `blocks.reward_sats`. No floating-point money anywhere; a regression
      test pins the plumbing.
- [x] `internal/rewards`: pure calculators over an abstract `ShareSource` —
      SOLO (full reward to the finder), PROP (per-round shares since the
      previous block), PPLNS (backward window of `factor × network
      difficulty`, classic semantics, proportional to counted work, graceful
      under-filled window for young pools). `distribute` floors per miner and
      hands remainder satoshis to the largest contributors: credits sum
      EXACTLY to the distributable amount, proven across amount/split
      matrices. Pool fee is floored in the miners' favor and recorded as a
      `pool-fee` audit row.
- [x] Confirmation tracking (`ChainView` over getblockcount/getblockhash):
      confirmations + progress toward `maturityDepth` (default 100), orphan
      declaration only once the chain is `orphanDepth` (default 12) past a
      mismatched height — shallow mismatches stay pending for reorg-back.
      Orphans get status `orphaned` and zero reward.
- [x] Migration 003: `blocks.{reward_sats,rewarded,confirmations}`,
      `balances` (poolid+miner, `amount_sats`), `balance_changes` audit trail.
- [x] Atomic crediting: one transaction with a `FOR UPDATE` lock on the block
      row and an in-transaction `rewarded` re-check — reprocessing can never
      double-credit (integration-tested).
- [x] `cmd/rewardd`: standalone daemon, one panic-isolated processor per pool
      (interval per pool via `rewardInterval`), `-once` for single passes,
      systemd unit in `deploy/`. Runs wherever the database lives.
- [x] Tests: fee math, distribution exactness, all three calculators
      (window/round boundaries, first-block, under-filled PPLNS), confirmation
      state machine, full processor pass over fakes, postgres integration for
      the share-source queries and atomic/idempotent crediting.
- [x] Live smoke: mined a real block through stratumd, ran `rewardd -once`
      against a confirming daemon — block confirmed at maturity, PPLNS
      credited 309,375,000 sats to the miner + 3,125,000 sats pool fee
      (= exactly the 312,500,000-sat coinbase), and a second run changed
      nothing.

Payouts (moving balances on-chain) belong to a future `payoutd`; Stage 8 is
hardening.

## Stage 8 — DONE: hardening

- [x] Per-session vardiff controller (`internal/stratum/vardiff/controller.go`):
      share-rate estimation per retarget window, ±variance dead band (default
      30%), per-step clamp (max 4x), port min/max bounds, idle decay toward
      MinDiff. Server retarget loop pushes `mining.set_difficulty`; a raise
      keeps an 8s grace window during which shares mined against the previous
      (lower) difficulty are still validated and credited at that difficulty
      (`Session.EffectiveDifficulty`). Verified live: an honest miner was
      walked 1e-9 → 4e-9 → 1.42e-8 with every share accepted throughout.
- [x] Per-IP banning (`internal/bans`): invalid-share ratio over a judged
      window (with clean-window reset so history cannot mask an attack),
      malformed-flood threshold, failed-auth threshold, time-limited bans with
      lazy expiry, bounded tracking maps. Enforced at accept (refused before
      any protocol work) and fed from the submit and codec paths. Verified
      live: malformed flood → banned → reconnect dropped → ban expired →
      service restored.
- [x] Metrics (`internal/metrics`): dependency-free Prometheus text registry
      (counters + sampled gauges) served at `GET /metrics` on the API:
      `pool_shares_total{pool,result}`, `pool_blocks_found_total{pool}`,
      `stratum_sessions`, `bans_active`, `spool_bytes`,
      `sharewriter_written_total` / `sharewriter_dropped_total`.
- [x] Region/node health: `GET /api/health` now includes per-pool lifecycle
      states alongside region, node id, session count, and database presence.
- [x] Shutdown polish: the vardiff loop joins the server WaitGroup and every
      Stage 6/7 component already stops in dependency order.
- [x] Tests: controller math (raise/lower/clamp/ceiling/idle-floor/variance
      band/window reset), ban thresholds + expiry + disabled mode + healthy
      miner never banned, metrics rendering (series identity, headers).

All eight stages are complete. Future work beyond this roadmap: payoutd
(moving credited balances on-chain), further coin families (RXD, SCASH, ALPH)
behind the existing `CoinAdapter`/`bitcoinbase` seams, and a standalone YAML
config front-end.

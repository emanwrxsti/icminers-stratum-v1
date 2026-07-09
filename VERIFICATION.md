# Verification snapshot

Push this tree as ONE commit, replacing the working tree wholesale. Do NOT
cherry-pick files: the earlier breakage was a packaging bug on my side — the
zip's exclude glob `*/spool/*` accidentally dropped the `internal/spool/`
SOURCE package, so `sharewal.go` (which calls DrainBatch) reached main without
`spool.go` (which defines it). Fixed by anchoring the exclude to the top-level
runtime data dir only.

## The WAL fix is a matched pair — both present here
- internal/spool/spool.go          DEFINES func (s *Spool) DrainBatch(...)  (line 154)
- internal/storage/postgres/sharewal.go  CALLS  w.sp.DrainBatch(...)

DrainBatch semantics: records in a batch are removed from the spool only after
fn(batch) returns nil; if fn returns an error, that batch and all later records
stay on disk for the next attempt.

Confirm before pushing:
    grep -n 'func (s \*Spool) DrainBatch' internal/spool/spool.go
    grep -n 'DrainBatch' internal/storage/postgres/sharewal.go
    go build ./...

## DrainBatch tests (internal/spool/spool_test.go)
- TestDrainBatchFullSuccess       — every batch succeeds, spool emptied, order preserved
- TestDrainBatchFirstBatchFailure — first batch fails; ALL records retained
- TestDrainBatchSecondBatchFailure— second batch fails; first committed, rest retained
- TestDrainBatchEmpty             — empty spool: callback never invoked

## Verified from a CLEAN extract of the shipped zip (Go 1.22, PostgreSQL 16)
- go build ./...        -> clean (exit 0)
- gofmt -l .            -> empty
- go vet ./...          -> clean
- go test ./...         -> 21/21 packages pass, 0 failures (uncached)
- go test -race ./...   -> clean
- stress suite (tag `stress`, under -race) -> pass:
    go test -tags stress -race -run TestStress ./internal/storage/postgres/

## CI
.github/workflows/go.yml present. Jobs: test (gofmt, vet, test, -race, build
all three binaries, Postgres+NATS services), smoke (mine->reward->payout),
stress (share-persistence outage/overflow under -race).

## Coins wired in stratumd/rewardd/payoutd
BTC (sha256d), LTC (scrypt), RXD (sha512/256d PoW+identity),
SCASH (RandomX; fails closed until a librandomx backend is wired).

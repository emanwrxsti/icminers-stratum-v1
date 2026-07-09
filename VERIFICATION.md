# Verification snapshot

This tree is a single coherent snapshot. Push it as ONE commit; do not
cherry-pick individual files (the previous breakage came from `sharewal.go`
reaching `main` without the matching `spool.go` change).

## The WAL fix travels as a pair
- `internal/spool/spool.go`        — DEFINES `func (s *Spool) DrainBatch(...)`
- `internal/storage/postgres/sharewal.go` — CALLS `w.sp.DrainBatch(...)`
Both are present here. Grep to confirm before pushing:
    grep -n 'func (s \*Spool) DrainBatch' internal/spool/spool.go
    grep -n 'DrainBatch' internal/storage/postgres/sharewal.go

## Verified locally (Go 1.22, PostgreSQL 16)
- go build ./...                    -> clean
- gofmt -l .                        -> empty (clean)
- go vet ./...                      -> clean
- go test ./...                     -> 21/21 packages pass (uncached)
- go test -race ./...               -> clean
- WAL regression tests pass:
    TestDrainBatchCommitsAndRetains, TestDrainBatchEmpty (spool)
    TestWALRetainsRecordsWhenInsertFails, TestWALPartialBatchFailureKeepsUncommitted (postgres)
- Stress suite (build tag `stress`) passes under -race:
    go test -tags stress -race -run TestStress ./internal/storage/postgres/

## CI
`.github/workflows/go.yml` present (jobs: test, smoke, stress).

## Coins wired in stratumd/rewardd/payoutd
BTC (sha256d), LTC (scrypt), RXD (sha512/256d PoW+identity),
SCASH (RandomX; fails closed until a librandomx backend is wired).

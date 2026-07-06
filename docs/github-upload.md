# GitHub upload checklist

## Before pushing

```bash
gofmt -w $(find . -name '*.go')
go test ./...
go vet ./...
go build -o bin/stratumd ./cmd/stratumd
```

## Create a repository

```bash
git init
git add .
git commit -m "Initial GoStratumPool stratum scaffold"
git branch -M main
git remote add origin git@github.com:icminers/gostratumpool.git
git push -u origin main
```

Change the remote URL if you create the repository under a different GitHub
account or organization.

## Do not commit secrets

Copy `configs/config.example.json` to `configs/config.json` for real nodes.
`configs/config.json` is ignored by `.gitignore` so RPC usernames/passwords do
not get pushed.

## Current milestone

This is Stage 1. It is a working Stratum V1 handshake and lifecycle scaffold,
not a complete mining pool yet. Share validation, real jobs, PostgreSQL, NATS,
API, rewards, and payouts are staged in `docs/roadmap.md`.

#!/usr/bin/env bash
# Run GoStratumPool in all-in-one mode for local development.
set -euo pipefail
cd "$(dirname "$0")/.."

CONFIG="${1:-configs/config.example.json}"

echo "Building stratumd..."
go build -o ./bin/stratumd ./cmd/stratumd

echo "Starting stratumd with config: ${CONFIG}"
exec ./bin/stratumd -config "${CONFIG}"

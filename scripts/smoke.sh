#!/usr/bin/env bash
# Integration smoke test: fake bitcoind -> stratumd -> miner submit -> block
# candidate -> DB block -> rewardd credit -> payoutd sendmany. Asserts exact
# satoshi conservation end to end. Used by CI and runnable locally.
#
# Requires: built binaries at ./bin/{stratumd,rewardd,payoutd} (or on PATH),
# python3, psql, and a PostgreSQL reachable via $SMOKE_PG_DSN.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE="$HERE/smoke"
WORK="${SMOKE_WORKDIR:-/tmp/gostratum-smoke}"
DSN="${SMOKE_PG_DSN:-postgres://pooltest:pooltest@127.0.0.1:5432/pooltest}"

BIN_STRATUMD="${BIN_STRATUMD:-./bin/stratumd}"
BIN_REWARDD="${BIN_REWARDD:-./bin/rewardd}"
BIN_PAYOUTD="${BIN_PAYOUTD:-./bin/payoutd}"

# psql connection args parsed from the DSN.
PSQL="psql $DSN -qtA"

rm -rf "$WORK"; mkdir -p "$WORK"
CONFIG="$WORK/config.json"
sed "s|REPLACED_BY_SCRIPT|$DSN|; s|/tmp/gostratum-smoke/|$WORK/|" "$SMOKE/config.json" > "$CONFIG"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

echo "== reset database =="
$PSQL -c "DROP TABLE IF EXISTS shares, blocks, balances, balance_changes, payments, schema_migrations CASCADE;" >/dev/null

echo "== start fake bitcoind =="
python3 "$SMOKE/fake_bitcoind.py" & pids+=($!)
sleep 0.5

echo "== start stratumd =="
"$BIN_STRATUMD" -config "$CONFIG" > "$WORK/stratumd.log" 2>&1 & pids+=($!)
sleep 1.5

echo "== run miner (submit share = block candidate) =="
python3 "$SMOKE/miner.py" || fail "miner did not complete cleanly"
sleep 1.5

echo "== assert block landed in DB =="
BLOCKS=$($PSQL -c "SELECT COUNT(*) FROM blocks WHERE poolid='btc-solo';")
[ "$BLOCKS" = "1" ] || fail "expected 1 block, got $BLOCKS"
REWARD=$($PSQL -c "SELECT reward_sats FROM blocks WHERE poolid='btc-solo';")
[ "$REWARD" = "312500000" ] || fail "block reward_sats = $REWARD, want 312500000"
echo "   block persisted with reward_sats=$REWARD"

# stop stratumd so the writer's final flush + WAL drain complete
kill "${pids[1]}" 2>/dev/null || true
sleep 1

echo "== rewardd: confirm + credit =="
"$BIN_REWARDD" -config "$CONFIG" -once > "$WORK/rewardd.log" 2>&1 || fail "rewardd failed"
grep -q "block CONFIRMED" "$WORK/rewardd.log" || fail "block not confirmed"
grep -q "block REWARDED" "$WORK/rewardd.log" || fail "block not rewarded"

CREDITED=$($PSQL -c "SELECT COALESCE(SUM(amount_sats),0) FROM balances WHERE poolid='btc-solo';")
FEE=$($PSQL -c "SELECT COALESCE(SUM(amount_sats),0) FROM balance_changes WHERE poolid='btc-solo' AND usage='pool-fee';")
echo "   credited=$CREDITED fee=$FEE"
[ "$CREDITED" = "309375000" ] || fail "credited = $CREDITED, want 309375000"
[ "$FEE" = "3125000" ] || fail "fee = $FEE, want 3125000"
# conservation: distributed + fee == block reward
[ $((CREDITED + FEE)) = "312500000" ] || fail "conservation broken: $CREDITED + $FEE != 312500000"

echo "== payoutd: send on-chain =="
"$BIN_PAYOUTD" -config "$CONFIG" -once > "$WORK/payoutd.log" 2>&1 || fail "payoutd failed"
grep -q "payout batch SENT" "$WORK/payoutd.log" || fail "payout not sent"

REMAINING=$($PSQL -c "SELECT COALESCE(SUM(amount_sats),0) FROM balances WHERE poolid='btc-solo';")
[ "$REMAINING" = "0" ] || fail "balances not zeroed after payout: $REMAINING"
SENT=$($PSQL -c "SELECT COUNT(*) FROM payments WHERE poolid='btc-solo' AND status='sent';")
[ "$SENT" = "1" ] || fail "expected 1 sent payment, got $SENT"

# the daemon must have seen exactly one sendmany with the exact amount string
SENDMANY=$(python3 -c "import urllib.request,json; print(json.dumps(json.loads(urllib.request.urlopen('http://127.0.0.1:18333/sendmany').read())))")
echo "   sendmany seen: $SENDMANY"
echo "$SENDMANY" | grep -q "3.09375000" || fail "sendmany missing exact amount 3.09375000"

echo "== payoutd second pass is a no-op =="
"$BIN_PAYOUTD" -config "$CONFIG" -once > "$WORK/payoutd2.log" 2>&1 || fail "second payoutd failed"
PAYMENTS=$($PSQL -c "SELECT COUNT(*) FROM payments WHERE poolid='btc-solo';")
[ "$PAYMENTS" = "1" ] || fail "second pass created extra payments: $PAYMENTS"

echo ""
echo "SMOKE PASSED: mine -> block(312500000) -> reward(309375000+3125000 fee) -> payout(3.09375000 on-chain), balances zeroed, idempotent."

#!/usr/bin/env bash
# bench/run_pgbench_tpcb.sh — populate a fresh motlite DB and run pgbench's
# built-in TPC-B-like transaction at a given concurrency. Designed for CI
# (returns non-zero on any catastrophic failure) and local benchmarking.
#
# Usage:
#   run_pgbench_tpcb.sh [--port N] [--db PATH] [--bin PATH]
#                      [--clients N] [--threads N] [--duration SEC]
#                      [--min-tps N]
#
# Defaults: port=5560, db=/tmp/motlite_tpcb.db, bin=build/debug/oro_server,
# clients=1, threads=1, duration=10s, min-tps=20 (very lax for CI).

set -euo pipefail

PORT=5560
DB=/tmp/motlite_tpcb.db
BIN=""
CLIENTS=1
THREADS=1
DURATION=10
MIN_TPS=20

while (($#)); do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --db) DB="$2"; shift 2 ;;
        --bin) BIN="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --threads) THREADS="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --min-tps) MIN_TPS="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$here/.." && pwd)"
runner="$repo_root/test/harness/oro_server_runner.sh"
[[ -z "$BIN" ]] && BIN="$repo_root/build/debug/oro_server"
LOG=/tmp/motlite_tpcb.log
PIDFILE=/tmp/motlite_tpcb.pid

if [[ ! -x "$BIN" ]]; then
    echo "FATAL: server binary not found at $BIN" >&2
    exit 2
fi
if ! command -v pgbench >/dev/null; then
    echo "FATAL: pgbench not installed (apt-get install postgresql-client)" >&2
    exit 2
fi

cleanup() {
    "$runner" stop --pidfile "$PIDFILE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$runner" start --port "$PORT" --db "$DB" --bin "$BIN" \
    --log "$LOG" --pidfile "$PIDFILE" >/dev/null
"$runner" wait-ready --port "$PORT" --timeout 15 --pidfile "$PIDFILE" >/dev/null

# Populate via the in-tree init script.
PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -At \
    -f "$here/tpcb_init.sql" >/dev/null

# Sanity check: 1000 accounts populated.
count="$(PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -At \
    -c 'SELECT count(*) FROM pgbench_accounts')"
if [[ "$count" != "1000" ]]; then
    echo "FATAL: pgbench_accounts has $count rows, expected 1000" >&2
    exit 1
fi

# Run pgbench using the in-tree custom script. -n skips its own init steps.
echo "== pgbench TPC-B: clients=$CLIENTS threads=$THREADS duration=${DURATION}s =="
out="$(PGPASSWORD=t pgbench -h localhost -p "$PORT" -U t -d t -n \
    -f "$here/tpcb.sql" -M extended \
    -c "$CLIENTS" -j "$THREADS" -T "$DURATION" 2>&1)" || true
echo "$out" | grep -E "tps|transactions actually|failed|latency average" || true

# Parse TPS. pgbench prints e.g. "tps = 165.713829 (without initial connection time)".
tps_line="$(echo "$out" | grep -E "^tps =" | tail -1 || true)"
tps_val="$(echo "$tps_line" | awk '{print $3}')"
if [[ -z "$tps_val" ]]; then
    echo "FATAL: could not parse TPS from pgbench output" >&2
    exit 1
fi

# Floating-point compare via awk.
awk -v got="$tps_val" -v min="$MIN_TPS" '
BEGIN { exit (got >= min) ? 0 : 1 }
'
status=$?
if [[ $status -ne 0 ]]; then
    echo "FAIL: tps=$tps_val below minimum $MIN_TPS" >&2
    exit 1
fi
echo "OK: tps=$tps_val >= min=$MIN_TPS"

# =====================================================================
# Correctness oracle: TPC-B's invariant is that every transaction applies
# `delta` to one accounts row, one tellers row, one branches row, AND
# inserts that delta into pgbench_history. So after the run, the four
# sums must all be equal. Drift implies a lost UPDATE, a partial commit,
# an MVCC visibility bug, or a backfill that missed a row.
# =====================================================================
echo "== correctness check =="
sums="$(PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -At -F '|' -c "
    SELECT
        (SELECT COALESCE(sum(abalance),0) FROM pgbench_accounts),
        (SELECT COALESCE(sum(tbalance),0) FROM pgbench_tellers),
        (SELECT COALESCE(sum(bbalance),0) FROM pgbench_branches),
        (SELECT COALESCE(sum(delta),0)    FROM pgbench_history),
        (SELECT count(*) FROM pgbench_history)
")"
IFS='|' read -r sum_a sum_t sum_b sum_h n_hist <<<"$sums"
echo "  sum(accounts.abalance) = $sum_a"
echo "  sum(tellers.tbalance)  = $sum_t"
echo "  sum(branches.bbalance) = $sum_b"
echo "  sum(history.delta)     = $sum_h"
echo "  count(history)         = $n_hist"

if [[ "$sum_a" != "$sum_t" || "$sum_t" != "$sum_b" || "$sum_b" != "$sum_h" ]]; then
    echo "FAIL: TPC-B invariant violated — the four sums must be equal" >&2
    exit 1
fi
# Also sanity-check that we actually did some work.
if [[ "$n_hist" == "0" ]]; then
    echo "FAIL: pgbench_history is empty — no transactions ran" >&2
    exit 1
fi
echo "OK: TPC-B invariant holds (a==t==b==h=$sum_a across $n_hist transactions)"

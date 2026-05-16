#!/usr/bin/env bash
# run_concurrency_check.sh — driver for the concurrency-hardening tests.
#
# Usage:
#   run_concurrency_check.sh <build_dir> <mode> [--threads N] [--duration SEC] [--port P]
#
# Spawns oro_server from <build_dir> (e.g. build/debug, build/asan, build/tsan),
# waits until it's accepting connections, runs harness_libpq_load in the
# requested mode against it, tears it down, and returns the harness exit code.
#
# Sanitizer-friendly: TSAN_OPTIONS/ASAN_OPTIONS in the parent env are inherited
# by the spawned server (both library options work through fork+exec).

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: $0 <build_dir> <mode> [--threads N] [--duration SEC] [--port P]" >&2
    exit 2
fi

BUILD_DIR="$1"; shift
MODE="$1"; shift

THREADS=16
DURATION=10
PORT=5500

while (($#)); do
    case "$1" in
        --threads) THREADS="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
runner="$repo_root/test/harness/oro_server_runner.sh"
server_bin="$repo_root/$BUILD_DIR/oro_server"
harness_bin="$repo_root/$BUILD_DIR/harness_libpq_load"
pidfile="/tmp/concur_${MODE}.pid"
logfile="/tmp/concur_${MODE}.log"
dbfile="/tmp/concur_${MODE}.db"

for p in "$server_bin" "$harness_bin"; do
    [[ -x "$p" ]] || { echo "missing binary: $p" >&2; exit 2; }
done

cleanup() { "$runner" stop --pidfile "$pidfile" >/dev/null 2>&1 || true; }
trap cleanup EXIT

"$runner" start --port "$PORT" --db "$dbfile" --bin "$server_bin" \
    --log "$logfile" --pidfile "$pidfile" >/dev/null
"$runner" wait-ready --port "$PORT" --timeout 15 --pidfile "$pidfile"

set +e
"$harness_bin" --port "$PORT" --mode "$MODE" \
    --threads "$THREADS" --duration "$DURATION"
rc=$?
set -e

"$runner" stop --pidfile "$pidfile" >/dev/null

# Surface any sanitizer reports the server logged to stderr.
if grep -qE "==[0-9]+==(ERROR|WARNING)|ThreadSanitizer|AddressSanitizer" "$logfile" 2>/dev/null; then
    echo "--- sanitizer report from server log: ---" >&2
    grep -E "ThreadSanitizer|AddressSanitizer|SUMMARY|==.*==" "$logfile" >&2 | head -50
    rc=1
fi
exit "$rc"

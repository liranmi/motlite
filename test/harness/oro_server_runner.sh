#!/usr/bin/env bash
# oro_server_runner.sh — spawn oro_server with a pidfile, capture logs.
#
# Usage:
#   oro_server_runner.sh start [--port N] [--db PATH] [--bin PATH] [--log PATH] [--pidfile PATH]
#   oro_server_runner.sh stop  [--pidfile PATH]
#   oro_server_runner.sh kill9 [--pidfile PATH]
#   oro_server_runner.sh wait-ready --port N [--timeout SECONDS]
#
# Defaults: port=5433, db=/tmp/motlite_harness.db,
#           bin=build/debug/oro_server (relative to repo root),
#           log=/tmp/motlite_harness.log, pidfile=/tmp/motlite_harness.pid.

set -euo pipefail

cmd="${1:-}"
shift || true

PORT=5433
DB=/tmp/motlite_harness.db
BIN=""
LOG=/tmp/motlite_harness.log
PIDFILE=/tmp/motlite_harness.pid
TIMEOUT=10
NO_CLEAN=0

while (($#)); do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --db) DB="$2"; shift 2 ;;
        --bin) BIN="$2"; shift 2 ;;
        --log) LOG="$2"; shift 2 ;;
        --pidfile) PIDFILE="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --no-clean) NO_CLEAN=1; shift ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$BIN" ]]; then
    here="$(cd "$(dirname "$0")" && pwd)"
    BIN="$here/../../build/debug/oro_server"
fi

case "$cmd" in
    start)
        if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
            echo "already running: pid=$(cat "$PIDFILE")" >&2
            exit 1
        fi
        if [[ "$NO_CLEAN" -eq 0 ]]; then
            rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" "$LOG"
        fi
        "$BIN" --port "$PORT" --db "$DB" >>"$LOG" 2>&1 &
        echo $! >"$PIDFILE"
        echo "started pid=$! port=$PORT db=$DB log=$LOG"
        ;;
    stop)
        if [[ ! -f "$PIDFILE" ]]; then exit 0; fi
        pid="$(cat "$PIDFILE")"
        kill -TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
            if ! kill -0 "$pid" 2>/dev/null; then break; fi
            sleep 0.2
        done
        rm -f "$PIDFILE"
        ;;
    kill9)
        if [[ ! -f "$PIDFILE" ]]; then exit 0; fi
        pid="$(cat "$PIDFILE")"
        kill -KILL "$pid" 2>/dev/null || true
        rm -f "$PIDFILE"
        ;;
    wait-ready)
        # Poll-connect with psql until the server accepts a session.
        for _ in $(seq 1 $((TIMEOUT * 10))); do
            if PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t \
                    -c "SELECT 1" >/dev/null 2>&1; then
                exit 0
            fi
            sleep 0.1
        done
        echo "server did not become ready within ${TIMEOUT}s" >&2
        exit 1
        ;;
    *)
        echo "usage: $0 {start|stop|kill9|wait-ready} [--port N] [--db PATH] ..." >&2
        exit 2
        ;;
esac

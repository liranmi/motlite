#!/usr/bin/env bash
# run_crash_loop.sh — kill-9 oracle for oro_server crash recovery (#7).
#
# Loops: start server → drive ack'd INSERTs from a writer client → kill -9
# at random ms → restart server → verify every ack'd row is present.
#
# A row is considered "ack'd" once the writer received "INSERT 0 1" for it,
# which means psql got a CommandComplete back from oro_server, which means
# the SQLite txn has fsync'd through the MOT WAL. The expectation is then:
# after restart, every ack'd id must be visible.
#
# Usage:
#   run_crash_loop.sh [ITERATIONS] [--port N] [--db PATH] [--bin PATH]
#
# Defaults: ITERATIONS=5, port=5450, db=/tmp/oro_crash.db,
#           bin=build/debug/oro_server (relative to repo root).
#
# Exit 0 if every iteration agrees with the oracle, 1 otherwise.

set -euo pipefail

iters="${1:-5}"
shift || true

PORT=5450
DB=/tmp/oro_crash.db
BIN=""
ACK=/tmp/oro_crash.ack
LOG=/tmp/oro_crash.server.log
PIDFILE=/tmp/oro_crash.pid

while (($#)); do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --db) DB="$2"; shift 2 ;;
        --bin) BIN="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

here="$(cd "$(dirname "$0")" && pwd)"
runner="$here/oro_server_runner.sh"
[[ -z "$BIN" ]] && BIN="$here/../../build/debug/oro_server"

cleanup() {
    "$runner" kill9 --pidfile "$PIDFILE" >/dev/null 2>&1 || true
    [[ -n "${WRITER_PID:-}" ]] && kill -KILL "$WRITER_PID" 2>/dev/null || true
}
trap cleanup EXIT

fail=0
for iter in $(seq 1 "$iters"); do
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-journal" "$ACK" "$LOG"
    "$runner" start --port "$PORT" --db "$DB" --bin "$BIN" \
        --log "$LOG" --pidfile "$PIDFILE" >/dev/null
    "$runner" wait-ready --port "$PORT" --timeout 10 >/dev/null

    # Create the table.
    PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -q -c \
        "CREATE MOT TABLE crash_t (id INT PRIMARY KEY, v TEXT)" >/dev/null

    # Writer: insert 1..200, echo each id to $ACK ONLY after psql confirms it.
    (
        for i in $(seq 1 200); do
            if PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -q -c \
                "INSERT INTO crash_t VALUES ($i, 'v$i')" >/dev/null 2>&1; then
                echo "$i" >>"$ACK"
            else
                exit 0
            fi
        done
    ) &
    WRITER_PID=$!

    # Random delay 50–500 ms then kill -9.
    delay_ms=$((50 + RANDOM % 451))
    sleep_arg=$(awk -v ms="$delay_ms" 'BEGIN{printf "%.3f", ms/1000.0}')
    sleep "$sleep_arg"
    "$runner" kill9 --pidfile "$PIDFILE" >/dev/null

    # Stop the writer (its connection is dead anyway).
    pkill -KILL -P "$WRITER_PID" 2>/dev/null || true
    kill -KILL "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
    WRITER_PID=""
    sleep 0.2  # let SQLite WAL files settle before reopen

    # Restart on the same DB file (do NOT wipe it — recovery is what we test).
    "$runner" start --port "$PORT" --db "$DB" --bin "$BIN" \
        --log "$LOG" --pidfile "$PIDFILE" --no-clean >/dev/null
    "$runner" wait-ready --port "$PORT" --timeout 10 >/dev/null

    expected="$(sort -n "$ACK" 2>/dev/null | wc -l || echo 0)"
    got="$(PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -At -c \
        "SELECT count(*) FROM crash_t" 2>>"$LOG" || echo NA)"
    if [[ "$expected" -gt 0 ]]; then
        values="$(awk 'NR>0{printf "(%s)%s",$0,(NR<n?",":"")}' n=$(wc -l <"$ACK") "$ACK")"
        missing="$(PGPASSWORD=t psql -h localhost -p "$PORT" -U t -d t -At -c "
            WITH ack(id) AS (VALUES $values)
            SELECT count(*) FROM ack WHERE id NOT IN (SELECT id FROM crash_t)" \
            2>>"$LOG" || echo NA)"
    else
        missing=0
    fi

    "$runner" stop --pidfile "$PIDFILE" >/dev/null

    echo "[crash $iter/$iters] delay=${delay_ms}ms ack=${expected} got=${got} missing=${missing}"
    if [[ "$got" != "$expected" || "$missing" != "0" ]]; then
        if [[ "$expected" == "0" && "$got" == "0" ]]; then
            : # writer didn't ack anything before kill — vacuously OK.
        else
            fail=$((fail + 1))
        fi
    fi
done

echo "---"
echo "[crash] iterations=$iters failed=$fail"
exit $((fail > 0 ? 1 : 0))

#!/usr/bin/env bash
# run_copy_check.sh — end-to-end check of the COPY protocol (text + CSV) against
# a live oro_server, driven by psql. Verifies COPY FROM STDIN, COPY TO STDOUT,
# CSV options (FORMAT/HEADER), column lists, the COPY (query) form, NULL
# handling, and embedded commas/quotes.
#
# Usage: run_copy_check.sh [BUILD_DIR]   (default: build/debug)

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
BUILD_DIR="${1:-build/debug}"
BIN="$root/$BUILD_DIR/oro_server"
RUNNER="$here/oro_server_runner.sh"

PORT=$(( (RANDOM % 2000) + 6000 ))
DB="/tmp/motlite_copy_$$.db"
PIDFILE="/tmp/motlite_copy_$$.pid"
LOG="/tmp/motlite_copy_$$.log"
OUT="/tmp/motlite_copy_$$.out"

cleanup() {
    "$RUNNER" stop --pidfile "$PIDFILE" >/dev/null 2>&1 || true
    rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" "$PIDFILE" "$LOG" "$OUT"
}
trap cleanup EXIT

if [[ ! -x "$BIN" ]]; then
    echo "[copy] missing binary: $BIN" >&2
    exit 2
fi

"$RUNNER" start --port "$PORT" --db "$DB" --bin "$BIN" \
    --pidfile "$PIDFILE" --log "$LOG" >/dev/null
"$RUNNER" wait-ready --port "$PORT" --timeout 15

PSQL=(psql "host=127.0.0.1 port=$PORT user=t dbname=t sslmode=disable" -q -t -A -v ON_ERROR_STOP=1)

"${PSQL[@]}" >"$OUT" 2>&1 <<'SQL'
CREATE MOT TABLE c (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);

-- text COPY FROM with a NULL (\N)
COPY c FROM STDIN;
1	alice	100
2	bob	90
3	carol	\N
\.

-- CSV COPY FROM with embedded comma, doubled quote, and unquoted-empty NULL
CREATE MOT TABLE d (id INTEGER PRIMARY KEY, txt TEXT);
COPY d FROM STDIN WITH (FORMAT csv);
10,"hello, world"
11,"quote ""x"" inside"
12,
\.

-- CSV COPY FROM with HEADER (first line skipped) and explicit column list
CREATE MOT TABLE f (a INTEGER PRIMARY KEY, b TEXT, c INTEGER);
COPY f (a, c) FROM STDIN CSV HEADER;
a,c
7,700
8,800
\.

-- Assertions, one boolean per line.
SELECT 'A=' || (count(*)=3 AND sum(score)=190) FROM c;            -- 3 rows, NULL score skipped in sum
SELECT 'B=' || (score IS NULL) FROM c WHERE id=3;                 -- carol's score is NULL
SELECT 'C=' || (txt='hello, world') FROM d WHERE id=10;           -- CSV embedded comma
SELECT 'D=' || (txt='quote "x" inside') FROM d WHERE id=11;       -- CSV doubled quote
SELECT 'E=' || (txt IS NULL) FROM d WHERE id=12;                  -- CSV unquoted empty = NULL
SELECT 'F=' || (count(*)=2 AND b IS NULL AND sum(c)=1500) FROM f; -- header skipped, b default NULL
SQL

echo "--- COPY TO STDOUT (text) ---"           >>"$OUT"
"${PSQL[@]}" -c "COPY c TO STDOUT" >>"$OUT" 2>&1
echo "--- COPY (query) TO STDOUT CSV ---"       >>"$OUT"
"${PSQL[@]}" -c "COPY (SELECT id,txt FROM d ORDER BY id) TO STDOUT WITH (FORMAT csv)" >>"$OUT" 2>&1

fail=0
check() {  # check <label> <expected-substring>
    if grep -qF "$1" "$OUT"; then
        echo "  ok: $1"
    else
        echo "  FAIL: expected '$1'"; fail=1
    fi
}

check "A=1"
check "B=1"
check "C=1"
check "D=1"
check "E=1"
check "F=1"
# COPY TO text: carol's NULL renders as \N, embedded comma stays literal (tab-delim)
check $'3\tcarol\t\\N'
# COPY (query) TO CSV: embedded comma forces quoting
check '10,"hello, world"'

if [[ "$fail" -ne 0 ]]; then
    echo "[copy] FAILED"
    echo "----- psql output -----"; cat "$OUT"
    echo "----- server log -----"; cat "$LOG"
    exit 1
fi

echo "[copy] all COPY checks passed (text + CSV, FROM + TO)"

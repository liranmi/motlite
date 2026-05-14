#!/usr/bin/env bash
# run_under_debug.sh — wrap oro_server (or any binary) under gdb or valgrind.
#
# Usage:
#   run_under_debug.sh gdb     <binary> [args...]   # batch backtrace on crash
#   run_under_debug.sh valgrind <binary> [args...]  # memcheck, error-exit 1
set -euo pipefail
mode="${1:-}"; shift || true
case "$mode" in
    gdb)
        exec gdb --batch \
            -ex "handle SIGPIPE nostop" \
            -ex "run" \
            -ex "bt full" \
            --args "$@"
        ;;
    valgrind)
        exec valgrind --error-exitcode=1 --track-origins=yes \
            --leak-check=no --show-leak-kinds=none "$@"
        ;;
    *)
        echo "usage: $0 {gdb|valgrind} <binary> [args...]" >&2
        exit 2
        ;;
esac

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT/harness/loop/state"
mkdir -p "$STATE_DIR"

STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
LOG="$STATE_DIR/tcl-breadth-overnight-$STAMP.log"
PID_FILE="$STATE_DIR/tcl-breadth-overnight-$STAMP.pid"

cd "$ROOT"
nohup bash harness/run-tcl-breadth-overnight.sh "$@" >"$LOG" 2>&1 &
PID="$!"
printf '%s\n' "$PID" >"$PID_FILE"

printf 'launched redis tcl breadth overnight loop\n'
printf 'pid: %s\n' "$PID"
printf 'log: %s\n' "$LOG"
printf 'pid_file: %s\n' "$PID_FILE"
printf '\nwatch:\n'
printf '  bash harness/watch-tcl-breadth.sh\n'
printf '  tail -f %q\n' "$LOG"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_HARNESS="${PORT_HARNESS:-$(dirname "$ROOT")/port-harness}"

exec python3 "$PORT_HARNESS/loop/watch.py" \
  --project "$ROOT" \
  --follow \
  --interval "${TCL_BREADTH_WATCH_INTERVAL:-5}" \
  "$@"

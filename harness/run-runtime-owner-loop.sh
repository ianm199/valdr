#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_HARNESS="${PORT_HARNESS:-$(dirname "$ROOT")/port-harness}"

exec python3 "$PORT_HARNESS/loop/run-loop.py" \
  --project "$ROOT" \
  --selector "${RUNTIME_OWNER_SELECTOR:-auto}" \
  --auto-dispatch \
  --dispatch-runtime "${RUNTIME_OWNER_RUNTIME:-claude}" \
  --dispatch-budget-usd "${RUNTIME_OWNER_BUDGET_USD:-35}" \
  --dispatch-timeout-s "${RUNTIME_OWNER_TIMEOUT_S:-3600}" \
  --dispatch-model "${RUNTIME_OWNER_MODEL:-opus}" \
  --max-iterations "${RUNTIME_OWNER_MAX_ITERATIONS:-10}" \
  --max-failures "${RUNTIME_OWNER_MAX_FAILURES:-2}" \
  "$@"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_HARNESS="${PORT_HARNESS:-$(dirname "$ROOT")/port-harness}"

MODEL_ARGS=()
if [[ -n "${RUNTIME_OWNER_MODEL:-}" ]]; then
  MODEL_ARGS=(--dispatch-model "$RUNTIME_OWNER_MODEL")
fi

exec python3 "$PORT_HARNESS/loop/run-loop.py" \
  --project "$ROOT" \
  --selector "${RUNTIME_OWNER_SELECTOR:-auto}" \
  --auto-dispatch \
  --dispatch-runtime "${RUNTIME_OWNER_RUNTIME:-codex}" \
  --dispatch-budget-usd "${RUNTIME_OWNER_BUDGET_USD:-35}" \
  --dispatch-timeout-s "${RUNTIME_OWNER_TIMEOUT_S:-5400}" \
  "${MODEL_ARGS[@]}" \
  --dispatch-sandbox "${RUNTIME_OWNER_SANDBOX:-danger-full-access}" \
  --dispatch-approval "${RUNTIME_OWNER_APPROVAL:-never}" \
  --max-iterations "${RUNTIME_OWNER_MAX_ITERATIONS:-24}" \
  --max-failures "${RUNTIME_OWNER_MAX_FAILURES:-3}" \
  --max-same-packet-failures "${RUNTIME_OWNER_MAX_SAME_PACKET_FAILURES:-2}" \
  "$@"

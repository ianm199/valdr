#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_HARNESS="${PORT_HARNESS:-$(dirname "$ROOT")/port-harness}"

MODEL_ARGS=()
if [[ -n "${TCL_BREADTH_MODEL:-}" ]]; then
  MODEL_ARGS=(--dispatch-model "$TCL_BREADTH_MODEL")
fi

RESET_ARGS=()
if [[ "${TCL_BREADTH_RESET:-1}" != "0" ]]; then
  RESET_ARGS=(--reset)
fi

CMD=(python3 "$PORT_HARNESS/loop/run-loop.py" \
  --project "$ROOT" \
  --selector "${TCL_BREADTH_SELECTOR:-nightly}" \
  --auto-dispatch \
  --dispatch-runtime "${TCL_BREADTH_RUNTIME:-codex}" \
  --dispatch-timeout-s "${TCL_BREADTH_TIMEOUT_S:-2400}" \
  --dispatch-sandbox "${TCL_BREADTH_SANDBOX:-danger-full-access}" \
  --dispatch-approval "${TCL_BREADTH_APPROVAL:-never}" \
  --max-iterations "${TCL_BREADTH_MAX_ITERATIONS:-18}" \
  --max-failures "${TCL_BREADTH_MAX_FAILURES:-4}" \
  --max-same-packet-failures "${TCL_BREADTH_MAX_SAME_PACKET_FAILURES:-2}")

if ((${#MODEL_ARGS[@]} > 0)); then
  CMD+=("${MODEL_ARGS[@]}")
fi

if [[ "${TCL_BREADTH_RUNTIME:-codex}" == "claude" ]]; then
  CMD+=(--dispatch-budget-usd "${TCL_BREADTH_BUDGET_USD:-12}")
fi

if ((${#RESET_ARGS[@]} > 0)); then
  CMD+=("${RESET_ARGS[@]}")
fi
CMD+=("$@")

printf 'Running TCL breadth overnight loop:\n'
printf '  %q' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"

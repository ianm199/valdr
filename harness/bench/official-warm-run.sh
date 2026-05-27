#!/usr/bin/env bash
# Run the warmed performance packet used for release-facing result tables.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="${ROOT}/harness/bench/results"
mkdir -p "$RESULTS_DIR"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
COMMIT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
LOG="${RESULTS_DIR}/${STAMP}-${COMMIT}-official-warm-run.log"
MD="${RESULTS_DIR}/${STAMP}-${COMMIT}-official-warm-results.md"

WARMUP_REQUESTS="${WARMUP_REQUESTS:-1000}"
WARMUP_CLIENTS="${WARMUP_CLIENTS:-1}"
WARMUP_PIPELINE="${WARMUP_PIPELINE:-1}"
WARMUP_COMMAND="${WARMUP_COMMAND:-ping_mbulk}"

DEFAULT_REQUESTS="${DEFAULT_REQUESTS:-100000}"
DEFAULT_CLIENTS="${DEFAULT_CLIENTS:-50}"
DEFAULT_PIPELINE="${DEFAULT_PIPELINE:-100}"
DEFAULT_PAYLOAD="${DEFAULT_PAYLOAD:-64}"
DEFAULT_TIMEOUT_S="${DEFAULT_TIMEOUT_S:-60}"

PIPELINE_COMMANDS="${PIPELINE_COMMANDS:-get,ping_mbulk,set}"
PIPELINE_DEPTHS="${PIPELINE_DEPTHS:-1,16,100}"
PIPELINE_P1_REQUESTS="${PIPELINE_P1_REQUESTS:-100000}"
PIPELINE_REQUESTS_PIPELINED="${PIPELINE_REQUESTS_PIPELINED:-1000000}"
PIPELINE_CLIENTS="${PIPELINE_CLIENTS:-50}"
PIPELINE_PAYLOAD="${PIPELINE_PAYLOAD:-64}"
PIPELINE_TIMEOUT_S="${PIPELINE_TIMEOUT_S:-60}"

JSON_REQUESTS="${JSON_REQUESTS:-50000}"
JSON_CLIENTS="${JSON_CLIENTS:-50}"
JSON_PIPELINE="${JSON_PIPELINE:-1}"
JSON_KEYSPACE="${JSON_KEYSPACE:-5000}"
JSON_DOC_BYTES="${JSON_DOC_BYTES:-4096}"

run_logged() {
    echo "==> $*" | tee -a "$LOG"
    if ! "$@" >>"$LOG" 2>&1; then
        echo "ERROR: command failed; tail of $LOG:" >&2
        tail -80 "$LOG" >&2
        exit 1
    fi
}

latest_json() {
    local probe_id=$1
    ls -t "${RESULTS_DIR}"/*-"${probe_id}".json | head -1
}

{
    echo "# valkey-rs official warmed benchmark run"
    echo "# timestamp_utc: ${STAMP}"
    echo "# commit: ${COMMIT}"
    echo "# warmup: ${WARMUP_REQUESTS} ${WARMUP_COMMAND} request(s), clients=${WARMUP_CLIENTS}, pipeline=${WARMUP_PIPELINE}"
    echo
} >"$LOG"

cd "$ROOT"
run_logged cargo build --release -p redis-server
export VALKEY_BENCH_SKIP_BUILD=1

run_logged python3 harness/bench/pipeline-smoke.py \
    --commands "$PIPELINE_COMMANDS" \
    --pipelines "$PIPELINE_DEPTHS" \
    --requests-p1 "$PIPELINE_P1_REQUESTS" \
    --requests-pipelined "$PIPELINE_REQUESTS_PIPELINED" \
    --clients "$PIPELINE_CLIENTS" \
    --payload "$PIPELINE_PAYLOAD" \
    --timeout-s "$PIPELINE_TIMEOUT_S" \
    --warmup-requests "$WARMUP_REQUESTS" \
    --warmup-clients "$WARMUP_CLIENTS" \
    --warmup-pipeline "$WARMUP_PIPELINE" \
    --warmup-command "$WARMUP_COMMAND"

run_logged python3 harness/bench/default-suite-parts.py run \
    --mode ordered \
    --target both \
    --tests all \
    --requests "$DEFAULT_REQUESTS" \
    --clients "$DEFAULT_CLIENTS" \
    --pipeline "$DEFAULT_PIPELINE" \
    --payload "$DEFAULT_PAYLOAD" \
    --timeout-s "$DEFAULT_TIMEOUT_S" \
    --warmup-requests "$WARMUP_REQUESTS" \
    --warmup-clients "$WARMUP_CLIENTS" \
    --warmup-pipeline "$WARMUP_PIPELINE" \
    --warmup-command "$WARMUP_COMMAND"

run_logged python3 harness/bench/json-doc-mix.py \
    --requests "$JSON_REQUESTS" \
    --clients "$JSON_CLIENTS" \
    --pipeline "$JSON_PIPELINE" \
    --keyspace "$JSON_KEYSPACE" \
    --doc-bytes "$JSON_DOC_BYTES" \
    --no-build

PIPELINE_JSON="$(latest_json pipeline-smoke)"
DEFAULT_JSON="$(latest_json default-suite-parts)"
JSON_MIX_JSON="$(latest_json json-doc-mix)"

python3 harness/bench/format-results.py "$DEFAULT_JSON" "$PIPELINE_JSON" "$JSON_MIX_JSON" >"$MD"

echo "==> log: ${LOG}"
echo "==> markdown: ${MD}"
echo "==> source artifacts:"
echo "    ${DEFAULT_JSON}"
echo "    ${PIPELINE_JSON}"
echo "    ${JSON_MIX_JSON}"

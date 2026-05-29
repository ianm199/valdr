#!/usr/bin/env bash
#
# Canonical performance benchmark entry point.
#
# This is the ONE way to run a benchmark. There is intentionally no
# alternative wrapper. Single-command runs, narrow probes, and bench-client
# swaps all go through this script via environment-variable overrides
# (documented below).
#
# Pinned reference Valkey lives at reference/valkey; the script runs whatever
# is currently built there as the adversary. The Rust target is built once
# at the top of the run.
#
# Common invocations
# ──────────────────
#
#   bash harness/bench/official-warm-run.sh
#       Full release-facing packet (pipeline-smoke + default-suite + JSON
#       cache mix). ~90 s on M3 Max.
#
#   TESTS=mget PIPELINE_COMMANDS=mget SKIP_JSON=1 \
#       bash harness/bench/official-warm-run.sh
#       Narrow run: only the MGET command in the default-suite, only the
#       MGET workload in pipeline-smoke, skip the JSON mix. ~5 s.
#
#   BENCH_BIN=/path/to/newer/valkey-benchmark \
#       bash harness/bench/official-warm-run.sh
#       Override the bench-client binary while keeping reference/valkey/'s
#       server as the adversary. Required when the pinned reference's
#       valkey-benchmark doesn't recognize a test target — e.g. Valkey 8.1.7
#       ships a benchmark that doesn't know -t mget; pair this with a 9.x
#       valkey-benchmark to fill that gap.
#
# Env overrides (all optional; defaults preserve the release-grade packet)
# ──────────────
#   TESTS                    default-suite-parts.py --tests (default "all")
#   PIPELINE_COMMANDS        pipeline-smoke.py --commands (default GET/PING/SET)
#   BENCH_BIN                path to valkey-benchmark; empty = use reference/
#   SKIP_PIPELINE_SMOKE=1    skip the pipeline-smoke probe
#   SKIP_DEFAULT_SUITE=1     skip the default-suite probe
#   SKIP_JSON=1              skip the JSON-doc-mix probe
#   (request counts / clients / pipeline depths / payload / timeout knobs:
#    see the DEFAULT_*, PIPELINE_*, JSON_* and WARMUP_* env vars below.)
#
# Output
# ──────
#   harness/bench/results/<stamp>-<commit>-official-warm-run.log
#   harness/bench/results/<stamp>-<commit>-official-warm-results.md
#

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

# Narrowing knobs (default-suite + pipeline-smoke test selection,
# bench-client override, per-probe skips). All optional.
TESTS="${TESTS:-all}"
BENCH_BIN="${BENCH_BIN:-}"
SKIP_PIPELINE_SMOKE="${SKIP_PIPELINE_SMOKE:-0}"
SKIP_DEFAULT_SUITE="${SKIP_DEFAULT_SUITE:-0}"
SKIP_JSON="${SKIP_JSON:-0}"

# Build the optional --benchmark-bin argument once; reused per probe.
BENCH_BIN_ARG=()
if [[ -n "${BENCH_BIN}" ]]; then
    if [[ ! -x "${BENCH_BIN}" ]]; then
        echo "ERROR: BENCH_BIN does not point at an executable: ${BENCH_BIN}" >&2
        exit 2
    fi
    BENCH_BIN_ARG=(--benchmark-bin "${BENCH_BIN}")
fi

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

if [[ "${SKIP_PIPELINE_SMOKE}" != "1" ]]; then
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
        --warmup-command "$WARMUP_COMMAND" \
        "${BENCH_BIN_ARG[@]}"
fi

if [[ "${SKIP_DEFAULT_SUITE}" != "1" ]]; then
    run_logged python3 harness/bench/default-suite-parts.py run \
        --mode ordered \
        --target both \
        --tests "$TESTS" \
        --requests "$DEFAULT_REQUESTS" \
        --clients "$DEFAULT_CLIENTS" \
        --pipeline "$DEFAULT_PIPELINE" \
        --payload "$DEFAULT_PAYLOAD" \
        --timeout-s "$DEFAULT_TIMEOUT_S" \
        --warmup-requests "$WARMUP_REQUESTS" \
        --warmup-clients "$WARMUP_CLIENTS" \
        --warmup-pipeline "$WARMUP_PIPELINE" \
        --warmup-command "$WARMUP_COMMAND" \
        "${BENCH_BIN_ARG[@]}"
fi

if [[ "${SKIP_JSON}" != "1" ]]; then
    run_logged python3 harness/bench/json-doc-mix.py \
        --requests "$JSON_REQUESTS" \
        --clients "$JSON_CLIENTS" \
        --pipeline "$JSON_PIPELINE" \
        --keyspace "$JSON_KEYSPACE" \
        --doc-bytes "$JSON_DOC_BYTES" \
        --no-build
fi

FORMAT_INPUTS=()
[[ "${SKIP_DEFAULT_SUITE}" != "1" ]] && FORMAT_INPUTS+=("$(latest_json default-suite-parts)")
[[ "${SKIP_PIPELINE_SMOKE}" != "1" ]] && FORMAT_INPUTS+=("$(latest_json pipeline-smoke)")
[[ "${SKIP_JSON}" != "1" ]] && FORMAT_INPUTS+=("$(latest_json json-doc-mix)")

python3 harness/bench/format-results.py "${FORMAT_INPUTS[@]}" >"$MD"

echo "==> log: ${LOG}"
echo "==> markdown: ${MD}"
echo "==> source artifacts:"
for art in "${FORMAT_INPUTS[@]}"; do
    echo "    ${art}"
done

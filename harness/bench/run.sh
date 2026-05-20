#!/usr/bin/env bash
# harness/bench/run.sh — side-by-side throughput benchmark between
# upstream Valkey and valkey-rs.
#
# Uses the official `valkey-benchmark` from `reference/valkey/src/` so the
# numbers are immediately legible to anyone in the Redis/Valkey ecosystem.
#
# Both servers are run on the same host, on different ports, sequentially
# (not in parallel) to keep CPU contention out of the picture.
#
# Usage:
#   bash harness/bench/run.sh                       # default workload
#   bash harness/bench/run.sh --requests 100000     # smaller run (smoke)
#   bash harness/bench/run.sh --pipeline 1          # no pipelining
#   bash harness/bench/run.sh --tests set,get,incr  # subset of commands
#
# Output:
#   harness/bench/results/<UTC-timestamp>-<short-sha>.tsv
#
# Reproducibility note: record CPU + OS + commit hash in the TSV header so
# results from different machines are not silently merged.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VALKEY_BIN="${ROOT}/reference/valkey/src/valkey-server"
VALKEY_BENCH="${ROOT}/reference/valkey/src/valkey-benchmark"
RUST_BIN="${ROOT}/target/release/redis-server"

# ── flags ────────────────────────────────────────────────────────────────

REQUESTS=1000000
CLIENTS=50
PIPELINE=100
PAYLOAD=64
TESTS="set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,lrange_100,lrange_300,mset,ping_mbulk"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests)  REQUESTS=$2;  shift 2;;
        --clients)   CLIENTS=$2;   shift 2;;
        --pipeline)  PIPELINE=$2;  shift 2;;
        --payload)   PAYLOAD=$2;   shift 2;;
        --tests)     TESTS=$2;     shift 2;;
        -h|--help)
            sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^# //; s/^#//'
            exit 0;;
        *) echo "unknown flag: $1" >&2; exit 1;;
    esac
done

# ── sanity ───────────────────────────────────────────────────────────────

[[ -x "$VALKEY_BIN" ]]   || { echo "ERROR: missing $VALKEY_BIN. Run scripts/setup-reference.sh first." >&2; exit 1; }
[[ -x "$VALKEY_BENCH" ]] || { echo "ERROR: missing $VALKEY_BENCH. Run scripts/setup-reference.sh first." >&2; exit 1; }
[[ -x "$RUST_BIN" ]]     || { echo "ERROR: missing $RUST_BIN. Run 'cargo build --release' first." >&2; exit 1; }

# ── result file ──────────────────────────────────────────────────────────

TS=$(date -u +%Y%m%dT%H%M%SZ)
COMMIT=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
OUTDIR="${ROOT}/harness/bench/results"
mkdir -p "$OUTDIR"
TSV="${OUTDIR}/${TS}-${COMMIT}.tsv"

# Hardware / OS fingerprint
OS_NAME="$(uname -sr)"
ARCH="$(uname -m)"
CPU="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || \
       grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || \
       echo 'unknown')"

# ── server lifecycle ─────────────────────────────────────────────────────

C_PORT=16379
RUST_PORT=16390
C_PID=""
RUST_PID=""

cleanup() {
    [[ -n "$C_PID"    ]] && kill "$C_PID"    2>/dev/null || true
    [[ -n "$RUST_PID" ]] && kill "$RUST_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
    local port=$1
    for _ in $(seq 1 50); do
        nc -z 127.0.0.1 "$port" 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

run_one() {
    local label=$1
    local port=$2
    echo "==> benchmarking $label on port $port" >&2
    "$VALKEY_BENCH" -h 127.0.0.1 -p "$port" \
        -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$PAYLOAD" \
        -t "$TESTS" --csv 2>/dev/null
}

# ── benchmark upstream Valkey ────────────────────────────────────────────

echo "==> starting upstream valkey-server on $C_PORT" >&2
"$VALKEY_BIN" --port "$C_PORT" --bind 127.0.0.1 \
    --save "" --appendonly no --daemonize no --loglevel warning \
    >/tmp/bench-c.log 2>&1 &
C_PID=$!
wait_for_port "$C_PORT" || { echo "ERROR: upstream did not come up" >&2; exit 1; }
C_CSV=$(run_one "upstream-valkey" "$C_PORT")
kill "$C_PID" 2>/dev/null || true; wait "$C_PID" 2>/dev/null || true; C_PID=""

# ── benchmark valkey-rs ──────────────────────────────────────────────────

echo "==> starting valkey-rs on $RUST_PORT" >&2
"$RUST_BIN" --port "$RUST_PORT" --bind 127.0.0.1 --rdb-disabled \
    >/tmp/bench-rust.log 2>&1 &
RUST_PID=$!
wait_for_port "$RUST_PORT" || { echo "ERROR: valkey-rs did not come up" >&2; exit 1; }
RUST_CSV=$(run_one "valkey-rs" "$RUST_PORT")
kill "$RUST_PID" 2>/dev/null || true; wait "$RUST_PID" 2>/dev/null || true; RUST_PID=""

# ── emit TSV ─────────────────────────────────────────────────────────────

{
    echo "# valkey-rs benchmark"
    echo "# timestamp_utc: ${TS}"
    echo "# commit:        ${COMMIT}"
    echo "# os:            ${OS_NAME}"
    echo "# arch:          ${ARCH}"
    echo "# cpu:           ${CPU}"
    echo "# requests:      ${REQUESTS}"
    echo "# clients:       ${CLIENTS}"
    echo "# pipeline:      ${PIPELINE}"
    echo "# payload_bytes: ${PAYLOAD}"
    echo
    printf "test\tupstream_rps\tvalkey_rs_rps\tratio\tupstream_p99_ms\tvalkey_rs_p99_ms\n"

    # valkey-benchmark --csv emits:
    #   "test","rps","avg_lat_ms","min_lat_ms","p50_lat_ms","p95_lat_ms","p99_lat_ms","max_lat_ms"
    paste <(echo "$C_CSV") <(echo "$RUST_CSV") | \
    awk -F'\t' 'NR > 1 {
        n=split($1, c, ","); m=split($2, r, ",")
        if (n < 7 || m < 7) next
        cmd=c[1]; gsub(/"/, "", cmd)
        cps=c[2]; rps=r[2]; gsub(/"/, "", cps); gsub(/"/, "", rps)
        cp99=c[7]; rp99=r[7]; gsub(/"/, "", cp99); gsub(/"/, "", rp99)
        ratio = (cps+0 > 0) ? (rps+0)/(cps+0) : 0
        printf "%s\t%s\t%s\t%.2f\t%s\t%s\n", cmd, cps, rps, ratio, cp99, rp99
    }'
} > "$TSV"

echo "==> results: $TSV" >&2
echo "" >&2
cat "$TSV"

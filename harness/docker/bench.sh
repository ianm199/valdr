#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-ghcr.io/ianm199/valkey-rs:alpha}"
BENCH_IMAGE="${BENCH_IMAGE:-redis:7-alpine}"
REQUESTS="${REQUESTS:-100000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-16}"
PAYLOAD="${PAYLOAD:-64}"
TESTS="${TESTS:-ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,lrange_100,lrange_300}"
PULL="${PULL:-1}"
CSV="${CSV:-0}"
OUTPUT="${OUTPUT:-}"

RUN_ID="valkey-rs-bench-$$"
NETWORK="$RUN_ID"
SERVER="$RUN_ID-server"

cleanup() {
  docker rm -f "$SERVER" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "$PULL" != "0" ]]; then
  docker pull "$IMAGE" >/dev/null
  docker pull "$BENCH_IMAGE" >/dev/null
fi

docker network create "$NETWORK" >/dev/null
docker run -d --name "$SERVER" --network "$NETWORK" "$IMAGE" >/dev/null

for _ in $(seq 1 100); do
  if docker run --rm --network "$NETWORK" "$BENCH_IMAGE" \
    redis-cli -h "$SERVER" -p 6379 PING 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.1
done

if ! docker run --rm --network "$NETWORK" "$BENCH_IMAGE" \
  redis-cli -h "$SERVER" -p 6379 PING 2>/dev/null | grep -q PONG; then
  echo "server did not become ready: $IMAGE" >&2
  exit 2
fi

cmd=(
  docker run --rm --network "$NETWORK" "$BENCH_IMAGE"
  redis-benchmark
  -h "$SERVER"
  -p 6379
  -n "$REQUESTS"
  -c "$CLIENTS"
  -P "$PIPELINE"
  -d "$PAYLOAD"
  -t "$TESTS"
)

if [[ "$CSV" == "1" ]]; then
  cmd+=(--csv)
fi

if [[ -n "${BENCH_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  extra=( $BENCH_ARGS )
  cmd+=("${extra[@]}")
fi

echo "image=$IMAGE"
echo "benchmark_client=$BENCH_IMAGE"
echo "requests=$REQUESTS clients=$CLIENTS pipeline=$PIPELINE payload=$PAYLOAD tests=$TESTS"

if [[ -n "$OUTPUT" ]]; then
  mkdir -p "$(dirname "$OUTPUT")"
  "${cmd[@]}" | tee "$OUTPUT"
else
  "${cmd[@]}"
fi

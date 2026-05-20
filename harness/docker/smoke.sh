#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${IMAGE:-valkey-rs:smoke}"
CONTAINER="valkey-rs-smoke-$$"
VOLUME="valkey-rs-smoke-$$"
TMPDIR="$(mktemp -d)"
PYTHON="${PYTHON:-python3}"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker volume rm "$VOLUME" >/dev/null 2>&1 || true
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

cd "$ROOT"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  docker build -t "$IMAGE" .
fi
docker volume create "$VOLUME" >/dev/null
docker run -d --name "$CONTAINER" -p 127.0.0.1::6379 -v "$VOLUME:/data" "$IMAGE" >/dev/null

HOST_PORT="$(docker port "$CONTAINER" 6379/tcp | awk -F: '{print $NF}')"

"$PYTHON" -m venv "$TMPDIR/venv"
# shellcheck disable=SC1091
. "$TMPDIR/venv/bin/activate"
python -m pip install -q 'redis>=5,<7'

python harness/docker/smoke.py --port "$HOST_PORT" --phase initial --prefix "$CONTAINER"
docker restart "$CONTAINER" >/dev/null
HOST_PORT="$(docker port "$CONTAINER" 6379/tcp | awk -F: '{print $NF}')"
python harness/docker/smoke.py --port "$HOST_PORT" --phase verify --prefix "$CONTAINER"

echo "docker smoke PASS: $IMAGE on 127.0.0.1:$HOST_PORT"

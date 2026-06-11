#!/usr/bin/env sh
set -eu

BASE="${BASE:-http://127.0.0.1:8787}"
TENANT="${TENANT:-tenant-smoke-$(date +%s)-$$}"

expect() {
  label="$1"
  expected="$2"
  actual="$3"
  if [ "$actual" != "$expected" ]; then
    printf '%s\nexpected: %s\nactual:   %s\n' "$label" "$expected" "$actual" >&2
    exit 1
  fi
  printf 'ok %s\n' "$label"
}

policy="$(curl -fsS -X PUT "$BASE/v1/policy/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"capacity":10,"refill_tokens":5,"refill_ms":1000,"ttl_ms":60000}')"
expect policy '{"result":"OK"}' "$policy"

first="$(curl -fsS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"now_millis":1000,"cost":7}')"
expect first-limit '{"allowed":true,"capacity":10,"remaining":3,"reset_ms":2400,"retry_after_ms":0}' "$first"

second="$(curl -fsS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"now_millis":1100,"cost":7}')"
expect second-limit '{"allowed":false,"capacity":10,"remaining":3,"reset_ms":2400,"retry_after_ms":700}' "$second"

set_value="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/raw%2Fkey/hello%20edge")"
expect valdr-set '{"result":"OK"}' "$set_value"

get_value="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/raw%2Fkey")"
expect valdr-get '{"result":"hello edge"}' "$get_value"

#!/usr/bin/env sh
# Deterministic smoke: drives the limiter with client-supplied now_millis, so
# the dev server must run with client time explicitly allowed:
#   npx wrangler dev --var EDGESTASH_ALLOW_CLIENT_TIME:true
# Without the var the limit/ai routes reject body time (see smoke-secure.sh).
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

ai="$(curl -fsS -X POST "$BASE/v1/ai/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"now_millis":2000,"tokens":3,"prompt":"summarize invoices"}')"
expect ai-demo "{\"charged_tokens\":3,\"completion\":\"EdgeStash accepted: summarize invoices\",\"limit\":{\"allowed\":true,\"capacity\":10,\"remaining\":5,\"reset_ms\":3000,\"retry_after_ms\":0},\"model\":\"toy-edge-llm\",\"ok\":true,\"tenant\":\"$TENANT\"}" "$ai"

set_value="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/raw%2Fkey/hello%20edge")"
expect valdr-set '{"result":"OK"}' "$set_value"

get_value="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/raw%2Fkey")"
expect valdr-get '{"result":"hello edge"}' "$get_value"

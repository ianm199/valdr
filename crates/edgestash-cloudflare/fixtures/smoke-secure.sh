#!/usr/bin/env sh
# Production-default smoke: the Worker clock is authoritative. Run against
# plain `npx wrangler dev` (no EDGESTASH_ALLOW_CLIENT_TIME var). Verifies that
# client-supplied now_millis is rejected and that the limiter drains and
# refills on the real clock without exact-timestamp assertions.
set -eu

BASE="${BASE:-http://127.0.0.1:8787}"
TENANT="${TENANT:-tenant-secure-$(date +%s)-$$}"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

expect_contains() {
  label="$1"
  needle="$2"
  haystack="$3"
  case "$haystack" in
    *"$needle"*) printf 'ok %s\n' "$label" ;;
    *)
      printf '%s\nexpected to contain: %s\nactual: %s\n' "$label" "$needle" "$haystack" >&2
      exit 1
      ;;
  esac
}

policy="$(curl -fsS -X PUT "$BASE/v1/policy/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"capacity":4,"refill_tokens":4,"refill_ms":1000,"ttl_ms":60000}')"
expect_contains policy '"result":"OK"' "$policy"

rejected="$(curl -sS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"now_millis":1000,"cost":1}')"
expect_contains client-time-rejected 'now_millis is not allowed' "$rejected"

first="$(curl -fsS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"cost":4}')"
expect_contains first-limit '"allowed":true' "$first"

second="$(curl -sS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"cost":4}')"
expect_contains drained '"allowed":false' "$second"

sleep 2

third="$(curl -fsS -X POST "$BASE/v1/limit/$TENANT" \
  -H 'content-type: application/json' \
  --data '{"cost":4}')"
expect_contains refilled-on-real-clock '"allowed":true' "$third"

set_value="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/session%2Fabc/live?PX=1500")"
expect_contains session-set '"result":"OK"' "$set_value"

live="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/session%2Fabc")"
expect_contains session-live '"result":"live"' "$live"

sleep 2

expired="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/session%2Fabc")"
expect_contains session-expired-on-real-clock '"result":null' "$expired"

printf 'secure smoke passed for %s\n' "$TENANT"

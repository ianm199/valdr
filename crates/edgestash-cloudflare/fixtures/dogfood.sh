#!/usr/bin/env sh
# Dogfood scenarios over real Worker HTTP in production time mode (plain
# `npx wrangler dev`): webhook idempotency via SET NX, a gaming leaderboard
# via ZADD/ZINCRBY/ZRANGE/ZREVRANK, and an atomic Lua room-join with capacity.
# Everything here is clock-independent, so no time vars are needed.
set -eu

BASE="${BASE:-http://127.0.0.1:8787}"
TENANT="${TENANT:-tenant-dogfood-$(date +%s)-$$}"

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

first="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/idem%3Aevt-1/processed?NX=&PX=86400000")"
expect idem-first-claim '{"result":"OK"}' "$first"

retry="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/idem%3Aevt-1/changed?NX=&PX=86400000")"
expect idem-retry-rejected '{"result":null}' "$retry"

original="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/idem%3Aevt-1")"
expect idem-original-survives '{"result":"processed"}' "$original"

zadd="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZADD/board/120/alice/95/bob/140/carol/95/dave")"
expect board-seeded '{"result":4}' "$zadd"

zincr="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZINCRBY/board/30/bob")"
expect bob-wins-a-match '{"result":"125"}' "$zincr"

top3="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZRANGE/board/0/2/REV/WITHSCORES")"
expect top-three '{"result":["carol","140","bob","125","alice","120"]}' "$top3"

rank="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZREVRANK/board/dave")"
expect dave-is-fourth '{"result":3}' "$rank"

JOIN_SCRIPT="
local members = KEYS[1]
local counter = KEYS[2]
local player = ARGV[1]
local capacity = tonumber(ARGV[2])
if redis.call('HGET', members, player) then
  return {ok='already-joined'}
end
local count = tonumber(redis.call('GET', counter) or '0')
if count >= capacity then
  return {err='ROOMFULL room is at capacity'}
end
redis.call('HSET', members, player, '1')
redis.call('SET', counter, tostring(count + 1))
return {ok='joined'}
"

loaded="$(curl -fsS -X POST "$BASE/v1/valdr/$TENANT/SCRIPT/LOAD" \
  -H 'content-type: text/plain' --data-binary "$JOIN_SCRIPT")"
sha="$(printf '%s' "$loaded" | sed -n 's/.*"result":"\([0-9a-f]\{40\}\)".*/\1/p')"
[ -n "$sha" ] || { printf 'script load failed: %s\n' "$loaded" >&2; exit 1; }
printf 'ok room-script-loaded %s\n' "$sha"

join_alice="$(curl -fsS "$BASE/v1/valdr/$TENANT/EVALSHA/$sha/2/room%3Amembers/room%3Acount/alice/2")"
expect alice-joins '{"result":"joined"}' "$join_alice"

join_bob="$(curl -fsS "$BASE/v1/valdr/$TENANT/EVALSHA/$sha/2/room%3Amembers/room%3Acount/bob/2")"
expect bob-joins '{"result":"joined"}' "$join_bob"

join_carol="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$sha/2/room%3Amembers/room%3Acount/carol/2")"
expect_contains room-full ROOMFULL "$join_carol"

rejoin="$(curl -fsS "$BASE/v1/valdr/$TENANT/EVALSHA/$sha/2/room%3Amembers/room%3Acount/alice/2")"
expect rejoin-idempotent '{"result":"already-joined"}' "$rejoin"

printf 'dogfood passed for %s\n' "$TENANT"

#!/usr/bin/env sh
# Flash-sale inventory: oversell prevention under REAL concurrency.
#
# One Durable Object per SKU owns that SKU's stock counter and every
# outstanding reservation hold. Cloudflare serializes requests to a single
# Durable Object and each Lua EVALSHA runs atomically inside the Valdr engine,
# so the read-decrement-write of a reservation can never interleave with
# another buyer's. We seed stock=10, then fire 50 simultaneous buyers (each a
# distinct hold id, qty 1). Exactly 10 must win (`reserved`), exactly 40 must
# lose (`SOLDOUT`), and the final stock must be exactly 0 — never negative.
# That is the money shot: 50 concurrent buyers, zero oversell.
#
# The call sequence mirrors, byte-for-byte, the sequence proven deterministic
# in crates/edgestash-demo/tests/demo_inventory.rs (run `cargo test -p
# edgestash-demo --test demo_inventory`). The hold key is composed the way the
# Worker route layer composes it: hold:flash:{hold_id}, percent-encoded.
#
# Reply shapes follow Redis Lua semantics: RESERVE returns the array
# {"result":["reserved",N]} on success and the error {"error":"SOLDOUT no
# stock"} (non-200) when drained; CANCEL returns {"result":["cancelled",qty]};
# CONFIRM returns {"result":"confirmed"}; a missing hold is {"error":"NOHOLD"}.
#
# True auto-release-on-expiry (stock reclaimed the instant a hold lapses) would
# need a scheduled reclaim via Durable Object alarms and is out of scope; the
# TTL on the hold key models the reservation window and the idempotency token,
# while explicit CONFIRM/CANCEL finalize.
set -eu

BASE="${BASE:-https://edgestash-valdr.ianmclaughlin1398.workers.dev}"
TENANT="${TENANT:-sku-flash-$(date +%s)-$$}"
BUYERS="${BUYERS:-50}"
STOCK="${STOCK:-10}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() {
  printf 'FAIL %s\n' "$1" >&2
  exit 1
}

expect() {
  label="$1"
  expected="$2"
  actual="$3"
  if [ "$actual" != "$expected" ]; then
    printf 'FAIL %s\nexpected: %s\nactual:   %s\n' "$label" "$expected" "$actual" >&2
    exit 1
  fi
  printf 'ok %s\n' "$label"
}

# The three flash-sale scripts, verbatim from the Rust test. RESERVE is
# idempotent on hold id (a retried buyer gets the same remaining without a
# second decrement); CANCEL is idempotent on the hold (a second cancel is
# NOHOLD and never double-restocks).
RESERVE_SCRIPT="
local stock_key = KEYS[1]
local hold_key = KEYS[2]
local qty = tonumber(ARGV[1])
local existing = redis.call('GET', hold_key)
if existing then
  local sep = string.find(existing, ':', 1, true)
  local remaining = tonumber(string.sub(existing, sep + 1))
  return {'reserved', remaining}
end
local stock = tonumber(redis.call('GET', stock_key) or '0')
if stock < qty then
  return {err='SOLDOUT no stock'}
end
local remaining = tonumber(redis.call('INCRBY', stock_key, -qty))
redis.call('SET', hold_key, tostring(qty) .. ':' .. tostring(remaining), 'PX', tonumber(ARGV[2]))
return {'reserved', remaining}
"

CONFIRM_SCRIPT="
local hold_key = KEYS[1]
if not redis.call('GET', hold_key) then
  return {err='NOHOLD'}
end
redis.call('DEL', hold_key)
return {ok='confirmed'}
"

CANCEL_SCRIPT="
local stock_key = KEYS[1]
local hold_key = KEYS[2]
local existing = redis.call('GET', hold_key)
if not existing then
  return {err='NOHOLD'}
end
local sep = string.find(existing, ':', 1, true)
local qty = tonumber(string.sub(existing, 1, sep - 1))
redis.call('INCRBY', stock_key, qty)
redis.call('DEL', hold_key)
return {'cancelled', qty}
"

load_script() {
  loaded="$(curl -fsS -X POST "$BASE/v1/valdr/$TENANT/SCRIPT/LOAD" \
    -H 'content-type: text/plain' --data-binary "$1")"
  sha="$(printf '%s' "$loaded" | sed -n 's/.*"result":"\([0-9a-f]\{40\}\)".*/\1/p')"
  [ -n "$sha" ] || fail "script load failed: $loaded"
  printf '%s' "$sha"
}

reserve_sha="$(load_script "$RESERVE_SCRIPT")"
printf 'ok reserve-script-loaded %s\n' "$reserve_sha"
confirm_sha="$(load_script "$CONFIRM_SCRIPT")"
printf 'ok confirm-script-loaded %s\n' "$confirm_sha"
cancel_sha="$(load_script "$CANCEL_SCRIPT")"
printf 'ok cancel-script-loaded %s\n' "$cancel_sha"

seed="$(curl -fsS "$BASE/v1/valdr/$TENANT/SET/stock/$STOCK")"
expect seed-stock '{"result":"OK"}' "$seed"

# The money shot: fire BUYERS reserves in parallel, each a distinct hold id,
# qty 1, TTL 600000ms. Capture each buyer's response to its own file so the
# concurrent writes never tangle, then wait for all of them.
printf 'firing %s parallel reserves against %s buyers for stock=%s ...\n' "$BUYERS" "$BUYERS" "$STOCK"
i=0
while [ "$i" -lt "$BUYERS" ]; do
  hold="hold%3Aflash%3Abuyer-$i"
  curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$reserve_sha/2/stock/$hold/1/600000" \
    >"$work/r-$i" 2>"$work/e-$i" &
  i=$((i + 1))
done
wait

reserved=0
soldout=0
other=0
i=0
while [ "$i" -lt "$BUYERS" ]; do
  resp="$(cat "$work/r-$i")"
  case "$resp" in
    *'"reserved"'*) reserved=$((reserved + 1)) ;;
    *SOLDOUT*) soldout=$((soldout + 1)) ;;
    *) other=$((other + 1)); printf 'unexpected buyer-%s response: %s\n' "$i" "$resp" >&2 ;;
  esac
  i=$((i + 1))
done

printf 'reserved=%s soldout=%s other=%s\n' "$reserved" "$soldout" "$other"
[ "$other" -eq 0 ] || fail "every buyer must be reserved or SOLDOUT; got $other unexpected"
[ "$reserved" -eq "$STOCK" ] || fail "exactly $STOCK buyers must win; got $reserved reserved"
[ "$soldout" -eq "$((BUYERS - STOCK))" ] || fail "exactly $((BUYERS - STOCK)) buyers must lose; got $soldout SOLDOUT"
printf 'ok exactly-%s-reserved-and-%s-soldout\n' "$STOCK" "$((BUYERS - STOCK))"

# The invariant that makes oversell impossible: stock landed at exactly 0 and
# was never allowed to go negative (the SOLDOUT guard runs before the decrement).
final="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/stock")"
expect final-stock-zero '{"result":"0"}' "$final"
case "$final" in
  *'"result":"-'*) fail "stock went negative: $final" ;;
esac
printf 'ok stock-never-negative\n'

# Two-phase finalize: confirm one winning hold (sale final, stock stays down),
# cancel another (stock restocked, then re-reservable). Both are idempotent.
confirm_path="$BASE/v1/valdr/$TENANT/EVALSHA/$confirm_sha/1/hold%3Aflash%3Abuyer-0"
confirmed="$(curl -fsS "$confirm_path")"
expect confirm-finalizes-sale '{"result":"confirmed"}' "$confirmed"
double_confirm="$(curl -sS "$confirm_path")"
case "$double_confirm" in
  *NOHOLD*) printf 'ok double-confirm-is-nohold\n' ;;
  *) fail "second confirm must be NOHOLD: $double_confirm" ;;
esac

cancel_path="$BASE/v1/valdr/$TENANT/EVALSHA/$cancel_sha/2/stock/hold%3Aflash%3Abuyer-1"
cancelled="$(curl -fsS "$cancel_path")"
expect cancel-restocks '{"result":["cancelled",1]}' "$cancelled"
restocked="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/stock")"
expect stock-back-to-one '{"result":"1"}' "$restocked"
double_cancel="$(curl -sS "$cancel_path")"
case "$double_cancel" in
  *NOHOLD*) printf 'ok double-cancel-is-nohold\n' ;;
  *) fail "second cancel must be NOHOLD: $double_cancel" ;;
esac
still_one="$(curl -fsS "$BASE/v1/valdr/$TENANT/GET/stock")"
expect no-double-restock '{"result":"1"}' "$still_one"

# The restocked unit is reservable again by a fresh buyer.
reborn="$(curl -fsS "$BASE/v1/valdr/$TENANT/EVALSHA/$reserve_sha/2/stock/hold%3Aflash%3Abuyer-restock/1/600000")"
expect restocked-unit-reservable '{"result":["reserved",0]}' "$reborn"

printf 'flash-sale oversell-prevention passed for %s: %s sold of %s, no oversell\n' \
  "$TENANT" "$STOCK" "$BUYERS"

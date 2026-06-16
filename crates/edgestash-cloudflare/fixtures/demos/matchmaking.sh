#!/usr/bin/env sh
# Skill-based matchmaking over real Worker HTTP.
#
# One Durable Object per matchmaking pool (a region+mode tuple) owns a single
# sorted set, `queue`, scored by player rating. Cloudflare serializes requests
# to a single Durable Object and each Lua EVALSHA runs atomically inside the
# Valdr engine, so the band query and the dual ZREM that claims a pair execute
# as one indivisible step: no two FIND_MATCH calls can see the same opponent as
# available and both remove it. That is what makes "never double-matched" a hard
# invariant.
#
# We enqueue six players — two close pairs (1000/1040, 1500/1520) and two
# ratings isolated in their own band (2000, 2400) — with band=100. FIND_MATCH
# pairs each close pair (each pairing is within band, each drops ZCARD by
# exactly 2) and reports the isolated ratings as `waiting`. We then assert no
# player appears in two matches and every pairing is within band.
#
# The call sequence mirrors, byte-for-byte, the sequence proven deterministic in
# crates/edgestash-demo/tests/demo_matchmaking.rs (run `cargo test -p
# edgestash-demo --test demo_matchmaking`).
#
# Band bounds are INCLUSIVE: ZRANGEBYSCORE is called with bare bounds
# (rating-band .. rating+band), so a candidate exactly `band` away is a match.
# `band` is the widest acceptable skill gap and the edge counts.
#
# Reply shapes follow Redis Lua semantics: a table with an `ok` field collapses
# to a bare status string (other fields dropped) and a table with `err` becomes
# a non-200 HTTP error. So FIND_MATCH returns the array
# {"result":["matched","<opponent>",<rating>]} on a pairing, {"result":
# ["waiting"]} for an isolated rating, and {"error":"NOTQUEUED ..."} (non-200)
# when the caller is not enqueued. ENQUEUE returns {"result":["queued",<rating>]}.
set -eu

BASE="${BASE:-https://edgestash-valdr.ianmclaughlin1398.workers.dev}"
TENANT="${TENANT:-pool-na-ranked-$(date +%s)-$$}"
BAND="${BAND:-100}"

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

expect_contains() {
  label="$1"
  needle="$2"
  haystack="$3"
  case "$haystack" in
    *"$needle"*) printf 'ok %s\n' "$label" ;;
    *)
      printf 'FAIL %s\nexpected to contain: %s\nactual: %s\n' "$label" "$needle" "$haystack" >&2
      exit 1
      ;;
  esac
}

# The two matchmaking scripts, verbatim from the Rust test. ENQUEUE is
# idempotent on the player (a re-enqueue does not duplicate). FIND_MATCH reads
# the caller's rating, queries the inclusive skill band with ZRANGEBYSCORE
# WITHSCORES, picks the closest in-band opponent (not merely the first), and
# atomically ZREMs both so neither can be matched again.
ENQUEUE_SCRIPT="
local queue = KEYS[1]
local player = ARGV[1]
local rating = tonumber(ARGV[2])
local existing = redis.call('ZSCORE', queue, player)
if existing then
  return {'queued', tonumber(existing)}
end
redis.call('ZADD', queue, rating, player)
return {'queued', rating}
"

FIND_MATCH_SCRIPT="
local queue = KEYS[1]
local player = ARGV[1]
local band = tonumber(ARGV[2])
local rating = redis.call('ZSCORE', queue, player)
if not rating then
  return {err='NOTQUEUED player is not in the queue'}
end
rating = tonumber(rating)
local low = tostring(rating - band)
local high = tostring(rating + band)
local candidates = redis.call('ZRANGEBYSCORE', queue, low, high, 'WITHSCORES')
local best_player = nil
local best_rating = nil
local best_diff = nil
local index = 1
while index < #candidates do
  local candidate = candidates[index]
  local candidate_rating = tonumber(candidates[index + 1])
  if candidate ~= player then
    local diff = candidate_rating - rating
    if diff < 0 then diff = -diff end
    if best_diff == nil or diff < best_diff then
      best_player = candidate
      best_rating = candidate_rating
      best_diff = diff
    end
  end
  index = index + 2
end
if best_player == nil then
  return {'waiting'}
end
redis.call('ZREM', queue, player, best_player)
return {'matched', best_player, best_rating}
"

load_script() {
  loaded="$(curl -fsS -X POST "$BASE/v1/valdr/$TENANT/SCRIPT/LOAD" \
    -H 'content-type: text/plain' --data-binary "$1")"
  sha="$(printf '%s' "$loaded" | sed -n 's/.*"result":"\([0-9a-f]\{40\}\)".*/\1/p')"
  [ -n "$sha" ] || fail "script load failed: $loaded"
  printf '%s' "$sha"
}

enqueue_sha="$(load_script "$ENQUEUE_SCRIPT")"
printf 'ok enqueue-script-loaded %s\n' "$enqueue_sha"
find_sha="$(load_script "$FIND_MATCH_SCRIPT")"
printf 'ok find-match-script-loaded %s\n' "$find_sha"

enqueue() {
  player="$1"
  rating="$2"
  resp="$(curl -fsS "$BASE/v1/valdr/$TENANT/EVALSHA/$enqueue_sha/1/queue/$player/$rating")"
  expect "enqueue-$player" "{\"result\":[\"queued\",$rating]}" "$resp"
}

# Six seeds: two close pairs and two isolated ratings. band=100 means
# 1000/1040 (40 apart) and 1500/1520 (20 apart) are in band; 2000 and 2400 are
# each 400+ from every other player and alone in their band.
enqueue p-1000 1000
enqueue p-1040 1040
enqueue p-1500 1500
enqueue p-1520 1520
enqueue p-2000 2000
enqueue p-2400 2400

card="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZCARD/queue")"
expect six-queued '{"result":6}' "$card"

card_of() {
  resp="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZCARD/queue")"
  printf '%s' "$resp" | sed -n 's/.*"result":\([0-9]*\).*/\1/p'
}

# matched_opponents accumulates every player that takes part in a match (caller
# and opponent both). A duplicate in this list would mean a player was matched
# twice — the invariant we forbid.
matched_opponents=""

# find_match runs FIND_MATCH for $1 and asserts the outcome named in $2:
#   matched:<opponent>:<opponent_rating>  — a pairing within band, card -2
#   waiting                               — isolated rating, card unchanged
# It enforces, on every match, that the gap is within band, that neither the
# caller nor the opponent has already been matched, and that ZCARD drops by
# exactly two.
find_match() {
  caller="$1"
  caller_rating="$2"
  expectation="$3"
  before="$(card_of)"
  resp="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$find_sha/1/queue/$caller/$BAND")"
  case "$expectation" in
    matched:*)
      opponent="$(printf '%s' "$expectation" | cut -d: -f2)"
      opp_rating="$(printf '%s' "$expectation" | cut -d: -f3)"
      expect "$caller-matches-$opponent" \
        "{\"result\":[\"matched\",\"$opponent\",$opp_rating]}" "$resp"
      gap=$((opp_rating - caller_rating))
      [ "$gap" -lt 0 ] && gap=$((-gap))
      [ "$gap" -le "$BAND" ] || fail "$caller->$opponent gap $gap exceeds band $BAND"
      printf 'ok %s-within-band-%s\n' "$caller" "$gap"
      for seen in $matched_opponents; do
        [ "$seen" = "$caller" ] && fail "$caller appears in two matches"
        [ "$seen" = "$opponent" ] && fail "$opponent appears in two matches"
      done
      matched_opponents="$matched_opponents $caller $opponent"
      after="$(card_of)"
      [ "$after" -eq "$((before - 2))" ] \
        || fail "$caller match must drop ZCARD by exactly 2 ($before -> $after)"
      printf 'ok %s-card-drops-by-two\n' "$caller"
      ;;
    waiting)
      expect "$caller-waiting" '{"result":["waiting"]}' "$resp"
      after="$(card_of)"
      [ "$after" -eq "$before" ] \
        || fail "$caller waiting must leave ZCARD unchanged ($before -> $after)"
      printf 'ok %s-card-unchanged\n' "$caller"
      ;;
    *)
      fail "unknown expectation: $expectation"
      ;;
  esac
}

find_match p-1000 1000 matched:p-1040:1040
find_match p-1500 1500 matched:p-1520:1520
find_match p-2000 2000 waiting
find_match p-2400 2400 waiting

# After two pairings, only the two isolated players remain queued.
final_card="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZCARD/queue")"
expect two-isolated-remain '{"result":2}' "$final_card"

# No double-match: a player removed by its own match is now NOTQUEUED. The
# opponent removed by the same match is NOTQUEUED too.
gone="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$find_sha/1/queue/p-1000/$BAND")"
expect_contains matched-player-is-notqueued NOTQUEUED "$gone"
gone_opp="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$find_sha/1/queue/p-1040/$BAND")"
expect_contains matched-opponent-is-notqueued NOTQUEUED "$gone_opp"

# Idempotent enqueue: re-enqueuing a still-queued player does not duplicate.
enqueue p-2000 2000
recard="$(curl -fsS "$BASE/v1/valdr/$TENANT/ZCARD/queue")"
expect re-enqueue-no-duplicate '{"result":2}' "$recard"

printf 'matchmaking passed for %s: 2 pairs matched (each within band, each -2 to ZCARD), 2 isolated ratings waiting, no double-match\n' \
  "$TENANT"

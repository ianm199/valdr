#!/usr/bin/env sh
# Edge feature-flag demo over real Worker HTTP: percentage rollout + kill
# switch + allow/deny lists, decided entirely at the edge with deterministic
# SHA-1 bucketing — no round-trip to a central flag service.
#
# Model: one app tenant (a Durable Object) owns its flags. A flag is one hash
#   HSET flag:{name} rollout <0-100> kill <0|1> allow <csv> deny <csv>
# and two Lua scripts are the whole decision surface:
#   SET_FLAG  validates rollout in 0..100, then HSETs the four fields.
#   EVALUATE  applies kill > deny > allow > rollout precedence, bucketing on
#             redis.sha1hex(flag .. ':' .. user). It returns the ARRAY
#             [state, reason, bucket] (not {ok=...}, which Redis status-reply
#             semantics would collapse to just the state) so the on/off state,
#             the reason, and the numeric bucket are all observable over REST
#             as {"result":["on"|"off","<reason>",N]}.
#
# Empty allow/deny lists travel as the sentinel "-" because the URL router
# drops empty path segments; SET_FLAG normalises "-" back to "".
#
# The on/off split below is hard-coded because SHA-1 bucketing is fully
# deterministic: flag "checkout-v2" at rollout 25 puts erin/grace/ivan/judy on
# (buckets 8/2/19/4 < 25) and the other eight off. This mirrors the Rust gate
# `cargo test -p edgestash-demo --test demo_feature_flags`.
set -eu

BASE="${BASE:-https://edgestash-valdr.ianmclaughlin1398.workers.dev}"
APP="${APP:-flags-demo-$(date +%s)-$$}"
FLAG="checkout-v2"
FLAG_KEY="flag%3Acheckout-v2"

USERS="alice bob carol dave erin frank grace heidi ivan judy mallory niaj"
# Users whose stable bucket is < 25 (the rollout-25 cohort that is ON).
ROLLOUT25_ON="erin grace ivan judy"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

# Extract the JSON "result" array first element ("on"/"off") from an EVALUATE
# reply like {"result":["on","rollout",37]}.
state_of() {
  printf '%s' "$1" | sed -n 's/.*"result":\["\([a-z]*\)".*/\1/p'
}

reason_of() {
  printf '%s' "$1" | sed -n 's/.*"result":\["[a-z]*","\([a-z]*\)".*/\1/p'
}

is_on_expected() {
  # Returns 0 (true) if $1 is in the ROLLOUT25_ON cohort.
  for on_user in $ROLLOUT25_ON; do
    [ "$1" = "$on_user" ] && return 0
  done
  return 1
}

load_script() {
  loaded="$(curl -fsS -X POST "$BASE/v1/valdr/$APP/SCRIPT/LOAD" \
    -H 'content-type: text/plain' --data-binary "$1")"
  sha="$(printf '%s' "$loaded" | sed -n 's/.*"result":"\([0-9a-f]\{40\}\)".*/\1/p')"
  [ -n "$sha" ] || fail "script load failed: $loaded"
  printf '%s' "$sha"
}

SET_FLAG_SCRIPT='
local flag_key = KEYS[1]
local rollout = tonumber(ARGV[1])
if rollout == nil or rollout < 0 or rollout > 100 then
  return {err="BADROLLOUT rollout must be an integer 0..100"}
end
local function unsentinel(value)
  if value == "-" then return "" end
  return value
end
redis.call("HSET", flag_key,
  "rollout", tostring(rollout),
  "kill", ARGV[2],
  "allow", unsentinel(ARGV[3]),
  "deny", unsentinel(ARGV[4]))
return {ok="set"}
'

EVALUATE_SCRIPT='
local flag_key = KEYS[1]
local flag_name = ARGV[1]
local user_id = ARGV[2]

local function csv_has(csv, needle)
  if csv == nil or csv == "" then
    return false
  end
  local hay = "," .. csv .. ","
  local pin = "," .. needle .. ","
  return string.find(hay, pin, 1, true) ~= nil
end

if redis.call("HGET", flag_key, "kill") == "1" then
  return {"off", "kill", -1}
end

if csv_has(redis.call("HGET", flag_key, "deny"), user_id) then
  return {"off", "deny", -1}
end

if csv_has(redis.call("HGET", flag_key, "allow"), user_id) then
  return {"on", "allow", -1}
end

local rollout = tonumber(redis.call("HGET", flag_key, "rollout") or "0")
local digest = redis.sha1hex(flag_name .. ":" .. user_id)
local bucket = tonumber(string.sub(digest, 1, 8), 16) % 100
if bucket < rollout then
  return {"on", "rollout", bucket}
end
return {"off", "rollout", bucket}
'

# A missing list is carried as the sentinel "-".
set_flag() {
  rollout="$1"; kill="$2"; allow="$3"; deny="$4"
  [ -n "$allow" ] || allow='-'
  [ -n "$deny" ] || deny='-'
  out="$(curl -fsS "$BASE/v1/valdr/$APP/EVALSHA/$SET_SHA/1/$FLAG_KEY/$rollout/$kill/$allow/$deny")"
  [ "$out" = '{"result":"set"}' ] || fail "SET_FLAG failed: $out"
}

evaluate() {
  curl -fsS "$BASE/v1/valdr/$APP/EVALSHA/$EVAL_SHA/1/$FLAG_KEY/$FLAG/$1"
}

# Count the on/off split across all USERS and assert it matches the expected
# deterministic cohort. $1 is a label for the output line.
assert_rollout25_split() {
  label="$1"
  on_count=0
  off_count=0
  for user in $USERS; do
    reply="$(evaluate "$user")"
    state="$(state_of "$reply")"
    case "$state" in
      on) on_count=$((on_count + 1)) ;;
      off) off_count=$((off_count + 1)) ;;
      *) fail "$label: unparseable EVALUATE reply for $user: $reply" ;;
    esac
    if is_on_expected "$user"; then
      [ "$state" = "on" ] || fail "$label: $user (bucket < 25) must be on, got $state"
    else
      [ "$state" = "off" ] || fail "$label: $user (bucket >= 25) must be off, got $state"
    fi
  done
  [ "$on_count" -eq 4 ] || fail "$label: expected 4 on, got $on_count"
  [ "$off_count" -eq 8 ] || fail "$label: expected 8 off, got $off_count"
  printf 'ok %s on=%s off=%s (%s)\n' "$label" "$on_count" "$off_count" "$ROLLOUT25_ON"
}

SET_SHA="$(load_script "$SET_FLAG_SCRIPT")"
printf 'ok set-flag-script-loaded %s\n' "$SET_SHA"
EVAL_SHA="$(load_script "$EVALUATE_SCRIPT")"
printf 'ok evaluate-script-loaded %s\n' "$EVAL_SHA"

# 1. Install checkout-v2 at rollout 25; show the deterministic on/off split.
set_flag 25 0 '' ''
printf 'ok flag-installed rollout=25 kill=0\n'
assert_rollout25_split rollout25-split

# 2. Re-run the split: identical, proving bucketing is stable across calls.
assert_rollout25_split rollout25-stable-rerun

# 3. Raise rollout to 100 -> everyone on.
set_flag 100 0 '' ''
for user in $USERS; do
  reply="$(evaluate "$user")"
  [ "$(state_of "$reply")" = "on" ] || fail "rollout-100: $user must be on, got $reply"
done
printf 'ok rollout-100-everyone-on\n'

# 4. Flip the kill switch -> everyone off (overrides the 100% rollout).
set_flag 100 1 '' ''
for user in $USERS; do
  reply="$(evaluate "$user")"
  [ "$(state_of "$reply")" = "off" ] || fail "kill: $user must be off, got $reply"
  [ "$(reason_of "$reply")" = "kill" ] || fail "kill: $user reason must be kill, got $reply"
done
printf 'ok kill-switch-everyone-off\n'

# 5. At rollout 0 with the kill switch off, an allow-listed user is forced on.
set_flag 0 0 'vip-user' ''
vip="$(evaluate "vip-user")"
[ "$(state_of "$vip")" = "on" ] || fail "allow: vip-user must be on at rollout 0, got $vip"
[ "$(reason_of "$vip")" = "allow" ] || fail "allow: reason must be allow, got $vip"
printf 'ok allow-listed-on-at-rollout-0\n'

# A non-allow-listed user at rollout 0 stays off by rollout.
plain="$(evaluate "alice")"
[ "$(state_of "$plain")" = "off" ] || fail "rollout-0: alice must be off, got $plain"
[ "$(reason_of "$plain")" = "rollout" ] || fail "rollout-0: alice reason must be rollout, got $plain"
printf 'ok non-allow-listed-off-at-rollout-0\n'

printf 'feature-flags demo passed for %s\n' "$APP"

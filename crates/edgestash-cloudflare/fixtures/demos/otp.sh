#!/usr/bin/env sh
# OTP / 2FA verification demo over real Worker HTTP. One Durable Object per user
# (keyed here by phone/email) owns an atomic brute-force lockout: two Lua scripts
# installed via SCRIPT LOAD + EVALSHA hold the whole policy.
#
#   ISSUE  stores the code and a per-user attempt budget, then sets the TTL.
#   VERIFY burns one attempt per wrong guess; once the budget is exhausted the
#          key is left LOCKED until its natural TTL, so the CORRECT code
#          submitted after lockout is STILL rejected.
#
# This demonstrates the lockout, which is clock-independent and therefore works
# against the live Worker where the clock is the real Date.now() and there is no
# way to fast-forward. TTL expiry (a code becoming EXPIRED once its window
# elapses) also runs on that real Worker clock; it is verified deterministically
# in the Rust test (crates/edgestash-demo/tests/demo_otp.rs, the
# `code_expires_on_the_request_clock` case) by advancing now_millis, because you
# cannot fast-forward a live edge clock in a shell demo.
#
# A fresh tenant per run keeps reruns independent.
set -eu

BASE="${BASE:-https://edgestash-valdr.ianmclaughlin1398.workers.dev}"
TENANT="${TENANT:-otp-demo-$(date +%s)-$$}"

MAX_ATTEMPTS=3
TTL_MS=300000
USER_KEY='user%3Aalice'
CODE=123456

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

# ISSUE stores code + attempt budget in a hash, then sets the key TTL. Re-issuing
# resets code, budget, and TTL together.
ISSUE_SCRIPT="
local otp = KEYS[1]
local code = ARGV[1]
local max_attempts = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
redis.call('HSET', otp, 'code', code, 'attempts', tostring(max_attempts))
redis.call('PEXPIRE', otp, tostring(ttl_ms))
return {ok='issued'}
"

# VERIFY is the gate. Absent key -> EXPIRED. Exhausted budget -> LOCKED without
# comparing the code. Correct code -> DEL + verified. Wrong code -> decrement;
# the decrement that hits zero leaves the key LOCKED for the rest of the TTL.
VERIFY_SCRIPT="
local otp = KEYS[1]
local submitted = ARGV[1]
local code = redis.call('HGET', otp, 'code')
if not code then
  return {err='EXPIRED no active code'}
end
local attempts = tonumber(redis.call('HGET', otp, 'attempts'))
if attempts <= 0 then
  return {err='LOCKED too many attempts'}
end
if submitted == code then
  redis.call('DEL', otp)
  return {ok='verified'}
end
attempts = attempts - 1
redis.call('HSET', otp, 'attempts', tostring(attempts))
if attempts <= 0 then
  return {err='LOCKED remaining=0'}
end
return {err='INVALID remaining=' .. tostring(attempts)}
"

load_sha() {
  script="$1"
  loaded="$(curl -fsS -X POST "$BASE/v1/valdr/$TENANT/SCRIPT/LOAD" \
    -H 'content-type: text/plain' --data-binary "$script")"
  sha="$(printf '%s' "$loaded" | sed -n 's/.*"result":"\([0-9a-f]\{40\}\)".*/\1/p')"
  [ -n "$sha" ] || { printf 'script load failed: %s\n' "$loaded" >&2; exit 1; }
  printf '%s' "$sha"
}

ISSUE_SHA="$(load_sha "$ISSUE_SCRIPT")"
printf 'ok issue-script-loaded %s\n' "$ISSUE_SHA"
VERIFY_SHA="$(load_sha "$VERIFY_SCRIPT")"
printf 'ok verify-script-loaded %s\n' "$VERIFY_SHA"

# ISSUE the code: EVALSHA <sha> 1 <otp_key> <code> <max_attempts> <ttl_ms>.
issued="$(curl -fsS \
  "$BASE/v1/valdr/$TENANT/EVALSHA/$ISSUE_SHA/1/$USER_KEY/$CODE/$MAX_ATTEMPTS/$TTL_MS")"
expect code-issued '{"result":"issued"}' "$issued"

# Three wrong guesses burn the budget 2 -> 1 -> 0. These return HTTP 400, so drop
# curl's -f (which would abort on non-2xx) and inspect the body instead.
wrong1="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$VERIFY_SHA/1/$USER_KEY/000000")"
expect wrong-guess-1-remaining-2 '{"error":"INVALID remaining=2"}' "$wrong1"

wrong2="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$VERIFY_SHA/1/$USER_KEY/111111")"
expect wrong-guess-2-remaining-1 '{"error":"INVALID remaining=1"}' "$wrong2"

wrong3="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$VERIFY_SHA/1/$USER_KEY/222222")"
expect wrong-guess-3-locks '{"error":"LOCKED remaining=0"}' "$wrong3"

# THE SECURITY INVARIANT: the CORRECT code submitted after lockout is still
# refused. The brute-forcer who finally guesses right gains nothing.
correct_after_lock="$(curl -sS "$BASE/v1/valdr/$TENANT/EVALSHA/$VERIFY_SHA/1/$USER_KEY/$CODE")"
expect_contains correct-code-after-lockout-still-rejected LOCKED "$correct_after_lock"
expect correct-code-after-lockout-exact '{"error":"LOCKED too many attempts"}' "$correct_after_lock"

printf 'otp lockout demo passed for %s (brute force defeated)\n' "$TENANT"

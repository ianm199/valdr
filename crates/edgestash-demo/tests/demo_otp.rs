//! OTP / 2FA verification demo: one EdgeStash Durable Object per user (keyed by
//! phone or email), driving an atomic brute-force lockout entirely from edge
//! state. Two Lua scripts installed over the wire via `SCRIPT LOAD` + `EVALSHA`
//! own the whole policy:
//!
//!   * `ISSUE` writes the code and an attempt budget into a hash, then sets the
//!     key's expiry. Re-issuing resets code, budget, and TTL together.
//!   * `VERIFY` is the security gate. A wrong submission burns one attempt; once
//!     the budget is exhausted the key is left in a locked state until its
//!     natural TTL, so the *correct* code submitted after lockout is still
//!     rejected. That is the property a per-user atomic counter buys you and a
//!     stateless verifier cannot: the brute-forcer cannot outrun the lock by
//!     guessing fast, because every guess mutates the same edge object.
//!
//! Time is driven by `now_millis` on each request — the raw `/v1/valdr` routes
//! treat the request clock as authoritative (secure mode), so TTL expiry is
//! exercised deterministically by advancing the clock rather than sleeping.

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage};
use serde_json::{json, Value as JsonValue};

/// Stores `code` and an `attempts` budget in a hash, then sets the key's expiry
/// to `ttl_ms`. Re-issuing overwrites both fields and resets the TTL, so a fresh
/// code always starts from a full attempt budget. The `attempts` field is the
/// single source of truth the verifier decrements.
const ISSUE_SCRIPT: &str = r#"
    local otp = KEYS[1]
    local code = ARGV[1]
    local max_attempts = tonumber(ARGV[2])
    local ttl_ms = tonumber(ARGV[3])
    redis.call('HSET', otp, 'code', code, 'attempts', tostring(max_attempts))
    redis.call('PEXPIRE', otp, tostring(ttl_ms))
    return {ok='issued'}
"#;

/// The brute-force gate. Order of checks is deliberate:
///   1. Absent key (expired or never issued) -> EXPIRED.
///   2. Exhausted budget -> LOCKED, *without* comparing the code, so a correct
///      guess after lockout leaks nothing and is still refused.
///   3. Correct code -> delete the key (single-use, no replay) and verify.
///   4. Wrong code -> decrement; if the budget just hit zero, leave the key in
///      place (do not delete) so the lock persists for the rest of the TTL
///      window; otherwise report how many attempts remain.
/// The remaining count rides inside the error message because the host maps a
/// returned `{err=...}` table straight to a RESP error whose extra table fields
/// are dropped.
const VERIFY_SCRIPT: &str = r#"
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
"#;

const TENANT: &str = "otp";
const MAX_ATTEMPTS: u32 = 3;
const TTL_MS: u64 = 300_000;

fn open(storage: MemoryObjectStorage) -> EdgeObject<MemoryObjectStorage> {
    EdgeObject::open(storage).unwrap()
}

fn body_json(body: &[u8]) -> JsonValue {
    serde_json::from_slice(body).unwrap()
}

fn get(
    object: &mut EdgeObject<MemoryObjectStorage>,
    path: &str,
    now_millis: u64,
) -> (u16, JsonValue) {
    let response = object.handle_http(EdgeHttpRequest::get(path, now_millis));
    (response.status, body_json(&response.body))
}

fn post(
    object: &mut EdgeObject<MemoryObjectStorage>,
    path: &str,
    body: &[u8],
    now_millis: u64,
) -> (u16, JsonValue) {
    let response = object.handle_http(EdgeHttpRequest::post(path, body, now_millis));
    (response.status, body_json(&response.body))
}

fn load_script(
    object: &mut EdgeObject<MemoryObjectStorage>,
    script: &str,
    now_millis: u64,
) -> String {
    let (status, value) = post(
        object,
        &format!("/v1/valdr/{TENANT}/SCRIPT/LOAD"),
        script.as_bytes(),
        now_millis,
    );
    assert_eq!(status, 200, "SCRIPT LOAD failed: {value}");
    value["result"].as_str().unwrap().to_owned()
}

/// `ISSUE otp_key code max_attempts ttl_ms` — one EVALSHA over the raw route.
fn issue(
    object: &mut EdgeObject<MemoryObjectStorage>,
    sha: &str,
    otp_key: &str,
    code: &str,
    now_millis: u64,
) -> (u16, JsonValue) {
    get(
        object,
        &format!("/v1/valdr/{TENANT}/EVALSHA/{sha}/1/{otp_key}/{code}/{MAX_ATTEMPTS}/{TTL_MS}"),
        now_millis,
    )
}

/// `VERIFY otp_key submitted_code`.
fn verify(
    object: &mut EdgeObject<MemoryObjectStorage>,
    sha: &str,
    otp_key: &str,
    submitted: &str,
    now_millis: u64,
) -> (u16, JsonValue) {
    get(
        object,
        &format!("/v1/valdr/{TENANT}/EVALSHA/{sha}/1/{otp_key}/{submitted}"),
        now_millis,
    )
}

/// Three wrong guesses burn the budget 2 -> 1 -> 0; the third wrong guess locks
/// the key, and the *correct* code submitted afterwards is still rejected as
/// LOCKED. This is the headline assertion: an attacker who finally guesses right
/// after exhausting the budget gains nothing.
#[test]
fn brute_force_lockout_defeats_a_late_correct_guess() {
    let mut object = open(MemoryObjectStorage::default());
    let issue_sha = load_script(&mut object, ISSUE_SCRIPT, 1_000);
    let verify_sha = load_script(&mut object, VERIFY_SCRIPT, 1_000);

    let (status, value) = issue(&mut object, &issue_sha, "user%3Aalice", "123456", 1_000);
    assert_eq!(status, 200, "issue failed: {value}");
    assert_eq!(value, json!({"result": "issued"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Aalice", "000000", 1_001);
    assert_eq!(status, 400, "first wrong guess: {value}");
    assert_eq!(value, json!({"error": "INVALID remaining=2"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Aalice", "111111", 1_002);
    assert_eq!(status, 400, "second wrong guess: {value}");
    assert_eq!(value, json!({"error": "INVALID remaining=1"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Aalice", "222222", 1_003);
    assert_eq!(status, 400, "third wrong guess locks the key: {value}");
    assert_eq!(value, json!({"error": "LOCKED remaining=0"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Aalice", "123456", 1_004);
    assert_eq!(
        status, 400,
        "the CORRECT code after lockout must still be rejected: {value}"
    );
    assert_eq!(
        value,
        json!({"error": "LOCKED too many attempts"}),
        "brute force defeated: a late correct guess gains nothing"
    );
}

/// A correct code on the first try verifies and deletes the key, so a replay of
/// the same code afterwards sees no active code at all.
#[test]
fn first_try_success_consumes_the_code_with_no_replay() {
    let mut object = open(MemoryObjectStorage::default());
    let issue_sha = load_script(&mut object, ISSUE_SCRIPT, 1_000);
    let verify_sha = load_script(&mut object, VERIFY_SCRIPT, 1_000);

    let (status, value) = issue(&mut object, &issue_sha, "user%3Abob", "654321", 1_000);
    assert_eq!(status, 200, "issue failed: {value}");
    assert_eq!(value, json!({"result": "issued"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Abob", "654321", 1_001);
    assert_eq!(status, 200, "correct on first try: {value}");
    assert_eq!(value, json!({"result": "verified"}));

    let (status, value) = verify(&mut object, &verify_sha, "user%3Abob", "654321", 1_002);
    assert_eq!(status, 400, "code is single-use, no replay: {value}");
    assert_eq!(value, json!({"error": "EXPIRED no active code"}));
}

/// Re-issuing the same key resets both the code and the attempt budget. A guess
/// that was wrong for the old code does not carry its decrement into the new
/// budget, and the new code verifies.
#[test]
fn reissue_resets_code_and_attempt_budget() {
    let mut object = open(MemoryObjectStorage::default());
    let issue_sha = load_script(&mut object, ISSUE_SCRIPT, 1_000);
    let verify_sha = load_script(&mut object, VERIFY_SCRIPT, 1_000);

    issue(&mut object, &issue_sha, "user%3Acarol", "111000", 1_000);
    let (_, value) = verify(&mut object, &verify_sha, "user%3Acarol", "999999", 1_001);
    assert_eq!(
        value,
        json!({"error": "INVALID remaining=2"}),
        "budget burned once"
    );

    let (status, value) = issue(&mut object, &issue_sha, "user%3Acarol", "222000", 1_002);
    assert_eq!(status, 200, "reissue failed: {value}");

    let (_, value) = verify(&mut object, &verify_sha, "user%3Acarol", "888888", 1_003);
    assert_eq!(
        value,
        json!({"error": "INVALID remaining=2"}),
        "budget reset to full on reissue, old decrement discarded"
    );

    let (status, value) = verify(&mut object, &verify_sha, "user%3Acarol", "222000", 1_004);
    assert_eq!(status, 200, "new code verifies: {value}");
    assert_eq!(value, json!({"result": "verified"}));
}

/// TTL expiry runs on the request clock: a code issued with a 300s TTL is gone
/// the moment the clock crosses the deadline, and the correct code submitted
/// then reads as EXPIRED. No sleeping — `now_millis` is the authority.
#[test]
fn code_expires_on_the_request_clock() {
    let mut object = open(MemoryObjectStorage::default());
    let issue_sha = load_script(&mut object, ISSUE_SCRIPT, 1_000);
    let verify_sha = load_script(&mut object, VERIFY_SCRIPT, 1_000);

    let (status, _) = issue(&mut object, &issue_sha, "user%3Adave", "424242", 1_000);
    assert_eq!(status, 200);

    let just_before = 1_000 + TTL_MS - 1;
    let (status, value) = verify(
        &mut object,
        &verify_sha,
        "user%3Adave",
        "000000",
        just_before,
    );
    assert_eq!(status, 400, "still live just before expiry: {value}");
    assert_eq!(value, json!({"error": "INVALID remaining=2"}));

    let after_expiry = 1_000 + TTL_MS + 1;
    let (status, value) = verify(
        &mut object,
        &verify_sha,
        "user%3Adave",
        "424242",
        after_expiry,
    );
    assert_eq!(status, 400, "expired code reads as EXPIRED: {value}");
    assert_eq!(value, json!({"error": "EXPIRED no active code"}));
}

/// An in-flight challenge survives a cold start: snapshot the object to storage,
/// reopen it, and the issued code plus its remaining attempt budget are intact,
/// so verification continues exactly where it left off.
#[test]
fn pending_challenge_survives_cold_start() {
    let mut object = open(MemoryObjectStorage::default());
    let issue_sha = load_script(&mut object, ISSUE_SCRIPT, 1_000);
    let verify_sha = load_script(&mut object, VERIFY_SCRIPT, 1_000);

    issue(&mut object, &issue_sha, "user%3Aerin", "777777", 1_000);
    let (_, value) = verify(&mut object, &verify_sha, "user%3Aerin", "000000", 1_001);
    assert_eq!(
        value,
        json!({"error": "INVALID remaining=2"}),
        "one attempt burned pre-restart"
    );

    let storage = object.into_storage();
    let mut reopened = open(storage);

    let reissue_sha = load_script(&mut reopened, ISSUE_SCRIPT, 2_000);
    assert_eq!(reissue_sha, issue_sha, "script sha is content-addressed");
    let reverify_sha = load_script(&mut reopened, VERIFY_SCRIPT, 2_000);

    let (_, value) = verify(&mut reopened, &reverify_sha, "user%3Aerin", "111111", 2_001);
    assert_eq!(
        value,
        json!({"error": "INVALID remaining=1"}),
        "attempt budget survived cold start: still decrementing from where it stopped"
    );

    let (status, value) = verify(&mut reopened, &reverify_sha, "user%3Aerin", "777777", 2_002);
    assert_eq!(status, 200, "stored code survived cold start: {value}");
    assert_eq!(value, json!({"result": "verified"}));
}

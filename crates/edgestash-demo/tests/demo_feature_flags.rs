//! Edge feature-flag demo: percentage rollout + kill switch + allow/deny lists,
//! evaluated entirely at the edge with deterministic SHA-1 bucketing — no
//! round-trip to a central flag service.
//!
//! Each app/project is one Durable Object (one tenant). A flag is one hash:
//!   HSET flag:{name} rollout <0-100> kill <0|1> allow <csv> deny <csv>
//!
//! Two Lua scripts, installed over the wire via `SCRIPT LOAD` + `EVALSHA`, are
//! the entire decision surface:
//!   * `SET_FLAG_SCRIPT`  — validates `rollout` is in 0..=100, then HSETs the
//!     four fields atomically.
//!   * `EVALUATE_SCRIPT`  — kill > deny > allow > rollout precedence, with a
//!     stable per-(flag,user) bucket from `redis.sha1hex(flag .. ':' .. user)`.
//!
//! ## Why EVALUATE returns an array, not `{ok=...}`
//!
//! The Valdr Lua bridge follows Redis status-reply semantics: a returned table
//! with an `ok` field collapses to a bare simple-string reply and every other
//! field (`reason`, `bucket`) is discarded (see `valdr_engine::lua_to_resp`).
//! To surface all three observable facts — the on/off state, the reason, and
//! the numeric bucket — EVALUATE returns a Lua array `{state, reason, bucket}`,
//! which the REST adapter renders as `{"result": ["on"|"off", "<reason>", N]}`.
//! `bucket` is the real 0..99 bucket whenever rollout was the deciding stage,
//! and `-1` for the kill / deny / allow short-circuits (no bucket computed).

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage};
use serde_json::{json, Value as JsonValue};

/// Validating writer: rejects an out-of-range rollout instead of storing a
/// nonsense percentage. `ARGV = [rollout, kill, allow_csv, deny_csv]`.
///
/// The edge URL router drops empty path segments, so an empty allow/deny list
/// is carried on the wire as the explicit sentinel `-`; the script normalises
/// `-` back to the empty string before storing it.
const SET_FLAG_SCRIPT: &str = r#"
    local flag_key = KEYS[1]
    local rollout = tonumber(ARGV[1])
    if rollout == nil or rollout < 0 or rollout > 100 then
        return {err='BADROLLOUT rollout must be an integer 0..100'}
    end
    local function unsentinel(value)
        if value == '-' then return '' end
        return value
    end
    redis.call('HSET', flag_key,
        'rollout', tostring(rollout),
        'kill', ARGV[2],
        'allow', unsentinel(ARGV[3]),
        'deny', unsentinel(ARGV[4]))
    return {ok='set'}
"#;

/// Edge evaluator. `KEYS = [flag_key]`, `ARGV = [flag_name, user_id]`.
/// Returns the array `{state, reason, bucket}`.
///
/// Precedence: kill switch first, then deny list, then allow list, then the
/// deterministic percentage rollout. Membership is an exact whole-token match:
/// both the csv haystack and the user needle are comma-bracketed before the
/// substring search so that user `1` never matches inside `10,21`.
const EVALUATE_SCRIPT: &str = r#"
    local flag_key = KEYS[1]
    local flag_name = ARGV[1]
    local user_id = ARGV[2]

    local function csv_has(csv, needle)
        if csv == nil or csv == '' then
            return false
        end
        local hay = ',' .. csv .. ','
        local pin = ',' .. needle .. ','
        return string.find(hay, pin, 1, true) ~= nil
    end

    if redis.call('HGET', flag_key, 'kill') == '1' then
        return {'off', 'kill', -1}
    end

    if csv_has(redis.call('HGET', flag_key, 'deny'), user_id) then
        return {'off', 'deny', -1}
    end

    if csv_has(redis.call('HGET', flag_key, 'allow'), user_id) then
        return {'on', 'allow', -1}
    end

    local rollout = tonumber(redis.call('HGET', flag_key, 'rollout') or '0')
    local digest = redis.sha1hex(flag_name .. ':' .. user_id)
    local bucket = tonumber(string.sub(digest, 1, 8), 16) % 100
    if bucket < rollout then
        return {'on', 'rollout', bucket}
    end
    return {'off', 'rollout', bucket}
"#;

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
    tenant: &str,
    script: &str,
    now_millis: u64,
) -> String {
    let (status, value) = post(
        object,
        &format!("/v1/valdr/{tenant}/SCRIPT/LOAD"),
        script.as_bytes(),
        now_millis,
    );
    assert_eq!(status, 200, "SCRIPT LOAD failed: {value}");
    value["result"].as_str().unwrap().to_owned()
}

/// One decision as it comes back from EVALUATE: `(state, reason, bucket)`.
struct Decision {
    state: String,
    reason: String,
    bucket: i64,
}

const APP: &str = "checkout-app";
const FLAG: &str = "checkout-v2";
const FLAG_KEY: &str = "flag%3Acheckout-v2";

/// The empty-list sentinel carried over the wire, since the URL router drops
/// empty path segments. `SET_FLAG_SCRIPT` normalises it back to `""`.
const EMPTY_LIST: &str = "-";

fn list_arg(list: &str) -> &str {
    if list.is_empty() {
        EMPTY_LIST
    } else {
        list
    }
}

fn install_flag(
    object: &mut EdgeObject<MemoryObjectStorage>,
    set_sha: &str,
    rollout: i64,
    kill: i64,
    allow: &str,
    deny: &str,
    now_millis: u64,
) {
    let path = format!(
        "/v1/valdr/{APP}/EVALSHA/{set_sha}/1/{FLAG_KEY}/{rollout}/{kill}/{}/{}",
        list_arg(allow),
        list_arg(deny)
    );
    let (status, value) = get(object, &path, now_millis);
    assert_eq!(status, 200, "SET_FLAG failed: {value}");
    assert_eq!(value, json!({"result": "set"}), "SET_FLAG reply");
}

fn evaluate(
    object: &mut EdgeObject<MemoryObjectStorage>,
    eval_sha: &str,
    user: &str,
    now_millis: u64,
) -> Decision {
    let path = format!("/v1/valdr/{APP}/EVALSHA/{eval_sha}/1/{FLAG_KEY}/{FLAG}/{user}");
    let (status, value) = get(object, &path, now_millis);
    assert_eq!(status, 200, "EVALUATE failed for {user}: {value}");
    let array = value["result"].as_array().unwrap();
    assert_eq!(
        array.len(),
        3,
        "EVALUATE must return [state, reason, bucket]"
    );
    Decision {
        state: array[0].as_str().unwrap().to_owned(),
        reason: array[1].as_str().unwrap().to_owned(),
        bucket: array[2].as_i64().unwrap(),
    }
}

const USERS: [&str; 6] = ["u-alice", "u-bob", "u-carol", "u-dave", "u-erin", "u-frank"];

#[test]
fn rollout_zero_is_everyone_off_and_buckets_are_stable() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);
    install_flag(&mut object, &set_sha, 0, 0, "", "", 1_001);

    for user in USERS {
        let first = evaluate(&mut object, &eval_sha, user, 2_000);
        assert_eq!(first.state, "off", "{user} must be off at rollout 0");
        assert_eq!(first.reason, "rollout", "{user} reason at rollout 0");
        assert!(
            (0..100).contains(&first.bucket),
            "{user} bucket {} out of range",
            first.bucket
        );

        let second = evaluate(&mut object, &eval_sha, user, 2_001);
        assert_eq!(
            first.bucket, second.bucket,
            "bucket for {user} must be stable across calls"
        );
    }
}

#[test]
fn rollout_hundred_is_everyone_on() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);
    install_flag(&mut object, &set_sha, 100, 0, "", "", 1_001);

    for user in USERS {
        let decision = evaluate(&mut object, &eval_sha, user, 2_000);
        assert_eq!(decision.state, "on", "{user} must be on at rollout 100");
        assert_eq!(decision.reason, "rollout");
    }
}

#[test]
fn allow_list_forces_on_and_deny_list_forces_off() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);

    install_flag(&mut object, &set_sha, 0, 0, "u-alice", "", 1_001);
    let allowed = evaluate(&mut object, &eval_sha, "u-alice", 2_000);
    assert_eq!(allowed.state, "on", "allow-listed user is on at rollout 0");
    assert_eq!(allowed.reason, "allow");
    assert_eq!(allowed.bucket, -1, "allow short-circuits before bucketing");

    install_flag(&mut object, &set_sha, 100, 0, "", "u-bob", 1_002);
    let denied = evaluate(&mut object, &eval_sha, "u-bob", 2_001);
    assert_eq!(
        denied.state, "off",
        "deny-listed user off even at rollout 100"
    );
    assert_eq!(denied.reason, "deny");
    assert_eq!(denied.bucket, -1, "deny short-circuits before bucketing");
}

#[test]
fn deny_takes_precedence_over_allow() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);

    install_flag(&mut object, &set_sha, 100, 0, "u-carol", "u-carol", 1_001);
    let decision = evaluate(&mut object, &eval_sha, "u-carol", 2_000);
    assert_eq!(decision.state, "off", "deny beats allow for the same user");
    assert_eq!(decision.reason, "deny");
}

#[test]
fn membership_is_whole_token_not_substring() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);

    install_flag(&mut object, &set_sha, 0, 0, "10,21,33", "", 1_001);
    let substring_user = evaluate(&mut object, &eval_sha, "1", 2_000);
    assert_eq!(
        substring_user.reason, "rollout",
        "user '1' must NOT match inside allow token '10'"
    );
    let whole_user = evaluate(&mut object, &eval_sha, "10", 2_001);
    assert_eq!(
        whole_user.reason, "allow",
        "user '10' is a whole-token member"
    );
}

#[test]
fn kill_switch_overrides_allow_list_and_rollout() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);

    install_flag(&mut object, &set_sha, 100, 1, "u-alice", "", 1_001);
    for user in ["u-alice", "u-bob", "u-carol"] {
        let decision = evaluate(&mut object, &eval_sha, user, 2_000);
        assert_eq!(decision.state, "off", "kill switch turns {user} off");
        assert_eq!(
            decision.reason, "kill",
            "kill switch overrides allow list and rollout for {user}"
        );
        assert_eq!(decision.bucket, -1);
    }
}

#[test]
fn rollout_boundary_flips_at_the_users_own_bucket() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);

    install_flag(&mut object, &set_sha, 0, 0, "", "", 1_001);
    let probe = evaluate(&mut object, &eval_sha, "u-boundary", 2_000);
    let bucket = probe.bucket;
    assert!(
        (0..99).contains(&bucket),
        "need a bucket with room to add 1 (got {bucket})"
    );

    install_flag(&mut object, &set_sha, bucket, 0, "", "", 2_001);
    let at = evaluate(&mut object, &eval_sha, "u-boundary", 2_002);
    assert_eq!(
        at.state, "off",
        "bucket {bucket} is off at rollout {bucket} (bucket < rollout is false)"
    );

    install_flag(&mut object, &set_sha, bucket + 1, 0, "", "", 2_003);
    let above = evaluate(&mut object, &eval_sha, "u-boundary", 2_004);
    assert_eq!(
        above.state,
        "on",
        "bucket {bucket} flips on at rollout {}",
        bucket + 1
    );
    assert_eq!(above.bucket, bucket, "bucket value itself is unchanged");
}

#[test]
fn decisions_are_identical_after_cold_start() {
    let mut object = open(MemoryObjectStorage::default());
    let set_sha = load_script(&mut object, APP, SET_FLAG_SCRIPT, 1_000);
    let eval_sha = load_script(&mut object, APP, EVALUATE_SCRIPT, 1_000);
    install_flag(&mut object, &set_sha, 50, 0, "u-alice", "u-bob", 1_001);

    let before: Vec<(String, String, i64)> = USERS
        .iter()
        .map(|user| {
            let decision = evaluate(&mut object, &eval_sha, user, 2_000);
            (decision.state, decision.reason, decision.bucket)
        })
        .collect();

    let storage = object.into_storage();
    let mut reopened = open(storage);
    let eval_sha_after = load_script(&mut reopened, APP, EVALUATE_SCRIPT, 3_000);
    assert_eq!(eval_sha, eval_sha_after, "script SHA is content-addressed");

    let after: Vec<(String, String, i64)> = USERS
        .iter()
        .map(|user| {
            let decision = evaluate(&mut reopened, &eval_sha_after, user, 3_001);
            (decision.state, decision.reason, decision.bucket)
        })
        .collect();

    assert_eq!(before, after, "every flag decision survives cold start");
}

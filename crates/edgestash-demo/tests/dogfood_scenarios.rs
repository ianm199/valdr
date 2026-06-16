//! Dogfood scenarios: end-to-end product workloads driven through the
//! Worker-facing HTTP route layer (`EdgeObject::handle_http`) exactly as a
//! Cloudflare Durable Object would drive them — secure time mode (the request
//! clock is authoritative), snapshot persistence through `ObjectStorage`, and
//! Lua scripts installed over the wire via `SCRIPT LOAD` + `EVALSHA`.
//!
//! Scenario classes:
//!   1. Upstash-style: webhook idempotency keys, session revocation, AI spend
//!      guard with mid-stream plan upgrade.
//!   2. Gaming / shared state: atomic room join/leave with capacity, turn-based
//!      move validation, leaderboard with rank queries surviving cold start.

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage};
use serde_json::{json, Value as JsonValue};

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

#[test]
fn webhook_idempotency_keys_dedupe_retries() {
    let mut object = open(MemoryObjectStorage::default());

    let (status, value) = get(
        &mut object,
        "/v1/valdr/hooks/SET/idem%3Aevt-1001/processed?NX=&PX=86400000",
        1_000,
    );
    assert_eq!(status, 200, "first delivery must claim the key: {value}");
    assert_eq!(value, json!({"result": "OK"}));

    let (_, value) = get(
        &mut object,
        "/v1/valdr/hooks/SET/idem%3Aevt-1001/processed-again?NX=&PX=86400000",
        2_000,
    );
    assert_eq!(
        value,
        json!({"result": JsonValue::Null}),
        "retry of the same event id must be rejected by NX"
    );

    let (_, value) = get(&mut object, "/v1/valdr/hooks/GET/idem%3Aevt-1001", 3_000);
    assert_eq!(
        value,
        json!({"result": "processed"}),
        "original result must survive the duplicate delivery"
    );

    let day_later = 1_000 + 86_400_000;
    let (_, value) = get(
        &mut object,
        "/v1/valdr/hooks/SET/idem%3Aevt-1001/fresh?NX=&PX=86400000",
        day_later,
    );
    assert_eq!(
        value,
        json!({"result": "OK"}),
        "after the idempotency window expires the key is claimable again"
    );
}

#[test]
fn session_tokens_revoke_and_expire_on_request_clock() {
    let mut object = open(MemoryObjectStorage::default());

    let (status, value) = get(
        &mut object,
        "/v1/valdr/auth/SETEX/sess%3Aalice/3600/token-a",
        10_000,
    );
    assert_eq!(status, 200);
    assert_eq!(value, json!({"result": "OK"}));
    let (_, value) = get(
        &mut object,
        "/v1/valdr/auth/SETEX/sess%3Abob/3600/token-b",
        10_000,
    );
    assert_eq!(value, json!({"result": "OK"}));

    let (_, value) = get(&mut object, "/v1/valdr/auth/TTL/sess%3Aalice", 1_810_000);
    assert_eq!(value, json!({"result": 1800}), "half the hour remains");

    let (_, value) = get(&mut object, "/v1/valdr/auth/DEL/sess%3Aalice", 1_900_000);
    assert_eq!(value, json!({"result": 1}), "explicit revocation");
    let (_, value) = get(&mut object, "/v1/valdr/auth/EXISTS/sess%3Aalice", 1_900_001);
    assert_eq!(value, json!({"result": 0}));

    let storage = object.into_storage();
    let mut reopened = open(storage);
    let (_, value) = get(&mut reopened, "/v1/valdr/auth/GET/sess%3Abob", 2_000_000);
    assert_eq!(
        value,
        json!({"result": "token-b"}),
        "unrevoked session survives object reopen"
    );
    let (_, value) = get(&mut reopened, "/v1/valdr/auth/GET/sess%3Abob", 3_700_000);
    assert_eq!(
        value,
        json!({"result": JsonValue::Null}),
        "session expires on the request clock"
    );
}

#[test]
fn ai_spend_guard_enforces_plan_upgrade_midstream() {
    let mut object = open(MemoryObjectStorage::default());

    let free_plan = br#"{"capacity":10,"refill_tokens":1,"refill_ms":1000,"ttl_ms":600000}"#;
    let response = object.handle_http(EdgeHttpRequest::put("/v1/policy/acme", free_plan, 1_000));
    assert_eq!(response.status, 200);

    let (status, value) = post(
        &mut object,
        "/v1/ai/acme",
        br#"{"tokens":8,"prompt":"draft launch tweet"}"#,
        1_000,
    );
    assert_eq!(status, 200);
    assert_eq!(value["ok"], json!(true));
    assert_eq!(value["limit"]["remaining"], json!(2));

    let (status, value) = post(
        &mut object,
        "/v1/ai/acme",
        br#"{"tokens":8,"prompt":"draft launch blog"}"#,
        1_100,
    );
    assert_eq!(status, 429, "free plan is drained: {value}");
    assert_eq!(value["error"], json!("rate_limited"));

    let pro_plan = br#"{"capacity":100,"refill_tokens":50,"refill_ms":1000,"ttl_ms":600000}"#;
    let response = object.handle_http(EdgeHttpRequest::put("/v1/policy/acme", pro_plan, 1_200));
    assert_eq!(response.status, 200);

    let (status, value) = post(
        &mut object,
        "/v1/ai/acme",
        br#"{"tokens":8,"prompt":"draft launch blog"}"#,
        1_300,
    );
    assert_eq!(
        status, 200,
        "plan upgrade unlocks the same request: {value}"
    );
    assert_eq!(value["ok"], json!(true));
    assert_eq!(value["charged_tokens"], json!(8));
}

const ROOM_JOIN_SCRIPT: &str = r#"
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
"#;

const ROOM_LEAVE_SCRIPT: &str = r#"
    local members = KEYS[1]
    local counter = KEYS[2]
    local player = ARGV[1]
    if not redis.call('HGET', members, player) then
        return {ok='not-present'}
    end
    redis.call('HDEL', members, player)
    local count = tonumber(redis.call('GET', counter) or '1')
    redis.call('SET', counter, tostring(count - 1))
    return {ok='left'}
"#;

#[test]
fn game_room_join_leave_capacity_is_atomic() {
    let mut object = open(MemoryObjectStorage::default());
    let join = load_script(&mut object, "game", ROOM_JOIN_SCRIPT, 1_000);
    let leave = load_script(&mut object, "game", ROOM_LEAVE_SCRIPT, 1_000);

    let join_path = |player: &str| {
        format!("/v1/valdr/game/EVALSHA/{join}/2/room%3A7%3Amembers/room%3A7%3Acount/{player}/2")
    };

    let (_, value) = get(&mut object, &join_path("alice"), 2_000);
    assert_eq!(value, json!({"result": "joined"}));
    let (_, value) = get(&mut object, &join_path("bob"), 2_001);
    assert_eq!(value, json!({"result": "joined"}));

    let (status, value) = get(&mut object, &join_path("carol"), 2_002);
    assert_ne!(status, 200, "room at capacity must reject the third player");
    assert!(
        value["error"].as_str().unwrap().starts_with("ROOMFULL"),
        "unexpected error: {value}"
    );

    let (_, value) = get(&mut object, &join_path("alice"), 2_003);
    assert_eq!(
        value,
        json!({"result": "already-joined"}),
        "rejoin is idempotent and does not double-count"
    );

    let (_, value) = get(
        &mut object,
        &format!("/v1/valdr/game/EVALSHA/{leave}/2/room%3A7%3Amembers/room%3A7%3Acount/bob"),
        2_004,
    );
    assert_eq!(value, json!({"result": "left"}));

    let (_, value) = get(&mut object, &join_path("carol"), 2_005);
    assert_eq!(
        value,
        json!({"result": "joined"}),
        "freed slot is claimable: {value}"
    );

    let storage = object.into_storage();
    let mut reopened = open(storage);
    let rejoin = load_script(&mut reopened, "game", ROOM_JOIN_SCRIPT, 3_000);
    let (status, value) = get(
        &mut reopened,
        &format!("/v1/valdr/game/EVALSHA/{rejoin}/2/room%3A7%3Amembers/room%3A7%3Acount/dave/2"),
        3_001,
    );
    assert_ne!(
        status, 200,
        "room occupancy must survive cold start: {value}"
    );
}

const TURN_MOVE_SCRIPT: &str = r#"
    local game = KEYS[1]
    local player = ARGV[1]
    local move = ARGV[2]
    local turn = redis.call('HGET', game, 'turn')
    if turn ~= player then
        return {err='WRONGTURN it is not your move'}
    end
    local opponent = redis.call('HGET', game, 'opponent')
    redis.call('HSET', game, 'last_move', move, 'turn', opponent, 'opponent', player)
    return {ok='moved'}
"#;

#[test]
fn turn_based_game_rejects_out_of_turn_moves() {
    let mut object = open(MemoryObjectStorage::default());
    let sha = load_script(&mut object, "game", TURN_MOVE_SCRIPT, 1_000);

    let (_, value) = get(
        &mut object,
        "/v1/valdr/game/HSET/match%3A1/turn/p1/opponent/p2",
        1_001,
    );
    assert_eq!(value, json!({"result": 2}));

    let move_path =
        |player: &str, mv: &str| format!("/v1/valdr/game/EVALSHA/{sha}/1/match%3A1/{player}/{mv}");

    let (status, value) = get(&mut object, &move_path("p2", "e5"), 1_002);
    assert_ne!(status, 200, "p2 cannot move first: {value}");
    assert!(value["error"].as_str().unwrap().starts_with("WRONGTURN"));

    let (_, value) = get(&mut object, &move_path("p1", "e4"), 1_003);
    assert_eq!(value, json!({"result": "moved"}));

    let (status, value) = get(&mut object, &move_path("p1", "d4"), 1_004);
    assert_ne!(status, 200, "p1 cannot move twice: {value}");

    let (_, value) = get(&mut object, &move_path("p2", "e5"), 1_005);
    assert_eq!(value, json!({"result": "moved"}));

    let (_, value) = get(
        &mut object,
        "/v1/valdr/game/HGET/match%3A1/last_move",
        1_006,
    );
    assert_eq!(value, json!({"result": "e5"}));
}

#[test]
fn leaderboard_top_n_and_rank_survive_cold_start() {
    let mut object = open(MemoryObjectStorage::default());

    let (status, value) = get(
        &mut object,
        "/v1/valdr/game/ZADD/board%3As1/120/alice/95/bob/140/carol/95/dave",
        1_000,
    );
    assert_eq!(status, 200, "ZADD failed: {value}");
    assert_eq!(value, json!({"result": 4}));

    let (_, value) = get(
        &mut object,
        "/v1/valdr/game/ZINCRBY/board%3As1/30/bob",
        1_001,
    );
    assert_eq!(value, json!({"result": "125"}), "bob wins a match");

    let (_, value) = get(
        &mut object,
        "/v1/valdr/game/ZRANGE/board%3As1/0/2/REV/WITHSCORES",
        1_002,
    );
    assert_eq!(
        value,
        json!({"result": ["carol", "140", "bob", "125", "alice", "120"]}),
        "top three by score, highest first"
    );

    let (_, value) = get(
        &mut object,
        "/v1/valdr/game/ZREVRANK/board%3As1/dave",
        1_003,
    );
    assert_eq!(value, json!({"result": 3}), "dave is fourth");

    let (_, value) = get(&mut object, "/v1/valdr/game/ZREM/board%3As1/alice", 1_004);
    assert_eq!(value, json!({"result": 1}));

    let storage = object.into_storage();
    let mut reopened = open(storage);
    let (_, value) = get(&mut reopened, "/v1/valdr/game/ZCARD/board%3As1", 2_000);
    assert_eq!(value, json!({"result": 3}), "board survives cold start");
    let (_, value) = get(
        &mut reopened,
        "/v1/valdr/game/ZRANGE/board%3As1/0/0/REV/WITHSCORES",
        2_001,
    );
    assert_eq!(
        value,
        json!({"result": ["carol", "140"]}),
        "leader intact after restore"
    );
}

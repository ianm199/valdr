//! Flash-sale inventory: oversell prevention via atomic edge state.
//!
//! The model is one Durable Object per SKU; that object owns the SKU's stock
//! counter and every outstanding reservation hold. Because Cloudflare
//! serializes requests to a single Durable Object and each Lua `EVALSHA` runs
//! atomically inside the Valdr engine, the read-decrement-write of a
//! reservation can never interleave with another buyer's. The decrement is a
//! single `INCRBY stock -qty` (there is no `DECR`/`HINCRBY` in this engine);
//! the `SOLDOUT` guard runs before it so stock never goes negative.
//!
//! Reservations are a two-phase commit. `RESERVE` decrements stock and writes a
//! TTL hold key keyed by the caller's `hold_id`; it is idempotent on that id so
//! a retried request returns the original `remaining` without decrementing
//! twice. `CONFIRM` finalizes a sale (the hold is deleted, the stock stays
//! down). `CANCEL` releases a hold (stock is restocked, the hold is deleted)
//! and is idempotent so a double cancel cannot double-restock.
//!
//! Reply shapes follow Redis Lua semantics, which the engine reproduces
//! faithfully: a Lua table carrying an `ok` field collapses to a bare status
//! string on the wire (the rest of the table is discarded), and a table
//! carrying an `err` field becomes a Redis error (a non-200 HTTP status with
//! the message in `error`). So the success replies that must also carry a
//! payload — `remaining`, `restocked` — are returned as plain Lua arrays
//! (`{'reserved', remaining}`) which survive intact as multi-bulk replies. The
//! payload-free successes (`CONFIRM`) and every failure stay in `{ok=...}` /
//! `{err=...}` status form.
//!
//! The TTL on the hold key models the reservation window: it is the lifetime of
//! a hold a buyer never confirms or cancels. True auto-release-on-expiry — stock
//! reclaimed the instant a hold lapses — would require a scheduled reclaim
//! (Durable Object alarms) and is out of scope here; the TTL provides the
//! window and the idempotency token, while explicit `CONFIRM`/`CANCEL` are the
//! finalizers this demo exercises.

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage};
use serde_json::{json, Value as JsonValue};

const RESERVE_SCRIPT: &str = r#"
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
"#;

const CONFIRM_SCRIPT: &str = r#"
    local hold_key = KEYS[1]
    if not redis.call('GET', hold_key) then
        return {err='NOHOLD'}
    end
    redis.call('DEL', hold_key)
    return {ok='confirmed'}
"#;

const CANCEL_SCRIPT: &str = r#"
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

/// Hold key the way the Worker route layer composes it: `hold:{sku}:{hold_id}`.
/// The SKU prefix keeps every SKU's holds in that SKU's Durable Object key
/// space; the percent-encoded colon (`%3A`) survives the route segment decode.
fn hold_key_path(hold_id: &str) -> String {
    format!("hold%3Aflash%3A{hold_id}")
}

fn stock_of(object: &mut EdgeObject<MemoryObjectStorage>, tenant: &str, now: u64) -> i64 {
    let (status, value) = get(object, &format!("/v1/valdr/{tenant}/GET/stock"), now);
    assert_eq!(status, 200, "stock GET failed: {value}");
    value["result"]
        .as_str()
        .expect("stock is stored as a string by SET/INCRBY")
        .parse()
        .expect("stock is an integer")
}

#[test]
fn reserve_counts_down_then_soldout_and_never_negative() {
    let mut object = open(MemoryObjectStorage::default());
    let reserve = load_script(&mut object, "sku", RESERVE_SCRIPT, 1_000);

    let (status, value) = get(&mut object, "/v1/valdr/sku/SET/stock/10", 1_001);
    assert_eq!(status, 200, "seed stock failed: {value}");
    assert_eq!(value, json!({"result": "OK"}));

    let reserve_path = |hold_id: &str| {
        format!(
            "/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{}/1/600000",
            hold_key_path(hold_id)
        )
    };

    for index in 0..10 {
        let hold_id = format!("buyer-{index}");
        let (status, value) = get(&mut object, &reserve_path(&hold_id), 2_000 + index);
        assert_eq!(status, 200, "reserve {hold_id} must succeed: {value}");
        assert_eq!(
            value,
            json!({"result": ["reserved", 9 - index as i64]}),
            "remaining must count down 9..0"
        );
        assert!(
            stock_of(&mut object, "sku", 2_100 + index) >= 0,
            "stock must never go negative mid-drain"
        );
    }

    let (status, value) = get(&mut object, &reserve_path("buyer-eleven"), 2_500);
    assert_ne!(status, 200, "the 11th buyer must be rejected: {value}");
    assert!(
        value["error"].as_str().unwrap().starts_with("SOLDOUT"),
        "unexpected error: {value}"
    );

    assert_eq!(
        stock_of(&mut object, "sku", 2_600),
        0,
        "stock lands at exactly 0, never negative"
    );
}

#[test]
fn reserve_is_idempotent_on_hold_id() {
    let mut object = open(MemoryObjectStorage::default());
    let reserve = load_script(&mut object, "sku", RESERVE_SCRIPT, 1_000);
    let (status, _) = get(&mut object, "/v1/valdr/sku/SET/stock/10", 1_001);
    assert_eq!(status, 200);

    let path = format!(
        "/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{}/1/600000",
        hold_key_path("buyer-1")
    );

    let (_, first) = get(&mut object, &path, 2_000);
    assert_eq!(first, json!({"result": ["reserved", 9]}));
    assert_eq!(stock_of(&mut object, "sku", 2_001), 9);

    let (status, retry) = get(&mut object, &path, 2_002);
    assert_eq!(status, 200, "idempotent retry must succeed: {retry}");
    assert_eq!(
        retry,
        json!({"result": ["reserved", 9]}),
        "retried hold id returns the original remaining"
    );
    assert_eq!(
        stock_of(&mut object, "sku", 2_003),
        9,
        "a retried reservation must not decrement again"
    );
}

#[test]
fn cancel_restocks_once_and_a_second_cancel_is_nohold() {
    let mut object = open(MemoryObjectStorage::default());
    let reserve = load_script(&mut object, "sku", RESERVE_SCRIPT, 1_000);
    let cancel = load_script(&mut object, "sku", CANCEL_SCRIPT, 1_000);
    let (status, _) = get(&mut object, "/v1/valdr/sku/SET/stock/10", 1_001);
    assert_eq!(status, 200);

    let hold = hold_key_path("buyer-1");
    let reserve_path = format!("/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{hold}/1/600000");
    let cancel_path = format!("/v1/valdr/sku/EVALSHA/{cancel}/2/stock/{hold}");

    let (_, value) = get(&mut object, &reserve_path, 2_000);
    assert_eq!(value, json!({"result": ["reserved", 9]}));
    assert_eq!(stock_of(&mut object, "sku", 2_001), 9);

    let (status, value) = get(&mut object, &cancel_path, 2_002);
    assert_eq!(status, 200, "cancel must succeed: {value}");
    assert_eq!(value, json!({"result": ["cancelled", 1]}));
    assert_eq!(
        stock_of(&mut object, "sku", 2_003),
        10,
        "cancel restocks the held unit"
    );

    let (status, value) = get(&mut object, &cancel_path, 2_004);
    assert_ne!(status, 200, "second cancel must be NOHOLD: {value}");
    assert!(value["error"].as_str().unwrap().starts_with("NOHOLD"));
    assert_eq!(
        stock_of(&mut object, "sku", 2_005),
        10,
        "a second cancel must not double-restock"
    );

    let (status, value) = get(&mut object, &reserve_path, 2_006);
    assert_eq!(
        status, 200,
        "the restocked unit is reservable again: {value}"
    );
    assert_eq!(value, json!({"result": ["reserved", 9]}));
}

#[test]
fn confirm_finalizes_a_sale_and_double_confirm_is_nohold() {
    let mut object = open(MemoryObjectStorage::default());
    let reserve = load_script(&mut object, "sku", RESERVE_SCRIPT, 1_000);
    let confirm = load_script(&mut object, "sku", CONFIRM_SCRIPT, 1_000);
    let (status, _) = get(&mut object, "/v1/valdr/sku/SET/stock/10", 1_001);
    assert_eq!(status, 200);

    let hold = hold_key_path("buyer-1");
    let reserve_path = format!("/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{hold}/1/600000");
    let confirm_path = format!("/v1/valdr/sku/EVALSHA/{confirm}/1/{hold}");

    let (_, value) = get(&mut object, &reserve_path, 2_000);
    assert_eq!(value, json!({"result": ["reserved", 9]}));

    let (status, value) = get(&mut object, &confirm_path, 2_001);
    assert_eq!(status, 200, "confirm must succeed: {value}");
    assert_eq!(value, json!({"result": "confirmed"}));
    assert_eq!(
        stock_of(&mut object, "sku", 2_002),
        9,
        "a confirmed sale keeps the stock decremented"
    );

    let (status, value) = get(&mut object, &confirm_path, 2_003);
    assert_ne!(status, 200, "the hold is gone after confirm: {value}");
    assert!(value["error"].as_str().unwrap().starts_with("NOHOLD"));
    assert_eq!(
        stock_of(&mut object, "sku", 2_004),
        9,
        "double confirm changes nothing"
    );
}

#[test]
fn stock_and_outstanding_holds_survive_cold_start() {
    let mut object = open(MemoryObjectStorage::default());
    let reserve = load_script(&mut object, "sku", RESERVE_SCRIPT, 1_000);
    let (status, _) = get(&mut object, "/v1/valdr/sku/SET/stock/10", 1_001);
    assert_eq!(status, 200);

    let reserve_path = |hold_id: &str| {
        format!(
            "/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{}/1/600000",
            hold_key_path(hold_id)
        )
    };

    for index in 0..3 {
        let (status, value) = get(
            &mut object,
            &reserve_path(&format!("pre-{index}")),
            2_000 + index,
        );
        assert_eq!(status, 200, "pre-restart reserve failed: {value}");
        assert_eq!(value, json!({"result": ["reserved", 9 - index as i64]}));
    }
    assert_eq!(stock_of(&mut object, "sku", 2_100), 7);

    let storage = object.into_storage();
    let mut reopened = open(storage);

    assert_eq!(
        stock_of(&mut reopened, "sku", 3_000),
        7,
        "stock survives the cold start"
    );

    let cancel = load_script(&mut reopened, "sku", CANCEL_SCRIPT, 3_001);
    let (status, value) = get(
        &mut reopened,
        &format!(
            "/v1/valdr/sku/EVALSHA/{cancel}/2/stock/{}",
            hold_key_path("pre-0")
        ),
        3_002,
    );
    assert_eq!(status, 200, "an outstanding hold survives reopen: {value}");
    assert_eq!(value, json!({"result": ["cancelled", 1]}));
    assert_eq!(
        stock_of(&mut reopened, "sku", 3_003),
        8,
        "cancelling a restored hold restocks correctly"
    );

    let reserve = load_script(&mut reopened, "sku", RESERVE_SCRIPT, 3_004);
    let (status, value) = get(
        &mut reopened,
        &format!(
            "/v1/valdr/sku/EVALSHA/{reserve}/2/stock/{}/1/600000",
            hold_key_path("post-1")
        ),
        3_005,
    );
    assert_eq!(
        status, 200,
        "reserve after reopen still respects count: {value}"
    );
    assert_eq!(value, json!({"result": ["reserved", 7]}));
    assert!(
        stock_of(&mut reopened, "sku", 3_006) >= 0,
        "stock is never negative across a cold start"
    );
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from dogfood_scenarios.rs (demo scenario test)
//   target_crate:  edgestash-demo
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         flash-sale inventory oversell-prevention scenarios over the
//                  EdgeObject HTTP route layer; RESERVE/CONFIRM/CANCEL Lua.
// ──────────────────────────────────────────────────────────────────────────

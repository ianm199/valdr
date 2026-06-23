//! The read-only `/v1/_debug/<tenant>` keyspace-dump route.
//!
//! A host adapter that runs a Rust/workers-rs Durable Object cannot use the
//! wrangler Local Explorer to inspect its storage (the Explorer talks to a DO
//! over JS-native RPC, which a fetch-style workers-rs DO does not expose). This
//! route is the provider-neutral substitute: it returns the engine's whole-DB
//! snapshot JSON for the tenant so the dashboard (or `curl`) can render the
//! keyspace. It is gated behind `with_debug_allowed(true)` so a deployment does
//! not expose tenant keyspaces unless explicitly opted in.

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage, Policy};

/// An eager object holding one limiter policy hash and one raw string key, with
/// the debug route enabled or disabled as requested.
fn populated(allow_debug: bool) -> EdgeObject<MemoryObjectStorage> {
    let mut object = EdgeObject::open(MemoryObjectStorage::default())
        .unwrap()
        .with_debug_allowed(allow_debug);
    object
        .install_policy("acme", Policy::token_bucket(10, 5, 1_000, 60_000))
        .unwrap();
    let set = object.handle_http(EdgeHttpRequest::get("/v1/valdr/acme/SET/note/hello", 1_000));
    assert_eq!(set.status, 200, "seed SET should succeed");
    object
}

#[test]
fn debug_route_dumps_the_tenant_keyspace_as_a_snapshot() {
    let mut object = populated(true);
    let before = object.shard().mutation_epoch();

    let resp = object.handle_http(EdgeHttpRequest::get("/v1/_debug/acme", 2_000));
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_type, "application/json");

    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["format"], "valdr-engine-snapshot");
    assert_eq!(body["version"], 1);
    let keys = body["keys"].as_array().expect("snapshot carries a keys array");

    let names: Vec<String> = keys
        .iter()
        .map(|k| decode_hex(k["key"].as_str().unwrap()))
        .collect();
    assert!(
        names.iter().any(|name| name == "note"),
        "the raw SET key should appear in the dump: {names:?}"
    );
    assert!(
        names.iter().any(|name| name.ends_with(":policy")),
        "the limiter policy hash should appear in the dump: {names:?}"
    );

    assert_eq!(
        object.shard().mutation_epoch(),
        before,
        "the debug dump must be read-only and never mutate engine state"
    );
}

#[test]
fn debug_route_is_disabled_by_default() {
    let mut object = populated(false);
    let resp = object.handle_http(EdgeHttpRequest::get("/v1/_debug/acme", 2_000));
    assert_eq!(
        resp.status, 403,
        "the debug route must be off unless a host explicitly enables it"
    );
}

#[test]
fn debug_route_rejects_non_get() {
    let mut object = populated(true);
    let resp = object.handle_http(EdgeHttpRequest::post("/v1/_debug/acme", b"{}", 2_000));
    assert_eq!(resp.status, 405, "the debug route is read-only (GET)");
}

/// Decode the lowercase-hex key names the snapshot emits back to UTF-8.
fn decode_hex(hex: &str) -> String {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    String::from_utf8(bytes).unwrap()
}

//! `RedisObject` — the runtime value held in a Redis database slot.
//!
//! STUB. Minimal enum so translator packets that need to construct/
//! match `RedisObject` can proceed. Inner encodings (ListPack, IntSet,
//! SkipList, etc.) are deferred to Phase 4 — for now each variant
//! holds a simple Rust collection.

use redis_types::RedisString;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum RedisObject {
    String(RedisString),
    List(Vec<RedisString>),
    Hash(HashMap<RedisString, RedisString>),
    Set(std::collections::HashSet<RedisString>),
    /// (member, score) pairs. Phase 4 replaces with skiplist + hash.
    ZSet(Vec<(RedisString, f64)>),
    /// Streams — Phase 5. Placeholder unit variant.
    Stream,
}

impl RedisObject {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisObject::String(_) => "string",
            RedisObject::List(_)   => "list",
            RedisObject::Hash(_)   => "hash",
            RedisObject::Set(_)    => "set",
            RedisObject::ZSet(_)   => "zset",
            RedisObject::Stream    => "stream",
        }
    }

    pub fn from_string(s: RedisString) -> Self {
        RedisObject::String(s)
    }

    pub fn as_string(&self) -> Option<&RedisString> {
        match self {
            RedisObject::String(s) => Some(s),
            _ => None,
        }
    }

    /// Byte-view of the object when it carries a `RedisString` payload.
    ///
    /// STUB — Phase B placeholder. Returns the inner `RedisString` bytes for
    /// `RedisObject::String`, an empty slice otherwise. Translated callers use
    /// this where the C source called `obj->ptr` directly on a `RedisString`-
    /// backed object.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RedisObject::String(s) => s.as_bytes(),
            _ => &[],
        }
    }

    /// Expiration deadline in absolute milliseconds, if one is set.
    ///
    /// STUB — Phase B placeholder. Expiry tracking is owned by `RedisDb`'s
    /// expires-dict in the real Valkey implementation; this method is
    /// invoked by translated expire-cycle code and returns `None`
    /// (meaning "no expiry") until that storage lands in Phase 4.
    pub fn expire_ms(&self) -> Option<i64> {
        None
    }
}

// ──────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (stub for translate-loop unblock)
//   target_crate:  redis-core
//   confidence:    skeleton
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Minimal enum; encoding sub-variants land in Phase 4. type_name + as_string suffice for first command packets.
// ──────────────────────────────────────────────────────────────────────

//! `RedisDb` — one logical database (keyspace).
//!
//! STUB. HashMap-backed for now; kvstore + slot-based addressing land
//! in Phase 4. Provides the lookup_key_read/write/add/delete/exists
//! shape that command implementations call against. Expiry not yet
//! tracked.

use crate::client::ClientId;
use crate::object::RedisObject;
use redis_types::RedisString;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct RedisDb {
    /// Database index (0..N-1 for standalone server).
    pub id: u32,
    /// Main keyspace.
    dict: HashMap<RedisString, RedisObject>,
    /// Per-key watcher list for MULTI/EXEC CAS.
    ///
    /// STUB — Phase B placeholder mapping a watched key to the list of client
    /// ids watching it. The full intrusive-list structure of C's
    /// `watched_keys` hashtable lands in Phase 3.
    watched_keys: HashMap<RedisString, Vec<ClientId>>,
}

impl RedisDb {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            dict: HashMap::new(),
            watched_keys: HashMap::new(),
        }
    }

    /// Database id as `i32` (matches the C `redisDb.id` type used by callers).
    pub fn id(&self) -> i32 {
        self.id as i32
    }

    /// Look up `key` for read.
    ///
    /// Accepts anything that views as bytes (`&RedisString`, `&[u8]`,
    /// `&Vec<u8>`), since translated command code mixes all three.
    pub fn lookup_key_read(&self, key: impl AsRef<[u8]>) -> Option<&RedisObject> {
        let k = RedisString::from_bytes(key.as_ref());
        self.dict.get(&k)
    }

    pub fn lookup_key_write(&mut self, key: impl AsRef<[u8]>) -> Option<&mut RedisObject> {
        let k = RedisString::from_bytes(key.as_ref());
        self.dict.get_mut(&k)
    }

    /// Add a new key. Returns false if the key already existed.
    pub fn add(&mut self, key: RedisString, value: RedisObject) -> bool {
        !self.dict.contains_key(&key) && self.dict.insert(key, value).is_none()
    }

    /// Insert (overwrite if present). Returns the previous value if any.
    pub fn insert(&mut self, key: RedisString, value: RedisObject) -> Option<RedisObject> {
        self.dict.insert(key, value)
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> bool {
        let k = RedisString::from_bytes(key.as_ref());
        self.dict.remove(&k).is_some()
    }

    pub fn exists(&self, key: impl AsRef<[u8]>) -> bool {
        let k = RedisString::from_bytes(key.as_ref());
        self.dict.contains_key(&k)
    }

    /// Mark `key` as modified (for WATCH / replication / keyspace notify).
    ///
    /// STUB — Phase B placeholder; full signaling lives in db.c's
    /// `signalModifiedKey` (replication backlog, WATCH dirty bit, key-space
    /// notifications) and lands in Phase 3+.
    pub fn signal_modified(&mut self, _key: impl AsRef<[u8]>) {
        // TODO(port): wire replication / WATCH / keyspace notify.
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    pub fn clear(&mut self) {
        self.dict.clear()
    }

    /// Whether `key` has an active expiry that is already in the past.
    ///
    /// STUB — Phase B placeholder. Expiry tracking is owned by the future
    /// `expires` dict on `RedisDb`; returns `false` until that lands.
    pub fn key_is_expired(&self, _key: &RedisObject) -> bool {
        false
    }

    /// Whether this db has any watched keys at all.
    ///
    /// STUB — Phase B placeholder backed by the per-key watcher map.
    pub fn watched_keys_is_empty(&self) -> bool {
        self.watched_keys.is_empty()
    }

    /// Register `client_id` as a watcher of `key` in this db.
    ///
    /// STUB — Phase B placeholder. Real implementation in `multi.c` chains
    /// watchedKey records into both client- and db-side intrusive lists.
    pub fn watched_keys_add_client(&mut self, key: &RedisObject, client_id: ClientId) {
        let k = RedisString::from_bytes(key.as_bytes());
        self.watched_keys.entry(k).or_default().push(client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_lookup_delete_round_trip() {
        let mut db = RedisDb::new(0);
        let key = RedisString::from_bytes(b"foo");
        assert!(!db.exists(&key));
        assert!(db.add(key.clone(), RedisObject::from_string(RedisString::from_bytes(b"bar"))));
        assert!(db.exists(&key));
        assert_eq!(db.lookup_key_read(&key).and_then(|o| o.as_string()).map(|s| s.as_bytes().to_vec()),
                   Some(b"bar".to_vec()));
        assert!(db.delete(&key));
        assert!(!db.exists(&key));
    }

    #[test]
    fn add_returns_false_when_key_present() {
        let mut db = RedisDb::new(0);
        let key = RedisString::from_bytes(b"k");
        db.add(key.clone(), RedisObject::from_string(RedisString::from_bytes(b"v1")));
        assert!(!db.add(key, RedisObject::from_string(RedisString::from_bytes(b"v2"))));
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
//   notes:         HashMap-backed keyspace. Expiry, kvstore slots, notifications deferred.
// ──────────────────────────────────────────────────────────────────────

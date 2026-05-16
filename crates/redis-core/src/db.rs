//! `RedisDb` — one logical database (keyspace).
//!
//! STUB. HashMap-backed for now; kvstore + slot-based addressing land
//! in Phase 4. Provides the lookup_key_read/write/add/delete/exists
//! shape that command implementations call against. Expiry not yet
//! tracked.

use crate::object::RedisObject;
use redis_types::RedisString;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct RedisDb {
    /// Database index (0..N-1 for standalone server).
    pub id: u32,
    /// Main keyspace.
    dict: HashMap<RedisString, RedisObject>,
}

impl RedisDb {
    pub fn new(id: u32) -> Self {
        Self { id, dict: HashMap::new() }
    }

    pub fn lookup_key_read(&self, key: &RedisString) -> Option<&RedisObject> {
        self.dict.get(key)
    }

    pub fn lookup_key_write(&mut self, key: &RedisString) -> Option<&mut RedisObject> {
        self.dict.get_mut(key)
    }

    /// Add a new key. Returns false if the key already existed.
    pub fn add(&mut self, key: RedisString, value: RedisObject) -> bool {
        !self.dict.contains_key(&key) && self.dict.insert(key, value).is_none()
    }

    /// Insert (overwrite if present). Returns the previous value if any.
    pub fn insert(&mut self, key: RedisString, value: RedisObject) -> Option<RedisObject> {
        self.dict.insert(key, value)
    }

    pub fn delete(&mut self, key: &RedisString) -> bool {
        self.dict.remove(key).is_some()
    }

    pub fn exists(&self, key: &RedisString) -> bool {
        self.dict.contains_key(key)
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

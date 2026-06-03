//! Segmented copy-on-write keyspace map.
//!
//! This is the first production step toward forkless snapshots. The live
//! keyspace remains hash-table based, but the table is split into Arc-owned
//! segments. A snapshot clones segment roots in O(segment_count); the first
//! write to a shared segment clones only that segment.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use redis_types::RedisString;

use crate::keyspace_cow::{new_snapshot_guard, record_segment_clone, KeyspaceCowSnapshotGuard};
use crate::object::RedisObject;

pub const DEFAULT_KEYSPACE_SEGMENTS: usize = 1024;

#[derive(Debug)]
pub struct KeyspaceMap {
    segments: Vec<Arc<HashMap<RedisString, RedisObject>>>,
    len: usize,
}

impl Default for KeyspaceMap {
    fn default() -> Self {
        Self::with_segment_count(DEFAULT_KEYSPACE_SEGMENTS)
    }
}

impl KeyspaceMap {
    pub fn with_segment_count(segment_count: usize) -> Self {
        let segment_count = segment_count.max(1);
        let segments = (0..segment_count)
            .map(|_| Arc::new(HashMap::new()))
            .collect();
        Self { segments, len: 0 }
    }

    pub fn snapshot(&self) -> KeyspaceMapSnapshot {
        KeyspaceMapSnapshot {
            segments: self.segments.clone(),
            len: self.len,
            _guard: new_snapshot_guard(),
        }
    }

    pub fn insert(&mut self, key: RedisString, value: RedisObject) -> Option<RedisObject> {
        let idx = self.segment_index(&key);
        let segment = self.make_segment_mut(idx);
        let old = segment.insert(key, value);
        if old.is_none() {
            self.len += 1;
        }
        old
    }

    pub fn get(&self, key: &RedisString) -> Option<&RedisObject> {
        self.segments[self.segment_index(key)].get(key)
    }

    pub fn get_mut(&mut self, key: &RedisString) -> Option<&mut RedisObject> {
        let idx = self.segment_index(key);
        if !self.segments[idx].contains_key(key) {
            return None;
        }
        self.make_segment_mut(idx).get_mut(key)
    }

    pub fn remove(&mut self, key: &RedisString) -> Option<RedisObject> {
        let idx = self.segment_index(key);
        if !self.segments[idx].contains_key(key) {
            return None;
        }
        let removed = self.make_segment_mut(idx).remove(key);
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    pub fn contains_key(&self, key: &RedisString) -> bool {
        self.segments[self.segment_index(key)].contains_key(key)
    }

    pub fn clear(&mut self) {
        for idx in 0..self.segments.len() {
            if !self.segments[idx].is_empty() {
                self.make_segment_mut(idx).clear();
            }
        }
        self.len = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RedisString, &RedisObject)> {
        self.segments.iter().flat_map(|segment| segment.iter())
    }

    pub fn keys(&self) -> impl Iterator<Item = &RedisString> {
        self.iter().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = &RedisObject> {
        self.iter().map(|(_, value)| value)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn segment_index(&self, key: &RedisString) -> usize {
        segment_index_for(self.segments.len(), key)
    }

    fn make_segment_mut(&mut self, idx: usize) -> &mut HashMap<RedisString, RedisObject> {
        let before = Arc::as_ptr(&self.segments[idx]);
        let segment = Arc::make_mut(&mut self.segments[idx]);
        if !std::ptr::eq(before, segment as *const HashMap<RedisString, RedisObject>) {
            // Keep the normal write path to one make_mut and a pointer check.
            // Clone timing is left as zero so INFO telemetry does not add an
            // Instant/refcount branch to every key mutation.
            record_segment_clone(segment.len(), estimated_segment_clone_bytes(segment), 0);
        }
        segment
    }
}

#[derive(Clone, Debug)]
pub struct KeyspaceMapSnapshot {
    segments: Vec<Arc<HashMap<RedisString, RedisObject>>>,
    len: usize,
    _guard: Arc<KeyspaceCowSnapshotGuard>,
}

impl KeyspaceMapSnapshot {
    pub fn iter(&self) -> impl Iterator<Item = (&RedisString, &RedisObject)> {
        self.segments.iter().flat_map(|segment| segment.iter())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

fn segment_index_for(segment_count: usize, key: &RedisString) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % segment_count
}

fn estimated_segment_clone_bytes(segment: &HashMap<RedisString, RedisObject>) -> usize {
    let entry_bytes = segment
        .len()
        .saturating_mul(mem::size_of::<(RedisString, RedisObject)>());
    let obvious_payload_bytes = segment
        .values()
        .filter_map(RedisObject::as_string_bytes)
        .map(<[u8]>::len)
        .sum::<usize>();
    entry_bytes.saturating_add(obvious_payload_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace_cow::{reset_for_test, stats_snapshot, test_counter_lock};

    fn key(bytes: &[u8]) -> RedisString {
        RedisString::from_bytes(bytes)
    }

    fn obj(bytes: &[u8]) -> RedisObject {
        RedisObject::new_string(bytes)
    }

    #[test]
    fn snapshot_lifetime_updates_active_counters() {
        let _guard = test_counter_lock();
        reset_for_test();

        let mut map = KeyspaceMap::with_segment_count(1);
        map.insert(key(b"a"), obj(b"one"));

        let snapshot = map.snapshot();
        let cloned_snapshot = snapshot.clone();
        let stats = stats_snapshot();
        assert_eq!(stats.snapshot_starts, 1);
        assert_eq!(stats.active_snapshots, 1);
        assert_eq!(stats.snapshot_drops, 0);

        drop(snapshot);
        let stats = stats_snapshot();
        assert_eq!(stats.active_snapshots, 1);
        assert_eq!(stats.snapshot_drops, 0);

        drop(cloned_snapshot);
        let stats = stats_snapshot();
        assert_eq!(stats.active_snapshots, 0);
        assert_eq!(stats.snapshot_drops, 1);
    }

    #[test]
    fn first_write_to_shared_segment_counts_one_clone() {
        let _guard = test_counter_lock();
        reset_for_test();

        let mut map = KeyspaceMap::with_segment_count(1);
        map.insert(key(b"a"), obj(b"one"));
        map.insert(key(b"b"), obj(b"two"));
        let snapshot = map.snapshot();

        map.insert(key(b"c"), obj(b"three"));
        let stats = stats_snapshot();
        assert_eq!(stats.segment_clones, 1);
        assert_eq!(stats.segment_clone_keys, 2);
        assert!(stats.segment_clone_estimated_bytes >= 6);
        assert_eq!(stats.segment_clone_max_keys, 2);

        map.insert(key(b"d"), obj(b"four"));
        let stats = stats_snapshot();
        assert_eq!(stats.segment_clones, 1);

        drop(snapshot);
    }

    #[test]
    fn misses_do_not_clone_shared_segments() {
        let _guard = test_counter_lock();
        reset_for_test();

        let mut map = KeyspaceMap::with_segment_count(1);
        map.insert(key(b"a"), obj(b"one"));
        let snapshot = map.snapshot();

        assert!(map.get_mut(&key(b"missing")).is_none());
        assert!(map.remove(&key(b"missing")).is_none());
        let stats = stats_snapshot();
        assert_eq!(stats.segment_clones, 0);

        drop(snapshot);
    }

    #[test]
    fn metadata_touch_to_shared_segment_counts_clone_once() {
        let _guard = test_counter_lock();
        reset_for_test();

        let mut map = KeyspaceMap::with_segment_count(1);
        let key = key(b"a");
        map.insert(key.clone(), obj(b"one"));
        let snapshot = map.snapshot();

        map.get_mut(&key).expect("key exists").lru = 42;
        let stats = stats_snapshot();
        assert_eq!(stats.segment_clones, 1);
        assert_eq!(stats.segment_clone_keys, 1);

        map.get_mut(&key).expect("key still exists").expire = 99;
        let stats = stats_snapshot();
        assert_eq!(stats.segment_clones, 1);

        drop(snapshot);
    }
}

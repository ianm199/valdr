//! Segmented copy-on-write keyspace map.
//!
//! This is the first production step toward forkless snapshots. The live
//! keyspace remains hash-table based, but the table is split into Arc-owned
//! segments. A snapshot clones segment roots in O(segment_count); the first
//! write to a shared segment clones only that segment.

use std::collections::HashMap;
use std::sync::Arc;

use redis_types::RedisString;

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
        }
    }

    pub fn insert(&mut self, key: RedisString, value: RedisObject) -> Option<RedisObject> {
        let idx = self.segment_index(&key);
        let segment = Arc::make_mut(&mut self.segments[idx]);
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
        Arc::make_mut(&mut self.segments[idx]).get_mut(key)
    }

    pub fn remove(&mut self, key: &RedisString) -> Option<RedisObject> {
        let idx = self.segment_index(key);
        if !self.segments[idx].contains_key(key) {
            return None;
        }
        let removed = Arc::make_mut(&mut self.segments[idx]).remove(key);
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    pub fn contains_key(&self, key: &RedisString) -> bool {
        self.segments[self.segment_index(key)].contains_key(key)
    }

    pub fn clear(&mut self) {
        for segment in &mut self.segments {
            if !segment.is_empty() {
                Arc::make_mut(segment).clear();
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
}

#[derive(Clone, Debug)]
pub struct KeyspaceMapSnapshot {
    segments: Vec<Arc<HashMap<RedisString, RedisObject>>>,
    len: usize,
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

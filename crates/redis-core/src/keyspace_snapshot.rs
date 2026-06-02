//! Point-in-time keyspace snapshot facade.
//!
//! This packet deliberately keeps the current deep-copy implementation. The
//! value is centralizing the snapshot contract and metadata before replacing
//! the backing representation with structural sharing.

use std::time::Duration;

use redis_types::RedisString;

use crate::db::RedisDb;
use crate::keyspace_map::KeyspaceMapSnapshot;
use crate::object::RedisObject;

#[derive(Clone, Debug)]
pub struct KeyspaceSnapshotDb {
    id: u32,
    entries: KeyspaceSnapshotEntries,
}

#[derive(Clone, Debug)]
enum KeyspaceSnapshotEntries {
    Owned(Vec<(RedisString, RedisObject)>),
    Shared(KeyspaceMapSnapshot),
}

impl KeyspaceSnapshotDb {
    pub fn new(id: u32, entries: Vec<(RedisString, RedisObject)>) -> Self {
        Self {
            id,
            entries: KeyspaceSnapshotEntries::Owned(entries),
        }
    }

    pub fn from_keyspace(id: u32, snapshot: KeyspaceMapSnapshot) -> Self {
        Self {
            id,
            entries: KeyspaceSnapshotEntries::Shared(snapshot),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn len(&self) -> usize {
        match &self.entries {
            KeyspaceSnapshotEntries::Owned(entries) => entries.len(),
            KeyspaceSnapshotEntries::Shared(snapshot) => snapshot.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cloned_entries(&self) -> Vec<(RedisString, RedisObject)> {
        match &self.entries {
            KeyspaceSnapshotEntries::Owned(entries) => entries.clone(),
            KeyspaceSnapshotEntries::Shared(snapshot) => snapshot
                .iter()
                .map(|(key, object)| (key.clone(), object.clone()))
                .collect(),
        }
    }

    fn into_entries(self) -> Vec<(RedisString, RedisObject)> {
        match self.entries {
            KeyspaceSnapshotEntries::Owned(entries) => entries,
            KeyspaceSnapshotEntries::Shared(snapshot) => snapshot
                .iter()
                .map(|(key, object)| (key.clone(), object.clone()))
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KeyspaceSnapshotStats {
    pub db_count: usize,
    pub key_count: usize,
    pub capture_micros: u64,
}

#[derive(Clone, Debug)]
pub struct KeyspaceSnapshot {
    dbs: Vec<KeyspaceSnapshotDb>,
    stats: KeyspaceSnapshotStats,
}

impl KeyspaceSnapshot {
    pub fn new(dbs: Vec<KeyspaceSnapshotDb>, capture_duration: Duration) -> Self {
        let key_count = dbs.iter().map(KeyspaceSnapshotDb::len).sum();
        let capture_micros = capture_duration.as_micros().min(u128::from(u64::MAX)) as u64;
        let stats = KeyspaceSnapshotStats {
            db_count: dbs.len(),
            key_count,
            capture_micros,
        };
        Self { dbs, stats }
    }

    pub fn stats(&self) -> KeyspaceSnapshotStats {
        self.stats
    }

    pub fn db_count(&self) -> usize {
        self.stats.db_count
    }

    pub fn key_count(&self) -> usize {
        self.stats.key_count
    }

    pub fn capture_micros(&self) -> u64 {
        self.stats.capture_micros
    }

    pub fn to_dbs(&self) -> Vec<RedisDb> {
        self.dbs
            .iter()
            .map(|snapshot_db| {
                let mut db = RedisDb::from_snapshot(snapshot_db.cloned_entries());
                db.id = snapshot_db.id;
                db
            })
            .collect()
    }

    pub fn into_dbs(self) -> Vec<RedisDb> {
        self.dbs
            .into_iter()
            .map(|snapshot_db| {
                let id = snapshot_db.id;
                let mut db = RedisDb::from_snapshot(snapshot_db.into_entries());
                db.id = id;
                db
            })
            .collect()
    }
}

use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::mem;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static KEY_CLONE_BYTES: AtomicU64 = AtomicU64::new(0);
static ENTRY_CLONE_BYTES: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_CLONE_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Eq)]
struct Key(Vec<u8>);

impl Key {
    fn from_id(id: usize) -> Self {
        Self(key_bytes(id))
    }
}

impl Clone for Key {
    fn clone(&self) -> Self {
        KEY_CLONE_BYTES.fetch_add(self.0.len() as u64, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug)]
struct Payload {
    bytes: Vec<u8>,
}

impl Payload {
    fn new(value_bytes: usize, id: usize, salt: u8) -> Self {
        let mut bytes = vec![0u8; value_bytes];
        for (idx, b) in bytes.iter_mut().enumerate() {
            *b = salt.wrapping_add(id as u8).wrapping_add(idx as u8);
        }
        Self { bytes }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn first(&self) -> u8 {
        self.bytes.first().copied().unwrap_or(0)
    }

    fn mutate_one_byte(&mut self, salt: u8) {
        if let Some(first) = self.bytes.first_mut() {
            *first = first.wrapping_add(salt).wrapping_add(1);
        }
    }

    fn incr_counter(&mut self, delta: u64) -> u64 {
        let mut bytes = [0u8; 8];
        let copy_len = self.bytes.len().min(bytes.len());
        bytes[..copy_len].copy_from_slice(&self.bytes[..copy_len]);
        let next = u64::from_le_bytes(bytes).wrapping_add(delta);
        if self.bytes.len() < bytes.len() {
            self.bytes.resize(bytes.len(), 0);
        }
        self.bytes[..8].copy_from_slice(&next.to_le_bytes());
        next
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        PAYLOAD_CLONE_BYTES.fetch_add(self.bytes.len() as u64, Ordering::Relaxed);
        Self {
            bytes: self.bytes.clone(),
        }
    }
}

#[derive(Debug)]
struct Entry {
    lru: u64,
    expire: i64,
    payload: Arc<Payload>,
}

impl Entry {
    fn new(value_bytes: usize, id: usize, salt: u8) -> Self {
        Self {
            lru: id as u64,
            expire: -1,
            payload: Arc::new(Payload::new(value_bytes, id, salt)),
        }
    }

    fn observe(&self) -> usize {
        self.payload.len()
            ^ self.payload.first() as usize
            ^ self.lru as usize
            ^ self.expire as usize
    }

    fn touch_metadata(&mut self, salt: u8) {
        self.lru = self.lru.wrapping_add(u64::from(salt).wrapping_add(1));
        self.expire = self.expire.wrapping_add(i64::from(salt) + 1);
    }

    fn replace_payload(&mut self, value_bytes: usize, id: usize, salt: u8) {
        self.payload = Arc::new(Payload::new(value_bytes, id, salt));
    }

    fn mutate_payload(&mut self, salt: u8) {
        Arc::make_mut(&mut self.payload).mutate_one_byte(salt);
    }

    fn incr_payload(&mut self, delta: u64) -> u64 {
        Arc::make_mut(&mut self.payload).incr_counter(delta)
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        ENTRY_CLONE_BYTES.fetch_add(mem::size_of::<Entry>() as u64, Ordering::Relaxed);
        Self {
            lru: self.lru,
            expire: self.expire,
            payload: self.payload.clone(),
        }
    }
}

#[derive(Clone)]
struct SegmentedDeepCow {
    segments: Vec<Arc<HashMap<Key, Payload>>>,
    value_bytes: usize,
    hash_route: bool,
}

impl SegmentedDeepCow {
    fn with_keys(keys: usize, value_bytes: usize, segment_count: usize, hash_route: bool) -> Self {
        let segment_count = segment_count.max(1).min(keys.max(1));
        let mut segments: Vec<HashMap<Key, Payload>> =
            (0..segment_count).map(|_| HashMap::new()).collect();
        for id in 0..keys {
            let key = Key::from_id(id);
            let segment = segment_for_key(segment_count, id, key.0.as_slice(), hash_route);
            segments[segment].insert(key, Payload::new(value_bytes, id, 0x19));
        }
        Self {
            segments: segments.into_iter().map(Arc::new).collect(),
            value_bytes,
            hash_route,
        }
    }

    fn segment_for(&self, id: usize, key: &[u8]) -> usize {
        segment_for_key(self.segments.len(), id, key, self.hash_route)
    }

    fn get(&self, id: usize, key: &[u8]) -> usize {
        self.segments[self.segment_for(id, key)]
            .get(key)
            .map(|v| v.len() ^ v.first() as usize)
            .unwrap_or(0)
    }

    fn replace(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        map.insert(Key(key.to_vec()), Payload::new(self.value_bytes, id, salt));
        1
    }

    fn mutate(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(payload) => {
                payload.mutate_one_byte(salt);
                1
            }
            None => 0,
        }
    }

    fn incr(&mut self, id: usize, key: &[u8], delta: u64) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(payload) => payload.incr_counter(delta) as usize,
            None => {
                map.insert(
                    Key(key.to_vec()),
                    Payload::new(self.value_bytes, id, delta as u8),
                );
                0
            }
        }
    }

    fn iter_sum(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| {
                segment
                    .iter()
                    .map(|(k, v)| k.0.len() ^ v.len() ^ v.first() as usize)
                    .sum::<usize>()
            })
            .sum()
    }
}

#[derive(Clone)]
struct SegmentedCow {
    segments: Vec<Arc<HashMap<Key, Arc<Payload>>>>,
    value_bytes: usize,
    hash_route: bool,
}

impl SegmentedCow {
    fn with_keys(keys: usize, value_bytes: usize, segment_count: usize, hash_route: bool) -> Self {
        let segment_count = segment_count.max(1).min(keys.max(1));
        let mut segments: Vec<HashMap<Key, Arc<Payload>>> =
            (0..segment_count).map(|_| HashMap::new()).collect();
        for id in 0..keys {
            let key = Key::from_id(id);
            let segment = segment_for_key(segment_count, id, key.0.as_slice(), hash_route);
            segments[segment].insert(key, Arc::new(Payload::new(value_bytes, id, 0x31)));
        }
        Self {
            segments: segments.into_iter().map(Arc::new).collect(),
            value_bytes,
            hash_route,
        }
    }

    fn segment_for(&self, id: usize, key: &[u8]) -> usize {
        segment_for_key(self.segments.len(), id, key, self.hash_route)
    }

    fn get(&self, id: usize, key: &[u8]) -> usize {
        self.segments[self.segment_for(id, key)]
            .get(key)
            .map(|v| v.len() ^ v.first() as usize)
            .unwrap_or(0)
    }

    fn replace(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        map.insert(
            Key(key.to_vec()),
            Arc::new(Payload::new(self.value_bytes, id, salt)),
        );
        1
    }

    fn mutate(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(payload) => {
                Arc::make_mut(payload).mutate_one_byte(salt);
                1
            }
            None => 0,
        }
    }

    fn incr(&mut self, id: usize, key: &[u8], delta: u64) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(payload) => Arc::make_mut(payload).incr_counter(delta) as usize,
            None => {
                map.insert(
                    Key(key.to_vec()),
                    Arc::new(Payload::new(self.value_bytes, id, delta as u8)),
                );
                0
            }
        }
    }

    fn iter_sum(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| {
                segment
                    .iter()
                    .map(|(k, v)| k.0.len() ^ v.len() ^ v.first() as usize)
                    .sum::<usize>()
            })
            .sum()
    }
}

#[derive(Clone)]
struct SegmentedEntryCow {
    segments: Vec<Arc<HashMap<Key, Entry>>>,
    value_bytes: usize,
    hash_route: bool,
}

impl SegmentedEntryCow {
    fn with_keys(keys: usize, value_bytes: usize, segment_count: usize, hash_route: bool) -> Self {
        let segment_count = segment_count.max(1).min(keys.max(1));
        let mut segments: Vec<HashMap<Key, Entry>> =
            (0..segment_count).map(|_| HashMap::new()).collect();
        for id in 0..keys {
            let key = Key::from_id(id);
            let segment = segment_for_key(segment_count, id, key.0.as_slice(), hash_route);
            segments[segment].insert(key, Entry::new(value_bytes, id, 0x51));
        }
        Self {
            segments: segments.into_iter().map(Arc::new).collect(),
            value_bytes,
            hash_route,
        }
    }

    fn segment_for(&self, id: usize, key: &[u8]) -> usize {
        segment_for_key(self.segments.len(), id, key, self.hash_route)
    }

    fn get(&self, id: usize, key: &[u8]) -> usize {
        self.segments[self.segment_for(id, key)]
            .get(key)
            .map(Entry::observe)
            .unwrap_or(0)
    }

    fn touch_metadata(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(entry) => {
                entry.touch_metadata(salt);
                1
            }
            None => 0,
        }
    }

    fn replace(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(entry) => entry.replace_payload(self.value_bytes, id, salt),
            None => {
                map.insert(Key(key.to_vec()), Entry::new(self.value_bytes, id, salt));
            }
        }
        1
    }

    fn mutate(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(entry) => {
                entry.mutate_payload(salt);
                1
            }
            None => 0,
        }
    }

    fn incr(&mut self, id: usize, key: &[u8], delta: u64) -> usize {
        let segment = self.segment_for(id, key);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(entry) => entry.incr_payload(delta) as usize,
            None => {
                map.insert(
                    Key(key.to_vec()),
                    Entry::new(self.value_bytes, id, delta as u8),
                );
                0
            }
        }
    }

    fn iter_sum(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| {
                segment
                    .iter()
                    .map(|(k, v)| k.0.len() ^ v.observe())
                    .sum::<usize>()
            })
            .sum()
    }
}

#[derive(Debug, Clone)]
struct Config {
    keys: usize,
    value_bytes: usize,
    read_ops: usize,
    write_ops: usize,
    segments: usize,
    variants: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keys: 100_000,
            value_bytes: 64,
            read_ops: 200_000,
            write_ops: 10_000,
            segments: 1024,
            variants: vec![
                "deep".to_string(),
                "arc".to_string(),
                "entry".to_string(),
                "seg_deep_hash".to_string(),
                "seg".to_string(),
                "seg_hash".to_string(),
                "seg_entry_hash".to_string(),
                "im".to_string(),
            ],
        }
    }
}

#[derive(Debug)]
struct Row {
    variant: &'static str,
    phase: &'static str,
    ops: usize,
    elapsed: Duration,
    key_clone_bytes: u64,
    entry_clone_bytes: u64,
    payload_clone_bytes: u64,
    rss_kb: u64,
    rss_delta_kb: i64,
    checksum: usize,
}

fn main() {
    let cfg = parse_args();
    let key_bytes_cache: Vec<Vec<u8>> = (0..cfg.keys).map(key_bytes).collect();
    let read_ids = make_ids(cfg.read_ops, cfg.keys, 0x1234_5678_9abc_def0);
    let write_ids = make_ids(cfg.write_ops, cfg.keys, 0x0ddc_0ffe_e15e_d00d);

    println!(
        "variant\tkeys\tvalue_bytes\tsegments\tphase\tops\telapsed_ms\tns_per_op\tkey_clone_mb\tentry_clone_mb\tpayload_clone_mb\trss_kb\trss_delta_kb\tchecksum"
    );

    for variant in &cfg.variants {
        match variant.as_str() {
            "deep" => run_deep(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            "arc" => run_arc(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            "entry" => run_entry(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            "seg_deep" => run_segmented_deep(
                "seg_deep",
                false,
                &cfg,
                &key_bytes_cache,
                &read_ids,
                &write_ids,
            ),
            "seg_deep_hash" => run_segmented_deep(
                "seg_deep_hash",
                true,
                &cfg,
                &key_bytes_cache,
                &read_ids,
                &write_ids,
            ),
            "seg" => run_segmented("seg", false, &cfg, &key_bytes_cache, &read_ids, &write_ids),
            "seg_hash" => run_segmented(
                "seg_hash",
                true,
                &cfg,
                &key_bytes_cache,
                &read_ids,
                &write_ids,
            ),
            "seg_entry" => run_segmented_entry(
                "seg_entry",
                false,
                &cfg,
                &key_bytes_cache,
                &read_ids,
                &write_ids,
            ),
            "seg_entry_hash" => run_segmented_entry(
                "seg_entry_hash",
                true,
                &cfg,
                &key_bytes_cache,
                &read_ids,
                &write_ids,
            ),
            "im" => run_im(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            other => eprintln!("unknown variant ignored: {other}"),
        }
    }
}

fn run_deep(cfg: &Config, keys: &[Vec<u8>], read_ids: &[usize], write_ids: &[usize]) {
    let live = build_deep(cfg.keys, cfg.value_bytes);
    measure_snapshot("deep", cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| iter_deep(&snapshot))
        },
    );
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_deep(&live, keys, read_ids))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_deep(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_deep(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = build_deep(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "deep",
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_deep(&mut live, keys, write_ids))
        },
    );
}

fn run_arc(cfg: &Config, keys: &[Vec<u8>], read_ids: &[usize], write_ids: &[usize]) {
    let live = build_arc(cfg.keys, cfg.value_bytes);
    measure_snapshot("arc", cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| iter_arc(&snapshot))
        },
    );
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_arc(&live, keys, read_ids))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_arc(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_arc(&mut live, keys, write_ids))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_arc(&mut live, keys, write_ids))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_arc(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_arc(&mut live, keys, write_ids))
        },
    );

    let mut live = build_arc(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "arc",
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_arc(&mut live, keys, write_ids))
        },
    );
}

fn run_entry(cfg: &Config, keys: &[Vec<u8>], read_ids: &[usize], write_ids: &[usize]) {
    let live = build_entry(cfg.keys, cfg.value_bytes);
    measure_snapshot("entry", cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| iter_entry(&snapshot))
        },
    );
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_entry(&live, keys, read_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "metadata_live",
            ops: write_ids.len(),
            ..measure(|| bench_metadata_entry(&mut live, keys, write_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_entry(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_entry(&mut live, keys, write_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_entry(&mut live, keys, write_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "metadata_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_metadata_entry(&mut live, keys, write_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_entry(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_entry(&mut live, keys, write_ids))
        },
    );

    let mut live = build_entry(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "entry",
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_entry(&mut live, keys, write_ids))
        },
    );
}

fn run_segmented_deep(
    variant: &'static str,
    hash_route: bool,
    cfg: &Config,
    keys: &[Vec<u8>],
    read_ids: &[usize],
    write_ids: &[usize],
) {
    let live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    measure_snapshot(variant, cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| snapshot.iter_sum())
        },
    );
    emit(
        cfg,
        Row {
            variant,
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_segmented_deep(&live, keys, read_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented_deep(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedDeepCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented_deep(&mut live, keys, write_ids))
        },
    );
}

fn run_segmented(
    variant: &'static str,
    hash_route: bool,
    cfg: &Config,
    keys: &[Vec<u8>],
    read_ids: &[usize],
    write_ids: &[usize],
) {
    let live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    measure_snapshot(variant, cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| snapshot.iter_sum())
        },
    );
    emit(
        cfg,
        Row {
            variant,
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_segmented(&live, keys, read_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented(&mut live, keys, write_ids))
        },
    );
}

fn run_segmented_entry(
    variant: &'static str,
    hash_route: bool,
    cfg: &Config,
    keys: &[Vec<u8>],
    read_ids: &[usize],
    write_ids: &[usize],
) {
    let live = SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    measure_snapshot(variant, cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| snapshot.iter_sum())
        },
    );
    emit(
        cfg,
        Row {
            variant,
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_segmented_entry(&live, keys, read_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "metadata_live",
            ops: write_ids.len(),
            ..measure(|| bench_metadata_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "metadata_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_metadata_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented_entry(&mut live, keys, write_ids))
        },
    );

    let mut live =
        SegmentedEntryCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments, hash_route);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant,
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_segmented_entry(&mut live, keys, write_ids))
        },
    );
}

fn run_im(cfg: &Config, keys: &[Vec<u8>], read_ids: &[usize], write_ids: &[usize]) {
    let live = build_im(cfg.keys, cfg.value_bytes);
    measure_snapshot("im", cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| iter_im(&snapshot))
        },
    );
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_im(&live, keys, read_ids))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "incr_live",
            ops: write_ids.len(),
            ..measure(|| bench_incr_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );

    let mut live = build_im(cfg.keys, cfg.value_bytes);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "im",
            phase: "incr_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_incr_im(&mut live, keys, write_ids, cfg.value_bytes))
        },
    );
}

fn build_deep(keys: usize, value_bytes: usize) -> HashMap<Key, Payload> {
    let mut map = HashMap::with_capacity(keys);
    for id in 0..keys {
        map.insert(Key::from_id(id), Payload::new(value_bytes, id, 0x11));
    }
    map
}

fn build_arc(keys: usize, value_bytes: usize) -> HashMap<Key, Arc<Payload>> {
    let mut map = HashMap::with_capacity(keys);
    for id in 0..keys {
        map.insert(
            Key::from_id(id),
            Arc::new(Payload::new(value_bytes, id, 0x21)),
        );
    }
    map
}

fn build_entry(keys: usize, value_bytes: usize) -> HashMap<Key, Entry> {
    let mut map = HashMap::with_capacity(keys);
    for id in 0..keys {
        map.insert(Key::from_id(id), Entry::new(value_bytes, id, 0x31));
    }
    map
}

fn build_im(keys: usize, value_bytes: usize) -> im::HashMap<Key, Arc<Payload>> {
    let mut map = im::HashMap::new();
    for id in 0..keys {
        map.insert(
            Key::from_id(id),
            Arc::new(Payload::new(value_bytes, id, 0x41)),
        );
    }
    map
}

fn bench_get_deep(map: &HashMap<Key, Payload>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        if let Some(value) = map.get(keys[id].as_slice()) {
            sum = sum.wrapping_add(value.len() ^ value.first() as usize);
        }
    }
    sum
}

fn bench_replace_deep(
    map: &mut HashMap<Key, Payload>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        map.insert(
            Key(keys[id].clone()),
            Payload::new(value_bytes, id, (op & 0xff) as u8),
        );
        sum = sum.wrapping_add(1);
    }
    sum
}

fn bench_mutate_deep(map: &mut HashMap<Key, Payload>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            value.mutate_one_byte((op & 0xff) as u8);
            sum = sum.wrapping_add(1);
        }
    }
    sum
}

fn bench_incr_deep(map: &mut HashMap<Key, Payload>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            sum = sum.wrapping_add(value.incr_counter((op as u64) + 1) as usize);
        }
    }
    sum
}

fn bench_get_arc(map: &HashMap<Key, Arc<Payload>>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        if let Some(value) = map.get(keys[id].as_slice()) {
            sum = sum.wrapping_add(value.len() ^ value.first() as usize);
        }
    }
    sum
}

fn bench_replace_arc(
    map: &mut HashMap<Key, Arc<Payload>>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        map.insert(
            Key(keys[id].clone()),
            Arc::new(Payload::new(value_bytes, id, (op & 0xff) as u8)),
        );
        sum = sum.wrapping_add(1);
    }
    sum
}

fn bench_mutate_arc(
    map: &mut HashMap<Key, Arc<Payload>>,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            Arc::make_mut(value).mutate_one_byte((op & 0xff) as u8);
            sum = sum.wrapping_add(1);
        }
    }
    sum
}

fn bench_incr_arc(map: &mut HashMap<Key, Arc<Payload>>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            sum = sum.wrapping_add(Arc::make_mut(value).incr_counter((op as u64) + 1) as usize);
        }
    }
    sum
}

fn bench_get_entry(map: &HashMap<Key, Entry>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        if let Some(entry) = map.get(keys[id].as_slice()) {
            sum = sum.wrapping_add(entry.observe());
        }
    }
    sum
}

fn bench_metadata_entry(map: &mut HashMap<Key, Entry>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(entry) = map.get_mut(keys[id].as_slice()) {
            entry.touch_metadata((op & 0xff) as u8);
            sum = sum.wrapping_add(1);
        }
    }
    sum
}

fn bench_replace_entry(
    map: &mut HashMap<Key, Entry>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(entry) = map.get_mut(keys[id].as_slice()) {
            entry.replace_payload(value_bytes, id, (op & 0xff) as u8);
        } else {
            map.insert(
                Key(keys[id].clone()),
                Entry::new(value_bytes, id, (op & 0xff) as u8),
            );
        }
        sum = sum.wrapping_add(1);
    }
    sum
}

fn bench_mutate_entry(map: &mut HashMap<Key, Entry>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(entry) = map.get_mut(keys[id].as_slice()) {
            entry.mutate_payload((op & 0xff) as u8);
            sum = sum.wrapping_add(1);
        }
    }
    sum
}

fn bench_incr_entry(map: &mut HashMap<Key, Entry>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        if let Some(entry) = map.get_mut(keys[id].as_slice()) {
            sum = sum.wrapping_add(entry.incr_payload((op as u64) + 1) as usize);
        }
    }
    sum
}

fn bench_get_segmented_deep(model: &SegmentedDeepCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        sum = sum.wrapping_add(model.get(id, keys[id].as_slice()));
    }
    sum
}

fn bench_replace_segmented_deep(
    model: &mut SegmentedDeepCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.replace(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_mutate_segmented_deep(
    model: &mut SegmentedDeepCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.mutate(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_incr_segmented_deep(
    model: &mut SegmentedDeepCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.incr(id, keys[id].as_slice(), (op as u64) + 1));
    }
    sum
}

fn bench_get_segmented(model: &SegmentedCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        sum = sum.wrapping_add(model.get(id, keys[id].as_slice()));
    }
    sum
}

fn bench_replace_segmented(model: &mut SegmentedCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.replace(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_mutate_segmented(model: &mut SegmentedCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.mutate(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_incr_segmented(model: &mut SegmentedCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.incr(id, keys[id].as_slice(), (op as u64) + 1));
    }
    sum
}

fn bench_get_segmented_entry(model: &SegmentedEntryCow, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        sum = sum.wrapping_add(model.get(id, keys[id].as_slice()));
    }
    sum
}

fn bench_metadata_segmented_entry(
    model: &mut SegmentedEntryCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.touch_metadata(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_replace_segmented_entry(
    model: &mut SegmentedEntryCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.replace(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_mutate_segmented_entry(
    model: &mut SegmentedEntryCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.mutate(id, keys[id].as_slice(), (op & 0xff) as u8));
    }
    sum
}

fn bench_incr_segmented_entry(
    model: &mut SegmentedEntryCow,
    keys: &[Vec<u8>],
    ids: &[usize],
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        sum = sum.wrapping_add(model.incr(id, keys[id].as_slice(), (op as u64) + 1));
    }
    sum
}

fn bench_get_im(map: &im::HashMap<Key, Arc<Payload>>, keys: &[Vec<u8>], ids: &[usize]) -> usize {
    let mut sum = 0usize;
    for &id in ids {
        if let Some(value) = map.get(keys[id].as_slice()) {
            sum = sum.wrapping_add(value.len() ^ value.first() as usize);
        }
    }
    sum
}

fn bench_replace_im(
    map: &mut im::HashMap<Key, Arc<Payload>>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        map.insert(
            Key(keys[id].clone()),
            Arc::new(Payload::new(value_bytes, id, (op & 0xff) as u8)),
        );
        sum = sum.wrapping_add(1);
    }
    sum
}

fn bench_mutate_im(
    map: &mut im::HashMap<Key, Arc<Payload>>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        let salt = (op & 0xff) as u8;
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            Arc::make_mut(value).mutate_one_byte(salt);
            sum = sum.wrapping_add(1);
        } else {
            map.insert(
                Key(keys[id].clone()),
                Arc::new(Payload::new(value_bytes, id, salt)),
            );
        }
    }
    sum
}

fn bench_incr_im(
    map: &mut im::HashMap<Key, Arc<Payload>>,
    keys: &[Vec<u8>],
    ids: &[usize],
    value_bytes: usize,
) -> usize {
    let mut sum = 0usize;
    for (op, &id) in ids.iter().enumerate() {
        let delta = (op as u64) + 1;
        if let Some(value) = map.get_mut(keys[id].as_slice()) {
            sum = sum.wrapping_add(Arc::make_mut(value).incr_counter(delta) as usize);
        } else {
            map.insert(
                Key(keys[id].clone()),
                Arc::new(Payload::new(value_bytes, id, delta as u8)),
            );
        }
    }
    sum
}

fn iter_deep(map: &HashMap<Key, Payload>) -> usize {
    map.iter()
        .map(|(k, v)| k.0.len() ^ v.len() ^ v.first() as usize)
        .sum()
}

fn iter_arc(map: &HashMap<Key, Arc<Payload>>) -> usize {
    map.iter()
        .map(|(k, v)| k.0.len() ^ v.len() ^ v.first() as usize)
        .sum()
}

fn iter_entry(map: &HashMap<Key, Entry>) -> usize {
    map.iter().map(|(k, v)| k.0.len() ^ v.observe()).sum()
}

fn iter_im(map: &im::HashMap<Key, Arc<Payload>>) -> usize {
    map.iter()
        .map(|(k, v)| k.0.len() ^ v.len() ^ v.first() as usize)
        .sum()
}

fn measure_snapshot<T>(variant: &'static str, cfg: &Config, mut snapshot: impl FnMut() -> T) {
    let row = Row {
        variant,
        phase: "snapshot",
        ops: 1,
        ..measure(|| {
            let snap = snapshot();
            std::mem::size_of_val(&snap)
        })
    };
    emit(cfg, row);
}

fn measure(mut f: impl FnMut() -> usize) -> Row {
    reset_clone_counters();
    let rss_before = current_rss_kb();
    let start = Instant::now();
    let checksum = f();
    let elapsed = start.elapsed();
    let rss_after = current_rss_kb();
    Row {
        variant: "",
        phase: "",
        ops: 0,
        elapsed,
        key_clone_bytes: KEY_CLONE_BYTES.load(Ordering::Relaxed),
        entry_clone_bytes: ENTRY_CLONE_BYTES.load(Ordering::Relaxed),
        payload_clone_bytes: PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed),
        rss_kb: rss_after,
        rss_delta_kb: rss_after as i64 - rss_before as i64,
        checksum,
    }
}

fn reset_clone_counters() {
    KEY_CLONE_BYTES.store(0, Ordering::Relaxed);
    ENTRY_CLONE_BYTES.store(0, Ordering::Relaxed);
    PAYLOAD_CLONE_BYTES.store(0, Ordering::Relaxed);
}

fn emit(cfg: &Config, row: Row) {
    let ns_per_op = if row.ops == 0 {
        0.0
    } else {
        row.elapsed.as_nanos() as f64 / row.ops as f64
    };
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.6}\t{:.6}\t{:.6}\t{}\t{}\t{}",
        row.variant,
        cfg.keys,
        cfg.value_bytes,
        cfg.segments,
        row.phase,
        row.ops,
        row.elapsed.as_secs_f64() * 1000.0,
        ns_per_op,
        bytes_to_mb(row.key_clone_bytes),
        bytes_to_mb(row.entry_clone_bytes),
        bytes_to_mb(row.payload_clone_bytes),
        row.rss_kb,
        row.rss_delta_kb,
        row.checksum
    );
}

fn segment_for_key(segment_count: usize, id: usize, key: &[u8], hash_route: bool) -> usize {
    if !hash_route {
        return id % segment_count;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % segment_count
}

fn key_bytes(id: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&(id as u64).to_le_bytes());
    bytes.extend_from_slice(&(!id as u64).to_le_bytes());
    bytes
}

fn make_ids(count: usize, modulo: usize, seed: u64) -> Vec<usize> {
    let modulo = modulo.max(1);
    let mut x = seed;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        ids.push((x as usize) % modulo);
    }
    ids
}

fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn current_rss_kb() -> u64 {
    let pid = std::process::id().to_string();
    let Ok(output) = Command::new("ps").args(["-o", "rss=", "-p", &pid]).output() else {
        return 0;
    };
    if !output.status.success() {
        return 0;
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    raw.trim().parse().unwrap_or(0)
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--keys" => cfg.keys = parse_next(&mut args, "--keys"),
            "--value-bytes" => cfg.value_bytes = parse_next(&mut args, "--value-bytes"),
            "--read-ops" => cfg.read_ops = parse_next(&mut args, "--read-ops"),
            "--write-ops" => cfg.write_ops = parse_next(&mut args, "--write-ops"),
            "--segments" => cfg.segments = parse_next(&mut args, "--segments"),
            "--variants" => {
                let raw: String = parse_next_string(&mut args, "--variants");
                cfg.variants = raw
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_ascii_lowercase())
                    .collect();
            }
            "--help" | "-h" => {
                print_help_and_exit();
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_help_and_exit();
            }
        }
    }
    cfg
}

fn parse_next<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T {
    let raw = parse_next_string(args, flag);
    raw.parse()
        .unwrap_or_else(|_| panic!("invalid value for {flag}: {raw}"))
}

fn parse_next_string(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| panic!("missing value for {flag}"))
}

fn print_help_and_exit() -> ! {
    eprintln!(
        "usage: keyspace-cow-model [--keys N] [--value-bytes N] [--read-ops N] [--write-ops N] [--segments N] [--variants deep,arc,entry,seg_deep,seg_deep_hash,seg,seg_hash,seg_entry,seg_entry_hash,im]"
    );
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn keys(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(key_bytes).collect()
    }

    #[test]
    fn deep_snapshot_clone_counts_keys_and_payloads() {
        let _guard = TEST_LOCK.lock().unwrap();
        let live = build_deep(3, 5);
        reset_clone_counters();

        let snapshot = live.clone();

        assert_eq!(snapshot.len(), 3);
        assert_eq!(KEY_CLONE_BYTES.load(Ordering::Relaxed), 3 * 16);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 3 * 5);
    }

    #[test]
    fn arc_snapshot_keeps_old_payload_after_live_mutation() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(4);
        let mut live = build_arc(4, 8);
        let snapshot = live.clone();
        let before = snapshot.get(keys[0].as_slice()).unwrap().first();
        reset_clone_counters();

        assert_eq!(bench_mutate_arc(&mut live, &keys, &[0]), 1);

        assert_ne!(live.get(keys[0].as_slice()).unwrap().first(), before);
        assert_eq!(snapshot.get(keys[0].as_slice()).unwrap().first(), before);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn entry_snapshot_clones_keys_and_entries_but_not_payloads() {
        let _guard = TEST_LOCK.lock().unwrap();
        let live = build_entry(3, 5);
        reset_clone_counters();

        let snapshot = live.clone();

        assert_eq!(snapshot.len(), 3);
        assert_eq!(KEY_CLONE_BYTES.load(Ordering::Relaxed), 3 * 16);
        assert_eq!(
            ENTRY_CLONE_BYTES.load(Ordering::Relaxed),
            3 * mem::size_of::<Entry>() as u64
        );
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn entry_metadata_update_under_snapshot_does_not_clone_payload() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(4);
        let mut live = build_entry(4, 8);
        let snapshot = live.clone();
        let before = snapshot.get(keys[0].as_slice()).unwrap().observe();
        reset_clone_counters();

        assert_eq!(bench_metadata_entry(&mut live, &keys, &[0]), 1);

        assert_ne!(live.get(keys[0].as_slice()).unwrap().observe(), before);
        assert_eq!(snapshot.get(keys[0].as_slice()).unwrap().observe(), before);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn segmented_snapshot_keeps_old_segment_after_live_replace() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedCow::with_keys(8, 8, 4, false);
        let snapshot = live.clone();
        let before = snapshot.get(0, keys[0].as_slice());
        reset_clone_counters();

        assert_eq!(live.replace(0, keys[0].as_slice(), 0x99), 1);

        assert_ne!(live.get(0, keys[0].as_slice()), before);
        assert_eq!(snapshot.get(0, keys[0].as_slice()), before);
        assert!(KEY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn segmented_deep_snapshot_clones_payloads_in_touched_segment() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedDeepCow::with_keys(8, 8, 4, false);
        let snapshot = live.clone();
        let before = snapshot.get(0, keys[0].as_slice());
        reset_clone_counters();

        assert_eq!(live.mutate(0, keys[0].as_slice(), 7), 1);

        assert_ne!(live.get(0, keys[0].as_slice()), before);
        assert_eq!(snapshot.get(0, keys[0].as_slice()), before);
        assert_eq!(KEY_CLONE_BYTES.load(Ordering::Relaxed), 2 * 16);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 2 * 8);
    }

    #[test]
    fn segmented_hash_snapshot_keeps_old_segment_after_live_incr() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedCow::with_keys(8, 8, 4, true);
        let snapshot = live.clone();
        let before = snapshot.get(3, keys[3].as_slice());
        reset_clone_counters();

        assert_ne!(live.incr(3, keys[3].as_slice(), 7), 0);

        assert_ne!(live.get(3, keys[3].as_slice()), before);
        assert_eq!(snapshot.get(3, keys[3].as_slice()), before);
        assert!(KEY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn segmented_entry_hash_metadata_clone_keeps_payload_shared() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedEntryCow::with_keys(8, 8, 4, true);
        let snapshot = live.clone();
        let before = snapshot.get(3, keys[3].as_slice());
        reset_clone_counters();

        assert_eq!(live.touch_metadata(3, keys[3].as_slice(), 7), 1);

        assert_ne!(live.get(3, keys[3].as_slice()), before);
        assert_eq!(snapshot.get(3, keys[3].as_slice()), before);
        assert!(KEY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
        assert!(ENTRY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn segmented_entry_hash_payload_mutation_clones_touched_payload() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedEntryCow::with_keys(8, 8, 4, true);
        let snapshot = live.clone();
        let before = snapshot.get(3, keys[3].as_slice());
        reset_clone_counters();

        assert_eq!(live.mutate(3, keys[3].as_slice(), 7), 1);

        assert_ne!(live.get(3, keys[3].as_slice()), before);
        assert_eq!(snapshot.get(3, keys[3].as_slice()), before);
        assert!(KEY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
        assert!(ENTRY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn im_snapshot_keeps_old_payload_after_live_mutation() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(4);
        let mut live = build_im(4, 8);
        let snapshot = live.clone();
        let before = snapshot.get(keys[0].as_slice()).unwrap().first();
        reset_clone_counters();

        assert_eq!(bench_mutate_im(&mut live, &keys, &[0], 8), 1);

        assert_ne!(live.get(keys[0].as_slice()).unwrap().first(), before);
        assert_eq!(snapshot.get(keys[0].as_slice()).unwrap().first(), before);
        assert_eq!(PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed), 8);
    }
}

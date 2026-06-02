use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static KEY_CLONE_BYTES: AtomicU64 = AtomicU64::new(0);
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
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        PAYLOAD_CLONE_BYTES.fetch_add(self.bytes.len() as u64, Ordering::Relaxed);
        Self {
            bytes: self.bytes.clone(),
        }
    }
}

#[derive(Clone)]
struct SegmentedCow {
    segments: Vec<Arc<HashMap<Key, Arc<Payload>>>>,
    value_bytes: usize,
}

impl SegmentedCow {
    fn with_keys(keys: usize, value_bytes: usize, segment_count: usize) -> Self {
        let segment_count = segment_count.max(1).min(keys.max(1));
        let mut segments: Vec<HashMap<Key, Arc<Payload>>> =
            (0..segment_count).map(|_| HashMap::new()).collect();
        for id in 0..keys {
            let segment = id % segment_count;
            segments[segment].insert(
                Key::from_id(id),
                Arc::new(Payload::new(value_bytes, id, 0x31)),
            );
        }
        Self {
            segments: segments.into_iter().map(Arc::new).collect(),
            value_bytes,
        }
    }

    fn segment_for(&self, id: usize) -> usize {
        id % self.segments.len()
    }

    fn get(&self, id: usize, key: &[u8]) -> usize {
        self.segments[self.segment_for(id)]
            .get(key)
            .map(|v| v.len() ^ v.first() as usize)
            .unwrap_or(0)
    }

    fn replace(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id);
        let map = Arc::make_mut(&mut self.segments[segment]);
        map.insert(
            Key(key.to_vec()),
            Arc::new(Payload::new(self.value_bytes, id, salt)),
        );
        1
    }

    fn mutate(&mut self, id: usize, key: &[u8], salt: u8) -> usize {
        let segment = self.segment_for(id);
        let map = Arc::make_mut(&mut self.segments[segment]);
        match map.get_mut(key) {
            Some(payload) => {
                Arc::make_mut(payload).mutate_one_byte(salt);
                1
            }
            None => 0,
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
                "seg".to_string(),
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
    payload_clone_bytes: u64,
    checksum: usize,
}

fn main() {
    let cfg = parse_args();
    let key_bytes_cache: Vec<Vec<u8>> = (0..cfg.keys).map(key_bytes).collect();
    let read_ids = make_ids(cfg.read_ops, cfg.keys, 0x1234_5678_9abc_def0);
    let write_ids = make_ids(cfg.write_ops, cfg.keys, 0x0ddc_0ffe_e15e_d00d);

    println!(
        "variant\tkeys\tvalue_bytes\tsegments\tphase\tops\telapsed_ms\tns_per_op\tkey_clone_mb\tpayload_clone_mb\tchecksum"
    );

    for variant in &cfg.variants {
        match variant.as_str() {
            "deep" => run_deep(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            "arc" => run_arc(&cfg, &key_bytes_cache, &read_ids, &write_ids),
            "seg" => run_segmented(&cfg, &key_bytes_cache, &read_ids, &write_ids),
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
}

fn run_segmented(cfg: &Config, keys: &[Vec<u8>], read_ids: &[usize], write_ids: &[usize]) {
    let live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments);
    measure_snapshot("seg", cfg, || live.clone());
    let snapshot = live.clone();
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "iter_snapshot",
            ops: cfg.keys,
            ..measure(|| snapshot.iter_sum())
        },
    );
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "get_live",
            ops: read_ids.len(),
            ..measure(|| bench_get_segmented(&live, keys, read_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments);
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "replace_live",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments);
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "mutate_live",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "replace_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_replace_segmented(&mut live, keys, write_ids))
        },
    );

    let mut live = SegmentedCow::with_keys(cfg.keys, cfg.value_bytes, cfg.segments);
    let _held = live.clone();
    emit(
        cfg,
        Row {
            variant: "seg",
            phase: "mutate_held_snapshot",
            ops: write_ids.len(),
            ..measure(|| bench_mutate_segmented(&mut live, keys, write_ids))
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
    let start = Instant::now();
    let checksum = f();
    let elapsed = start.elapsed();
    Row {
        variant: "",
        phase: "",
        ops: 0,
        elapsed,
        key_clone_bytes: KEY_CLONE_BYTES.load(Ordering::Relaxed),
        payload_clone_bytes: PAYLOAD_CLONE_BYTES.load(Ordering::Relaxed),
        checksum,
    }
}

fn reset_clone_counters() {
    KEY_CLONE_BYTES.store(0, Ordering::Relaxed);
    PAYLOAD_CLONE_BYTES.store(0, Ordering::Relaxed);
}

fn emit(cfg: &Config, row: Row) {
    let ns_per_op = if row.ops == 0 {
        0.0
    } else {
        row.elapsed.as_nanos() as f64 / row.ops as f64
    };
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.6}\t{:.6}\t{}",
        row.variant,
        cfg.keys,
        cfg.value_bytes,
        cfg.segments,
        row.phase,
        row.ops,
        row.elapsed.as_secs_f64() * 1000.0,
        ns_per_op,
        bytes_to_mb(row.key_clone_bytes),
        bytes_to_mb(row.payload_clone_bytes),
        row.checksum
    );
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
        "usage: keyspace-cow-model [--keys N] [--value-bytes N] [--read-ops N] [--write-ops N] [--segments N] [--variants deep,arc,seg,im]"
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
    fn segmented_snapshot_keeps_old_segment_after_live_replace() {
        let _guard = TEST_LOCK.lock().unwrap();
        let keys = keys(8);
        let mut live = SegmentedCow::with_keys(8, 8, 4);
        let snapshot = live.clone();
        let before = snapshot.get(0, keys[0].as_slice());
        reset_clone_counters();

        assert_eq!(live.replace(0, keys[0].as_slice(), 0x99), 1);

        assert_ne!(live.get(0, keys[0].as_slice()), before);
        assert_eq!(snapshot.get(0, keys[0].as_slice()), before);
        assert!(KEY_CLONE_BYTES.load(Ordering::Relaxed) > 0);
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

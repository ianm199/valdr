//! Fast in-memory POC: does a flat `Vec` (listpack-style) beat `HashMap` for the
//! small-collection hot path that the default benchmark hammers?
//!
//! The benchmark issues `SADD myset element:__rand_int__` / `HSET myhash ...`
//! WITHOUT `-r`, so the member/field is the same literal every call: a
//! ONE-ELEMENT collection that Valkey stores as a `listpack` (flat, no hashing)
//! while we run it through `HashMap`/`HashSet` (SipHash + bucket touch per op).
//!
//! This compares the two representations head-to-head, in process, with no
//! server — so we can validate the win and find the promotion threshold in <1s
//! before changing any production code. Run:
//!
//!   cargo test --release -p redis-core --test small_encoding_poc -- --nocapture
//!
//! The hot op is "operate on an EXISTING element" (SADD/HSET of a member that's
//! already present — the steady state after the first call), measured both as a
//! pure lookup and as an insert/replace.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use redis_types::RedisString;

fn rs(s: &str) -> RedisString {
    RedisString::from_bytes(s.as_bytes())
}

/// Flat, linear-scan map — the listpack-style small encoding under test.
struct VecMap {
    data: Vec<(RedisString, RedisString)>,
}

impl VecMap {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn insert(&mut self, field: RedisString, value: RedisString) -> Option<RedisString> {
        for (k, v) in self.data.iter_mut() {
            if k.as_bytes() == field.as_bytes() {
                return Some(std::mem::replace(v, value));
            }
        }
        self.data.push((field, value));
        None
    }

    fn get(&self, field: &RedisString) -> Option<&RedisString> {
        self.data
            .iter()
            .find(|(k, _)| k.as_bytes() == field.as_bytes())
            .map(|(_, v)| v)
    }
}

fn time_op<F: FnMut()>(iters: u64, mut f: F) -> f64 {
    for _ in 0..iters / 8 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

#[test]
fn small_encoding_poc() {
    let iters = 3_000_000u64;
    let value = rs("payloadvalue000000000000000000000000000000000000000000000000064b");

    println!("\n=== small-collection hot path: HashMap vs flat Vec (ns/op) ===");
    println!("    (probe = LAST element = worst case for the Vec linear scan)\n");
    println!(
        "{:>5} | {:>12} {:>12} {:>8} | {:>12} {:>12} {:>8}",
        "size", "hm_get", "vec_get", "x", "hm_upd", "vec_upd", "x"
    );
    println!("{}", "-".repeat(80));

    for &size in &[1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        let fields: Vec<RedisString> = (0..size).map(|i| rs(&format!("element:{i}"))).collect();

        let mut hm: HashMap<RedisString, RedisString> = HashMap::new();
        let mut vm = VecMap::new();
        for f in &fields {
            hm.insert(f.clone(), value.clone());
            vm.insert(f.clone(), value.clone());
        }

        let probe = fields[size - 1].clone();

        let hm_get = time_op(iters, || {
            black_box(hm.get(black_box(&probe)));
        });
        let vec_get = time_op(iters, || {
            black_box(vm.get(black_box(&probe)));
        });
        // Update-existing: both pay an identical key+value clone, so the delta
        // is the lookup-and-replace cost (what HSET does in steady state).
        let hm_upd = time_op(iters, || {
            black_box(hm.insert(probe.clone(), value.clone()));
        });
        let vec_upd = time_op(iters, || {
            black_box(vm.insert(probe.clone(), value.clone()));
        });

        println!(
            "{:>5} | {:>12.1} {:>12.1} {:>7.2}x | {:>12.1} {:>12.1} {:>7.2}x",
            size,
            hm_get,
            vec_get,
            hm_get / vec_get,
            hm_upd,
            vec_upd,
            hm_upd / vec_upd,
        );
    }
    println!("\n>1.00x in a column means the flat Vec is faster at that size.");
    println!("The crossover (where Vec stops winning) is the promotion threshold.\n");
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from object.rs encoding hot-path analysis
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         POC micro-bench comparing HashMap vs flat-Vec small encoding
//                  for the 1-element SADD/HSET benchmark hot path; not shipped.
// ──────────────────────────────────────────────────────────────────────────

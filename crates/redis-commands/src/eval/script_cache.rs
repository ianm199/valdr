//! Process-wide Lua script cache and SHA helpers.
//!
//! EVAL scripts are cached by lowercase 40-byte SHA-1 hex digest. Scripts
//! inserted by EVAL participate in the bounded LRU, while SCRIPT LOAD entries
//! are persistent and do not count toward that eviction limit.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

const EVAL_SCRIPT_CACHE_LIMIT: usize = 500;

#[derive(Clone)]
pub(super) struct CachedScript {
    pub(super) body: Vec<u8>,
    pub(super) evictable: bool,
}

#[derive(Default)]
pub(super) struct ScriptCache {
    pub(super) entries: HashMap<[u8; 40], CachedScript>,
    pub(super) lru: VecDeque<[u8; 40]>,
    evicted: u64,
}

impl ScriptCache {
    pub(super) fn touch_eval_script(&mut self, sha: [u8; 40]) {
        self.lru.retain(|existing| existing != &sha);
        self.lru.push_back(sha);
    }

    fn evict_eval_scripts_if_needed(&mut self) {
        while self
            .entries
            .values()
            .filter(|entry| entry.evictable)
            .count()
            > EVAL_SCRIPT_CACHE_LIMIT
        {
            let Some(candidate) = self.lru.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.evictable)
            {
                self.entries.remove(&candidate);
                self.evicted = self.evicted.saturating_add(1);
            }
        }
    }
}

/// Process-wide script cache. Keys are the 40-byte lowercase SHA-1 hex of the
/// source bytes. `EVAL` scripts are capped by a small LRU; `SCRIPT LOAD`
/// entries are persistent and do not participate in that LRU, matching Valkey.
pub(super) fn script_cache() -> &'static Mutex<ScriptCache> {
    static CACHE: OnceLock<Mutex<ScriptCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ScriptCache::default()))
}

pub(super) fn cache_script(script_bytes: &[u8], evictable: bool) -> [u8; 40] {
    let hex = sha1_hex(script_bytes);
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.entries.insert(
        hex,
        CachedScript {
            body: script_bytes.to_vec(),
            evictable,
        },
    );
    if evictable {
        guard.touch_eval_script(hex);
        guard.evict_eval_scripts_if_needed();
    }
    hex
}

pub(crate) fn script_cache_memory_estimate() -> usize {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .entries
        .values()
        .map(|entry| entry.body.len() + 96)
        .sum()
}

pub(crate) fn script_cache_len() -> usize {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.entries.len()
}

pub(crate) fn evicted_scripts_count() -> u64 {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.evicted
}

pub(crate) fn reset_script_cache_stats() {
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.evicted = 0;
}

/// Accept any case for the input sha; return `Some` with the lowercase
/// canonical 40-byte buffer when the input is exactly 40 hex bytes.
pub(super) fn normalise_sha(bytes: &[u8]) -> Option<[u8; 40]> {
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    for (i, b) in bytes.iter().enumerate() {
        let c = match *b {
            b'0'..=b'9' | b'a'..=b'f' => *b,
            b'A'..=b'F' => *b + 32,
            _ => return None,
        };
        out[i] = c;
    }
    Some(out)
}

/// Compute the lowercase 40-byte SHA-1 hex digest of `data` using a pure-Rust
/// implementation. Stays inside this crate so we do not pull a hash-crate
/// dependency for a single use site.
pub(super) fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 40];
    for (i, byte) in digest.iter().enumerate() {
        out[i * 2] = HEX[(byte >> 4) as usize];
        out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out
}

/// Compute the raw 20-byte SHA-1 digest of `data`.
/// Direct translation of FIPS 180-4 section 6.1.2; zero unsafe, no dependency.
fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    let bit_len: u64 = (data.len() as u64) * 8;

    let mut padded: Vec<u8> = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for (i, word) in w.iter().enumerate() {
            let (f, k) = if i < 20 {
                ((b & c) | ((!b) & d), 0x5A827999u32)
            } else if i < 40 {
                (b ^ c ^ d, 0x6ED9EBA1u32)
            } else if i < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32)
            } else {
                (b ^ c ^ d, 0xCA62C1D6u32)
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

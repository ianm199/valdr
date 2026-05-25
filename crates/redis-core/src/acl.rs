//! Multi-user ACL state — per-user permission model.
//!
//! Implements the Redis 6+ ACL semantics:
//!   - Multiple named users, each with passwords (SHA-256 hashed), enabled/disabled
//!     flag, allowed commands (by category bitmask), key patterns, and channel patterns.
//!   - The `default` user starts as `on nopass ~* &* +@all` for backwards compatibility.
//!   - ACL state lives in a process-global `OnceLock<Arc<Mutex<AclState>>>`.
//!
//! TODOs:
//!   - ACL persistence to aclfile / `users` config section.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::RedisString;

use crate::util::string_match_len;

/// SHA-256 hash of a password (32 bytes).
pub type PasswordHash = [u8; 32];

/// Compute the SHA-256 hash of a cleartext password.
pub fn sha256_hash(password: &[u8]) -> PasswordHash {
    use std::num::Wrapping;

    let mut hash = [0u8; 32];
    sha256_raw(password, &mut hash);
    hash
}

/// Minimal pure-Rust SHA-256 implementation (no external crate needed).
///
/// Implements FIPS 180-4 SHA-256. Used only on the ACL write path (SETUSER),
/// not in hot paths.
fn sha256_raw(data: &[u8], out: &mut [u8; 32]) {
    #[allow(clippy::unreadable_literal)]
    let k: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg: Vec<u8> = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (i, bytes) in chunk.chunks(4).enumerate().take(16) {
            w[i] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(k[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    for (i, &word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
}

/// Hex-encode a SHA-256 hash for wire output.
pub fn hash_to_hex(hash: &PasswordHash) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    for &b in hash {
        out.push(HEX_CHARS[(b >> 4) as usize]);
        out.push(HEX_CHARS[(b & 0xf) as usize]);
    }
    out
}

const HEX_CHARS: &[u8] = b"0123456789abcdef";

/// Attempt to decode a 64-char hex string into a 32-byte hash.
pub fn hex_to_hash(hex: &[u8]) -> Option<PasswordHash> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in hex.chunks(2).enumerate() {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Bitmask flags on an ACL user.
#[derive(Debug, Clone, Copy, Default)]
pub struct AclUserFlags {
    /// User is enabled (can authenticate and run commands).
    pub enabled: bool,
    /// No password required; any password (or none) is accepted.
    pub nopass: bool,
    /// RESTORE payloads are sanitized by default.
    pub sanitize_payload: bool,
    /// Allow all commands regardless of `allowed_categories`.
    pub allcommands: bool,
    /// Allow all keys (`~*`).
    pub allkeys: bool,
    /// Allow all channels (`&*`).
    pub allchannels: bool,
    /// Allow every logical database (`alldbs`).
    pub alldbs: bool,
}

/// ACL category bitmask constants (aligned with Valkey acl.c).
pub mod category {
    pub const KEYSPACE: u64 = 1 << 0;
    pub const READ: u64 = 1 << 1;
    pub const WRITE: u64 = 1 << 2;
    pub const SET: u64 = 1 << 3;
    pub const SORTEDSET: u64 = 1 << 4;
    pub const LIST: u64 = 1 << 5;
    pub const HASH: u64 = 1 << 6;
    pub const STRING: u64 = 1 << 7;
    pub const BITMAP: u64 = 1 << 8;
    pub const HYPERLOGLOG: u64 = 1 << 9;
    pub const GEO: u64 = 1 << 10;
    pub const STREAM: u64 = 1 << 11;
    pub const PUBSUB: u64 = 1 << 12;
    pub const ADMIN: u64 = 1 << 13;
    pub const FAST: u64 = 1 << 14;
    pub const SLOW: u64 = 1 << 15;
    pub const BLOCKING: u64 = 1 << 16;
    pub const DANGEROUS: u64 = 1 << 17;
    pub const CONNECTION: u64 = 1 << 18;
    pub const TRANSACTION: u64 = 1 << 19;
    pub const SCRIPTING: u64 = 1 << 20;

    /// All categories combined.
    pub const ALL: u64 = KEYSPACE
        | READ
        | WRITE
        | SET
        | SORTEDSET
        | LIST
        | HASH
        | STRING
        | BITMAP
        | HYPERLOGLOG
        | GEO
        | STREAM
        | PUBSUB
        | ADMIN
        | FAST
        | SLOW
        | BLOCKING
        | DANGEROUS
        | CONNECTION
        | TRANSACTION
        | SCRIPTING;
}

/// Map a category name (lowercase ASCII) to its bitmask bit.
pub fn category_name_to_bit(name: &[u8]) -> Option<u64> {
    let lower: Vec<u8> = name.iter().map(|b| b.to_ascii_lowercase()).collect();
    match lower.as_slice() {
        b"keyspace" => Some(category::KEYSPACE),
        b"read" => Some(category::READ),
        b"write" => Some(category::WRITE),
        b"set" => Some(category::SET),
        b"sortedset" => Some(category::SORTEDSET),
        b"list" => Some(category::LIST),
        b"hash" => Some(category::HASH),
        b"string" => Some(category::STRING),
        b"bitmap" => Some(category::BITMAP),
        b"hyperloglog" => Some(category::HYPERLOGLOG),
        b"geo" => Some(category::GEO),
        b"stream" => Some(category::STREAM),
        b"pubsub" => Some(category::PUBSUB),
        b"admin" => Some(category::ADMIN),
        b"fast" => Some(category::FAST),
        b"slow" => Some(category::SLOW),
        b"blocking" => Some(category::BLOCKING),
        b"dangerous" => Some(category::DANGEROUS),
        b"connection" => Some(category::CONNECTION),
        b"transaction" => Some(category::TRANSACTION),
        b"scripting" => Some(category::SCRIPTING),
        b"all" => Some(category::ALL),
        _ => None,
    }
}

/// Sorted list of all category names (for ACL CAT output).
pub const ALL_CATEGORY_NAMES: &[&[u8]] = &[
    b"admin",
    b"bitmap",
    b"blocking",
    b"connection",
    b"dangerous",
    b"fast",
    b"geo",
    b"hash",
    b"hyperloglog",
    b"keyspace",
    b"list",
    b"pubsub",
    b"read",
    b"scripting",
    b"set",
    b"slow",
    b"sortedset",
    b"stream",
    b"string",
    b"transaction",
    b"write",
];

pub const DEFAULT_ACLLOG_MAX_LEN: usize = 128;

static ACL_PUBSUB_DEFAULT_ALLCHANNELS: AtomicBool = AtomicBool::new(false);

pub const ACL_KEY_READ: u8 = 0b01;
pub const ACL_KEY_WRITE: u8 = 0b10;
pub const ACL_KEY_READ_WRITE: u8 = ACL_KEY_READ | ACL_KEY_WRITE;
pub const ACL_KEY_ANY: u8 = 0;

pub fn acl_pubsub_default_allchannels() -> bool {
    ACL_PUBSUB_DEFAULT_ALLCHANNELS.load(Ordering::Relaxed)
}

pub fn acl_pubsub_default_config_value() -> &'static str {
    if acl_pubsub_default_allchannels() {
        "allchannels"
    } else {
        "resetchannels"
    }
}

pub fn set_acl_pubsub_default(value: &[u8]) -> bool {
    if value.eq_ignore_ascii_case(b"allchannels") {
        ACL_PUBSUB_DEFAULT_ALLCHANNELS.store(true, Ordering::Relaxed);
        true
    } else if value.eq_ignore_ascii_case(b"resetchannels") {
        ACL_PUBSUB_DEFAULT_ALLCHANNELS.store(false, Ordering::Relaxed);
        true
    } else {
        false
    }
}

pub fn apply_acl_pubsub_default_to_user(user: &mut AclUser) {
    if acl_pubsub_default_allchannels() {
        user.flags.allchannels = true;
        user.channel_patterns = vec![RedisString::from_bytes(b"*")];
    }
}

/// One entry in the ACL access-denied log.
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: RedisString,
    pub context: RedisString,
    pub object: RedisString,
    pub username: RedisString,
    pub client_info: RedisString,
    pub entry_id: u64,
    pub timestamp_created: i64,
    pub timestamp_last_updated: i64,
}

/// A single ACL user entry.
#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: RedisString,
    pub flags: AclUserFlags,
    /// SHA-256 hashed passwords.
    pub passwords: Vec<PasswordHash>,
    /// Bitmask of allowed ACL categories.
    pub allowed_categories: u64,
    /// Bitmask of explicitly denied ACL categories.
    pub denied_categories: u64,
    /// Explicitly allowed individual command names (lowercase).
    pub allowed_commands: Vec<RedisString>,
    /// Explicitly denied individual command names (lowercase).
    pub denied_commands: Vec<RedisString>,
    /// Lossless command/category rule order for ACL GETUSER/LIST rendering.
    pub command_rules: Vec<RedisString>,
    /// Key glob patterns allowed (`~pattern`).
    pub key_patterns: Vec<RedisString>,
    /// Key glob patterns with separate read/write permissions (`%R~pattern`).
    pub key_permissions: Vec<AclKeyPattern>,
    /// Channel glob patterns allowed (`&pattern`).
    pub channel_patterns: Vec<RedisString>,
    /// Logical database ids allowed by `db=N` rules.
    pub allowed_dbs: Vec<u32>,
    /// ACL v2 selectors. Each selector is an independent rule set; permissions
    /// from different selectors are intentionally not additive.
    pub selectors: Vec<AclUser>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclKeyPattern {
    pub pattern: RedisString,
    pub permissions: u8,
}

impl AclUser {
    /// Create a new user in reset/deny-all state.
    pub fn new_reset(name: RedisString) -> Self {
        AclUser {
            name,
            flags: AclUserFlags {
                enabled: false,
                nopass: false,
                sanitize_payload: true,
                allcommands: false,
                allkeys: false,
                allchannels: false,
                alldbs: true,
            },
            passwords: Vec::new(),
            allowed_categories: 0,
            denied_categories: 0,
            allowed_commands: Vec::new(),
            denied_commands: Vec::new(),
            command_rules: Vec::new(),
            key_patterns: Vec::new(),
            key_permissions: Vec::new(),
            channel_patterns: Vec::new(),
            allowed_dbs: Vec::new(),
            selectors: Vec::new(),
        }
    }

    /// Create a selector rule set. Selectors share the same deny-all command,
    /// no-key, no-channel defaults as reset users, but do not carry identity or
    /// authentication state.
    pub fn new_selector() -> Self {
        AclUser::new_reset(RedisString::from_static(b""))
    }

    /// Create the default user: `on nopass ~* &* +@all`.
    pub fn new_default() -> Self {
        AclUser {
            name: RedisString::from_bytes(b"default"),
            flags: AclUserFlags {
                enabled: true,
                nopass: true,
                sanitize_payload: true,
                allcommands: true,
                allkeys: true,
                allchannels: true,
                alldbs: true,
            },
            passwords: Vec::new(),
            allowed_categories: category::ALL,
            denied_categories: 0,
            allowed_commands: Vec::new(),
            denied_commands: Vec::new(),
            command_rules: Vec::new(),
            key_patterns: vec![RedisString::from_bytes(b"*")],
            key_permissions: Vec::new(),
            channel_patterns: vec![RedisString::from_bytes(b"*")],
            allowed_dbs: Vec::new(),
            selectors: Vec::new(),
        }
    }

    /// Check whether this user can execute the given command name.
    pub fn can_execute_command(&self, cmd_name: &[u8], cmd_categories: u64) -> bool {
        self.can_execute_command_with_arg(cmd_name, None, cmd_categories)
    }

    /// Check whether this user can execute `cmd_name`, considering
    /// first-argument ACL subcommand rules such as `+client|id`.
    pub fn can_execute_command_with_arg(
        &self,
        cmd_name: &[u8],
        first_arg: Option<&[u8]>,
        cmd_categories: u64,
    ) -> bool {
        let lower: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_lowercase()).collect();
        let lower_rs = RedisString::from_bytes(&lower);
        let subcommand = first_arg.map(|arg| {
            let mut full = Vec::with_capacity(lower.len() + 1 + arg.len());
            full.extend_from_slice(&lower);
            full.push(b'|');
            full.extend(arg.iter().map(|b| b.to_ascii_lowercase()));
            RedisString::from_vec(full)
        });
        if let Some(full) = &subcommand {
            if self.denied_commands.iter().any(|c| c == full) {
                return false;
            }
            if self.allowed_commands.iter().any(|c| c == full) {
                return true;
            }
        }
        if self.denied_commands.iter().any(|c| c == &lower_rs) {
            return false;
        }
        if self.allowed_commands.iter().any(|c| c == &lower_rs) {
            return true;
        }
        if self.flags.allcommands {
            return self.denied_categories & cmd_categories == 0;
        }
        self.allowed_categories & cmd_categories != 0
    }

    /// Check whether this user may access `key`.
    pub fn can_access_key(&self, key: &[u8]) -> bool {
        self.can_access_key_for(key, ACL_KEY_READ_WRITE)
    }

    /// Check whether this selector/user may access `key` with the requested
    /// read/write mode. `ACL_KEY_ANY` is used for existence/cardinality-style
    /// commands where Valkey accepts either read or write key permission.
    pub fn can_access_key_for(&self, key: &[u8], required: u8) -> bool {
        if self.flags.allkeys {
            return true;
        }
        let mut granted = 0u8;
        if self
            .key_patterns
            .iter()
            .any(|pat| string_match_len(pat.as_bytes(), key, false))
        {
            granted |= ACL_KEY_READ_WRITE;
        }
        for pat in &self.key_permissions {
            if string_match_len(pat.pattern.as_bytes(), key, false) {
                granted |= pat.permissions;
            }
        }
        if required == ACL_KEY_ANY {
            granted != 0
        } else {
            granted & required == required
        }
    }

    /// Check whether this user may access `channel`.
    pub fn can_access_channel(&self, channel: &[u8]) -> bool {
        if self.flags.allchannels {
            return true;
        }
        self.channel_patterns
            .iter()
            .any(|pat| string_match_len(pat.as_bytes(), channel, false))
    }

    /// Check whether this user may subscribe to channel pattern `pattern`.
    ///
    /// Valkey matches PSUBSCRIBE patterns literally against ACL channel
    /// patterns; normal channels use glob matching.
    pub fn can_access_channel_pattern(&self, pattern: &[u8]) -> bool {
        if self.flags.allchannels {
            return true;
        }
        self.channel_patterns
            .iter()
            .any(|pat| pat.as_bytes() == pattern)
    }

    /// Check whether this user may access logical database `db`.
    pub fn can_access_db(&self, db: u32) -> bool {
        self.flags.alldbs || self.allowed_dbs.contains(&db)
    }

    /// Check whether this user's password list contains the given cleartext password.
    pub fn check_password(&self, cleartext: &[u8]) -> bool {
        if self.flags.nopass {
            return true;
        }
        let hash = sha256_hash(cleartext);
        self.passwords.iter().any(|h| *h == hash)
    }

    /// Render this user as an `ACL LIST` / `ACL SETUSER` rule string.
    pub fn to_rule_string(&self) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        out.extend_from_slice(b"user ");
        out.extend_from_slice(self.name.as_bytes());
        out.push(b' ');
        if self.flags.enabled {
            out.extend_from_slice(b"on");
        } else {
            out.extend_from_slice(b"off");
        }
        if self.flags.nopass {
            out.extend_from_slice(b" nopass");
        }
        if self.flags.sanitize_payload {
            out.extend_from_slice(b" sanitize-payload");
        } else {
            out.extend_from_slice(b" skip-sanitize-payload");
        }
        for hash in &self.passwords {
            out.extend_from_slice(b" #");
            out.extend_from_slice(&hash_to_hex(hash));
        }
        if self.flags.allkeys {
            out.extend_from_slice(b" ~*");
        } else {
            for item in self.key_summary_items() {
                out.push(b' ');
                out.extend_from_slice(&item);
            }
        }
        if self.flags.allchannels {
            out.extend_from_slice(b" &*");
        } else {
            out.extend_from_slice(b" resetchannels");
            for pat in &self.channel_patterns {
                out.push(b' ');
                out.push(b'&');
                out.extend_from_slice(pat.as_bytes());
            }
        }
        out.push(b' ');
        out.extend_from_slice(&self.databases_summary());
        out.push(b' ');
        out.extend_from_slice(&self.commands_summary());
        for selector in &self.selectors {
            out.extend_from_slice(b" (");
            out.extend_from_slice(&selector.selector_rule_body());
            out.push(b')');
        }
        out
    }

    /// Return a string describing the commands permission state for GETUSER.
    pub fn commands_summary(&self) -> Vec<u8> {
        let mut out = if self.flags.allcommands {
            b"+@all".to_vec()
        } else {
            b"-@all".to_vec()
        };
        for rule in &self.command_rules {
            out.push(b' ');
            out.extend_from_slice(rule.as_bytes());
        }
        out
    }

    /// Render key patterns for GETUSER.
    pub fn keys_summary(&self) -> Vec<u8> {
        if self.flags.allkeys {
            return b"~*".to_vec();
        }
        let items = self.key_summary_items();
        if items.is_empty() {
            return b"".to_vec();
        }
        let mut out: Vec<u8> = Vec::new();
        for item in items {
            if !out.is_empty() {
                out.push(b' ');
            }
            out.extend_from_slice(&item);
        }
        out
    }

    fn key_summary_items(&self) -> Vec<Vec<u8>> {
        let mut items: Vec<(RedisString, u8)> = Vec::new();
        for pat in &self.key_permissions {
            merge_key_summary_item(&mut items, pat.pattern.clone(), pat.permissions);
        }
        for pat in &self.key_patterns {
            merge_key_summary_item(&mut items, pat.clone(), ACL_KEY_READ_WRITE);
        }
        items
            .into_iter()
            .map(|(pattern, permissions)| {
                let mut out = Vec::new();
                match permissions & ACL_KEY_READ_WRITE {
                    ACL_KEY_READ_WRITE => out.push(b'~'),
                    ACL_KEY_READ => out.extend_from_slice(b"%R~"),
                    ACL_KEY_WRITE => out.extend_from_slice(b"%W~"),
                    _ => out.extend_from_slice(b"%RW~"),
                }
                out.extend_from_slice(pattern.as_bytes());
                out
            })
            .collect()
    }

    /// Render channel patterns for GETUSER.
    pub fn channels_summary(&self) -> Vec<u8> {
        if self.flags.allchannels {
            return b"&*".to_vec();
        }
        if self.channel_patterns.is_empty() {
            return b"".to_vec();
        }
        let mut out: Vec<u8> = Vec::new();
        for pat in &self.channel_patterns {
            if !out.is_empty() {
                out.push(b' ');
            }
            out.push(b'&');
            out.extend_from_slice(pat.as_bytes());
        }
        out
    }

    fn selector_channels_rule_summary(&self) -> Vec<u8> {
        if self.flags.allchannels {
            return b"&*".to_vec();
        }
        let channels = self.channels_summary();
        if channels.is_empty() {
            return channels;
        }
        let mut out = b"resetchannels ".to_vec();
        out.extend_from_slice(&channels);
        out
    }

    /// Render database selectors for GETUSER/LIST.
    pub fn databases_summary(&self) -> Vec<u8> {
        if self.flags.alldbs {
            return b"alldbs".to_vec();
        }
        if self.allowed_dbs.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut dbs = self.allowed_dbs.clone();
        dbs.sort_unstable();
        dbs.dedup();
        out.extend_from_slice(b"db=");
        for (idx, db) in dbs.iter().enumerate() {
            if idx > 0 {
                out.push(b',');
            }
            out.extend_from_slice(db.to_string().as_bytes());
        }
        out
    }

    pub fn selector_rule_body(&self) -> Vec<u8> {
        let mut parts: Vec<Vec<u8>> = Vec::new();
        parts.push(self.databases_summary());
        parts.push(self.commands_summary());
        let channels = self.selector_channels_rule_summary();
        if !channels.is_empty() {
            parts.push(channels);
        }
        let keys = self.keys_summary();
        if !keys.is_empty() {
            parts.push(keys);
        }
        let mut out = Vec::new();
        for part in parts.into_iter().filter(|p| !p.is_empty()) {
            if !out.is_empty() {
                out.push(b' ');
            }
            out.extend_from_slice(&part);
        }
        out
    }
}

fn merge_key_summary_item(
    items: &mut Vec<(RedisString, u8)>,
    pattern: RedisString,
    permissions: u8,
) {
    if let Some((_, existing)) = items.iter_mut().find(|(pat, _)| pat == &pattern) {
        *existing |= permissions;
    } else {
        items.push((pattern, permissions));
    }
}

/// Process-wide ACL state: map of username → `AclUser` plus ACL LOG ring.
pub struct AclState {
    pub users: HashMap<RedisString, AclUser>,
    log: VecDeque<AclLogEntry>,
    log_next_entry_id: u64,
    log_max_len: usize,
}

impl AclState {
    /// Initialise with just the `default` user.
    pub fn new() -> Self {
        let mut users = HashMap::new();
        let default_user = AclUser::new_default();
        users.insert(default_user.name.clone(), default_user);
        AclState {
            users,
            log: VecDeque::new(),
            log_next_entry_id: 0,
            log_max_len: DEFAULT_ACLLOG_MAX_LEN,
        }
    }

    pub fn clear_log(&mut self) {
        self.log.clear();
    }

    pub fn log_max_len(&self) -> usize {
        self.log_max_len
    }

    pub fn set_log_max_len(&mut self, max_len: usize) {
        self.log_max_len = max_len;
    }

    pub fn log_entries(&self, limit: Option<usize>) -> Vec<AclLogEntry> {
        let limit = limit.unwrap_or(self.log.len());
        self.log.iter().take(limit).cloned().collect()
    }

    pub fn record_log_entry(
        &mut self,
        reason: RedisString,
        context: RedisString,
        object: RedisString,
        username: RedisString,
        client_info: RedisString,
    ) {
        if self.log_max_len == 0 {
            return;
        }
        let now = acl_log_now_millis();
        if let Some(pos) = self.log.iter().position(|entry| {
            entry.reason == reason
                && entry.context == context
                && entry.object == object
                && entry.username == username
        }) {
            if let Some(mut entry) = self.log.remove(pos) {
                entry.count = entry.count.saturating_add(1);
                entry.timestamp_last_updated = now;
                entry.client_info = client_info;
                self.log.push_front(entry);
            }
            return;
        }

        let entry = AclLogEntry {
            count: 1,
            reason,
            context,
            object,
            username,
            client_info,
            entry_id: self.log_next_entry_id,
            timestamp_created: now,
            timestamp_last_updated: now,
        };
        self.log_next_entry_id = self.log_next_entry_id.saturating_add(1);
        self.log.push_front(entry);
        while self.log.len() > self.log_max_len {
            self.log.pop_back();
        }
    }
}

impl Default for AclState {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_ACL_STATE: OnceLock<Arc<Mutex<AclState>>> = OnceLock::new();

/// Initialise the global ACL state. Idempotent.
pub fn install_acl_state() {
    let _ = GLOBAL_ACL_STATE.set(Arc::new(Mutex::new(AclState::new())));
}

/// Return a clone of the global ACL state handle.
pub fn global_acl_state() -> Arc<Mutex<AclState>> {
    GLOBAL_ACL_STATE
        .get_or_init(|| Arc::new(Mutex::new(AclState::new())))
        .clone()
}

pub fn record_acl_log_entry(
    reason: &[u8],
    context: &[u8],
    object: RedisString,
    username: RedisString,
    client_info: RedisString,
) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.record_log_entry(
        RedisString::from_bytes(reason),
        RedisString::from_bytes(context),
        object,
        username,
        client_info,
    );
}

pub fn clear_acl_log() {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clear_log();
}

pub fn acl_log_entries(limit: Option<usize>) -> Vec<AclLogEntry> {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.log_entries(limit)
}

pub fn set_acl_log_max_len(max_len: usize) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.set_log_max_len(max_len);
}

pub fn acl_log_max_len() -> usize {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.log_max_len()
}

pub fn acl_log_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty_input() {
        let hash = sha256_hash(b"");
        let hex = hash_to_hex(&hash);
        assert_eq!(
            hex,
            b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_known_vector() {
        let hash = sha256_hash(b"abc");
        let hex = hash_to_hex(&hash);
        assert_eq!(
            hex,
            b"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".as_ref()
        );
    }

    #[test]
    fn default_user_allows_all() {
        let user = AclUser::new_default();
        assert!(
            user.can_execute_command(b"GET", category::READ | category::FAST | category::STRING)
        );
        assert!(
            user.can_execute_command(b"SET", category::WRITE | category::SLOW | category::STRING)
        );
        assert!(user.flags.enabled);
        assert!(user.flags.nopass);
    }

    #[test]
    fn reset_user_denies_all() {
        let user = AclUser::new_reset(RedisString::from_bytes(b"testuser"));
        assert!(!user.can_execute_command(b"GET", category::READ));
        assert!(!user.flags.enabled);
    }

    #[test]
    fn password_check() {
        let mut user = AclUser::new_reset(RedisString::from_bytes(b"alice"));
        user.passwords.push(sha256_hash(b"secret123"));
        assert!(user.check_password(b"secret123"));
        assert!(!user.check_password(b"wrong"));
    }

    #[test]
    fn hex_roundtrip() {
        let hash = sha256_hash(b"hello");
        let hex = hash_to_hex(&hash);
        let back = hex_to_hash(&hex).unwrap();
        assert_eq!(hash, back);
    }
}

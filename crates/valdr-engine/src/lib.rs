//! Wasm-safe embedded Valdr command engine.
//!
//! This crate is intentionally smaller than `redis-core` + `redis-commands`.
//! It is the first EdgeStash boundary: no networking, TLS, process APIs,
//! background workers, native filesystem access, or C Lua.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use lua_rs_runtime::{
    Lua, LuaError, LuaString, LuaVersion, Table as LuaTable, Value as LuaValue, Variadic,
};
use redis_protocol::{encode_resp2, RespFrame};
use redis_types::RedisString;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    Unavailable,
    Message(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    InvalidJson,
    InvalidVersion,
    MissingField(&'static str),
    InvalidField(&'static str),
    InvalidHex,
}

pub trait Host {
    fn now_millis(&self) -> u64;

    fn random_bytes(&mut self, _out: &mut [u8]) -> Result<(), HostError> {
        Err(HostError::Unavailable)
    }

    fn persist_append(&mut self, _record: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopHost {
    now_millis: u64,
}

impl NoopHost {
    pub fn new(now_millis: u64) -> Self {
        Self { now_millis }
    }

    pub fn set_now_millis(&mut self, now_millis: u64) {
        self.now_millis = now_millis;
    }
}

impl Host for NoopHost {
    fn now_millis(&self) -> u64 {
        self.now_millis
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: StoredValue,
    expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum StoredValue {
    String(Vec<u8>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    ZSet(HashMap<Vec<u8>, f64>),
}

impl StoredValue {
    /// The Valkey `TYPE` name for this value, mirroring `getObjectTypeName`
    /// (`db.c`). Only the variants the edge engine currently models appear
    /// here; later type waves extend both the enum and this mapping.
    fn type_name(&self) -> &'static [u8] {
        match self {
            StoredValue::String(_) => b"string",
            StoredValue::Hash(_) => b"hash",
            StoredValue::ZSet(_) => b"zset",
        }
    }
}

/// Unit of the relative/absolute argument to the EXPIRE family. Seconds inputs
/// are scaled to milliseconds before being applied as absolute deadlines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireUnit {
    Seconds,
    Milliseconds,
}

/// Optional trailing condition flag of the EXPIRE family
/// (`parseExtendedExpireArgumentsOrReply`, `expire.c`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCondition {
    Nx,
    Xx,
    Gt,
    Lt,
}

/// Maximum number of distinct scripts retained in the per-engine cache before
/// the least-recently-used entry is evicted. The reference server never evicts,
/// but at the edge one hot engine lives for the whole life of a Durable Object,
/// so an unbounded cache is a slow-burn memory-growth surface for a tenant that
/// keeps loading distinct scripts. Eviction is safe: a dropped script answers
/// `EVALSHA` with `NOSCRIPT`, which clients already handle by re-sending `EVAL`.
const MAX_CACHED_SCRIPTS: usize = 256;

/// Aggregate byte ceiling for cached script bodies. A single script larger than
/// this is still cached when it is the only entry (mirroring that one `EVAL`
/// must always run); the ceiling only forces eviction of older entries.
const MAX_SCRIPT_CACHE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct Engine<H> {
    host: H,
    db: HashMap<Vec<u8>, Entry>,
    scripts: HashMap<[u8; 40], Vec<u8>>,
    /// SHA order from least- to most-recently used, kept in lockstep with
    /// `scripts` for LRU eviction. Touch-on-use keeps a frequently invoked
    /// script (e.g. a tenant's limiter) resident even under load from
    /// one-off scripts.
    script_lru: Vec<[u8; 40]>,
    script_cache_bytes: usize,
    mutation_epoch: u64,
    /// Keys whose snapshot-visible state changed since the last `take_dirty`.
    /// Drives per-key persistence: a host writes only these keys back to
    /// storage instead of re-serializing the whole database on every command.
    /// Populated at every write/delete/expiry site (including `redis.call`
    /// mutations inside scripts); passive expiry does not add to it because a
    /// stale persisted copy of an expired key is harmless under absolute
    /// `expire_at_ms`.
    dirty: HashSet<Vec<u8>>,
}

impl Engine<NoopHost> {
    pub fn new_in_memory() -> Self {
        Self::new(NoopHost::default())
    }
}

impl<H: Host> Engine<H> {
    pub fn new(host: H) -> Self {
        Self {
            host,
            db: HashMap::new(),
            scripts: HashMap::new(),
            script_lru: Vec::new(),
            script_cache_bytes: 0,
            mutation_epoch: 0,
            dirty: HashSet::new(),
        }
    }

    /// Insert (or refresh) a script in the bounded LRU cache. Re-caching an
    /// already-resident script only marks it most-recently-used. After
    /// inserting a new body, the oldest entries are evicted until both the
    /// count and aggregate-byte ceilings hold, never evicting the entry just
    /// inserted. Never bumps the mutation epoch — the script cache is excluded
    /// from snapshots.
    fn cache_script(&mut self, sha: [u8; 40], body: &[u8]) {
        if self.scripts.contains_key(&sha) {
            self.touch_script(&sha);
            return;
        }
        self.scripts.insert(sha, body.to_vec());
        self.script_lru.push(sha);
        self.script_cache_bytes += body.len();
        while self.script_lru.len() > 1
            && (self.script_lru.len() > MAX_CACHED_SCRIPTS
                || self.script_cache_bytes > MAX_SCRIPT_CACHE_BYTES)
        {
            let evicted = self.script_lru.remove(0);
            if let Some(body) = self.scripts.remove(&evicted) {
                self.script_cache_bytes -= body.len();
            }
        }
    }

    /// Mark a resident script most-recently-used.
    fn touch_script(&mut self, sha: &[u8; 40]) {
        if let Some(index) = self.script_lru.iter().position(|entry| entry == sha) {
            let sha = self.script_lru.remove(index);
            self.script_lru.push(sha);
        }
    }

    fn clear_script_cache(&mut self) {
        self.scripts.clear();
        self.script_lru.clear();
        self.script_cache_bytes = 0;
    }

    /// Monotonic counter bumped whenever snapshot-visible state changes:
    /// key writes, deletes, and expiry updates, including those made through
    /// `redis.call` inside scripts. Passive expiry of already-dead keys and
    /// script-cache changes do not bump it because absolute `expire_at_ms`
    /// values make a stale persisted copy of an expired key harmless and the
    /// script cache is excluded from snapshots. Persistence layers compare
    /// epochs to skip exporting and rewriting unchanged state.
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }

    /// Record that `key` was written, deleted, or had its expiry changed:
    /// bumps the mutation epoch and marks the key dirty for per-key
    /// persistence. Every mutating command path calls this with the exact
    /// key(s) it touched, so `take_dirty` yields precisely the keys a host
    /// must flush.
    fn note_write(&mut self, key: &[u8]) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        self.dirty.insert(key.to_vec());
    }

    /// Drain the set of keys changed since the last call, sorted for
    /// deterministic flush order. A host persists each returned key by calling
    /// `export_key` (write the bytes) or, when it returns `None`, deleting the
    /// key from storage.
    pub fn take_dirty(&mut self) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self.dirty.drain().collect();
        keys.sort();
        keys
    }

    /// Serialize one key's live entry to the same JSON shape used inside a
    /// full snapshot. Returns `None` when the key is absent or already expired
    /// (the host then deletes it from storage). Purges the key first so an
    /// expired key never round-trips.
    pub fn export_key(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.purge_if_expired(key);
        let entry = self.db.get(key)?;
        let object = encode_entry(key, entry);
        Some(serde_json::to_vec(&JsonValue::Object(object)).unwrap_or_default())
    }

    /// Restore one key from bytes produced by `export_key`. Does not mark the
    /// key dirty — this is a load from authoritative storage, not a mutation.
    pub fn import_key(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let value: JsonValue =
            serde_json::from_slice(bytes).map_err(|_| SnapshotError::InvalidJson)?;
        let object = value
            .as_object()
            .ok_or(SnapshotError::InvalidField("key"))?;
        let (key, entry) = decode_entry(object)?;
        self.db.insert(key, entry);
        Ok(())
    }

    fn mark_all_dirty(&mut self) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let keys: Vec<Vec<u8>> = self.db.keys().cloned().collect();
        for key in keys {
            self.dirty.insert(key);
        }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn host_mut(&mut self) -> &mut H {
        &mut self.host
    }

    pub fn execute(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        self.execute_inner(argv, false)
    }

    pub fn export_snapshot(&mut self) -> Vec<u8> {
        self.purge_expired_keys();

        let mut keys: Vec<_> = self.db.iter().collect();
        keys.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut encoded_keys = Vec::with_capacity(keys.len());
        for (key, entry) in keys {
            encoded_keys.push(JsonValue::Object(encode_entry(key, entry)));
        }

        serde_json::to_vec(&json!({
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": encoded_keys,
        }))
        .unwrap_or_else(|_| {
            b"{\"format\":\"valdr-engine-snapshot\",\"version\":1,\"keys\":[]}".to_vec()
        })
    }

    pub fn import_snapshot(&mut self, snapshot: &[u8]) -> Result<(), SnapshotError> {
        let json: JsonValue =
            serde_json::from_slice(snapshot).map_err(|_| SnapshotError::InvalidJson)?;
        if json.get("format").and_then(JsonValue::as_str) != Some("valdr-engine-snapshot") {
            return Err(SnapshotError::InvalidField("format"));
        }
        if json.get("version").and_then(JsonValue::as_u64) != Some(1) {
            return Err(SnapshotError::InvalidVersion);
        }
        let keys = json
            .get("keys")
            .and_then(JsonValue::as_array)
            .ok_or(SnapshotError::MissingField("keys"))?;

        let mut next_db = HashMap::new();
        for item in keys {
            let object = item
                .as_object()
                .ok_or(SnapshotError::InvalidField("keys"))?;
            let (key, entry) = decode_entry(object)?;
            next_db.insert(key, entry);
        }

        self.db = next_db;
        self.clear_script_cache();
        self.mark_all_dirty();
        Ok(())
    }

    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> RestResponse {
        if !matches!(
            request.method,
            RestMethod::Get | RestMethod::Post | RestMethod::Put | RestMethod::Head
        ) {
            return RestResponse::json_error(405, b"ERR unsupported HTTP method");
        }

        let command_result = match rest_command_from_request(request) {
            Ok(RestCommand::Single(argv)) => {
                let frame = self.execute(&argv);
                if request.method == RestMethod::Head {
                    return RestResponse {
                        status: rest_status_for_frame(&frame),
                        content_type: rest_content_type(request.response_format),
                        body: Vec::new(),
                    };
                }
                match request.response_format {
                    RestResponseFormat::Json => RestResponse::json_frame(frame),
                    RestResponseFormat::Resp2 => RestResponse::resp2_frame(frame),
                }
            }
            Ok(RestCommand::Pipeline(commands)) => {
                let mut items = Vec::with_capacity(commands.len());
                for argv in commands {
                    items.push(rest_frame_json(self.execute(&argv)));
                }
                match serde_json::to_vec(&JsonValue::Array(items)) {
                    Ok(body) => RestResponse {
                        status: 200,
                        content_type: APPLICATION_JSON,
                        body,
                    },
                    Err(_) => RestResponse::json_error(500, b"ERR JSON encode failed"),
                }
            }
            Err(error) => RestResponse::json_error(400, &error),
        };

        command_result
    }

    fn execute_inner(&mut self, argv: &[Vec<u8>], from_script: bool) -> RespFrame {
        let Some(command) = argv.first() else {
            return unknown_command_error(b"", &[]);
        };
        if from_script && script_blocked_command(command) {
            return err(b"ERR This Redis command is not allowed from script");
        }

        if ascii_eq(command, b"GET") {
            self.get_command(argv)
        } else if ascii_eq(command, b"SET") {
            self.set_command(argv)
        } else if ascii_eq(command, b"SETEX") {
            self.setex_command(argv)
        } else if ascii_eq(command, b"DEL") {
            self.del_command(argv, b"del")
        } else if ascii_eq(command, b"EXISTS") {
            self.exists_command(argv)
        } else if ascii_eq(command, b"INCR") {
            self.incr_command(argv)
        } else if ascii_eq(command, b"INCRBY") {
            self.incrby_command(argv)
        } else if ascii_eq(command, b"DECR") {
            self.decr_command(argv)
        } else if ascii_eq(command, b"DECRBY") {
            self.decrby_command(argv)
        } else if ascii_eq(command, b"APPEND") {
            self.append_command(argv)
        } else if ascii_eq(command, b"STRLEN") {
            self.strlen_command(argv)
        } else if ascii_eq(command, b"SETNX") {
            self.setnx_command(argv)
        } else if ascii_eq(command, b"GETSET") {
            self.getset_command(argv)
        } else if ascii_eq(command, b"GETDEL") {
            self.getdel_command(argv)
        } else if ascii_eq(command, b"MGET") {
            self.mget_command(argv)
        } else if ascii_eq(command, b"EXPIRE") {
            self.expire_command(argv, ExpireUnit::Seconds, false)
        } else if ascii_eq(command, b"PEXPIRE") {
            self.expire_command(argv, ExpireUnit::Milliseconds, false)
        } else if ascii_eq(command, b"EXPIREAT") {
            self.expire_command(argv, ExpireUnit::Seconds, true)
        } else if ascii_eq(command, b"PEXPIREAT") {
            self.expire_command(argv, ExpireUnit::Milliseconds, true)
        } else if ascii_eq(command, b"PERSIST") {
            self.persist_command(argv)
        } else if ascii_eq(command, b"TTL") {
            self.ttl_command(argv, false, false)
        } else if ascii_eq(command, b"PTTL") {
            self.ttl_command(argv, true, false)
        } else if ascii_eq(command, b"EXPIRETIME") {
            self.ttl_command(argv, false, true)
        } else if ascii_eq(command, b"PEXPIRETIME") {
            self.ttl_command(argv, true, true)
        } else if ascii_eq(command, b"TYPE") {
            self.type_command(argv)
        } else if ascii_eq(command, b"RENAME") {
            self.rename_command(argv, false)
        } else if ascii_eq(command, b"RENAMENX") {
            self.rename_command(argv, true)
        } else if ascii_eq(command, b"COPY") {
            self.copy_command(argv)
        } else if ascii_eq(command, b"TOUCH") {
            self.touch_command(argv)
        } else if ascii_eq(command, b"UNLINK") {
            self.del_command(argv, b"unlink")
        } else if ascii_eq(command, b"PING") {
            self.ping_command(argv)
        } else if ascii_eq(command, b"ECHO") {
            self.echo_command(argv)
        } else if ascii_eq(command, b"FLUSHALL") {
            self.flushall_command(argv)
        } else if ascii_eq(command, b"HSET") {
            self.hset_command(argv)
        } else if ascii_eq(command, b"HGET") {
            self.hget_command(argv)
        } else if ascii_eq(command, b"HGETALL") {
            self.hgetall_command(argv)
        } else if ascii_eq(command, b"HDEL") {
            self.hdel_command(argv)
        } else if ascii_eq(command, b"HEXISTS") {
            self.hexists_command(argv)
        } else if ascii_eq(command, b"HLEN") {
            self.hlen_command(argv)
        } else if ascii_eq(command, b"HMGET") {
            self.hmget_command(argv)
        } else if ascii_eq(command, b"HKEYS") {
            self.hkeys_command(argv)
        } else if ascii_eq(command, b"HVALS") {
            self.hvals_command(argv)
        } else if ascii_eq(command, b"HSTRLEN") {
            self.hstrlen_command(argv)
        } else if ascii_eq(command, b"HSETNX") {
            self.hsetnx_command(argv)
        } else if ascii_eq(command, b"HINCRBY") {
            self.hincrby_command(argv)
        } else if ascii_eq(command, b"HMSET") {
            self.hmset_command(argv)
        } else if ascii_eq(command, b"ZADD") {
            self.zadd_command(argv)
        } else if ascii_eq(command, b"ZSCORE") {
            self.zscore_command(argv)
        } else if ascii_eq(command, b"ZINCRBY") {
            self.zincrby_command(argv)
        } else if ascii_eq(command, b"ZREM") {
            self.zrem_command(argv)
        } else if ascii_eq(command, b"ZCARD") {
            self.zcard_command(argv)
        } else if ascii_eq(command, b"ZRANK") {
            self.zrank_command(argv, false)
        } else if ascii_eq(command, b"ZREVRANK") {
            self.zrank_command(argv, true)
        } else if ascii_eq(command, b"ZRANGE") {
            self.zrange_command(argv)
        } else if ascii_eq(command, b"ZRANGEBYSCORE") {
            self.zrangebyscore_command(argv)
        } else if ascii_eq(command, b"SCRIPT") {
            self.script_command(argv)
        } else if ascii_eq(command, b"EVAL") {
            self.eval_command(argv)
        } else if ascii_eq(command, b"EVALSHA") {
            self.evalsha_command(argv)
        } else {
            unknown_command_error(command, &argv[1..])
        }
    }

    fn get_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"get");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => bulk(value),
            Some(_) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn set_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"set");
        }
        let mut expire_at_ms = None;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;
        let mut index = 3;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"NX") {
                if xx {
                    return err(b"ERR syntax error");
                }
                nx = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"XX") {
                if nx {
                    return err(b"ERR syntax error");
                }
                xx = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"GET") {
                get = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"PX") || ascii_eq(&argv[index], b"EX") {
                if expire_at_ms.is_some() || keepttl {
                    return err(b"ERR syntax error");
                }
                if index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if raw <= 0 {
                    return invalid_expire_time(b"set");
                }
                let unit = if ascii_eq(&argv[index], b"PX") {
                    1
                } else {
                    1000
                };
                let Some(ttl) = checked_ttl_ms(raw, unit) else {
                    return invalid_expire_time(b"set");
                };
                expire_at_ms = self.host.now_millis().checked_add(ttl);
                if expire_at_ms.is_none() {
                    return invalid_expire_time(b"set");
                }
                index += 2;
            } else if ascii_eq(&argv[index], b"EXAT") || ascii_eq(&argv[index], b"PXAT") {
                if expire_at_ms.is_some() || keepttl {
                    return err(b"ERR syntax error");
                }
                if index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if raw <= 0 {
                    return invalid_expire_time(b"set");
                }
                let unit = if ascii_eq(&argv[index], b"PXAT") { 1 } else { 1000 };
                let Some(absolute) = (raw as u64).checked_mul(unit) else {
                    return invalid_expire_time(b"set");
                };
                expire_at_ms = Some(absolute);
                index += 2;
            } else if ascii_eq(&argv[index], b"KEEPTTL") {
                if expire_at_ms.is_some() {
                    return err(b"ERR syntax error");
                }
                keepttl = true;
                index += 1;
            } else {
                return err(b"ERR syntax error");
            }
        }
        self.purge_if_expired(&argv[1]);
        let mut exists = false;
        let mut old_string = None;
        match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => {
                exists = true;
                old_string = Some(value.clone());
            }
            Some(_) => {
                if get {
                    return wrong_type();
                }
                exists = true;
            }
            None => {}
        }
        if (nx && exists) || (xx && !exists) {
            return if get {
                old_string_reply(old_string)
            } else {
                RespFrame::null_bulk()
            };
        }
        if keepttl {
            expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        }
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms,
            },
        );
        self.note_write(&argv[1]);
        if get {
            old_string_reply(old_string)
        } else {
            simple(b"OK")
        }
    }

    fn setex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"setex");
        }
        let Some(seconds) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if seconds <= 0 {
            return invalid_expire_time(b"setex");
        }
        let Some(ttl) = checked_ttl_ms(seconds, 1000) else {
            return invalid_expire_time(b"setex");
        };
        let Some(expire_at_ms) = self.host.now_millis().checked_add(ttl) else {
            return invalid_expire_time(b"setex");
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[3].clone()),
                expire_at_ms: Some(expire_at_ms),
            },
        );
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    fn del_command(&mut self, argv: &[Vec<u8>], name: &[u8]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(name);
        }
        let mut deleted = 0;
        for key in &argv[1..] {
            self.purge_if_expired(key);
            if self.db.remove(key).is_some() {
                self.note_write(key);
                deleted += 1;
            }
        }
        RespFrame::integer(deleted)
    }

    fn exists_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"exists");
        }
        let mut count = 0;
        for key in &argv[1..] {
            if self.get_value(key).is_some() {
                count += 1;
            }
        }
        RespFrame::integer(count)
    }

    fn incr_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"incr");
        }
        self.apply_increment(&argv[1], 1)
    }

    fn incrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"incrby");
        }
        let Some(delta) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.apply_increment(&argv[1], delta)
    }

    fn apply_increment(&mut self, key: &[u8], delta: i64) -> RespFrame {
        let current = match self.get_value(key) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => match parse_i64(value) {
                Some(n) => n,
                None => return err(b"ERR value is not an integer or out of range"),
            },
            Some(_) => return wrong_type(),
            None => 0,
        };
        let Some(next) = current.checked_add(delta) else {
            return err(b"ERR increment or decrement would overflow");
        };
        let expire_at_ms = self.db.get(key).and_then(|entry| entry.expire_at_ms);
        self.db.insert(
            key.to_vec(),
            Entry {
                value: StoredValue::String(next.to_string().into_bytes()),
                expire_at_ms,
            },
        );
        self.note_write(key);
        RespFrame::integer(next)
    }

    fn decr_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"decr");
        }
        self.apply_increment(&argv[1], -1)
    }

    fn decrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"decrby");
        }
        let Some(delta) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(negated) = delta.checked_neg() else {
            return err(b"ERR decrement would overflow");
        };
        self.apply_increment(&argv[1], negated)
    }

    fn append_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"append");
        }
        self.purge_if_expired(&argv[1]);
        let new_len = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => {
                value.extend_from_slice(&argv[2]);
                value.len()
            }
            Some(_) => return wrong_type(),
            None => {
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::String(argv[2].clone()),
                        expire_at_ms: None,
                    },
                );
                argv[2].len()
            }
        };
        self.note_write(&argv[1]);
        RespFrame::integer(new_len as i64)
    }

    fn strlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"strlen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => RespFrame::integer(value.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn setnx_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"setnx");
        }
        self.purge_if_expired(&argv[1]);
        if self.db.contains_key(&argv[1]) {
            return RespFrame::integer(0);
        }
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms: None,
            },
        );
        self.note_write(&argv[1]);
        RespFrame::integer(1)
    }

    fn getset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"getset");
        }
        self.purge_if_expired(&argv[1]);
        let old = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => Some(value.clone()),
            Some(_) => return wrong_type(),
            None => None,
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms: None,
            },
        );
        self.note_write(&argv[1]);
        old_string_reply(old)
    }

    fn getdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"getdel");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.remove(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => {
                self.note_write(&argv[1]);
                bulk(value)
            }
            Some(other) => {
                self.db.insert(argv[1].clone(), other);
                wrong_type()
            }
            None => RespFrame::null_bulk(),
        }
    }

    fn mget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"mget");
        }
        let mut items = Vec::with_capacity(argv.len() - 1);
        for key in &argv[1..] {
            let item = match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::String(value)) => bulk(value),
                _ => RespFrame::null_bulk(),
            };
            items.push(item);
        }
        RespFrame::array(items)
    }

    fn hset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 || argv.len() % 2 != 0 {
            return wrong_arity(b"hset");
        }
        self.purge_if_expired(&argv[1]);

        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields.insert(pair[0].clone(), pair[1].clone()).is_none() {
                        added += 1;
                    }
                }
                self.note_write(&argv[1]);
                RespFrame::integer(added)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_),
                ..
            }) => wrong_type(),
            None => {
                let mut fields = HashMap::new();
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields.insert(pair[0].clone(), pair[1].clone()).is_none() {
                        added += 1;
                    }
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
                self.note_write(&argv[1]);
                RespFrame::integer(added)
            }
        }
    }

    fn hget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hget");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => match fields.get(&argv[2]) {
                Some(value) => bulk(value),
                None => RespFrame::null_bulk(),
            },
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn hgetall_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hgetall");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut pairs: Vec<_> = fields.iter().collect();
                pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
                let mut items = Vec::with_capacity(fields.len() * 2);
                for (field, value) in pairs {
                    items.push(bulk(field));
                    items.push(bulk(value));
                }
                RespFrame::array(items)
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"hdel");
        }
        self.purge_if_expired(&argv[1]);
        let mut remove_empty_hash = false;
        let mut mutated = false;
        let response = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut deleted = 0;
                for field in &argv[2..] {
                    if fields.remove(field).is_some() {
                        deleted += 1;
                    }
                }
                remove_empty_hash = fields.is_empty();
                mutated = deleted > 0;
                RespFrame::integer(deleted)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_),
                ..
            }) => wrong_type(),
            None => RespFrame::integer(0),
        };
        if remove_empty_hash {
            self.db.remove(&argv[1]);
        }
        if mutated {
            self.note_write(&argv[1]);
        }
        response
    }

    fn hexists_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hexists");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                RespFrame::integer(if fields.contains_key(&argv[2]) { 1 } else { 0 })
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn hlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hlen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => RespFrame::integer(fields.len() as i64),
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn hmget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"hmget");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut items = Vec::with_capacity(argv.len() - 2);
                for field in &argv[2..] {
                    let item = match fields.get(field) {
                        Some(value) => bulk(value),
                        None => RespFrame::null_bulk(),
                    };
                    items.push(item);
                }
                RespFrame::array(items)
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => {
                let items = (2..argv.len()).map(|_| RespFrame::null_bulk()).collect();
                RespFrame::array(items)
            }
        }
    }

    fn hkeys_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hkeys");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut keys: Vec<_> = fields.keys().collect();
                keys.sort();
                RespFrame::array(keys.into_iter().map(|field| bulk(field)).collect())
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hvals_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hvals");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut pairs: Vec<_> = fields.iter().collect();
                pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
                RespFrame::array(pairs.into_iter().map(|(_, value)| bulk(value)).collect())
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hstrlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hstrlen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => match fields.get(&argv[2]) {
                Some(value) => RespFrame::integer(value.len() as i64),
                None => RespFrame::integer(0),
            },
            Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn hsetnx_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"hsetnx");
        }
        self.purge_if_expired(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                if fields.contains_key(&argv[2]) {
                    return RespFrame::integer(0);
                }
                fields.insert(argv[2].clone(), argv[3].clone());
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_),
                ..
            }) => wrong_type(),
            None => {
                let mut fields = HashMap::new();
                fields.insert(argv[2].clone(), argv[3].clone());
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
        }
    }

    fn hincrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"hincrby");
        }
        let Some(incr) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_if_expired(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_),
                ..
            }) => return wrong_type(),
            None => {
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(HashMap::new()),
                        expire_at_ms,
                    },
                );
                match &mut self.db.get_mut(&argv[1]).expect("just inserted").value {
                    StoredValue::Hash(fields) => fields,
                    _ => unreachable!(),
                }
            }
        };
        let current = match fields.get(&argv[2]) {
            Some(value) => match parse_i64(value) {
                Some(n) => n,
                None => return err(b"ERR hash value is not an integer"),
            },
            None => 0,
        };
        let Some(next) = current.checked_add(incr) else {
            return err(b"ERR increment or decrement would overflow");
        };
        fields.insert(argv[2].clone(), next.to_string().into_bytes());
        self.note_write(&argv[1]);
        RespFrame::integer(next)
    }

    fn hmset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 || argv.len() % 2 != 0 {
            return wrong_arity(b"hmset");
        }
        self.purge_if_expired(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                for pair in argv[2..].chunks_exact(2) {
                    fields.insert(pair[0].clone(), pair[1].clone());
                }
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_),
                ..
            }) => return wrong_type(),
            None => {
                let mut fields = HashMap::new();
                for pair in argv[2..].chunks_exact(2) {
                    fields.insert(pair[0].clone(), pair[1].clone());
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
            }
        }
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    /// ZADD with the NX | XX | CH flag subset. Mirrors the reference
    /// server's check order: flag parse, pair-shape syntax check, NX+XX
    /// conflict, score parse for every pair, then the WRONGTYPE check.
    fn zadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zadd");
        }
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;
        let mut index = 2;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"NX") {
                nx = true;
            } else if ascii_eq(&argv[index], b"XX") {
                xx = true;
            } else if ascii_eq(&argv[index], b"CH") {
                ch = true;
            } else {
                break;
            }
            index += 1;
        }
        let pairs = &argv[index..];
        if pairs.is_empty() || pairs.len() % 2 != 0 {
            return err(b"ERR syntax error");
        }
        if nx && xx {
            return err(b"ERR XX and NX options at the same time are not compatible");
        }
        let mut scored = Vec::with_capacity(pairs.len() / 2);
        for pair in pairs.chunks_exact(2) {
            let Some(score) = parse_score(&pair[0]) else {
                return err(b"ERR value is not a valid float");
            };
            scored.push((normalize_zero(score), pair[1].clone()));
        }
        self.purge_if_expired(&argv[1]);
        let (added, updated) = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => apply_zadd(members, scored, nx, xx),
            Some(_) => return wrong_type(),
            None => {
                if xx {
                    (0, 0)
                } else {
                    let mut members = HashMap::new();
                    let counts = apply_zadd(&mut members, scored, nx, xx);
                    self.db.insert(
                        argv[1].clone(),
                        Entry {
                            value: StoredValue::ZSet(members),
                            expire_at_ms: None,
                        },
                    );
                    counts
                }
            }
        };
        if added + updated > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(if ch { added + updated } else { added })
    }

    fn zscore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"zscore");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => match members.get(&argv[2]) {
                Some(score) => bulk(format_score(*score)),
                None => RespFrame::null_bulk(),
            },
            Some(_) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn zincrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zincrby");
        }
        let Some(increment) = parse_score(&argv[2]) else {
            return err(b"ERR value is not a valid float");
        };
        self.purge_if_expired(&argv[1]);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let next = match members.get(&argv[3]) {
                    Some(current) => current + increment,
                    None => increment,
                };
                if next.is_nan() {
                    return err(b"ERR resulting score is not a number (NaN)");
                }
                members.insert(argv[3].clone(), normalize_zero(next));
                self.note_write(&argv[1]);
                bulk(format_score(next))
            }
            Some(_) => wrong_type(),
            None => {
                let mut members = HashMap::new();
                members.insert(argv[3].clone(), normalize_zero(increment));
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::ZSet(members),
                        expire_at_ms: None,
                    },
                );
                self.note_write(&argv[1]);
                bulk(format_score(increment))
            }
        }
    }

    fn zrem_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"zrem");
        }
        self.purge_if_expired(&argv[1]);
        let mut remove_empty_zset = false;
        let mut deleted = 0;
        let response = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                for member in &argv[2..] {
                    if members.remove(member).is_some() {
                        deleted += 1;
                    }
                }
                remove_empty_zset = members.is_empty();
                RespFrame::integer(deleted)
            }
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        };
        if remove_empty_zset {
            self.db.remove(&argv[1]);
        }
        if deleted > 0 {
            self.note_write(&argv[1]);
        }
        response
    }

    fn zcard_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"zcard");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => RespFrame::integer(members.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn zrank_command(&mut self, argv: &[Vec<u8>], reverse: bool) -> RespFrame {
        let name: &[u8] = if reverse { b"zrevrank" } else { b"zrank" };
        if argv.len() < 3 || argv.len() > 4 {
            return wrong_arity(name);
        }
        if argv.len() == 4 {
            return err(b"ERR syntax error");
        }
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::null_bulk(),
        };
        let ordered = sorted_zset_entries(members);
        match ordered.iter().position(|(member, _)| member == &argv[2]) {
            Some(rank) => {
                let rank = if reverse {
                    ordered.len() - 1 - rank
                } else {
                    rank
                };
                RespFrame::integer(rank as i64)
            }
            None => RespFrame::null_bulk(),
        }
    }

    /// ZRANGE in its index form only, with the REV and WITHSCORES options.
    fn zrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zrange");
        }
        let mut rev = false;
        let mut withscores = false;
        for option in &argv[4..] {
            if ascii_eq(option, b"REV") {
                rev = true;
            } else if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let mut ordered = sorted_zset_entries(members);
        if rev {
            ordered.reverse();
        }
        let len = ordered.len() as i64;
        let mut start = if start < 0 { start + len } else { start };
        let mut stop = if stop < 0 { stop + len } else { stop };
        if start < 0 {
            start = 0;
        }
        if start > stop || start >= len {
            return RespFrame::array(Vec::new());
        }
        if stop >= len {
            stop = len - 1;
        }
        let mut items = Vec::new();
        for (member, score) in &ordered[start as usize..=stop as usize] {
            items.push(bulk(member));
            if withscores {
                items.push(bulk(format_score(*score)));
            }
        }
        RespFrame::array(items)
    }

    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`, the
    /// score form only. Read-only: never marks a mutation. Mirrors the
    /// reference command's argument order, parsing the trailing options
    /// (WITHSCORES, LIMIT) before the min/max bounds so that a bogus option or
    /// a non-integer LIMIT argument is reported ahead of a malformed bound,
    /// matching `zrangeGenericCommand`.
    fn zrangebyscore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zrangebyscore");
        }
        let mut withscores = false;
        let mut limit: Option<(i64, i64)> = None;
        let mut index = 4;
        while index < argv.len() {
            let option = &argv[index];
            if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
                index += 1;
            } else if ascii_eq(option, b"LIMIT") && argv.len() - index - 1 >= 2 {
                let Some(offset) = parse_limit_arg(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                let Some(count) = parse_limit_arg(&argv[index + 2]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let Some(min) = parse_score_bound(&argv[2]) else {
            return err(b"ERR min or max is not a float");
        };
        let Some(max) = parse_score_bound(&argv[3]) else {
            return err(b"ERR min or max is not a float");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let in_range: Vec<(Vec<u8>, f64)> = sorted_zset_entries(members)
            .into_iter()
            .filter(|(_, score)| min.gte_min(*score) && max.lte_max(*score))
            .collect();
        let selected = apply_score_limit(&in_range, limit);
        let mut items = Vec::new();
        for (member, score) in selected {
            items.push(bulk(member));
            if withscores {
                items.push(bulk(format_score(*score)));
            }
        }
        RespFrame::array(items)
    }

    /// The generic EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT implementation,
    /// mirroring `expireGenericCommand` (`expire.c`). `unit` scales `argv[2]`;
    /// `absolute` selects the basetime (0 for the *AT variants, the current
    /// wall clock for the relative variants). The optional trailing flag is
    /// parsed exactly like `parseExtendedExpireArgumentsOrReply`, including the
    /// NX/XX/GT/LT conflict checks.
    fn expire_command(
        &mut self,
        argv: &[Vec<u8>],
        unit: ExpireUnit,
        absolute: bool,
    ) -> RespFrame {
        let name: &[u8] = match (unit, absolute) {
            (ExpireUnit::Seconds, false) => b"expire",
            (ExpireUnit::Milliseconds, false) => b"pexpire",
            (ExpireUnit::Seconds, true) => b"expireat",
            (ExpireUnit::Milliseconds, true) => b"pexpireat",
        };
        if argv.len() < 3 {
            return wrong_arity(name);
        }

        let condition = match parse_expire_condition(&argv[3..]) {
            Ok(condition) => condition,
            Err(frame) => return frame,
        };

        let Some(mut when) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };

        if unit == ExpireUnit::Seconds {
            if when > i64::MAX / 1000 || when < i64::MIN / 1000 {
                return invalid_expire_time(name);
            }
            when *= 1000;
        }

        let basetime: i64 = if absolute {
            0
        } else {
            self.host.now_millis() as i64
        };
        if when > i64::MAX - basetime {
            return invalid_expire_time(name);
        }
        when += basetime;
        if when < 0 {
            when = 0;
        }

        self.purge_if_expired(&argv[1]);
        let current_expire: Option<u64> = match self.db.get(&argv[1]) {
            Some(entry) => entry.expire_at_ms,
            None => return RespFrame::integer(0),
        };

        if let Some(condition) = condition {
            let current = current_expire.map(|value| value as i64);
            let blocked = match condition {
                ExpireCondition::Nx => current.is_some(),
                ExpireCondition::Xx => current.is_none(),
                ExpireCondition::Gt => match current {
                    None => true,
                    Some(value) => when <= value,
                },
                ExpireCondition::Lt => match current {
                    None => false,
                    Some(value) => when >= value,
                },
            };
            if blocked {
                return RespFrame::integer(0);
            }
        }

        if when <= self.host.now_millis() as i64 {
            self.db.remove(&argv[1]);
            self.note_write(&argv[1]);
            return RespFrame::integer(1);
        }

        if let Some(entry) = self.db.get_mut(&argv[1]) {
            entry.expire_at_ms = Some(when as u64);
            self.note_write(&argv[1]);
        }
        RespFrame::integer(1)
    }

    /// PERSIST key (`persistCommand`, `expire.c`): drop a key's TTL, replying
    /// :1 only when a TTL was actually removed.
    fn persist_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"persist");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.get_mut(&argv[1]) {
            Some(entry) if entry.expire_at_ms.is_some() => {
                entry.expire_at_ms = None;
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
            _ => RespFrame::integer(0),
        }
    }

    /// TTL / PTTL / EXPIRETIME / PEXPIRETIME (`ttlGenericCommand`, `expire.c`).
    /// `milliseconds` selects ms vs second granularity; `absolute` returns the
    /// absolute expiry timestamp rather than the remaining TTL.
    fn ttl_command(&mut self, argv: &[Vec<u8>], milliseconds: bool, absolute: bool) -> RespFrame {
        let name: &[u8] = match (milliseconds, absolute) {
            (false, false) => b"ttl",
            (true, false) => b"pttl",
            (false, true) => b"expiretime",
            (true, true) => b"pexpiretime",
        };
        if argv.len() != 2 {
            return wrong_arity(name);
        }
        self.purge_if_expired(&argv[1]);
        let Some(entry) = self.db.get(&argv[1]) else {
            return RespFrame::integer(-2);
        };
        let Some(expire_at_ms) = entry.expire_at_ms else {
            return RespFrame::integer(-1);
        };
        let ttl = if absolute {
            expire_at_ms
        } else {
            expire_at_ms.saturating_sub(self.host.now_millis())
        };
        if milliseconds {
            RespFrame::integer(ttl as i64)
        } else {
            RespFrame::integer(((ttl + 500) / 1000) as i64)
        }
    }

    /// TYPE key (`typeCommand`, `db.c`): a simple-string naming the value's
    /// type, or `none` for a missing key. Only the variants the engine models
    /// today are reachable; later type waves extend `StoredValue::type_name`.
    fn type_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"type");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.get(&argv[1]) {
            Some(entry) => simple(entry.value.type_name()),
            None => simple(b"none"),
        }
    }

    /// RENAME / RENAMENX (`renameGenericCommand`, `db.c`). The source value and
    /// its TTL move to the destination, overwriting any existing destination
    /// (RENAME) unless RENAMENX finds the destination already present. A missing
    /// source replies "ERR no such key". src == dst is a no-op that still
    /// validates the source exists.
    fn rename_command(&mut self, argv: &[Vec<u8>], nx: bool) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(if nx { b"renamenx" } else { b"rename" });
        }
        let samekey = argv[1] == argv[2];
        self.purge_if_expired(&argv[1]);
        if !self.db.contains_key(&argv[1]) {
            return err(b"ERR no such key");
        }
        if samekey {
            return if nx {
                RespFrame::integer(0)
            } else {
                simple(b"OK")
            };
        }
        self.purge_if_expired(&argv[2]);
        if self.db.contains_key(&argv[2]) && nx {
            return RespFrame::integer(0);
        }
        let entry = self.db.remove(&argv[1]).expect("source verified present");
        self.db.insert(argv[2].clone(), entry);
        self.note_write(&argv[1]);
        self.note_write(&argv[2]);
        if nx {
            RespFrame::integer(1)
        } else {
            simple(b"OK")
        }
    }

    /// COPY src dst [REPLACE] (`copyCommand`, `db.c`). Deep-copies the source
    /// value and its TTL to the destination. Replies :0 when the source is
    /// missing, or when the destination already exists without REPLACE. src ==
    /// dst replies the same-object error. The optional DB target form is out of
    /// scope for the single-database edge engine.
    fn copy_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"copy");
        }
        let mut replace = false;
        let mut index = 3;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"REPLACE") {
                replace = true;
                index += 1;
            } else {
                return err(b"ERR syntax error");
            }
        }
        if argv[1] == argv[2] {
            return err(b"ERR source and destination objects are the same");
        }
        self.purge_if_expired(&argv[1]);
        let Some(entry) = self.db.get(&argv[1]).cloned() else {
            return RespFrame::integer(0);
        };
        self.purge_if_expired(&argv[2]);
        if self.db.contains_key(&argv[2]) && !replace {
            return RespFrame::integer(0);
        }
        self.db.insert(argv[2].clone(), entry);
        self.note_write(&argv[2]);
        RespFrame::integer(1)
    }

    /// TOUCH key [key ...] (`touchCommand`, `expire.c`): count of existing keys
    /// with no mutation.
    fn touch_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"touch");
        }
        let mut touched = 0;
        for key in &argv[1..] {
            if self.get_value(key).is_some() {
                touched += 1;
            }
        }
        RespFrame::integer(touched)
    }

    /// PING [message] (`pingCommand`, `server.c`): +PONG with no argument, the
    /// echoed message otherwise. More than one argument is an arity error.
    fn ping_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() > 2 {
            return wrong_arity(b"ping");
        }
        if argv.len() == 2 {
            bulk(argv[1].clone())
        } else {
            simple(b"PONG")
        }
    }

    /// ECHO message (`echoCommand`, `server.c`): bulk-string echo of the
    /// argument.
    fn echo_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"echo");
        }
        bulk(argv[1].clone())
    }

    /// FLUSHALL [ASYNC|SYNC] (`flushallCommand`, `db.c`): clear every key and
    /// reply +OK. Every removed key is marked dirty *before* the database is
    /// cleared, so a host flushes the deletions back to storage.
    fn flushall_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() == 2 {
            if !(ascii_eq(&argv[1], b"ASYNC") || ascii_eq(&argv[1], b"SYNC")) {
                return err(b"ERR syntax error");
            }
        } else if argv.len() != 1 {
            return err(b"ERR syntax error");
        }
        self.mark_all_dirty();
        self.db.clear();
        simple(b"OK")
    }

    fn script_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"script");
        }
        if ascii_eq(&argv[1], b"LOAD") {
            if argv.len() != 3 {
                return wrong_arity(b"script|load");
            }
            if let Some(message) = compile_error_message(&argv[2]) {
                return compile_error_reply(&message);
            }
            let sha = sha1_hex(&argv[2]);
            self.cache_script(sha, &argv[2]);
            bulk(sha.to_vec())
        } else if ascii_eq(&argv[1], b"EXISTS") {
            if argv.len() < 3 {
                return wrong_arity(b"script|exists");
            }
            RespFrame::array(
                argv[2..]
                    .iter()
                    .map(|raw| {
                        let exists = normalise_sha(raw)
                            .map(|sha| self.scripts.contains_key(&sha))
                            .unwrap_or(false);
                        RespFrame::integer(if exists { 1 } else { 0 })
                    })
                    .collect(),
            )
        } else if ascii_eq(&argv[1], b"FLUSH") {
            if argv.len() == 3 {
                if !(ascii_eq(&argv[2], b"SYNC") || ascii_eq(&argv[2], b"ASYNC")) {
                    return err(b"ERR SCRIPT FLUSH only support SYNC|ASYNC option");
                }
            } else if argv.len() != 2 {
                return err(b"ERR SCRIPT FLUSH only support SYNC|ASYNC option");
            }
            self.clear_script_cache();
            simple(b"OK")
        } else {
            let mut msg = b"ERR unknown subcommand '".to_vec();
            msg.extend_from_slice(&argv[1]);
            msg.extend_from_slice(b"'. Try SCRIPT HELP.");
            err(&msg)
        }
    }

    /// EVAL registers the script in the cache exactly like the reference
    /// server, so a later SCRIPT EXISTS / EVALSHA of the same body succeeds.
    /// The cache insert deliberately does not bump the mutation epoch: the
    /// script cache is excluded from snapshots.
    fn eval_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"eval");
        }
        let numkeys = match validate_numkeys(&argv[2..]) {
            Ok(numkeys) => numkeys,
            Err(frame) => return frame,
        };
        if let Some(message) = compile_error_message(&argv[1]) {
            return compile_error_reply(&message);
        }
        let sha = sha1_hex(&argv[1]);
        self.cache_script(sha, &argv[1]);
        self.eval_script(&argv[1], &argv[2..], numkeys)
    }

    fn evalsha_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"evalsha");
        }
        let numkeys = match validate_numkeys(&argv[2..]) {
            Ok(numkeys) => numkeys,
            Err(frame) => return frame,
        };
        let Some(sha) = normalise_sha(&argv[1]) else {
            return err(b"NOSCRIPT No matching script.");
        };
        let Some(script) = self.scripts.get(&sha).cloned() else {
            return err(b"NOSCRIPT No matching script.");
        };
        self.touch_script(&sha);
        self.eval_script(&script, &argv[2..], numkeys)
    }

    fn eval_script(&mut self, script: &[u8], rest: &[Vec<u8>], numkeys: usize) -> RespFrame {
        let keys = &rest[1..1 + numkeys];
        let args = &rest[1 + numkeys..];
        match self.run_lua_script(script, keys, args) {
            Ok(frame) => frame,
            Err(error) => {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(error.message_lossy().as_bytes());
                err(&msg)
            }
        }
    }

    /// Runs `script` wrapped in a Lua-level pcall harness.
    ///
    /// The harness implements Redis's Lua error semantics entirely inside the
    /// interpreter: `redis.call` raises on an error reply while `redis.pcall`
    /// returns it, and an otherwise-uncaught script error becomes an `{err=...}`
    /// reply instead of aborting the call. The host `redis.call`/`redis.pcall`
    /// functions both return `{err=...}` tables (never raise); the prelude
    /// rebinds `redis.call` in Lua to re-raise that table, and the suffix's
    /// `pcall` catches everything and returns it. Errors therefore stay Lua
    /// values throughout — `lua_to_resp` maps the `{err=...}` table to a RESP
    /// error verbatim, preserving codes such as WRONGTYPE from `redis.call`,
    /// and the engine never has to construct (or correctly root) a
    /// value-carrying Rust error.
    ///
    /// Historically this also dodged an omnilua use-after-sweep when an error
    /// value was raised through `lua.scope` — fixed upstream in omnilua 0.2.0
    /// (issue #189, regression-tested in `scope_error_rooting.rs`), so the
    /// harness is now a deliberate fidelity choice, not a safety workaround.
    fn run_lua_script(
        &mut self,
        script: &[u8],
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
    ) -> lua_rs_runtime::Result<RespFrame> {
        let lua = Lua::new_versioned(LuaVersion::V51);
        install_sandbox(&lua)?;
        install_keys_argv(&lua, keys, args)?;

        let engine_cell: RefCell<&mut Engine<H>> = RefCell::new(self);
        let value = lua.scope(|scope| {
            let redis_table = lua.create_table()?;

            let call_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        host_call(cell, lua_inner, call_args)
                    },
                )?
            };

            let pcall_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        host_call(cell, lua_inner, call_args)
                    },
                )?
            };

            let status_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("ok", msg)?;
                    Ok(table)
                },
            )?;
            let error_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("err", msg)?;
                    Ok(table)
                },
            )?;
            let sha1hex_fn = lua.create_function(
                |_lua_inner, msg: LuaString| -> lua_rs_runtime::Result<String> {
                    let hex = sha1_hex(&msg.as_bytes()?);
                    Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
                },
            )?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;
            redis_table.set("status_reply", status_reply_fn)?;
            redis_table.set("error_reply", error_reply_fn)?;
            redis_table.set("sha1hex", sha1hex_fn)?;
            lua.globals().set("redis", redis_table.clone())?;
            lua.globals().set("server", redis_table)?;

            lua.load(wrap_script_in_pcall_harness(script))
                .set_name("user_script")
                .eval::<LuaValue>()
        })?;
        lua_to_resp(&value)
    }

    fn get_value(&mut self, key: &[u8]) -> Option<&Entry> {
        self.purge_if_expired(key);
        self.db.get(key)
    }

    fn purge_if_expired(&mut self, key: &[u8]) {
        let expired = self
            .db
            .get(key)
            .and_then(|entry| entry.expire_at_ms)
            .is_some_and(|deadline| deadline <= self.host.now_millis());
        if expired {
            self.db.remove(key);
        }
    }

    fn purge_expired_keys(&mut self) {
        let now = self.host.now_millis();
        self.db
            .retain(|_, entry| !entry.expire_at_ms.is_some_and(|deadline| deadline <= now));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestMethod {
    Get,
    Post,
    Put,
    Head,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestResponseFormat {
    Json,
    Resp2,
}

#[derive(Debug, Clone, Copy)]
pub struct RestRequest<'a> {
    pub method: RestMethod,
    pub path: &'a str,
    pub body: &'a [u8],
    pub response_format: RestResponseFormat,
}

impl<'a> RestRequest<'a> {
    pub fn get(path: &'a str) -> Self {
        Self {
            method: RestMethod::Get,
            path,
            body: &[],
            response_format: RestResponseFormat::Json,
        }
    }

    pub fn post(path: &'a str, body: &'a [u8]) -> Self {
        Self {
            method: RestMethod::Post,
            path,
            body,
            response_format: RestResponseFormat::Json,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl RestResponse {
    fn json_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        match serde_json::to_vec(&rest_frame_json(frame)) {
            Ok(body) => Self {
                status,
                content_type: APPLICATION_JSON,
                body,
            },
            Err(_) => Self::json_error(500, b"ERR JSON encode failed"),
        }
    }

    fn resp2_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        let mut body = Vec::new();
        encode_resp2(&frame, &mut body);
        Self {
            status,
            content_type: APPLICATION_OCTET_STREAM,
            body,
        }
    }

    fn json_error(status: u16, message: &[u8]) -> Self {
        let body = serde_json::to_vec(&json!({
            "error": String::from_utf8_lossy(message)
        }))
        .unwrap_or_else(|_| b"{\"error\":\"ERR JSON encode failed\"}".to_vec());
        Self {
            status,
            content_type: APPLICATION_JSON,
            body,
        }
    }
}

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";

enum RestCommand {
    Single(Vec<Vec<u8>>),
    Pipeline(Vec<Vec<Vec<u8>>>),
}

fn rest_command_from_request(request: RestRequest<'_>) -> Result<RestCommand, Vec<u8>> {
    let (path, query) = split_path_query(request.path);
    let segments = parse_path_segments(path)?;
    let is_pipeline = segments
        .first()
        .is_some_and(|segment| ascii_eq(segment, b"pipeline"));

    if is_pipeline {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR pipeline requires POST or PUT".to_vec());
        }
        return parse_pipeline_body(request.body);
    }

    if segments.is_empty() {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR missing command".to_vec());
        }
        return parse_single_command_body(request.body).map(RestCommand::Single);
    }

    if matches!(request.method, RestMethod::Post | RestMethod::Put)
        && body_looks_like_json_array(request.body)
    {
        let argv = parse_single_command_body(request.body)?;
        if argv
            .first()
            .is_some_and(|command| ascii_eq(command, &segments[0]))
        {
            return Ok(RestCommand::Single(argv));
        }
    }

    let mut argv = segments;
    if matches!(request.method, RestMethod::Post | RestMethod::Put) && !request.body.is_empty() {
        argv.push(request.body.to_vec());
    }
    append_query_args(query, &mut argv)?;
    Ok(RestCommand::Single(argv))
}

fn split_path_query(path: &str) -> (&str, &str) {
    match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    }
}

fn parse_path_segments(path: &str) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| percent_decode(segment.as_bytes(), false))
        .collect()
}

fn append_query_args(query: &str, argv: &mut Vec<Vec<u8>>) -> Result<(), Vec<u8>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "_token" {
            continue;
        }
        argv.push(percent_decode(key.as_bytes(), true)?);
        if !value.is_empty() {
            argv.push(percent_decode(value.as_bytes(), true)?);
        }
    }
    Ok(())
}

fn percent_decode(input: &[u8], plus_as_space: bool) -> Result<Vec<u8>, Vec<u8>> {
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'%' => {
                if index + 2 >= input.len() {
                    return Err(b"ERR invalid URL escape".to_vec());
                }
                let Some(high) = hex_nibble(input[index + 1]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                let Some(low) = hex_nibble(input[index + 2]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                out.push((high << 4) | low);
                index += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn body_looks_like_json_array(body: &[u8]) -> bool {
    body.iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[')
}

fn parse_single_command_body(body: &[u8]) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON command body".to_vec())?;
    json_array_to_argv(&json)
}

fn parse_pipeline_body(body: &[u8]) -> Result<RestCommand, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON pipeline body".to_vec())?;
    let Some(rows) = json.as_array() else {
        return Err(b"ERR pipeline body must be an array".to_vec());
    };
    let mut commands = Vec::with_capacity(rows.len());
    for row in rows {
        commands.push(json_array_to_argv(row)?);
    }
    Ok(RestCommand::Pipeline(commands))
}

fn json_array_to_argv(value: &JsonValue) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let Some(items) = value.as_array() else {
        return Err(b"ERR command body must be an array".to_vec());
    };
    if items.is_empty() {
        return Err(b"ERR command body must not be empty".to_vec());
    }
    let mut argv = Vec::with_capacity(items.len());
    for item in items {
        argv.push(json_arg_to_bytes(item)?);
    }
    Ok(argv)
}

fn json_arg_to_bytes(value: &JsonValue) -> Result<Vec<u8>, Vec<u8>> {
    match value {
        JsonValue::String(value) => Ok(value.as_bytes().to_vec()),
        JsonValue::Number(value) => Ok(value.to_string().into_bytes()),
        JsonValue::Bool(true) => Ok(b"true".to_vec()),
        JsonValue::Bool(false) => Ok(b"false".to_vec()),
        JsonValue::Null => Ok(b"null".to_vec()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Err(b"ERR command arguments must be scalar JSON values".to_vec())
        }
    }
}

fn rest_status_for_frame(frame: &RespFrame) -> u16 {
    match frame {
        RespFrame::Error(_) | RespFrame::BulkError(_) => 400,
        _ => 200,
    }
}

fn rest_content_type(format: RestResponseFormat) -> &'static str {
    match format {
        RestResponseFormat::Json => APPLICATION_JSON,
        RestResponseFormat::Resp2 => APPLICATION_OCTET_STREAM,
    }
}

/// Serialize one key's entry into the canonical JSON object used both inside a
/// full snapshot and as a standalone per-key value. Keys, hash fields, and
/// zset members are hex-encoded; zset members are sorted by member and scores
/// use the lossless snapshot string form so a round-trip is exact.
fn encode_entry(key: &[u8], entry: &Entry) -> JsonMap<String, JsonValue> {
    let mut object = JsonMap::new();
    object.insert("key".to_owned(), JsonValue::String(hex_encode(key)));
    if let Some(expire_at_ms) = entry.expire_at_ms {
        object.insert("expire_at_ms".to_owned(), json!(expire_at_ms));
    }
    match &entry.value {
        StoredValue::String(value) => {
            object.insert("type".to_owned(), JsonValue::String("string".to_owned()));
            object.insert("value".to_owned(), JsonValue::String(hex_encode(value)));
        }
        StoredValue::Hash(fields) => {
            let mut field_items: Vec<_> = fields.iter().collect();
            field_items.sort_by(|(left, _), (right, _)| left.cmp(right));
            object.insert("type".to_owned(), JsonValue::String("hash".to_owned()));
            object.insert(
                "fields".to_owned(),
                JsonValue::Array(
                    field_items
                        .into_iter()
                        .map(|(field, value)| {
                            JsonValue::Array(vec![
                                JsonValue::String(hex_encode(field)),
                                JsonValue::String(hex_encode(value)),
                            ])
                        })
                        .collect(),
                ),
            );
        }
        StoredValue::ZSet(members) => {
            let mut member_items: Vec<_> = members.iter().collect();
            member_items.sort_by(|(left, _), (right, _)| left.cmp(right));
            object.insert("type".to_owned(), JsonValue::String("zset".to_owned()));
            object.insert(
                "members".to_owned(),
                JsonValue::Array(
                    member_items
                        .into_iter()
                        .map(|(member, score)| {
                            JsonValue::Array(vec![
                                JsonValue::String(hex_encode(member)),
                                JsonValue::String(score_snapshot_string(*score)),
                            ])
                        })
                        .collect(),
                ),
            );
        }
    }
    object
}

/// Inverse of `encode_entry`: decode one JSON key object into `(key, entry)`.
fn decode_entry(object: &JsonMap<String, JsonValue>) -> Result<(Vec<u8>, Entry), SnapshotError> {
    let key = hex_decode(
        object
            .get("key")
            .and_then(JsonValue::as_str)
            .ok_or(SnapshotError::MissingField("key"))?,
    )?;
    let expire_at_ms = match object.get("expire_at_ms") {
        Some(value) => Some(
            value
                .as_u64()
                .ok_or(SnapshotError::InvalidField("expire_at_ms"))?,
        ),
        None => None,
    };
    let value = match object
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or(SnapshotError::MissingField("type"))?
    {
        "string" => StoredValue::String(hex_decode(
            object
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or(SnapshotError::MissingField("value"))?,
        )?),
        "hash" => {
            let fields = object
                .get("fields")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("fields"))?;
            let mut decoded_fields = HashMap::new();
            for pair in fields {
                let pair = pair
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("fields"))?;
                if pair.len() != 2 {
                    return Err(SnapshotError::InvalidField("fields"));
                }
                let field = hex_decode(
                    pair[0]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("fields"))?,
                )?;
                let value = hex_decode(
                    pair[1]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("fields"))?,
                )?;
                decoded_fields.insert(field, value);
            }
            StoredValue::Hash(decoded_fields)
        }
        "zset" => {
            let members = object
                .get("members")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("members"))?;
            let mut decoded_members = HashMap::new();
            for pair in members {
                let pair = pair
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("members"))?;
                if pair.len() != 2 {
                    return Err(SnapshotError::InvalidField("members"));
                }
                let member = hex_decode(
                    pair[0]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("members"))?,
                )?;
                let score = pair[1]
                    .as_str()
                    .and_then(|text| parse_score(text.as_bytes()))
                    .ok_or(SnapshotError::InvalidField("members"))?;
                decoded_members.insert(member, score);
            }
            StoredValue::ZSet(decoded_members)
        }
        _ => return Err(SnapshotError::InvalidField("type")),
    };
    Ok((
        key,
        Entry {
            value,
            expire_at_ms,
        },
    ))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, SnapshotError> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(SnapshotError::InvalidHex);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or(SnapshotError::InvalidHex)?;
        let low = hex_nibble(pair[1]).ok_or(SnapshotError::InvalidHex)?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn rest_frame_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Error(message) | RespFrame::BulkError(message) => json!({
            "error": String::from_utf8_lossy(message.as_bytes())
        }),
        other => json!({
            "result": resp_frame_result_json(other)
        }),
    }
}

fn resp_frame_result_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Simple(value)
        | RespFrame::Bulk(Some(value))
        | RespFrame::BigNumber(value)
        | RespFrame::VerbatimString { data: value, .. } => {
            JsonValue::String(String::from_utf8_lossy(value.as_bytes()).into_owned())
        }
        RespFrame::Integer(value) => json!(value),
        RespFrame::Bulk(None) | RespFrame::Array(None) | RespFrame::Null => JsonValue::Null,
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            JsonValue::Array(items.into_iter().map(resp_frame_result_json).collect())
        }
        RespFrame::Boolean(value) => JsonValue::Bool(value),
        RespFrame::Double(value) => json!(value),
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => JsonValue::Array(
            pairs
                .into_iter()
                .flat_map(|(key, value)| {
                    [resp_frame_result_json(key), resp_frame_result_json(value)]
                })
                .collect(),
        ),
        RespFrame::Error(message) | RespFrame::BulkError(message) => {
            JsonValue::String(String::from_utf8_lossy(message.as_bytes()).into_owned())
        }
    }
}

fn install_sandbox(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let globals = lua.globals();
    for name in [
        "io",
        "debug",
        "package",
        "require",
        "load",
        "loadfile",
        "dofile",
        "loadstring",
        "print",
        "getfenv",
        "setfenv",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

fn install_keys_argv(lua: &Lua, keys: &[Vec<u8>], args: &[Vec<u8>]) -> lua_rs_runtime::Result<()> {
    let keys_table = lua.create_table()?;
    for (index, key) in keys.iter().enumerate() {
        keys_table.set(index as i64 + 1, lua.create_string(key)?)?;
    }
    lua.globals().set("KEYS", keys_table)?;

    let argv_table = lua.create_table()?;
    for (index, arg) in args.iter().enumerate() {
        argv_table.set(index as i64 + 1, lua.create_string(arg)?)?;
    }
    lua.globals().set("ARGV", argv_table)?;
    Ok(())
}

/// Host side of `redis.call`/`redis.pcall`. Always returns errors as
/// `{err=...}` tables; the script harness prelude turns them into raised
/// Lua errors for `redis.call`. Argument-conversion failures use the
/// reference server's wording.
fn host_call<H: Host>(
    cell: &RefCell<&mut Engine<H>>,
    lua: &Lua,
    call_args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<LuaValue> {
    let argv = match collect_lua_args(call_args) {
        Ok(argv) => argv,
        Err(_) => return error_table(lua, b"ERR Command arguments must be strings or integers"),
    };
    let mut engine = cell.borrow_mut();
    match engine.execute_inner(&argv, true) {
        RespFrame::Error(message) => error_table(lua, message.as_bytes()),
        frame => resp_to_lua(lua, &frame),
    }
}

/// Single-line prelude (so user code starts on chunk line 2) that rebinds
/// `redis.call` to raise `{err=...}` tables, then opens the function the
/// user script becomes the body of. See `run_lua_script` for why.
const SCRIPT_HARNESS_PRELUDE: &[u8] = b"local __edge_raw_pcall = redis.pcall redis.call = function(...) local __edge_reply = __edge_raw_pcall(...) if type(__edge_reply) == 'table' and __edge_reply.err ~= nil then error(__edge_reply) end return __edge_reply end local __edge_fn = function()\n";

const SCRIPT_HARNESS_SUFFIX: &[u8] = b"\nend local __edge_ok, __edge_res = pcall(__edge_fn) if __edge_ok then return __edge_res end if type(__edge_res) == 'table' and __edge_res.err ~= nil then return __edge_res end return {err='ERR ' .. tostring(__edge_res)}\n";

fn wrap_script_in_pcall_harness(script: &[u8]) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(
        SCRIPT_HARNESS_PRELUDE.len() + script.len() + SCRIPT_HARNESS_SUFFIX.len(),
    );
    wrapped.extend_from_slice(SCRIPT_HARNESS_PRELUDE);
    wrapped.extend_from_slice(script);
    wrapped.extend_from_slice(SCRIPT_HARNESS_SUFFIX);
    wrapped
}

/// Compiles `script` in a throwaway interpreter without executing any of it
/// (the probe chunk only defines a local function), so SCRIPT LOAD and EVAL
/// can reject syntax errors up front like the reference server does.
fn compile_error_message(script: &[u8]) -> Option<String> {
    let lua = Lua::new_versioned(LuaVersion::V51);
    let mut probe = Vec::with_capacity(script.len() + 40);
    probe.extend_from_slice(b"local __edge_fn = function()\n");
    probe.extend_from_slice(script);
    probe.extend_from_slice(b"\nend");
    match lua.load(probe).set_name("user_script").exec() {
        Ok(()) => None,
        Err(error) => Some(error.message_lossy()),
    }
}

fn compile_error_reply(message: &str) -> RespFrame {
    let mut msg = b"ERR Error compiling script (new function): ".to_vec();
    msg.extend_from_slice(message.as_bytes());
    err(&msg)
}

/// Numkeys validation in the reference server's check order: integer parse,
/// then the greater-than-args check, then the negative check.
fn validate_numkeys(rest: &[Vec<u8>]) -> Result<usize, RespFrame> {
    let Some(numkeys) = parse_i64(&rest[0]) else {
        return Err(err(b"ERR value is not an integer or out of range"));
    };
    let available = (rest.len() - 1) as i64;
    if numkeys > available {
        return Err(err(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    if numkeys < 0 {
        return Err(err(b"ERR Number of keys can't be negative"));
    }
    Ok(numkeys as usize)
}

/// Formats the unknown-command error exactly like the reference server:
/// each argument rendered as 'arg' plus a trailing space, stopping once the
/// rendered argument text reaches 128 bytes.
fn unknown_command_error(command: &[u8], args: &[Vec<u8>]) -> RespFrame {
    let mut msg = b"ERR unknown command '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"', with args beginning with: ");
    let mut rendered = Vec::new();
    for arg in args {
        if rendered.len() >= 128 {
            break;
        }
        let budget = 128 - rendered.len();
        rendered.push(b'\'');
        rendered.extend_from_slice(&arg[..arg.len().min(budget)]);
        rendered.extend_from_slice(b"' ");
    }
    msg.extend_from_slice(&rendered);
    err(&msg)
}

fn invalid_expire_time(command: &[u8]) -> RespFrame {
    let mut msg = b"ERR invalid expire time in '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"' command");
    err(&msg)
}

/// Parse the trailing NX|XX|GT|LT flags of the EXPIRE family, mirroring
/// `parseExtendedExpireArgumentsOrReply` (`expire.c`). Returns the resolved
/// condition (the edge engine accepts at most one), or an error frame for an
/// unsupported option or an incompatible flag combination. Empty `opts` yields
/// `None` (no condition).
fn parse_expire_condition(opts: &[Vec<u8>]) -> Result<Option<ExpireCondition>, RespFrame> {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut chosen = None;
    for opt in opts {
        if ascii_eq(opt, b"NX") {
            nx = true;
            chosen = Some(ExpireCondition::Nx);
        } else if ascii_eq(opt, b"XX") {
            xx = true;
            chosen = Some(ExpireCondition::Xx);
        } else if ascii_eq(opt, b"GT") {
            gt = true;
            chosen = Some(ExpireCondition::Gt);
        } else if ascii_eq(opt, b"LT") {
            lt = true;
            chosen = Some(ExpireCondition::Lt);
        } else {
            let mut msg = b"ERR Unsupported option ".to_vec();
            msg.extend_from_slice(opt);
            return Err(err(&msg));
        }
    }
    if (nx && xx) || (nx && gt) || (nx && lt) {
        return Err(err(
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(err(
            b"ERR GT and LT options at the same time are not compatible",
        ));
    }
    Ok(chosen)
}

fn old_string_reply(old_string: Option<Vec<u8>>) -> RespFrame {
    match old_string {
        Some(value) => bulk(value),
        None => RespFrame::null_bulk(),
    }
}

fn apply_zadd(
    members: &mut HashMap<Vec<u8>, f64>,
    scored: Vec<(f64, Vec<u8>)>,
    nx: bool,
    xx: bool,
) -> (i64, i64) {
    let mut added = 0;
    let mut updated = 0;
    for (score, member) in scored {
        match members.get_mut(&member) {
            Some(existing) => {
                if !nx && *existing != score {
                    *existing = score;
                    updated += 1;
                }
            }
            None => {
                if !xx {
                    members.insert(member, score);
                    added += 1;
                }
            }
        }
    }
    (added, updated)
}

/// Ascending (score, then member-lexicographic) order, the canonical zset
/// ordering used by ZRANK and ZRANGE.
fn sorted_zset_entries(members: &HashMap<Vec<u8>, f64>) -> Vec<(Vec<u8>, f64)> {
    let mut entries: Vec<(Vec<u8>, f64)> = members
        .iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect();
    entries.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .expect("zset scores are never NaN")
            .then_with(|| left.0.cmp(&right.0))
    });
    entries
}

/// One side of a `ZRANGEBYSCORE` score interval: a finite-or-infinite bound
/// value plus whether the bound itself is excluded (the leading `(` form).
/// Mirrors the reference `zrangespec` min/max + minex/maxex fields.
struct ScoreBound {
    value: f64,
    exclusive: bool,
}

impl ScoreBound {
    /// Whether `score` satisfies this bound used as the interval minimum,
    /// matching the reference `zslValueGteMin` (`>` when exclusive, else `>=`).
    fn gte_min(&self, score: f64) -> bool {
        if self.exclusive {
            score > self.value
        } else {
            score >= self.value
        }
    }

    /// Whether `score` satisfies this bound used as the interval maximum,
    /// matching the reference `zslValueLteMax` (`<` when exclusive, else `<=`).
    fn lte_max(&self, score: f64) -> bool {
        if self.exclusive {
            score < self.value
        } else {
            score <= self.value
        }
    }
}

/// Parses one `ZRANGEBYSCORE` bound exactly like the reference `zslParseRange`
/// over `valkey_strtod_n`: a leading `(` marks the bound exclusive, the rest is
/// a float where the inf/infinity spellings (any case, optional sign) are
/// accepted, an empty body parses as `0.0`, and a `NaN` or trailing-garbage
/// body is rejected.
fn parse_score_bound(bytes: &[u8]) -> Option<ScoreBound> {
    let (exclusive, body) = match bytes.split_first() {
        Some((b'(', rest)) => (true, rest),
        _ => (false, bytes),
    };
    if body.is_empty() {
        return Some(ScoreBound {
            value: 0.0,
            exclusive,
        });
    }
    let text = std::str::from_utf8(body).ok()?;
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(ScoreBound { value, exclusive })
}

/// Parses a `LIMIT` offset/count argument the way `getLongFromObjectOrReply`
/// does for these commands: a base-10 signed integer with no surrounding
/// whitespace or trailing characters. Anything else yields the integer error.
fn parse_limit_arg(bytes: &[u8]) -> Option<i64> {
    parse_i64(bytes)
}

/// Applies the `LIMIT offset count` window to an already in-range, ascending
/// slice, mirroring the reference forward-direction loop: a negative offset
/// (or one at/after the end) yields nothing, and a negative count returns all
/// remaining elements after the offset.
fn apply_score_limit(
    in_range: &[(Vec<u8>, f64)],
    limit: Option<(i64, i64)>,
) -> Vec<&(Vec<u8>, f64)> {
    let Some((offset, count)) = limit else {
        return in_range.iter().collect();
    };
    if offset < 0 {
        return Vec::new();
    }
    let offset = offset as usize;
    if offset >= in_range.len() {
        return Vec::new();
    }
    let tail = &in_range[offset..];
    if count < 0 {
        tail.iter().collect()
    } else {
        tail.iter().take(count as usize).collect()
    }
}

/// The reference server keeps small zsets listpack-encoded; listpack scores
/// round-trip through their decimal string, where "-0" int-encodes to 0, so
/// a stored negative zero loses its sign (only the transient ZINCRBY reply
/// keeps it). Stored scores are normalized the same way here.
fn normalize_zero(score: f64) -> f64 {
    if score == 0.0 {
        0.0
    } else {
        score
    }
}

/// Score parser matching the reference server: integer-looking input goes
/// through the long-long path first (the server int-encodes such arguments,
/// which turns "-0" into +0.0), everything else through a string2d-style
/// float parse that rejects garbage and NaN but accepts the inf/infinity
/// spellings in any case.
fn parse_score(bytes: &[u8]) -> Option<f64> {
    if let Some(integer) = parse_i64(bytes) {
        return Some(integer as f64);
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(value)
}

/// Renders a score exactly like the reference server's d2string: "inf" and
/// "-inf" for infinities, "0"/"-0" for zeroes, plain integers via the
/// double2ll fast path, and otherwise shortest round-trip digits laid out
/// with the plain/decimal/scientific thresholds of fpconv's emit_digits.
fn format_score(value: f64) -> Vec<u8> {
    if value.is_nan() {
        return b"nan".to_vec();
    }
    if value.is_infinite() {
        return if value < 0.0 {
            b"-inf".to_vec()
        } else {
            b"inf".to_vec()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            b"-0".to_vec()
        } else {
            b"0".to_vec()
        };
    }
    let integer_bound = (i64::MAX / 2) as f64;
    if (-integer_bound..=integer_bound).contains(&value) && value == (value as i64) as f64 {
        return (value as i64).to_string().into_bytes();
    }
    let exponential = format!("{:e}", value.abs());
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("LowerExp always emits an exponent");
    let exp10: i32 = exponent.parse().expect("LowerExp exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let ndigits = digits.len() as i32;
    let k = exp10 - (ndigits - 1);
    let mut out = String::new();
    if value < 0.0 {
        out.push('-');
    }
    if k >= 0 && exp10 < ndigits + 7 {
        out.push_str(&digits);
        for _ in 0..k {
            out.push('0');
        }
    } else if k < 0 && (k > -7 || exp10.abs() < 4) {
        let offset = ndigits + k;
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(&digits);
        } else {
            out.push_str(&digits[..offset as usize]);
            out.push('.');
            out.push_str(&digits[offset as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if ndigits > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp10 < 0 { '-' } else { '+' });
        out.push_str(&exp10.abs().to_string());
    }
    out.into_bytes()
}

/// Snapshots serialize scores as the same canonical string used in replies;
/// both the integer fast path and the shortest-digit path round-trip f64
/// exactly through `parse_score`.
fn score_snapshot_string(score: f64) -> String {
    String::from_utf8(format_score(score)).expect("score strings are ASCII")
}

fn resp_to_lua(lua: &Lua, frame: &RespFrame) -> lua_rs_runtime::Result<LuaValue> {
    match frame {
        RespFrame::Simple(value) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Error(value) | RespFrame::BulkError(value) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Integer(value) => Ok(LuaValue::Integer(*value)),
        RespFrame::Bulk(Some(value)) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::Bulk(None) | RespFrame::Null => Ok(LuaValue::Boolean(false)),
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            let table = lua.create_table()?;
            for (index, item) in items.iter().enumerate() {
                table.set(index as i64 + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespFrame::Array(None) => Ok(LuaValue::Boolean(false)),
        RespFrame::Boolean(value) => Ok(LuaValue::Boolean(*value)),
        RespFrame::Double(value) => Ok(LuaValue::Number(*value)),
        RespFrame::BigNumber(value) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::VerbatimString { data, .. } => {
            Ok(LuaValue::String(lua.create_string(data.as_bytes())?))
        }
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => {
            let table = lua.create_table()?;
            let mut index = 1_i64;
            for (key, value) in pairs {
                table.set(index, resp_to_lua(lua, key)?)?;
                table.set(index + 1, resp_to_lua(lua, value)?)?;
                index += 2;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn lua_to_resp(value: &LuaValue) -> lua_rs_runtime::Result<RespFrame> {
    match value {
        LuaValue::Nil => Ok(RespFrame::null_bulk()),
        LuaValue::Boolean(true) => Ok(RespFrame::integer(1)),
        LuaValue::Boolean(false) => Ok(RespFrame::null_bulk()),
        LuaValue::Integer(value) => Ok(RespFrame::integer(*value)),
        LuaValue::Number(value) => Ok(RespFrame::integer(*value as i64)),
        LuaValue::String(value) => Ok(bulk(value.as_bytes()?)),
        LuaValue::Table(table) => {
            if let Some(message) = table_string_bytes(table, "err")? {
                return Ok(err(&message));
            }
            if let Some(message) = table_string_bytes(table, "ok")? {
                return Ok(simple(&message));
            }

            let mut items = Vec::new();
            let mut index = 1_i64;
            loop {
                let item = table.get::<_, LuaValue>(index)?;
                if matches!(item, LuaValue::Nil) {
                    break;
                }
                items.push(lua_to_resp(&item)?);
                index += 1;
            }
            Ok(RespFrame::array(items))
        }
        _ => Ok(RespFrame::null_bulk()),
    }
}

fn table_string_bytes(table: &LuaTable, key: &str) -> lua_rs_runtime::Result<Option<Vec<u8>>> {
    match table.get::<_, Option<LuaString>>(key)? {
        Some(value) => Ok(Some(value.as_bytes()?)),
        None => Ok(None),
    }
}

fn collect_lua_args(args: Variadic<LuaValue>) -> lua_rs_runtime::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for value in args {
        out.push(lua_arg_to_bytes(&value)?);
    }
    Ok(out)
}

fn lua_arg_to_bytes(value: &LuaValue) -> lua_rs_runtime::Result<Vec<u8>> {
    match value {
        LuaValue::String(value) => value.as_bytes(),
        LuaValue::Integer(value) => Ok(value.to_string().into_bytes()),
        LuaValue::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok((*value as i64).to_string().into_bytes())
        }
        LuaValue::Number(value) if value.is_finite() => Ok(value.to_string().into_bytes()),
        _ => Err(lua_runtime_error(
            "command arguments must be strings or integers",
        )
        .into()),
    }
}

fn error_table(lua: &Lua, message: &[u8]) -> lua_rs_runtime::Result<LuaValue> {
    let table = lua.create_table()?;
    table.set("err", lua.create_string(message)?)?;
    Ok(LuaValue::Table(table))
}

fn lua_runtime_error(message: &str) -> LuaError {
    LuaError::runtime(format_args!("{message}"))
}

fn script_blocked_command(command: &[u8]) -> bool {
    ascii_eq(command, b"EVAL") || ascii_eq(command, b"EVALSHA") || ascii_eq(command, b"SCRIPT")
}

fn simple(value: &[u8]) -> RespFrame {
    RespFrame::simple(RedisString::from_bytes(value))
}

fn bulk(value: impl AsRef<[u8]>) -> RespFrame {
    RespFrame::bulk(RedisString::from_bytes(value))
}

fn err(value: &[u8]) -> RespFrame {
    RespFrame::error(RedisString::from_bytes(value))
}

fn wrong_type() -> RespFrame {
    err(b"WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn wrong_arity(command: &[u8]) -> RespFrame {
    let mut msg = b"ERR wrong number of arguments for '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"' command");
    err(&msg)
}

fn checked_ttl_ms(raw: i64, unit_ms: u64) -> Option<u64> {
    u64::try_from(raw).ok()?.checked_mul(unit_ms)
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.is_empty()
        || text.as_bytes().iter().any(|b| !b.is_ascii_digit())
            && !matches!(text.as_bytes(), [b'-', rest @ ..] if !rest.is_empty() && rest.iter().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    text.parse::<i64>().ok()
}

fn ascii_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(l, r)| l.eq_ignore_ascii_case(r))
}

fn normalise_sha(bytes: &[u8]) -> Option<[u8; 40]> {
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    for (index, byte) in bytes.iter().enumerate() {
        out[index] = match *byte {
            b'0'..=b'9' | b'a'..=b'f' => *byte,
            b'A'..=b'F' => *byte + 32,
            _ => return None,
        };
    }
    Some(out)
}

fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 40];
    for (index, byte) in digest.iter().enumerate() {
        out[index * 2] = HEX[(byte >> 4) as usize];
        out[index * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out
}

fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;
    let bit_len = (data.len() as u64) * 8;

    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for index in 0..16 {
            w[index] = u32::from_be_bytes([
                chunk[index * 4],
                chunk[index * 4 + 1],
                chunk[index * 4 + 2],
                chunk[index * 4 + 3],
            ]);
        }
        for index in 16..80 {
            w[index] = (w[index - 3] ^ w[index - 8] ^ w[index - 14] ^ w[index - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for index in 0..80 {
            let (f, k) = if index < 20 {
                ((b & c) | ((!b) & d), 0x5A827999u32)
            } else if index < 40 {
                (b ^ c ^ d, 0x6ED9EBA1u32)
            } else if index < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32)
            } else {
                (b ^ c ^ d, 0xCA62C1D6)
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[index]);
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

#[cfg(test)]
mod tests {
    use redis_protocol::encode_resp2;

    use super::*;

    #[test]
    fn mutation_epoch_tracks_state_changes_not_reads() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"GET", b"missing"]));
        engine.execute(&argv(&[b"EXISTS", b"missing"]));
        engine.execute(&argv(&[b"TTL", b"missing"]));
        engine.execute(&argv(&[b"DEL", b"missing"]));
        engine.execute(&argv(&[b"EXPIRE", b"missing", b"10"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[b"SET", b"k", b"v"]));
        let after_set = engine.mutation_epoch();
        assert_ne!(after_set, start);

        engine.execute(&argv(&[b"GET", b"k"]));
        assert_eq!(engine.mutation_epoch(), after_set);

        engine.execute(&argv(&[b"EXPIRE", b"k", b"10"]));
        let after_expire = engine.mutation_epoch();
        assert_ne!(after_expire, after_set);

        engine.execute(&argv(&[b"DEL", b"k"]));
        assert_ne!(engine.mutation_epoch(), after_expire);
    }

    fn script_load(engine: &mut Engine<NoopHost>, body: &[u8]) -> [u8; 40] {
        let frame = engine.execute(&argv(&[b"SCRIPT", b"LOAD", body]));
        match frame {
            RespFrame::Bulk(Some(sha)) => {
                let mut out = [0u8; 40];
                out.copy_from_slice(&sha);
                out
            }
            other => panic!("SCRIPT LOAD did not return a sha: {other:?}"),
        }
    }

    fn script_exists(engine: &mut Engine<NoopHost>, sha: &[u8; 40]) -> bool {
        let frame = engine.execute(&argv(&[b"SCRIPT", b"EXISTS", sha]));
        matches!(frame, RespFrame::Array(Some(items))
            if matches!(items.first(), Some(RespFrame::Integer(1))))
    }

    #[test]
    fn script_cache_evicts_least_recently_used_past_the_count_cap() {
        let mut engine = Engine::new_in_memory();
        let first = script_load(&mut engine, b"return 0");
        let mut shas = vec![first];
        for index in 1..=MAX_CACHED_SCRIPTS {
            let body = format!("return {index}");
            shas.push(script_load(&mut engine, body.as_bytes()));
        }

        assert!(
            !script_exists(&mut engine, &shas[0]),
            "the oldest untouched script must be evicted once the cap is exceeded"
        );
        assert!(
            script_exists(&mut engine, shas.last().unwrap()),
            "the most recent script must survive"
        );
        assert_eq!(
            engine.execute(&argv(&[b"EVALSHA", &shas[0], b"0"])),
            err(b"NOSCRIPT No matching script."),
            "an evicted script answers EVALSHA with NOSCRIPT"
        );
    }

    #[test]
    fn evalsha_use_protects_a_hot_script_from_eviction() {
        let mut engine = Engine::new_in_memory();
        let hot = script_load(&mut engine, b"return 'hot'");
        for index in 0..MAX_CACHED_SCRIPTS {
            let body = format!("return {index}");
            script_load(&mut engine, body.as_bytes());
            engine.execute(&argv(&[b"EVALSHA", &hot, b"0"]));
        }
        assert!(
            script_exists(&mut engine, &hot),
            "a script kept warm by EVALSHA must not be evicted by one-off loads"
        );
    }

    #[test]
    fn script_flush_resets_the_bounded_cache() {
        let mut engine = Engine::new_in_memory();
        let sha = script_load(&mut engine, b"return 1");
        assert!(script_exists(&mut engine, &sha));
        engine.execute(&argv(&[b"SCRIPT", b"FLUSH"]));
        assert!(!script_exists(&mut engine, &sha));
        let reloaded = script_load(&mut engine, b"return 1");
        assert_eq!(reloaded, sha, "the same body re-hashes to the same sha");
        assert!(script_exists(&mut engine, &sha));
    }

    #[test]
    fn mutation_epoch_ignores_script_cache_but_sees_script_writes() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"SCRIPT", b"LOAD", b"return 1"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[b"EVAL", b"return 1", b"0"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[
            b"EVAL",
            b"return redis.call('SET', KEYS[1], ARGV[1])",
            b"1",
            b"k",
            b"v",
        ]));
        assert_ne!(engine.mutation_epoch(), start);
    }

    /// Run `seed` (unmeasured), then assert that executing `measured` bumps the
    /// mutation epoch whenever it changes snapshot-visible state. `export_snapshot`
    /// is the content fingerprint: it purges already-expired keys before
    /// serializing, so passive expiry never registers as a change and the clock
    /// is held fixed across the before/after pair. The guarded direction is
    /// `content changed ⇒ epoch bumped`: under-marking (a write the persistence
    /// layer would silently drop) fails here; conservative over-marking (a bump
    /// without a content change, e.g. HSET writing an identical value) is safe
    /// and allowed.
    fn assert_write_bumps_epoch(seed: &[&[&[u8]]], measured: &[&[u8]]) {
        let label = || {
            measured
                .iter()
                .map(|part| String::from_utf8_lossy(part).into_owned())
                .collect::<Vec<_>>()
        };
        let mut engine = Engine::new_in_memory();
        engine.host_mut().set_now_millis(1_000);
        for command in seed {
            engine.execute(&argv(command));
        }
        // A per-key storage map standing in for the host. Seed it with the
        // pre-command state so the dirty-key flush has a baseline to mutate.
        let mut storage: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for key in engine.take_dirty() {
            if let Some(bytes) = engine.export_key(&key) {
                storage.insert(key, bytes);
            }
        }
        let before = engine.export_snapshot();
        let epoch_before = engine.mutation_epoch();
        engine.execute(&argv(measured));
        let after = engine.export_snapshot();
        let epoch_after = engine.mutation_epoch();
        let content_changed = before != after;
        let epoch_bumped = epoch_after != epoch_before;
        assert!(
            !content_changed || epoch_bumped,
            "command {:?} changed snapshot state without bumping the mutation epoch — the persistence layer would silently drop this write",
            label(),
        );
        // Flush only the dirty keys into storage, then rebuild a fresh engine
        // from storage alone. It must reproduce the full post-command state —
        // proving the dirty set captured every change a host needs to persist.
        for key in engine.take_dirty() {
            match engine.export_key(&key) {
                Some(bytes) => {
                    storage.insert(key, bytes);
                }
                None => {
                    storage.remove(&key);
                }
            }
        }
        let mut restored = Engine::new_in_memory();
        restored.host_mut().set_now_millis(1_000);
        let mut storage_keys: Vec<_> = storage.keys().cloned().collect();
        storage_keys.sort();
        for key in storage_keys {
            restored.import_key(&storage[&key]).unwrap();
        }
        assert_eq!(
            restored.export_snapshot(),
            after,
            "command {:?}: per-key dirty flush did not reproduce the full state",
            label(),
        );
    }

    #[test]
    fn every_command_that_writes_visible_state_bumps_the_epoch() {
        let seed_k: &[&[u8]] = &[b"SET", b"k", b"v"];
        let seed_n: &[&[u8]] = &[b"SET", b"n", b"5"];
        let seed_h: &[&[u8]] = &[b"HSET", b"h", b"f", b"v"];
        let seed_z: &[&[u8]] = &[b"ZADD", b"z", b"1", b"a"];

        let cases: &[(&[&[&[u8]]], &[&[u8]])] = &[
            (&[], &[b"SET", b"k", b"v"]),
            (&[seed_k], &[b"GET", b"k"]),
            (&[seed_k], &[b"EXISTS", b"k"]),
            (&[seed_k], &[b"SET", b"k", b"v2"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"NX"]),
            (&[], &[b"SET", b"fresh", b"v", b"NX"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"XX"]),
            (&[], &[b"SET", b"missing", b"v", b"XX"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"GET"]),
            (&[seed_k], &[b"SETEX", b"k", b"100", b"v2"]),
            (&[seed_k], &[b"DEL", b"k"]),
            (&[], &[b"DEL", b"missing"]),
            (&[seed_n], &[b"INCR", b"n"]),
            (&[], &[b"INCR", b"new"]),
            (&[seed_n], &[b"INCRBY", b"n", b"3"]),
            (&[seed_k], &[b"EXPIRE", b"k", b"100"]),
            (&[], &[b"EXPIRE", b"missing", b"100"]),
            (&[seed_k], &[b"PEXPIRE", b"k", b"100000"]),
            (&[seed_k], &[b"EXPIRE", b"k", b"-1"]),
            (&[seed_k], &[b"EXPIREAT", b"k", b"99999999999"]),
            (&[seed_k], &[b"PEXPIREAT", b"k", b"99999999999000"]),
            (&[seed_k], &[b"EXPIREAT", b"k", b"1"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"PERSIST", b"k"],
            ),
            (&[seed_k], &[b"PERSIST", b"k"]),
            (&[seed_k], &[b"TTL", b"k"]),
            (&[seed_k], &[b"PTTL", b"k"]),
            (&[seed_k], &[b"EXPIRETIME", b"k"]),
            (&[seed_k], &[b"PEXPIRETIME", b"k"]),
            (&[seed_k], &[b"TYPE", b"k"]),
            (&[], &[b"TYPE", b"missing"]),
            (&[seed_k], &[b"TOUCH", b"k"]),
            (&[seed_k], &[b"UNLINK", b"k"]),
            (&[seed_k], &[b"RENAME", b"k", b"k2"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"RENAME", b"k", b"k2"],
            ),
            (&[seed_k], &[b"RENAMENX", b"k", b"k2"]),
            (&[seed_k], &[b"COPY", b"k", b"k2"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"COPY", b"k", b"k2"],
            ),
            (&[seed_k], &[b"FLUSHALL"]),
            (&[seed_k, seed_h, seed_z], &[b"FLUSHALL"]),
            (&[], &[b"PING"]),
            (&[], &[b"ECHO", b"hi"]),
            (&[seed_h], &[b"HSET", b"h", b"f2", b"v2"]),
            (&[seed_h], &[b"HGET", b"h", b"f"]),
            (&[seed_h], &[b"HGETALL", b"h"]),
            (&[seed_h], &[b"HDEL", b"h", b"f"]),
            (&[seed_h], &[b"HDEL", b"h", b"missing"]),
            (&[], &[b"ZADD", b"z", b"1", b"a"]),
            (&[seed_z], &[b"ZADD", b"z", b"1", b"a", b"NX"]),
            (&[seed_z], &[b"ZADD", b"z", b"2", b"a"]),
            (&[seed_z], &[b"ZADD", b"z", b"9", b"b", b"XX"]),
            (&[seed_z], &[b"ZSCORE", b"z", b"a"]),
            (&[seed_z], &[b"ZINCRBY", b"z", b"5", b"a"]),
            (&[seed_z], &[b"ZREM", b"z", b"a"]),
            (&[seed_z], &[b"ZREM", b"z", b"missing"]),
            (&[seed_z], &[b"ZCARD", b"z"]),
            (&[seed_z], &[b"ZRANGE", b"z", b"0", b"-1"]),
            (&[seed_z], &[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf"]),
            (&[], &[b"SCRIPT", b"LOAD", b"return 1"]),
            (&[], &[b"EVAL", b"return 1", b"0"]),
            (
                &[],
                &[
                    b"EVAL",
                    b"return redis.call('SET', KEYS[1], '1')",
                    b"1",
                    b"ek",
                ],
            ),
            (
                &[seed_k],
                &[b"EVAL", b"return redis.call('GET', KEYS[1])", b"1", b"k"],
            ),
        ];

        for (seed, measured) in cases {
            assert_write_bumps_epoch(seed, measured);
        }
    }

    const TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local refill_tokens = tonumber(ARGV[3])
        local refill_ms = tonumber(ARGV[4])
        local cost = tonumber(ARGV[5])
        local ttl_ms = tonumber(ARGV[6])

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after}
    "#;

    const HASH_POLICY_TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local bucket_key = KEYS[1]
        local policy_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local cost = tonumber(ARGV[2])

        local capacity = tonumber(redis.call('HGET', policy_key, 'capacity') or '10')
        local refill_tokens = tonumber(redis.call('HGET', policy_key, 'refill_tokens') or '5')
        local refill_ms = tonumber(redis.call('HGET', policy_key, 'refill_ms') or '1000')
        local ttl_ms = tonumber(redis.call('HGET', policy_key, 'ttl_ms') or '60000')

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', bucket_key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', bucket_key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after, 'capacity', capacity}
    "#;

    fn argv(items: &[&[u8]]) -> Vec<Vec<u8>> {
        items.iter().map(|item| item.to_vec()).collect()
    }

    fn resp2(frame: &RespFrame) -> Vec<u8> {
        let mut out = Vec::new();
        encode_resp2(frame, &mut out);
        out
    }

    fn response_json(response: RestResponse) -> JsonValue {
        assert_eq!(response.content_type, APPLICATION_JSON);
        serde_json::from_slice(&response.body).unwrap()
    }

    fn load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let reply = engine.execute(&argv(&[b"SCRIPT", b"LOAD", TOKEN_BUCKET_SCRIPT]));
        match reply {
            RespFrame::Bulk(Some(sha)) => sha.into_bytes(),
            other => panic!("unexpected script load reply: {other:?}"),
        }
    }

    fn evalsha_token_bucket(engine: &mut Engine<NoopHost>, sha: &[u8], now: &[u8]) -> Vec<u8> {
        let request = vec![
            b"EVALSHA".to_vec(),
            sha.to_vec(),
            b"1".to_vec(),
            b"edge:tenant:42:tokens".to_vec(),
            now.to_vec(),
            b"10".to_vec(),
            b"5".to_vec(),
            b"1000".to_vec(),
            b"7".to_vec(),
            b"60000".to_vec(),
        ];
        resp2(&engine.execute(&request))
    }

    fn rest_load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let script = String::from_utf8_lossy(TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        value["result"].as_str().unwrap().as_bytes().to_vec()
    }

    fn rest_evalsha_token_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            1,
            "edge:tenant:42:tokens",
            now,
            10,
            5,
            1000,
            7,
            60000
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    fn rest_evalsha_hash_policy_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            2,
            "edge:tenant:42:tokens",
            "edge:tenant:42:policy",
            now,
            7
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    #[test]
    fn basic_mvp_commands_work_without_redis_core() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"41"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"INCR", b"k"])),
            RespFrame::integer(42)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\n42\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"k"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"k", b"2500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"TTL", b"k"])),
            RespFrame::integer(3)
        );

        engine.host_mut().set_now_millis(3_501);
        assert_eq!(
            engine.execute(&argv(&[b"GET", b"k"])),
            RespFrame::null_bulk()
        );
    }

    #[test]
    fn hash_commands_cover_policy_storage_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_tokens",
                b"5",
                b"refill_ms",
                b"1000",
                b"ttl_ms",
                b"60000"
            ])),
            RespFrame::integer(4)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HGET", b"policy", b"missing"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGETALL", b"policy"]))),
            b"*8\r\n$8\r\ncapacity\r\n$2\r\n10\r\n$9\r\nrefill_ms\r\n$4\r\n1000\r\n$13\r\nrefill_tokens\r\n$1\r\n5\r\n$6\r\nttl_ms\r\n$5\r\n60000\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HDEL", b"policy", b"ttl_ms", b"missing"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"policy", b"500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/HGET/policy/refill_ms"))),
            json!({"result": "1000"})
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"plain", b"field"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"policy"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_strings_hashes_and_absolute_expiry() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_ms",
                b"1000"
            ])),
            RespFrame::integer(2)
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"volatile", b"soon", b"PX", b"500"])),
            RespFrame::simple("OK")
        );

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_200));
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            resp2(&restored.execute(&argv(&[b"GET", b"plain"]))),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            restored.execute(&argv(&[b"PTTL", b"volatile"])),
            RespFrame::integer(300)
        );

        restored.host_mut().set_now_millis(1_500);
        assert_eq!(
            restored.execute(&argv(&[b"GET", b"volatile"])),
            RespFrame::null_bulk()
        );
    }

    #[test]
    fn snapshot_import_rejects_malformed_hex() {
        let mut engine = Engine::new_in_memory();
        let malformed = br#"{
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": [{"key": "0", "type": "string", "value": "00"}]
        }"#;

        assert_eq!(
            engine.import_snapshot(malformed),
            Err(SnapshotError::InvalidHex)
        );
    }

    #[test]
    fn script_load_exists_flush_round_trip() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        let exists = engine.execute(&vec![
            b"SCRIPT".to_vec(),
            b"EXISTS".to_vec(),
            sha.clone(),
            b"ffffffffffffffffffffffffffffffffffffffff".to_vec(),
        ]);
        assert_eq!(resp2(&exists), b"*2\r\n:1\r\n:0\r\n");

        assert_eq!(
            engine.execute(&argv(&[b"SCRIPT", b"FLUSH"])),
            RespFrame::simple("OK")
        );
        let missing = engine.execute(&vec![b"EVALSHA".to_vec(), sha, b"0".to_vec()]);
        assert_eq!(missing, RespFrame::error("NOSCRIPT No matching script."));
    }

    #[test]
    fn evalsha_runs_stateful_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1000"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1100"),
            b"*8\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1800"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:3800\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
    }

    #[test]
    fn rest_path_post_body_and_resp2_modes_match_edge_adapter_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        let set = engine.execute_rest(RestRequest::get("/SET/foo/bar"));
        assert_eq!(set.status, 200);
        assert_eq!(response_json(set), json!({"result": "OK"}));

        let get = engine.execute_rest(RestRequest::get("/GET/foo"));
        assert_eq!(get.status, 200);
        assert_eq!(response_json(get), json!({"result": "bar"}));

        let post = engine.execute_rest(RestRequest::post("/SET/post-key?PX=2500", b"post-value"));
        assert_eq!(post.status, 200);
        assert_eq!(response_json(post), json!({"result": "OK"}));
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/PTTL/post-key"))),
            json!({"result": 2500})
        );

        let resp2_get = engine.execute_rest(RestRequest {
            method: RestMethod::Get,
            path: "/GET/post-key",
            body: &[],
            response_format: RestResponseFormat::Resp2,
        });
        assert_eq!(resp2_get.status, 200);
        assert_eq!(resp2_get.content_type, APPLICATION_OCTET_STREAM);
        assert_eq!(resp2_get.body, b"$10\r\npost-value\r\n");
    }

    #[test]
    fn rest_pipeline_is_ordered_and_non_atomic() {
        let mut engine = Engine::new_in_memory();
        let body = br#"[
            ["SET", "key1", "1"],
            ["INCR", "key1"],
            ["GET", "key1"],
            ["NOPE"]
        ]"#;

        let response = engine.execute_rest(RestRequest::post("/pipeline", body));

        assert_eq!(response.status, 200);
        assert_eq!(
            response_json(response),
            json!([
                {"result": "OK"},
                {"result": 2},
                {"result": "2"},
                {"error": "ERR unknown command 'NOPE', with args beginning with: "}
            ])
        );
    }

    #[test]
    fn rest_adapter_runs_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = rest_load_token_bucket(&mut engine);

        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 3800, "retry_after_ms", 0]})
        );
    }

    #[test]
    fn rest_adapter_runs_hash_policy_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let policy = engine.execute_rest(RestRequest::post(
            "/HSET/edge%3Atenant%3A42%3Apolicy",
            b"[\"HSET\",\"edge:tenant:42:policy\",\"capacity\",\"10\",\"refill_tokens\",\"5\",\"refill_ms\",\"1000\",\"ttl_ms\",\"60000\"]",
        ));
        assert_eq!(response_json(policy), json!({"result": 4}));

        let script = String::from_utf8_lossy(HASH_POLICY_TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        let sha = value["result"].as_str().unwrap().as_bytes().to_vec();

        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0, "capacity", 10]})
        );
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700, "capacity", 10]})
        );

        let upgrade = engine.execute_rest(RestRequest::get(
            "/HSET/edge%3Atenant%3A42%3Apolicy/capacity/20",
        ));
        assert_eq!(response_json(upgrade), json!({"result": 0}));
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 5800, "retry_after_ms", 0, "capacity", 20]})
        );
    }

    #[test]
    fn sha1_hex_known_vectors() {
        assert_eq!(&sha1_hex(b""), b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            &sha1_hex(b"abc"),
            b"a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn set_options_nx_xx_get_follow_reference_semantics() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v1", b"NX"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v2", b"NX"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\nv1\r\n"
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"missing", b"v", b"XX"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"missing"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v2", b"XX"])),
            RespFrame::simple("OK")
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"k", b"v3", b"GET"]))),
            b"$2\r\nv2\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"fresh", b"v1", b"GET"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"k", b"v9", b"NX", b"GET"]))),
            b"$2\r\nv3\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\nv3\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"missing", b"v", b"XX", b"GET"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"missing"])),
            RespFrame::integer(0)
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v", b"NX", b"XX"])),
            RespFrame::error("ERR syntax error")
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v", b"EX", b"10", b"PX", b"10000"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k2", b"v", b"NX", b"PX", b"2500"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"PTTL", b"k2"])),
            RespFrame::integer(2500)
        );

        engine.execute(&argv(&[b"HSET", b"h", b"f", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"h", b"v", b"GET"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"h", b"f"]))),
            b"$1\r\nv\r\n"
        );
    }

    #[test]
    fn zset_commands_cover_leaderboard_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[
                b"ZADD", b"board", b"1", b"a", b"2", b"b", b"2", b"bb", b"3", b"c"
            ])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZCARD", b"board"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"board", b"b"]))),
            b"$1\r\n2\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZSCORE", b"board", b"nope"])),
            RespFrame::null_bulk()
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"5", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"CH", b"6", b"a", b"4", b"d"])),
            RespFrame::integer(2)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"NX", b"9", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"board", b"a"]))),
            b"$1\r\n6\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"XX", b"8", b"nope2"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZSCORE", b"board", b"nope2"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"NX", b"XX", b"1", b"m"])),
            RespFrame::error("ERR XX and NX options at the same time are not compatible")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"oops", b"m"])),
            RespFrame::error("ERR value is not a valid float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"1", b"m", b"2"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"b"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"bb"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREVRANK", b"board", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREVRANK", b"board", b"b"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"zzz"])),
            RespFrame::null_bulk()
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"2"]))),
            b"*3\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"-2", b"-1", b"WITHSCORES"]))),
            b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\na\r\n$1\r\n6\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"1", b"REV"]))),
            b"*2\r\n$1\r\na\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGE", b"board", b"5", b"9"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"-1", b"BOGUS"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"board", b"1.5", b"b"]))),
            b"$3\r\n3.5\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZINCRBY", b"board", b"oops", b"b"])),
            RespFrame::error("ERR value is not a valid float")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"znan", b"inf", b"m"]))),
            b"$3\r\ninf\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZINCRBY", b"znan", b"-inf", b"m"])),
            RespFrame::error("ERR resulting score is not a number (NaN)")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"zneg", b"-0.0", b"m"]))),
            b"$2\r\n-0\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"zneg", b"m"]))),
            b"$1\r\n0\r\n"
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"a", b"missing"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"b", b"bb", b"c", b"d"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"board"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"a"])),
            RespFrame::integer(0)
        );

        engine.execute(&argv(&[b"SET", b"s", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZADD", b"s", b"1", b"m"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"znan"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zrangebyscore_matches_reference_ranges_options_and_errors() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"ZADD", b"z", b"1", b"a", b"2", b"b", b"2", b"bb", b"3", b"c", b"4", b"d",
        ]));
        let epoch = engine.mutation_epoch();

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"2", b"3"]))),
            b"*3\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n"
        );
        assert_eq!(engine.mutation_epoch(), epoch, "ZRANGEBYSCORE is read-only");

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(2", b"3"]))),
            b"*1\r\n$1\r\nc\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"2", b"(3"]))),
            b"*2\r\n$1\r\nb\r\n$2\r\nbb\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ]))),
            b"*10\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$2\r\nbb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"-inf", b"inf"]))),
            b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1",
                b"2"
            ]))),
            b"*2\r\n$1\r\nb\r\n$2\r\nbb\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1",
                b"-1"
            ]))),
            b"*4\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"3",
                b"100"
            ]))),
            b"*2\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"100",
                b"5"
            ])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"-1",
                b"2"
            ])),
            RespFrame::array(Vec::new())
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"100", b"200"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"nokey", b"0", b"10"])),
            RespFrame::array(Vec::new())
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"foo", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(foo", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"nan", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );

        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"x",
                b"2"
            ])),
            RespFrame::error("ERR value is not an integer or out of range")
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1"
            ])),
            RespFrame::error("ERR syntax error")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf", b"BOGUS"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"foo", b"10", b"BOGUS"])),
            RespFrame::error("ERR syntax error"),
            "option parse precedes bound parse"
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"foo",
                b"10",
                b"LIMIT",
                b"x",
                b"y"
            ])),
            RespFrame::error("ERR value is not an integer or out of range"),
            "LIMIT integer error precedes bound parse"
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"0"])),
            RespFrame::error("ERR wrong number of arguments for 'zrangebyscore' command")
        );

        engine.execute(&argv(&[b"SET", b"zstr", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"zstr", b"0", b"10"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zrangebyscore_handles_lex_tiebreak_inf_members_and_empty_bound() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"ZADD", b"z", b"1.5", b"a", b"-2.5", b"neg", b"0", b"zero", b"3.0e15", b"big", b"0.1",
            b"tenth", b"inf", b"top", b"-inf", b"bottom",
        ]));

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ]))),
            b"*14\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n$3\r\nneg\r\n$4\r\n-2.5\r\n$4\r\nzero\r\n$1\r\n0\r\n$5\r\ntenth\r\n$3\r\n0.1\r\n$1\r\na\r\n$3\r\n1.5\r\n$3\r\nbig\r\n$16\r\n3000000000000000\r\n$3\r\ntop\r\n$3\r\ninf\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"100",
                b"(inf",
                b"WITHSCORES"
            ]))),
            b"*2\r\n$3\r\nbig\r\n$16\r\n3000000000000000\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(-inf", b"-100"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"-100",
                b"WITHSCORES"
            ]))),
            b"*2\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n"
        );

        engine.execute(&argv(&[
            b"ZADD", b"lex", b"5", b"banana", b"5", b"apple", b"5", b"cherry",
        ]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"lex", b"5", b"5"]))),
            b"*3\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\ncherry\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"", b"0.1"]))),
            b"*2\r\n$4\r\nzero\r\n$5\r\ntenth\r\n",
            "an empty bound body parses as 0.0, inclusive"
        );
    }

    #[test]
    fn zset_score_replies_match_reference_formatting() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"0", b"0"),
            (b"1", b"1"),
            (b"-1", b"-1"),
            (b"1.5", b"1.5"),
            (b"3.0e15", b"3000000000000000"),
            (b"0.1", b"0.1"),
            (b"-0", b"0"),
            (b"-0.0", b"0"),
            (b"0.001", b"0.001"),
            (b"100.25", b"100.25"),
            (b"1e300", b"1e+300"),
            (b"1e-9", b"1e-9"),
            (b"inf", b"inf"),
            (b"-Infinity", b"-inf"),
        ];
        let mut engine = Engine::new_in_memory();
        for (index, (input, expected)) in cases.iter().enumerate() {
            let member = format!("m{index}").into_bytes();
            engine.execute(&vec![
                b"ZADD".to_vec(),
                b"fmt".to_vec(),
                input.to_vec(),
                member.clone(),
            ]);
            let reply = engine.execute(&vec![b"ZSCORE".to_vec(), b"fmt".to_vec(), member]);
            match reply {
                RespFrame::Bulk(Some(value)) => assert_eq!(
                    value.as_bytes(),
                    *expected,
                    "score input {:?}",
                    String::from_utf8_lossy(input)
                ),
                other => panic!("unexpected ZSCORE reply for {input:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn snapshot_round_trip_preserves_zsets() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[
            b"ZADD",
            b"board",
            b"1.5",
            b"alice",
            b"0.1",
            b"bob",
            b"3000000000000000",
            b"carol",
            b"-1",
            b"dave",
        ]));
        engine.execute(&argv(&[
            b"ZADD", b"edges", b"inf", b"top", b"-inf", b"bottom",
        ]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        for key in [&b"board"[..], b"edges"] {
            assert_eq!(
                resp2(&engine.execute(&argv(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"]))),
                resp2(&restored.execute(&argv(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"])))
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"ZSCORE", b"board", b"bob"]))),
            b"$3\r\n0.1\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"ZSCORE", b"edges", b"top"]))),
            b"$3\r\ninf\r\n"
        );
    }

    #[test]
    fn mutation_epoch_zset_and_set_options_contract() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"ZADD", b"z", b"1", b"a"]));
        let after_zadd = engine.mutation_epoch();
        assert_ne!(after_zadd, start);

        engine.execute(&argv(&[b"ZSCORE", b"z", b"a"]));
        engine.execute(&argv(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]));
        engine.execute(&argv(&[b"ZRANK", b"z", b"a"]));
        engine.execute(&argv(&[b"ZREVRANK", b"z", b"a"]));
        engine.execute(&argv(&[b"ZCARD", b"z"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);

        engine.execute(&argv(&[b"ZADD", b"z", b"1", b"a"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);
        engine.execute(&argv(&[b"ZADD", b"z", b"XX", b"5", b"missing"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);

        engine.execute(&argv(&[b"ZINCRBY", b"z", b"1", b"a"]));
        let after_zincrby = engine.mutation_epoch();
        assert_ne!(after_zincrby, after_zadd);

        engine.execute(&argv(&[b"ZREM", b"z", b"missing"]));
        assert_eq!(engine.mutation_epoch(), after_zincrby);
        engine.execute(&argv(&[b"ZREM", b"z", b"a"]));
        let after_zrem = engine.mutation_epoch();
        assert_ne!(after_zrem, after_zincrby);

        engine.execute(&argv(&[b"SET", b"k", b"v"]));
        let after_set = engine.mutation_epoch();
        engine.execute(&argv(&[b"SET", b"k", b"v2", b"NX"]));
        assert_eq!(engine.mutation_epoch(), after_set);
        engine.execute(&argv(&[b"SET", b"other", b"v", b"XX"]));
        assert_eq!(engine.mutation_epoch(), after_set);
        engine.execute(&argv(&[b"SET", b"k", b"v2", b"XX", b"GET"]));
        assert_ne!(engine.mutation_epoch(), after_set);
    }

    #[test]
    fn eval_registers_script_in_cache_for_evalsha() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(
            engine.execute(&argv(&[b"EVAL", b"return 2", b"0"])),
            RespFrame::integer(2)
        );
        let sha = sha1_hex(b"return 2");
        let exists = engine.execute(&vec![b"SCRIPT".to_vec(), b"EXISTS".to_vec(), sha.to_vec()]);
        assert_eq!(resp2(&exists), b"*1\r\n:1\r\n");
        assert_eq!(
            engine.execute(&vec![b"EVALSHA".to_vec(), sha.to_vec(), b"0".to_vec()]),
            RespFrame::integer(2)
        );
    }

    #[test]
    fn eval_uncaught_script_errors_return_resp_errors() {
        let mut engine = Engine::new_in_memory();

        let reply = engine.execute(&argv(&[b"EVAL", b"error('boom')", b"0"]));
        match &reply {
            RespFrame::Error(message) => {
                let text = String::from_utf8_lossy(message.as_bytes()).into_owned();
                assert!(text.starts_with("ERR "), "unexpected error shape: {text}");
                assert!(text.contains("boom"), "error message lost: {text}");
            }
            other => panic!("expected error frame, got {other:?}"),
        }

        engine.execute(&argv(&[b"SET", b"s", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('INCR', KEYS[1])",
                b"1",
                b"s"
            ]))),
            b"-ERR value is not an integer or out of range\r\n"
        );

        engine.execute(&argv(&[b"HSET", b"h", b"f", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('GET', KEYS[1])",
                b"1",
                b"h"
            ]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('SET', KEYS[1], true)",
                b"1",
                b"b"
            ]))),
            b"-ERR Command arguments must be strings or integers\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return type(redis.call('GET', KEYS[1]))",
                b"1",
                b"missing"
            ]))),
            b"$7\r\nboolean\r\n"
        );
    }
}

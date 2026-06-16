//! Worker-shaped EdgeStash demo.
//!
//! This crate models the part a Cloudflare Worker plus Durable Object would
//! own without depending on a specific edge SDK: stable shard routing, one hot
//! Valdr engine per shard, tenant policy stored in hashes, and Lua `EVALSHA`
//! decisions through the Upstash-style REST adapter.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value as JsonValue};
use valdr_engine::{Engine, NoopHost, RestRequest, RestResponse, SnapshotError};

/// Prefix for every storage key that holds one Redis key's serialized entry.
/// The storage layout is `format!("k:{}", hex(redis_key))` so an arbitrary
/// binary Redis key maps to a safe storage-key string. `open`/restore only
/// imports storage entries under this prefix and ignores any others.
const KEY_PREFIX: &str = "k:";
const APPLICATION_JSON: &str = "application/json";
const LIMITER_SCRIPT: &str = r#"
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub capacity: i64,
    pub refill_tokens: i64,
    pub refill_ms: i64,
    pub ttl_ms: i64,
}

impl Policy {
    pub const fn token_bucket(
        capacity: i64,
        refill_tokens: i64,
        refill_ms: i64,
        ttl_ms: i64,
    ) -> Self {
        Self {
            capacity,
            refill_tokens,
            refill_ms,
            ttl_ms,
        }
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::token_bucket(10, 5, 1_000, 60_000)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitRequest<'a> {
    pub tenant_id: &'a str,
    pub now_millis: u64,
    pub cost: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitDecision {
    pub allowed: bool,
    pub remaining: i64,
    pub reset_ms: i64,
    pub retry_after_ms: i64,
    pub capacity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoAiRequest {
    now_millis: Option<u64>,
    prompt: String,
    tokens: i64,
}

struct LimitBody {
    now_millis: Option<u64>,
    cost: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeError {
    InvalidShardCount,
    JsonBody,
    Snapshot(SnapshotError),
    Storage,
    RestError { status: u16, body: Vec<u8> },
    MissingResult,
    UnexpectedResult,
    ValueBudgetExceeded { value_bytes: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeHttpMethod {
    Get,
    Post,
    Put,
    Head,
    Other,
}

/// One HTTP request as seen by the route layer. `now_millis` is the host
/// adapter's clock reading for this request and is the time authority for
/// every route unless the object was explicitly built with
/// `with_client_time_allowed(true)` for deterministic fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeHttpRequest<'a> {
    pub method: EdgeHttpMethod,
    pub path: &'a str,
    pub body: &'a [u8],
    pub now_millis: u64,
}

impl<'a> EdgeHttpRequest<'a> {
    pub fn get(path: &'a str, now_millis: u64) -> Self {
        Self {
            method: EdgeHttpMethod::Get,
            path,
            body: &[],
            now_millis,
        }
    }

    pub fn post(path: &'a str, body: &'a [u8], now_millis: u64) -> Self {
        Self {
            method: EdgeHttpMethod::Post,
            path,
            body,
            now_millis,
        }
    }

    pub fn put(path: &'a str, body: &'a [u8], now_millis: u64) -> Self {
        Self {
            method: EdgeHttpMethod::Put,
            path,
            body,
            now_millis,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeHttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

pub trait ObjectStorage {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, EdgeError>;
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), EdgeError>;
    fn delete(&mut self, key: &str) -> Result<(), EdgeError>;
    fn scan(&mut self) -> Result<Vec<(String, Vec<u8>)>, EdgeError>;
}

/// In-memory per-key key/value store with a change-log. Each `put` or `delete`
/// records the storage-key in `dirty`, so a host adapter can flush only what
/// changed since the last `drain_dirty` instead of rewriting the whole store.
#[derive(Debug, Clone, Default)]
pub struct MemoryObjectStorage {
    values: HashMap<String, Vec<u8>>,
    dirty: HashSet<String>,
}

impl MemoryObjectStorage {
    /// Build a store from already-persisted entries with an empty change-log.
    /// A host adapter uses this on cold start after listing provider storage.
    pub fn from_entries(entries: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            values: entries.into_iter().collect(),
            dirty: HashSet::new(),
        }
    }

    /// Drain the set of storage-keys put or deleted since the last call,
    /// sorted for deterministic flush order. A host resolves each returned
    /// key to put-vs-delete with `value`.
    pub fn drain_dirty(&mut self) -> Vec<String> {
        let mut keys: Vec<String> = self.dirty.drain().collect();
        keys.sort();
        keys
    }

    /// Read the final stored value of a storage-key, or `None` when the key
    /// was deleted. A host adapter calls this for each drained dirty key to
    /// decide whether to write the bytes or delete the key from provider
    /// storage.
    pub fn value(&self, key: &str) -> Option<&[u8]> {
        self.values.get(key).map(Vec::as_slice)
    }
}

impl ObjectStorage for MemoryObjectStorage {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, EdgeError> {
        Ok(self.values.get(key).cloned())
    }

    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), EdgeError> {
        self.values.insert(key.to_owned(), value.to_vec());
        self.dirty.insert(key.to_owned());
        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), EdgeError> {
        self.values.remove(key);
        self.dirty.insert(key.to_owned());
        Ok(())
    }

    fn scan(&mut self) -> Result<Vec<(String, Vec<u8>)>, EdgeError> {
        Ok(self
            .values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

/// Ceiling on one exported key's serialized value. Cloudflare Durable Object
/// storage caps a single value at 128 KiB; this leaves headroom below that
/// limit. A mutating request that would write one key whose value exceeds the
/// ceiling is rejected with HTTP 507 and the in-memory state is rolled back to
/// the last persisted state, so storage stays authoritative.
pub const MAX_VALUE_BYTES: usize = 120 * 1024;

/// Storage-key under which one Redis key's serialized entry is held:
/// `format!("k:{}", hex(redis_key))`, lowercase hex of the raw key bytes.
fn key_storage_key(redis_key: &[u8]) -> String {
    let mut out = String::with_capacity(KEY_PREFIX.len() + redis_key.len() * 2);
    out.push_str(KEY_PREFIX);
    for byte in redis_key {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    out
}

#[derive(Debug, Clone)]
pub struct EdgeObject<S> {
    shard: EdgeShard,
    storage: S,
    allow_client_time: bool,
    persisted_epoch: u64,
}

impl<S: ObjectStorage> EdgeObject<S> {
    /// Restore from per-key storage entries: scan the store and import every
    /// entry whose storage-key carries `KEY_PREFIX`, ignoring any others.
    /// `persisted_epoch` starts at the freshly-imported shard's epoch (0 after
    /// a clean cold start, since `import_key` does not dirty anything), which
    /// is correct because nothing is pending a flush.
    pub fn open(mut storage: S) -> Result<Self, EdgeError> {
        let mut shard = EdgeShard::new();
        for (skey, bytes) in storage.scan()? {
            if skey.starts_with(KEY_PREFIX) {
                shard.import_key(&bytes)?;
            }
        }
        let persisted_epoch = shard.mutation_epoch();
        Ok(Self {
            shard,
            storage,
            allow_client_time: false,
            persisted_epoch,
        })
    }

    /// Deterministic-fixture mode: routes take `now_millis` from request
    /// bodies and the engine clock advances only through those values. The
    /// default (false) makes the host adapter's request clock authoritative
    /// and rejects client-supplied time, because a client that controls the
    /// clock can refill its own rate-limit buckets.
    pub fn with_client_time_allowed(mut self, allowed: bool) -> Self {
        self.allow_client_time = allowed;
        self
    }

    /// Engine mutation epoch covered by the bytes last written to (or read
    /// from) the underlying storage. Host adapters compare this against their
    /// own last-flushed epoch to skip redundant writes to provider storage.
    pub fn persisted_epoch(&self) -> u64 {
        self.persisted_epoch
    }

    pub fn install_policy(&mut self, tenant_id: &str, policy: Policy) -> Result<(), EdgeError> {
        self.shard.install_policy(tenant_id, policy)?;
        self.persist()
    }

    pub fn check(&mut self, request: LimitRequest<'_>) -> Result<LimitDecision, EdgeError> {
        let decision = self.shard.check(request)?;
        self.persist()?;
        Ok(decision)
    }

    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> Result<RestResponse, EdgeError> {
        let response = self.shard.execute_rest(request);
        self.persist()?;
        Ok(response)
    }

    pub fn handle_http(&mut self, request: EdgeHttpRequest<'_>) -> EdgeHttpResponse {
        if !self.allow_client_time {
            self.shard.set_now_millis(request.now_millis);
        }
        let (path, query) = split_query(request.path);
        let segments = match route_segments(path) {
            Ok(segments) => segments,
            Err(message) => return http_error(400, message),
        };

        if segments.len() == 3 && segments[0] == "v1" && segments[1] == "policy" {
            if !matches!(request.method, EdgeHttpMethod::Post | EdgeHttpMethod::Put) {
                return http_error(405, "ERR policy route requires POST or PUT");
            }
            let policy = match policy_from_json(request.body) {
                Ok(policy) => policy,
                Err(message) => return http_error(400, message),
            };
            return match self.install_policy(&segments[2], policy) {
                Ok(()) => json_response(200, json!({"result": "OK"})),
                Err(error) => edge_error_response(error),
            };
        }

        if segments.len() == 3 && segments[0] == "v1" && segments[1] == "limit" {
            if request.method != EdgeHttpMethod::Post {
                return http_error(405, "ERR limit route requires POST");
            }
            let body = match limit_body_from_json(request.body) {
                Ok(body) => body,
                Err(message) => return http_error(400, message),
            };
            let now_millis = match self.resolve_now(body.now_millis, request.now_millis) {
                Ok(now_millis) => now_millis,
                Err(message) => return http_error(400, message),
            };
            return match self.check(LimitRequest {
                tenant_id: &segments[2],
                now_millis,
                cost: body.cost,
            }) {
                Ok(decision) => json_response(200, limit_decision_json(decision)),
                Err(error) => edge_error_response(error),
            };
        }

        if segments.len() == 3 && segments[0] == "v1" && segments[1] == "ai" {
            if request.method != EdgeHttpMethod::Post {
                return http_error(405, "ERR AI route requires POST");
            }
            let demo = match demo_ai_from_json(request.body) {
                Ok(demo) => demo,
                Err(message) => return http_error(400, message),
            };
            let now_millis = match self.resolve_now(demo.now_millis, request.now_millis) {
                Ok(now_millis) => now_millis,
                Err(message) => return http_error(400, message),
            };
            let decision = match self.check(LimitRequest {
                tenant_id: &segments[2],
                now_millis,
                cost: demo.tokens,
            }) {
                Ok(decision) => decision,
                Err(error) => return edge_error_response(error),
            };
            if !decision.allowed {
                return json_response(
                    429,
                    json!({
                        "ok": false,
                        "error": "rate_limited",
                        "tenant": segments[2],
                        "charged_tokens": 0,
                        "limit": limit_decision_json(decision),
                    }),
                );
            }
            return json_response(
                200,
                json!({
                    "ok": true,
                    "tenant": segments[2],
                    "model": "toy-edge-llm",
                    "charged_tokens": demo.tokens,
                    "completion": toy_completion(&demo.prompt),
                    "limit": limit_decision_json(decision),
                }),
            );
        }

        if segments.len() >= 4 && segments[0] == "v1" && segments[1] == "valdr" {
            let rest_path = valdr_rest_path(&segments[3..], query);
            let rest_method = match request.method {
                EdgeHttpMethod::Get => valdr_engine::RestMethod::Get,
                EdgeHttpMethod::Post => valdr_engine::RestMethod::Post,
                EdgeHttpMethod::Put => valdr_engine::RestMethod::Put,
                EdgeHttpMethod::Head => valdr_engine::RestMethod::Head,
                EdgeHttpMethod::Other => return http_error(405, "ERR unsupported method"),
            };
            return match self.execute_rest(RestRequest {
                method: rest_method,
                path: &rest_path,
                body: request.body,
                response_format: valdr_engine::RestResponseFormat::Json,
            }) {
                Ok(response) => response.into(),
                Err(error) => edge_error_response(error),
            };
        }

        http_error(404, "ERR route not found")
    }

    pub fn shard(&self) -> &EdgeShard {
        &self.shard
    }

    pub fn shard_mut(&mut self) -> &mut EdgeShard {
        &mut self.shard
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Flush only the keys the shard changed since the last flush, O(dirty).
    /// Pass one exports every dirty key and rejects the whole flush if any one
    /// value exceeds the per-key budget, rolling state back from storage on
    /// that rare error path. Pass two commits the writes and deletes.
    fn persist(&mut self) -> Result<(), EdgeError> {
        let dirty = self.shard.take_dirty();
        if dirty.is_empty() {
            return Ok(());
        }
        let mut writes: Vec<(String, Option<Vec<u8>>)> = Vec::with_capacity(dirty.len());
        for key in &dirty {
            let bytes = self.shard.export_key(key);
            if let Some(ref b) = bytes {
                if b.len() > MAX_VALUE_BYTES {
                    let value_bytes = b.len();
                    self.rollback_from_storage()?;
                    return Err(EdgeError::ValueBudgetExceeded { value_bytes });
                }
            }
            writes.push((key_storage_key(key), bytes));
        }
        for (skey, bytes) in writes {
            match bytes {
                Some(b) => self.storage.put(&skey, &b)?,
                None => self.storage.delete(&skey)?,
            }
        }
        self.persisted_epoch = self.shard.mutation_epoch();
        Ok(())
    }

    /// Rebuild the shard from authoritative storage, discarding the in-flight
    /// mutation that triggered the rollback. The limiter script cache resets,
    /// which is fine because it reloads on next use.
    fn rollback_from_storage(&mut self) -> Result<(), EdgeError> {
        let entries = self.storage.scan()?;
        let mut shard = EdgeShard::new();
        for (skey, bytes) in entries {
            if skey.starts_with(KEY_PREFIX) {
                shard.import_key(&bytes)?;
            }
        }
        self.shard = shard;
        self.persisted_epoch = self.shard.mutation_epoch();
        Ok(())
    }

    fn resolve_now(&self, client_now: Option<u64>, request_now: u64) -> Result<u64, &'static str> {
        if self.allow_client_time {
            return client_now.ok_or("ERR missing now_millis");
        }
        match client_now {
            Some(_) => Err("ERR client now_millis is not allowed; server time is authoritative"),
            None => Ok(request_now),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EdgeWorker {
    shards: Vec<EdgeShard>,
}

impl EdgeWorker {
    pub fn new(shard_count: usize) -> Result<Self, EdgeError> {
        if shard_count == 0 {
            return Err(EdgeError::InvalidShardCount);
        }
        Ok(Self {
            shards: (0..shard_count).map(|_| EdgeShard::new()).collect(),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_index(&self, tenant_id: &str) -> usize {
        shard_index(tenant_id.as_bytes(), self.shards.len())
    }

    pub fn install_policy(&mut self, tenant_id: &str, policy: Policy) -> Result<(), EdgeError> {
        self.shard_for_mut(tenant_id)
            .install_policy(tenant_id, policy)
    }

    pub fn check(&mut self, request: LimitRequest<'_>) -> Result<LimitDecision, EdgeError> {
        self.shard_for_mut(request.tenant_id).check(request)
    }

    pub fn execute_rest_on_tenant_shard(
        &mut self,
        tenant_id: &str,
        request: RestRequest<'_>,
    ) -> RestResponse {
        self.shard_for_mut(tenant_id).execute_rest(request)
    }

    fn shard_for_mut(&mut self, tenant_id: &str) -> &mut EdgeShard {
        let index = shard_index(tenant_id.as_bytes(), self.shards.len());
        &mut self.shards[index]
    }
}

#[derive(Debug, Clone)]
pub struct EdgeShard {
    engine: Engine<NoopHost>,
    limiter_sha: Option<String>,
}

impl EdgeShard {
    pub fn new() -> Self {
        Self {
            engine: Engine::new_in_memory(),
            limiter_sha: None,
        }
    }

    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> RestResponse {
        self.engine.execute_rest(request)
    }

    pub fn set_now_millis(&mut self, now_millis: u64) {
        self.engine.host_mut().set_now_millis(now_millis);
    }

    pub fn mutation_epoch(&self) -> u64 {
        self.engine.mutation_epoch()
    }

    pub fn export_snapshot(&mut self) -> Vec<u8> {
        self.engine.export_snapshot()
    }

    /// Drain the Redis keys the engine changed since the last call, sorted.
    /// A host flushes each one with `export_key`.
    pub fn take_dirty(&mut self) -> Vec<Vec<u8>> {
        self.engine.take_dirty()
    }

    /// Serialize one Redis key's live entry, or `None` when it is absent or
    /// expired (the host then deletes the matching storage-key).
    pub fn export_key(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.engine.export_key(key)
    }

    /// Restore one Redis key from bytes produced by `export_key`. Does not
    /// dirty. The cached limiter sha is cleared so it is re-derived from the
    /// freshly-imported script state on next use.
    pub fn import_key(&mut self, bytes: &[u8]) -> Result<(), EdgeError> {
        self.engine.import_key(bytes).map_err(EdgeError::Snapshot)?;
        self.limiter_sha = None;
        Ok(())
    }

    pub fn import_snapshot(&mut self, snapshot: &[u8]) -> Result<(), EdgeError> {
        self.engine
            .import_snapshot(snapshot)
            .map_err(EdgeError::Snapshot)?;
        self.limiter_sha = None;
        Ok(())
    }

    pub fn from_snapshot(snapshot: &[u8]) -> Result<Self, EdgeError> {
        let mut shard = Self::new();
        shard.import_snapshot(snapshot)?;
        Ok(shard)
    }

    pub fn install_policy(&mut self, tenant_id: &str, policy: Policy) -> Result<(), EdgeError> {
        let body = serde_json::to_vec(&json!([
            "HSET",
            policy_key(tenant_id),
            "capacity",
            policy.capacity,
            "refill_tokens",
            policy.refill_tokens,
            "refill_ms",
            policy.refill_ms,
            "ttl_ms",
            policy.ttl_ms
        ]))
        .map_err(|_| EdgeError::JsonBody)?;
        let response = self.engine.execute_rest(RestRequest::post("/", &body));
        ensure_ok(response).map(|_| ())
    }

    pub fn check(&mut self, request: LimitRequest<'_>) -> Result<LimitDecision, EdgeError> {
        self.engine.host_mut().set_now_millis(request.now_millis);
        let sha = self.ensure_limiter_script()?;
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            2,
            bucket_key(request.tenant_id),
            policy_key(request.tenant_id),
            request.now_millis,
            request.cost
        ]))
        .map_err(|_| EdgeError::JsonBody)?;
        let response = self
            .engine
            .execute_rest(RestRequest::post("/EVALSHA", &body));
        parse_limit_decision(response)
    }

    fn ensure_limiter_script(&mut self) -> Result<&str, EdgeError> {
        if self.limiter_sha.is_none() {
            let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", LIMITER_SCRIPT]))
                .map_err(|_| EdgeError::JsonBody)?;
            let response = self.engine.execute_rest(RestRequest::post("/", &body));
            let value = ensure_ok(response)?;
            let sha = value
                .get("result")
                .and_then(JsonValue::as_str)
                .ok_or(EdgeError::MissingResult)?;
            self.limiter_sha = Some(sha.to_owned());
        }
        self.limiter_sha.as_deref().ok_or(EdgeError::MissingResult)
    }
}

impl Default for EdgeShard {
    fn default() -> Self {
        Self::new()
    }
}

impl From<RestResponse> for EdgeHttpResponse {
    fn from(response: RestResponse) -> Self {
        Self {
            status: response.status,
            content_type: response.content_type,
            body: response.body,
        }
    }
}

fn split_query(path: &str) -> (&str, &str) {
    match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    }
}

fn route_segments(path: &str) -> Result<Vec<String>, &'static str> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            String::from_utf8(percent_decode(segment.as_bytes())?)
                .map_err(|_| "ERR route segment must be UTF-8")
        })
        .collect()
}

fn valdr_rest_path(command: &[String], query: &str) -> String {
    let mut out = String::new();
    for segment in command {
        out.push('/');
        out.push_str(&percent_encode(segment.as_bytes()));
    }
    if !query.is_empty() {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn policy_from_json(body: &[u8]) -> Result<Policy, &'static str> {
    let value: JsonValue = serde_json::from_slice(body).map_err(|_| "ERR invalid policy JSON")?;
    Ok(Policy {
        capacity: required_i64(&value, "capacity")?,
        refill_tokens: required_i64(&value, "refill_tokens")?,
        refill_ms: required_i64(&value, "refill_ms")?,
        ttl_ms: required_i64(&value, "ttl_ms")?,
    })
}

fn limit_body_from_json(body: &[u8]) -> Result<LimitBody, &'static str> {
    let value: JsonValue = serde_json::from_slice(body).map_err(|_| "ERR invalid limit JSON")?;
    Ok(LimitBody {
        now_millis: optional_now_millis(&value)?,
        cost: required_i64(&value, "cost")?,
    })
}

fn optional_now_millis(value: &JsonValue) -> Result<Option<u64>, &'static str> {
    match value.get("now_millis") {
        Some(raw) => raw
            .as_u64()
            .map(Some)
            .ok_or("ERR now_millis must be a non-negative integer"),
        None => Ok(None),
    }
}

fn demo_ai_from_json(body: &[u8]) -> Result<DemoAiRequest, &'static str> {
    let value: JsonValue = serde_json::from_slice(body).map_err(|_| "ERR invalid AI JSON")?;
    let prompt = value
        .get("prompt")
        .and_then(JsonValue::as_str)
        .ok_or("ERR missing prompt")?;
    let tokens = value
        .get("tokens")
        .or_else(|| value.get("cost"))
        .and_then(JsonValue::as_i64)
        .ok_or("ERR missing tokens")?;
    if tokens <= 0 {
        return Err("ERR tokens must be positive");
    }
    Ok(DemoAiRequest {
        now_millis: optional_now_millis(&value)?,
        prompt: prompt.to_owned(),
        tokens,
    })
}

fn required_i64(value: &JsonValue, field: &'static str) -> Result<i64, &'static str> {
    value
        .get(field)
        .and_then(JsonValue::as_i64)
        .ok_or(match field {
            "capacity" => "ERR missing capacity",
            "refill_tokens" => "ERR missing refill_tokens",
            "refill_ms" => "ERR missing refill_ms",
            "ttl_ms" => "ERR missing ttl_ms",
            "cost" => "ERR missing cost",
            _ => "ERR missing field",
        })
}

fn limit_decision_json(decision: LimitDecision) -> JsonValue {
    json!({
        "allowed": decision.allowed,
        "remaining": decision.remaining,
        "reset_ms": decision.reset_ms,
        "retry_after_ms": decision.retry_after_ms,
        "capacity": decision.capacity,
    })
}

fn toy_completion(prompt: &str) -> String {
    let trimmed = prompt.trim();
    if trimmed.is_empty() {
        "EdgeStash accepted an empty prompt and charged the request.".to_owned()
    } else {
        format!("EdgeStash accepted: {trimmed}")
    }
}

fn edge_error_response(error: EdgeError) -> EdgeHttpResponse {
    match error {
        EdgeError::RestError { status, body } => EdgeHttpResponse {
            status,
            content_type: APPLICATION_JSON,
            body,
        },
        EdgeError::ValueBudgetExceeded { .. } => http_error(
            507,
            "ERR value too large; request rolled back to last persisted state",
        ),
        other => http_error(500, edge_error_message(&other)),
    }
}

fn edge_error_message(error: &EdgeError) -> &'static str {
    match error {
        EdgeError::InvalidShardCount => "ERR invalid shard count",
        EdgeError::JsonBody => "ERR JSON encode failed",
        EdgeError::Snapshot(_) => "ERR snapshot failed",
        EdgeError::Storage => "ERR storage failed",
        EdgeError::RestError { .. } => "ERR command failed",
        EdgeError::MissingResult => "ERR missing result",
        EdgeError::UnexpectedResult => "ERR unexpected result",
        EdgeError::ValueBudgetExceeded { .. } => "ERR value too large",
    }
}

fn json_response(status: u16, value: JsonValue) -> EdgeHttpResponse {
    match serde_json::to_vec(&value) {
        Ok(body) => EdgeHttpResponse {
            status,
            content_type: APPLICATION_JSON,
            body,
        },
        Err(_) => http_error(500, "ERR JSON encode failed"),
    }
}

fn http_error(status: u16, message: &str) -> EdgeHttpResponse {
    json_response(status, json!({ "error": message }))
}

fn percent_decode(input: &[u8]) -> Result<Vec<u8>, &'static str> {
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'%' => {
                if index + 2 >= input.len() {
                    return Err("ERR invalid URL escape");
                }
                let high = hex_nibble(input[index + 1]).ok_or("ERR invalid URL escape")?;
                let low = hex_nibble(input[index + 2]).ok_or("ERR invalid URL escape")?;
                out.push((high << 4) | low);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Ok(out)
}

fn percent_encode(input: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(input.len());
    for byte in input {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            out.push(*byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    out
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_limit_decision(response: RestResponse) -> Result<LimitDecision, EdgeError> {
    let value = ensure_ok(response)?;
    let items = value
        .get("result")
        .and_then(JsonValue::as_array)
        .ok_or(EdgeError::MissingResult)?;
    Ok(LimitDecision {
        allowed: field_i64(items, "allowed")? != 0,
        remaining: field_i64(items, "remaining")?,
        reset_ms: field_i64(items, "reset_ms")?,
        retry_after_ms: field_i64(items, "retry_after_ms")?,
        capacity: field_i64(items, "capacity")?,
    })
}

fn field_i64(items: &[JsonValue], name: &str) -> Result<i64, EdgeError> {
    let mut iter = items.chunks_exact(2);
    for pair in &mut iter {
        if pair[0].as_str() == Some(name) {
            return pair[1].as_i64().ok_or(EdgeError::UnexpectedResult);
        }
    }
    Err(EdgeError::UnexpectedResult)
}

fn ensure_ok(response: RestResponse) -> Result<JsonValue, EdgeError> {
    if response.status != 200 {
        return Err(EdgeError::RestError {
            status: response.status,
            body: response.body,
        });
    }
    serde_json::from_slice(&response.body).map_err(|_| EdgeError::JsonBody)
}

fn bucket_key(tenant_id: &str) -> String {
    format!("edgestash:{{{tenant_id}}}:tokens")
}

fn policy_key(tenant_id: &str) -> String {
    format!("edgestash:{{{tenant_id}}}:policy")
}

fn shard_index(key: &[u8], shard_count: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % shard_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use valdr_engine::RestRequest;

    #[test]
    fn worker_routes_tenant_to_stable_shard_and_runs_limiter() {
        let mut worker = EdgeWorker::new(8).unwrap();
        let shard = worker.shard_index("tenant-42");
        assert_eq!(worker.shard_index("tenant-42"), shard);
        assert_ne!(worker.shard_count(), 0);

        worker
            .install_policy("tenant-42", Policy::token_bucket(10, 5, 1_000, 60_000))
            .unwrap();

        assert_eq!(
            worker
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_000,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: true,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 0,
                capacity: 10,
            }
        );
        assert_eq!(
            worker
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_100,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: false,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 700,
                capacity: 10,
            }
        );

        worker
            .install_policy("tenant-42", Policy::token_bucket(20, 5, 1_000, 60_000))
            .unwrap();
        assert_eq!(
            worker
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_800,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: true,
                remaining: 0,
                reset_ms: 5_800,
                retry_after_ms: 0,
                capacity: 20,
            }
        );
    }

    #[test]
    fn tenants_are_isolated_even_when_sharing_a_worker() {
        let mut worker = EdgeWorker::new(2).unwrap();
        worker
            .install_policy("free", Policy::token_bucket(10, 5, 1_000, 60_000))
            .unwrap();
        worker
            .install_policy("enterprise", Policy::token_bucket(100, 50, 1_000, 60_000))
            .unwrap();

        let free = worker
            .check(LimitRequest {
                tenant_id: "free",
                now_millis: 1_000,
                cost: 7,
            })
            .unwrap();
        let enterprise = worker
            .check(LimitRequest {
                tenant_id: "enterprise",
                now_millis: 1_000,
                cost: 7,
            })
            .unwrap();

        assert_eq!(free.remaining, 3);
        assert_eq!(free.capacity, 10);
        assert_eq!(enterprise.remaining, 93);
        assert_eq!(enterprise.capacity, 100);
    }

    #[test]
    fn shard_snapshot_restore_preserves_limiter_state_after_cold_start() {
        let mut shard = EdgeShard::new();
        shard
            .install_policy("tenant-42", Policy::token_bucket(10, 5, 1_000, 60_000))
            .unwrap();
        assert_eq!(
            shard
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_000,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: true,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 0,
                capacity: 10,
            }
        );

        let snapshot = shard.export_snapshot();
        let mut restored = EdgeShard::from_snapshot(&snapshot).unwrap();

        assert_eq!(
            restored
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_100,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: false,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 700,
                capacity: 10,
            }
        );
    }

    #[test]
    fn edge_object_storage_binding_persists_limiter_state_across_reopen() {
        let storage = MemoryObjectStorage::default();
        let mut object = EdgeObject::open(storage).unwrap();

        object
            .install_policy("tenant-42", Policy::token_bucket(10, 5, 1_000, 60_000))
            .unwrap();
        assert!(object
            .storage_mut()
            .scan()
            .unwrap()
            .iter()
            .any(|(skey, _)| skey.starts_with(KEY_PREFIX)));

        assert_eq!(
            object
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_000,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: true,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 0,
                capacity: 10,
            }
        );

        let storage = object.into_storage();
        let mut reopened = EdgeObject::open(storage).unwrap();
        assert_eq!(
            reopened
                .check(LimitRequest {
                    tenant_id: "tenant-42",
                    now_millis: 1_100,
                    cost: 7,
                })
                .unwrap(),
            LimitDecision {
                allowed: false,
                remaining: 3,
                reset_ms: 2_400,
                retry_after_ms: 700,
                capacity: 10,
            }
        );
    }

    #[test]
    fn edge_object_rest_commands_persist_across_reopen() {
        let storage = MemoryObjectStorage::default();
        let mut object = EdgeObject::open(storage).unwrap();
        let response = object
            .execute_rest(RestRequest::get("/SET/raw-key/42"))
            .unwrap();
        assert_eq!(response.status, 200);

        let storage = object.into_storage();
        let mut reopened = EdgeObject::open(storage).unwrap();
        let response = reopened
            .execute_rest(RestRequest::get("/GET/raw-key"))
            .unwrap();
        let value: JsonValue = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(value, json!({"result": "42"}));
    }

    #[test]
    fn http_policy_and_limit_routes_persist_across_reopen() {
        let storage = MemoryObjectStorage::default();
        let mut object = EdgeObject::open(storage)
            .unwrap()
            .with_client_time_allowed(true);

        let policy = br#"{
            "capacity": 10,
            "refill_tokens": 5,
            "refill_ms": 1000,
            "ttl_ms": 60000
        }"#;
        let response = object.handle_http(EdgeHttpRequest::put("/v1/policy/tenant-42", policy, 0));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": "OK"})
        );

        let response = object.handle_http(EdgeHttpRequest::post(
            "/v1/limit/tenant-42",
            br#"{"now_millis":1000,"cost":7}"#,
            0,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({
                "allowed": true,
                "remaining": 3,
                "reset_ms": 2400,
                "retry_after_ms": 0,
                "capacity": 10
            })
        );

        let storage = object.into_storage();
        let mut reopened = EdgeObject::open(storage)
            .unwrap()
            .with_client_time_allowed(true);
        let response = reopened.handle_http(EdgeHttpRequest::post(
            "/v1/limit/tenant-42",
            br#"{"now_millis":1100,"cost":7}"#,
            0,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({
                "allowed": false,
                "remaining": 3,
                "reset_ms": 2400,
                "retry_after_ms": 700,
                "capacity": 10
            })
        );
    }

    #[test]
    fn http_raw_valdr_route_uses_upstash_shape_and_storage_binding() {
        let storage = MemoryObjectStorage::default();
        let mut object = EdgeObject::open(storage).unwrap();

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/raw%2Fkey/hello%20edge",
            1_000,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": "OK"})
        );

        let storage = object.into_storage();
        let mut reopened = EdgeObject::open(storage).unwrap();
        let response = reopened.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/GET/raw%2Fkey",
            2_000,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": "hello edge"})
        );
    }

    #[test]
    fn http_ai_demo_route_spends_tokens_through_lua_limiter() {
        let storage = MemoryObjectStorage::default();
        let mut object = EdgeObject::open(storage)
            .unwrap()
            .with_client_time_allowed(true);

        let policy = br#"{
            "capacity": 10,
            "refill_tokens": 5,
            "refill_ms": 1000,
            "ttl_ms": 60000
        }"#;
        assert_eq!(
            object
                .handle_http(EdgeHttpRequest::put("/v1/policy/tenant-42", policy, 0))
                .status,
            200
        );

        let response = object.handle_http(EdgeHttpRequest::post(
            "/v1/ai/tenant-42",
            br#"{"now_millis":1000,"tokens":7,"prompt":"summarize invoices"}"#,
            0,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({
                "ok": true,
                "tenant": "tenant-42",
                "model": "toy-edge-llm",
                "charged_tokens": 7,
                "completion": "EdgeStash accepted: summarize invoices",
                "limit": {
                    "allowed": true,
                    "remaining": 3,
                    "reset_ms": 2400,
                    "retry_after_ms": 0,
                    "capacity": 10
                }
            })
        );

        let storage = object.into_storage();
        let mut reopened = EdgeObject::open(storage)
            .unwrap()
            .with_client_time_allowed(true);
        let response = reopened.handle_http(EdgeHttpRequest::post(
            "/v1/ai/tenant-42",
            br#"{"now_millis":1100,"tokens":7,"prompt":"summarize invoices"}"#,
            0,
        ));
        assert_eq!(response.status, 429);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({
                "ok": false,
                "error": "rate_limited",
                "tenant": "tenant-42",
                "charged_tokens": 0,
                "limit": {
                    "allowed": false,
                    "remaining": 3,
                    "reset_ms": 2400,
                    "retry_after_ms": 700,
                    "capacity": 10
                }
            })
        );
    }

    #[test]
    fn http_routes_return_explicit_errors_for_bad_requests() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();

        let response = object.handle_http(EdgeHttpRequest::get("/v1/limit/tenant-42", 0));
        assert_eq!(response.status, 405);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"error": "ERR limit route requires POST"})
        );

        let response = object.handle_http(EdgeHttpRequest::put(
            "/v1/policy/tenant-42",
            br#"{"capacity":10}"#,
            0,
        ));
        assert_eq!(response.status, 400);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"error": "ERR missing refill_tokens"})
        );

        let response = object.handle_http(EdgeHttpRequest::post(
            "/v1/ai/tenant-42",
            br#"{"now_millis":1000,"tokens":0,"prompt":"hello"}"#,
            0,
        ));
        assert_eq!(response.status, 400);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"error": "ERR tokens must be positive"})
        );
    }

    #[test]
    fn worker_can_still_expose_raw_upstash_style_rest_on_the_tenant_shard() {
        let mut worker = EdgeWorker::new(1).unwrap();
        let response =
            worker.execute_rest_on_tenant_shard("tenant-42", RestRequest::get("/SET/raw-key/42"));
        assert_eq!(response.status, 200);

        let response =
            worker.execute_rest_on_tenant_shard("tenant-42", RestRequest::get("/GET/raw-key"));
        let value: JsonValue = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(value, json!({"result": "42"}));
    }

    #[test]
    fn zero_shards_is_rejected() {
        assert_eq!(
            EdgeWorker::new(0).unwrap_err(),
            EdgeError::InvalidShardCount
        );
    }

    #[test]
    fn secure_mode_rejects_client_now_millis() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let policy = br#"{"capacity":10,"refill_tokens":5,"refill_ms":1000,"ttl_ms":60000}"#;
        assert_eq!(
            object
                .handle_http(EdgeHttpRequest::put("/v1/policy/tenant-42", policy, 500))
                .status,
            200
        );

        let response = object.handle_http(EdgeHttpRequest::post(
            "/v1/limit/tenant-42",
            br#"{"now_millis":1000,"cost":7}"#,
            1_000,
        ));
        assert_eq!(response.status, 400);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"error": "ERR client now_millis is not allowed; server time is authoritative"})
        );

        let response = object.handle_http(EdgeHttpRequest::post(
            "/v1/ai/tenant-42",
            br#"{"now_millis":1000,"tokens":7,"prompt":"hello"}"#,
            1_000,
        ));
        assert_eq!(response.status, 400);
    }

    #[test]
    fn secure_mode_limiter_runs_on_request_clock() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let policy = br#"{"capacity":10,"refill_tokens":5,"refill_ms":1000,"ttl_ms":60000}"#;
        assert_eq!(
            object
                .handle_http(EdgeHttpRequest::put("/v1/policy/tenant-42", policy, 500))
                .status,
            200
        );

        let first = object.handle_http(EdgeHttpRequest::post(
            "/v1/limit/tenant-42",
            br#"{"cost":7}"#,
            1_000,
        ));
        assert_eq!(first.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&first.body).unwrap(),
            json!({
                "allowed": true,
                "remaining": 3,
                "reset_ms": 2400,
                "retry_after_ms": 0,
                "capacity": 10
            })
        );

        let second = object.handle_http(EdgeHttpRequest::post(
            "/v1/limit/tenant-42",
            br#"{"cost":7}"#,
            1_100,
        ));
        assert_eq!(second.status, 200);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&second.body).unwrap(),
            json!({
                "allowed": false,
                "remaining": 3,
                "reset_ms": 2400,
                "retry_after_ms": 700,
                "capacity": 10
            })
        );
    }

    #[test]
    fn secure_mode_raw_route_expiries_follow_request_clock() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/session/abc?PX=5000",
            10_000,
        ));
        assert_eq!(response.status, 200);

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/PTTL/session",
            12_000,
        ));
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": 3000})
        );

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/GET/session",
            16_000,
        ));
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": JsonValue::Null})
        );
    }

    #[test]
    fn read_only_requests_do_not_advance_persisted_epoch() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/raw-key/42",
            1_000,
        ));
        assert_eq!(response.status, 200);
        let epoch_after_write = object.persisted_epoch();

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/GET/raw-key",
            2_000,
        ));
        assert_eq!(response.status, 200);
        assert_eq!(object.persisted_epoch(), epoch_after_write);

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/raw-key/43",
            3_000,
        ));
        assert_eq!(response.status, 200);
        assert_ne!(object.persisted_epoch(), epoch_after_write);
    }

    #[test]
    fn value_budget_rejects_oversized_state_and_rolls_back() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/small/keep-me",
            1_000,
        ));
        assert_eq!(response.status, 200);

        let oversized = vec![b'x'; MAX_VALUE_BYTES];
        let response = object.handle_http(EdgeHttpRequest {
            method: EdgeHttpMethod::Post,
            path: "/v1/valdr/tenant-42/SET/big",
            body: &oversized,
            now_millis: 2_000,
        });
        assert_eq!(response.status, 507);
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"error": "ERR value too large; request rolled back to last persisted state"})
        );

        let response =
            object.handle_http(EdgeHttpRequest::get("/v1/valdr/tenant-42/GET/big", 3_000));
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": JsonValue::Null})
        );
        let response =
            object.handle_http(EdgeHttpRequest::get("/v1/valdr/tenant-42/GET/small", 3_000));
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": "keep-me"})
        );
    }

    #[test]
    fn value_budget_on_fresh_object_resets_to_empty_state() {
        let mut object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        let oversized = vec![b'x'; MAX_VALUE_BYTES];
        let response = object.handle_http(EdgeHttpRequest {
            method: EdgeHttpMethod::Post,
            path: "/v1/valdr/tenant-42/SET/big",
            body: &oversized,
            now_millis: 1_000,
        });
        assert_eq!(response.status, 507);

        let response = object.handle_http(EdgeHttpRequest::get(
            "/v1/valdr/tenant-42/SET/after/ok",
            2_000,
        ));
        assert_eq!(response.status, 200);
        let response =
            object.handle_http(EdgeHttpRequest::get("/v1/valdr/tenant-42/GET/after", 3_000));
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&response.body).unwrap(),
            json!({"result": "ok"})
        );
    }
}

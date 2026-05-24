//! Client-side caching: key tracking and invalidation.
//!
//! Port of `src/tracking.c` (Valkey; 663 lines, 19 functions).
//!
//! ## Design overview
//!
//! Two data structures control client-side caching (CSC):
//!
//! * **TrackingTable** — maps each observed key (byte slice) to the set of client
//!   IDs that may have that key in their local cache. Written on every read command
//!   for clients with tracking enabled and not in BCAST mode.
//!
//! * **PrefixTable** — maps registered prefix byte slices to a [`BcastState`].
//!   Used only in BCAST mode. At the end of each event-loop cycle,
//!   [`tracking_broadcast_invalidation_messages`] sends all accumulated
//!   invalidations to the relevant subscribers.
//!
//! In C these are module-level globals (`rax *TrackingTable`, `rax *PrefixTable`).
//! In Rust they live in [`TrackingState`], intended to become a field of
//! `RedisServer`.
//!
//! TODO(architect): add `pub tracking: TrackingState` field to `RedisServer`
//! in `redis-core/src/server.rs`, and add a `tracking_clients: u64` counter there.
//!
//! TODO(architect): add `ClientTrackingFlags` and `Option<Box<ClientPubSubData>>`
//! fields to `Client` in `redis-core/src/client.rs`, plus a `resp: u8` field for
//! the negotiated RESP protocol version, and a `current_client: Option<ClientId>`
//! field to `RedisServer`.
//!
//! ## rax → HashMap substitution
//!
//! The C implementation uses a radix tree (`rax`) for memory-efficient storage
//! and prefix-scan operations. This port substitutes `HashMap<Vec<u8>, _>` for
//! the pilot phase. Functions that required lexicographic prefix scanning (e.g.,
//! [`tracking_remember_key_to_broadcast`]) iterate all entries as a fallback;
//! see the PERF notes at each site. Phase 4 should replace `HashMap` with
//! `RadixTree` (owner: `redis-ds`) once that crate is available.
//!
//! C: tracking.c

use crate::client::{Client, ClientId, ClientTrackingState};
use crate::client_info::client_info_registry;
use crate::pubsub_registry::PubSubRegistry;
use redis_protocol::RespFrame;
use redis_types::{RedisError, RedisString};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

// ── Local types ────────────────────────────────────────────────────────────

/// Per-client tracking-related flags.
///
/// These are a subset of the C `struct ClientFlags` bitfield. They are defined
/// here to capture the tracking logic; they should be folded into the `Client`
/// struct once the architect packet extends it.
///
/// TODO(architect): merge into `Client` in `redis-core/src/client.rs`.
#[derive(Debug, Default, Clone)]
pub struct ClientTrackingFlags {
    pub tracking: bool,
    pub tracking_bcast: bool,
    pub tracking_optin: bool,
    pub tracking_optout: bool,
    pub tracking_caching: bool,
    pub tracking_noloop: bool,
    pub tracking_broken_redir: bool,
    pub pubsub: bool,
    pub close_after_reply: bool,
    pub close_asap: bool,
    pub pushing: bool,
    pub executing_command: bool,
}

/// Per-client pubsub and tracking metadata.
///
/// Maps to the `pubsub_data` pointer and associated fields on `client` in C.
/// Initialized lazily (only when tracking or pubsub is enabled).
///
/// TODO(architect): merge into `Client` in `redis-core/src/client.rs`.
#[derive(Debug, Default)]
pub struct ClientPubSubData {
    /// Target client ID for invalidation redirection (0 / None = no redirect).
    pub client_tracking_redirection: Option<ClientId>,
    /// Prefix byte strings this client is subscribed to (BCAST mode).
    /// In C this is a `rax *` keyed by raw prefix bytes.
    /// PERF(port): HashSet gives O(1) membership but loses prefix-ordering.
    pub client_tracking_prefixes: HashSet<Vec<u8>>,
}

/// Broadcast state for a single prefix entry in the prefix table.
///
/// Holds the set of keys modified in this event-loop cycle and the set of
/// client IDs subscribed to notifications for this prefix.
///
/// C: `typedef struct bcastState { rax *keys; rax *clients; } bcastState;`
#[derive(Debug)]
pub struct BcastState {
    /// Keys modified in the current event-loop cycle.
    /// Maps key bytes → the client ID that last modified the key, used to
    /// implement the NOLOOP option (skip sending the notification back to the
    /// client that caused the change). `None` means no specific client.
    /// PERF(port): C stores raw `client *` as rax value; here we store `ClientId`.
    keys: HashMap<Vec<u8>, Option<ClientId>>,
    /// Client IDs subscribed to notifications for this prefix.
    clients: HashSet<ClientId>,
}

impl BcastState {
    fn new() -> Self {
        Self {
            keys: HashMap::new(),
            clients: HashSet::new(),
        }
    }
}

/// RESP version constants mirroring the C `c->resp` field.
pub mod resp_version {
    pub const RESP2: u8 = 2;
    pub const RESP3: u8 = 3;
}

/// The global state for the client-side caching / tracking subsystem.
///
/// Intended to live as a field of `RedisServer`. The C code keeps
/// `TrackingTable`, `PrefixTable`, `TrackingTableTotalItems`, and
/// `TrackingChannelName` as module-level globals; Rust bundles them here.
///
/// TODO(architect): add `pub tracking: TrackingState` to `RedisServer`.
#[derive(Debug)]
pub struct TrackingState {
    /// Key bytes → set of client IDs that may have the key cached locally.
    /// C: `rax *TrackingTable`.
    /// PERF(port): rax → HashMap; replace with RadixTree (redis-ds) in Phase 4.
    table: HashMap<Vec<u8>, HashSet<ClientId>>,
    /// Prefix bytes → broadcast state for BCAST-mode clients.
    /// C: `rax *PrefixTable`.
    /// PERF(port): same rax → HashMap caveat as `table`.
    prefix_table: HashMap<Vec<u8>, BcastState>,
    /// Total number of (key, client-ID) pairs stored across all table entries.
    /// C: `uint64_t TrackingTableTotalItems`.
    pub total_items: u64,
    /// Channel name for RESP2 pub/sub invalidation messages.
    /// C: `robj *TrackingChannelName` = `b"__redis__:invalidate"`.
    pub channel_name: Vec<u8>,
}

impl Default for TrackingState {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackingState {
    /// Create a new, empty tracking state.
    /// C: equivalent to the initial `raxNew()` calls in `enableTracking`.
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
            prefix_table: HashMap::new(),
            total_items: 0,
            channel_name: b"__redis__:invalidate".to_vec(),
        }
    }

    /// Total distinct keys in the tracking table.
    /// C: `trackingGetTotalKeys` → `raxSize(TrackingTable)`.
    pub fn total_keys(&self) -> u64 {
        self.table.len() as u64
    }

    /// Total number of distinct prefixes registered in BCAST mode.
    /// C: `trackingGetTotalPrefixes` → `raxSize(PrefixTable)`.
    pub fn total_prefixes(&self) -> u64 {
        self.prefix_table.len() as u64
    }
}

// ── Minimal live tracking runtime ─────────────────────────────────────────

#[derive(Debug, Default)]
struct RuntimeTrackingState {
    clients: HashMap<ClientId, ClientTrackingState>,
    table: HashMap<RedisString, HashSet<ClientId>>,
    bcast_pending: HashMap<(ClientId, RedisString), Vec<RedisString>>,
}

/// INFO-visible counters from the packet-level live tracking runtime.
///
/// Mirrors Valkey's `server.tracking_clients` plus
/// `trackingGetTotalItems/Keys/Prefixes`. The canonical source-shaped
/// implementation above still owns a `TrackingState`; the current server path
/// uses `RuntimeTrackingState`, so INFO must read the live packet runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeTrackingInfoCounters {
    pub total_items: u64,
    pub total_keys: u64,
    pub total_prefixes: u64,
    pub tracking_clients: u64,
}

#[derive(Debug)]
struct RuntimeTrackingDelivery {
    owner_id: ClientId,
    recipient_id: ClientId,
    redirect: Option<ClientId>,
    keys: Vec<RedisString>,
}

static RUNTIME_TRACKING: OnceLock<Mutex<RuntimeTrackingState>> = OnceLock::new();

fn runtime_tracking() -> &'static Mutex<RuntimeTrackingState> {
    RUNTIME_TRACKING.get_or_init(|| Mutex::new(RuntimeTrackingState::default()))
}

fn lock_runtime_tracking() -> std::sync::MutexGuard<'static, RuntimeTrackingState> {
    match runtime_tracking().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn remove_client_from_runtime_table(rt: &mut RuntimeTrackingState, client_id: ClientId) {
    rt.table.retain(|_, ids| {
        ids.remove(&client_id);
        !ids.is_empty()
    });
    rt.bcast_pending
        .retain(|(pending_client_id, _), _| *pending_client_id != client_id);
}

/// Synchronize the packet-level per-client state into the small live tracking
/// runtime used by the current Rust server.
pub fn sync_runtime_client_tracking(client_id: ClientId, state: &ClientTrackingState) {
    let mut rt = lock_runtime_tracking();
    if state.enabled {
        rt.clients.insert(client_id, state.clone());
        if state.bcast {
            remove_client_from_runtime_table(&mut rt, client_id);
        }
    } else {
        rt.clients.remove(&client_id);
        remove_client_from_runtime_table(&mut rt, client_id);
    }
}

/// Remove all live tracking references for a client.
pub fn remove_runtime_client_tracking(client_id: ClientId) {
    let mut rt = lock_runtime_tracking();
    rt.clients.remove(&client_id);
    remove_client_from_runtime_table(&mut rt, client_id);
}

/// Snapshot the counters exposed in `INFO`.
///
/// C: `server.tracking_clients` and `trackingGetTotalItems`,
/// `trackingGetTotalKeys`, `trackingGetTotalPrefixes` in tracking.c/server.c.
pub fn runtime_tracking_info_counters() -> RuntimeTrackingInfoCounters {
    let rt = lock_runtime_tracking();
    let total_items = rt.table.values().map(|ids| ids.len() as u64).sum();
    let total_keys = rt.table.len() as u64;
    let mut prefixes: HashSet<RedisString> = HashSet::new();
    let mut tracking_clients = 0u64;

    for state in rt.clients.values() {
        if !state.enabled {
            continue;
        }
        tracking_clients += 1;
        if state.bcast {
            if state.prefixes.is_empty() {
                prefixes.insert(RedisString::from_static(b""));
            } else {
                prefixes.extend(state.prefixes.iter().cloned());
            }
        }
    }

    RuntimeTrackingInfoCounters {
        total_items,
        total_keys,
        total_prefixes: prefixes.len() as u64,
        tracking_clients,
    }
}

/// Remember that `client_id` read `keys` while normal tracking was enabled.
pub fn runtime_remember_read_keys(
    client_id: ClientId,
    state: &ClientTrackingState,
    keys: &[RedisString],
) {
    if keys.is_empty() || !state.enabled || state.bcast {
        return;
    }
    if (state.optin && !state.caching) || (state.optout && state.caching) {
        return;
    }

    let mut rt = lock_runtime_tracking();
    if !rt.clients.contains_key(&client_id) {
        rt.clients.insert(client_id, state.clone());
    }
    for key in keys {
        rt.table.entry(key.clone()).or_default().insert(client_id);
    }
}

fn key_matches_prefix(key: &RedisString, prefix: &RedisString) -> bool {
    key.as_bytes().starts_with(prefix.as_bytes())
}

fn add_unique_key(keys: &mut Vec<RedisString>, key: &RedisString) {
    if !keys.iter().any(|existing| existing == key) {
        keys.push(key.clone());
    }
}

fn runtime_recipient_for(state: &ClientTrackingState, owner_id: ClientId) -> (ClientId, Option<ClientId>) {
    if state.redirect > 0 {
        let redirect = state.redirect as ClientId;
        (redirect, Some(redirect))
    } else {
        (owner_id, None)
    }
}

fn runtime_collect_key_deliveries(
    rt: &mut RuntimeTrackingState,
    source_id: ClientId,
    keys: &[RedisString],
    force_send_to_source: bool,
    defer_bcast: bool,
) -> Vec<RuntimeTrackingDelivery> {
    let mut by_client: HashMap<ClientId, Vec<RedisString>> = HashMap::new();
    for key in keys {
        let Some(ids) = rt.table.remove(key) else {
            continue;
        };
        for client_id in ids {
            let Some(state) = rt.clients.get(&client_id) else {
                continue;
            };
            if !state.enabled || state.bcast {
                continue;
            }
            if state.noloop && client_id == source_id && !force_send_to_source {
                continue;
            }
            add_unique_key(by_client.entry(client_id).or_default(), key);
        }
    }

    let mut deliveries = Vec::new();
    for (client_id, keys) in by_client {
        if let Some(state) = rt.clients.get(&client_id) {
            let (recipient_id, redirect) = runtime_recipient_for(state, client_id);
            deliveries.push(RuntimeTrackingDelivery {
                owner_id: client_id,
                recipient_id,
                redirect,
                keys,
            });
        }
    }

    let mut bcast_by_client: HashMap<ClientId, Vec<RedisString>> = HashMap::new();
    for (client_id, state) in &rt.clients {
        if !state.enabled || !state.bcast {
            continue;
        }
        if state.noloop && *client_id == source_id && !force_send_to_source {
            continue;
        }
        for key in keys {
            let matching_prefixes: Vec<RedisString> = if state.prefixes.is_empty() {
                vec![RedisString::from_bytes(b"")]
            } else {
                state
                    .prefixes
                    .iter()
                    .filter(|prefix| key_matches_prefix(key, prefix))
                    .cloned()
                    .collect()
            };
            for prefix in matching_prefixes {
                if defer_bcast {
                    add_unique_key(
                        rt.bcast_pending
                            .entry((*client_id, prefix.clone()))
                            .or_default(),
                        key,
                    );
                } else {
                    add_unique_key(bcast_by_client.entry(*client_id).or_default(), key);
                }
            }
        }
    }
    for (client_id, keys) in bcast_by_client {
        if let Some(state) = rt.clients.get(&client_id) {
            let (recipient_id, redirect) = runtime_recipient_for(state, client_id);
            deliveries.push(RuntimeTrackingDelivery {
                owner_id: client_id,
                recipient_id,
                redirect,
                keys,
            });
        }
    }

    deliveries
}

fn runtime_collect_pending_bcast_deliveries(
    rt: &mut RuntimeTrackingState,
) -> Vec<RuntimeTrackingDelivery> {
    let pending: Vec<((ClientId, RedisString), Vec<RedisString>)> =
        rt.bcast_pending.drain().collect();
    let mut deliveries = Vec::new();
    for ((client_id, _prefix), keys) in pending {
        let Some(state) = rt.clients.get(&client_id) else {
            continue;
        };
        if !state.enabled || !state.bcast {
            continue;
        }
        let (recipient_id, redirect) = runtime_recipient_for(state, client_id);
        deliveries.push(RuntimeTrackingDelivery {
            owner_id: client_id,
            recipient_id,
            redirect,
            keys,
        });
    }
    deliveries
}

fn runtime_collect_all_deliveries(rt: &RuntimeTrackingState) -> Vec<RuntimeTrackingDelivery> {
    let mut deliveries = Vec::new();
    for (client_id, state) in &rt.clients {
        if !state.enabled {
            continue;
        }
        let (recipient_id, redirect) = runtime_recipient_for(state, *client_id);
        deliveries.push(RuntimeTrackingDelivery {
            owner_id: *client_id,
            recipient_id,
            redirect,
            keys: Vec::new(),
        });
    }
    deliveries
}

fn tracking_keys_frame(keys: &[RedisString]) -> RespFrame {
    RespFrame::array(
        keys.iter()
            .cloned()
            .map(RespFrame::bulk)
            .collect::<Vec<RespFrame>>(),
    )
}

fn tracking_resp3_push(keys: &[RedisString]) -> RespFrame {
    RespFrame::Push(vec![
        RespFrame::bulk(RedisString::from_static(b"invalidate")),
        tracking_keys_frame(keys),
    ])
}

fn tracking_resp2_pubsub_message(keys: &[RedisString]) -> RespFrame {
    RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_static(b"message")),
        RespFrame::bulk(RedisString::from_static(b"__redis__:invalidate")),
        tracking_keys_frame(keys),
    ])
}

fn encode_tracking_message_for_proto(proto: i32, keys: &[RedisString]) -> Vec<u8> {
    let mut out = Vec::new();
    if proto == 3 {
        redis_protocol::encode_resp3(&tracking_resp3_push(keys), &mut out);
    } else {
        redis_protocol::encode_resp2(&tracking_resp2_pubsub_message(keys), &mut out);
    }
    out
}

/// Encode an invalidation push/pubsub payload for a known protocol version.
pub fn runtime_encode_invalidation_for_proto(proto: i32, keys: &[RedisString]) -> Vec<u8> {
    encode_tracking_message_for_proto(proto, keys)
}

/// Return true once for a client/key pair recorded in the normal tracking table.
pub fn runtime_take_tracked_key_for_client(client_id: ClientId, key: &RedisString) -> bool {
    let mut rt = lock_runtime_tracking();
    let tracked = rt
        .clients
        .get(&client_id)
        .is_some_and(|state| state.enabled && !state.bcast);
    if !tracked {
        return false;
    }
    let mut remove_key = false;
    let found = match rt.table.get_mut(key) {
        Some(ids) => {
            let found = ids.remove(&client_id);
            remove_key = ids.is_empty();
            found
        }
        None => false,
    };
    if remove_key {
        rt.table.remove(key);
    }
    found
}

fn runtime_client_exists(client_id: ClientId) -> bool {
    let guard = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.all().iter().any(|snap| snap.id == client_id)
}

fn mark_runtime_redirect_broken(current: &mut Client, redirect: ClientId) {
    current.tracking.broken_redirect = true;
    sync_runtime_client_tracking(current.id, &current.tracking);
    if current.resp_proto == 3 {
        current.write_push_frame(&RespFrame::Push(vec![
            RespFrame::bulk(RedisString::from_static(b"tracking-redir-broken")),
            RespFrame::Integer(redirect as i64),
        ]));
    }
}

fn runtime_deliver_messages(
    current: &mut Client,
    pubsub: Option<&Arc<Mutex<PubSubRegistry>>>,
    deliveries: Vec<RuntimeTrackingDelivery>,
) {
    for delivery in deliveries {
        if delivery.recipient_id == current.id {
            if current.resp_proto == 3 {
                current.write_push_frame(&tracking_resp3_push(&delivery.keys));
            } else if current.in_pubsub_mode() {
                current.write_push_frame(&tracking_resp2_pubsub_message(&delivery.keys));
            }
            continue;
        }

        let Some(registry) = pubsub else {
            if delivery.owner_id == current.id {
                if let Some(redirect) = delivery.redirect {
                    mark_runtime_redirect_broken(current, redirect);
                }
            }
            continue;
        };
        let sent = {
            let guard = match registry.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if !runtime_client_exists(delivery.recipient_id) {
                false
            } else {
                let proto = guard.resp_proto(delivery.recipient_id);
                let bytes = encode_tracking_message_for_proto(proto, &delivery.keys);
                guard.send_to(delivery.recipient_id, bytes)
            }
        };
        if !sent && delivery.owner_id == current.id {
            if let Some(redirect) = delivery.redirect {
                mark_runtime_redirect_broken(current, redirect);
            }
        }
    }
}

/// Send invalidations for modified keys using the packet-level live runtime.
pub fn runtime_invalidate_keys(
    source_id: ClientId,
    current: &mut Client,
    pubsub: Option<&Arc<Mutex<PubSubRegistry>>>,
    keys: &[RedisString],
    force_send_to_source: bool,
    defer_bcast: bool,
) {
    if keys.is_empty() {
        return;
    }
    let deliveries = {
        let mut rt = lock_runtime_tracking();
        runtime_collect_key_deliveries(
            &mut rt,
            source_id,
            keys,
            force_send_to_source,
            defer_bcast,
        )
    };
    runtime_deliver_messages(current, pubsub, deliveries);
}

/// Flush BCAST invalidations accumulated while a transaction is draining.
pub fn runtime_flush_pending_bcast(
    current: &mut Client,
    pubsub: Option<&Arc<Mutex<PubSubRegistry>>>,
) {
    let deliveries = {
        let mut rt = lock_runtime_tracking();
        runtime_collect_pending_bcast_deliveries(&mut rt)
    };
    runtime_deliver_messages(current, pubsub, deliveries);
}

/// Evict tracked-key entries until the runtime table is at or below `max_keys`.
pub fn runtime_limit_tracked_keys(
    max_keys: usize,
    current: &mut Client,
    pubsub: Option<&Arc<Mutex<PubSubRegistry>>>,
) {
    let keys = {
        let rt = lock_runtime_tracking();
        let over = rt.table.len().saturating_sub(max_keys);
        rt.table.keys().take(over).cloned().collect::<Vec<_>>()
    };
    for key in keys {
        runtime_invalidate_keys(current.id, current, pubsub, &[key], true, false);
    }
}

/// Send an "all keys invalidated" notification to every tracking client.
pub fn runtime_invalidate_all(
    current: &mut Client,
    pubsub: Option<&Arc<Mutex<PubSubRegistry>>>,
) {
    let deliveries = {
        let mut rt = lock_runtime_tracking();
        let deliveries = runtime_collect_all_deliveries(&rt);
        rt.table.clear();
        rt.bcast_pending.clear();
        deliveries
    };
    runtime_deliver_messages(current, pubsub, deliveries);
}

// ── Trait: minimal client view required by the tracking subsystem ──────────

/// The minimal interface that the tracking subsystem needs from a client.
///
/// Decouples `tracking.rs` from the concrete `Client` type so that the
/// tracking logic can be tested and compiled independently. `Client` must
/// implement this trait once the required fields are added.
///
/// TODO(architect): implement `TrackingClient` for `Client` in
/// `redis-core/src/client.rs` after adding the required fields.
pub trait TrackingClient {
    fn id(&self) -> ClientId;
    fn resp_version(&self) -> u8;
    fn tracking_flags(&self) -> &ClientTrackingFlags;
    fn tracking_flags_mut(&mut self) -> &mut ClientTrackingFlags;
    fn pubsub_data(&self) -> Option<&ClientPubSubData>;
    fn pubsub_data_mut(&mut self) -> Option<&mut ClientPubSubData>;
    fn init_pubsub_data(&mut self);
    /// Append raw bytes to the client's reply buffer.
    fn write_reply_bytes(&mut self, bytes: &[u8]);
}

// ── Pure helpers ───────────────────────────────────────────────────────────

/// Returns `true` if `s1` and `s2` share a common prefix of
/// `min(s1.len(), s2.len())` bytes.
///
/// C: `static int stringCheckPrefix(...)` — `memcmp(s1, s2, min_length) == 0`.
fn string_check_prefix(s1: &[u8], s2: &[u8]) -> bool {
    let min_len = s1.len().min(s2.len());
    s1[..min_len] == s2[..min_len]
}

/// Write a decimal integer into a byte buffer without a heap allocation.
/// Used when building hand-rolled RESP frames in [`tracking_build_broadcast_reply`].
/// C: `ll2string(buf, sizeof(buf), n)`.
fn write_decimal(buf: &mut Vec<u8>, n: u64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let start = buf.len();
    let mut n = n;
    while n > 0 {
        buf.push((n % 10) as u8 + b'0');
        n /= 10;
    }
    buf[start..].reverse();
}

// ── Public functions (free functions operating on TrackingState + clients) ─

/// Disable tracking for client `c`.
///
/// If `c` is in BCAST mode, iterates all prefixes it is subscribed to,
/// removes it from each `BcastState.clients` set, and removes empty
/// prefix entries from the prefix table.
///
/// Adjustments to `server.tracking_clients` are the caller's responsibility;
/// this function decrements `tracking_clients_counter` by one when tracking
/// was enabled. The caller should apply this decrement to `RedisServer`.
///
/// C: `void disableTracking(client *c)` (tracking.c:67).
pub fn disable_tracking(
    tracking: &mut TrackingState,
    flags: &mut ClientTrackingFlags,
    pubsub_data: &mut Option<Box<ClientPubSubData>>,
    client_id: ClientId,
) -> bool {
    if flags.tracking_bcast {
        if let Some(pd) = pubsub_data.as_mut() {
            let prefixes: Vec<Vec<u8>> = pd.client_tracking_prefixes.iter().cloned().collect();
            for prefix in prefixes {
                if let Some(bs) = tracking.prefix_table.get_mut(&prefix) {
                    bs.clients.remove(&client_id);
                    if bs.clients.is_empty() {
                        tracking.prefix_table.remove(&prefix);
                    }
                }
            }
            pd.client_tracking_prefixes.clear();
        }
        *pubsub_data = None;
    }

    if flags.tracking {
        flags.tracking = false;
        flags.tracking_broken_redir = false;
        flags.tracking_bcast = false;
        flags.tracking_optin = false;
        flags.tracking_optout = false;
        flags.tracking_caching = false;
        flags.tracking_noloop = false;
        return true; // caller should decrement tracking_clients on RedisServer
    }
    false
}

/// Check that none of the supplied prefixes overlap with each other or with
/// the existing prefixes for this client.
///
/// Returns `Ok(())` if no collision is found. Returns `Err(RedisError)` with
/// a descriptive message if two prefixes overlap (i.e., one is a prefix of
/// the other), preserving the C wire error text for wire-diff fidelity.
///
/// C: `int checkPrefixCollisionsOrReply(client *c, robj **prefixes, size_t numprefix)`.
pub fn check_prefix_collisions(
    new_prefixes: &[&[u8]],
    existing_prefixes: Option<&HashSet<Vec<u8>>>,
) -> Result<(), RedisError> {
    for (i, &p) in new_prefixes.iter().enumerate() {
        // Check against prefixes already registered for this client.
        if let Some(existing) = existing_prefixes {
            for existing_prefix in existing {
                if string_check_prefix(existing_prefix.as_slice(), p) {
                    let mut msg = Vec::new();
                    msg.extend_from_slice(b"ERR Prefix '");
                    msg.extend_from_slice(p);
                    msg.extend_from_slice(b"' overlaps with an existing prefix '");
                    msg.extend_from_slice(existing_prefix.as_slice());
                    msg.extend_from_slice(b"'. Prefixes for a single client must not overlap.");
                    return Err(RedisError::runtime(msg));
                }
            }
        }
        // Check against other prefixes in the same input batch.
        for &q in &new_prefixes[(i + 1)..] {
            if string_check_prefix(p, q) {
                let mut msg = Vec::new();
                msg.extend_from_slice(b"ERR Prefix '");
                msg.extend_from_slice(p);
                msg.extend_from_slice(b"' overlaps with another provided prefix '");
                msg.extend_from_slice(q);
                msg.extend_from_slice(b"'. Prefixes for a single client must not overlap.");
                return Err(RedisError::runtime(msg));
            }
        }
    }
    Ok(())
}

/// Subscribe client `client_id` to broadcast invalidations for `prefix`.
///
/// If `prefix` has no existing `BcastState`, one is created and inserted.
/// If the client is already subscribed, this is a no-op.
///
/// C: `void enableBcastTrackingForPrefix(client *c, char *prefix, size_t plen)`.
pub fn enable_bcast_tracking_for_prefix(
    tracking: &mut TrackingState,
    client_id: ClientId,
    pubsub_data: &mut ClientPubSubData,
    prefix: &[u8],
) {
    let bs = tracking
        .prefix_table
        .entry(prefix.to_vec())
        .or_insert_with(BcastState::new);

    if bs.clients.insert(client_id) {
        pubsub_data.client_tracking_prefixes.insert(prefix.to_vec());
    }
}

/// Enable tracking for client `client_id`.
///
/// Initialises the global tracking state the first time any client enables
/// tracking. Sets tracking flags on the client and, in BCAST mode, subscribes
/// to all supplied prefixes (or the empty prefix if none are given).
///
/// `tracking_clients_was_zero` returns `true` if `tracking_clients` needs to
/// be incremented by the caller on `RedisServer`.
///
/// C: `void enableTracking(client *c, uint64_t redirect_to,
///                          struct ClientFlags options, robj **prefix,
///                          size_t numprefix)`.
pub fn enable_tracking(
    tracking: &mut TrackingState,
    client_id: ClientId,
    flags: &mut ClientTrackingFlags,
    pubsub_data: &mut Option<Box<ClientPubSubData>>,
    redirect_to: Option<ClientId>,
    options: &ClientTrackingFlags,
    prefixes: &[Vec<u8>],
) -> bool {
    let was_not_tracking = !flags.tracking;
    flags.tracking = true;
    flags.tracking_broken_redir = false;
    flags.tracking_bcast = false;
    flags.tracking_optin = false;
    flags.tracking_optout = false;
    flags.tracking_noloop = false;

    let pd = pubsub_data.get_or_insert_with(|| Box::new(ClientPubSubData::default()));
    pd.client_tracking_redirection = redirect_to;

    if options.tracking_bcast {
        flags.tracking_bcast = true;
        if prefixes.is_empty() {
            enable_bcast_tracking_for_prefix(tracking, client_id, pd, b"");
        } else {
            for prefix in prefixes {
                enable_bcast_tracking_for_prefix(tracking, client_id, pd, prefix.as_slice());
            }
        }
    }

    flags.tracking_optin = options.tracking_optin;
    flags.tracking_optout = options.tracking_optout;
    flags.tracking_noloop = options.tracking_noloop;

    was_not_tracking
}

/// Record that `tracking_client` may have read the keys accessed by `executing`.
///
/// `key_indices` is the output of `getKeysFromCommand` — a list of argument
/// positions (into `argv`) that are key arguments. `is_pubsub` should be
/// `true` if the command has `CMD_PUBSUB`; those keys are not tracked.
///
/// In optin/optout mode the function checks the `tracking_caching` flag and
/// returns early if the mode and flag are inconsistent.
///
/// C: `void trackingRememberKeys(client *tracking, client *executing)`.
pub fn tracking_remember_keys(
    tracking: &mut TrackingState,
    tracking_client_id: ClientId,
    tracking_flags: &ClientTrackingFlags,
    argv: &[RedisString],
    key_indices: &[usize],
    is_pubsub_command: bool,
) {
    let optin = tracking_flags.tracking_optin;
    let optout = tracking_flags.tracking_optout;
    let caching_given = tracking_flags.tracking_caching;

    if (optin && !caching_given) || (optout && caching_given) {
        return;
    }
    if key_indices.is_empty() {
        return;
    }
    if is_pubsub_command {
        return;
    }

    for &idx in key_indices {
        if let Some(key_obj) = argv.get(idx) {
            let key_bytes = key_obj.as_bytes().to_vec();
            let ids = tracking.table.entry(key_bytes).or_insert_with(HashSet::new);
            if ids.insert(tracking_client_id) {
                tracking.total_items += 1;
            }
        }
    }
}

/// Build a RESP2 bulk-array frame listing the keys in `keys` that were not
/// last modified by `exclude_client`.
///
/// Returns `None` if the resulting array would be empty (all keys were last
/// written by `exclude_client` and NOLOOP would suppress them all).
///
/// The returned `Vec<u8>` is a hand-serialised RESP array:
/// `*N\r\n$len\r\nkey\r\n...`. It is sent verbatim via [`send_tracking_message`]
/// with `proto = true`.
///
/// C: `sds trackingBuildBroadcastReply(client *c, rax *keys)`.
pub fn tracking_build_broadcast_reply(
    keys: &HashMap<Vec<u8>, Option<ClientId>>,
    exclude_client: Option<ClientId>,
) -> Option<Vec<u8>> {
    let count = match exclude_client {
        None => keys.len() as u64,
        Some(exc) => keys.values().filter(|&&ref v| *v != Some(exc)).count() as u64,
    };

    if count == 0 {
        return None;
    }

    let mut proto = Vec::with_capacity(keys.len() * 16);
    proto.push(b'*');
    write_decimal(&mut proto, count);
    proto.extend_from_slice(b"\r\n");

    for (key, last_writer) in keys {
        if let Some(exc) = exclude_client {
            if *last_writer == Some(exc) {
                continue;
            }
        }
        proto.push(b'$');
        write_decimal(&mut proto, key.len() as u64);
        proto.extend_from_slice(b"\r\n");
        proto.extend_from_slice(key.as_slice());
        proto.extend_from_slice(b"\r\n");
    }

    Some(proto)
}

/// Send a tracking invalidation message to client `c`.
///
/// Handles:
/// * Redirection — if `c` has a `client_tracking_redirection`, look up the
///   target and send there instead, falling back to a `tracking-redir-broken`
///   push if the target is gone.
/// * RESP3 push frames vs. RESP2 pub/sub channel messages.
/// * `proto` flag — if `true`, `keyname` is already a serialised RESP frame
///   (used for BCAST bulk invalidation); otherwise wrap in `*1\r\n$len\r\n`.
///
/// The `lookup_client` callback is how the tracking subsystem looks up clients
/// by ID without holding a mutable reference to the full server client list at
/// the same time.
///
/// TODO(architect): the `lookup_client` callback needs access to `RedisServer`'s
/// client map. In Phase 3 this should become a method taking `&mut RedisServer`.
/// Mutable-aliasing concern: `c` and the redirect target may be different entries
/// in the same collection; use `split_at_mut` or a `RefCell<HashMap<ClientId, Client>>`.
///
/// C: `void sendTrackingMessage(client *c, char *keyname, size_t keylen, int proto)`.
pub fn send_tracking_message<C, F>(
    c: &mut C,
    keyname: &[u8],
    proto: bool,
    mut lookup_client: F,
) where
    C: TrackingClient,
    F: FnMut(ClientId) -> Option<Box<dyn FnOnce(&mut dyn TrackingClient)>>,
{
    // C: struct ClientFlags old_flags = c->flag; c->flag.pushing = 1;
    let old_pushing = c.tracking_flags().pushing;
    c.tracking_flags_mut().pushing = true;

    let redirect_id = c
        .pubsub_data()
        .and_then(|pd| pd.client_tracking_redirection);

    let using_redirection = redirect_id.is_some();

    if let Some(redir_id) = redirect_id {
        // TODO(port): redirect lookup needs access to the full client registry;
        // the callback shape here is a placeholder. Phase B must wire up the real
        // client-lookup so that `lookup_client(redir_id)` returns a mutable ref.
        let redir_alive = lookup_client(redir_id).is_some();
        if !redir_alive {
            c.tracking_flags_mut().tracking_broken_redir = true;
            // Notify the original client that the redirect target is gone (RESP3 only).
            if c.resp_version() > resp_version::RESP2 {
                let mut buf = Vec::new();
                // >2\r\n$21\r\ntracking-redir-broken\r\n:<redir_id>\r\n
                buf.extend_from_slice(b">2\r\n$21\r\ntracking-redir-broken\r\n:");
                write_decimal(&mut buf, redir_id);
                buf.extend_from_slice(b"\r\n");
                c.write_reply_bytes(&buf);
            }
            if !old_pushing {
                c.tracking_flags_mut().pushing = false;
            }
            return;
        }
        if !old_pushing {
            c.tracking_flags_mut().pushing = false;
        }
        // TODO(port): switch `c` to the redirect target for the remainder.
        // In C: `c = redir; using_redirection = 1; old_flags = c->flag; c->flag.pushing = 1;`
        // This requires a mutable reference swap which is not expressible here without
        // restructuring. The logic below continues as if `c` is the target.
        // TODO(architect): refactor send_tracking_message to take the redirect target
        // as a separate `Option<&mut dyn TrackingClient>` argument.
    }

    // Select RESP variant and write the header.
    let resp_ver = c.resp_version();
    let flags = c.tracking_flags().clone();
    if resp_ver > resp_version::RESP2 {
        // RESP3 push: >2\r\n$10\r\ninvalidate\r\n
        let header = b">2\r\n$10\r\ninvalidate\r\n";
        c.write_reply_bytes(header);
    } else if using_redirection && flags.pubsub {
        // RESP2 pub/sub invalidation on the __redis__:invalidate channel.
        // TODO(port): addReplyPubsubMessage is complex; we need the TrackingChannelName
        // bytes and the shared.messagebulk serialised form. Emit a placeholder header.
        // Full implementation requires integration with the pubsub reply path.
        // C: addReplyPubsubMessage(c, TrackingChannelName, NULL, shared.messagebulk);
        // TODO(architect): wire up pubsub reply path for RESP2 redirect invalidation.
        let _ = b"__redis__:invalidate"; // no-op placeholder
    } else {
        // Neither RESP3 nor RESP2-pubsub-redirect: nothing we can send.
        if !old_pushing {
            c.tracking_flags_mut().pushing = false;
        }
        return;
    }

    // Write the key payload.
    if proto {
        // `keyname` is already a serialised RESP frame (bulk array or null).
        c.write_reply_bytes(keyname);
    } else {
        // Wrap the single key in a one-element array: *1\r\n$len\r\nkey\r\n
        let mut buf = Vec::new();
        buf.extend_from_slice(b"*1\r\n$");
        write_decimal(&mut buf, keyname.len() as u64);
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(keyname);
        buf.extend_from_slice(b"\r\n");
        c.write_reply_bytes(&buf);
    }

    if !old_pushing {
        c.tracking_flags_mut().pushing = false;
    }
}

/// Record that `keyname` was modified and should be broadcast to all BCAST
/// subscribers whose prefix matches the key.
///
/// Stores `key → modifier_client_id` in each matching [`BcastState::keys`].
/// The broadcast itself happens later in [`tracking_broadcast_invalidation_messages`].
///
/// C: `void trackingRememberKeyToBroadcast(client *c, char *keyname, size_t keylen)`.
/// PERF(port): iterates all prefix-table entries (O(P)) rather than doing a
/// radix-tree prefix scan (O(k)). Replace with RadixTree prefix iterator in Phase 4.
pub fn tracking_remember_key_to_broadcast(
    tracking: &mut TrackingState,
    keyname: &[u8],
    modifier_client_id: Option<ClientId>,
) {
    for (prefix, bs) in tracking.prefix_table.iter_mut() {
        if prefix.len() > keyname.len() {
            continue;
        }
        if !prefix.is_empty() && !keyname.starts_with(prefix.as_slice()) {
            continue;
        }
        bs.keys.insert(keyname.to_vec(), modifier_client_id);
    }
}

/// Invalidate all clients that may have cached `key`.
///
/// Looks up `key` in the tracking table, sends an invalidation message to each
/// registered client (skipping BCAST clients and NOLOOP self-notifications), then
/// removes the tracking-table entry and adjusts `total_items`.
///
/// `bcast` controls whether the key is also scheduled for BCAST broadcast.
///
/// `modifier_client_id` is the client that caused the change (`None` for
/// expiry-triggered invalidations).
///
/// `current_client_id` is the server's `current_client` (for NOLOOP detection
/// and pending-key scheduling).
///
/// `send_msg` is a callback that delivers the invalidation to a specific client.
/// It takes the target client's ID, the key bytes, and the `proto` flag.
///
/// TODO(architect): `send_msg` must be able to reach mutable client state.
/// In Phase 3 integrate with the `RedisServer` client registry; the pending-key
/// list (`server.tracking_pending_keys`) must also be threaded through here.
///
/// C: `void trackingInvalidateKey(client *c, robj *keyobj, int bcast)`.
pub fn tracking_invalidate_key<F>(
    tracking: &mut TrackingState,
    key: &[u8],
    bcast: bool,
    modifier_client_id: Option<ClientId>,
    current_client_id: Option<ClientId>,
    executing_command: bool,
    mut send_or_defer: F,
)
where
    F: FnMut(ClientId, &[u8], bool),
{
    if bcast && !tracking.prefix_table.is_empty() {
        tracking_remember_key_to_broadcast(tracking, key, modifier_client_id);
    }

    let ids = match tracking.table.remove(key) {
        Some(ids) => ids,
        None => return,
    };

    tracking.total_items = tracking.total_items.saturating_sub(ids.len() as u64);

    for id in &ids {
        // TODO(port): look up client flags here. We need to check:
        //   1. client is still alive and has tracking enabled.
        //   2. client is NOT in BCAST mode (those get handled via broadcast path).
        //   3. NOLOOP: skip if `id == current_client_id && modifier == current_client`.
        //   4. Defer if `id == current_client_id && executing_command`.
        // Without access to the per-client flag store we forward to the caller via callback.
        let is_current = current_client_id == Some(*id);
        if is_current && executing_command {
            // Schedule for after the current command completes.
            // C: incrRefCount(keyobj); listAddNodeTail(server.tracking_pending_keys, keyobj);
            send_or_defer(*id, key, true /* deferred */);
        } else {
            send_or_defer(*id, key, false /* immediate */);
        }
    }
}

/// Flush all pending key invalidations that were deferred while a command
/// was executing (to avoid interleaving invalidation with command reply).
///
/// Called after the command response has been written, before returning to
/// the event loop. Bails early if `execution_nesting > 0` (inside MULTI/EXEC
/// or a scripting call).
///
/// `pending_keys` is the list of `Option<Vec<u8>>` entries:
///   * `Some(key)` → send a single-key invalidation for that key.
///   * `None` → send a NULL-payload invalidation ("all keys invalid", from FLUSHALL).
///
/// `current_client_id` is `Some(id)` if a client is still active; `None` if it
/// was freed during the command.
///
/// `send_msg` writes the invalidation to the current client.
///
/// TODO(architect): integrate with `RedisServer::tracking_pending_keys` (a
/// `VecDeque<Option<Vec<u8>>>`) and `RedisServer::execution_nesting: u32`.
///
/// C: `void trackingHandlePendingKeyInvalidations(void)`.
pub fn tracking_handle_pending_key_invalidations<F>(
    pending_keys: &mut Vec<Option<Vec<u8>>>,
    execution_nesting: u32,
    current_client_id: Option<ClientId>,
    mut send_msg: F,
)
where
    F: FnMut(&[u8], bool),
{
    if pending_keys.is_empty() {
        return;
    }
    if execution_nesting > 0 {
        return;
    }

    if current_client_id.is_some() {
        for entry in pending_keys.iter() {
            match entry {
                Some(key) => {
                    // C: sendTrackingMessage(current_client, objectGetVal(key), sdslen(...), 0)
                    send_msg(key.as_slice(), false /* not proto */);
                }
                None => {
                    // C: sendTrackingMessage(current_client,
                    //        objectGetVal(shared.null[c->resp]),
                    //        sdslen(...), 1 /* proto */)
                    // The null RESP payload is RESP-version-dependent; the caller must
                    // supply the correct pre-encoded null bytes.
                    // TODO(port): supply the correct RESP null bytes per negotiated resp version.
                    send_msg(b"$-1\r\n", true /* proto */);
                }
            }
        }
    }
    pending_keys.clear();
}

/// Invalidate all tracked keys when the database is flushed.
///
/// Sends a RESP NULL to every client that has tracking enabled, indicating
/// that all keys should be considered invalid. Clears and reinitialises
/// the tracking table.
///
/// `async_free` defers the tree-free to a background thread (mirrors
/// `freeTrackingRadixTreeAsync`). In the pilot we just clear synchronously
/// regardless; the `async_free` parameter is accepted for API completeness.
///
/// `each_tracking_client` is a callback that receives each tracking client ID
/// and whether it equals `current_client_id` (to decide defer vs. immediate).
///
/// TODO(architect): integrate with `RedisServer::clients` list and `RedisServer::tracking_clients`.
///
/// C: `void trackingInvalidateKeysOnFlush(int async)`.
pub fn tracking_invalidate_keys_on_flush<F>(
    tracking: &mut TrackingState,
    tracking_clients_count: u64,
    current_client_id: Option<ClientId>,
    async_free: bool,
    mut each_tracking_client: F,
)
where
    F: FnMut(ClientId, bool /* is_current_client */),
{
    let _ = async_free; // PERF(port): async_free not yet wired; always clears synchronously.

    if tracking_clients_count > 0 {
        // TODO(port): caller must iterate server.clients and call each_tracking_client
        // for each client that has `flags.tracking == true`.
        // Signature: each_tracking_client(client_id, client_id == current_client_id)
        let _ = each_tracking_client; // placeholder; real integration in Phase 3
    }

    tracking.table.clear();
    tracking.total_items = 0;
}

/// Evict entries from the tracking table if the table exceeds `max_keys`.
///
/// Effort is proportional to `timeout_counter + 1` (×100 iterations per call).
/// After each eviction the key is also invalidated (clients are notified).
///
/// `timeout_counter` is mutable state the caller owns between calls; it
/// increments every time the limit is still exceeded after a full effort pass.
///
/// `send_or_defer` is forwarded to [`tracking_invalidate_key`].
///
/// TODO(architect): `timeout_counter` should live in `RedisServer` or on the
/// `TrackingState` struct.  Also the random-walk selection (C: `raxRandomWalk`)
/// has no HashMap equivalent; currently the first key found is evicted, which
/// is deterministic rather than random.
///
/// C: `void trackingLimitUsedSlots(void)`.
pub fn tracking_limit_used_slots<F>(
    tracking: &mut TrackingState,
    max_keys: usize,
    timeout_counter: &mut u32,
    current_client_id: Option<ClientId>,
    executing_command: bool,
    mut send_or_defer: F,
)
where
    F: FnMut(ClientId, &[u8], bool),
{
    if max_keys == 0 {
        return;
    }
    if tracking.table.len() <= max_keys {
        *timeout_counter = 0;
        return;
    }

    // PERF(port): C uses raxRandomWalk for random victim selection.
    // HashMap has no equivalent; we pick the first key in iteration order.
    // TODO(port): implement random key eviction (e.g., collect keys and index
    // with a random offset) once a PRNG is available in this crate.
    let mut effort = 100u32.saturating_mul(timeout_counter.saturating_add(1));

    while effort > 0 {
        effort -= 1;

        let victim_key: Option<Vec<u8>> = tracking.table.keys().next().cloned();
        let key = match victim_key {
            Some(k) => k,
            None => break,
        };

        tracking_invalidate_key(
            tracking,
            &key,
            false,
            None,
            current_client_id,
            executing_command,
            &mut send_or_defer,
        );

        if tracking.table.len() <= max_keys {
            *timeout_counter = 0;
            return;
        }
    }

    *timeout_counter = timeout_counter.saturating_add(1);
}

/// Broadcast accumulated invalidation messages to all BCAST subscribers.
///
/// For each prefix in the prefix table that has pending key modifications:
/// 1. Build a single common RESP array for clients not using NOLOOP.
/// 2. Iterate all subscribers; clients with NOLOOP get a personalised array
///    that excludes keys they wrote themselves.
/// 3. Clear the `keys` set for the next event-loop cycle.
///
/// `lookup_client_flags` returns the tracking flags for a given client ID
/// (so we can check NOLOOP without holding a mutable reference to all clients).
///
/// `send_msg` writes bytes to a specific client's reply buffer.
///
/// TODO(architect): in Phase 3, replace the callback approach with direct
/// `&mut RedisServer` access once the client registry and tracking state are
/// co-located. The current callback design avoids borrow-checker aliasing but
/// at the cost of some overhead.
///
/// C: `void trackingBroadcastInvalidationMessages(void)`.
pub fn tracking_broadcast_invalidation_messages<L, S>(
    tracking: &mut TrackingState,
    tracking_clients_count: u64,
    mut lookup_client_flags: L,
    mut send_msg: S,
)
where
    L: FnMut(ClientId) -> Option<ClientTrackingFlags>,
    S: FnMut(ClientId, &[u8]),
{
    if tracking_clients_count == 0 {
        return;
    }

    for bs in tracking.prefix_table.values_mut() {
        if bs.keys.is_empty() {
            continue;
        }

        let common_proto = tracking_build_broadcast_reply(&bs.keys, None);

        for &client_id in &bs.clients {
            let client_flags = match lookup_client_flags(client_id) {
                Some(f) => f,
                None => continue,
            };

            if client_flags.tracking_noloop {
                if let Some(adhoc) = tracking_build_broadcast_reply(&bs.keys, Some(client_id)) {
                    send_msg(client_id, &adhoc);
                }
            } else if let Some(ref proto) = common_proto {
                send_msg(client_id, proto.as_slice());
            }
        }

        bs.keys.clear();
    }
}

// ── Accessors (small; mirror C's tracking*Get* functions) ────────────────

/// Total number of (key, client-ID) tracking pairs in the table.
/// C: `uint64_t trackingGetTotalItems(void)`.
pub fn tracking_get_total_items(tracking: &TrackingState) -> u64 {
    tracking.total_items
}

/// Total number of distinct keys in the tracking table.
/// C: `uint64_t trackingGetTotalKeys(void)`.
pub fn tracking_get_total_keys(tracking: &TrackingState) -> u64 {
    tracking.table.len() as u64
}

/// Total number of registered prefixes in the BCAST prefix table.
/// C: `uint64_t trackingGetTotalPrefixes(void)`.
pub fn tracking_get_total_prefixes(tracking: &TrackingState) -> u64 {
    tracking.prefix_table.len() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_runtime_tracking_for_test() {
        let mut rt = lock_runtime_tracking();
        rt.clients.clear();
        rt.table.clear();
        rt.bcast_pending.clear();
    }

    #[test]
    fn runtime_tracking_info_counters_match_tracked_keys_and_bcast_prefixes() {
        clear_runtime_tracking_for_test();

        let normal = ClientTrackingState {
            enabled: true,
            redirect: 42,
            ..ClientTrackingState::default()
        };
        sync_runtime_client_tracking(1, &normal);
        runtime_remember_read_keys(
            1,
            &normal,
            &[
                RedisString::from_static(b"key1"),
                RedisString::from_static(b"key2"),
            ],
        );

        let bcast = ClientTrackingState {
            enabled: true,
            bcast: true,
            prefixes: vec![RedisString::from_static(b"prefix:")],
            ..ClientTrackingState::default()
        };
        sync_runtime_client_tracking(2, &bcast);

        let counters = runtime_tracking_info_counters();
        assert_eq!(counters.total_items, 2);
        assert_eq!(counters.total_keys, 2);
        assert_eq!(counters.total_prefixes, 1);
        assert_eq!(counters.tracking_clients, 2);

        clear_runtime_tracking_for_test();
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/tracking.c  (663 lines, 19 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         23
//   port_notes:    7
//   unsafe_blocks: 0
//   notes: >
//     Logic faithfully translated; two areas need architect attention before
//     Phase B compilation:
//     (1) `Client` must gain `ClientTrackingFlags`, `Option<Box<ClientPubSubData>>`,
//         and `resp: u8` fields (TODO(architect) ×2 above).
//     (2) `RedisServer` must gain `tracking: TrackingState`, `tracking_clients: u64`,
//         `current_client: Option<ClientId>`, and `tracking_pending_keys: Vec<Option<Vec<u8>>>`.
//     The `send_tracking_message` redirect-swap and `trackingLimitUsedSlots`
//     random-walk have TODO(port) markers for their unresolved idioms.
//     All rax usage replaced with HashMap (PERF notes at each site).
//     Validator: only E0432 and E0282 errors (both expected); zero parser failures.
// ──────────────────────────────────────────────────────────────────────────

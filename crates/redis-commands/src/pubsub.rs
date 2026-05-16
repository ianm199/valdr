//! Pub/Sub command implementations: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE,
//! PUNSUBSCRIBE, PUBLISH, PUBSUB, SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH.
//!
//! C source: `reference/valkey/src/pubsub.c` (797 lines, 30 functions)
//! Crate: `redis-commands` (later phase)
//!
//! All Redis data — channel names, patterns, messages — uses `RedisString` /
//! `&[u8]`. `String` / `&str` / `from_utf8` are banned for stored Redis data
//! per PORTING.md §1.
//!
//! The C code uses a `pubsubtype` struct with function pointers to unify
//! global-channel and shard-channel code paths. This port replaces that with
//! a `PubSubKind` enum whose variants dispatch to the appropriate client/server
//! fields. PORT NOTE: this restructuring is intentional — Rust enums express
//! the two-variant dispatch more clearly than C function-pointer structs.
//!
//! ## Architect items
//!
//! TODO(architect): `ClientPubSubData` logically belongs in
//! `crates/redis-core/src/client.rs` alongside the `Client` type. It is placed
//! here temporarily for Phase A; move it in Phase 3 and add
//! `redis-commands → redis-core` dep edge if not already present.
//!
//! TODO(architect): Server-level pub/sub maps (`pubsub_channels`,
//! `pubsub_patterns`, `pubsubshard_channels`) need to be fields on `RedisServer`
//! and accessible via `CommandContext`. Blocked on Phase 3 server-state design.
//!
//! TODO(architect): `pubsub_publish_message_internal` broadcasts to multiple
//! subscriber `Client` objects simultaneously. Rust ownership forbids multiple
//! `&mut Client` at once. Resolve with either an arena, `Arc<Mutex<Client>>`,
//! or channel-based message-passing (mpsc/broadcast). Decision deferred to
//! Phase 3 pub/sub architecture.
//!
//! TODO(architect): `string_match_len` (glob pattern matching, used by
//! PUBSUB CHANNELS and publish pattern dispatch) is in `util.c` →
//! `redis_core::util`. Add dep edge `redis-commands → redis-core` if absent.
//!
//! TODO(architect): `cluster_propagate_publish` and `cluster_slot_stats_*`
//! functions — cluster integration; deferred to Phase 4.
//!
//! TODO(architect): `sentinel_publish_command` — sentinel mode; deferred.
//!
//! TODO(architect): `disable_tracking` (client-tracking subsystem) — deferred
//! to Phase 5.
//!
//! TODO(architect): `force_command_propagation` — replication propagation hook;
//! deferred to Phase 3+ replication layer.
//!
//! TODO(architect): `update_client_mem_usage_and_bucket` — memory-usage
//! tracking per client; deferred.
//!
//! TODO(architect): kvstore (cluster-slot-aware hash-table) used for
//! `server.pubsub_channels` and `server.pubsubshard_channels` is mapped to
//! `HashMap<u32, HashMap<RedisString, HashSet<u64>>>` (slot → channel →
//! client-ids) for Phase A. Replace with the real `KvStore` type once
//! `redis-ds::kvstore` is in pilot.
//!
//! TODO(architect): `CommandContext` needs a `client_id() -> u64` accessor and
//! a way to look up a `&mut Client` by ID for the broadcast publish path.

use std::collections::{HashMap, HashSet};

use redis_core::command_context::CommandContext;
use redis_types::{RedisError, RedisString};

// ─────────────────────────────────────────────────────────────────────────────
// PubSubKind  —  replaces C `pubsubtype` struct with function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Distinguishes global pub/sub channels from shard-level (cluster) channels.
///
/// PORT NOTE: The C `pubsubtype` struct carries function pointers
/// (`clientPubSubChannels`, `subscriptionCount`, `serverPubSubChannels`,
/// `subscribeMsg`, `unsubscribeMsg`, `messageBulk`). Rust replaces that with
/// this two-variant enum; all former function-pointer dispatch becomes `match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubKind {
    /// Global pub/sub (SUBSCRIBE / UNSUBSCRIBE / PUBLISH).
    Global,
    /// Shard-level pub/sub (SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH).
    Shard,
}

// ─────────────────────────────────────────────────────────────────────────────
// ClientPubSubData
// ─────────────────────────────────────────────────────────────────────────────

/// Per-client pub/sub bookkeeping.
///
/// C: `ClientPubSubData` struct embedded in `client` via `client.pubsub_data`
/// pointer (server.h). Heap-allocated on first subscribe and freed on
/// unsubscribe-all / client-close.
///
/// TODO(architect): Move to `crates/redis-core/src/client.rs` in Phase 3.
pub struct ClientPubSubData {
    /// Global channels this client is subscribed to.
    /// C: `hashtable *pubsub_channels`
    /// PERF(port): C uses a custom hashtable; `HashSet` is adequate for Phase A.
    pub pubsub_channels: HashSet<RedisString>,

    /// Glob patterns this client is subscribed to.
    /// C: `hashtable *pubsub_patterns`
    pub pubsub_patterns: HashSet<RedisString>,

    /// Shard-level channels this client is subscribed to.
    /// C: `hashtable *pubsubshard_channels`
    pub pubsubshard_channels: HashSet<RedisString>,

    /// Client tracking redirection target ID (0 = none).
    /// C: `client_tracking_redirection`
    pub client_tracking_redirection: i64,

    /// Client tracking key-prefix filters. `None` when tracking is not active.
    /// C: `rax *client_tracking_prefixes`
    /// TODO(architect): replace `Vec<RedisString>` with `RadixTree` when
    /// `redis-ds::rax` enters the pilot.
    pub client_tracking_prefixes: Option<Vec<RedisString>>,
}

impl ClientPubSubData {
    /// Allocate a fresh, empty pub/sub data block.
    /// C: `initClientPubSubData` allocation portion.
    pub fn new() -> Self {
        ClientPubSubData {
            pubsub_channels: HashSet::new(),
            pubsub_patterns: HashSet::new(),
            pubsubshard_channels: HashSet::new(),
            client_tracking_redirection: 0,
            client_tracking_prefixes: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Server-side pub/sub state (Phase A placeholder types)
// ─────────────────────────────────────────────────────────────────────────────

/// Client identifier (Phase A placeholder).
/// TODO(architect): replace with the canonical `ClientId` newtype once defined
/// in `redis-core`.
pub type ClientId = u64;

/// Server-side mapping: slot → (channel → set of subscriber client IDs).
///
/// For non-cluster mode the outer map always has a single entry at slot 0.
/// C: `kvstore *pubsub_channels` / `kvstore *pubsubshard_channels`.
///
/// TODO(architect): replace with the real `KvStore` type from `redis-ds`.
pub type ServerChannelMap = HashMap<u32, HashMap<RedisString, HashSet<ClientId>>>;

/// Server-side mapping: pattern → set of subscriber client IDs.
/// C: `dict *pubsub_patterns` in `redisServer`.
pub type ServerPatternMap = HashMap<RedisString, HashSet<ClientId>>;

// ─────────────────────────────────────────────────────────────────────────────
// Subscription count helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Return the number of global channels + patterns a client is subscribed to.
/// C: `clientSubscriptionsCount(client *c)`
pub fn client_subscriptions_count(data: &ClientPubSubData) -> i32 {
    (data.pubsub_channels.len() + data.pubsub_patterns.len()) as i32
}

/// Return the number of shard-level channels a client is subscribed to.
/// C: `clientShardSubscriptionsCount(client *c)`
pub fn client_shard_subscriptions_count(data: &ClientPubSubData) -> i32 {
    data.pubsubshard_channels.len() as i32
}

/// Return the total pub/sub subscription count across all three tables.
/// C: `clientTotalPubSubSubscriptionCount(client *c)`
pub fn client_total_pub_sub_subscription_count(data: &ClientPubSubData) -> i32 {
    client_subscriptions_count(data) + client_shard_subscriptions_count(data)
}

/// Return the number of global channels + patterns tracked server-wide.
/// C: `serverPubsubSubscriptionCount(void)`
///
/// TODO(architect): needs access to `&RedisServer`; called through
/// `CommandContext::server()` in Phase 3.
pub fn server_pubsub_subscription_count(
    channels: &ServerChannelMap,
    patterns: &ServerPatternMap,
) -> i32 {
    let channel_total: usize = channels.values().map(|slot| slot.len()).sum();
    (channel_total + patterns.len()) as i32
}

/// Return the number of shard-level channels tracked server-wide.
/// C: `serverPubsubShardSubscriptionCount(void)`
pub fn server_pubsub_shard_subscription_count(shard_channels: &ServerChannelMap) -> i32 {
    let total: usize = shard_channels.values().map(|slot| slot.len()).sum();
    total as i32
}

/// Return global + shard channel totals (including patterns) for INFO output.
/// C: `pubsubTotalSubscriptions(void)`
pub fn pubsub_total_subscriptions(
    channels: &ServerChannelMap,
    patterns: &ServerPatternMap,
    shard_channels: &ServerChannelMap,
) -> i32 {
    server_pubsub_subscription_count(channels, patterns)
        + server_pubsub_shard_subscription_count(shard_channels)
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory overhead
// ─────────────────────────────────────────────────────────────────────────────

/// Approximate memory overhead for a client's pub/sub bookkeeping.
/// C: `pubsubMemOverhead(client *c)`
///
/// PERF(port): C uses `hashtableMemUsage`; Rust's `HashSet` has no direct
/// memory-query API. We return an approximation via `capacity * entry_size`.
pub fn pubsub_mem_overhead(data: Option<&ClientPubSubData>) -> usize {
    let Some(data) = data else { return 0 };
    let pattern_mem = data.pubsub_patterns.capacity() * std::mem::size_of::<RedisString>();
    let channel_mem = data.pubsub_channels.capacity() * std::mem::size_of::<RedisString>();
    let shard_mem = data.pubsubshard_channels.capacity() * std::mem::size_of::<RedisString>();
    pattern_mem + channel_mem + shard_mem
}

// ─────────────────────────────────────────────────────────────────────────────
// Pubsub client reply helpers
//
// In C these write directly to `client.reply_buf` via `addReply*`.
// In Rust they call methods on `CommandContext` (which owns the reply writer).
//
// For the *broadcast* publish path (pubsub_publish_message_internal) they must
// write to arbitrary subscriber clients — not just the current client bundled
// in CommandContext. That multi-client mutation is deferred to Phase 3; each
// function below is annotated with a TODO(architect) at the broadcast call site.
// ─────────────────────────────────────────────────────────────────────────────

/// Send a `message` push frame to the current client.
///
/// When `msg` is `None` the caller is responsible for writing the message
/// payload immediately after (used for special-case array construction).
///
/// C: `addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk)`
/// C: pubsub.c:108-119
pub fn add_reply_pubsub_message(
    ctx: &mut CommandContext,
    channel: &RedisString,
    msg: Option<&RedisString>,
    message_bulk: &[u8],
) -> Result<(), RedisError> {
    // C: saves pushing flag, sets it to 1, restores on exit
    // TODO(architect): CommandContext needs a `set_pushing_flag(bool) -> bool`
    // round-trip so the old value can be restored. Sketched inline below.
    ctx.reply_push_or_array_header(3)?;
    ctx.reply_bulk(message_bulk)?;
    ctx.reply_bulk(channel.as_slice())?;
    if let Some(m) = msg {
        ctx.reply_bulk(m.as_slice())?;
    }
    Ok(())
}

/// Send a `pmessage` push frame (pattern-matched message) to the current client.
///
/// C: `addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg)`
/// C: pubsub.c:124-136
pub fn add_reply_pubsub_pat_message(
    ctx: &mut CommandContext,
    pattern: &RedisString,
    channel: &RedisString,
    msg: &RedisString,
) -> Result<(), RedisError> {
    ctx.reply_push_or_array_header(4)?;
    ctx.reply_bulk(b"pmessage")?;
    ctx.reply_bulk(pattern.as_slice())?;
    ctx.reply_bulk(channel.as_slice())?;
    ctx.reply_bulk(msg.as_slice())?;
    Ok(())
}

/// Send a `subscribe` / `ssubscribe` confirmation frame to the current client.
///
/// C: `addReplyPubsubSubscribed(client *c, robj *channel, pubsubtype type)`
/// C: pubsub.c:139-150
pub fn add_reply_pubsub_subscribed(
    ctx: &mut CommandContext,
    channel: &RedisString,
    kind: PubSubKind,
    subscription_count: i32,
) -> Result<(), RedisError> {
    ctx.reply_push_or_array_header(3)?;
    let subscribe_msg: &[u8] = match kind {
        PubSubKind::Global => b"subscribe",
        PubSubKind::Shard => b"ssubscribe",
    };
    ctx.reply_bulk(subscribe_msg)?;
    ctx.reply_bulk(channel.as_slice())?;
    ctx.reply_integer(subscription_count as i64)
}

/// Send an `unsubscribe` / `sunsubscribe` confirmation frame.
///
/// When `channel` is `None` the client had no subscriptions; the frame still
/// carries a `nil` channel slot (matches C behaviour).
///
/// C: `addReplyPubsubUnsubscribed(client *c, robj *channel, pubsubtype type)`
/// C: pubsub.c:156-170
pub fn add_reply_pubsub_unsubscribed(
    ctx: &mut CommandContext,
    channel: Option<&RedisString>,
    kind: PubSubKind,
    subscription_count: i32,
) -> Result<(), RedisError> {
    ctx.reply_push_or_array_header(3)?;
    let unsub_msg: &[u8] = match kind {
        PubSubKind::Global => b"unsubscribe",
        PubSubKind::Shard => b"sunsubscribe",
    };
    ctx.reply_bulk(unsub_msg)?;
    match channel {
        Some(ch) => ctx.reply_bulk(ch.as_slice())?,
        None => ctx.reply_null()?,
    }
    ctx.reply_integer(subscription_count as i64)
}

/// Send a `psubscribe` confirmation frame.
///
/// C: `addReplyPubsubPatSubscribed(client *c, robj *pattern)`
/// C: pubsub.c:173-184
pub fn add_reply_pubsub_pat_subscribed(
    ctx: &mut CommandContext,
    pattern: &RedisString,
    subscription_count: i32,
) -> Result<(), RedisError> {
    ctx.reply_push_or_array_header(3)?;
    ctx.reply_bulk(b"psubscribe")?;
    ctx.reply_bulk(pattern.as_slice())?;
    ctx.reply_integer(subscription_count as i64)
}

/// Send a `punsubscribe` confirmation frame.
///
/// When `pattern` is `None` the client had no pattern subscriptions.
///
/// C: `addReplyPubsubPatUnsubscribed(client *c, robj *pattern)`
/// C: pubsub.c:190-204
pub fn add_reply_pubsub_pat_unsubscribed(
    ctx: &mut CommandContext,
    pattern: Option<&RedisString>,
    subscription_count: i32,
) -> Result<(), RedisError> {
    ctx.reply_push_or_array_header(3)?;
    ctx.reply_bulk(b"punsubscribe")?;
    match pattern {
        Some(p) => ctx.reply_bulk(p.as_slice())?,
        None => ctx.reply_null()?,
    }
    ctx.reply_integer(subscription_count as i64)
}

// ─────────────────────────────────────────────────────────────────────────────
// Low-level subscribe / unsubscribe
// ─────────────────────────────────────────────────────────────────────────────

/// Subscribe the current client to `channel`.
///
/// Returns `true` if the client was not already subscribed (new subscription),
/// `false` if it was already subscribed.
///
/// C: `pubsubSubscribeChannel(client *c, robj *channel, pubsubtype type)`
/// C: pubsub.c:290-328
///
/// TODO(architect): Needs mutable access to:
///   1. `ctx.client_pub_sub_data_mut()` — per-client channels set.
///   2. `ctx.server_mut().pubsub_channels` (or `pubsubshard_channels`) —
///      server-wide channel→subscribers map.
/// Both are hidden behind `CommandContext`; wire them up in Phase 3.
///
/// TODO(port): Cluster slot calculation (`getKeySlot`, `keyHashSlot`) for
/// shard pub/sub is omitted; requires cluster integration (Phase 4).
pub fn pubsub_subscribe_channel(
    ctx: &mut CommandContext,
    channel: RedisString,
    kind: PubSubKind,
) -> Result<bool, RedisError> {
    // TODO(port): initialise ClientPubSubData if not present (mirrors C
    // `if (!c->pubsub_data) initClientPubSubData(c)`).

    // TODO(port): insert `channel` into client's per-kind channel set and
    // into server's channel→subscriber map. Return `false` if already present.
    // Sketched logic:
    //   let already = !ctx.client_pub_sub_data_mut().channels_for(kind).insert(channel.clone());
    //   if !already { ... insert client_id into server map ... }
    //   send subscribe confirmation
    let newly_subscribed = true; // TODO(port): replace with real insertion result

    // Subscription count after this operation.
    // TODO(port): compute from actual data; placeholder uses 0.
    let count: i32 = 0; // TODO(port): ctx.client_pub_sub_data().subscription_count(kind)

    add_reply_pubsub_subscribed(ctx, &channel, kind, count)?;
    Ok(newly_subscribed)
}

/// Unsubscribe the current client from `channel`.
///
/// Returns `true` if the client was subscribed (and is now unsubscribed),
/// `false` if the client was not subscribed to that channel.
///
/// When `notify` is `true` a confirmation frame is sent to the client.
///
/// C: `pubsubUnsubscribeChannel(client *c, robj *channel, int notify, pubsubtype type)`
/// C: pubsub.c:332-366
///
/// TODO(architect): same server-state access as `pubsub_subscribe_channel`.
/// TODO(port): Cluster slot for shard kind omitted (Phase 4).
/// TODO(port): When server-side subscriber set becomes empty, remove the
/// channel key entirely (mirrors C `kvstoreHashtableDelete`).
pub fn pubsub_unsubscribe_channel(
    ctx: &mut CommandContext,
    channel: RedisString,
    notify: bool,
    kind: PubSubKind,
) -> Result<bool, RedisError> {
    // TODO(port): remove channel from client's per-kind channel set.
    // TODO(port): remove client_id from server's channel→subscribers map.
    // TODO(port): if server subscriber set is now empty, delete channel entry.
    let was_subscribed = false; // TODO(port): real removal result

    if notify {
        // TODO(port): compute real subscription count after removal.
        let count: i32 = 0;
        add_reply_pubsub_unsubscribed(ctx, Some(&channel), kind, count)?;
    }
    Ok(was_subscribed)
}

/// Unsubscribe all subscribers from every channel in `slot` (cluster shard slot).
///
/// Called when a cluster slot migrates away.
///
/// C: `pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot)`
/// C: pubsub.c:369-396
///
/// TODO(architect): requires iterating the server's `pubsubshard_channels`
/// kvstore for the given slot and calling `add_reply_pubsub_unsubscribed` on
/// each subscriber's client handle — multi-client mutable access problem
/// described in the module-level architect TODO. Deferred to Phase 3.
pub fn pubsub_shard_unsubscribe_all_channels_in_slot(
    _slot: u32,
    _shard_channels: &mut ServerChannelMap,
) -> Result<(), RedisError> {
    // TODO(port): iterate slot's channel→clients map, remove each client's
    // subscription, send unsubscribe push to each client, then clear the slot.
    Ok(())
}

/// Subscribe the current client to a glob pattern.
///
/// Returns `true` if this is a new subscription.
///
/// C: `pubsubSubscribePattern(client *c, robj *pattern)`
/// C: pubsub.c:400-420
///
/// TODO(architect): server-pattern map access via `CommandContext`.
pub fn pubsub_subscribe_pattern(
    ctx: &mut CommandContext,
    pattern: RedisString,
) -> Result<bool, RedisError> {
    // TODO(port): initialise ClientPubSubData if absent.
    // TODO(port): insert pattern into client's pubsub_patterns set.
    // TODO(port): insert client_id into server's pubsub_patterns map.
    let newly_subscribed = true; // TODO(port): real result

    // TODO(port): real subscription count.
    let count: i32 = 0;
    add_reply_pubsub_pat_subscribed(ctx, &pattern, count)?;
    Ok(newly_subscribed)
}

/// Unsubscribe the current client from a glob pattern.
///
/// Returns `true` if the client was subscribed to the pattern.
/// When `notify` is `true` a confirmation frame is sent.
///
/// C: `pubsubUnsubscribePattern(client *c, robj *pattern, int notify)`
/// C: pubsub.c:424-444
///
/// TODO(architect): server-pattern map access via `CommandContext`.
pub fn pubsub_unsubscribe_pattern(
    ctx: &mut CommandContext,
    pattern: RedisString,
    notify: bool,
) -> Result<bool, RedisError> {
    // TODO(port): initialise ClientPubSubData if absent.
    // TODO(port): remove pattern from client's pubsub_patterns.
    // TODO(port): remove client from server's pattern→clients map.
    // TODO(port): if server set empty, remove pattern key.
    let was_subscribed = false; // TODO(port): real result

    if notify {
        let count: i32 = 0; // TODO(port): real count
        add_reply_pubsub_pat_unsubscribed(ctx, Some(&pattern), count)?;
    }
    Ok(was_subscribed)
}

/// Unsubscribe the current client from all channels of the given `kind`.
///
/// Returns the number of channels unsubscribed. Sends a null-channel frame
/// when `notify` is `true` and the client had no subscriptions.
///
/// C: `pubsubUnsubscribeAllChannelsInternal(client *c, int notify, pubsubtype type)`
/// C: pubsub.c:448-464
pub fn pubsub_unsubscribe_all_channels_internal(
    ctx: &mut CommandContext,
    notify: bool,
    kind: PubSubKind,
) -> Result<i32, RedisError> {
    // TODO(port): collect channels from client's per-kind set, then iterate
    // and unsubscribe each (must collect first to avoid mutating while iterating).
    // Placeholder:
    let count: i32 = 0; // TODO(port): real unsubscribe loop

    if notify && count == 0 {
        // C: still sends unsubscribed notification with null channel
        add_reply_pubsub_unsubscribed(ctx, None, kind, 0)?;
    }
    Ok(count)
}

/// Unsubscribe the current client from all global channels.
///
/// C: `pubsubUnsubscribeAllChannels(client *c, int notify)`
/// C: pubsub.c:469-472
pub fn pubsub_unsubscribe_all_channels(
    ctx: &mut CommandContext,
    notify: bool,
) -> Result<i32, RedisError> {
    pubsub_unsubscribe_all_channels_internal(ctx, notify, PubSubKind::Global)
}

/// Unsubscribe the current client from all shard channels.
///
/// C: `pubsubUnsubscribeShardAllChannels(client *c, int notify)`
/// C: pubsub.c:477-480
pub fn pubsub_unsubscribe_shard_all_channels(
    ctx: &mut CommandContext,
    notify: bool,
) -> Result<i32, RedisError> {
    pubsub_unsubscribe_all_channels_internal(ctx, notify, PubSubKind::Shard)
}

/// Unsubscribe the current client from all glob patterns.
///
/// Returns the number of patterns unsubscribed. Sends a null-pattern frame
/// when `notify` is `true` and the client had no pattern subscriptions.
///
/// C: `pubsubUnsubscribeAllPatterns(client *c, int notify)`
/// C: pubsub.c:484-503
pub fn pubsub_unsubscribe_all_patterns(
    ctx: &mut CommandContext,
    notify: bool,
) -> Result<i32, RedisError> {
    // TODO(port): initialise ClientPubSubData if absent.
    // TODO(port): collect patterns, iterate, call pubsub_unsubscribe_pattern.
    let count: i32 = 0; // TODO(port): real loop

    if notify && count == 0 {
        add_reply_pubsub_pat_unsubscribed(ctx, None, 0)?;
    }
    Ok(count)
}

// ─────────────────────────────────────────────────────────────────────────────
// Publish
// ─────────────────────────────────────────────────────────────────────────────

/// Publish `message` to all subscribers of `channel` (and matching patterns
/// for `PubSubKind::Global`).
///
/// Returns the number of clients that received the message.
///
/// C: `pubsubPublishMessageInternal(robj *channel, robj *message, pubsubtype type)`
/// C: pubsub.c:508-563
///
/// TODO(architect): Broadcasting to subscriber clients requires mutable access
/// to multiple `Client` objects concurrently — the core ownership problem
/// described in the module-level TODO. For Phase A this is a skeleton only.
///
/// TODO(port): Pattern matching against subscriber patterns (the second loop
/// in the C function) requires `string_match_len` from `redis_core::util`.
/// Also `getDecodedObject` (decoded string representation) must be provided by
/// `RedisString::decoded()` or equivalent.
///
/// TODO(port): Cluster-slot calculation for shard pub/sub omitted (Phase 4).
pub fn pubsub_publish_message_internal(
    channel: &RedisString,
    message: &RedisString,
    kind: PubSubKind,
    _channels: &ServerChannelMap,
    _patterns: &ServerPatternMap,
) -> Result<i32, RedisError> {
    let mut receivers: i32 = 0;

    // C: Send to clients listening for that channel
    // TODO(port): look up channel in server channel map for this kind/slot,
    // iterate subscriber client IDs, acquire each client, call
    // add_reply_pubsub_message on each. Multi-client mutation deferred.
    let _ = (channel, message); // suppress unused warnings until TODO resolved

    if kind == PubSubKind::Shard {
        // C: shard pubsub ignores patterns — return early
        return Ok(receivers);
    }

    // C: Send to clients listening to matching patterns
    // TODO(port): iterate server.pubsub_patterns, run string_match_len against
    // channel name, send pmessage push to each matching subscriber client.

    Ok(receivers)
}

/// Publish `message` to all subscribers of `channel`.
///
/// `sharded` selects between global and shard pub/sub.
///
/// C: `pubsubPublishMessage(robj *channel, robj *message, int sharded)`
/// C: pubsub.c:566-568
pub fn pubsub_publish_message(
    channel: &RedisString,
    message: &RedisString,
    sharded: bool,
    channels: &ServerChannelMap,
    patterns: &ServerPatternMap,
    shard_channels: &ServerChannelMap,
) -> Result<i32, RedisError> {
    let kind = if sharded {
        PubSubKind::Shard
    } else {
        PubSubKind::Global
    };
    let target_channels = if sharded { shard_channels } else { channels };
    pubsub_publish_message_internal(channel, message, kind, target_channels, patterns)
}

/// Publish and also propagate to cluster peers (if cluster enabled).
///
/// C: `pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded)`
/// C: pubsub.c:643-647
///
/// TODO(architect): `cluster_propagate_publish` is in `redis-cluster`; add
/// dep edge or call via `CommandContext::cluster_propagate_publish()`.
pub fn pubsub_publish_message_and_propagate_to_cluster(
    ctx: &mut CommandContext,
    channel: &RedisString,
    message: &RedisString,
    sharded: bool,
) -> Result<i32, RedisError> {
    // TODO(port): wire actual ServerChannelMap / ServerPatternMap access through ctx.
    let receivers: i32 = 0; // TODO(port): call pubsub_publish_message with server state

    // TODO(port): if server.cluster_enabled { cluster_propagate_publish(...) }
    let _ = (ctx, channel, message, sharded);
    Ok(receivers)
}

// ─────────────────────────────────────────────────────────────────────────────
// Command implementations
// ─────────────────────────────────────────────────────────────────────────────

/// SUBSCRIBE channel [channel ...]
///
/// C: `subscribeCommand(client *c)` — pubsub.c:575-590
pub fn subscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: deny_blocking clients cannot subscribe (except inside MULTI)
    // TODO(port): check ctx.client_flags().deny_blocking and ctx.client_flags().multi
    // Placeholder: assume allowed.

    let argc = ctx.argc();
    for i in 1..argc {
        let channel = ctx.arg_owned(i)?;
        pubsub_subscribe_channel(ctx, channel, PubSubKind::Global)?;
    }

    // TODO(port): mark_client_as_pub_sub via ctx — sets client.flag.pubsub and
    // increments server.pubsub_clients.
    Ok(())
}

/// UNSUBSCRIBE [channel ...]
///
/// C: `unsubscribeCommand(client *c)` — pubsub.c:593-606
pub fn unsubscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): initialise ClientPubSubData if absent.

    if ctx.argc() == 1 {
        pubsub_unsubscribe_all_channels(ctx, true)?;
    } else {
        let argc = ctx.argc();
        for i in 1..argc {
            let channel = ctx.arg_owned(i)?;
            pubsub_unsubscribe_channel(ctx, channel, true, PubSubKind::Global)?;
        }
    }

    // TODO(port): if total subscription count == 0, unmark_client_as_pub_sub.
    Ok(())
}

/// PSUBSCRIBE pattern [pattern ...]
///
/// C: `psubscribeCommand(client *c)` — pubsub.c:609-625
pub fn psubscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: deny_blocking clients cannot subscribe (except inside MULTI)
    // TODO(port): check client flags.

    let argc = ctx.argc();
    for i in 1..argc {
        let pattern = ctx.arg_owned(i)?;
        pubsub_subscribe_pattern(ctx, pattern)?;
    }

    // TODO(port): mark_client_as_pub_sub.
    Ok(())
}

/// PUNSUBSCRIBE [pattern [pattern ...]]
///
/// C: `punsubscribeCommand(client *c)` — pubsub.c:628-639
pub fn punsubscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.argc() == 1 {
        pubsub_unsubscribe_all_patterns(ctx, true)?;
    } else {
        let argc = ctx.argc();
        for i in 1..argc {
            let pattern = ctx.arg_owned(i)?;
            pubsub_unsubscribe_pattern(ctx, pattern, true)?;
        }
    }

    // TODO(port): if total subscription count == 0, unmark_client_as_pub_sub.
    Ok(())
}

/// PUBLISH <channel> <message>
///
/// C: `publishCommand(client *c)` — pubsub.c:650-659
///
/// TODO(architect): sentinel mode (`server.sentinel_mode`) needs to be
/// accessible via `CommandContext`; calls `sentinel_publish_command` when set.
pub fn publish_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): if server.sentinel_mode { sentinel_publish_command(ctx); return Ok(()); }

    let channel = ctx.arg_owned(1)?;
    let message = ctx.arg_owned(2)?;
    let receivers = pubsub_publish_message_and_propagate_to_cluster(
        ctx, &channel, &message, false,
    )?;

    // TODO(port): if !server.cluster_enabled { force_command_propagation(PROPAGATE_REPL) }
    ctx.reply_integer(receivers as i64)
}

/// PUBSUB CHANNELS | NUMSUB | NUMPAT | SHARDCHANNELS | SHARDNUMSUB
///
/// C: `pubsubCommand(client *c)` — pubsub.c:662-717
pub fn pubsub_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): subcommand is compared case-insensitively in C via strcasecmp.
    // Use RedisString byte-comparison in Rust (ASCII case-fold manually or via
    // an ascii_lowercase helper). Placeholder routes on exact lowercase bytes.

    let argc = ctx.argc();
    let subcmd_raw = ctx.arg(1)?;
    let subcmd: Vec<u8> = subcmd_raw.iter().map(|b| b.to_ascii_lowercase()).collect();

    match subcmd.as_slice() {
        b"help" if argc == 2 => {
            // C: addReplyHelp — emit a multi-bulk array of help strings.
            // TODO(port): wire to ctx.reply_help_array(lines).
            let help_lines: &[&[u8]] = &[
                b"CHANNELS [<pattern>]",
                b"    Return the currently active channels matching a <pattern> (default: '*').",
                b"NUMPAT",
                b"    Return number of subscriptions to patterns.",
                b"NUMSUB [<channel> ...]",
                b"    Return the number of subscribers for the specified channels, excluding",
                b"    pattern subscriptions (default: no channels).",
                b"SHARDCHANNELS [<pattern>]",
                b"    Return the currently active shard level channels matching a <pattern> (default: '*').",
                b"SHARDNUMSUB [<shardchannel> ...]",
                b"    Return the number of subscribers for the specified shard level channel(s)",
            ];
            ctx.reply_array_header(help_lines.len() as i64)?;
            for line in help_lines {
                ctx.reply_bulk(line)?;
            }
            Ok(())
        }
        b"channels" if argc == 2 || argc == 3 => {
            // PUBSUB CHANNELS [<pattern>]
            let pattern: Option<Vec<u8>> = if argc == 3 {
                Some(ctx.arg(2)?.to_vec())
            } else {
                None
            };
            channel_list(ctx, pattern.as_deref(), false)
        }
        b"numsub" if argc >= 2 => {
            // PUBSUB NUMSUB [Channel_1 ... Channel_N]
            ctx.reply_array_header(((argc - 2) * 2) as i64)?;
            for i in 2..argc {
                let ch = ctx.arg_owned(i)?;
                // TODO(port): look up ch in server.pubsub_channels and return
                // subscriber count. Placeholder returns 0.
                ctx.reply_bulk(ch.as_slice())?;
                ctx.reply_integer(0)?; // TODO(port): real subscriber count
            }
            Ok(())
        }
        b"numpat" if argc == 2 => {
            // PUBSUB NUMPAT
            // TODO(port): return dictSize(server.pubsub_patterns) via ctx.
            ctx.reply_integer(0) // TODO(port): real pattern count
        }
        b"shardchannels" if argc == 2 || argc == 3 => {
            // PUBSUB SHARDCHANNELS [<pattern>]
            let pattern: Option<Vec<u8>> = if argc == 3 {
                Some(ctx.arg(2)?.to_vec())
            } else {
                None
            };
            channel_list(ctx, pattern.as_deref(), true)
        }
        b"shardnumsub" if argc >= 2 => {
            // PUBSUB SHARDNUMSUB [ShardChannel_1 ... ShardChannel_N]
            ctx.reply_array_header(((argc - 2) * 2) as i64)?;
            for i in 2..argc {
                let ch = ctx.arg_owned(i)?;
                // TODO(port): compute slot for ch (cluster-enabled), look up
                // in server.pubsubshard_channels, return hashtable size.
                ctx.reply_bulk(ch.as_slice())?;
                ctx.reply_integer(0)?; // TODO(port): real shard subscriber count
            }
            Ok(())
        }
        _ => {
            // C: addReplySubcommandSyntaxError
            Err(RedisError::syntax(b"unknown subcommand or wrong number of arguments"))
        }
    }
}

/// Emit an array of active channel names, optionally filtered by a glob pattern.
///
/// `shard` selects whether to query `server.pubsub_channels` (global) or
/// `server.pubsubshard_channels` (shard).
///
/// C: `channelList(client *c, sds pat, kvstore *pubsub_channels)` — pubsub.c:719-742
///
/// PORT NOTE: The C function receives a `kvstore *` directly; the Rust version
/// receives a `bool` flag (`shard`) and looks up the right map via
/// `CommandContext`. The indirection is equivalent; it just moves the kvstore
/// pointer selection into `CommandContext` methods (Phase 3).
///
/// TODO(architect): iterate `server.pubsub_channels` / `pubsubshard_channels`
/// via `CommandContext`. Until Phase 3, this emits an empty array.
///
/// TODO(port): `string_match_len` for pattern filtering needs `redis_core::util`.
pub fn channel_list(
    ctx: &mut CommandContext,
    pattern: Option<&[u8]>,
    _shard: bool,
) -> Result<(), RedisError> {
    // C: uses deferred array length (addReplyDeferredLen / setDeferredArrayLen)
    // because we don't know the count up front.
    // TODO(port): wire deferred-len API when available. For now emit an empty array.
    let _ = pattern; // TODO(port): apply glob filter
    ctx.reply_array_header(0)?; // TODO(port): iterate real channel map
    Ok(())
}

/// SPUBLISH <shardchannel> <message>
///
/// C: `spublishCommand(client *c)` — pubsub.c:745-749
pub fn spublish_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let channel = ctx.arg_owned(1)?;
    let message = ctx.arg_owned(2)?;
    let receivers = pubsub_publish_message_and_propagate_to_cluster(
        ctx, &channel, &message, true,
    )?;

    // TODO(port): if !server.cluster_enabled { force_command_propagation(PROPAGATE_REPL) }
    ctx.reply_integer(receivers as i64)
}

/// SSUBSCRIBE shardchannel [shardchannel ...]
///
/// C: `ssubscribeCommand(client *c)` — pubsub.c:752-764
///
/// Unlike SUBSCRIBE, SSUBSCRIBE does not have a MULTI exemption for the
/// deny_blocking check.
pub fn ssubscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: deny_blocking clients cannot ssubscribe (no MULTI exception)
    // TODO(port): check ctx.client_flags().deny_blocking.

    let argc = ctx.argc();
    for i in 1..argc {
        let channel = ctx.arg_owned(i)?;
        pubsub_subscribe_channel(ctx, channel, PubSubKind::Shard)?;
    }

    // TODO(port): mark_client_as_pub_sub.
    Ok(())
}

/// SUNSUBSCRIBE [shardchannel [shardchannel ...]]
///
/// C: `sunsubscribeCommand(client *c)` — pubsub.c:767-780
pub fn sunsubscribe_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): initialise ClientPubSubData if absent.

    if ctx.argc() == 1 {
        pubsub_unsubscribe_shard_all_channels(ctx, true)?;
    } else {
        let argc = ctx.argc();
        for i in 1..argc {
            let channel = ctx.arg_owned(i)?;
            pubsub_unsubscribe_channel(ctx, channel, true, PubSubKind::Shard)?;
        }
    }

    // TODO(port): if total subscription count == 0, unmark_client_as_pub_sub.
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/pubsub.c  (797 lines, 30 functions)
//   target_crate:  redis-commands
//   confidence:    low
//   todos:         67
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Logic skeleton is faithful; all server-state access and
//                  multi-client broadcast are TODO(architect) / TODO(port)
//                  because they require Phase 3 CommandContext wiring and a
//                  decision on the concurrent-client-mutation model for the
//                  publish broadcast loop. rustc --emit=metadata shows only
//                  expected E0282/E0432/E0433 name-resolution errors.
// ──────────────────────────────────────────────────────────────────────────

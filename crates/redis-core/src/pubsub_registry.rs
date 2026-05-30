//! Global pub/sub registry shared by every connection.
//! Two-layer mapping plus a per-client outbound channel:
//! * `channels` — channel name → set of subscriber client ids.
//! * `patterns` — glob pattern → set of subscriber client ids.
//! * `shard_channels` — shard channel name → set of subscriber client ids.
//! * `senders` — client id → mpsc::Sender used to push frames to that
//! client's writer thread.
//! All access goes through `Arc<Mutex<PubSubRegistry>>` so PUBLISH (running on
//! a foreign connection's thread) can look up subscribers, enqueue bytes onto
//! each subscriber's mpsc sender, and return the receiver count atomically.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;

use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::RedisString;

use crate::client::ClientId;

/// Server-wide pub/sub state.
pub struct PubSubRegistry {
    channels: HashMap<RedisString, HashSet<ClientId>>,
    patterns: HashMap<RedisString, HashSet<ClientId>>,
    shard_channels: HashMap<RedisString, HashSet<ClientId>>,
    senders: HashMap<ClientId, Sender<Vec<u8>>>,
 /// Per-client RESP protocol version negotiated by `HELLO` (2 or 3).
 /// Looked up by PUBLISH / keyspace-notify paths so message frames can be
 /// emitted as RESP3 push frames (`>`) for subscribers that asked for it.
 /// Defaults to 2 for clients that never called `HELLO 3`.
    resp_protos: HashMap<ClientId, i32>,
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubRegistry {
 /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            shard_channels: HashMap::new(),
            senders: HashMap::new(),
            resp_protos: HashMap::new(),
        }
    }

 /// Register the outbound mpsc sender for `client_id`.
 /// Called once per connection from the accept loop, before dispatch runs.
    pub fn register_sender(&mut self, client_id: ClientId, tx: Sender<Vec<u8>>) {
        self.senders.insert(client_id, tx);
    }

 /// Drop the outbound sender and remove every subscription tied
 /// `client_id`. Called when a connection closes.
    pub fn drop_client(&mut self, client_id: ClientId) {
        if let Some(sender) = self.senders.remove(&client_id) {
            let _ = sender.send(Vec::new());
        }
        self.resp_protos.remove(&client_id);
        self.channels.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
        self.shard_channels.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
    }

 /// Record (or update) the RESP protocol version for `client_id`. Called
 /// from the HELLO command handler when the client successfully negotiates
 /// RESP3 (and during accept-loop setup so RESP2 is the default).
    pub fn set_resp_proto(&mut self, client_id: ClientId, proto: i32) {
        self.resp_protos.insert(client_id, proto);
    }

 /// Look up `client_id`'s negotiated RESP protocol version. Returns `2`
 /// for clients that have not run `HELLO 3` (or that aren't tracked).
    pub fn resp_proto(&self, client_id: ClientId) -> i32 {
        match self.resp_protos.get(&client_id) {
            Some(p) => *p,
            None => 2,
        }
    }

 /// Add `client_id` to the subscriber set for `channel`. Returns `true`
 /// when the client was newly subscribed.
    pub fn subscribe_channel(&mut self, channel: RedisString, client_id: ClientId) -> bool {
        self.channels.entry(channel).or_default().insert(client_id)
    }

 /// Remove `client_id` from `channel`'s subscriber set. Returns `true` if
 /// the client had been subscribed.
    pub fn unsubscribe_channel(&mut self, channel: &RedisString, client_id: ClientId) -> bool {
        let mut removed = false;
        let mut now_empty = false;
        if let Some(set) = self.channels.get_mut(channel) {
            removed = set.remove(&client_id);
            now_empty = set.is_empty();
        }
        if now_empty {
            self.channels.remove(channel);
        }
        removed
    }

 /// Add `client_id` to the subscriber set for `pattern`. Returns `true`
 /// when the client was newly subscribed.
    pub fn subscribe_pattern(&mut self, pattern: RedisString, client_id: ClientId) -> bool {
        self.patterns.entry(pattern).or_default().insert(client_id)
    }

 /// Remove `client_id` from `pattern`'s subscriber set. Returns `true` if
 /// the client had been subscribed.
    pub fn unsubscribe_pattern(&mut self, pattern: &RedisString, client_id: ClientId) -> bool {
        let mut removed = false;
        let mut now_empty = false;
        if let Some(set) = self.patterns.get_mut(pattern) {
            removed = set.remove(&client_id);
            now_empty = set.is_empty();
        }
        if now_empty {
            self.patterns.remove(pattern);
        }
        removed
    }

 /// Add `client_id` to the subscriber set for shard `channel`.
    pub fn subscribe_shard_channel(&mut self, channel: RedisString, client_id: ClientId) -> bool {
        self.shard_channels
            .entry(channel)
            .or_default()
            .insert(client_id)
    }

 /// Remove `client_id` from shard `channel`'s subscriber set.
    pub fn unsubscribe_shard_channel(
        &mut self,
        channel: &RedisString,
        client_id: ClientId,
    ) -> bool {
        let mut removed = false;
        let mut now_empty = false;
        if let Some(set) = self.shard_channels.get_mut(channel) {
            removed = set.remove(&client_id);
            now_empty = set.is_empty();
        }
        if now_empty {
            self.shard_channels.remove(channel);
        }
        removed
    }

 /// Snapshot the subscriber ids for an exact channel match.
    pub fn channel_subscribers(&self, channel: &RedisString) -> Vec<ClientId> {
        self.channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

 /// Snapshot the subscriber ids for an exact shard channel match.
    pub fn shard_channel_subscribers(&self, channel: &RedisString) -> Vec<ClientId> {
        self.shard_channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

 /// Snapshot every `(pattern, subscribers)` pair where the pattern matches
 /// `channel`. Returns owned pattern strings so callers can release
 /// registry lock before pushing payloads through senders.
    pub fn pattern_matches(
        &self,
        channel: &RedisString,
        matcher: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Vec<(RedisString, Vec<ClientId>)> {
        self.patterns
            .iter()
            .filter(|(pat, _)| matcher(pat.as_bytes(), channel.as_bytes()))
            .map(|(pat, subs)| (pat.clone(), subs.iter().copied().collect()))
            .collect()
    }

 /// Send raw bytes to `client_id` via its outbound mpsc sender. Returns
 /// `true` when the send succeeded (the receiver was alive).
    pub fn send_to(&self, client_id: ClientId, bytes: Vec<u8>) -> bool {
        match self.senders.get(&client_id) {
            Some(tx) => tx.send(bytes).is_ok(),
            None => false,
        }
    }

 /// Clone the outbound mpsc sender for `client_id` if it is registered.
 /// Used by the blocked-keys index so a parked BLPOP waiter can be woken
 /// later from a different connection's push handler — the wake hook owns
 /// the cloned sender and never has to re-enter the registry mutex.
    pub fn sender_for(&self, client_id: ClientId) -> Option<Sender<Vec<u8>>> {
        self.senders.get(&client_id).cloned()
    }

 /// Iterate every currently-active channel, optionally filtered by a glob
 /// pattern. Returns owned clones; intended for PUBSUB CHANNELS.
    pub fn list_channels(
        &self,
        pattern: Option<&[u8]>,
        matcher: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Vec<RedisString> {
        self.channels
            .keys()
            .filter(|ch| match pattern {
                Some(pat) => matcher(pat, ch.as_bytes()),
                None => true,
            })
            .cloned()
            .collect()
    }

 /// Iterate every currently-active shard channel, optionally filtered by a
 /// glob pattern. Returns owned clones; intended for PUBSUB SHARDCHANNELS.
    pub fn list_shard_channels(
        &self,
        pattern: Option<&[u8]>,
        matcher: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Vec<RedisString> {
        self.shard_channels
            .keys()
            .filter(|ch| match pattern {
                Some(pat) => matcher(pat, ch.as_bytes()),
                None => true,
            })
            .cloned()
            .collect()
    }

 /// Subscriber count for an exact channel.
    pub fn num_sub(&self, channel: &RedisString) -> i64 {
        self.channels
            .get(channel)
            .map(|s| s.len() as i64)
            .unwrap_or(0)
    }

 /// Subscriber count for an exact shard channel.
    pub fn num_shard_sub(&self, channel: &RedisString) -> i64 {
        self.shard_channels
            .get(channel)
            .map(|s| s.len() as i64)
            .unwrap_or(0)
    }

 /// Total number of distinct active patterns across all clients.
    pub fn num_pat(&self) -> i64 {
        self.patterns.len() as i64
    }
}

// ── Pub/Sub message-encoding helpers (moved from command_context.rs 2026-05-28) ──
// These were tail-call helpers used by PUBLISH/SPUBLISH dispatch; they live
// next to the PubSubRegistry now, not the dispatch context.

pub fn encode_pubsub_message_resp2(channel: &RedisString, message: &RedisString) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + channel.as_bytes().len() + message.as_bytes().len());
    encode_resp2(
        &RespFrame::array(vec![
            RespFrame::bulk(RedisString::from_static(b"message")),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

/// Encode a RESP3 `>3 message channel payload` push frame.
pub fn encode_pubsub_message_resp3(channel: &RedisString, message: &RedisString) -> Vec<u8> {
    let mut buf = Vec::with_capacity(48 + channel.as_bytes().len() + message.as_bytes().len());
    redis_protocol::encode_resp3(
        &RespFrame::Push(vec![
            RespFrame::bulk(RedisString::from_static(b"message")),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

/// Encode a RESP2 `*4 pmessage pattern channel payload` array.
pub fn encode_pubsub_pmessage_resp2(
    pattern: &RedisString,
    channel: &RedisString,
    message: &RedisString,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        48 + pattern.as_bytes().len() + channel.as_bytes().len() + message.as_bytes().len(),
    );
    encode_resp2(
        &RespFrame::array(vec![
            RespFrame::bulk(RedisString::from_static(b"pmessage")),
            RespFrame::bulk(pattern.clone()),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

/// Encode a RESP3 `>4 pmessage pattern channel payload` push frame.
pub fn encode_pubsub_pmessage_resp3(
    pattern: &RedisString,
    channel: &RedisString,
    message: &RedisString,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        64 + pattern.as_bytes().len() + channel.as_bytes().len() + message.as_bytes().len(),
    );
    redis_protocol::encode_resp3(
        &RespFrame::Push(vec![
            RespFrame::bulk(RedisString::from_static(b"pmessage")),
            RespFrame::bulk(pattern.clone()),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        round-8a pub/sub architecture (no direct C analogue —
//                  Rust shared-state wrapper around the kvstore/dict roles
//                  played by server.pubsub_channels / pubsub_patterns).
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Holds the central channel/pattern subscriber tables plus
//                  per-client mpsc senders so PUBLISH can deliver bytes
//                  cross-thread without locking subscriber sockets directly.
// ──────────────────────────────────────────────────────────────────────────

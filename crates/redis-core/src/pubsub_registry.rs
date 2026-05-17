//! Global pub/sub registry shared by every connection.
//!
//! Two-layer mapping plus a per-client outbound channel:
//!   * `channels`  — channel name → set of subscriber client ids.
//!   * `patterns`  — glob pattern → set of subscriber client ids.
//!   * `senders`   — client id → mpsc::Sender used to push frames to that
//!     client's writer thread.
//!
//! All access goes through `Arc<Mutex<PubSubRegistry>>` so PUBLISH (running on
//! a foreign connection's thread) can look up subscribers, enqueue bytes onto
//! each subscriber's mpsc sender, and return the receiver count atomically.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;

use redis_types::RedisString;

use crate::client::ClientId;

/// Server-wide pub/sub state.
pub struct PubSubRegistry {
    channels: HashMap<RedisString, HashSet<ClientId>>,
    patterns: HashMap<RedisString, HashSet<ClientId>>,
    senders: HashMap<ClientId, Sender<Vec<u8>>>,
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
            senders: HashMap::new(),
        }
    }

    /// Register the outbound mpsc sender for `client_id`.
    ///
    /// Called once per connection from the accept loop, before dispatch runs.
    pub fn register_sender(&mut self, client_id: ClientId, tx: Sender<Vec<u8>>) {
        self.senders.insert(client_id, tx);
    }

    /// Drop the outbound sender and remove every subscription tied to
    /// `client_id`. Called when a connection closes.
    pub fn drop_client(&mut self, client_id: ClientId) {
        self.senders.remove(&client_id);
        self.channels.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
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

    /// Snapshot the subscriber ids for an exact channel match.
    pub fn channel_subscribers(&self, channel: &RedisString) -> Vec<ClientId> {
        self.channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Snapshot every `(pattern, subscribers)` pair where the pattern matches
    /// `channel`. Returns owned pattern strings so callers can release the
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

    /// Subscriber count for an exact channel.
    pub fn num_sub(&self, channel: &RedisString) -> i64 {
        self.channels
            .get(channel)
            .map(|s| s.len() as i64)
            .unwrap_or(0)
    }

    /// Total number of distinct active patterns across all clients.
    pub fn num_pat(&self) -> i64 {
        self.patterns.len() as i64
    }
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

//! Replication command handlers: REPLICAOF / SLAVEOF, PSYNC, SYNC.
//!
//! Session 3A scope: just the master-side handshake accept path and the
//! REPLICAOF toggle. The replica-side handshake (dialling the master, running
//! PING / REPLCONF / PSYNC, applying the streamed RDB blob) is Wave C; the
//! actual full-sync RDB transfer back to a freshly-attached replica is Wave B.
//!
//! All three handlers route through [`redis_core::replication`] for the
//! global replication state. The pubsub registry is reused as the source of
//! per-client outbound mpsc senders — the same writer-thread mechanism that
//! PUBLISH and BLPOP wakes ride on.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use redis_core::client::ClientId;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::replication::{
    continue_reply, fullresync_reply, global_replication_state, ReplicaConn, ReplicaState,
    ReplicationState,
};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult};

/// `REPLICAOF host port` / `REPLICAOF NO ONE` (alias: `SLAVEOF`).
///
/// `REPLICAOF NO ONE` cancels replica mode and becomes a standalone primary.
/// `REPLICAOF <host> <port>` configures this server as a replica of the named
/// master. The actual handshake — opening a TCP connection to the master and
/// running PING / REPLCONF / PSYNC — is Wave C. Session 3A just records the
/// target on [`ReplicationState`] and emits a TODO log.
pub fn replicaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"replicaof"));
    }
    let host = ctx.arg_owned(1usize)?;
    let port_str = ctx.arg_owned(2usize)?;
    let repl = global_replication_state();

    if is_no_one(host.as_bytes(), port_str.as_bytes()) {
        repl.become_master();
        return ctx.reply_simple_string(b"OK");
    }

    let port: u16 = match parse_port(port_str.as_bytes()) {
        Some(p) => p,
        None => {
            return Err(RedisError::runtime(
                b"ERR value is out of range, value must between 1 and 65535".to_vec(),
            ));
        }
    };
    repl.become_replica_of(host.clone(), port);
    eprintln!(
        "redis-server: REPLICAOF {} {} — TODO: replica handshake (Wave C)",
        String::from_utf8_lossy(host.as_bytes()),
        port
    );
    ctx.reply_simple_string(b"OK")
}

/// True when both arguments spell `NO` and `ONE` case-insensitively.
fn is_no_one(host: &[u8], port: &[u8]) -> bool {
    ascii_eq_ignore_case(host, b"NO") && ascii_eq_ignore_case(port, b"ONE")
}

/// `PSYNC <runid> <offset>` — master-side handshake accept.
///
/// Decides between partial resync (`+CONTINUE <runid>`) and full resync
/// (`+FULLRESYNC <runid> <offset>`) based on whether the replica's claimed
/// run id matches ours and its offset is still inside the live backlog
/// window.
///
/// Session 3A wires the reply line correctly and registers a `ReplicaConn`
/// entry in the global registry. The actual full-sync RDB transfer to the
/// replica after `+FULLRESYNC` is a Wave B TODO — for now we just log that
/// the master-side BGSAVE-for-replica path is not yet armed.
pub fn psync_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"psync"));
    }
    let provided_runid = ctx.arg_owned(1usize)?;
    let provided_offset = parse_offset(ctx.arg_owned(2usize)?.as_bytes())?;
    handle_psync(ctx, provided_runid.as_bytes(), provided_offset)
}

/// `SYNC` — deprecated alias for `PSYNC ? -1` (always-full-resync).
pub fn sync_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"sync"));
    }
    handle_psync(ctx, b"?", -1)
}

/// Shared body of `PSYNC` and `SYNC`. `provided_runid == b"?"` and
/// `provided_offset == -1` is the canonical full-resync request.
fn handle_psync(
    ctx: &mut CommandContext<'_>,
    provided_runid: &[u8],
    provided_offset: i64,
) -> RedisResult<()> {
    let repl = global_replication_state();
    let our_runid = repl.runid();
    let master_offset = repl.master_offset();

    let runid_matches = provided_runid == &our_runid[..] || provided_runid == b"?";
    let can_partial = runid_matches
        && provided_offset >= 0
        && partial_in_window(&repl, provided_offset, master_offset);

    let client_id = ctx.client_ref().id();
    let outbound = steal_outbound_sender(ctx.pubsub.as_ref(), client_id);

    if can_partial {
        if let Some(sender) = outbound {
            register_replica(&repl, client_id, ReplicaState::Online, provided_offset, sender);
        }
        let line = continue_reply(our_runid);
        ctx.client_mut().reply_buf.extend_from_slice(&line);
        ctx.client_mut().is_replica = true;
        return Ok(());
    }

    let snapshot_offset = master_offset;
    if let Some(sender) = outbound {
        register_replica(
            &repl,
            client_id,
            ReplicaState::WaitingBgsave,
            snapshot_offset,
            sender,
        );
    }
    eprintln!(
        "redis-server: PSYNC client_id={} → FULLRESYNC at offset {} \
         (TODO: full-sync RDB transfer arms in Wave B)",
        client_id, snapshot_offset
    );
    let line = fullresync_reply(our_runid, snapshot_offset);
    ctx.client_mut().reply_buf.extend_from_slice(&line);
    ctx.client_mut().is_replica = true;
    Ok(())
}

/// True when the replica's requested offset lies inside the live backlog
/// window (lower bound is the backlog's `min_offset`, upper bound is the
/// current master offset).
fn partial_in_window(repl: &Arc<ReplicationState>, provided: i64, master_offset: i64) -> bool {
    if provided > master_offset {
        return false;
    }
    let (min, _, _, _) = repl.backlog_snapshot();
    provided >= min
}

/// Look up `client_id`'s writer-thread mpsc sender through the shared pubsub
/// registry. Returns `None` only when the registry was not installed (unit
/// tests) or when the client predated registration; the live server always
/// registers a sender before dispatch runs.
fn steal_outbound_sender(
    pubsub: Option<&Arc<std::sync::Mutex<PubSubRegistry>>>,
    client_id: ClientId,
) -> Option<std::sync::mpsc::Sender<Vec<u8>>> {
    let registry = pubsub?;
    let guard = match registry.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.sender_for(client_id)
}

/// Install a `ReplicaConn` for `client_id` in the global replication state.
fn register_replica(
    repl: &Arc<ReplicationState>,
    client_id: ClientId,
    state: ReplicaState,
    offset: i64,
    sender: std::sync::mpsc::Sender<Vec<u8>>,
) {
    let conn = ReplicaConn::new(client_id, state, offset, sender);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    conn.last_ack_time_ms.store(now_ms, Ordering::Relaxed);
    repl.add_replica(conn);
}

/// Parse a PSYNC offset argument. `-1` is the canonical "no prior offset"
/// sentinel and is accepted verbatim. Other negatives produce a protocol
/// error to match real Redis behaviour.
fn parse_offset(bytes: &[u8]) -> RedisResult<i64> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range".to_vec()))?;
    s.parse::<i64>()
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range".to_vec()))
}

/// Parse a TCP port literal. Returns `None` on parse failure or out-of-range.
fn parse_port(bytes: &[u8]) -> Option<u16> {
    let s = std::str::from_utf8(bytes).ok()?;
    let n: i64 = s.parse().ok()?;
    if !(1..=65535).contains(&n) {
        return None;
    }
    Some(n as u16)
}

/// Case-insensitive ASCII byte equality.
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;
    use redis_types::RedisString;

    #[test]
    fn replicaof_no_one_returns_ok() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"REPLICAOF"),
            RedisString::from_bytes(b"NO"),
            RedisString::from_bytes(b"ONE"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        replicaof_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+OK\r\n");
    }

    #[test]
    fn psync_full_resync_with_question_mark() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"PSYNC"),
            RedisString::from_bytes(b"?"),
            RedisString::from_bytes(b"-1"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        psync_command(&mut ctx).unwrap();
        let reply = c.drain_reply();
        assert!(reply.starts_with(b"+FULLRESYNC "), "reply: {:?}", reply);
        assert!(c.is_replica);
    }

    #[test]
    fn sync_routes_through_full_resync() {
        let mut c = Client::new(2);
        c.set_args(vec![RedisString::from_bytes(b"SYNC")]);
        let mut ctx = CommandContext::new(&mut c);
        sync_command(&mut ctx).unwrap();
        let reply = c.drain_reply();
        assert!(reply.starts_with(b"+FULLRESYNC "));
        assert!(c.is_replica);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/replication.c (semantics reference)
//                  plus the architect packet for Session 3A.
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         3
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         PSYNC/SYNC handshake accept; REPLICAOF toggle. Replica
//                  dialer + RDB transfer are Wave B/C TODOs.
// ──────────────────────────────────────────────────────────────────────────

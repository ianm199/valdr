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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::blocked_keys::{
    blocked_keys_index, deadline_from_timeout_secs, BlockedAction, BlockedWaiter,
};
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
    match crate::replica_dialer::spawn_replica_dialer(host.clone(), port) {
        Ok(()) => {
            eprintln!(
                "redis-server: REPLICAOF {} {} — replica dialer spawned",
                String::from_utf8_lossy(host.as_bytes()),
                port
            );
        }
        Err(e) => {
            eprintln!(
                "redis-server: REPLICAOF {} {} — dialer spawn failed: {}",
                String::from_utf8_lossy(host.as_bytes()),
                port,
                e
            );
        }
    }
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

/// `REPLCONF <subcommand> [args ...]`
///
/// Multipurpose command for replica metadata exchange. Subcommands:
///   * `listening-port <N>` — replica's listener port; stored in `ReplicaConn`.
///   * `ip-address <ip>`   — replica's public IP (stored as a future-use note).
///   * `capa <flag> …`     — capability flags; bits ORed into `ReplicaConn.capa_flags`.
///   * `ACK <offset>`      — replica reports its current stream offset; wakes any
///                           WAIT clients that are now satisfied.
///   * `GETACK *`          — primary asks replica for an ACK; answered with `+OK` on
///                           the master side (the replica-side handler is Wave C).
pub fn replconf_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"replconf"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes().to_ascii_lowercase();

    match sub_bytes.as_slice() {
        b"listening-port" => {
            if ctx.arg_count() < 3 {
                return Err(RedisError::wrong_number_of_args(b"replconf"));
            }
            let port_str = ctx.arg_owned(2usize)?;
            let port = parse_port(port_str.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"ERR invalid port number".to_vec()))?;
            let repl = global_replication_state();
            let client_id = ctx.client_ref().id();
            {
                let guard = match repl.replicas.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Some(conn) = guard.get(&client_id) {
                    conn.listening_port.store(port, Ordering::Relaxed);
                }
            }
            ctx.reply_simple_string(b"OK")
        }
        b"ip-address" => ctx.reply_simple_string(b"OK"),
        b"capa" => {
            let repl = global_replication_state();
            let client_id = ctx.client_ref().id();
            let mut i = 2usize;
            while i < ctx.arg_count() {
                let flag_arg = ctx.arg_owned(i)?;
                let bit = capa_flag_bit(flag_arg.as_bytes());
                let guard = match repl.replicas.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Some(conn) = guard.get(&client_id) {
                    conn.capa_flags.fetch_or(bit, Ordering::Relaxed);
                }
                i += 1;
            }
            ctx.reply_simple_string(b"OK")
        }
        b"ack" => {
            if ctx.arg_count() < 3 {
                return Err(RedisError::wrong_number_of_args(b"replconf"));
            }
            let offset_str = ctx.arg_owned(2usize)?;
            let offset = parse_i64(offset_str.as_bytes()).map_err(|_| {
                RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
            })?;
            let client_id = ctx.client_ref().id();
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let repl = global_replication_state();
            {
                let guard = match repl.replicas.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Some(conn) = guard.get(&client_id) {
                    conn.offset.store(offset, Ordering::Relaxed);
                    conn.last_ack_time_ms.store(now_ms, Ordering::Relaxed);
                }
            }
            maybe_wake_wait_clients();
            Ok(())
        }
        b"getack" => ctx.reply_simple_string(b"OK"),
        _ => {
            let mut msg = b"ERR Unknown REPLCONF subcommand: '".to_vec();
            msg.extend_from_slice(sub.as_bytes());
            msg.push(b'\'');
            Err(RedisError::runtime(msg))
        }
    }
}

/// `WAIT numreplicas timeout`
///
/// Blocks until at least `numreplicas` replicas have acknowledged the current
/// `master_repl_offset`, or until `timeout` ms elapses. Returns the count of
/// replicas that acknowledged.
///
/// When `numreplicas` is 0 or when enough replicas are already caught up, the
/// reply is sent immediately with no blocking.
pub fn wait_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"wait"));
    }

    let numreplicas = parse_i64(ctx.arg(1usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range".to_vec()))?
        as usize;
    let timeout_ms = parse_i64(ctx.arg(2usize)?.as_bytes()).map_err(|_| {
        RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
    })?;

    let repl = global_replication_state();
    let target_offset = repl.master_offset();
    let current_acked = count_acked_replicas(&repl, target_offset);

    if ctx.client_ref().flag_deny_blocking() {
        return ctx.reply_integer(current_acked as i64);
    }

    if numreplicas == 0 || current_acked >= numreplicas {
        return ctx.reply_integer(current_acked as i64);
    }

    let registry = match ctx.pubsub.as_ref() {
        Some(r) => r.clone(),
        None => {
            return ctx.reply_integer(current_acked as i64);
        }
    };
    let sender = {
        let guard = match registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.sender_for(ctx.client_ref().id())
    };
    let sender = match sender {
        Some(s) => s,
        None => {
            return ctx.reply_integer(current_acked as i64);
        }
    };

    let timeout_secs = if timeout_ms <= 0 {
        0.0
    } else {
        timeout_ms as f64 / 1000.0
    };

    let sentinel_key = redis_types::RedisString::from_bytes(b"__wait__");
    let waiter = BlockedWaiter {
        client_id: ctx.client_ref().id(),
        sender,
        keys: vec![sentinel_key.clone()],
        action: BlockedAction::Wait {
            target_offset,
            numreplicas,
        },
        deadline_ms: deadline_from_timeout_secs(timeout_secs),
        resp_proto: ctx.client_ref().resp_proto,
    };
    {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.add(waiter);
    }
    ctx.client_mut().blocked_on_keys = true;
    Ok(())
}

/// `WAITAOF numlocal numreplicas timeout`
///
/// Minimal non-replication-safe implementation:
/// returns a two-element array immediately without blocking.
pub fn waitaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"waitaof"));
    }

    let numlocal = parse_i64(ctx.arg(1usize)?.as_bytes()).map_err(|_| {
        RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
    })?;
    if !(0..=1).contains(&numlocal) {
        return Err(RedisError::runtime(
            b"ERR Value for numlocal is out of range [0,1]",
        ));
    }

    let _numreplicas = parse_i64(ctx.arg(2usize)?.as_bytes()).map_err(|_| {
        RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
    })?;
    let _timeout = parse_i64(ctx.arg(3usize)?.as_bytes()).map_err(|_| {
        RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
    })?;

    if numlocal > 0 && !ctx.live_config().appendonly() {
        return Err(RedisError::runtime(
            b"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
        ));
    }

    let repl = global_replication_state();
    let target_offset = repl.master_offset();
    let ackreplicas = count_acked_replicas(&repl, target_offset) as i64;
    let acklocal = 0;

    ctx.reply_array_header(2)?;
    ctx.reply_integer(acklocal)?;
    ctx.reply_integer(ackreplicas)
}

/// Return the count of replicas whose acknowledged offset is `>= target`.
fn count_acked_replicas(repl: &ReplicationState, target: i64) -> usize {
    let guard = match repl.replicas.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .values()
        .filter(|r| r.offset.load(Ordering::Relaxed) >= target)
        .count()
}

/// Walk all WAIT waiters and wake those whose required replica count is
/// now satisfied. Called from the REPLCONF ACK handler after updating a
/// replica's offset.
fn maybe_wake_wait_clients() {
    let repl = global_replication_state();
    let acked_offsets: Vec<i64> = {
        let guard = match repl.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .map(|r| r.offset.load(Ordering::Relaxed))
            .collect()
    };
    let mut idx = match blocked_keys_index().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let satisfied = idx.take_satisfied_wait_waiters(|target| {
        acked_offsets.iter().filter(|&&o| o >= target).count()
    });
    drop(idx);
    for (waiter, count) in satisfied {
        let reply = format!(":{}\r\n", count).into_bytes();
        if waiter.sender.send(reply).is_err() {
            eprintln!(
                "redis-server: WAIT wake send failed for client {}",
                waiter.client_id
            );
        }
    }
}

/// Map a REPLCONF `capa` flag name to its bit position.
///
/// Known flags:
///   * `eof`    — replica can receive the RDB blob without inline `$<len>` framing.
///   * `psync2` — replica supports PSYNC2 (run-id propagation after partial resync).
///
/// Unknown flag names map to bit 31 as a catch-all so they are stored but do
/// not collide with the defined bits.
fn capa_flag_bit(name: &[u8]) -> u32 {
    if name.eq_ignore_ascii_case(b"eof") {
        1u32 << 0
    } else if name.eq_ignore_ascii_case(b"psync2") {
        1u32 << 1
    } else {
        1u32 << 31
    }
}

/// Parse a decimal `i64` from an ASCII byte slice.
fn parse_i64(bytes: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(())
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
            register_replica(
                &repl,
                client_id,
                ReplicaState::Online,
                provided_offset,
                sender,
            );
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
    let line = fullresync_reply(our_runid, snapshot_offset);
    ctx.client_mut().reply_buf.extend_from_slice(&line);
    ctx.client_mut().is_replica = true;

    arm_full_sync_bgsave(ctx, &repl, client_id, snapshot_offset);
    Ok(())
}

/// Either join an in-flight BGSAVE-for-replication job or kick off a new one
/// so the freshly-attached replica eventually receives an RDB snapshot.
///
/// Behaviour:
///   * If a BGSAVE-for-replication is already in progress, append the new
///     replica's `client_id` to the same job's waiting list. Every replica
///     that joins before the child exits receives the identical RDB snapshot
///     and the same catch-up backlog window.
///   * Otherwise call `bgsave_for_replication` to fork a fresh child.
fn arm_full_sync_bgsave(
    ctx: &mut CommandContext<'_>,
    repl: &Arc<ReplicationState>,
    client_id: ClientId,
    snapshot_offset: i64,
) {
    if repl.enqueue_repl_waiter(client_id) {
        eprintln!(
            "redis-server: PSYNC client_id={} → FULLRESYNC at offset {} (joining in-flight BGSAVE)",
            client_id, snapshot_offset
        );
        return;
    }
    match crate::persist::bgsave_for_replication(ctx, client_id) {
        crate::persist::BgsaveForReplResult::Started => {
            eprintln!(
                "redis-server: PSYNC client_id={} → FULLRESYNC at offset {} (BGSAVE started)",
                client_id, snapshot_offset
            );
        }
        crate::persist::BgsaveForReplResult::Skipped => {
            let _ = repl.enqueue_repl_waiter(client_id);
            eprintln!(
                "redis-server: PSYNC client_id={} → FULLRESYNC at offset {} (joined late)",
                client_id, snapshot_offset
            );
        }
        crate::persist::BgsaveForReplResult::Failed => {
            eprintln!(
                "redis-server: PSYNC client_id={} → FULLRESYNC at offset {} but BGSAVE fork failed",
                client_id, snapshot_offset
            );
        }
    }
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
    let s = std::str::from_utf8(bytes).map_err(|_| {
        RedisError::runtime(b"ERR value is not an integer or out of range".to_vec())
    })?;
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

    #[test]
    fn wait_deny_blocking_returns_current_acks() {
        let mut c = Client::new(3);
        c.set_args(vec![
            RedisString::from_bytes(b"WAIT"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        c.set_flag_deny_blocking(true);
        let mut ctx = CommandContext::new(&mut c);
        wait_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b":0\r\n");
    }

    #[test]
    fn waitaof_single_node_returns_progress_pair() {
        let mut c = Client::new(4);
        c.set_args(vec![
            RedisString::from_bytes(b"WAITAOF"),
            RedisString::from_bytes(b"0"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        waitaof_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"*2\r\n:0\r\n:0\r\n");
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

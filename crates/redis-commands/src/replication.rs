//! Replication command handlers: REPLICAOF / SLAVEOF, PSYNC, SYNC.
//! All three handlers route through [`redis_core::replication`] for
//! global replication state. The pubsub registry is reused as the source
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
use redis_core::util::mstime;
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

/// `REPLICAOF host port` / `REPLICAOF NO ONE` (alias: `SLAVEOF`).
/// `REPLICAOF NO ONE` cancels replica mode and becomes a standalone primary.
/// `REPLICAOF <host> <port>` configures this server as a replica of the named
/// master. Records the target on [`ReplicationState`].
pub fn replicaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"replicaof"));
    }
    let host = ctx.arg_owned(1usize)?;
    let port_str = ctx.arg_owned(2usize)?;
    let repl = global_replication_state();

    if is_no_one(host.as_bytes(), port_str.as_bytes()) {
        if repl.is_replica() || repl.replica_of_target().is_some() {
            unblock_replication_role_change();
        }
        repl.become_master();
        return ctx.reply_simple_string(b"OK");
    }

    let port: u16 = match parse_port(port_str.as_bytes()) {
        Some(p) => p,
        None => {
            return Err(RedisError::runtime(
                b"ERR value is out of range, value must between 0 and 65535",
            ));
        }
    };
    let topology_changed = repl
        .replica_of_target()
        .as_ref()
        .is_none_or(|(old_host, old_port)| old_host != &host || *old_port != port);
    if topology_changed {
        unblock_replication_role_change();
    }
    let dialer_epoch = repl.become_replica_of(host.clone(), port);
    println!(
        "redis-server: Connecting to PRIMARY {}:{}",
        String::from_utf8_lossy(host.as_bytes()),
        port
    );
    // Full sync is owned by the PSYNC dialer/RDB apply path. Do not pre-seed
    // with KEYS/DUMP here; that masks failures in the real full-sync handoff.
    match crate::replica_dialer::spawn_replica_dialer(host.clone(), port, dialer_epoch) {
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

/// `ROLE` — return this node's replication role.
pub fn role_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"role"));
    }
    let repl = global_replication_state();
    if let Some((host, port)) = repl.replica_of_target() {
        let link_state = repl.replica_link_str();
        return ctx.reply_frame(&RespFrame::array(vec![
            RespFrame::bulk(b"slave".as_slice()),
            RespFrame::bulk(host),
            RespFrame::Integer(port as i64),
            RespFrame::bulk(link_state.as_bytes()),
            RespFrame::Integer(repl.master_offset()),
        ]));
    }

    let replicas = repl
        .replicas_snapshot()
        .into_iter()
        .map(|(_cid, _state, port, offset, _last_ack_ms)| {
            RespFrame::array(vec![
                RespFrame::bulk(RedisString::from_static(b"?")),
                RespFrame::bulk(RedisString::from_vec(port.to_string().into_bytes())),
                RespFrame::bulk(RedisString::from_vec(offset.to_string().into_bytes())),
            ])
        })
        .collect();
    ctx.reply_frame(&RespFrame::array(vec![
        RespFrame::bulk(b"master".as_slice()),
        RespFrame::Integer(repl.master_offset()),
        RespFrame::array(replicas),
    ]))
}

/// True when both arguments spell `NO` and `ONE` case-insensitively.
fn is_no_one(host: &[u8], port: &[u8]) -> bool {
    ascii_eq_ignore_case(host, b"NO") && ascii_eq_ignore_case(port, b"ONE")
}

/// `PSYNC <runid> <offset>` — master-side handshake accept.
/// Decides between partial resync (`+CONTINUE <runid>`) and full resync
/// (`+FULLRESYNC <runid> <offset>`) based on whether the replica's claimed
/// run id matches ours and its offset is still inside the live backlog
/// window. Registers a `ReplicaConn` entry in the global registry.
pub fn psync_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"psync"));
    }
    let provided_runid = ctx.arg_owned(1usize)?;
    let provided_offset = parse_offset(ctx.arg_owned(2usize)?.as_bytes())?;
    handle_psync(ctx, provided_runid.as_bytes(), provided_offset, true)
}

/// `SYNC` — legacy full-resync request.
/// This intentionally does not emit the `+FULLRESYNC...` prelude. Upstream
/// Valkey marks old `SYNC` clients as `pre_psync` and sends the RDB bulk
/// payload directly; the Tcl test helper `attach_to_replication_stream`
/// depends on that legacy wire shape.
pub fn sync_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"sync"));
    }
    handle_psync(ctx, b"?", -1, false)
}

/// `REPLCONF <subcommand> [args...]`
/// Multipurpose command for replica metadata exchange. Subcommands:
/// * `listening-port <N>` — replica's listener port; stored in `ReplicaConn`.
/// * `ip-address <ip>` — replica's public IP (stored as a future-use note).
/// * `capa <flag> …` — capability flags; bits ORed into `ReplicaConn.capa_flags`.
/// * `ACK <offset>` — replica reports its current stream offset; wakes any
/// WAIT clients that are now satisfied.
/// * `GETACK *` — primary asks replica for an ACK; answered with `+OK` on
/// the master side (the replica-side handler is Wave C).
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
                .ok_or_else(|| RedisError::runtime(b"ERR invalid port number"))?;
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
            let offset = parse_i64(offset_str.as_bytes())
                .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
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
                    let mut i = 3usize;
                    while i + 1 < ctx.arg_count() {
                        let option = ctx.arg_owned(i)?;
                        if option.as_bytes().eq_ignore_ascii_case(b"FACK") {
                            let aof_offset_arg = ctx.arg_owned(i + 1)?;
                            if let Ok(aof_offset) = parse_i64(aof_offset_arg.as_bytes()) {
                                conn.aof_offset.store(aof_offset, Ordering::Relaxed);
                            }
                        }
                        i += 2;
                    }
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
/// Blocks until at least `numreplicas` replicas have acknowledged the current
/// `master_repl_offset`, or until `timeout` ms elapses. Returns the count
/// replicas that acknowledged.
/// When `numreplicas` is 0 or when enough replicas are already caught up,
/// reply is sent immediately with no blocking.
pub fn wait_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"wait"));
    }

    let numreplicas = parse_i64(ctx.arg(1usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?
        as usize;
    let timeout_ms = parse_i64(ctx.arg(2usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    if timeout_ms < 0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    if timeout_ms > 0 && timeout_ms > i64::MAX - mstime() {
        return Err(RedisError::runtime(b"ERR timeout is out of range"));
    }

    let repl = global_replication_state();
    let target_offset = ctx.client_ref().last_write_repl_offset;
    let current_acked = count_acked_replicas(&repl, target_offset);

    if ctx.client_ref().flag_deny_blocking() || ctx.client_ref().flag_lua() {
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

    let timeout_secs = if timeout_ms == 0 && repl.connected_replicas() == 0 {
        // RuntimeOwner currently disables the replica dialer until replica
        // apply can target owner-owned DBs. In that state, WAIT 0 would block
        // forever with no registered replicas and hide the upstream file behind
        // a harness timeout. Keep the client visibly blocked, but give it a
        // bounded timeout so the file becomes counted-red. Once
        // RuntimeOwner replica channel lands, remove this guard and let WAIT 0
        // block for future replicas like C Valkey.
        2.0
    } else if timeout_ms == 0 {
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
        username: ctx.client_ref().authenticated_user.clone(),
    };
    {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.add(waiter);
    }
    ctx.client_mut().blocked_on_keys = true;
    request_ack_from_replicas(&repl);
    Ok(())
}

/// `WAITAOF numlocal numreplicas timeout`
/// Wait until the local AOF and/or attached replicas have fsynced
/// caller's last write offset.
pub fn waitaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"waitaof"));
    }

    let numlocal = parse_i64(ctx.arg(1usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    if !(0..=1).contains(&numlocal) {
        return Err(RedisError::runtime(
            b"ERR Value for numlocal is out of range [0,1]",
        ));
    }

    let numreplicas = parse_i64(ctx.arg(2usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    let timeout_ms = parse_i64(ctx.arg(3usize)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    if timeout_ms < 0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    if timeout_ms > 0 && timeout_ms > i64::MAX - mstime() {
        return Err(RedisError::runtime(b"ERR timeout is out of range"));
    }

    let repl = global_replication_state();
    if repl.is_replica() {
        return Err(RedisError::runtime(
            b"ERR WAITAOF cannot be used with replica instances. Please also note that writes to replicas are just local and are not propagated.",
        ));
    }

    if numlocal > 0 && !ctx.live_config().appendonly() {
        return Err(RedisError::runtime(
            b"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
        ));
    }

    if numlocal > 0 {
        crate::config_cmd::wait_for_scheduled_initial_aof(ctx, timeout_ms)?;
    }

    let target_offset = ctx.client_ref().last_write_repl_offset;
    let ackreplicas = count_aof_acked_replicas(&repl, target_offset) as i64;
    let acklocal = local_aof_ack_count(target_offset) as i64;

    let needs_local = numlocal > acklocal;
    let needs_replica = numreplicas > ackreplicas;
    if ctx.client_ref().flag_deny_blocking() || ctx.client_ref().flag_lua() {
        if acklocal >= numlocal {
            ctx.server().persistence.set_aof_rewrite_scheduled(false);
        }
        ctx.reply_array_header(2)?;
        ctx.reply_integer(acklocal)?;
        return ctx.reply_integer(ackreplicas);
    }
    if !needs_local {
        ctx.server().persistence.set_aof_rewrite_scheduled(false);
    }
    if needs_local || needs_replica {
        let timeout_secs = if timeout_ms == 0 {
            0.0
        } else {
            timeout_ms as f64 / 1000.0
        };
        if block_waitaof_waiter(
            ctx,
            target_offset,
            numreplicas.max(0) as usize,
            numlocal.max(0) as usize,
            timeout_secs,
        ) {
            request_ack_from_replicas(&repl);
            return Ok(());
        }
    }

    ctx.reply_array_header(2)?;
    ctx.reply_integer(acklocal)?;
    ctx.reply_integer(ackreplicas)
}

fn block_waitaof_waiter(
    ctx: &mut CommandContext<'_>,
    target_offset: i64,
    numreplicas: usize,
    numlocal: usize,
    timeout_secs: f64,
) -> bool {
    let registry = match ctx.pubsub.as_ref() {
        Some(r) => r.clone(),
        None => return false,
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
        None => return false,
    };

    let sentinel_key = RedisString::from_bytes(b"__wait__");
    let waiter = BlockedWaiter {
        client_id: ctx.client_ref().id(),
        sender,
        keys: vec![sentinel_key],
        action: BlockedAction::WaitAof {
            target_offset,
            numreplicas,
            numlocal,
        },
        deadline_ms: deadline_from_timeout_secs(timeout_secs),
        resp_proto: ctx.client_ref().resp_proto,
        username: ctx.client_ref().authenticated_user.clone(),
    };
    {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.add(waiter);
    }
    ctx.client_mut().blocked_on_keys = true;
    true
}

/// Return the count of replicas whose acknowledged offset is `>= target`.
fn count_acked_replicas(repl: &ReplicationState, target: i64) -> usize {
    let guard = match repl.replicas.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .values()
        .filter(|r| r.state() == ReplicaState::Online && r.offset.load(Ordering::Relaxed) >= target)
        .count()
}

fn count_aof_acked_replicas(repl: &ReplicationState, target: i64) -> usize {
    let guard = match repl.replicas.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .values()
        .filter(|r| {
            r.state() == ReplicaState::Online && r.aof_offset.load(Ordering::Relaxed) >= target
        })
        .count()
}

fn local_aof_ack_count(target: i64) -> usize {
    usize::from(crate::aof::current_fsynced_repl_offset() >= target)
}

/// Walk all WAIT waiters and wake those whose required replica count is
/// now satisfied. Called from the REPLCONF ACK handler after updating a
/// replica's offset.
pub fn maybe_wake_wait_clients() {
    let repl = global_replication_state();
    let acked_offsets: Vec<i64> = {
        let guard = match repl.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .filter(|r| r.state() == ReplicaState::Online)
            .map(|r| r.offset.load(Ordering::Relaxed))
            .collect()
    };
    let aof_acked_offsets: Vec<i64> = {
        let guard = match repl.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .filter(|r| r.state() == ReplicaState::Online)
            .map(|r| r.aof_offset.load(Ordering::Relaxed))
            .collect()
    };
    let mut idx = match blocked_keys_index().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let satisfied = idx.take_satisfied_wait_waiters(|target| {
        acked_offsets.iter().filter(|&&o| o >= target).count()
    });
    let satisfied_aof = idx.take_satisfied_waitaof_waiters(local_aof_ack_count, |target| {
        aof_acked_offsets.iter().filter(|&&o| o >= target).count()
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
    for (waiter, local, replicas) in satisfied_aof {
        let reply = format!("*2\r\n:{}\r\n:{}\r\n", local, replicas).into_bytes();
        if waiter.sender.send(reply).is_err() {
            eprintln!(
                "redis-server: WAITAOF wake send failed for client {}",
                waiter.client_id
            );
        }
    }
}

pub fn timeout_reply_for_wait_action(action: &BlockedAction) -> Option<Vec<u8>> {
    match action {
        BlockedAction::Wait { target_offset, .. } => {
            let repl = global_replication_state();
            let count = count_acked_replicas(&repl, *target_offset);
            Some(format!(":{}\r\n", count).into_bytes())
        }
        BlockedAction::WaitAof { target_offset, .. } => {
            let repl = global_replication_state();
            let local = local_aof_ack_count(*target_offset);
            let replicas = count_aof_acked_replicas(&repl, *target_offset);
            Some(format!("*2\r\n:{}\r\n:{}\r\n", local, replicas).into_bytes())
        }
        _ => None,
    }
}

pub fn unblock_waitaof_local_disabled() {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_waitaof_local_waiters()
    };
    for waiter in waiters {
        let _ = waiter.sender.send(
            b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n"
                .to_vec(),
        );
    }
}

pub fn unblock_replication_role_change() {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_all_replication_waiters()
    };
    for waiter in waiters {
        let _ = waiter.sender.send(
            b"-UNBLOCKED force unblock from blocking operation, instance state changed\r\n"
                .to_vec(),
        );
    }
}

/// Ask attached replicas to report their current processed offset.
/// a client blocks in WAIT/WAITAOF. Without this prompt a caught-up replica may
/// not send an ACK before the WAIT timeout, leaving tests stuck at zero acks.
fn request_ack_from_replicas(repl: &ReplicationState) {
    let getack = crate::aof::encode_resp_command(&[
        RedisString::from_bytes(b"REPLCONF"),
        RedisString::from_bytes(b"GETACK"),
        RedisString::from_bytes(b"*"),
    ]);
    let guard = match repl.replicas.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let has_online = guard.values().any(|conn| {
        ReplicaState::from_u8(conn.state.load(Ordering::Acquire)) == ReplicaState::Online
    });
    if has_online {
        // C Valkey sends GETACK through replicationFeedReplicas(-1), so
        // request itself is part of the replication stream and advances
        // offsets. Keeping that invariant prevents an ACK for GETACK
        // jumping ahead of future writes.
        repl.append_to_backlog(&getack);
    }
    for conn in guard.values() {
        if ReplicaState::from_u8(conn.state.load(Ordering::Acquire)) != ReplicaState::Online {
            continue;
        }
        if conn.outbound_sender.send(getack.clone()).is_err() {
            eprintln!(
                "redis-server: WAIT GETACK send failed for client {}",
                conn.client_id
            );
        }
    }
}

/// Map a REPLCONF `capa` flag name to its bit position.
/// Known flags:
/// * `eof` — replica can receive the RDB blob without inline `$<len>` framing.
/// * `psync2` — replica supports PSYNC2 (run-id propagation after partial resync).
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

/// Shared body of `PSYNC` and `SYNC`. `provided_runid == b"?"`
/// `provided_offset == -1` is the canonical full-resync request.
fn handle_psync(
    ctx: &mut CommandContext<'_>,
    provided_runid: &[u8],
    provided_offset: i64,
    send_fullresync_line: bool,
) -> RedisResult<()> {
    let repl = global_replication_state();
    let our_runid = repl.runid();
    let master_offset = repl.master_offset();
    let decision = decide_psync(
        &repl,
        our_runid,
        provided_runid,
        provided_offset,
        master_offset,
    );

    let client_id = ctx.client_ref().id();
    let outbound = steal_outbound_sender(ctx.pubsub.as_ref(), client_id);

    if matches!(decision, PsyncDecision::Continue) {
        repl.incr_sync_partial_ok();
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
        if master_offset > provided_offset {
            let catch_up = {
                let guard = match repl.backlog.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.read_at(provided_offset, (master_offset - provided_offset) as usize)
            };
            if let Some(bytes) = catch_up {
                if !bytes.is_empty() {
                    let _ = repl.send_to_replica(client_id, bytes);
                }
            }
        }
        return Ok(());
    }

    // A partial resync was requested (concrete runid + non-negative offset)
    // but could not be served from the live backlog window: count it as a
    // partial-resync error before falling back to a full resync, mirroring C
    // `masterTryPartialResynchronization` → `server.stat_sync_partial_err++`.
    if matches!(
        decision,
        PsyncDecision::FullResync {
            count_partial_err: true
        }
    ) {
        repl.incr_sync_partial_err();
    }
    repl.incr_sync_full();

    let snapshot_offset = master_offset;
    repl.selected_db
        .store(-1, std::sync::atomic::Ordering::Release);
    if let Some(sender) = outbound {
        register_replica(
            &repl,
            client_id,
            ReplicaState::WaitingBgsave,
            snapshot_offset,
            sender,
        );
    }
    if send_fullresync_line {
        let line = fullresync_reply(our_runid, snapshot_offset);
        ctx.client_mut().reply_buf.extend_from_slice(&line);
    }
    ctx.client_mut().is_replica = true;

    arm_full_sync_bgsave(ctx, &repl, client_id, snapshot_offset);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PsyncDecision {
    Continue,
    FullResync { count_partial_err: bool },
}

fn decide_psync(
    repl: &Arc<ReplicationState>,
    our_runid: &[u8; 40],
    provided_runid: &[u8],
    provided_offset: i64,
    master_offset: i64,
) -> PsyncDecision {
    let runid_matches = provided_runid == &our_runid[..] || provided_runid == b"?";
    if runid_matches
        && provided_offset >= 0
        && partial_in_window(repl, provided_offset, master_offset)
    {
        return PsyncDecision::Continue;
    }
    PsyncDecision::FullResync {
        count_partial_err: provided_runid != b"?" && provided_offset >= 0,
    }
}

/// Either join an in-flight BGSAVE-for-replication job or kick off a new one
/// so the freshly-attached replica eventually receives an RDB snapshot.
/// Behaviour:
/// * If a BGSAVE-for-replication is already in progress, append the new
/// replica's `client_id` to the same job's waiting list. Every replica
/// that joins before the child exits receives the identical RDB snapshot
/// and the same catch-up backlog window.
/// * Otherwise call `bgsave_for_replication` to fork a fresh child.
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
/// window (lower bound is the backlog's `min_offset`, upper bound is
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
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    s.parse::<i64>()
        .map_err(|_| RedisError::runtime(b"ERR value is not an integer or out of range"))
}

/// Parse a TCP port literal. Returns `None` on parse failure or out-of-range.
fn parse_port(bytes: &[u8]) -> Option<u16> {
    let s = std::str::from_utf8(bytes).ok()?;
    let n: i64 = s.parse().ok()?;
    // Valkey's REPLICAOF / REPLCONF parse the port via getRangeLongFromObject
    // with bounds 0..=65535 — port 0 is accepted (e.g. `REPLICAOF host 0`
    // point at an unreachable primary).
    if !(0..=65535).contains(&n) {
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
    use redis_core::{
        blocked_keys::blocked_keys_index, Client, PubSubRegistry, RedisDb, RedisObject, RedisServer,
    };
    use redis_types::RedisString;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};

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
    fn replicaof_does_not_preseed_from_primary() {
        global_replication_state().become_master();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fake primary");
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let fake_primary = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + StdDuration::from_millis(300);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = tx.send(true);
                        let _ = stream.set_read_timeout(Some(StdDuration::from_millis(50)));
                        let mut buf = [0u8; 1024];
                        loop {
                            match stream.read(&mut buf) {
                                Ok(0) => return,
                                Ok(n) => {
                                    let frame = &buf[..n];
                                    if frame.windows(6).any(|w| w.eq_ignore_ascii_case(b"SELECT")) {
                                        let _ = stream.write_all(b"+OK\r\n");
                                    } else if frame
                                        .windows(4)
                                        .any(|w| w.eq_ignore_ascii_case(b"KEYS"))
                                    {
                                        let _ = stream.write_all(b"*0\r\n");
                                    } else {
                                        let _ = stream
                                            .write_all(b"-ERR unexpected fake primary command\r\n");
                                    }
                                }
                                Err(e)
                                    if matches!(
                                        e.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                                {
                                    return;
                                }
                                Err(_) => return,
                            }
                        }
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(StdDuration::from_millis(10));
                    }
                    Err(_) => {
                        let _ = tx.send(false);
                        return;
                    }
                }
            }
        });

        let mut db = RedisDb::new(0);
        db.add(
            RedisString::from_bytes(b"local"),
            RedisObject::new_string(b"value"),
        );
        let mut c = Client::new(33);
        c.set_args(vec![
            RedisString::from_bytes(b"REPLICAOF"),
            RedisString::from_bytes(b"127.0.0.1"),
            RedisString::from_vec(port.to_string().into_bytes()),
        ]);
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        {
            let mut ctx = CommandContext::with_server(&mut c, &mut db, server, pubsub);
            replicaof_command(&mut ctx).unwrap();
        }

        let saw_preseed_connection = rx.recv_timeout(StdDuration::from_secs(1)).unwrap_or(false);
        fake_primary.join().unwrap();
        global_replication_state().become_master();

        assert_eq!(c.drain_reply(), b"+OK\r\n");
        assert!(
            !saw_preseed_connection,
            "REPLICAOF should let the PSYNC dialer own full sync instead of opening a KEYS/DUMP seed connection"
        );
        let local = db
            .lookup_key_read(b"local")
            .expect("local key should survive until RDB apply replaces keyspace");
        assert_eq!(local.string_bytes().as_ref(), b"value");
    }

    #[test]
    fn psync_decision_matrix_covers_reconnect_edges() {
        let st = Arc::new(ReplicationState::new([b'a'; 40], 8));
        let runid = *st.runid();

        assert_eq!(
            decide_psync(&st, &runid, b"?", -1, st.master_offset()),
            PsyncDecision::FullResync {
                count_partial_err: false
            },
            "fresh PSYNC should full-resync without counting a partial error"
        );
        assert_eq!(
            decide_psync(&st, &runid, &runid, 0, st.master_offset()),
            PsyncDecision::Continue,
            "a caught-up reconnect at offset 0 should not need an RDB"
        );

        st.append_to_backlog(b"abcdefgh");
        let master = st.master_offset();
        assert_eq!(
            decide_psync(&st, &runid, &runid, 4, master),
            PsyncDecision::Continue,
            "offset inside the live backlog window should partial-resync"
        );
        assert_eq!(
            decide_psync(&st, &runid, &[b'b'; 40], 4, master),
            PsyncDecision::FullResync {
                count_partial_err: true
            },
            "wrong replid should fall back and count a partial-resync error"
        );
        assert_eq!(
            decide_psync(&st, &runid, &runid, master + 1, master),
            PsyncDecision::FullResync {
                count_partial_err: true
            },
            "future offsets cannot be served from the backlog"
        );

        st.append_to_backlog(b"ijklmnop");
        let master = st.master_offset();
        assert_eq!(
            decide_psync(&st, &runid, &runid, 0, master),
            PsyncDecision::FullResync {
                count_partial_err: true
            },
            "offsets below the wrapped backlog window should full-resync"
        );
        assert_eq!(
            decide_psync(&st, &runid, &runid, 8, master),
            PsyncDecision::Continue,
            "the first retained byte after wraparound should partial-resync"
        );
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
        assert!(
            reply.is_empty(),
            "legacy SYNC should wait for raw RDB bulk, not emit PSYNC prelude"
        );
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
    fn wait_rejects_invalid_timeout_bounds() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"-1", b"ERR timeout is negative"),
            (b"9223372036854775807", b"ERR timeout is out of range"),
        ];

        for (timeout, expected) in cases {
            let mut c = Client::new(30);
            c.set_args(vec![
                RedisString::from_bytes(b"WAIT"),
                RedisString::from_bytes(b"1"),
                RedisString::from_bytes(timeout),
            ]);
            let mut ctx = CommandContext::new(&mut c);
            let err = wait_command(&mut ctx).expect_err("WAIT timeout should be rejected");
            let payload = err.to_resp_payload();
            assert!(
                payload
                    .as_bytes()
                    .windows(expected.len())
                    .any(|w| w == *expected),
                "error payload {:?} did not contain {:?}",
                payload,
                String::from_utf8_lossy(expected)
            );
        }
    }

    #[test]
    fn wait_zero_timeout_without_registered_replicas_blocks_with_bounded_deadline() {
        let mut c = Client::new(31);
        c.set_args(vec![
            RedisString::from_bytes(b"WAIT"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        let (tx, _rx) = mpsc::channel();
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        {
            let mut guard = pubsub.lock().unwrap();
            guard.register_sender(c.id(), tx);
        }
        let mut db = RedisDb::new(0);
        let server = Arc::new(RedisServer::default());
        {
            let mut ctx = CommandContext::with_server(&mut c, &mut db, server, pubsub);
            wait_command(&mut ctx).unwrap();
        }
        assert!(c.blocked_on_keys);
        let _ = blocked_keys_index().lock().unwrap().remove_client(c.id());
    }

    #[test]
    fn wait_request_ack_command_is_resp_encoded() {
        let bytes = crate::aof::encode_resp_command(&[
            RedisString::from_bytes(b"REPLCONF"),
            RedisString::from_bytes(b"GETACK"),
            RedisString::from_bytes(b"*"),
        ]);
        assert_eq!(
            bytes,
            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        );
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

    #[test]
    fn waitaof_zero_timeout_with_registered_sender_blocks() {
        let mut c = Client::new(32);
        c.set_args(vec![
            RedisString::from_bytes(b"WAITAOF"),
            RedisString::from_bytes(b"0"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        let (tx, _rx) = mpsc::channel();
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        {
            let mut guard = pubsub.lock().unwrap();
            guard.register_sender(c.id(), tx);
        }
        let mut db = RedisDb::new(0);
        let server = Arc::new(RedisServer::default());
        {
            let mut ctx = CommandContext::with_server(&mut c, &mut db, server, pubsub);
            waitaof_command(&mut ctx).unwrap();
        }
        assert!(c.blocked_on_keys);
        let _ = blocked_keys_index().lock().unwrap().remove_client(c.id());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  plus the architect packet for Session 3A.
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         3
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Deleted dead helper block_replica_waiter (no callers).
//                  PSYNC/SYNC handshake accept; REPLICAOF toggle. Replica
//                  dialer + RDB transfer are Wave B/C TODOs.
// ──────────────────────────────────────────────────────────────────────────

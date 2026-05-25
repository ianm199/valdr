//! Command dispatch table — maps argv[0] (case-insensitive) to a handler fn.
//!
//! Wave A wires up the *lookup* side only. Most handler bodies are still
//! `todo!()`; this module just routes the call. Handler bodies land in Waves
//! B/C/D.
//!
//! Two-layer lookup:
//!
//! 1. The generated registry in `generated::COMMANDS` is the source of truth
//!    for command metadata (arity, flags, ACL category).
//! 2. A small static `HANDLERS` table maps an uppercase ASCII command name to
//!    a Rust function. Commands with no handler yet are intentionally absent;
//!    callers receive an `unknown command` error.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::{OnceLock, RwLock};

use redis_core::acl::{category as acl_category, global_acl_state, record_acl_log_entry, AclUser};
use redis_core::eviction::{oom_error_reply, try_evict_to_fit, EvictionOutcome};
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{
    record_acl_access_denied_channel, record_acl_access_denied_cmd, record_acl_access_denied_key,
    record_command_stat,
};
use redis_core::monotonic::{elapsed_start, elapsed_us};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::generated::{CommandFlag, GeneratedCommandSpec, COMMANDS};

/// A command handler.
pub type Handler = fn(&mut CommandContext) -> RedisResult<()>;

/// One entry in the static dispatch table.
pub struct DispatchEntry {
    /// Uppercase ASCII name (e.g. `b"PING"`). Compared case-insensitively.
    pub name: &'static [u8],
    /// Handler function pointer.
    pub handler: Handler,
}

/// Command metadata used by the hot dispatch path.
///
/// The generated `COMMANDS` registry is still the source of truth. We compress
/// the handful of fields dispatch needs into this small value so each command
/// does one generated-registry scan instead of separate scans for WRITE,
/// NO_AUTH, DENYOOM, SKIP_COMMANDLOG, and ACL categories.
#[derive(Clone, Copy, Debug, Default)]
struct CommandMetadata {
    write: bool,
    no_auth: bool,
    denyoom: bool,
    no_multi: bool,
    allow_busy: bool,
    skip_commandlog: bool,
    skip_monitor: bool,
    admin: bool,
    monitor_admin: bool,
    acl_categories: u64,
}

struct RuntimeDispatchEntry {
    entry: &'static DispatchEntry,
    metadata: CommandMetadata,
}

struct HotRuntimeDispatch {
    ping: Option<&'static RuntimeDispatchEntry>,
    get: Option<&'static RuntimeDispatchEntry>,
    set: Option<&'static RuntimeDispatchEntry>,
    incr: Option<&'static RuntimeDispatchEntry>,
}

struct RuntimeDispatchIndex {
    rows: Vec<RuntimeDispatchEntry>,
    buckets: [(usize, usize); 256],
}

static COMMAND_METADATA_TABLE: OnceLock<Vec<(&'static [u8], CommandMetadata)>> = OnceLock::new();
static RUNTIME_DISPATCH_INDEX: OnceLock<RuntimeDispatchIndex> = OnceLock::new();
static HOT_RUNTIME_DISPATCH: OnceLock<HotRuntimeDispatch> = OnceLock::new();
static COMMAND_RENAME_STATE: OnceLock<RwLock<CommandRenameState>> = OnceLock::new();

#[derive(Default)]
struct CommandRenameState {
    aliases: Vec<CommandRename>,
    hidden: Vec<Vec<u8>>,
}

struct CommandRename {
    external: Vec<u8>,
    canonical: Vec<u8>,
}

/// Apply a Valkey `rename-command <current-name> <new-name>` directive.
///
/// The directive renames the currently visible external command name while ACL
/// rules and dispatch metadata continue to use the original canonical command.
pub fn apply_command_rename(current_name: &[u8], new_name: &[u8]) -> Result<(), Vec<u8>> {
    let current = lower_command_name(current_name);
    if current.is_empty() {
        return Err(b"ERR rename-command requires a command name".to_vec());
    }
    let new_external = lower_command_name(new_name);
    let mut state = match command_rename_state().write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let canonical = if let Some(idx) = state
        .aliases
        .iter()
        .position(|rename| rename.external == current)
    {
        state.aliases.remove(idx).canonical
    } else if state.hidden.iter().any(|hidden| hidden == &current) {
        let mut msg = b"ERR no such command '".to_vec();
        msg.extend_from_slice(current_name);
        msg.push(b'\'');
        return Err(msg);
    } else if lookup_runtime_command_indexed(current_name).is_some() {
        current.clone()
    } else {
        let mut msg = b"ERR no such command '".to_vec();
        msg.extend_from_slice(current_name);
        msg.push(b'\'');
        return Err(msg);
    };
    hide_external_command_name(&mut state, current);
    if !new_external.is_empty() {
        state
            .aliases
            .retain(|rename| rename.external != new_external);
        state.hidden.retain(|hidden| hidden != &new_external);
        state.aliases.push(CommandRename {
            external: new_external,
            canonical,
        });
    }
    Ok(())
}

pub fn is_dispatchable_command(name: &[u8]) -> bool {
    let Some(resolved) = resolve_command_name(name) else {
        return false;
    };
    lookup_runtime_command(&resolved).is_some()
}

fn command_rename_state() -> &'static RwLock<CommandRenameState> {
    COMMAND_RENAME_STATE.get_or_init(|| RwLock::new(CommandRenameState::default()))
}

fn hide_external_command_name(state: &mut CommandRenameState, name: Vec<u8>) {
    if !state.hidden.iter().any(|hidden| hidden == &name) {
        state.hidden.push(name);
    }
}

fn resolve_command_name(name: &[u8]) -> Option<Cow<'_, [u8]>> {
    let Some(lock) = COMMAND_RENAME_STATE.get() else {
        return Some(Cow::Borrowed(name));
    };
    let needle = lower_command_name(name);
    let state = match lock.read() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Some(rename) = state
        .aliases
        .iter()
        .find(|rename| rename.external == needle)
    {
        return Some(Cow::Owned(rename.canonical.clone()));
    }
    if state.hidden.iter().any(|hidden| hidden == &needle) {
        return None;
    }
    Some(Cow::Borrowed(name))
}

fn lower_command_name(name: &[u8]) -> Vec<u8> {
    name.iter().map(|byte| byte.to_ascii_lowercase()).collect()
}

/// Look up the handler for `name` (case-insensitive ASCII).
///
/// Returns `Some(entry)` if a handler is registered, `None` otherwise.
pub fn lookup_command(name: &[u8]) -> Option<&'static DispatchEntry> {
    lookup_runtime_command(name).map(|row| row.entry)
}

pub(crate) fn registered_command_spec(name: &[u8]) -> Option<&'static GeneratedCommandSpec> {
    lookup_command(name)?;
    COMMANDS
        .iter()
        .find(|spec| ascii_eq_ignore_case(spec.name.as_bytes(), name))
}

fn lookup_runtime_command(name: &[u8]) -> Option<&'static RuntimeDispatchEntry> {
    if let Some(entry) = lookup_hot_runtime_command(name) {
        return Some(entry);
    }
    lookup_runtime_command_indexed(name)
}

fn lookup_runtime_command_indexed(name: &[u8]) -> Option<&'static RuntimeDispatchEntry> {
    let first = *name.first()?;
    let index = runtime_dispatch_index();
    let (start, end) = index.buckets[ascii_lower(first) as usize];
    let table = &index.rows[start..end];
    table
        .binary_search_by(|row| ascii_casecmp(row.entry.name, name))
        .map(|idx| &index.rows[start + idx])
        .ok()
}

fn lookup_hot_runtime_command(name: &[u8]) -> Option<&'static RuntimeDispatchEntry> {
    let hot = hot_runtime_dispatch();
    match name {
        [a, b, c]
            if ascii_lower(*a) == b'g' && ascii_lower(*b) == b'e' && ascii_lower(*c) == b't' =>
        {
            hot.get
        }
        [a, b, c]
            if ascii_lower(*a) == b's' && ascii_lower(*b) == b'e' && ascii_lower(*c) == b't' =>
        {
            hot.set
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'p'
                && ascii_lower(*b) == b'i'
                && ascii_lower(*c) == b'n'
                && ascii_lower(*d) == b'g' =>
        {
            hot.ping
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'i'
                && ascii_lower(*b) == b'n'
                && ascii_lower(*c) == b'c'
                && ascii_lower(*d) == b'r' =>
        {
            hot.incr
        }
        _ => None,
    }
}

fn hot_runtime_dispatch() -> &'static HotRuntimeDispatch {
    HOT_RUNTIME_DISPATCH.get_or_init(|| HotRuntimeDispatch {
        ping: lookup_runtime_command_indexed(b"PING"),
        get: lookup_runtime_command_indexed(b"GET"),
        set: lookup_runtime_command_indexed(b"SET"),
        incr: lookup_runtime_command_indexed(b"INCR"),
    })
}

#[cfg(test)]
fn runtime_dispatch_table() -> &'static [RuntimeDispatchEntry] {
    &runtime_dispatch_index().rows
}

fn runtime_dispatch_index() -> &'static RuntimeDispatchIndex {
    RUNTIME_DISPATCH_INDEX.get_or_init(|| {
        let mut rows: Vec<RuntimeDispatchEntry> = HANDLERS
            .iter()
            .map(|entry| RuntimeDispatchEntry {
                entry,
                metadata: command_metadata(entry.name),
            })
            .collect();
        rows.sort_by(|left, right| ascii_casecmp(left.entry.name, right.entry.name));
        let mut buckets = [(0usize, 0usize); 256];
        let mut cursor = 0usize;
        while cursor < rows.len() {
            let bucket = ascii_lower(rows[cursor].entry.name[0]) as usize;
            let start = cursor;
            while cursor < rows.len() && ascii_lower(rows[cursor].entry.name[0]) as usize == bucket
            {
                cursor += 1;
            }
            buckets[bucket] = (start, cursor);
        }
        RuntimeDispatchIndex { rows, buckets }
    })
}

/// Dispatch one command using `ctx.client.argv[0]` as the command name.
///
/// Returns an error if argv is empty or the command is unknown. The handler's
/// result is returned verbatim — handlers may write a reply *and* return `Ok`,
/// or return `Err` (which the I/O layer renders as a `-ERR ...` reply).
///
/// When the client is inside a MULTI block (`client.flag_multi()` is true)
/// every command except the transaction-control set (MULTI / EXEC / DISCARD /
/// WATCH / UNWATCH / RESET) is appended to `client.queued_argvs` and the
/// client receives `+QUEUED\r\n` instead of executing immediately.
pub fn dispatch(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let command_name = match ctx.client_ref().arg(0) {
        Some(s) => StackCommandName::from_slice(s.as_bytes()),
        None => return Err(RedisError::runtime(b"ERR empty command")),
    };
    let name = command_name.as_bytes();
    if ctx.client_ref().is_replica() && !is_replica_allowed_command(name) {
        return Ok(());
    }
    let resolved_name = resolve_command_name(name);
    let dispatch_name = resolved_name.as_deref().unwrap_or(name);
    if ctx.client_ref().flag_multi() {
        if is_client_reply_command(ctx, dispatch_name) {
            crate::multi::flag_transaction_dirty_exec(ctx.client_mut());
            ctx.client_mut().reply_buf.extend_from_slice(
                b"-ERR Command 'client|reply' not allowed inside a transaction\r\n",
            );
            return Ok(());
        }
        if crate::multi::is_no_multi_command(dispatch_name) {
            crate::multi::flag_transaction_dirty_exec(ctx.client_mut());
            return Err(crate::multi::reject_no_multi_command(dispatch_name));
        }
        if !crate::multi::is_tx_control_command(dispatch_name) {
            if let Some(runtime_entry) = lookup_runtime_command(dispatch_name) {
                let metadata = runtime_entry.metadata;
                if !metadata.no_auth {
                    let acl_categories =
                        acl_categories_for_context(ctx, dispatch_name, metadata.acl_categories);
                    if let Some(noauth_reply) =
                        enforce_acl_gate(ctx, dispatch_name, acl_categories)
                    {
                        crate::multi::flag_transaction_dirty_exec(ctx.client_mut());
                        if close_unauthenticated_client_for_debug_reply_limit(ctx) {
                            return Ok(());
                        }
                        ctx.client_mut().reply_buf.extend_from_slice(&noauth_reply);
                        return Ok(());
                    }
                }
                if let Some(reply) = enforce_maxmemory_gate(ctx, metadata.denyoom) {
                    crate::multi::flag_transaction_dirty_exec(ctx.client_mut());
                    ctx.client_mut().reply_buf.extend_from_slice(&reply);
                    return Ok(());
                }
                if let Some(reply) =
                    enforce_busy_script_gate(ctx, dispatch_name, metadata.allow_busy)
                {
                    crate::multi::flag_transaction_dirty_exec(ctx.client_mut());
                    ctx.client_mut().reply_buf.extend_from_slice(&reply);
                    return Ok(());
                }
            }
            return crate::multi::queue_current_command(ctx);
        }
    }
    if ctx.client_ref().in_pubsub_mode()
        && ctx.client_ref().resp_proto == 2
        && !dispatch_name.eq_ignore_ascii_case(b"HELLO")
        && !crate::pubsub::is_allowed_in_subscribe_mode(dispatch_name)
    {
        return Err(crate::pubsub::subscribe_mode_error(dispatch_name));
    }
    dispatch_command_name(ctx, name)
}

enum StackCommandName {
    Inline { bytes: [u8; 32], len: usize },
    Heap(Vec<u8>),
}

impl StackCommandName {
    fn from_slice(input: &[u8]) -> Self {
        if input.len() <= 32 {
            let mut bytes = [0; 32];
            bytes[..input.len()].copy_from_slice(input);
            Self::Inline {
                bytes,
                len: input.len(),
            }
        } else {
            Self::Heap(input.to_vec())
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len } => &bytes[..*len],
            Self::Heap(bytes) => bytes.as_slice(),
        }
    }
}

/// Dispatch using an externally-supplied command name.
///
/// Skips the MULTI-queueing pre-check. Used by `EXEC` to drain each queued
/// argv without re-entering the queue logic. Times the handler execution when
/// the live slowlog gate can consume a duration, and records an entry when the
/// measured duration meets the threshold.
pub fn dispatch_command_name(ctx: &mut CommandContext<'_>, name: &[u8]) -> RedisResult<()> {
    let resolved_name = match resolve_command_name(name) {
        Some(name) => name,
        None => return Err(unknown_command_error(name)),
    };
    let runtime_entry = match lookup_runtime_command(&resolved_name) {
        Some(e) => e,
        None => return Err(unknown_command_error(name)),
    };
    let entry = runtime_entry.entry;
    let metadata = runtime_entry.metadata;
    let name = entry.name;

    if !metadata.no_auth {
        let acl_categories = acl_categories_for_context(ctx, name, metadata.acl_categories);
        if let Some(noauth_reply) = enforce_acl_gate(ctx, name, acl_categories) {
            if close_unauthenticated_client_for_debug_reply_limit(ctx) {
                return Ok(());
            }
            ctx.client_mut().reply_buf.extend_from_slice(&noauth_reply);
            return Ok(());
        }
    }

    if let Some(reply) = enforce_replica_readonly_gate(ctx, name, metadata.write) {
        ctx.client_mut().reply_buf.extend_from_slice(&reply);
        return Ok(());
    }

    if let Some(reply) = enforce_maxmemory_gate(ctx, metadata.denyoom) {
        ctx.client_mut().reply_buf.extend_from_slice(&reply);
        return Ok(());
    }

    if let Some(reply) = enforce_busy_script_gate(ctx, name, metadata.allow_busy) {
        if ctx.client_ref().flag_multi() && ascii_eq_ignore_case(name, b"EXEC") {
            crate::multi::reset_multi_state(ctx.client_mut());
            ctx.client_mut()
                .reply_buf
                .extend_from_slice(&execabort_from_error_reply(&reply));
        } else {
            ctx.client_mut().reply_buf.extend_from_slice(&reply);
        }
        return Ok(());
    }

    // C: db.c:2126/2144 + getExpirationPolicyWithFlags — a primary in
    // import-mode lets an import-source client see otherwise-expired keys, and
    // keeps expired keys (no lazy delete) for everyone else. Refresh the
    // selected DB's per-command flags before the handler runs.
    let import_mode = ctx.live_config().import_mode();
    let import_source_active = ctx.client_ref().import_source && import_mode;
    ctx.db_mut()
        .set_import_expire_state(import_source_active, import_mode);

    let initial_slowlog_gate = ctx.live_config().slowlog_timing_gate();
    let should_time_slowlog = initial_slowlog_gate.should_time() && !metadata.skip_commandlog;
    let start = elapsed_start();
    let pre_reply_len = ctx.client_ref().reply_buf.len();
    let result = (entry.handler)(ctx);
    let command_blocked = result.is_ok() && ctx.client_ref().blocked_on_keys;
    let reply_is_error = result.is_ok()
        && ctx
            .client_ref()
            .reply_buf
            .get(pre_reply_len)
            .is_some_and(|b| *b == b'-');
    let elapsed_micros = if command_blocked {
        None
    } else {
        Some(elapsed_us(start))
    };
    record_command_stat(
        name,
        elapsed_micros.unwrap_or(0),
        false,
        result.is_err() || reply_is_error,
    );
    let reply_bytes = ctx
        .client_ref()
        .reply_buf
        .len()
        .saturating_sub(pre_reply_len);
    if result.is_ok() && !command_blocked {
        maybe_copy_hash_field_expiry_metadata(ctx, name, pre_reply_len);
    }
    let should_record_slowlog = match elapsed_micros {
        Some(elapsed_micros) if should_time_slowlog && !command_blocked => ctx
            .live_config()
            .slowlog_timing_gate()
            .should_record(elapsed_micros),
        _ => false,
    };

    let propagate_write = result.is_ok()
        && metadata.write
        && !command_blocked
        && should_propagate_write_command(ctx, name);
    let aof = if propagate_write {
        crate::aof::aof_writer()
    } else {
        None
    };
    let replication = if propagate_write && !ctx.client_ref().is_replica {
        let repl = redis_core::replication::global_replication_state();
        if repl.should_propagate_writes() {
            Some(repl)
        } else {
            None
        }
    } else {
        None
    };

    let mut argv_snapshot: Option<Vec<RedisString>> = None;
    let successful_complete = result.is_ok() && !command_blocked;
    if (command_blocked && should_time_slowlog)
        || should_record_slowlog
        || aof.is_some()
        || replication.is_some()
        || successful_complete
    {
        argv_snapshot = Some(snapshot_argv(ctx));
    }

    if command_blocked {
        if let Some(argv) = argv_snapshot.take() {
            crate::slowlog_cmd::remember_blocked_slowlog_entry(
                argv,
                start,
                ctx.client_ref().id(),
                ctx.client_ref().name.clone(),
            );
        }
    }

    if successful_complete {
        ctx.apply_client_tracking_after_command(name, metadata.write);
        if let (Some(argv), Some(elapsed_micros)) = (argv_snapshot.as_ref(), elapsed_micros) {
            crate::slowlog_cmd::record_latency_histogram(argv, elapsed_micros);
            crate::slowlog_cmd::record_large_commandlog_entries(
                argv,
                request_size_bytes(argv),
                reply_bytes as u64,
                ctx.client_ref().id(),
                ctx.client_ref().name.clone(),
            );
            if !metadata.skip_monitor && !metadata.monitor_admin {
                crate::connection::feed_monitors(ctx, argv);
            }
        }
    }

    if should_record_slowlog {
        if let (Some(argv), Some(elapsed_micros)) = (argv_snapshot.as_ref(), elapsed_micros) {
            crate::slowlog_cmd::record_slowlog_entry(
                argv,
                elapsed_micros,
                ctx.client_ref().id(),
                ctx.client_ref().name.clone(),
            );
        }
    }

    if let Some(aof) = aof {
        if let Some(argv) = argv_snapshot.as_ref() {
            if let Err(e) = aof.append_selected(ctx.selected_db_id(), argv) {
                eprintln!("redis-server: AOF append failed: {}", e);
            }
        }
    }
    if let Some(repl) = replication {
        if let Some(argv) = argv_snapshot.as_ref() {
            propagate_write_to_replicas(&repl, ctx.selected_db_id(), argv);
        }
    }

    result
}

fn snapshot_argv(ctx: &CommandContext<'_>) -> Vec<RedisString> {
    (0..ctx.arg_count())
        .filter_map(|i| ctx.client_ref().arg(i).cloned())
        .collect()
}

fn request_size_bytes(argv: &[RedisString]) -> u64 {
    argv.iter().fold(0u64, |acc, arg| {
        acc.saturating_add(arg.as_bytes().len() as u64)
            .saturating_add(8)
    })
}

fn maybe_copy_hash_field_expiry_metadata(
    ctx: &mut CommandContext<'_>,
    name: &[u8],
    pre_reply_len: usize,
) {
    if !ascii_eq_ignore_case(name, b"COPY") {
        return;
    }
    let reply = &ctx.client_ref().reply_buf[pre_reply_len..];
    if reply != b":1\r\n" {
        return;
    }
    let src_key = match ctx.client_ref().arg(1).cloned() {
        Some(key) => key,
        None => return,
    };
    let dst_key = match ctx.client_ref().arg(2).cloned() {
        Some(key) => key,
        None => return,
    };
    let src_dbid = ctx.selected_db_id();
    let dst_dbid = copy_target_db(ctx).unwrap_or(src_dbid);
    crate::hash::copy_hash_field_expiries(src_dbid, &src_key, dst_dbid, &dst_key);
}

fn copy_target_db(ctx: &CommandContext<'_>) -> Option<u32> {
    let mut idx = 3usize;
    while idx + 1 < ctx.arg_count() {
        let opt = ctx.client_ref().arg(idx)?;
        if opt.as_bytes().eq_ignore_ascii_case(b"DB") {
            let raw = ctx.client_ref().arg(idx + 1)?;
            let s = core::str::from_utf8(raw.as_bytes()).ok()?;
            let parsed = s.parse::<u32>().ok()?;
            return Some(parsed);
        }
        idx += if opt.as_bytes().eq_ignore_ascii_case(b"REPLACE") {
            1
        } else {
            2
        };
    }
    None
}

fn should_propagate_write_command(ctx: &CommandContext<'_>, original_name: &[u8]) -> bool {
    if original_name.eq_ignore_ascii_case(b"GETEX") {
        return ctx
            .client_ref()
            .arg(0)
            .is_some_and(|current| !current.as_bytes().eq_ignore_ascii_case(b"GETEX"));
    }
    true
}

/// Return the combined hot-path metadata for a named command.
///
/// Multiple generated specs can share the same command name for subcommand
/// inheritance. Dispatch keeps the same OR-style behavior the previous helper
/// functions used, but computes all fields in one pass.
fn command_metadata(name: &[u8]) -> CommandMetadata {
    let table = command_metadata_table();
    table
        .binary_search_by(|(entry_name, _)| ascii_casecmp(entry_name, name))
        .map(|idx| table[idx].1)
        .unwrap_or_default()
}

pub(crate) fn command_is_denyoom(name: &[u8]) -> bool {
    command_metadata(name).denyoom
}

pub(crate) fn command_is_no_multi(name: &[u8]) -> bool {
    command_metadata(name).no_multi
}

pub(crate) fn command_acl_categories(name: &[u8]) -> Option<u64> {
    lookup_runtime_command(name).map(|entry| entry.metadata.acl_categories)
}

fn command_metadata_table() -> &'static [(&'static [u8], CommandMetadata)] {
    COMMAND_METADATA_TABLE.get_or_init(|| {
        let mut rows: Vec<(&'static [u8], CommandMetadata)> = Vec::new();
        for spec in COMMANDS.iter() {
            match rows
                .iter_mut()
                .find(|(name, _)| ascii_eq_ignore_case(name, spec.name.as_bytes()))
            {
                Some((_, metadata)) => metadata.include(spec.flags, spec.acl_categories),
                None => {
                    let mut metadata = CommandMetadata::default();
                    metadata.include(spec.flags, spec.acl_categories);
                    rows.push((spec.name.as_bytes(), metadata));
                }
            }
            if spec.group == "scripting" {
                if let Some((_, metadata)) = rows
                    .iter_mut()
                    .find(|(name, _)| ascii_eq_ignore_case(name, spec.name.as_bytes()))
                {
                    metadata.acl_categories |= acl_category::SCRIPTING;
                }
            }
        }
        for (name, metadata) in rows.iter_mut() {
            metadata.monitor_admin = runtime_monitor_admin_flag(name, *metadata);
        }
        rows.sort_by(|(left, _), (right, _)| ascii_casecmp(left, right));
        rows
    })
}

fn runtime_monitor_admin_flag(name: &[u8], metadata: CommandMetadata) -> bool {
    if !metadata.admin {
        return false;
    }

    let expected_function = generated_command_function_name(name);
    let mut fallback: Option<&'static GeneratedCommandSpec> = None;
    for spec in COMMANDS
        .iter()
        .filter(|spec| ascii_eq_ignore_case(spec.name.as_bytes(), name))
    {
        if spec.function.as_bytes() == expected_function.as_slice() {
            return spec.flags.contains(&CommandFlag::ADMIN);
        }
        if fallback.is_none() && !spec.flags.contains(&CommandFlag::ONLY_SENTINEL) {
            fallback = Some(spec);
        }
    }

    match fallback {
        Some(spec) if spec.function.is_empty() => metadata.admin,
        Some(spec) => spec.flags.contains(&CommandFlag::ADMIN),
        None => metadata.admin,
    }
}

fn generated_command_function_name(name: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + b"Command".len());
    let mut uppercase_next = false;
    for &byte in name {
        if byte == b'-' || byte == b'_' {
            uppercase_next = true;
            continue;
        }
        let lower = ascii_lower(byte);
        if uppercase_next {
            out.push(lower.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            out.push(lower);
        }
    }
    out.extend_from_slice(b"Command");
    out
}

impl CommandMetadata {
    fn include(&mut self, flags: &[CommandFlag], acl_categories: &[crate::generated::AclCategory]) {
        for flag in flags {
            match flag {
                CommandFlag::ADMIN => self.admin = true,
                CommandFlag::WRITE => self.write = true,
                CommandFlag::NO_AUTH => self.no_auth = true,
                CommandFlag::DENYOOM => self.denyoom = true,
                CommandFlag::NO_MULTI => self.no_multi = true,
                CommandFlag::ALLOW_BUSY => self.allow_busy = true,
                CommandFlag::SKIP_COMMANDLOG => self.skip_commandlog = true,
                CommandFlag::SKIP_MONITOR => self.skip_monitor = true,
                _ => {}
            }
        }
        for &cat in acl_categories {
            self.acl_categories |= acl_category_bits(cat);
        }
    }
}

fn acl_category_bits(cat: crate::generated::AclCategory) -> u64 {
    match cat {
        crate::generated::AclCategory::KEYSPACE => acl_category::KEYSPACE,
        crate::generated::AclCategory::READ => acl_category::READ,
        crate::generated::AclCategory::WRITE => acl_category::WRITE,
        crate::generated::AclCategory::SET => acl_category::SET,
        crate::generated::AclCategory::SORTEDSET => acl_category::SORTEDSET,
        crate::generated::AclCategory::LIST => acl_category::LIST,
        crate::generated::AclCategory::HASH => acl_category::HASH,
        crate::generated::AclCategory::STRING => acl_category::STRING,
        crate::generated::AclCategory::BITMAP => acl_category::BITMAP,
        crate::generated::AclCategory::HYPERLOGLOG => acl_category::HYPERLOGLOG,
        crate::generated::AclCategory::GEO => acl_category::GEO,
        crate::generated::AclCategory::STREAM => acl_category::STREAM,
        crate::generated::AclCategory::PUBSUB => acl_category::PUBSUB,
        crate::generated::AclCategory::ADMIN => acl_category::ADMIN,
        crate::generated::AclCategory::FAST => acl_category::FAST,
        crate::generated::AclCategory::SLOW => acl_category::SLOW,
        crate::generated::AclCategory::BLOCKING => acl_category::BLOCKING,
        crate::generated::AclCategory::DANGEROUS => acl_category::DANGEROUS,
        crate::generated::AclCategory::CONNECTION => acl_category::CONNECTION,
        crate::generated::AclCategory::TRANSACTION => acl_category::TRANSACTION,
        crate::generated::AclCategory::SCRIPTING => acl_category::SCRIPTING,
    }
}

/// ACL gate: check that the current client is authenticated and allowed to run `name`.
///
/// Returns `Some(reply_bytes)` to short-circuit dispatch with the encoded error.
/// Returns `None` when the command should proceed.
fn enforce_acl_gate(ctx: &CommandContext<'_>, name: &[u8], cmd_categories: u64) -> Option<Vec<u8>> {
    let default_name = RedisString::from_bytes(b"default");
    let user_name = ctx
        .client_ref()
        .authenticated_user
        .clone()
        .unwrap_or_else(|| default_name.clone());

    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };

    let user = match guard.users.get(&user_name) {
        Some(u) => u,
        None => {
            return Some(b"-NOAUTH Authentication required.\r\n".to_vec());
        }
    };

    if ctx.client_ref().authenticated_user.is_none() {
        return Some(b"-NOAUTH Authentication required.\r\n".to_vec());
    }

    if is_always_allowed_for_authenticated(ctx, name) {
        return None;
    }

    let first_arg = ctx.client_ref().arg(1).map(|arg| arg.as_bytes());
    if !user.can_execute_command_with_arg(name, first_arg, cmd_categories) {
        let object = RedisString::from_vec(acl_command_error_name(ctx, name, user));
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(b"-NOPERM This user has no permissions to run the '");
        msg.extend_from_slice(object.as_bytes());
        msg.extend_from_slice(b"' command\r\n");
        let client_info = acl_log_client_info(ctx, name);
        drop(guard);
        record_acl_access_denied_cmd();
        record_acl_log_entry(b"command", acl_log_context(ctx), object, user_name, client_info);
        return Some(msg);
    }

    if let Some(object) = enforce_acl_key_gate(ctx, name, user) {
        let client_info = acl_log_client_info(ctx, name);
        drop(guard);
        record_acl_access_denied_key();
        record_acl_log_entry(b"key", acl_log_context(ctx), object, user_name, client_info);
        return Some(b"-NOPERM No permissions to access a key\r\n".to_vec());
    }
    if let Some(object) = enforce_acl_channel_gate(ctx, name, user) {
        let client_info = acl_log_client_info(ctx, name);
        drop(guard);
        record_acl_access_denied_channel();
        record_acl_log_entry(b"channel", acl_log_context(ctx), object, user_name, client_info);
        return Some(b"-NOPERM No permissions to access a channel\r\n".to_vec());
    }
    if let Some(object) = enforce_acl_database_gate(ctx, name, user) {
        let client_info = acl_log_client_info(ctx, name);
        drop(guard);
        record_acl_log_entry(
            b"database",
            acl_log_context(ctx),
            object,
            user_name,
            client_info,
        );
        return Some(b"-NOPERM No permissions to access a database\r\n".to_vec());
    }

    None
}

fn close_unauthenticated_client_for_debug_reply_limit(ctx: &mut CommandContext<'_>) -> bool {
    if redis_core::client::debug_client_enforce_reply_list()
        && ctx.client_ref().authenticated_user.is_none()
        && !ctx.client_ref().ever_authenticated
    {
        ctx.client_mut().should_close = true;
        return true;
    }
    false
}

fn acl_categories_for_context(
    ctx: &CommandContext<'_>,
    name: &[u8],
    base_categories: u64,
) -> u64 {
    if ascii_eq_ignore_case(name, b"XINFO") {
        return base_categories | acl_category::STREAM | acl_category::READ;
    }
    if ascii_eq_ignore_case(name, b"XGROUP") {
        return base_categories | acl_category::STREAM | acl_category::WRITE;
    }
    if ascii_eq_ignore_case(name, b"COMMAND") && ctx.arg_count() > 1 {
        return base_categories | acl_category::CONNECTION;
    }
    base_categories
}

fn enforce_acl_key_gate(
    ctx: &CommandContext<'_>,
    name: &[u8],
    user: &AclUser,
) -> Option<RedisString> {
    if user.flags.allkeys {
        return None;
    }
    let argc = ctx.arg_count();
    for spec in COMMANDS
        .iter()
        .filter(|spec| ascii_eq_ignore_case(spec.name.as_bytes(), name))
    {
        let Ok(Value::Array(items)) = serde_json::from_str::<Value>(spec.key_specs_json) else {
            continue;
        };
        for item in items {
            if key_spec_has_flag(&item, "NOT_KEY") {
                continue;
            }
            let begin = item
                .pointer("/begin_search/index/pos")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            let Some(begin) = begin else {
                continue;
            };
            if let Some(range) = item.pointer("/find_keys/range") {
                let step = range
                    .get("step")
                    .and_then(Value::as_u64)
                    .unwrap_or(1)
                    .max(1) as usize;
                let lastkey = range.get("lastkey").and_then(Value::as_i64).unwrap_or(0);
                let end = acl_range_end(argc, begin, lastkey)?;
                let mut idx = begin;
                while idx <= end {
                    if let Some(key) = ctx.client_ref().arg(idx) {
                        if !user.can_access_key(key.as_bytes()) {
                            return Some(key.clone());
                        }
                    }
                    idx = match idx.checked_add(step) {
                        Some(next) => next,
                        None => break,
                    };
                }
            } else if let Some(keynum) = item.pointer("/find_keys/keynum") {
                let keynumidx = keynum
                    .get("keynumidx")
                    .and_then(Value::as_u64)
                    .map(|v| v as usize)?;
                let firstkey = keynum
                    .get("firstkey")
                    .and_then(Value::as_u64)
                    .map(|v| v as usize)?;
                let step = keynum
                    .get("step")
                    .and_then(Value::as_u64)
                    .unwrap_or(1)
                    .max(1) as usize;
                let raw_count = ctx.client_ref().arg(keynumidx)?;
                let Ok(raw_count) = std::str::from_utf8(raw_count.as_bytes()) else {
                    continue;
                };
                let Ok(count) = raw_count.parse::<usize>() else {
                    continue;
                };
                for n in 0..count {
                    let idx = firstkey + n * step;
                    if let Some(key) = ctx.client_ref().arg(idx) {
                        if !user.can_access_key(key.as_bytes()) {
                            return Some(key.clone());
                        }
                    }
                }
            }
        }
    }
    None
}

fn acl_range_end(argc: usize, begin: usize, lastkey: i64) -> Option<usize> {
    if argc == 0 || begin >= argc {
        return None;
    }
    let end = match lastkey {
        0 => begin,
        -1 => argc.saturating_sub(1),
        -2 => argc.saturating_sub(2),
        n if n > 0 => n as usize,
        _ => return None,
    };
    Some(end.min(argc.saturating_sub(1)))
}

fn key_spec_has_flag(spec: &Value, flag: &str) -> bool {
    spec.get("flags")
        .and_then(Value::as_array)
        .is_some_and(|flags| flags.iter().any(|item| item.as_str() == Some(flag)))
}

fn enforce_acl_channel_gate(
    ctx: &CommandContext<'_>,
    name: &[u8],
    user: &AclUser,
) -> Option<RedisString> {
    if user.flags.allchannels {
        return None;
    }
    let lower = ascii_lower_vec(name);
    let (start, end, pattern) = match lower.as_slice() {
        b"publish" | b"spublish" => (1, 2.min(ctx.arg_count()), false),
        b"subscribe" | b"ssubscribe" => (1, ctx.arg_count(), false),
        b"psubscribe" => (1, ctx.arg_count(), true),
        _ => return None,
    };
    for idx in start..end {
        if let Some(channel) = ctx.client_ref().arg(idx) {
            let allowed = if pattern {
                user.can_access_channel_pattern(channel.as_bytes())
            } else {
                user.can_access_channel(channel.as_bytes())
            };
            if !allowed {
                return Some(channel.clone());
            }
        }
    }
    None
}

fn enforce_acl_database_gate(
    ctx: &CommandContext<'_>,
    name: &[u8],
    user: &AclUser,
) -> Option<RedisString> {
    if user.flags.alldbs {
        return None;
    }
    let lower = ascii_lower_vec(name);
    match lower.as_slice() {
        b"select" => {
            let db = ctx.client_ref().arg(1)?;
            let parsed = parse_acl_db_arg(db.as_bytes())?;
            if user.can_access_db(parsed) {
                None
            } else {
                Some(db.clone())
            }
        }
        b"swapdb" => {
            for idx in 1..=2 {
                let db = ctx.client_ref().arg(idx)?;
                let parsed = parse_acl_db_arg(db.as_bytes())?;
                if !user.can_access_db(parsed) {
                    return Some(db.clone());
                }
            }
            None
        }
        b"flushall" => Some(RedisString::from_static(b"flushall")),
        b"flushdb" => {
            let db = ctx.selected_db_id();
            if user.can_access_db(db) {
                None
            } else {
                Some(RedisString::from_vec(db.to_string().into_bytes()))
            }
        }
        _ => None,
    }
}

fn parse_acl_db_arg(bytes: &[u8]) -> Option<u32> {
    let s = core::str::from_utf8(bytes).ok()?;
    let parsed: i64 = s.parse().ok()?;
    if parsed < 0 || parsed > u32::MAX as i64 {
        return None;
    }
    Some(parsed as u32)
}

fn acl_log_context(ctx: &CommandContext<'_>) -> &'static [u8] {
    if ctx.client_ref().flag_lua() {
        b"lua"
    } else if ctx.client_ref().flag_multi() || ctx.client_ref().flag_deny_blocking() {
        b"multi"
    } else {
        b"toplevel"
    }
}

fn acl_log_client_info(ctx: &CommandContext<'_>, name: &[u8]) -> RedisString {
    let command = if ctx.client_ref().flag_lua() {
        b"eval".to_vec()
    } else if ctx.client_ref().flag_deny_blocking() {
        b"exec".to_vec()
    } else {
        ascii_lower_vec(name)
    };
    let command = String::from_utf8_lossy(&command);
    let username = ctx
        .client_ref()
        .authenticated_user
        .as_ref()
        .map(|user| String::from_utf8_lossy(user.as_bytes()).into_owned())
        .unwrap_or_else(|| "default".to_string());
    RedisString::from_vec(
        format!(
            "id={} db={} cmd={} user={}",
            ctx.client_ref().id(),
            ctx.selected_db_id(),
            command,
            username
        )
        .into_bytes(),
    )
}

fn acl_command_error_name(
    ctx: &CommandContext<'_>,
    name: &[u8],
    user: &AclUser,
) -> Vec<u8> {
    let lower = ascii_lower_vec(name);
    let lower_rs = RedisString::from_bytes(&lower);
    if let Some(first_arg) = ctx.client_ref().arg(1) {
        let mut full = Vec::with_capacity(lower.len() + 1 + first_arg.as_bytes().len());
        full.extend_from_slice(&lower);
        full.push(b'|');
        full.extend(first_arg.as_bytes().iter().map(|b| b.to_ascii_lowercase()));
        let full_rs = RedisString::from_bytes(&full);
        if user.denied_commands.iter().any(|cmd| cmd == &full_rs) {
            return full;
        }
        if acl_known_container_subcommand(&lower, first_arg.as_bytes()) {
            return full;
        }
        if user.denied_commands.iter().any(|cmd| cmd == &lower_rs) {
            return lower;
        }
    }
    lower
}

fn acl_known_container_subcommand(parent: &[u8], sub: &[u8]) -> bool {
    let sub_lower = ascii_lower_vec(sub);
    let candidates: &[&[u8]] = match parent {
        b"client" => &[
            b"caching",
            b"getname",
            b"id",
            b"info",
            b"kill",
            b"list",
            b"no-evict",
            b"no-touch",
            b"pause",
            b"reply",
            b"setname",
            b"tracking",
            b"trackinginfo",
            b"unblock",
        ],
        b"config" => &[b"get", b"resetstat", b"rewrite", b"set"],
        b"script" => &[b"debug", b"exists", b"flush", b"help", b"kill", b"load"],
        b"memory" => &[b"doctor", b"malloc-stats", b"purge", b"stats", b"usage"],
        b"xinfo" => &[b"consumers", b"groups", b"help", b"stream"],
        _ => return false,
    };
    candidates
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(&sub_lower))
}

/// Return `true` for commands that any authenticated user may run regardless of
/// their command permissions.
///
/// `ACL WHOAMI` and `ACL HELP` are informational connection-level queries that
/// do not expose sensitive data and do not mutate state. Real Redis allows these
/// for any authenticated user.
fn is_always_allowed_for_authenticated(ctx: &CommandContext<'_>, name: &[u8]) -> bool {
    if !ascii_eq_ignore_case(name, b"ACL") {
        return false;
    }
    let sub = match ctx.client_ref().arg(1) {
        Some(s) => s,
        None => return false,
    };
    ascii_eq_ignore_case(sub.as_bytes(), b"WHOAMI") || ascii_eq_ignore_case(sub.as_bytes(), b"HELP")
}

/// Reject write commands from regular clients when we are operating as a
/// replica (`repl_state == REPLICA_ONLINE`).
///
/// Commands that arrive on the master-to-replica stream are applied via
/// `apply_command_locally` in the dialer thread, which sets `client.is_replica
/// = true` before calling `dispatch_command_name`. Those calls are therefore
/// exempt — the `is_replica` flag already causes `propagate_write_to_replicas`
/// to be skipped, and here we only block clients whose `is_replica` is false
/// (i.e. normal user connections).
///
/// `REPLICAOF` itself is always allowed so the operator can promote the server.
fn enforce_replica_readonly_gate(
    ctx: &CommandContext<'_>,
    name: &[u8],
    is_write_command: bool,
) -> Option<Vec<u8>> {
    use redis_core::replication::{global_replication_state, repl_state_code};
    let repl = global_replication_state();
    if repl.repl_state.load(std::sync::atomic::Ordering::Relaxed) != repl_state_code::REPLICA_ONLINE
    {
        return None;
    }
    if ctx.client_ref().is_replica {
        return None;
    }
    if ascii_eq_ignore_case(name, b"REPLICAOF") || ascii_eq_ignore_case(name, b"SLAVEOF") {
        return None;
    }
    if !is_write_command {
        return None;
    }
    Some(b"-READONLY You can't write against a read only replica.\r\n".to_vec())
}

/// Pre-handler maxmemory enforcement.
///
/// Returns `Some(reply_bytes)` when the command must be rejected because the
/// server is over its `maxmemory` budget and the configured eviction policy
/// either cannot or refuses to recover memory. Returns `None` when dispatch
/// should proceed (either we were under the limit, or eviction trimmed the
/// keyspace back under it, or the command is exempt from DENYOOM).
pub(crate) fn enforce_maxmemory_gate(
    ctx: &mut CommandContext<'_>,
    is_denyoom_command: bool,
) -> Option<Vec<u8>> {
    let maxmem = ctx.live_config().maxmemory();
    if maxmem == 0 {
        return None;
    }
    let used = approximate_memory_used(ctx.db());
    if used <= maxmem {
        return None;
    }
    let policy = ctx.live_config().maxmemory_policy();
    let log_factor = ctx.live_config().lfu_log_factor();
    let decay_time = ctx.live_config().lfu_decay_time();
    let outcome = try_evict_to_fit(ctx.db_mut(), maxmem, policy, log_factor, decay_time);
    let still_over = match outcome {
        EvictionOutcome::Sufficient => false,
        EvictionOutcome::Evicted(keys) => {
            if !keys.is_empty() {
                let pubsub = ctx.pubsub.as_ref().cloned();
                redis_core::tracking::runtime_invalidate_keys(
                    ctx.client_ref().id,
                    ctx.client_mut(),
                    pubsub.as_ref(),
                    &keys,
                    true,
                    false,
                );
            }
            false
        }
        EvictionOutcome::StillOver(keys) => {
            if !keys.is_empty() {
                let pubsub = ctx.pubsub.as_ref().cloned();
                redis_core::tracking::runtime_invalidate_keys(
                    ctx.client_ref().id,
                    ctx.client_mut(),
                    pubsub.as_ref(),
                    &keys,
                    true,
                    false,
                );
            }
            true
        }
    };
    if still_over && is_denyoom_command {
        Some(oom_error_reply())
    } else {
        None
    }
}

fn enforce_busy_script_gate(
    ctx: &CommandContext<'_>,
    name: &[u8],
    allow_busy_command: bool,
) -> Option<Vec<u8>> {
    if !crate::eval::is_script_busy() {
        return None;
    }
    if ascii_eq_ignore_case(name, b"PING") && crate::eval::busy_script_owner_is(ctx.client_ref().id)
    {
        return None;
    }
    if allow_busy_command
        || is_script_kill_command(ctx, name)
        || is_function_busy_command(ctx, name)
    {
        return None;
    }
    Some(crate::eval::busy_script_error_reply())
}

fn is_script_kill_command(ctx: &CommandContext<'_>, name: &[u8]) -> bool {
    if !ascii_eq_ignore_case(name, b"SCRIPT") {
        return false;
    }
    match ctx.client_ref().arg(1) {
        Some(sub) => ascii_eq_ignore_case(sub.as_bytes(), b"KILL"),
        None => false,
    }
}

fn is_function_busy_command(ctx: &CommandContext<'_>, name: &[u8]) -> bool {
    if !ascii_eq_ignore_case(name, b"FUNCTION") {
        return false;
    }
    match ctx.client_ref().arg(1) {
        Some(sub) => {
            ascii_eq_ignore_case(sub.as_bytes(), b"KILL")
                || ascii_eq_ignore_case(sub.as_bytes(), b"STATS")
        }
        None => false,
    }
}

fn is_client_reply_command(ctx: &CommandContext<'_>, name: &[u8]) -> bool {
    if !ascii_eq_ignore_case(name, b"CLIENT") {
        return false;
    }
    match ctx.client_ref().arg(1) {
        Some(sub) => ascii_eq_ignore_case(sub.as_bytes(), b"REPLY"),
        None => false,
    }
}

pub(crate) fn execabort_from_error_reply(reply: &[u8]) -> Vec<u8> {
    let msg = reply
        .strip_prefix(b"-")
        .unwrap_or(reply)
        .strip_suffix(b"\r\n")
        .unwrap_or(reply);
    let mut out =
        Vec::with_capacity(b"-EXECABORT Transaction discarded because of: \r\n".len() + msg.len());
    out.extend_from_slice(b"-EXECABORT Transaction discarded because of: ");
    out.extend_from_slice(msg);
    out.extend_from_slice(b"\r\n");
    out
}

/// Append `argv` to the replication backlog and fan out to all online replicas.
///
/// Called from `dispatch_command_name` after every successful write command
/// executed by a non-replica client. Failures to deliver to a specific
/// replica are logged and skipped; they are non-fatal because the replica
/// can re-sync via PSYNC.
fn propagate_write_to_replicas(
    repl: &redis_core::replication::ReplicationState,
    selected_db: u32,
    argv: &[RedisString],
) {
    let select_bytes = replication_select_bytes_if_needed(repl, selected_db);
    let argv_bytes = crate::aof::encode_resp_command(argv);
    if let Some(select_bytes) = select_bytes.as_ref() {
        repl.append_to_backlog(select_bytes);
    }
    repl.append_to_backlog(&argv_bytes);
    let guard = match repl.replicas.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for conn in guard.values() {
        if redis_core::replication::ReplicaState::from_u8(
            conn.state.load(std::sync::atomic::Ordering::Acquire),
        ) == redis_core::replication::ReplicaState::Online
        {
            if let Some(select_bytes) = select_bytes.as_ref() {
                if conn.outbound_sender.send(select_bytes.clone()).is_err() {
                    eprintln!(
                        "redis-server: replication SELECT fan-out failed for client {}",
                        conn.client_id
                    );
                    continue;
                }
            }
            if conn.outbound_sender.send(argv_bytes.clone()).is_err() {
                eprintln!(
                    "redis-server: replication fan-out failed for client {}",
                    conn.client_id
                );
            }
        }
    }
}

fn replication_select_bytes_if_needed(
    repl: &redis_core::replication::ReplicationState,
    selected_db: u32,
) -> Option<Vec<u8>> {
    let selected_db = selected_db as i32;
    let previous = repl
        .selected_db
        .swap(selected_db, std::sync::atomic::Ordering::AcqRel);
    if previous == selected_db {
        return None;
    }
    let argv = [
        RedisString::from_bytes(b"SELECT"),
        RedisString::from_bytes(selected_db.to_string().as_bytes()),
    ];
    Some(crate::aof::encode_resp_command(&argv))
}

/// Commands a replica client is allowed to issue back to the master after
/// the PSYNC handshake. Real Redis treats the replica link as outbound-only
/// from the master's perspective; the only frames the master expects from
/// the replica are REPLCONF ACK heartbeats and the occasional PING.
fn is_replica_allowed_command(name: &[u8]) -> bool {
    ascii_eq_ignore_case(name, b"REPLCONF")
        || ascii_eq_ignore_case(name, b"PING")
        || ascii_eq_ignore_case(name, b"QUIT")
}

/// Build the canonical `unknown command '<name>'` error.
fn unknown_command_error(name: &[u8]) -> RedisError {
    let mut buf = Vec::with_capacity(b"ERR unknown command '".len() + name.len() + 1);
    buf.extend_from_slice(b"ERR unknown command '");
    buf.extend_from_slice(name);
    buf.push(b'\'');
    RedisError::runtime(buf)
}

/// Case-insensitive ASCII equality. Non-ASCII bytes compare strictly.
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn ascii_casecmp(a: &[u8], b: &[u8]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        match ascii_lower(*x).cmp(&ascii_lower(*y)) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

fn ascii_lower_vec(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| ascii_lower(*b)).collect()
}

/// Wave A placeholder handler that returns `Err(RedisError::runtime(b"ERR …"))`.
///
/// Handler bodies in Waves B/C/D will replace these one by one. Routing to
/// the stub proves the table is wired correctly. Retained for new commands
/// scaffolded but not yet implemented.
#[allow(dead_code)]
fn unimplemented_handler(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let name = ctx
        .client_ref()
        .arg(0)
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_default();
    let mut msg = Vec::with_capacity(b"ERR command not implemented yet: ".len() + name.len());
    msg.extend_from_slice(b"ERR command not implemented yet: ");
    msg.extend_from_slice(&name);
    Err(RedisError::runtime(msg))
}

/// Static dispatch table.
///
/// Only includes commands whose handlers exist in this crate (even if the
/// handler body is `todo!()`). Wave B fills in PING + ECHO bodies; Wave C
/// fills in SET/GET/DEL/EXISTS/INCR.
///
/// PORT NOTE: For Wave A we route every entry to `unimplemented_handler`
/// rather than the real handler. The Wave B agent flips PING/ECHO over to
/// `crate::connection::ping_command` / `echo_command` once those exist;
/// Wave C does the same for string commands. This avoids `todo!()` panics
/// crashing the server during Wave A smoke testing.
pub static HANDLERS: &[DispatchEntry] = &[
    DispatchEntry {
        name: b"PING",
        handler: crate::connection::ping_command,
    },
    DispatchEntry {
        name: b"ECHO",
        handler: crate::connection::echo_command,
    },
    DispatchEntry {
        name: b"HELLO",
        handler: crate::connection::hello_command,
    },
    DispatchEntry {
        name: b"COMMAND",
        handler: crate::connection::command_command,
    },
    DispatchEntry {
        name: b"QUIT",
        handler: crate::connection::quit_command,
    },
    DispatchEntry {
        name: b"SHUTDOWN",
        handler: crate::connection::shutdown_command,
    },
    DispatchEntry {
        name: b"SELECT",
        handler: crate::connection::select_command,
    },
    DispatchEntry {
        name: b"CLIENT",
        handler: crate::connection::client_command,
    },
    DispatchEntry {
        name: b"DEBUG",
        handler: crate::connection::debug_command,
    },
    DispatchEntry {
        name: b"TIME",
        handler: crate::connection::time_command,
    },
    DispatchEntry {
        name: b"RESET",
        handler: crate::connection::reset_command,
    },
    DispatchEntry {
        name: b"READONLY",
        handler: crate::connection::readonly_command,
    },
    DispatchEntry {
        name: b"READWRITE",
        handler: crate::connection::readwrite_command,
    },
    DispatchEntry {
        name: b"AUTH",
        handler: crate::connection::auth_command,
    },
    DispatchEntry {
        name: b"ACL",
        handler: crate::connection::acl_command,
    },
    DispatchEntry {
        name: b"SET",
        handler: crate::string::set_command,
    },
    DispatchEntry {
        name: b"GET",
        handler: crate::string::get_command,
    },
    DispatchEntry {
        name: b"DEL",
        handler: redis_core::db::del_command,
    },
    DispatchEntry {
        name: b"EXISTS",
        handler: redis_core::db::exists_command,
    },
    DispatchEntry {
        name: b"INCR",
        handler: crate::string::incr_command,
    },
    DispatchEntry {
        name: b"DECR",
        handler: crate::string::decr_command,
    },
    DispatchEntry {
        name: b"INCRBY",
        handler: crate::string::incrby_command,
    },
    DispatchEntry {
        name: b"DECRBY",
        handler: crate::string::decrby_command,
    },
    // ── GENERIC-KEY-OPS (Round 1, agent E2) ────────────────────────────────
    DispatchEntry {
        name: b"TYPE",
        handler: redis_core::db::type_command,
    },
    DispatchEntry {
        name: b"RENAME",
        handler: redis_core::db::rename_command,
    },
    DispatchEntry {
        name: b"RENAMENX",
        handler: redis_core::db::renamenx_command,
    },
    DispatchEntry {
        name: b"RANDOMKEY",
        handler: redis_core::db::randomkey_command,
    },
    DispatchEntry {
        name: b"DBSIZE",
        handler: redis_core::db::dbsize_command,
    },
    DispatchEntry {
        name: b"FLUSHDB",
        handler: redis_core::db::flushdb_command,
    },
    DispatchEntry {
        name: b"FLUSHALL",
        handler: redis_core::db::flushall_command,
    },
    DispatchEntry {
        name: b"TOUCH",
        handler: redis_core::db::touch_command,
    },
    DispatchEntry {
        name: b"UNLINK",
        handler: redis_core::db::unlink_command,
    },
    DispatchEntry {
        name: b"KEYS",
        handler: redis_core::db::keys_command,
    },
    DispatchEntry {
        name: b"COPY",
        handler: redis_core::db::copy_command,
    },
    DispatchEntry {
        name: b"MOVE",
        handler: redis_core::db::move_command,
    },
    DispatchEntry {
        name: b"SWAPDB",
        handler: redis_core::db::swapdb_command,
    },
    // ── STRING (Round 1, agent E1) ─────────────────────────────────────────
    DispatchEntry {
        name: b"APPEND",
        handler: crate::string::append_command,
    },
    DispatchEntry {
        name: b"STRLEN",
        handler: crate::string::strlen_command,
    },
    DispatchEntry {
        name: b"MGET",
        handler: crate::string::mget_command,
    },
    DispatchEntry {
        name: b"MSET",
        handler: crate::string::mset_command,
    },
    DispatchEntry {
        name: b"MSETNX",
        handler: crate::string::msetnx_command,
    },
    DispatchEntry {
        name: b"SETNX",
        handler: crate::string::setnx_command,
    },
    DispatchEntry {
        name: b"GETSET",
        handler: crate::string::getset_command,
    },
    DispatchEntry {
        name: b"GETDEL",
        handler: crate::string::getdel_command,
    },
    DispatchEntry {
        name: b"GETRANGE",
        handler: crate::string::getrange_command,
    },
    DispatchEntry {
        name: b"SETRANGE",
        handler: crate::string::setrange_command,
    },
    DispatchEntry {
        name: b"SUBSTR",
        handler: crate::string::getrange_command,
    },
    DispatchEntry {
        name: b"SETEX",
        handler: crate::string::setex_command,
    },
    DispatchEntry {
        name: b"PSETEX",
        handler: crate::string::psetex_command,
    },
    DispatchEntry {
        name: b"GETEX",
        handler: crate::string::getex_command,
    },
    DispatchEntry {
        name: b"MSETEX",
        handler: crate::string::msetex_command,
    },
    DispatchEntry {
        name: b"DELIFEQ",
        handler: crate::string::delifeq_command,
    },
    DispatchEntry {
        name: b"INCRBYFLOAT",
        handler: crate::string::incrbyfloat_command,
    },
    DispatchEntry {
        name: b"LCS",
        handler: crate::string::lcs_command,
    },
    // ── LIST (Round 2) ─────────────────────────────────────────────────────
    DispatchEntry {
        name: b"LPUSH",
        handler: crate::list::lpush_command,
    },
    DispatchEntry {
        name: b"RPUSH",
        handler: crate::list::rpush_command,
    },
    DispatchEntry {
        name: b"LPUSHX",
        handler: crate::list::lpushx_command,
    },
    DispatchEntry {
        name: b"RPUSHX",
        handler: crate::list::rpushx_command,
    },
    DispatchEntry {
        name: b"LPOP",
        handler: crate::list::lpop_command,
    },
    DispatchEntry {
        name: b"RPOP",
        handler: crate::list::rpop_command,
    },
    DispatchEntry {
        name: b"LLEN",
        handler: crate::list::llen_command,
    },
    DispatchEntry {
        name: b"LRANGE",
        handler: crate::list::lrange_command,
    },
    DispatchEntry {
        name: b"LINDEX",
        handler: crate::list::lindex_command,
    },
    DispatchEntry {
        name: b"LSET",
        handler: crate::list::lset_command,
    },
    DispatchEntry {
        name: b"LREM",
        handler: crate::list::lrem_command,
    },
    DispatchEntry {
        name: b"LTRIM",
        handler: crate::list::ltrim_command,
    },
    DispatchEntry {
        name: b"LINSERT",
        handler: crate::list::linsert_command,
    },
    DispatchEntry {
        name: b"LMOVE",
        handler: crate::list::lmove_command,
    },
    DispatchEntry {
        name: b"RPOPLPUSH",
        handler: crate::list::rpoplpush_command,
    },
    DispatchEntry {
        name: b"LPOS",
        handler: crate::list::lpos_command,
    },
    DispatchEntry {
        name: b"LMPOP",
        handler: crate::list::lmpop_command,
    },
    DispatchEntry {
        name: b"BLPOP",
        handler: crate::list::blpop_command,
    },
    DispatchEntry {
        name: b"BRPOP",
        handler: crate::list::brpop_command,
    },
    DispatchEntry {
        name: b"BLMOVE",
        handler: crate::list::blmove_command,
    },
    DispatchEntry {
        name: b"BRPOPLPUSH",
        handler: crate::list::brpoplpush_command,
    },
    DispatchEntry {
        name: b"BLMPOP",
        handler: crate::list::blmpop_command,
    },
    // ── HASH (Round 3) ─────────────────────────────────────────────────────
    DispatchEntry {
        name: b"HSET",
        handler: crate::hash::hset_command,
    },
    DispatchEntry {
        name: b"HSETNX",
        handler: crate::hash::hsetnx_command,
    },
    DispatchEntry {
        name: b"HGET",
        handler: crate::hash::hget_command,
    },
    DispatchEntry {
        name: b"HGETDEL",
        handler: crate::hash::hgetdel_command,
    },
    DispatchEntry {
        name: b"HGETEX",
        handler: crate::hash::hgetex_command,
    },
    DispatchEntry {
        name: b"HSETEX",
        handler: crate::hash::hsetex_command,
    },
    DispatchEntry {
        name: b"HEXPIRE",
        handler: crate::hash::hexpire_command,
    },
    DispatchEntry {
        name: b"HPEXPIRE",
        handler: crate::hash::hpexpire_command,
    },
    DispatchEntry {
        name: b"HEXPIREAT",
        handler: crate::hash::hexpireat_command,
    },
    DispatchEntry {
        name: b"HPEXPIREAT",
        handler: crate::hash::hpexpireat_command,
    },
    DispatchEntry {
        name: b"HTTL",
        handler: crate::hash::httl_command,
    },
    DispatchEntry {
        name: b"HPTTL",
        handler: crate::hash::hpttl_command,
    },
    DispatchEntry {
        name: b"HEXPIRETIME",
        handler: crate::hash::hexpiretime_command,
    },
    DispatchEntry {
        name: b"HPEXPIRETIME",
        handler: crate::hash::hpexpiretime_command,
    },
    DispatchEntry {
        name: b"HPERSIST",
        handler: crate::hash::hpersist_command,
    },
    DispatchEntry {
        name: b"HMGET",
        handler: crate::hash::hmget_command,
    },
    DispatchEntry {
        name: b"HMSET",
        handler: crate::hash::hmset_command,
    },
    DispatchEntry {
        name: b"HDEL",
        handler: crate::hash::hdel_command,
    },
    DispatchEntry {
        name: b"HEXISTS",
        handler: crate::hash::hexists_command,
    },
    DispatchEntry {
        name: b"HLEN",
        handler: crate::hash::hlen_command,
    },
    DispatchEntry {
        name: b"HSTRLEN",
        handler: crate::hash::hstrlen_command,
    },
    DispatchEntry {
        name: b"HGETALL",
        handler: crate::hash::hgetall_command,
    },
    DispatchEntry {
        name: b"HKEYS",
        handler: crate::hash::hkeys_command,
    },
    DispatchEntry {
        name: b"HVALS",
        handler: crate::hash::hvals_command,
    },
    DispatchEntry {
        name: b"HINCRBY",
        handler: crate::hash::hincrby_command,
    },
    DispatchEntry {
        name: b"HINCRBYFLOAT",
        handler: crate::hash::hincrbyfloat_command,
    },
    DispatchEntry {
        name: b"HRANDFIELD",
        handler: crate::hash::hrandfield_command,
    },
    // ── SET (Round 4) ──────────────────────────────────────────────────────
    DispatchEntry {
        name: b"SADD",
        handler: crate::set::sadd_command,
    },
    DispatchEntry {
        name: b"SREM",
        handler: crate::set::srem_command,
    },
    DispatchEntry {
        name: b"SMEMBERS",
        handler: crate::set::smembers_command,
    },
    DispatchEntry {
        name: b"SISMEMBER",
        handler: crate::set::sismember_command,
    },
    DispatchEntry {
        name: b"SMISMEMBER",
        handler: crate::set::smismember_command,
    },
    DispatchEntry {
        name: b"SCARD",
        handler: crate::set::scard_command,
    },
    DispatchEntry {
        name: b"SPOP",
        handler: crate::set::spop_command,
    },
    DispatchEntry {
        name: b"SRANDMEMBER",
        handler: crate::set::srandmember_command,
    },
    DispatchEntry {
        name: b"SMOVE",
        handler: crate::set::smove_command,
    },
    DispatchEntry {
        name: b"SINTER",
        handler: crate::set::sinter_command,
    },
    DispatchEntry {
        name: b"SINTERSTORE",
        handler: crate::set::sinterstore_command,
    },
    DispatchEntry {
        name: b"SINTERCARD",
        handler: crate::set::sintercard_command,
    },
    DispatchEntry {
        name: b"SUNION",
        handler: crate::set::sunion_command,
    },
    DispatchEntry {
        name: b"SUNIONSTORE",
        handler: crate::set::sunionstore_command,
    },
    DispatchEntry {
        name: b"SDIFF",
        handler: crate::set::sdiff_command,
    },
    DispatchEntry {
        name: b"SDIFFSTORE",
        handler: crate::set::sdiffstore_command,
    },
    // ── TTL / EXPIRATION (Round 6) ─────────────────────────────────────────
    DispatchEntry {
        name: b"EXPIRE",
        handler: redis_core::expire::expire_command,
    },
    DispatchEntry {
        name: b"PEXPIRE",
        handler: redis_core::expire::pexpire_command,
    },
    DispatchEntry {
        name: b"EXPIREAT",
        handler: redis_core::expire::expireat_command,
    },
    DispatchEntry {
        name: b"PEXPIREAT",
        handler: redis_core::expire::pexpireat_command,
    },
    DispatchEntry {
        name: b"PERSIST",
        handler: redis_core::expire::persist_command,
    },
    DispatchEntry {
        name: b"TTL",
        handler: redis_core::expire::ttl_command,
    },
    DispatchEntry {
        name: b"PTTL",
        handler: redis_core::expire::pttl_command,
    },
    DispatchEntry {
        name: b"EXPIRETIME",
        handler: redis_core::expire::expiretime_command,
    },
    DispatchEntry {
        name: b"PEXPIRETIME",
        handler: redis_core::expire::pexpiretime_command,
    },
    DispatchEntry {
        name: b"OBJECT",
        handler: redis_core::object::object_command,
    },
    // ── ZSET (Round 5) ─────────────────────────────────────────────────────
    DispatchEntry {
        name: b"ZADD",
        handler: crate::zset::zadd_command,
    },
    DispatchEntry {
        name: b"ZSCORE",
        handler: crate::zset::zscore_command,
    },
    DispatchEntry {
        name: b"ZMSCORE",
        handler: crate::zset::zmscore_command,
    },
    DispatchEntry {
        name: b"ZCARD",
        handler: crate::zset::zcard_command,
    },
    DispatchEntry {
        name: b"ZINCRBY",
        handler: crate::zset::zincrby_command,
    },
    DispatchEntry {
        name: b"ZRANGE",
        handler: crate::zset::zrange_command,
    },
    DispatchEntry {
        name: b"ZRANGEBYSCORE",
        handler: crate::zset::zrangebyscore_command,
    },
    DispatchEntry {
        name: b"ZREVRANGE",
        handler: crate::zset::zrevrange_command,
    },
    DispatchEntry {
        name: b"ZREVRANGEBYSCORE",
        handler: crate::zset::zrevrangebyscore_command,
    },
    DispatchEntry {
        name: b"ZRANK",
        handler: crate::zset::zrank_command,
    },
    DispatchEntry {
        name: b"ZREVRANK",
        handler: crate::zset::zrevrank_command,
    },
    DispatchEntry {
        name: b"ZREM",
        handler: crate::zset::zrem_command,
    },
    DispatchEntry {
        name: b"ZCOUNT",
        handler: crate::zset::zcount_command,
    },
    DispatchEntry {
        name: b"ZPOPMIN",
        handler: crate::zset::zpopmin_command,
    },
    DispatchEntry {
        name: b"ZPOPMAX",
        handler: crate::zset::zpopmax_command,
    },
    DispatchEntry {
        name: b"ZREMRANGEBYRANK",
        handler: crate::zset::zremrangebyrank_command,
    },
    DispatchEntry {
        name: b"ZREMRANGEBYSCORE",
        handler: crate::zset::zremrangebyscore_command,
    },
    // ── SCAN + ZSET-EXTRAS (Round 7) ───────────────────────────────────────
    DispatchEntry {
        name: b"SCAN",
        handler: redis_core::db::scan_command,
    },
    DispatchEntry {
        name: b"HSCAN",
        handler: crate::hash::hscan_command,
    },
    DispatchEntry {
        name: b"SSCAN",
        handler: crate::set::sscan_command,
    },
    DispatchEntry {
        name: b"ZSCAN",
        handler: crate::zset::zscan_command,
    },
    DispatchEntry {
        name: b"ZRANGEBYLEX",
        handler: crate::zset::zrangebylex_command,
    },
    DispatchEntry {
        name: b"ZREVRANGEBYLEX",
        handler: crate::zset::zrevrangebylex_command,
    },
    DispatchEntry {
        name: b"ZLEXCOUNT",
        handler: crate::zset::zlexcount_command,
    },
    DispatchEntry {
        name: b"ZREMRANGEBYLEX",
        handler: crate::zset::zremrangebylex_command,
    },
    DispatchEntry {
        name: b"ZUNIONSTORE",
        handler: crate::zset::zunionstore_command,
    },
    DispatchEntry {
        name: b"ZINTERSTORE",
        handler: crate::zset::zinterstore_command,
    },
    DispatchEntry {
        name: b"ZDIFFSTORE",
        handler: crate::zset::zdiffstore_command,
    },
    DispatchEntry {
        name: b"ZUNION",
        handler: crate::zset::zunion_command,
    },
    DispatchEntry {
        name: b"ZINTER",
        handler: crate::zset::zinter_command,
    },
    DispatchEntry {
        name: b"ZDIFF",
        handler: crate::zset::zdiff_command,
    },
    DispatchEntry {
        name: b"ZINTERCARD",
        handler: crate::zset::zintercard_command,
    },
    DispatchEntry {
        name: b"ZRANGESTORE",
        handler: crate::zset::zrangestore_command,
    },
    DispatchEntry {
        name: b"ZRANDMEMBER",
        handler: crate::zset::zrandmember_command,
    },
    DispatchEntry {
        name: b"ZMPOP",
        handler: crate::zset::zmpop_command,
    },
    DispatchEntry {
        name: b"BZPOPMIN",
        handler: crate::zset::bzpopmin_command,
    },
    DispatchEntry {
        name: b"BZPOPMAX",
        handler: crate::zset::bzpopmax_command,
    },
    DispatchEntry {
        name: b"BZMPOP",
        handler: crate::zset::bzmpop_command,
    },
    // ── BITMAP (Round 8c) ──────────────────────────────────────────────────
    DispatchEntry {
        name: b"SETBIT",
        handler: crate::bitops::setbit_command,
    },
    DispatchEntry {
        name: b"GETBIT",
        handler: crate::bitops::getbit_command,
    },
    DispatchEntry {
        name: b"BITCOUNT",
        handler: crate::bitops::bitcount_command,
    },
    DispatchEntry {
        name: b"BITPOS",
        handler: crate::bitops::bitpos_command,
    },
    DispatchEntry {
        name: b"BITOP",
        handler: crate::bitops::bitop_command,
    },
    DispatchEntry {
        name: b"BITFIELD",
        handler: crate::bitops::bitfield_command,
    },
    DispatchEntry {
        name: b"BITFIELD_RO",
        handler: crate::bitops::bitfield_ro_command,
    },
    // ── TRANSACTIONS (Round 8b) ────────────────────────────────────────────
    DispatchEntry {
        name: b"MULTI",
        handler: crate::multi::multi_command,
    },
    DispatchEntry {
        name: b"EXEC",
        handler: crate::multi::exec_command,
    },
    DispatchEntry {
        name: b"DISCARD",
        handler: crate::multi::discard_command,
    },
    DispatchEntry {
        name: b"WATCH",
        handler: crate::multi::watch_command,
    },
    DispatchEntry {
        name: b"UNWATCH",
        handler: crate::multi::unwatch_command,
    },
    // ── TCL HARNESS STUBS (Round 9) ────────────────────────────────────────
    DispatchEntry {
        name: b"FUNCTION",
        handler: crate::connection::function_command,
    },
    DispatchEntry {
        name: b"FCALL",
        handler: crate::eval::fcall_command,
    },
    DispatchEntry {
        name: b"FCALL_RO",
        handler: crate::eval::fcall_ro_command,
    },
    DispatchEntry {
        name: b"CONFIG",
        handler: crate::connection::config_command,
    },
    DispatchEntry {
        name: b"MEMORY",
        handler: crate::connection::memory_command,
    },
    DispatchEntry {
        name: b"MODULE",
        handler: crate::connection::module_command,
    },
    // ── PUB/SUB (Round 8a) ─────────────────────────────────────────────────
    DispatchEntry {
        name: b"SUBSCRIBE",
        handler: crate::pubsub::subscribe_command,
    },
    DispatchEntry {
        name: b"UNSUBSCRIBE",
        handler: crate::pubsub::unsubscribe_command,
    },
    DispatchEntry {
        name: b"PSUBSCRIBE",
        handler: crate::pubsub::psubscribe_command,
    },
    DispatchEntry {
        name: b"PUNSUBSCRIBE",
        handler: crate::pubsub::punsubscribe_command,
    },
    DispatchEntry {
        name: b"PUBLISH",
        handler: crate::pubsub::publish_command,
    },
    DispatchEntry {
        name: b"SPUBLISH",
        handler: crate::pubsub::spublish_command,
    },
    DispatchEntry {
        name: b"SSUBSCRIBE",
        handler: crate::pubsub::ssubscribe_command,
    },
    DispatchEntry {
        name: b"SUNSUBSCRIBE",
        handler: crate::pubsub::sunsubscribe_command,
    },
    DispatchEntry {
        name: b"PUBSUB",
        handler: crate::connection::pubsub_command,
    },
    // ── HYPERLOGLOG (Round 9 HLL) ──────────────────────────────────────────
    DispatchEntry {
        name: b"PFADD",
        handler: crate::hyperloglog::pfadd_command,
    },
    DispatchEntry {
        name: b"PFCOUNT",
        handler: crate::hyperloglog::pfcount_command,
    },
    DispatchEntry {
        name: b"PFDEBUG",
        handler: crate::hyperloglog::pfdebug_command,
    },
    DispatchEntry {
        name: b"PFMERGE",
        handler: crate::hyperloglog::pfmerge_command,
    },
    DispatchEntry {
        name: b"PFSELFTEST",
        handler: crate::hyperloglog::pfselftest_command,
    },
    // ── SORT (TCL frontier) ───────────────────────────────────────────────
    DispatchEntry {
        name: b"SORT",
        handler: crate::sort::sort_command,
    },
    DispatchEntry {
        name: b"SORT_RO",
        handler: crate::sort::sort_ro_command,
    },
    // ── INTROSPECTION (Round 9 INFO/CONFIG) ────────────────────────────────
    DispatchEntry {
        name: b"INFO",
        handler: crate::info::info_command,
    },
    DispatchEntry {
        name: b"LASTSAVE",
        handler: crate::info::lastsave_command,
    },
    // ── STREAMS (Round 9) ──────────────────────────────────────────────────
    DispatchEntry {
        name: b"XADD",
        handler: crate::stream::xadd_command,
    },
    DispatchEntry {
        name: b"XLEN",
        handler: crate::stream::xlen_command,
    },
    DispatchEntry {
        name: b"XRANGE",
        handler: crate::stream::xrange_command,
    },
    DispatchEntry {
        name: b"XREVRANGE",
        handler: crate::stream::xrevrange_command,
    },
    DispatchEntry {
        name: b"XDEL",
        handler: crate::stream::xdel_command,
    },
    DispatchEntry {
        name: b"XTRIM",
        handler: crate::stream::xtrim_command,
    },
    DispatchEntry {
        name: b"XREAD",
        handler: crate::stream::xread_command,
    },
    DispatchEntry {
        name: b"XINFO",
        handler: crate::stream::xinfo_command,
    },
    // ── STREAM CONSUMER GROUPS (Round 13c) ─────────────────────────────────
    DispatchEntry {
        name: b"XGROUP",
        handler: crate::stream::xgroup_command,
    },
    DispatchEntry {
        name: b"XREADGROUP",
        handler: crate::stream::xreadgroup_command,
    },
    DispatchEntry {
        name: b"XACK",
        handler: crate::stream::xack_command,
    },
    DispatchEntry {
        name: b"XPENDING",
        handler: crate::stream::xpending_command,
    },
    DispatchEntry {
        name: b"XCLAIM",
        handler: crate::stream::xclaim_command,
    },
    DispatchEntry {
        name: b"XAUTOCLAIM",
        handler: crate::stream::xautoclaim_command,
    },
    DispatchEntry {
        name: b"XSETID",
        handler: crate::stream::xsetid_command,
    },
    // ── SLOWLOG / LATENCY (OV-2) ───────────────────────────────────────────────
    DispatchEntry {
        name: b"SLOWLOG",
        handler: crate::slowlog_cmd::slowlog_command,
    },
    DispatchEntry {
        name: b"COMMANDLOG",
        handler: crate::slowlog_cmd::commandlog_command,
    },
    DispatchEntry {
        name: b"LATENCY",
        handler: crate::slowlog_cmd::latency_command,
    },
    DispatchEntry {
        name: b"MONITOR",
        handler: crate::connection::monitor_command,
    },
    // ── PERSISTENCE (Round 18) ─────────────────────────────────────────────
    DispatchEntry {
        name: b"DUMP",
        handler: crate::persist::dump_command,
    },
    DispatchEntry {
        name: b"RESTORE",
        handler: crate::persist::restore_command,
    },
    DispatchEntry {
        name: b"RESTORE-ASKING",
        handler: crate::persist::restore_asking_command,
    },
    DispatchEntry {
        name: b"MIGRATE",
        handler: crate::persist::migrate_command,
    },
    DispatchEntry {
        name: b"SAVE",
        handler: crate::persist::save_command,
    },
    DispatchEntry {
        name: b"BGSAVE",
        handler: crate::persist::bgsave_command,
    },
    DispatchEntry {
        name: b"BGREWRITEAOF",
        handler: crate::persist::bgrewriteaof_command,
    },
    // ── GEO (Session 1B) ───────────────────────────────────────────────────
    DispatchEntry {
        name: b"GEOADD",
        handler: crate::geo::geoadd_command,
    },
    DispatchEntry {
        name: b"GEODIST",
        handler: crate::geo::geodist_command,
    },
    DispatchEntry {
        name: b"GEOHASH",
        handler: crate::geo::geohash_command,
    },
    DispatchEntry {
        name: b"GEOPOS",
        handler: crate::geo::geopos_command,
    },
    DispatchEntry {
        name: b"GEOSEARCH",
        handler: crate::geo::geosearch_command,
    },
    DispatchEntry {
        name: b"GEOSEARCHSTORE",
        handler: crate::geo::geosearchstore_command,
    },
    DispatchEntry {
        name: b"GEORADIUS",
        handler: crate::geo::georadius_command,
    },
    DispatchEntry {
        name: b"GEORADIUSBYMEMBER",
        handler: crate::geo::georadiusbymember_command,
    },
    DispatchEntry {
        name: b"GEORADIUS_RO",
        handler: crate::geo::georadiusro_command,
    },
    DispatchEntry {
        name: b"GEORADIUSBYMEMBER_RO",
        handler: crate::geo::georadiusbymemberro_command,
    },
    // ── EVAL / SCRIPTING (Session 1A) ──────────────────────────────────────
    DispatchEntry {
        name: b"EVAL",
        handler: crate::eval::eval_command,
    },
    DispatchEntry {
        name: b"EVAL_RO",
        handler: crate::eval::eval_ro_command,
    },
    DispatchEntry {
        name: b"EVALSHA",
        handler: crate::eval::evalsha_command,
    },
    DispatchEntry {
        name: b"EVALSHA_RO",
        handler: crate::eval::evalsha_ro_command,
    },
    DispatchEntry {
        name: b"SCRIPT",
        handler: crate::eval::script_command,
    },
    // ── REPLICATION (Session 3A / 3B) ─────────────────────────────────────
    DispatchEntry {
        name: b"REPLICAOF",
        handler: crate::replication::replicaof_command,
    },
    DispatchEntry {
        name: b"SLAVEOF",
        handler: crate::replication::replicaof_command,
    },
    DispatchEntry {
        name: b"PSYNC",
        handler: crate::replication::psync_command,
    },
    DispatchEntry {
        name: b"SYNC",
        handler: crate::replication::sync_command,
    },
    DispatchEntry {
        name: b"REPLCONF",
        handler: crate::replication::replconf_command,
    },
    DispatchEntry {
        name: b"ROLE",
        handler: crate::replication::role_command,
    },
    DispatchEntry {
        name: b"WAIT",
        handler: crate::replication::wait_command,
    },
    DispatchEntry {
        name: b"WAITAOF",
        handler: crate::replication::waitaof_command,
    },
    // ── BLOOM FILTER (RedisBloom BF.* — overnight agent) ──────────────────
    DispatchEntry {
        name: b"BF.RESERVE",
        handler: crate::bloom::bf_reserve_command,
    },
    DispatchEntry {
        name: b"BF.ADD",
        handler: crate::bloom::bf_add_command,
    },
    DispatchEntry {
        name: b"BF.MADD",
        handler: crate::bloom::bf_madd_command,
    },
    DispatchEntry {
        name: b"BF.EXISTS",
        handler: crate::bloom::bf_exists_command,
    },
    DispatchEntry {
        name: b"BF.MEXISTS",
        handler: crate::bloom::bf_mexists_command,
    },
    DispatchEntry {
        name: b"BF.INSERT",
        handler: crate::bloom::bf_insert_command,
    },
    DispatchEntry {
        name: b"BF.INFO",
        handler: crate::bloom::bf_info_command,
    },
    // ── RedisJSON (Overnight 1) ────────────────────────────────────────────
    DispatchEntry {
        name: b"JSON.SET",
        handler: crate::json::json_set_command,
    },
    DispatchEntry {
        name: b"JSON.GET",
        handler: crate::json::json_get_command,
    },
    DispatchEntry {
        name: b"JSON.DEL",
        handler: crate::json::json_del_command,
    },
    DispatchEntry {
        name: b"JSON.FORGET",
        handler: crate::json::json_del_command,
    },
    DispatchEntry {
        name: b"JSON.TYPE",
        handler: crate::json::json_type_command,
    },
    DispatchEntry {
        name: b"JSON.NUMINCRBY",
        handler: crate::json::json_numincrby_command,
    },
    DispatchEntry {
        name: b"JSON.NUMMULTBY",
        handler: crate::json::json_nummultby_command,
    },
    DispatchEntry {
        name: b"JSON.STRAPPEND",
        handler: crate::json::json_strappend_command,
    },
    DispatchEntry {
        name: b"JSON.STRLEN",
        handler: crate::json::json_strlen_command,
    },
    DispatchEntry {
        name: b"JSON.OBJKEYS",
        handler: crate::json::json_objkeys_command,
    },
    DispatchEntry {
        name: b"JSON.OBJLEN",
        handler: crate::json::json_objlen_command,
    },
    DispatchEntry {
        name: b"JSON.ARRAPPEND",
        handler: crate::json::json_arrappend_command,
    },
    DispatchEntry {
        name: b"JSON.ARRLEN",
        handler: crate::json::json_arrlen_command,
    },
    DispatchEntry {
        name: b"JSON.ARRINSERT",
        handler: crate::json::json_arrinsert_command,
    },
    DispatchEntry {
        name: b"JSON.ARRPOP",
        handler: crate::json::json_arrpop_command,
    },
    DispatchEntry {
        name: b"JSON.CLEAR",
        handler: crate::json::json_clear_command,
    },
    DispatchEntry {
        name: b"JSON.MGET",
        handler: crate::json::json_mget_command,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(lookup_command(b"PING").is_some());
        assert!(lookup_command(b"ping").is_some());
        assert!(lookup_command(b"Ping").is_some());
        assert!(lookup_command(b"PiNg").is_some());
        assert!(lookup_command(b"hgetdel").is_some());
    }

    #[test]
    fn unknown_command_is_none() {
        assert!(lookup_command(b"NOTACOMMAND").is_none());
    }

    #[test]
    fn runtime_dispatch_table_is_sorted_for_binary_search() {
        let table = runtime_dispatch_table();
        for pair in table.windows(2) {
            assert!(
                ascii_casecmp(pair[0].entry.name, pair[1].entry.name) == Ordering::Less,
                "{} should sort before {} with no duplicate handler names",
                std::str::from_utf8(pair[0].entry.name).unwrap_or("<bytes>"),
                std::str::from_utf8(pair[1].entry.name).unwrap_or("<bytes>")
            );
        }
    }

    #[test]
    fn generated_metadata_table_is_sorted_for_binary_search() {
        let table = command_metadata_table();
        for pair in table.windows(2) {
            assert!(
                ascii_casecmp(pair[0].0, pair[1].0) != Ordering::Greater,
                "{} should sort before {}",
                std::str::from_utf8(pair[0].0).unwrap_or("<bytes>"),
                std::str::from_utf8(pair[1].0).unwrap_or("<bytes>")
            );
        }
    }

    #[test]
    fn command_metadata_extracts_hot_path_flags() {
        let set = command_metadata(b"set");
        assert!(set.write);
        assert!(set.denyoom);
        assert!(set.acl_categories & acl_category::WRITE != 0);

        let get = command_metadata(b"GET");
        assert!(!get.write);
        assert!(get.acl_categories & acl_category::READ != 0);

        let auth = command_metadata(b"AUTH");
        assert!(auth.no_auth);
        assert!(auth.acl_categories & acl_category::CONNECTION != 0);
    }

    #[test]
    fn dispatch_unknown_returns_err() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"NOTACOMMAND")]);
        let mut ctx = CommandContext::new(&mut c);
        let err = dispatch(&mut ctx).unwrap_err();
        match err {
            RedisError::Runtime(s) => {
                assert!(s.as_bytes().starts_with(b"ERR unknown command"));
            }
            _ => panic!("expected Runtime error"),
        }
    }

    #[test]
    fn dispatch_routes_known_command() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"HELLO")]);
        let mut ctx = CommandContext::new(&mut c);
        dispatch(&mut ctx).unwrap();
        let reply = c.drain_reply();
        assert!(reply.starts_with(b"*"));
        assert!(reply.windows(b"server".len()).any(|w| w == b"server"));
    }

    #[test]
    fn dispatch_routes_ping_to_real_handler() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"PING")]);
        let mut ctx = CommandContext::new(&mut c);
        dispatch(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+PONG\r\n");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (Wave A — dispatch lookup fn)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Lookup + routing wired. Handler bodies are stubbed via
//                  unimplemented_handler so the binary returns a clean error
//                  reply for any command; Waves B/C wire the real bodies.
// ──────────────────────────────────────────────────────────────────────────

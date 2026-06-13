//! AUTO-EXTRACTED from connection.rs by refactor/file-structure-splits phase 1.5.
#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    acl_log_entries, acl_log_max_len, acl_log_now_millis, acl_pubsub_default_config_value,
    apply_acl_pubsub_default_to_user, category as acl_category, category_name_to_bit,
    clear_acl_log, global_acl_state, hex_to_hash, record_acl_log_entry, set_acl_log_max_len,
    set_acl_pubsub_default, sha256_hash, AclKeyPattern, AclLogEntry, AclUser, ACL_KEY_READ,
    ACL_KEY_READ_WRITE, ACL_KEY_WRITE, ALL_CATEGORY_NAMES,
};
use redis_core::blocked_keys::{blocked_keys_index, BlockedAction};
use redis_core::client_info::client_info_registry;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::{
    record_acl_access_denied_auth, record_blocked_command_rejected, record_error_reply,
    server_metrics,
};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::{keyspace_events_string_to_flags, NOTIFY_EVICTED};
use redis_core::object::object_compute_size;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::acl_cmd::*;
use crate::client_limits::*;
use crate::command_meta::*;
use crate::config_cmd::*;
use crate::connection::*;
use crate::debug_cmd::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::listeners::*;
use crate::live_config_handle;
use crate::shutdown_signals::*;

#[derive(Default)]
pub struct ClientListFilters {
    ids: Vec<u64>,
    not_ids: Vec<u64>,
    addr: Option<Vec<u8>>,
    not_addr: Option<Vec<u8>>,
    laddr: Option<Vec<u8>>,
    not_laddr: Option<Vec<u8>>,
    type_filter: Option<ClientTypeFilter>,
    not_type_filter: Option<ClientTypeFilter>,
    name: Option<Vec<u8>>,
    not_name: Option<Vec<u8>>,
    flags: Option<Vec<u8>>,
    not_flags: Option<Vec<u8>>,
    user: Option<Vec<u8>>,
    not_user: Option<Vec<u8>>,
    skipme: Option<bool>,
    maxage: Option<i64>,
    idle: Option<i64>,
    ip: Option<Vec<u8>>,
    not_ip: Option<Vec<u8>>,
    capa: Option<Vec<u8>>,
    not_capa: Option<Vec<u8>>,
    lib_name: Option<Vec<u8>>,
    not_lib_name: Option<Vec<u8>>,
    lib_ver: Option<Vec<u8>>,
    not_lib_ver: Option<Vec<u8>>,
    db: Option<u32>,
    not_db: Option<u32>,
}

pub fn client_kill_only_skipme_filter(filters: &ClientListFilters) -> bool {
    filters.skipme.is_some()
        && filters.ids.is_empty()
        && filters.not_ids.is_empty()
        && filters.addr.is_none()
        && filters.not_addr.is_none()
        && filters.laddr.is_none()
        && filters.not_laddr.is_none()
        && filters.type_filter.is_none()
        && filters.not_type_filter.is_none()
        && filters.name.is_none()
        && filters.not_name.is_none()
        && filters.flags.is_none()
        && filters.not_flags.is_none()
        && filters.user.is_none()
        && filters.not_user.is_none()
        && filters.maxage.is_none()
        && filters.idle.is_none()
        && filters.ip.is_none()
        && filters.not_ip.is_none()
        && filters.capa.is_none()
        && filters.not_capa.is_none()
        && filters.lib_name.is_none()
        && filters.not_lib_name.is_none()
        && filters.lib_ver.is_none()
        && filters.not_lib_ver.is_none()
        && filters.db.is_none()
        && filters.not_db.is_none()
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ClientTypeFilter {
    Normal,
    Replica,
    PubSub,
    Primary,
}

pub fn client_user_bytes(client: &redis_core::client::Client) -> &[u8] {
    client
        .authenticated_user
        .as_ref()
        .map(|u| u.as_bytes())
        .unwrap_or(b"default")
}

pub fn client_name_bytes(client: &redis_core::client::Client) -> Option<&[u8]> {
    client.name.as_ref().map(|n| n.as_bytes())
}

pub fn client_flags_vec(client: &redis_core::client::Client) -> Vec<u8> {
    let mut out = Vec::new();
    if client.is_replica {
        out.push(b'S');
    }
    if client.in_pubsub_mode() {
        out.push(b'P');
    }
    if client.flag_multi() {
        out.push(b'x');
    }
    if client.blocked_on_keys || client.flag_blocked() {
        out.push(b'b');
    }
    if client.import_source {
        out.push(b'I');
    }
    if client.tracking.enabled {
        out.push(b't');
    }
    if client.tracking.bcast {
        out.push(b'B');
    }
    if client.tracking.broken_redirect {
        out.push(b'R');
    }
    if client.flags.monitor {
        out.push(b'O');
    }
    if client.flags.readonly {
        out.push(b'r');
    }
    if client.flags.no_touch {
        out.push(b'T');
    }
    if client.flags.dirty_cas {
        out.push(b'd');
    }
    if out.is_empty() {
        out.push(b'N');
    }
    out
}

pub fn watched_key_count_for_client(client_id: u64) -> usize {
    let idx = redis_core::db::watched_keys_index();
    let guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .watched
        .values()
        .filter(|watchers| watchers.contains(&client_id))
        .count()
}

pub fn client_capa_vec(client: &redis_core::client::Client) -> Vec<u8> {
    if client.capa_redirect {
        b"r".to_vec()
    } else {
        Vec::new()
    }
}

pub fn snapshot_in_pubsub_mode(snap: &redis_core::client_info::ClientSnapshot) -> bool {
    snap.subscribed_channels > 0
        || snap.subscribed_patterns > 0
        || snap.subscribed_shard_channels > 0
        || snap.cmd == "subscribe"
        || snap.cmd == "psubscribe"
        || snap.cmd == "ssubscribe"
}

pub fn snapshot_flags_vec(snap: &redis_core::client_info::ClientSnapshot) -> Vec<u8> {
    let mut out = Vec::new();
    if snap.is_replica {
        out.push(b'S');
    }
    if snapshot_in_pubsub_mode(snap) {
        out.push(b'P');
    }
    if snap.queued_multi_count.is_some() {
        out.push(b'x');
    }
    if snap.blocked {
        out.push(b'b');
    }
    if snap.import_source {
        out.push(b'I');
    }
    if snap.tracking {
        out.push(b't');
    }
    if snap.tracking_bcast {
        out.push(b'B');
    }
    if snap.tracking_broken_redirect {
        out.push(b'R');
    }
    if snap.readonly {
        out.push(b'r');
    }
    if out.is_empty() {
        out.push(b'N');
    }
    out
}

pub fn snapshot_type(snap: &redis_core::client_info::ClientSnapshot) -> ClientTypeFilter {
    if snap.is_replica {
        ClientTypeFilter::Replica
    } else if snapshot_in_pubsub_mode(snap) {
        ClientTypeFilter::PubSub
    } else {
        ClientTypeFilter::Normal
    }
}

pub fn current_client_type(client: &redis_core::client::Client) -> ClientTypeFilter {
    if client.is_replica {
        ClientTypeFilter::Replica
    } else if client.in_pubsub_mode() {
        ClientTypeFilter::PubSub
    } else {
        ClientTypeFilter::Normal
    }
}

pub fn client_tracking_redir(client: &redis_core::client::Client) -> i64 {
    if client.tracking.enabled {
        client.tracking.redirect
    } else {
        -1
    }
}

pub fn reported_reply_buffer_size(net_output_bytes: u64, pending_output_bytes: usize) -> usize {
    if pending_output_bytes > 0 || net_output_bytes >= 32 * 1024 {
        16 * 1024
    } else {
        1024
    }
}

pub fn format_current_client_info_line(
    client: &redis_core::client::Client,
    command_name: &[u8],
) -> Vec<u8> {
    let mut line = Vec::with_capacity(320);
    let addr = client.addr.as_deref().unwrap_or("127.0.0.1:0");
    let flags = client_flags_vec(client);
    let capa = client_capa_vec(client);
    let multi = if client.flag_multi() {
        client.queued_argvs.len() as i64
    } else {
        -1
    };
    let watch = watched_key_count_for_client(client.id);
    let rbs = reported_reply_buffer_size(client.net_output_bytes, client.reply_buf.len());
    let _ = write!(line, "id={} addr={}", client.id, addr);
    line.extend_from_slice(b" laddr=127.0.0.1:0 fd=0 name=");
    if let Some(name) = &client.name {
        line.extend_from_slice(name.as_bytes());
    }
    line.extend_from_slice(b" age=0 idle=0 flags=");
    line.extend_from_slice(&flags);
    line.extend_from_slice(b" capa=");
    line.extend_from_slice(&capa);
    let _ = write!(
        line,
        " db={} sub={} psub={} ssub={} multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs={} rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=",
        client.db_index,
        client.subscribed_channels.len(),
        client.subscribed_patterns.len(),
        client.subscribed_shard_channels.len(),
        multi,
        watch,
        rbs,
    );
    line.extend_from_slice(command_name);
    line.extend_from_slice(b" user=");
    line.extend_from_slice(client_user_bytes(client));
    let _ = write!(
        line,
        " redir={} resp={} lib-name=",
        client_tracking_redir(client),
        client.resp_proto,
    );
    if let Some(lib_name) = &client.lib_name {
        line.extend_from_slice(lib_name.as_bytes());
    }
    line.extend_from_slice(b" lib-ver=");
    if let Some(lib_ver) = &client.lib_ver {
        line.extend_from_slice(lib_ver.as_bytes());
    }
    let _ = writeln!(
        line,
        " tot-net-in={} tot-net-out={} tot-cmds={}",
        client.net_input_bytes, client.net_output_bytes, client.commands_processed
    );
    line
}

pub fn format_snapshot_client_info_line(
    snap: &redis_core::client_info::ClientSnapshot,
    command_name: &[u8],
) -> Vec<u8> {
    let mut line = Vec::with_capacity(320);
    let flags = snapshot_flags_vec(snap);
    let capa = if snap.capa_redirect {
        b"r".as_slice()
    } else {
        b"".as_slice()
    };
    let multi = snap.queued_multi_count.map(|n| n as i64).unwrap_or(-1);
    let rbs = reported_reply_buffer_size(snap.net_output_bytes, snap.output_buffer_bytes);
    let output_list_len = usize::from(snap.output_buffer_bytes > 0);
    let _ = write!(
        line,
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name=",
        snap.id, snap.addr,
    );
    if let Some(name) = &snap.name {
        line.extend_from_slice(name.as_bytes());
    }
    let _ = write!(line, " age=0 idle={} flags=", snap.idle_seconds);
    line.extend_from_slice(&flags);
    line.extend_from_slice(b" capa=");
    line.extend_from_slice(capa);
    let _ = write!(
        line,
        " db={} sub={} psub={} ssub={} multi={} watch=0 qbuf={} qbuf-free=0 argv-mem={} multi-mem={} rbs={} rbp=0 obl={} oll={} omem={} tot-mem={} events=r cmd=",
        snap.db_index,
        snap.subscribed_channels,
        snap.subscribed_patterns,
        snap.subscribed_shard_channels,
        multi,
        snap.query_buffer_bytes,
        snap.argv_memory_bytes,
        snap.multi_memory_bytes,
        rbs,
        snap.output_buffer_bytes,
        output_list_len,
        snap.output_buffer_bytes,
        snap.total_memory_bytes,
    );
    line.extend_from_slice(command_name);
    line.extend_from_slice(b" user=");
    if let Some(user) = &snap.user {
        line.extend_from_slice(user.as_bytes());
    } else {
        line.extend_from_slice(b"default");
    }
    let _ = write!(line, " redir=-1 resp={} lib-name=", snap.resp_proto);
    if let Some(lib_name) = &snap.lib_name {
        line.extend_from_slice(lib_name.as_bytes());
    }
    line.extend_from_slice(b" lib-ver=");
    if let Some(lib_ver) = &snap.lib_ver {
        line.extend_from_slice(lib_ver.as_bytes());
    }
    let _ = writeln!(
        line,
        " tot-net-in={} tot-net-out={} tot-cmds={}",
        snap.net_input_bytes, snap.net_output_bytes, snap.commands_processed
    );
    line
}

pub fn unknown_client_type_error(value: &[u8]) -> RedisError {
    let mut msg = b"ERR Unknown client type '".to_vec();
    msg.extend_from_slice(value);
    msg.push(b'\'');
    RedisError::runtime(msg)
}

pub fn no_such_user_error(value: &[u8]) -> RedisError {
    let mut msg = b"ERR No such user '".to_vec();
    msg.extend_from_slice(value);
    msg.push(b'\'');
    RedisError::runtime(msg)
}

pub fn append_value_error(prefix: &[u8], value: &[u8]) -> RedisError {
    let mut msg = prefix.to_vec();
    msg.extend_from_slice(value);
    RedisError::runtime(msg)
}

pub fn parse_client_type(value: &[u8]) -> Option<ClientTypeFilter> {
    if ascii_eq_ignore_case(value, b"normal") {
        Some(ClientTypeFilter::Normal)
    } else if ascii_eq_ignore_case(value, b"replica") || ascii_eq_ignore_case(value, b"slave") {
        Some(ClientTypeFilter::Replica)
    } else if ascii_eq_ignore_case(value, b"pubsub") {
        Some(ClientTypeFilter::PubSub)
    } else if ascii_eq_ignore_case(value, b"primary") || ascii_eq_ignore_case(value, b"master") {
        Some(ClientTypeFilter::Primary)
    } else {
        None
    }
}

pub fn parse_positive_i64_for_client_filter(value: &[u8], name: &[u8]) -> RedisResult<i64> {
    let Some(parsed) = parse_i64_strict(value) else {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" is not an integer or out of range");
        return Err(RedisError::runtime(msg));
    };
    if parsed <= 0 {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" should be greater than 0");
        return Err(RedisError::runtime(msg));
    }
    Ok(parsed)
}

pub fn parse_db_filter(ctx: &CommandContext<'_>, value: &[u8], name: &[u8]) -> RedisResult<u32> {
    let Some(parsed) = parse_i64_strict(value) else {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" is not an integer or out of range");
        return Err(RedisError::runtime(msg));
    };
    if parsed < 0 || parsed >= ctx.database_count() as i64 {
        let max = ctx.database_count().saturating_sub(1);
        let mut msg = Vec::new();
        msg.extend_from_slice(b"ERR ");
        msg.extend_from_slice(name);
        let _ = write!(msg, " number should be between 0 and {}", max);
        return Err(RedisError::runtime(msg));
    }
    Ok(parsed as u32)
}

pub fn flags_match(actual: &[u8], filter: &[u8]) -> bool {
    filter.iter().all(|b| actual.contains(b))
}

pub fn option_bytes_matches(actual: Option<&RedisString>, expected: &[u8]) -> bool {
    actual
        .map(|value| value.as_bytes() == expected)
        .unwrap_or(false)
}

pub fn option_bytes_not_matches(actual: Option<&RedisString>, expected: &[u8]) -> bool {
    actual
        .map(|value| value.as_bytes() != expected)
        .unwrap_or(true)
}

pub fn refresh_client_info_registry(client: &redis_core::client::Client) {
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.update_client_metadata(client);
    }
}

pub fn require_filter_value(ctx: &CommandContext<'_>, idx: usize) -> RedisResult<RedisString> {
    if idx + 1 >= ctx.arg_count() {
        return Err(RedisError::syntax(b"syntax error"));
    }
    ctx.arg_owned(idx + 1)
}

pub fn parse_client_list_filters(ctx: &CommandContext<'_>) -> RedisResult<ClientListFilters> {
    let mut filters = ClientListFilters::default();
    let mut idx = 2usize;
    while idx < ctx.arg_count() {
        let opt = ctx.arg(idx)?;
        let opt_bytes = opt.as_bytes();
        if opt_bytes.eq_ignore_ascii_case(b"ID") || opt_bytes.eq_ignore_ascii_case(b"NOT-ID") {
            let negative = opt_bytes.eq_ignore_ascii_case(b"NOT-ID");
            idx += 1;
            let mut saw_id = false;
            while idx < ctx.arg_count() {
                let raw = ctx.arg(idx)?;
                let Some(id) = parse_i64_strict(raw.as_bytes()) else {
                    break;
                };
                if id < 1 {
                    return Err(RedisError::runtime(
                        b"ERR client-id should be greater than 0",
                    ));
                }
                if negative {
                    filters.not_ids.push(id as u64);
                } else {
                    filters.ids.push(id as u64);
                }
                saw_id = true;
                idx += 1;
            }
            if !saw_id {
                return Err(RedisError::syntax(b"syntax error"));
            }
        } else if opt_bytes.eq_ignore_ascii_case(b"ADDR") {
            filters.addr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-ADDR") {
            filters.not_addr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LADDR") {
            filters.laddr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LADDR") {
            filters.not_laddr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"TYPE") {
            let value = require_filter_value(ctx, idx)?;
            filters.type_filter = Some(
                parse_client_type(value.as_bytes())
                    .ok_or_else(|| unknown_client_type_error(value.as_bytes()))?,
            );
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-TYPE") {
            let value = require_filter_value(ctx, idx)?;
            filters.not_type_filter = Some(
                parse_client_type(value.as_bytes())
                    .ok_or_else(|| unknown_client_type_error(value.as_bytes()))?,
            );
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"USER") {
            let value = require_filter_value(ctx, idx)?;
            if !acl_user_exists(value.as_bytes()) {
                return Err(no_such_user_error(value.as_bytes()));
            }
            filters.user = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-USER") {
            let value = require_filter_value(ctx, idx)?;
            if !acl_user_exists(value.as_bytes()) {
                return Err(no_such_user_error(value.as_bytes()));
            }
            filters.not_user = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"SKIPME") {
            let value = require_filter_value(ctx, idx)?;
            if value.as_bytes().eq_ignore_ascii_case(b"yes") {
                filters.skipme = Some(true);
            } else if value.as_bytes().eq_ignore_ascii_case(b"no") {
                filters.skipme = Some(false);
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"MAXAGE") {
            let value = require_filter_value(ctx, idx)?;
            filters.maxage = Some(parse_positive_i64_for_client_filter(
                value.as_bytes(),
                b"maxage",
            )?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"IDLE") {
            let value = require_filter_value(ctx, idx)?;
            filters.idle = Some(parse_positive_i64_for_client_filter(
                value.as_bytes(),
                b"idle",
            )?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"FLAGS") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_flag_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown flags found in the provided filter: ",
                    value.as_bytes(),
                ));
            }
            filters.flags = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-FLAGS") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_flag_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown flags found in the NOT-FLAGS filter: ",
                    value.as_bytes(),
                ));
            }
            filters.not_flags = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NAME") {
            filters.name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-NAME") {
            filters.not_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"IP") {
            filters.ip = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-IP") {
            filters.not_ip = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"CAPA") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_capa_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown capa found in the provided filter: ",
                    value.as_bytes(),
                ));
            }
            filters.capa = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-CAPA") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_capa_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown capa found in the NOT-CAPA filter: ",
                    value.as_bytes(),
                ));
            }
            filters.not_capa = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LIB-NAME") {
            filters.lib_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LIB-NAME") {
            filters.not_lib_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LIB-VER") {
            filters.lib_ver = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LIB-VER") {
            filters.not_lib_ver = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"DB") {
            let value = require_filter_value(ctx, idx)?;
            filters.db = Some(parse_db_filter(ctx, value.as_bytes(), b"DB")?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-DB") {
            let value = require_filter_value(ctx, idx)?;
            filters.not_db = Some(parse_db_filter(ctx, value.as_bytes(), b"NOT-DB")?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    Ok(filters)
}

pub fn current_client_matches_filters(
    client: &redis_core::client::Client,
    filters: &ClientListFilters,
) -> bool {
    if filters.skipme == Some(true) {
        return false;
    }
    if !filters.ids.is_empty() && !filters.ids.contains(&client.id) {
        return false;
    }
    if !filters.not_ids.is_empty() && filters.not_ids.contains(&client.id) {
        return false;
    }
    let addr = client.addr.as_deref().unwrap_or("127.0.0.1:0").as_bytes();
    if let Some(expected) = &filters.addr {
        if expected.as_slice() != addr {
            return false;
        }
    }
    if let Some(expected) = &filters.not_addr {
        if expected.as_slice() == addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected.as_slice() != b"127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.not_laddr {
        if expected.as_slice() == b"127.0.0.1:0" {
            return false;
        }
    }
    let client_type = current_client_type(client);
    if let Some(expected) = filters.type_filter {
        if expected != client_type {
            return false;
        }
    }
    if let Some(expected) = filters.not_type_filter {
        if expected == client_type {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if client_name_bytes(client) != Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_name {
        if client_name_bytes(client) == Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        if expected.as_slice() != client_user_bytes(client) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_user {
        if expected.as_slice() == client_user_bytes(client) {
            return false;
        }
    }
    if let Some(maxage) = filters.maxage {
        if 0 < maxage {
            return false;
        }
    }
    if let Some(_idle) = filters.idle {
 // The current client snapshot does not yet track second-granularity
 // idle time. Treat the filter as satisfied; the other supplied filters
 // still narrow the target set and CLIENT LIST continues to render idle=0.
    }
    if let Some(expected) = &filters.flags {
        let actual = client_flags_vec(client);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_flags {
        let actual = client_flags_vec(client);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.ip {
        if !client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_ip {
        if client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.capa {
        let actual = client_capa_vec(client);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_capa {
        let actual = client_capa_vec(client);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_name {
        if !option_bytes_matches(client.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_name {
        if !option_bytes_not_matches(client.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_ver {
        if !option_bytes_matches(client.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_ver {
        if !option_bytes_not_matches(client.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = filters.db {
        if client.db_index != expected {
            return false;
        }
    }
    if let Some(expected) = filters.not_db {
        if client.db_index == expected {
            return false;
        }
    }
    true
}

pub fn snapshot_matches_filters(
    snap: &redis_core::client_info::ClientSnapshot,
    filters: &ClientListFilters,
) -> bool {
    if !filters.ids.is_empty() && !filters.ids.contains(&snap.id) {
        return false;
    }
    if !filters.not_ids.is_empty() && filters.not_ids.contains(&snap.id) {
        return false;
    }
    let addr = snap.addr.as_bytes();
    if let Some(expected) = &filters.addr {
        if expected.as_slice() != addr {
            return false;
        }
    }
    if let Some(expected) = &filters.not_addr {
        if expected.as_slice() == addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected.as_slice() != b"127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.not_laddr {
        if expected.as_slice() == b"127.0.0.1:0" {
            return false;
        }
    }
    let client_type = snapshot_type(snap);
    if let Some(expected) = filters.type_filter {
        if expected != client_type {
            return false;
        }
    }
    if let Some(expected) = filters.not_type_filter {
        if expected == client_type {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        let actual = snap
            .user
            .as_ref()
            .map(|u| u.as_bytes())
            .unwrap_or(b"default");
        if expected.as_slice() != actual {
            return false;
        }
    }
    if let Some(expected) = &filters.not_user {
        let actual = snap
            .user
            .as_ref()
            .map(|u| u.as_bytes())
            .unwrap_or(b"default");
        if expected.as_slice() == actual {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if snap.name.as_ref().map(|n| n.as_bytes()) != Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_name {
        if snap.name.as_ref().map(|n| n.as_bytes()) == Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(maxage) = filters.maxage {
        if 0 < maxage {
            return false;
        }
    }
    if let Some(_idle) = filters.idle {
 // See current-client path above: idle accounting is not yet persisted
 // in the cross-thread snapshot, so we preserve filter syntax and let
 // the other predicates define the matched set.
    }
    if let Some(expected) = &filters.flags {
        let actual = snapshot_flags_vec(snap);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_flags {
        let actual = snapshot_flags_vec(snap);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.ip {
        if !client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_ip {
        if client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.capa {
        let actual = if snap.capa_redirect {
            b"r".as_slice()
        } else {
            b"".as_slice()
        };
        if !flags_match(actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_capa {
        let actual = if snap.capa_redirect {
            b"r".as_slice()
        } else {
            b"".as_slice()
        };
        if flags_match(actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_name {
        if !option_bytes_matches(snap.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_name {
        if !option_bytes_not_matches(snap.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_ver {
        if !option_bytes_matches(snap.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_ver {
        if !option_bytes_not_matches(snap.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = filters.db {
        if snap.db_index != expected {
            return false;
        }
    }
    if let Some(expected) = filters.not_db {
        if snap.db_index == expected {
            return false;
        }
    }
    true
}

/// `CLIENT <subcommand> [args]`.
/// Pilot subset:
/// * `CLIENT ID` — integer reply of the client's connection id.
/// * `CLIENT GETNAME` — bulk reply of the stored name (nil bulk when unset).
/// * `CLIENT SETNAME name` — store the name; replies `+OK\r\n`.
/// * `CLIENT NO-EVICT ON|OFF` — no-op, replies `+OK\r\n`.
/// * `CLIENT NO-TOUCH ON|OFF` — no-op, replies `+OK\r\n`.
/// * `CLIENT LIST` — single-line description of the current client.
pub fn client_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"client"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ignore_case(sub_bytes, b"ID") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|id"));
        }
        let id = ctx.client_ref().id() as i64;
        return ctx.reply_integer(id);
    }
    if ascii_eq_ignore_case(sub_bytes, b"GETNAME") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|getname"));
        }
        let name = ctx.client_ref().name.clone();
        return match name {
            Some(n) => ctx.reply_bulk_string(n),
            None => ctx.reply_null_bulk(),
        };
    }
    if ascii_eq_ignore_case(sub_bytes, b"SETNAME") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|setname"));
        }
        let name = ctx.arg_owned(2usize)?;
        validate_client_name(name.as_bytes())?;
        ctx.client_mut().name = Some(name);
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"NO-EVICT") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|no-evict"));
        }
        let flag = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(flag.as_bytes(), b"ON")
            && !ascii_eq_ignore_case(flag.as_bytes(), b"OFF")
        {
            return Err(RedisError::syntax(b""));
        }
        ctx.client_mut().flags.no_evict = ascii_eq_ignore_case(flag.as_bytes(), b"ON");
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"NO-TOUCH") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|no-touch"));
        }
        let flag = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(flag.as_bytes(), b"ON")
            && !ascii_eq_ignore_case(flag.as_bytes(), b"OFF")
        {
            return Err(RedisError::syntax(b""));
        }
        ctx.client_mut().flags.no_touch = ascii_eq_ignore_case(flag.as_bytes(), b"ON");
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"CAPA") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"client|capa"));
        }
        for i in 2..ctx.arg_count() {
            let opt = ctx.arg_owned(i)?;
            if ascii_eq_ignore_case(opt.as_bytes(), b"REDIRECT") {
                ctx.client_mut().capa_redirect = true;
            }
        }
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"SETINFO") {
        if ctx.arg_count() != 4 {
            return Err(RedisError::wrong_number_of_args(b"client|setinfo"));
        }
        let attr = ctx.arg_owned(2usize)?;
        let value = ctx.arg_owned(3usize)?;
        if ascii_eq_ignore_case(attr.as_bytes(), b"LIB-NAME") {
            validate_client_setinfo_attr(b"lib-name", value.as_bytes())?;
            ctx.client_mut().lib_name = Some(value);
        } else if ascii_eq_ignore_case(attr.as_bytes(), b"LIB-VER") {
            validate_client_setinfo_attr(b"lib-ver", value.as_bytes())?;
            ctx.client_mut().lib_ver = Some(value);
        } else {
            let mut msg = b"ERR Unrecognized option '".to_vec();
            msg.extend_from_slice(attr.as_bytes());
            msg.push(b'\'');
            return Err(RedisError::runtime(msg));
        }
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"INFO") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|info"));
        }
        let line = format_current_client_info_line(ctx.client_ref(), b"client|info");
        return ctx.reply_bulk(&line);
    }
    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        let filters = parse_client_list_filters(ctx)?;
        let snapshots = {
            let guard = match client_info_registry().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.all()
        };
        let mut snapshot_lines: Vec<(bool, u64, Vec<u8>)> = Vec::new();
        let mut current_line: Option<Vec<u8>> = None;
        if current_client_matches_filters(ctx.client_ref(), &filters) {
            current_line = Some(format_current_client_info_line(
                ctx.client_ref(),
                b"client|list",
            ));
        }
        for snap in &snapshots {
            if snap.id == ctx.client_ref().id {
                continue;
            }
            if !snapshot_matches_filters(snap, &filters) {
                continue;
            }
            let cmd = if snap.cmd.is_empty() {
                b"NULL".as_slice()
            } else {
                snap.cmd.as_bytes()
            };
            snapshot_lines.push((
                snapshot_in_pubsub_mode(snap),
                snap.id,
                format_snapshot_client_info_line(snap, cmd),
            ));
        }
        let mut out = Vec::new();
        if let Some(line) = current_line {
            out.extend_from_slice(&line);
        }
        snapshot_lines.sort_by_key(|(is_pubsub, id, _)| (!*is_pubsub, *id));
        for (_, _, line) in snapshot_lines {
            out.extend_from_slice(&line);
        }
        return ctx.reply_bulk(&out);
    }
    if ascii_eq_ignore_case(sub_bytes, b"TRACKING") {
        return client_tracking_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"CACHING") {
        return client_caching_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"GETREDIR") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|getredir"));
        }
        return ctx.reply_integer(client_tracking_redir(ctx.client_ref()));
    }
    if ascii_eq_ignore_case(sub_bytes, b"TRACKINGINFO") {
        return client_trackinginfo_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"IMPORT-SOURCE") {
        return client_import_source_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"REPLY") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|reply"));
        }
        let mode = ctx.arg_owned(2usize)?;
        let flags = &mut ctx.client_mut().flags;
        if ascii_eq_ignore_case(mode.as_bytes(), b"ON") {
            flags.reply_skip = false;
            flags.reply_skip_next = false;
            flags.reply_off = false;
            return ctx.reply_simple_string(b"OK");
        }
        if ascii_eq_ignore_case(mode.as_bytes(), b"OFF") {
            flags.reply_off = true;
            return Ok(());
        }
        if ascii_eq_ignore_case(mode.as_bytes(), b"SKIP") {
            if !flags.reply_off {
                flags.reply_skip_next = true;
            }
            return Ok(());
        }
        return Err(RedisError::syntax(b""));
    }
    if ascii_eq_ignore_case(sub_bytes, b"UNBLOCK") {
        if ctx.arg_count() < 3 || ctx.arg_count() > 4 {
            return Err(RedisError::wrong_number_of_args(b"client|unblock"));
        }
        let id_arg = ctx.arg_owned(2usize)?;
        let Some(client_id) = parse_i64_strict(id_arg.as_bytes()) else {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        };
        if client_id < 0 {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        }
        let mut error_mode = false;
        if ctx.arg_count() == 4 {
            let mode = ctx.arg_owned(3usize)?;
            if ascii_eq_ignore_case(mode.as_bytes(), b"TIMEOUT") {
                error_mode = false;
            } else if ascii_eq_ignore_case(mode.as_bytes(), b"ERROR") {
                error_mode = true;
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
        }
        let waiter = {
            let mut idx = match blocked_keys_index().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            idx.remove_client(client_id as u64)
        };
        let Some(waiter) = waiter else {
            return ctx.reply_integer(0);
        };
        let reply = if error_mode {
            b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n".to_vec()
        } else {
            waiter.action.timeout_reply_bytes().to_vec()
        };
        let delivered = waiter.sender.send(reply).is_ok();
        if delivered && error_mode {
            record_error_reply(b"UNBLOCKED client unblocked via CLIENT UNBLOCK");
            record_blocked_command_rejected(blocked_action_command_name(&waiter.action));
        }
        return ctx.reply_integer(if delivered { 1 } else { 0 });
    }
    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        let lines: &[&[u8]] = &[
            b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"ID",
            b"    Return the current connection id.",
            b"GETNAME",
            b"    Return the current connection name.",
            b"SETNAME <name>",
            b"    Assign a name to the current connection.",
            b"LIST [options ...]",
            b"    Return information about client connections.",
            b"INFO",
            b"    Return information about the current client connection.",
            b"TRACKING <ON|OFF> [options ...]",
            b"    Enable or disable server assisted client side caching.",
            b"REPLY <ON|OFF|SKIP>",
            b"    Control whether the server replies to commands.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub_bytes, b"PAUSE") {
        return redis_core::networking::client_pause_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"UNPAUSE") {
        return redis_core::networking::client_unpause_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"KILL") {
        return client_kill_command(ctx);
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CLIENT subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CLIENT subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

pub fn client_kill_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"client|kill"));
    }

    let (filters, old_style) = if ctx.arg_count() == 3 {
        let mut filters = ClientListFilters::default();
        filters.addr = Some(ctx.arg_owned(2usize)?.into_bytes());
        filters.skipme = Some(false);
        (filters, true)
    } else {
        let mut filters = parse_client_list_filters(ctx)?;
        if filters.skipme.is_none() {
            filters.skipme = Some(true);
        }
        (filters, false)
    };

    let current_id = ctx.client_ref().id();
    let mut kill_self = current_client_matches_filters(ctx.client_ref(), &filters);
    let snapshots = {
        let guard = match client_info_registry().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.all()
    };

    let mut victim_ids: Vec<u64> = Vec::new();
    for snap in &snapshots {
        if snap.id == current_id {
            continue;
        }
        if snapshot_matches_filters(snap, &filters) {
            victim_ids.push(snap.id);
        }
    }
    if kill_self {
        victim_ids.push(current_id);
    }

    victim_ids.sort_unstable();
    victim_ids.dedup();
    if !victim_ids.contains(&current_id) {
        kill_self = false;
    }
    let killed_replica_link = victim_ids.is_empty()
        && maybe_request_replica_link_drop_for_kill(&filters);
    let mut killed = victim_ids.len() as i64 + i64::from(killed_replica_link);
    if !old_style && client_kill_only_skipme_filter(&filters) {
        let connected = snapshots.len().min(i64::MAX as usize) as i64;
        killed = if filters.skipme == Some(true) {
            connected.saturating_sub(1)
        } else {
            connected
        };
    }

    {
        let mut guard = match client_info_registry().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for id in &victim_ids {
            if *id != current_id {
                guard.mark_killed(*id);
            }
        }
    }

    if old_style {
        if killed == 0 {
            return Err(RedisError::runtime(b"ERR No such client"));
        }
        ctx.reply_simple_string(b"OK")?;
    } else {
        ctx.reply_integer(killed)?;
    }

    if kill_self {
        ctx.client_mut().should_close = true;
    }
    Ok(())
}

fn maybe_request_replica_link_drop_for_kill(filters: &ClientListFilters) -> bool {
    let Some(addr) = filters.addr.as_deref() else {
        return false;
    };
    let repl = redis_core::replication::global_replication_state();
    if !repl.is_replica() {
        return false;
    }
    let Some((host, port)) = repl.replica_of_target() else {
        return false;
    };
    let mut expected = Vec::with_capacity(host.as_bytes().len() + 8);
    expected.extend_from_slice(host.as_bytes());
    expected.push(b':');
    expected.extend_from_slice(port.to_string().as_bytes());
    if addr != expected.as_slice() {
        return false;
    }
    repl.request_replica_link_drop();
    true
}

pub fn client_tracking_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"client|tracking"));
    }
    let mode = ctx.arg_owned(2usize)?;
    let mut redirect: i64 = 0;
    let mut bcast = false;
    let mut optin = false;
    let mut optout = false;
    let mut noloop = false;
    let mut prefixes: Vec<RedisString> = Vec::new();

    let mut idx = 3usize;
    while idx < ctx.arg_count() {
        let opt = ctx.arg_owned(idx)?;
        if ascii_eq_ignore_case(opt.as_bytes(), b"REDIRECT") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            if redirect != 0 {
                return Err(RedisError::runtime(
                    b"ERR A client can only redirect to a single other client",
                ));
            }
            let id = parse_i64_strict(ctx.arg(idx + 1)?.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            if id < 0 {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ));
            }
            redirect = id;
            idx += 2;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"BCAST") {
            bcast = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"OPTIN") {
            optin = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"OPTOUT") {
            optout = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"NOLOOP") {
            noloop = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"PREFIX") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            prefixes.push(ctx.arg_owned(idx + 1)?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    if ascii_eq_ignore_case(mode.as_bytes(), b"OFF") {
        let client_id = ctx.client_ref().id;
        ctx.client_mut().tracking = redis_core::client::ClientTrackingState::default();
        redis_core::tracking::remove_runtime_client_tracking(client_id);
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if !ascii_eq_ignore_case(mode.as_bytes(), b"ON") {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if !bcast && !prefixes.is_empty() {
        return Err(RedisError::runtime(
            b"ERR PREFIX option requires BCAST mode to be enabled",
        ));
    }
    if bcast && (optin || optout) {
        return Err(RedisError::runtime(
            b"ERR OPTIN and OPTOUT are not compatible with BCAST",
        ));
    }
    if optin && optout {
        return Err(RedisError::runtime(
            b"ERR You can't specify both OPTIN mode and OPTOUT mode",
        ));
    }
    let client = ctx.client_mut();
    if client.tracking.enabled && client.tracking.bcast != bcast {
        return Err(RedisError::runtime(
            b"ERR You can't switch BCAST mode on/off before disabling tracking for this client",
        ));
    }
    if client.tracking.enabled
        && ((optin && client.tracking.optout) || (optout && client.tracking.optin))
    {
        return Err(RedisError::runtime(
            b"ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client",
        ));
    }
    if bcast {
        let existing: Option<HashSet<Vec<u8>>> = if client.tracking.enabled && client.tracking.bcast
        {
            Some(
                client
                    .tracking
                    .prefixes
                    .iter()
                    .map(|prefix| prefix.as_bytes().to_vec())
                    .collect(),
            )
        } else {
            None
        };
        let prefix_refs: Vec<&[u8]> = prefixes.iter().map(|prefix| prefix.as_bytes()).collect();
        redis_core::tracking::check_prefix_collisions(&prefix_refs, existing.as_ref())?;
    }
    let mut effective_prefixes = if bcast && client.tracking.enabled && client.tracking.bcast {
        client.tracking.prefixes.clone()
    } else {
        Vec::new()
    };
    if bcast {
        if prefixes.is_empty() {
            if effective_prefixes.is_empty() {
                effective_prefixes.push(RedisString::from_bytes(b""));
            }
        } else {
            for prefix in prefixes {
                if !effective_prefixes
                    .iter()
                    .any(|existing| existing == &prefix)
                {
                    effective_prefixes.push(prefix);
                }
            }
        }
    }
    client.tracking.enabled = true;
    client.tracking.bcast = bcast;
    client.tracking.optin = optin;
    client.tracking.optout = optout;
    client.tracking.noloop = noloop;
    client.tracking.caching = false;
    client.tracking.broken_redirect = false;
    client.tracking.redirect = redirect;
    client.tracking.prefixes = effective_prefixes;
    redis_core::tracking::sync_runtime_client_tracking(client.id, &client.tracking);
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

pub fn client_caching_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"client|caching"));
    }
    let opt = ctx.arg_owned(2usize)?;
    let tracking = &mut ctx.client_mut().tracking;
    if !tracking.enabled || (!tracking.optin && !tracking.optout) {
        return Err(RedisError::runtime(
            b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        ));
    }
    if !ascii_eq_ignore_case(opt.as_bytes(), b"YES") && !ascii_eq_ignore_case(opt.as_bytes(), b"NO")
    {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if ascii_eq_ignore_case(opt.as_bytes(), b"YES") {
        if !tracking.optin {
            return Err(RedisError::runtime(
                b"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            ));
        }
        tracking.caching = true;
    } else {
        if !tracking.optout {
            return Err(RedisError::runtime(
                b"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            ));
        }
        tracking.caching = true;
    }
    redis_core::tracking::sync_runtime_client_tracking(
        ctx.client_ref().id,
        &ctx.client_ref().tracking,
    );
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

pub fn client_trackinginfo_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"client|trackinginfo"));
    }
    if redis_core::tracking::runtime_client_has_broken_redirect(ctx.client_ref().id()) {
        ctx.client_mut().tracking.broken_redirect = true;
    }
    let tracking = &ctx.client_ref().tracking;
    let mut flags = Vec::new();
    let state_flag: &[u8] = if tracking.enabled { b"on" } else { b"off" };
    flags.push(RespFrame::bulk(RedisString::from_bytes(state_flag)));
    if tracking.bcast {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"bcast")));
    }
    if tracking.optin {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"optin")));
        if tracking.caching {
            flags.push(RespFrame::bulk(RedisString::from_bytes(b"caching-yes")));
        }
    }
    if tracking.optout {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"optout")));
        if tracking.caching {
            flags.push(RespFrame::bulk(RedisString::from_bytes(b"caching-no")));
        }
    }
    if tracking.noloop {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"noloop")));
    }
    if tracking.broken_redirect {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"broken_redirect")));
    }
    let prefixes = tracking
        .prefixes
        .iter()
        .cloned()
        .map(RespFrame::bulk)
        .collect();
    ctx.reply_frame(&RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_bytes(b"flags")),
            RespFrame::array(flags),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"redirect")),
            RespFrame::Integer(client_tracking_redir(ctx.client_ref())),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"prefixes")),
            RespFrame::array(prefixes),
        ),
    ]))
}

pub fn client_import_source_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"client|import-source"));
    }
    let value = ctx.arg_owned(2usize)?;
    if ascii_eq_ignore_case(value.as_bytes(), b"ON") {
        if !ctx.server().live_config.import_mode() {
            return Err(RedisError::runtime(b"ERR Server is not in import mode"));
        }
        ctx.client_mut().import_source = true;
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(value.as_bytes(), b"OFF") {
        ctx.client_mut().import_source = false;
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    Err(RedisError::syntax(b"syntax error"))
}

/// Validate a client name per Redis rules: no spaces, newlines, or other
/// whitespace/control characters.
pub fn validate_client_name(name: &[u8]) -> RedisResult<()> {
    for &b in name {
        if b <= 0x20 || b == 0x7f {
            return Err(RedisError::runtime(
                b"ERR Client names cannot contain spaces, newlines or special characters.",
            ));
        }
    }
    Ok(())
}

pub fn validate_client_setinfo_attr(attr_name: &[u8], value: &[u8]) -> RedisResult<()> {
    for &b in value {
        if !(b'!'..=b'~').contains(&b) {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(attr_name);
            msg.extend_from_slice(b" cannot contain spaces, newlines or special characters.");
            return Err(RedisError::runtime(msg));
        }
    }
    Ok(())
}

/// Build the single-line description used by `CLIENT LIST`.
pub fn build_client_list_line(ctx: &CommandContext<'_>) -> Vec<u8> {
    let mut line: Vec<u8> = Vec::with_capacity(128);
    let client = ctx.client_ref();
    let _ = write!(line, "id={} addr=", client.id());
    match &client.addr {
        Some(s) => line.extend_from_slice(s.as_bytes()),
        None => line.extend_from_slice(b""),
    }
    line.extend_from_slice(b" name=");
    if let Some(n) = &client.name {
        line.extend_from_slice(n.as_bytes());
    }
    let _ = write!(line, " db={}", client.db_index);
    line
}

/// Case-insensitive ASCII equality.
pub(crate) fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

pub(crate) fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

pub(crate) fn parse_yes_no(value: &[u8]) -> Option<bool> {
    if ascii_eq_ignore_case(value, b"yes") {
        Some(true)
    } else if ascii_eq_ignore_case(value, b"no") {
        Some(false)
    } else {
        None
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from connection.rs (phase 1.5)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         CLIENT command + 5 subcommands + ClientListFilters + validators.
// ──────────────────────────────────────────────────────────────────────────

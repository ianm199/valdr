//! COMMAND introspection metadata and key-spec helpers.
//!
//! The generated command registry owns source-shaped command metadata. This
//! module turns that metadata into RESP replies for `COMMAND` subcommands and
//! derives key positions for `COMMAND GETKEYS` / `GETKEYSANDFLAGS`.
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
use crate::client_cmd::*;
use crate::client_limits::*;
use crate::config_cmd::*;
use crate::connection::*;
use crate::debug_cmd::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::listeners::*;
use crate::live_config_handle;
use crate::shutdown_signals::*;

/// `COMMAND` / `COMMAND COUNT` / `COMMAND GETKEYS` / `COMMAND GETKEYSANDFLAGS`.
/// `COMMAND` (no args) replies with an array of bulk-string command names
/// drawn from the dispatch table. This compatibility shortcut omits the
/// full per-command metadata array; `COMMAND INFO` is the metadata path used
/// by tests and clients that need flags/key positions.
/// `COMMAND COUNT` replies with the integer length of the dispatch table.
/// `COMMAND LIST` returns the generated command and subcommand names, including
/// `parent|subcommand` full names for source-shaped upstream introspection
/// tests.
/// `COMMAND INFO` returns a compact command-info array; currently
/// load-bearing field is index 2, the flags list.
/// `COMMAND GETKEYS` replies with keys derived from generated command metadata,
/// with SORT/SORT_RO/SET matching their upstream variable key parsing.
pub fn command_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 1 {
        let handlers = crate::dispatch::HANDLERS;
        let mut items: Vec<RespFrame> = Vec::with_capacity(handlers.len());
        for entry in handlers.iter() {
            items.push(RespFrame::bulk(RedisString::from_bytes(entry.name)));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }
    let sub = ctx.arg_owned(1usize)?;
    if ascii_eq_ignore_case(sub.as_bytes(), b"HELP") {
        let lines: &[&[u8]] = &[
            b"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"COUNT",
            b"    Return the total number of commands in this server.",
            b"LIST [FILTERBY MODULE|ACLCAT|PATTERN <arg>]",
            b"    Return a list of command names.",
            b"INFO [<command-name> ...]",
            b"    Return command metadata.",
            b"GETKEYS <full-command>",
            b"    Return the keys from a full command.",
            b"GETKEYSANDFLAGS <full-command>",
            b"    Return the keys and access flags from a full command.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"COUNT") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"command|count"));
        }
        let n = crate::dispatch::HANDLERS.len() as i64;
        return ctx.reply_integer(n);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"LIST") {
        return command_list(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"INFO") {
        return command_info(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"GETKEYS") {
        return command_getkeys(ctx, false);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"GETKEYSANDFLAGS") {
        return command_getkeys(ctx, true);
    }
    let mut msg =
        Vec::with_capacity(b"ERR Unknown COMMAND subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown COMMAND subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
}

pub enum CommandListFilter<'a> {
    None,
    Module,
    AclCategory(Option<u64>),
    Pattern(&'a [u8]),
}

pub fn command_list(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let filter = match ctx.arg_count() {
        2 => CommandListFilter::None,
        5 => {
            if !ascii_eq_ignore_case(ctx.arg(2)?.as_bytes(), b"FILTERBY") {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let filter_type = ctx.arg(3)?.as_bytes();
            let filter_arg = ctx.arg(4)?.as_bytes();
            if ascii_eq_ignore_case(filter_type, b"MODULE") {
                CommandListFilter::Module
            } else if ascii_eq_ignore_case(filter_type, b"ACLCAT") {
                CommandListFilter::AclCategory(category_name_to_bit(filter_arg))
            } else if ascii_eq_ignore_case(filter_type, b"PATTERN") {
                CommandListFilter::Pattern(filter_arg)
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
        }
        _ => return Err(RedisError::syntax(b"syntax error")),
    };

    let mut names = command_list_names(&filter);
    names.sort();
    names.dedup();
    let items = names
        .into_iter()
        .map(|name| RespFrame::bulk(RedisString::from_vec(name)))
        .collect();
    ctx.reply_frame(&RespFrame::array(items))
}

pub fn command_list_names(filter: &CommandListFilter<'_>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for spec in COMMANDS.iter() {
        let name = command_full_name(spec);
        if command_list_filter_allows(filter, spec, &name) {
            out.push(name);
        }
    }
    out
}

pub fn command_list_filter_allows(
    filter: &CommandListFilter<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    full_name: &[u8],
) -> bool {
    match filter {
        CommandListFilter::None => true,
        CommandListFilter::Module => false,
        CommandListFilter::AclCategory(Some(bit)) => spec.acl_categories.iter().any(|&cat| {
            let cat_bit = generated_acl_category_bit(cat);
            cat_bit & bit != 0
        }),
        CommandListFilter::AclCategory(None) => false,
        CommandListFilter::Pattern(pattern) => glob_match_ascii_ci(pattern, full_name),
    }
}

pub fn command_info(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 2 {
        let mut items = Vec::new();
        for spec in COMMANDS.iter() {
            items.push(command_info_frame(spec));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }

    let mut items = Vec::with_capacity(ctx.arg_count().saturating_sub(2));
    for i in 2..ctx.arg_count() {
        let name = ctx.arg(i)?.as_bytes();
        match lookup_command_info_spec(name) {
            Some(spec) => items.push(command_info_frame(spec)),
            None => items.push(RespFrame::null_bulk()),
        }
    }
    ctx.reply_frame(&RespFrame::array(items))
}

pub fn lookup_command_info_spec(
    name: &[u8],
) -> Option<&'static crate::generated::GeneratedCommandSpec> {
    COMMANDS
        .iter()
        .find(|spec| ascii_eq_ignore_case(&command_full_name(spec), name))
}

pub fn command_info_frame(spec: &crate::generated::GeneratedCommandSpec) -> RespFrame {
    RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_vec(command_full_name(spec))),
        RespFrame::integer(spec.arity as i64),
        RespFrame::array(
            command_info_flags(spec)
                .into_iter()
                .map(RespFrame::bulk)
                .collect(),
        ),
        RespFrame::integer(0),
        RespFrame::integer(0),
        RespFrame::integer(0),
        RespFrame::array(Vec::new()),
    ])
}

pub fn command_info_flags(spec: &crate::generated::GeneratedCommandSpec) -> Vec<RedisString> {
    let mut flags: Vec<RedisString> = spec
        .flags
        .iter()
        .filter_map(|flag| command_flag_name(*flag))
        .map(RedisString::from_bytes)
        .collect();
    let full_name = command_full_name(spec);
    if command_has_movable_keys(&full_name) && !flags.iter().any(|f| f.as_bytes() == b"movablekeys")
    {
        flags.push(RedisString::from_bytes(b"movablekeys"));
    }
    flags
}

pub fn command_flag_name(flag: crate::generated::CommandFlag) -> Option<&'static [u8]> {
    use crate::generated::CommandFlag;
    match flag {
        CommandFlag::ADMIN => Some(b"admin"),
        CommandFlag::ALLOW_BUSY => Some(b"allow-busy"),
        CommandFlag::ALL_DBS => Some(b"all-dbs"),
        CommandFlag::ASKING => Some(b"asking"),
        CommandFlag::BLOCKING => Some(b"blocking"),
        CommandFlag::DENYOOM => Some(b"denyoom"),
        CommandFlag::FAST => Some(b"fast"),
        CommandFlag::LOADING => Some(b"loading"),
        CommandFlag::MAY_REPLICATE => Some(b"may-replicate"),
        CommandFlag::NOSCRIPT => Some(b"noscript"),
        CommandFlag::NO_ASYNC_LOADING => Some(b"no-async-loading"),
        CommandFlag::NO_AUTH => Some(b"no-auth"),
        CommandFlag::NO_MANDATORY_KEYS => None,
        CommandFlag::NO_MULTI => Some(b"no-multi"),
        CommandFlag::ONLY_SENTINEL => Some(b"only-sentinel"),
        CommandFlag::PROTECTED => Some(b"protected"),
        CommandFlag::PUBSUB => Some(b"pubsub"),
        CommandFlag::READONLY => Some(b"readonly"),
        CommandFlag::SENTINEL => Some(b"sentinel"),
        CommandFlag::SKIP_COMMANDLOG => Some(b"skip-commandlog"),
        CommandFlag::SKIP_MONITOR => Some(b"skip-monitor"),
        CommandFlag::STALE => Some(b"stale"),
        CommandFlag::TOUCHES_ARBITRARY_KEYS => Some(b"movablekeys"),
        CommandFlag::WRITE => Some(b"write"),
    }
}

pub fn command_has_movable_keys(full_name: &[u8]) -> bool {
    [
        b"zunionstore".as_slice(),
        b"xread".as_slice(),
        b"eval".as_slice(),
        b"sort".as_slice(),
        b"sort_ro".as_slice(),
        b"migrate".as_slice(),
        b"georadius".as_slice(),
    ]
    .iter()
    .any(|name| ascii_eq_ignore_case(full_name, name))
}

pub fn command_full_name(spec: &crate::generated::GeneratedCommandSpec) -> Vec<u8> {
    let name = spec.name.as_bytes().to_ascii_lowercase();
    if let Some(parent) = command_parent_for_spec(spec) {
        if name.as_slice() != parent {
            let mut full = Vec::with_capacity(parent.len() + 1 + name.len());
            full.extend_from_slice(parent);
            full.push(b'|');
            full.extend_from_slice(&name);
            return full;
        }
    }
    name
}

pub fn command_parent_for_spec(
    spec: &crate::generated::GeneratedCommandSpec,
) -> Option<&'static [u8]> {
    let function = spec.function.as_bytes();
    for (prefix, parent) in [
        (b"acl".as_slice(), b"acl".as_slice()),
        (b"client".as_slice(), b"client".as_slice()),
        (b"cluster".as_slice(), b"cluster".as_slice()),
        (b"command".as_slice(), b"command".as_slice()),
        (b"config".as_slice(), b"config".as_slice()),
        (b"function".as_slice(), b"function".as_slice()),
        (b"latency".as_slice(), b"latency".as_slice()),
        (b"memory".as_slice(), b"memory".as_slice()),
        (b"module".as_slice(), b"module".as_slice()),
        (b"pubsub".as_slice(), b"pubsub".as_slice()),
        (b"script".as_slice(), b"script".as_slice()),
        (b"xgroup".as_slice(), b"xgroup".as_slice()),
        (b"xinfo".as_slice(), b"xinfo".as_slice()),
    ] {
        if starts_with_ascii_ci(function, prefix) {
            return Some(parent);
        }
    }
    None
}

pub fn starts_with_ascii_ci(text: &[u8], prefix: &[u8]) -> bool {
    text.len() >= prefix.len() && ascii_eq_ignore_case(&text[..prefix.len()], prefix)
}

#[derive(Clone)]
pub struct CommandKeyRef {
    key: RedisString,
    flags: Vec<RedisString>,
}

pub fn command_getkeys(ctx: &mut CommandContext<'_>, with_flags: bool) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        let name = if with_flags {
            b"command|getkeysandflags".as_slice()
        } else {
            b"command|getkeys".as_slice()
        };
        return Err(RedisError::wrong_number_of_args(name));
    }
    let spec = lookup_generated_command_for_getkeys(ctx)?;
    let command_argc = ctx.arg_count() - 2;
    validate_command_getkeys_arity(spec.arity, command_argc)?;

    let key_refs = command_key_refs(ctx, spec, command_argc)?;
    let items = if with_flags {
        key_refs
            .into_iter()
            .map(|key_ref| {
                RespFrame::array(vec![
                    RespFrame::bulk(key_ref.key),
                    RespFrame::array(key_ref.flags.into_iter().map(RespFrame::bulk).collect()),
                ])
            })
            .collect()
    } else {
        key_refs
            .into_iter()
            .map(|key_ref| RespFrame::bulk(key_ref.key))
            .collect()
    };
    ctx.reply_frame(&RespFrame::array(items))
}

pub fn lookup_generated_command_for_getkeys(
    ctx: &CommandContext<'_>,
) -> RedisResult<&'static crate::generated::GeneratedCommandSpec> {
    let parent = ctx.arg(2)?.as_bytes();
    if crate::dispatch::lookup_command(parent).is_none() {
        return Err(RedisError::runtime(b"ERR Invalid command specified"));
    }
    let expected_function = expected_command_function_name(parent);
    if ctx.arg_count() > 3 {
        let sub = ctx.arg(3)?.as_bytes();
        if let Some(spec) = lookup_generated_subcommand_spec(parent, sub) {
            return Ok(spec);
        }
        if let Some(spec) = crate::generated::COMMANDS.iter().find(|spec| {
            ascii_eq_ignore_case(spec.name.as_bytes(), sub)
                && ascii_eq_ignore_case(spec.function.as_bytes(), &expected_function)
        }) {
            return Ok(spec);
        }
    }
    crate::generated::COMMANDS
        .iter()
        .find(|spec| {
            ascii_eq_ignore_case(spec.name.as_bytes(), parent)
                && ascii_eq_ignore_case(spec.function.as_bytes(), &expected_function)
        })
        .ok_or_else(|| RedisError::runtime(b"ERR Invalid command specified"))
}

pub fn lookup_generated_subcommand_spec(
    parent: &[u8],
    sub: &[u8],
) -> Option<&'static crate::generated::GeneratedCommandSpec> {
    crate::generated::COMMANDS.iter().find(|spec| {
        spec.container
            .is_some_and(|container| ascii_eq_ignore_case(container.as_bytes(), parent))
            && ascii_eq_ignore_case(spec.name.as_bytes(), sub)
    })
}

pub fn expected_command_function_name(command: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(command.len() + b"Command".len());
    for &b in command {
        if b.is_ascii_alphanumeric() {
            out.push(ascii_lower(b));
        }
    }
    out.extend_from_slice(b"Command");
    out
}

pub fn command_key_refs(
    ctx: &CommandContext<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    command_argc: usize,
) -> RedisResult<Vec<CommandKeyRef>> {
    let cmd_name = ctx.arg(2)?.as_bytes();
    if ascii_eq_ignore_case(cmd_name, b"SET") {
        return Ok(vec![CommandKeyRef {
            key: ctx.arg_owned(3usize)?,
            flags: key_flags(&[b"OW".as_slice(), b"update".as_slice()]),
        }]);
    }
    if ascii_eq_ignore_case(cmd_name, b"SORT") {
        return sort_key_refs(ctx);
    }
    if ascii_eq_ignore_case(cmd_name, b"SORT_RO") {
        return Ok(vec![CommandKeyRef {
            key: ctx.arg_owned(3usize)?,
            flags: key_flags(&[b"RO".as_slice(), b"access".as_slice()]),
        }]);
    }
    match command_key_refs_from_specs(ctx, spec, command_argc) {
        Ok(Some(keys)) => Ok(keys),
        Ok(None) => Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        )),
        Err(err) if command_allows_no_mandatory_keys(spec) => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

pub fn validate_command_getkeys_arity(arity: i32, argc: usize) -> RedisResult<()> {
    let invalid = if arity > 0 {
        argc != arity as usize
    } else if arity < 0 {
        argc < (-arity) as usize
    } else {
        false
    };
    if invalid {
        Err(RedisError::runtime(
            b"ERR Invalid number of arguments specified for command",
        ))
    } else {
        Ok(())
    }
}

pub fn sort_key_refs(ctx: &CommandContext<'_>) -> RedisResult<Vec<CommandKeyRef>> {
    let argc = ctx.arg_count() - 2;
    let mut keys = Vec::with_capacity(2);
    keys.push(CommandKeyRef {
        key: ctx.arg_owned(3usize)?,
        flags: key_flags(&[b"RO".as_slice(), b"access".as_slice()]),
    });
    let mut store_key_index: Option<usize> = None;
    let mut i = 2usize;
    while i < argc {
        let arg = ctx.arg_owned(i + 2)?;
        let bytes = arg.as_bytes();
        if ascii_eq_ignore_case(bytes, b"LIMIT") {
            i += 3;
            continue;
        }
        if ascii_eq_ignore_case(bytes, b"GET") || ascii_eq_ignore_case(bytes, b"BY") {
            i += 2;
            continue;
        }
        if ascii_eq_ignore_case(bytes, b"STORE") && i + 1 < argc {
            store_key_index = Some(i + 3);
        }
        i += 1;
    }
    if let Some(index) = store_key_index {
        keys.push(CommandKeyRef {
            key: ctx.arg_owned(index)?,
            flags: key_flags(&[b"OW".as_slice(), b"update".as_slice()]),
        });
    }
    Ok(keys)
}

pub fn command_key_refs_from_specs(
    ctx: &CommandContext<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    command_argc: usize,
) -> RedisResult<Option<Vec<CommandKeyRef>>> {
    let key_specs: Value = serde_json::from_str(spec.key_specs_json)
        .map_err(|_| RedisError::runtime(b"ERR Invalid arguments specified for command"))?;
    let Some(specs) = key_specs.as_array() else {
        return Ok(None);
    };
    if specs.is_empty() || specs.iter().all(key_spec_is_not_key) {
        return Err(RedisError::runtime(b"ERR The command has no key arguments"));
    }

    let mut keys = Vec::new();
    let mut unsupported = false;
    for key_spec in specs {
        if key_spec_is_not_key(key_spec) {
            continue;
        }
        let Some(positions) = key_positions_from_spec(ctx, key_spec, command_argc)? else {
            unsupported = true;
            continue;
        };
        let flags = key_flags_from_spec(key_spec);
        for pos in positions {
            keys.push(CommandKeyRef {
                key: ctx.arg_owned(2 + pos)?,
                flags: flags.clone(),
            });
        }
    }
    if keys.is_empty() && unsupported {
        Ok(None)
    } else {
        Ok(Some(keys))
    }
}

pub fn command_allows_no_mandatory_keys(spec: &crate::generated::GeneratedCommandSpec) -> bool {
    spec.flags
        .contains(&crate::generated::CommandFlag::NO_MANDATORY_KEYS)
}

pub fn key_flags(flags: &[&[u8]]) -> Vec<RedisString> {
    flags.iter().map(RedisString::from_bytes).collect()
}

pub fn key_flags_from_spec(spec: &Value) -> Vec<RedisString> {
    let Some(flags) = spec.get("flags").and_then(Value::as_array) else {
        return Vec::new();
    };
    flags
        .iter()
        .filter_map(Value::as_str)
        .filter(|flag| *flag != "NOT_KEY" && *flag != "VARIABLE_FLAGS")
        .map(command_key_flag_name)
        .collect()
}

pub fn command_key_flag_name(flag: &str) -> RedisString {
    match flag {
        "RO" | "RW" | "OW" | "RM" => RedisString::from_bytes(flag.as_bytes()),
        _ => {
            let mut out = Vec::with_capacity(flag.len());
            for &b in flag.as_bytes() {
                out.push(ascii_lower(b));
            }
            RedisString::from_vec(out)
        }
    }
}

pub fn key_spec_is_not_key(spec: &Value) -> bool {
    spec.get("flags")
        .and_then(Value::as_array)
        .map(|flags| flags.iter().any(|flag| flag.as_str() == Some("NOT_KEY")))
        .unwrap_or(false)
}

pub fn key_positions_from_spec(
    ctx: &CommandContext<'_>,
    spec: &Value,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(start) = spec
        .pointer("/begin_search/index/pos")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    if let Some(range) = spec.pointer("/find_keys/range") {
        return range_key_positions(range, start, command_argc);
    }
    if let Some(keynum) = spec.pointer("/find_keys/keynum") {
        return keynum_key_positions(ctx, keynum, start, command_argc);
    }
    Ok(None)
}

pub fn range_key_positions(
    range: &Value,
    first: usize,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(lastkey) = range.get("lastkey").and_then(Value::as_i64) else {
        return Ok(None);
    };
    let Some(step) = range
        .get("step")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
        .filter(|step| *step > 0)
    else {
        return Ok(None);
    };
    let last = if lastkey >= 0 {
        first.saturating_add(lastkey as usize)
    } else {
        let offset = (-lastkey) as usize;
        if offset > command_argc {
            return Ok(Some(Vec::new()));
        }
        command_argc - offset
    };
    if first >= command_argc || last >= command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    if last < first {
        return Ok(Some(Vec::new()));
    }
    let mut out = Vec::new();
    let mut pos = first;
    while pos <= last {
        out.push(pos);
        match pos.checked_add(step) {
            Some(next) => pos = next,
            None => break,
        }
    }
    Ok(Some(out))
}

pub fn keynum_key_positions(
    ctx: &CommandContext<'_>,
    keynum: &Value,
    begin: usize,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(keynumidx) = keynum
        .get("keynumidx")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    let Some(firstkey) = keynum
        .get("firstkey")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    let Some(step) = keynum
        .get("step")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
        .filter(|step| *step > 0)
    else {
        return Ok(None);
    };
    let numkeys_index = begin + keynumidx;
    let first_key_index = begin + firstkey;
    if numkeys_index >= command_argc || first_key_index > command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    let numkeys_arg = ctx.arg_owned(2 + numkeys_index)?;
    let Some(numkeys) = parse_i64_strict(numkeys_arg.as_bytes()).filter(|n| *n >= 0) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    let numkeys = numkeys as usize;
    if numkeys == 0 {
        return Ok(Some(Vec::new()));
    }
    let Some(last_offset) = numkeys.checked_sub(1).and_then(|n| n.checked_mul(step)) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    let Some(last_pos) = first_key_index.checked_add(last_offset) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    if last_pos >= command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    let mut out = Vec::with_capacity(numkeys.min(command_argc));
    for idx in 0..numkeys {
        let pos = first_key_index + idx * step;
        out.push(pos);
    }
    Ok(Some(out))
}

pub fn nonnegative_usize(n: i64) -> Option<usize> {
    if n >= 0 {
        Some(n as usize)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::{Client, CommandContext};

    fn command_getkeys_refs(parts: &[&[u8]]) -> RedisResult<Vec<CommandKeyRef>> {
        let mut args = vec![
            RedisString::from_bytes(b"COMMAND"),
            RedisString::from_bytes(b"GETKEYS"),
        ];
        args.extend(parts.iter().map(|part| RedisString::from_bytes(part)));
        let mut client = Client::new(1);
        client.set_args(args);
        let ctx = CommandContext::new(&mut client);
        let spec = lookup_generated_command_for_getkeys(&ctx)?;
        let command_argc = ctx.arg_count() - 2;
        validate_command_getkeys_arity(spec.arity, command_argc)?;
        command_key_refs(&ctx, spec, command_argc)
    }

    fn command_getkeys(parts: &[&[u8]]) -> RedisResult<Vec<Vec<u8>>> {
        command_getkeys_refs(parts).map(|keys| {
            keys.into_iter()
                .map(|key_ref| key_ref.key.as_bytes().to_vec())
                .collect()
        })
    }

    #[test]
    fn command_keyspec_audit_extracts_range_and_keynum_keys() {
        assert_eq!(
            command_getkeys(&[b"MGET", b"{u}:a", b"{u}:b"]).unwrap(),
            vec![b"{u}:a".to_vec(), b"{u}:b".to_vec()]
        );
        assert_eq!(
            command_getkeys(&[
                b"EVAL",
                b"return redis.call('get', KEYS[1])",
                b"2",
                b"a",
                b"b"
            ])
            .unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn command_keyspec_audit_maps_extracted_keys_to_cluster_slots() {
        let same_slot = command_getkeys(&[b"MGET", b"{u}:a", b"{u}:b"]).unwrap();
        assert!(!crate::cluster::keys_cross_slot(
            same_slot.iter().map(Vec::as_slice)
        ));

        let cross_slot = command_getkeys(&[b"MGET", b"foo", b"bar"]).unwrap();
        assert!(crate::cluster::keys_cross_slot(
            cross_slot.iter().map(Vec::as_slice)
        ));
    }

    #[test]
    fn command_keyspec_audit_uses_generated_subcommand_containers() {
        let spec = lookup_generated_subcommand_spec(b"CLUSTER", b"KEYSLOT")
            .expect("CLUSTER KEYSLOT should be generated as a subcommand");
        assert_eq!(spec.key_specs_json, "[]");
        assert!(command_getkeys(&[b"CLUSTER", b"KEYSLOT", b"foo"]).is_err());
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
//   notes:         COMMAND command + INFO/LIST/GETKEYS + spec helpers.
// ──────────────────────────────────────────────────────────────────────────

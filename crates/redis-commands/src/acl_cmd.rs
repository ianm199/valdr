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

use crate::client_cmd::*;
use crate::client_limits::*;
use crate::command_meta::*;
use crate::config_cmd::*;
use crate::connection::*;
use crate::debug_cmd::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::listeners::*;
use crate::live_config_handle;
use crate::shutdown_signals::*;

pub fn acl_user_exists(name: &[u8]) -> bool {
    let key = RedisString::from_bytes(name);
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.users.contains_key(&key)
}

pub fn apply_requirepass_to_acl(secret: Option<&[u8]>) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let default_key = RedisString::from_static(b"default");
    guard.invalidate_fast_paths();
    if let Some(secret) = secret.filter(|s| !s.is_empty()) {
        let user = guard
            .users
            .entry(default_key.clone())
            .or_insert_with(AclUser::new_default);
        user.flags.enabled = true;
        user.flags.nopass = false;
        user.flags.allcommands = true;
        user.flags.allkeys = true;
        user.flags.allchannels = true;
        user.flags.alldbs = true;
        user.allowed_categories = acl_category::ALL;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        user.key_patterns = vec![RedisString::from_static(b"*")];
        user.channel_patterns = vec![RedisString::from_static(b"*")];
        user.allowed_dbs.clear();
        user.passwords = vec![sha256_hash(secret)];
    } else if let Some(user) = guard.users.get_mut(&default_key) {
        *user = AclUser::new_default();
    } else {
        guard.users.insert(default_key, AclUser::new_default());
    }
    guard.refresh_fast_paths();
}

pub fn default_user_has_no_password() -> bool {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let default_key = RedisString::from_static(b"default");
    guard
        .users
        .get(&default_key)
        .map(|user| user.flags.enabled && user.flags.nopass && user.passwords.is_empty())
        .unwrap_or(true)
}

pub fn record_auth_failure_acl_log(ctx: &CommandContext<'_>, username: &[u8], command_name: &[u8]) {
    record_acl_access_denied_auth();
    record_acl_log_entry(
        b"auth",
        b"toplevel",
        RedisString::from_static(b"AUTH"),
        RedisString::from_bytes(username),
        acl_log_client_info(ctx, command_name),
    );
}

pub fn acl_log_client_info(ctx: &CommandContext<'_>, command_name: &[u8]) -> RedisString {
    let command = lower_acl_token(command_name);
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

/// Attempt to authenticate as `username` with `cleartext`.
/// Returns `Some(username_as_RedisString)` on success, `None` on failure.
pub fn authenticate_user(username: &[u8], cleartext: &[u8]) -> Option<RedisString> {
    let key = RedisString::from_bytes(username);
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let user = guard.users.get(&key)?;
    if !user.flags.enabled {
        return None;
    }
    if user.check_password(cleartext) {
        Some(key)
    } else {
        None
    }
}

/// Try to match `cleartext` against every user's password list.
/// Used for the legacy one-argument AUTH form where no username is specified.
/// Returns the first matching enabled user's name, or `None`.
pub fn try_password_any_user(cleartext: &[u8]) -> Option<RedisString> {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for user in guard.users.values() {
        if user.flags.enabled && !user.flags.nopass && user.check_password(cleartext) {
            return Some(user.name.clone());
        }
    }
    None
}

pub fn load_acl_startup_config(
    user_lines: &[String],
    dir: &str,
    aclfile: Option<&str>,
) -> Result<(), Vec<u8>> {
    set_aclfile_config_name(aclfile.map(|name| name.to_string()));
    if !user_lines.is_empty() {
        let config_user_lines: Vec<String> = user_lines
            .iter()
            .map(|line| format!("user {}", line.trim()))
            .collect();
        let users = build_acl_users_from_lines(
            config_user_lines
                .iter()
                .enumerate()
                .map(|(idx, line)| (idx + 1, line.as_str())),
        )?;
        install_acl_users(users);
    }
    if let Some(name) = aclfile {
        let path = Path::new(dir).join(name);
        let users = load_acl_users_from_path(&path)?;
        install_acl_users(users);
    }
    Ok(())
}

pub(crate) fn install_acl_users(users: HashMap<RedisString, AclUser>) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.invalidate_fast_paths();
    guard.users = users;
    guard.refresh_fast_paths();
}

pub fn load_acl_users_from_path(path: &Path) -> Result<HashMap<RedisString, AclUser>, Vec<u8>> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        let mut msg = b"ERR Error loading ACL file: ".to_vec();
        msg.extend_from_slice(e.to_string().as_bytes());
        msg
    })?;
    build_acl_users_from_lines(
        contents
            .lines()
            .enumerate()
            .map(|(idx, line)| (idx + 1, line)),
    )
}

pub fn build_acl_users_from_lines<'a, I>(lines: I) -> Result<HashMap<RedisString, AclUser>, Vec<u8>>
where
    I: IntoIterator<Item = (usize, &'a str)>,
{
    let mut users = HashMap::new();
    for (line_no, line) in lines {
        let Some((username, user)) = parse_acl_user_line(line_no, line)? else {
            continue;
        };
        if users.contains_key(&username) {
            let mut msg = b"ERR Duplicate user '".to_vec();
            msg.extend_from_slice(username.as_bytes());
            msg.extend_from_slice(b"' found");
            return Err(msg);
        }
        users.insert(username, user);
    }
    let default_key = RedisString::from_static(b"default");
    users
        .entry(default_key)
        .or_insert_with(AclUser::new_default);
    Ok(users)
}

pub fn parse_acl_user_line(
    line_no: usize,
    line: &str,
) -> Result<Option<(RedisString, AclUser)>, Vec<u8>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() < 2 || !parts[0].eq_ignore_ascii_case("user") {
        let mut msg = b"ERR ACL file line ".to_vec();
        msg.extend_from_slice(line_no.to_string().as_bytes());
        msg.extend_from_slice(b" should start with user keyword followed by username");
        return Err(msg);
    }
    let username = RedisString::from_bytes(parts[1].as_bytes());
    if acl_string_has_spaces(username.as_bytes()) {
        return Err(b"ERR Usernames can't contain spaces or null characters".to_vec());
    }
    let mut user = AclUser::new_reset(username.clone());
    apply_acl_pubsub_default_to_user(&mut user);
    let rules: Vec<RedisString> = parts[2..]
        .iter()
        .map(|rule| RedisString::from_bytes(rule.as_bytes()))
        .collect();
    if let Err(e) = apply_acl_setuser_rules(&mut user, &rules) {
        let mut msg = b"ERR Error in ACL file line ".to_vec();
        msg.extend_from_slice(line_no.to_string().as_bytes());
        msg.extend_from_slice(b": ");
        msg.extend_from_slice(e.strip_prefix(b"ERR ").unwrap_or(&e));
        return Err(msg);
    }
    Ok(Some((username, user)))
}

pub fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

pub fn apply_acl_setuser_rules(user: &mut AclUser, rules: &[RedisString]) -> Result<(), Vec<u8>> {
    let mut idx = 0usize;
    while idx < rules.len() {
        let raw = rules[idx].as_bytes();
        let trimmed = trim_ascii(raw);
        if trimmed.is_empty() {
            idx += 1;
            continue;
        }
        if trimmed.eq_ignore_ascii_case(b"clearselectors") {
            user.selectors.clear();
            idx += 1;
            continue;
        }
        if trimmed.starts_with(b")") {
            return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
        }
        if trimmed.starts_with(b"(") {
            let (selector_rules, rendered, next_idx) = collect_acl_selector(rules, idx)?;
            let mut selector = AclUser::new_selector();
            for rule in selector_rules {
                if rule.starts_with(b"(") || rule.ends_with(b")") {
                    return Err(acl_setuser_error(&rendered, b"ERR Syntax error"));
                }
                if let Err(e) = apply_acl_rule(&mut selector, &rule) {
                    if e.starts_with(b"ERR Unrecognized parameter") {
                        return Err(acl_setuser_error(&rendered, b"ERR Syntax error"));
                    }
                    return Err(acl_setuser_error(&rendered, &e));
                }
            }
            user.selectors.push(selector);
            idx = next_idx;
            continue;
        }
        if let Err(e) = apply_acl_rule(user, trimmed) {
            return Err(acl_setuser_error(trimmed, &e));
        }
        idx += 1;
    }
    Ok(())
}

pub fn collect_acl_selector(
    rules: &[RedisString],
    start: usize,
) -> Result<(Vec<Vec<u8>>, Vec<u8>, usize), Vec<u8>> {
    let first_raw = rules[start].as_bytes();
    let first = trim_ascii(first_raw);
    if first_raw != first {
        return Err(acl_setuser_error(first, b"ERR Syntax error"));
    }

    let mut rendered = Vec::new();
    let mut end = start;
    loop {
        if end >= rules.len() {
            return Err(b"ERR Unmatched parenthesis in acl selector".to_vec());
        }
        if !rendered.is_empty() {
            rendered.push(b' ');
        }
        let token = trim_ascii(rules[end].as_bytes());
        rendered.extend_from_slice(token);
        if token.ends_with(b")") {
            break;
        }
        end += 1;
    }

    let trimmed = trim_ascii(&rendered);
    if !trimmed.starts_with(b"(") || !trimmed.ends_with(b")") || trimmed.len() < 2 {
        return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
    }
    let inner = trim_ascii(&trimmed[1..trimmed.len() - 1]);
    if inner.is_empty() {
        return Ok((Vec::new(), trimmed.to_vec(), end + 1));
    }
    if inner.contains(&b'(') || inner.contains(&b')') {
        return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
    }
    let pieces = inner
        .split(u8::is_ascii_whitespace)
        .filter(|piece| !piece.is_empty())
        .map(|piece| piece.to_vec())
        .collect();
    Ok((pieces, trimmed.to_vec(), end + 1))
}

pub fn aclfile_path_for_context(ctx: &CommandContext<'_>) -> Option<PathBuf> {
    let name = aclfile_config_name()?;
    let dir = ctx.live_config().rdb_dir();
    Some(Path::new(&dir).join(name))
}

pub fn apply_loaded_acl_users(
    ctx: &mut CommandContext<'_>,
    users: HashMap<RedisString, AclUser>,
) -> bool {
    let current_user = ctx.client_ref().authenticated_user.clone();
    let close_current_client = current_user
        .as_ref()
        .map(|user| !users.contains_key(user))
        .unwrap_or(false);
    let revoked_pubsub_ids = collect_revoked_pubsub_clients(&users);
    if let Some(pubsub) = &ctx.pubsub {
        let mut registry = match pubsub.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for id in revoked_pubsub_ids {
            registry.drop_client(id);
        }
    }
    install_acl_users(users);
    close_current_client
}

pub fn collect_revoked_pubsub_clients(users: &HashMap<RedisString, AclUser>) -> Vec<u64> {
    let mut registry = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let revoked: Vec<u64> = registry
        .all()
        .into_iter()
        .filter(snapshot_in_pubsub_mode)
        .filter(|snap| match &snap.user {
            Some(username) => users
                .get(username)
                .map(|user| acl_snapshot_has_revoked_channel(snap, user))
                .unwrap_or(true),
            None => false,
        })
        .map(|snap| snap.id)
        .collect();
    for id in &revoked {
        registry.deregister(*id);
    }
    revoked
}

pub fn acl_snapshot_has_revoked_channel(
    snap: &redis_core::client_info::ClientSnapshot,
    user: &AclUser,
) -> bool {
    snap.channel_names
        .iter()
        .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .shard_channel_names
            .iter()
            .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .pattern_names
            .iter()
            .any(|pattern| !user.can_access_channel_pattern(pattern.as_bytes()))
}

/// `ACL WHOAMI|LIST|USERS|GETUSER|CAT|SETUSER|DELUSER|LOG|HELP`.
pub fn acl_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"acl"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();

    if ascii_eq_ignore_case(sub_bytes, b"WHOAMI") {
        let name = ctx
            .client_ref()
            .authenticated_user
            .clone()
            .unwrap_or_else(|| RedisString::from_bytes(b"default"));
        return ctx.reply_bulk_string(name);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut users: Vec<&AclUser> = guard.users.values().collect();
        users.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        let mut items: Vec<RespFrame> = Vec::new();
        for user in users {
            items.push(RespFrame::bulk(RedisString::from_vec(
                user.to_rule_string(),
            )));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"USERS") {
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut names: Vec<RedisString> = guard.users.keys().cloned().collect();
        names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let items: Vec<RespFrame> = names.into_iter().map(RespFrame::bulk).collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"GETUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|getuser"));
        }
        let username = ctx.arg_owned(2usize)?;
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return match guard.users.get(&username) {
            None => ctx.reply_null_array(),
            Some(user) => ctx.reply_frame(&build_getuser_reply(user)),
        };
    }

    if ascii_eq_ignore_case(sub_bytes, b"CAT") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::runtime(
                b"ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.",
            ));
        }
        if ctx.arg_count() == 2 {
            let items: Vec<RespFrame> = ALL_CATEGORY_NAMES
                .iter()
                .map(|c| RespFrame::bulk(RedisString::from_bytes(c)))
                .collect();
            return ctx.reply_frame(&RespFrame::array(items));
        }
        let cat_name = ctx.arg_owned(2)?;
        let bit = match category_name_to_bit(cat_name.as_bytes()) {
            Some(b) => b,
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name.as_bytes());
                msg.push(b'\'');
                return Err(RedisError::runtime(msg));
            }
        };
        let cmds = commands_in_category(bit);
        let items: Vec<RespFrame> = cmds
            .into_iter()
            .map(|c| RespFrame::bulk(RedisString::from_vec(c)))
            .collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"DRYRUN") {
        if ctx.arg_count() < 4 {
            return Err(RedisError::wrong_number_of_args(b"acl|dryrun"));
        }
        let username = ctx.arg_owned(2usize)?;
        let command = ctx.arg_owned(3usize)?;
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let Some(user) = guard.users.get(&username) else {
            let mut msg = b"ERR User '".to_vec();
            msg.extend_from_slice(username.as_bytes());
            msg.extend_from_slice(b"' not found");
            return Err(RedisError::runtime(msg));
        };
        let dry_argc = ctx.arg_count().saturating_sub(3);
        let Some(spec) = acl_dryrun_command_spec(ctx, command.as_bytes(), dry_argc) else {
            let mut msg = b"ERR Command '".to_vec();
            msg.extend_from_slice(command.as_bytes());
            msg.extend_from_slice(b"' not found");
            return Err(RedisError::runtime(msg));
        };
        if (spec.arity > 0 && spec.arity as usize != dry_argc)
            || (spec.arity < 0 && dry_argc < (-spec.arity) as usize)
        {
            let mut msg = b"ERR wrong number of arguments for '".to_vec();
            msg.extend_from_slice(&lower_acl_token(command.as_bytes()));
            msg.extend_from_slice(b"' command");
            return Err(RedisError::runtime(msg));
        }
        let categories = spec
            .acl_categories
            .iter()
            .fold(0u64, |acc, cat| acc | generated_acl_category_bit(*cat));
        match acl_dryrun_check(
            ctx,
            user,
            command.as_bytes(),
            spec.name.as_bytes(),
            categories,
        ) {
            Ok(()) => return ctx.reply_simple_string(b"OK"),
            Err(AclDryrunDeny::Command) => {
                let mut msg = b"This user has no permissions to run the '".to_vec();
                msg.extend_from_slice(&lower_acl_token(command.as_bytes()));
                msg.extend_from_slice(b"' command");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Key(key)) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(key.as_bytes());
                msg.extend_from_slice(b"' key");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Channel(channel)) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(channel.as_bytes());
                msg.extend_from_slice(b"' channel");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Database) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access database");
                return ctx.reply_bulk(&msg);
            }
        }
    }

    if ascii_eq_ignore_case(sub_bytes, b"SETUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|setuser"));
        }
        let username = ctx.arg_owned(2usize)?;
        if acl_string_has_spaces(username.as_bytes()) {
            return Err(RedisError::runtime(
                b"ERR Usernames can't contain spaces or null characters",
            ));
        }
        let rules: Vec<RedisString> = (3..ctx.arg_count())
            .filter_map(|i| ctx.client_ref().arg(i).cloned())
            .collect();
        let acl = global_acl_state();
        let mut guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut user = guard.users.get(&username).cloned().unwrap_or_else(|| {
            let mut user = AclUser::new_reset(username.clone());
            apply_acl_pubsub_default_to_user(&mut user);
            user
        });
        apply_acl_setuser_rules(&mut user, &rules).map_err(RedisError::runtime)?;
        let revoked_pubsub_ids = {
            let mut registry = match client_info_registry().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            registry.deregister_revoked_pubsub_clients(&username, &user)
        };
        let current_id = ctx.client_ref().id();
        let mut close_current_client = false;
        if let Some(pubsub) = &ctx.pubsub {
            let mut registry = match pubsub.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            for id in revoked_pubsub_ids {
                if id == current_id {
                    close_current_client = true;
                } else {
                    registry.drop_client(id);
                }
            }
        }
        guard.invalidate_fast_paths();
        guard.users.insert(username, user);
        guard.refresh_fast_paths();
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_simple_string(b"OK");
    }

    if ascii_eq_ignore_case(sub_bytes, b"DELUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|deluser"));
        }
        let default_key = RedisString::from_bytes(b"default");
        let current_user = ctx.client_ref().authenticated_user.clone();
        let mut close_current_client = false;
        let acl = global_acl_state();
        let mut guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut count: i64 = 0;
        for i in 2..ctx.arg_count() {
            if let Some(name) = ctx.client_ref().arg(i).cloned() {
                if name == default_key {
                    return Err(RedisError::runtime(
                        b"ERR The 'default' user cannot be removed",
                    ));
                }
                if guard.users.remove(&name).is_some() {
                    count += 1;
                    if current_user.as_ref() == Some(&name) {
                        close_current_client = true;
                    }
                }
            }
        }
        if count > 0 {
            guard.refresh_fast_paths();
        }
        drop(guard);
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_integer(count);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LOG") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::runtime(
                b"ERR wrong number of arguments for 'acl|log' command",
            ));
        }
        if ctx.arg_count() == 3 {
            let sub2 = ctx.arg_owned(2)?;
            if ascii_eq_ignore_case(sub2.as_bytes(), b"RESET") {
                clear_acl_log();
                return ctx.reply_simple_string(b"OK");
            }
            let count = parse_usize_strict(sub2.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR ACL LOG argument must be a positive integer or RESET")
            })?;
            return ctx.reply_frame(&build_acl_log_reply(Some(count)));
        }
        return ctx.reply_frame(&build_acl_log_reply(None));
    }

    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|help"));
        }
        let lines: &[&[u8]] = &[
            b"ACL <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
            b"CAT [<category>]",
            b"    List all commands that belong to <category>, or all command categories",
            b"    when no category is specified.",
            b"DELUSER <username> [<username> ...]",
            b"    Delete a list of users.",
            b"GETUSER <username>",
            b"    Get the ACL details for <username>.",
            b"LIST",
            b"    Show users details in config file format.",
            b"LOAD",
            b"    Reload users from the configured ACL file.",
            b"LOG [<count> | RESET]",
            b"    Show the recent ACL log or clear it.",
            b"SAVE",
            b"    Save users to the configured ACL file.",
            b"SETUSER <username> [<rule> [<rule> ...]]",
            b"    Modify or create the rules for an existing user.",
            b"USERS",
            b"    List all the registered usernames.",
            b"WHOAMI",
            b"    Return the current connection username.",
            b"HELP",
            b"    Return subcommand help summary.",
        ];
        let items: Vec<RespFrame> = lines
            .iter()
            .map(|l| RespFrame::bulk(RedisString::from_bytes(l)))
            .collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"GENPASS") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|genpass"));
        }
        let bits = if ctx.arg_count() == 3 {
            parse_i64_strict(ctx.arg_owned(2)?.as_bytes())
        } else {
            Some(256)
        };
        let Some(bits) = bits else {
            return Err(RedisError::runtime(
                b"ERR ACL GENPASS argument must be the number of bits for output password, a positive number up to 4096",
            ));
        };
        if bits <= 0 || bits > 4096 {
            return Err(RedisError::runtime(
                b"ERR ACL GENPASS argument must be the number of bits for output password, a positive number up to 4096",
            ));
        }
        let hex_len = ((bits as usize).saturating_add(3)) / 4;
        return ctx.reply_bulk(&vec![b'0'; hex_len]);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LOAD") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|load"));
        }
        let Some(path) = aclfile_path_for_context(ctx) else {
            return Err(RedisError::runtime(
                b"ERR This Redis instance is not configured to use an ACL file. You may use CONFIG SET aclfile <filename> and then issue ACL LOAD",
            ));
        };
        let users = load_acl_users_from_path(&path).map_err(RedisError::runtime)?;
        let close_current_client = apply_loaded_acl_users(ctx, users);
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_simple_string(b"OK");
    }

    if ascii_eq_ignore_case(sub_bytes, b"SAVE") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|save"));
        }
        let Some(path) = aclfile_path_for_context(ctx) else {
            return Err(RedisError::runtime(
                b"ERR This Redis instance is not configured to use an ACL file. You may use CONFIG SET aclfile <filename> and then issue ACL SAVE",
            ));
        };
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut users: Vec<&AclUser> = guard.users.values().collect();
        users.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        let mut out = Vec::new();
        for user in users {
            out.extend_from_slice(&user.to_rule_string());
            out.push(b'\n');
        }
        std::fs::write(&path, out).map_err(|e| {
            let mut msg = b"ERR Error saving ACL file: ".to_vec();
            msg.extend_from_slice(e.to_string().as_bytes());
            RedisError::runtime(msg)
        })?;
        return ctx.reply_simple_string(b"OK");
    }

    let mut msg = Vec::with_capacity(b"ERR Unknown ACL subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown ACL subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

pub fn build_acl_log_reply(limit: Option<usize>) -> RespFrame {
    let now = acl_log_now_millis();
    let items: Vec<RespFrame> = acl_log_entries(limit)
        .iter()
        .map(|entry| build_acl_log_entry_reply(entry, now))
        .collect();
    RespFrame::array(items)
}

pub fn build_acl_log_entry_reply(entry: &AclLogEntry, now: i64) -> RespFrame {
    let age_seconds = now
        .saturating_sub(entry.timestamp_created)
        .checked_div(1000)
        .unwrap_or(0);
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"count")),
            RespFrame::Integer(saturating_i64_from_u64(entry.count)),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"reason")),
            RespFrame::bulk(entry.reason.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"context")),
            RespFrame::bulk(entry.context.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"object")),
            RespFrame::bulk(entry.object.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"username")),
            RespFrame::bulk(entry.username.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"age-seconds")),
            RespFrame::Integer(age_seconds),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"client-info")),
            RespFrame::bulk(entry.client_info.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"entry-id")),
            RespFrame::Integer(saturating_i64_from_u64(entry.entry_id)),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"timestamp-created")),
            RespFrame::Integer(entry.timestamp_created),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"timestamp-last-updated")),
            RespFrame::Integer(entry.timestamp_last_updated),
        ),
    ])
}

pub fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

pub enum AclDryrunDeny {
    Command,
    Key(RedisString),
    Channel(RedisString),
    Database,
}

pub fn acl_dryrun_command_spec(
    ctx: &CommandContext<'_>,
    command: &[u8],
    argc: usize,
) -> Option<&'static GeneratedCommandSpec> {
    if command.eq_ignore_ascii_case(b"MEMORY")
        && ctx
            .client_ref()
            .arg(4)
            .is_some_and(|arg| arg.as_bytes().eq_ignore_ascii_case(b"USAGE"))
    {
        return COMMANDS
            .iter()
            .find(|spec| spec.name.as_bytes().eq_ignore_ascii_case(b"USAGE"));
    }
    let mut fallback = None;
    for spec in COMMANDS
        .iter()
        .filter(|spec| spec.name.as_bytes().eq_ignore_ascii_case(command))
    {
        fallback.get_or_insert(spec);
        let arity_matches = (spec.arity > 0 && spec.arity as usize == argc)
            || (spec.arity < 0 && argc >= (-spec.arity) as usize);
        if arity_matches
            && spec.function != "configGetCommand"
            && spec.function != "configSetCommand"
        {
            return Some(spec);
        }
    }
    fallback
}

pub fn acl_dryrun_check(
    ctx: &CommandContext<'_>,
    user: &AclUser,
    command: &[u8],
    key_command: &[u8],
    categories: u64,
) -> Result<(), AclDryrunDeny> {
    let first_arg = ctx.client_ref().arg(4).map(|arg| arg.as_bytes());
    let mut key_denial = None;
    let mut channel_denial = None;
    let mut database_denial = None;
    for (idx, candidate) in std::iter::once(user)
        .chain(user.selectors.iter())
        .enumerate()
    {
        if !candidate.can_execute_command_with_arg(command, first_arg, categories) {
            continue;
        }
        if let Some(_db) =
            crate::dispatch::acl_database_denial_for_context(ctx, key_command, candidate, 3)
        {
            if idx == 0 {
                database_denial.get_or_insert(());
            }
            continue;
        }
        if let Some(channel) = acl_dryrun_channel_denial(ctx, command, candidate) {
            channel_denial.get_or_insert(channel);
            continue;
        }
        let denied_key = crate::dispatch::acl_key_requirements(ctx, key_command, 3)
            .into_iter()
            .find(|req| !candidate.can_access_key_for(req.key.as_bytes(), req.access))
            .map(|req| req.key);
        if let Some(key) = denied_key {
            key_denial.get_or_insert(key);
            continue;
        }
        return Ok(());
    }
    if let Some(key) = key_denial {
        return Err(AclDryrunDeny::Key(key));
    }
    if let Some(channel) = channel_denial {
        return Err(AclDryrunDeny::Channel(channel));
    }
    if database_denial.is_some() {
        return Err(AclDryrunDeny::Database);
    }
    Err(AclDryrunDeny::Command)
}

pub fn acl_dryrun_channel_denial(
    ctx: &CommandContext<'_>,
    command: &[u8],
    user: &AclUser,
) -> Option<RedisString> {
    if user.flags.allchannels {
        return None;
    }
    let lower = lower_acl_token(command);
    let (start, end, pattern) = match lower.as_slice() {
        b"publish" | b"spublish" => (4, 5.min(ctx.arg_count()), false),
        b"subscribe" | b"ssubscribe" => (4, ctx.arg_count(), false),
        b"psubscribe" => (4, ctx.arg_count(), true),
        _ => return None,
    };
    for idx in start..end {
        let Some(channel) = ctx.client_ref().arg(idx) else {
            continue;
        };
        let allowed = if pattern {
            user.can_access_channel_pattern(channel.as_bytes())
        } else {
            user.can_access_channel(channel.as_bytes())
        };
        if !allowed {
            return Some(channel.clone());
        }
    }
    None
}

pub fn acl_string_has_spaces(bytes: &[u8]) -> bool {
    bytes.iter().any(|b| b.is_ascii_whitespace() || *b == 0)
}

pub fn acl_setuser_error(rule: &[u8], reason: &[u8]) -> Vec<u8> {
    let reason = reason.strip_prefix(b"ERR ").unwrap_or(reason);
    let mut msg = Vec::with_capacity(
        b"ERR Error in ACL SETUSER modifier '': ".len() + rule.len() + reason.len(),
    );
    msg.extend_from_slice(b"ERR Error in ACL SETUSER modifier '");
    msg.extend_from_slice(rule);
    msg.extend_from_slice(b"': ");
    msg.extend_from_slice(reason);
    msg
}

pub fn acl_command_rule_error(reason: &[u8]) -> Vec<u8> {
    let mut msg = b"ERR ".to_vec();
    msg.extend_from_slice(reason);
    msg
}

pub fn lower_acl_token(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| b.to_ascii_lowercase()).collect()
}

pub fn command_exists(name: &[u8]) -> bool {
    COMMANDS
        .iter()
        .any(|spec| spec.name.as_bytes().eq_ignore_ascii_case(name))
}

pub fn known_container_subcommands(parent: &[u8]) -> Option<&'static [&'static [u8]]> {
    match parent {
        b"client" => Some(&[
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
        ]),
        b"config" => Some(&[b"get", b"resetstat", b"rewrite", b"set"]),
        b"memory" => Some(&[b"doctor", b"malloc-stats", b"purge", b"stats", b"usage"]),
        b"xinfo" => Some(&[b"consumers", b"groups", b"help", b"stream"]),
        _ => None,
    }
}

pub fn known_subcommand_rule(body: &[u8]) -> bool {
    let lower = lower_acl_token(body);
    let Some(pipe) = lower.iter().position(|b| *b == b'|') else {
        return false;
    };
    let parent = &lower[..pipe];
    let sub = &lower[pipe + 1..];
    known_container_subcommands(parent).is_some_and(|subs| {
        subs.iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(sub))
    })
}

pub fn validate_acl_command_rule(body: &[u8], allow: bool) -> Result<(), Vec<u8>> {
    if body.is_empty() {
        return Err(acl_command_rule_error(b"Syntax error"));
    }
    let lower = lower_acl_token(body);
    let pipes = lower.iter().filter(|b| **b == b'|').count();
    if pipes == 0 {
        if command_exists(&lower) {
            return Ok(());
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }

    let Some(last_pipe) = lower.iter().rposition(|b| *b == b'|') else {
        return Err(acl_command_rule_error(b"Syntax error"));
    };
    let (parent, sub_with_pipe) = lower.split_at(last_pipe);
    let sub = &sub_with_pipe[1..];
    if sub.is_empty() {
        return Err(acl_command_rule_error(b"Syntax error"));
    }
    if parent.contains(&b'|') {
        if allow && known_subcommand_rule(parent) {
            return Err(acl_command_rule_error(
                b"Allowing first-arg of a subcommand is not supported",
            ));
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }
    if let Some(subcommands) = known_container_subcommands(parent) {
        if subcommands
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(sub))
        {
            return Ok(());
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }
    if command_exists(parent) {
        return Ok(());
    }
    Err(acl_command_rule_error(
        b"Unknown command or category name in ACL",
    ))
}

pub fn remove_subcommand_rules(rules: &mut Vec<RedisString>, cmd_name: &[u8]) {
    if cmd_name.contains(&b'|') {
        return;
    }
    rules.retain(|rule| {
        let bytes = rule.as_bytes();
        !(bytes.len() > cmd_name.len()
            && bytes[..cmd_name.len()].eq_ignore_ascii_case(cmd_name)
            && bytes[cmd_name.len()] == b'|')
    });
}

pub fn push_acl_command_rule(rules: &mut Vec<RedisString>, sign: u8, body: &[u8]) {
    rules.retain(|rule| rule.as_bytes().get(1..) != Some(body));
    let mut rendered = Vec::with_capacity(1 + body.len());
    rendered.push(sign);
    rendered.extend_from_slice(body);
    rules.push(RedisString::from_vec(rendered));
}

pub fn remove_acl_command_rule_body(rules: &mut Vec<RedisString>, body: &[u8]) {
    rules.retain(|rule| rule.as_bytes().get(1..) != Some(body));
}

pub fn remove_acl_subcommand_rule_bodies(rules: &mut Vec<RedisString>, cmd_name: &[u8]) {
    if cmd_name.contains(&b'|') {
        return;
    }
    rules.retain(|rule| {
        let Some(body) = rule.as_bytes().get(1..) else {
            return true;
        };
        !(body.len() > cmd_name.len()
            && body[..cmd_name.len()].eq_ignore_ascii_case(cmd_name)
            && body[cmd_name.len()] == b'|')
    });
}

/// Apply a single ACL SETUSER rule token to `user`.
pub fn apply_acl_rule(user: &mut AclUser, rule: &[u8]) -> Result<(), Vec<u8>> {
    if rule.is_empty() {
        return Ok(());
    }
    if rule == b"on" {
        user.flags.enabled = true;
        return Ok(());
    }
    if rule == b"off" {
        user.flags.enabled = false;
        return Ok(());
    }
    if rule == b"nopass" {
        user.flags.nopass = true;
        user.passwords.clear();
        return Ok(());
    }
    if rule == b"resetpass" {
        user.flags.nopass = false;
        user.passwords.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"sanitize-payload") {
        user.flags.sanitize_payload = true;
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"skip-sanitize-payload") {
        user.flags.sanitize_payload = false;
        return Ok(());
    }
    if rule == b"allcommands" || rule == b"+@all" {
        user.flags.allcommands = true;
        user.allowed_categories = acl_category::ALL;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        return Ok(());
    }
    if rule == b"nocommands" || rule == b"-@all" {
        user.flags.allcommands = false;
        user.allowed_categories = 0;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        return Ok(());
    }
    if rule == b"allkeys" || rule == b"~*" {
        user.flags.allkeys = true;
        user.key_patterns = vec![RedisString::from_bytes(b"*")];
        user.key_permissions.clear();
        return Ok(());
    }
    if rule == b"resetkeys" {
        user.flags.allkeys = false;
        user.key_patterns.clear();
        user.key_permissions.clear();
        return Ok(());
    }
    if rule == b"allchannels" || rule == b"&*" {
        user.flags.allchannels = true;
        user.channel_patterns = vec![RedisString::from_bytes(b"*")];
        return Ok(());
    }
    if rule == b"resetchannels" {
        user.flags.allchannels = false;
        user.channel_patterns.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"alldbs") {
        user.flags.alldbs = true;
        user.allowed_dbs.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"resetdb") || rule.eq_ignore_ascii_case(b"resetdbs") {
        user.flags.alldbs = false;
        user.allowed_dbs.clear();
        return Ok(());
    }
    if rule.len() > 3 && rule[..3].eq_ignore_ascii_case(b"db=") {
        let raw = &rule[3..];
        let dbs = parse_acl_db_list(raw)?;
        user.flags.alldbs = false;
        user.allowed_dbs.clear();
        for db in dbs {
            if !user.allowed_dbs.contains(&db) {
                user.allowed_dbs.push(db);
            }
        }
        return Ok(());
    }
    if rule == b"reset" {
        *user = AclUser::new_reset(user.name.clone());
        apply_acl_pubsub_default_to_user(user);
        return Ok(());
    }
    if rule.starts_with(b">") {
        let cleartext = &rule[1..];
        let hash = sha256_hash(cleartext);
        if !user.passwords.contains(&hash) {
            user.passwords.push(hash);
        }
        user.flags.nopass = false;
        return Ok(());
    }
    if rule.starts_with(b"<") {
        let cleartext = &rule[1..];
        let hash = sha256_hash(cleartext);
        user.passwords.retain(|h| h != &hash);
        return Ok(());
    }
    if rule.starts_with(b"#") {
        let hex = &rule[1..];
        match hex_to_hash(hex) {
            Some(hash) => {
                if !user.passwords.contains(&hash) {
                    user.passwords.push(hash);
                }
                user.flags.nopass = false;
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Invalid password hash '".to_vec();
                msg.extend_from_slice(hex);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"!") {
        let hex = &rule[1..];
        match hex_to_hash(hex) {
            Some(hash) => {
                user.passwords.retain(|h| h != &hash);
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Invalid password hash '".to_vec();
                msg.extend_from_slice(hex);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"+@") {
        let cat_name = &rule[2..];
        match category_name_to_bit(cat_name) {
            Some(bit) => {
                if bit == acl_category::ALL {
                    user.flags.allcommands = true;
                    user.allowed_categories = acl_category::ALL;
                    user.denied_categories = 0;
                    user.command_rules.clear();
                } else {
                    user.allowed_categories |= bit;
                    user.denied_categories &= !bit;
                    let mut body = b"@".to_vec();
                    body.extend_from_slice(&lower_acl_token(cat_name));
                    push_acl_command_rule(&mut user.command_rules, b'+', &body);
                }
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"-@") {
        let cat_name = &rule[2..];
        match category_name_to_bit(cat_name) {
            Some(bit) => {
                if bit == acl_category::ALL {
                    user.flags.allcommands = false;
                    user.allowed_commands.clear();
                    user.denied_commands.clear();
                    user.allowed_categories = 0;
                    user.denied_categories = 0;
                    user.command_rules.clear();
                } else {
                    user.allowed_categories &= !bit;
                    user.denied_categories |= bit;
                    let mut body = b"@".to_vec();
                    body.extend_from_slice(&lower_acl_token(cat_name));
                    push_acl_command_rule(&mut user.command_rules, b'-', &body);
                }
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"+") {
        validate_acl_command_rule(&rule[1..], true)?;
        let lower = lower_acl_token(&rule[1..]);
        let cmd_name = RedisString::from_bytes(&lower);
        remove_subcommand_rules(&mut user.allowed_commands, &lower);
        remove_subcommand_rules(&mut user.denied_commands, &lower);
        remove_acl_subcommand_rule_bodies(&mut user.command_rules, &lower);
        remove_acl_command_rule_body(&mut user.command_rules, &lower);
        user.denied_commands.retain(|c| c != &cmd_name);
        if !user.allowed_commands.contains(&cmd_name) {
            user.allowed_commands.push(cmd_name);
        }
        push_acl_command_rule(&mut user.command_rules, b'+', &lower);
        return Ok(());
    }
    if rule.starts_with(b"-") {
        validate_acl_command_rule(&rule[1..], false)?;
        let lower = lower_acl_token(&rule[1..]);
        let cmd_name = RedisString::from_bytes(&lower);
        remove_subcommand_rules(&mut user.allowed_commands, &lower);
        remove_subcommand_rules(&mut user.denied_commands, &lower);
        remove_acl_subcommand_rule_bodies(&mut user.command_rules, &lower);
        remove_acl_command_rule_body(&mut user.command_rules, &lower);
        user.allowed_commands.retain(|c| c != &cmd_name);
        if !user.denied_commands.contains(&cmd_name) {
            user.denied_commands.push(cmd_name);
        }
        push_acl_command_rule(&mut user.command_rules, b'-', &lower);
        return Ok(());
    }
    if rule.starts_with(b"~") {
        let pat = RedisString::from_bytes(&rule[1..]);
        if pat.as_bytes() == b"*" {
            user.flags.allkeys = true;
        }
        if !user.key_patterns.contains(&pat) {
            user.key_patterns.push(pat);
        }
        return Ok(());
    }
    if rule.starts_with(b"%") {
        let (permissions, pattern) = parse_acl_key_permission(rule)?;
        let pat = RedisString::from_bytes(pattern);
        if let Some(existing) = user
            .key_permissions
            .iter_mut()
            .find(|existing| existing.pattern == pat)
        {
            existing.permissions |= permissions;
        } else {
            user.key_permissions.push(AclKeyPattern {
                pattern: pat,
                permissions,
            });
        }
        return Ok(());
    }
    if rule.starts_with(b"&") {
        let pat = RedisString::from_bytes(&rule[1..]);
        if user.flags.allchannels && pat.as_bytes() != b"*" {
            return Err(
                b"ERR Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels"
                    .to_vec(),
            );
        }
        if pat.as_bytes() == b"*" {
            user.flags.allchannels = true;
        }
        if !user.channel_patterns.contains(&pat) {
            user.channel_patterns.push(pat);
        }
        return Ok(());
    }
    let mut msg: Vec<u8> = b"ERR Unrecognized parameter '".to_vec();
    msg.extend_from_slice(rule);
    msg.push(b'\'');
    Err(msg)
}

/// Build the RESP reply for `ACL GETUSER <username>`.
pub fn build_getuser_reply(user: &AclUser) -> RespFrame {
    let mut flag_items: Vec<RespFrame> = Vec::new();
    if user.flags.enabled {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"on")));
    } else {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"off")));
    }
    if user.flags.nopass {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"nopass")));
    }
    if user.flags.sanitize_payload {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(
            b"sanitize-payload",
        )));
    } else {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(
            b"skip-sanitize-payload",
        )));
    }
    if user.flags.allkeys {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allkeys")));
    }
    if user.flags.allchannels {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allchannels")));
    }
    if user.flags.allcommands {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allcommands")));
    }

    let pass_items: Vec<RespFrame> = user
        .passwords
        .iter()
        .map(|h| {
            let mut hex: Vec<u8> = b"#".to_vec();
            hex.extend_from_slice(&redis_core::acl::hash_to_hex(h));
            RespFrame::bulk(RedisString::from_vec(hex))
        })
        .collect();

    let commands_str = user.commands_summary();
    let keys_str = user.keys_summary();
    let channels_str = user.channels_summary();
    let databases_str = user.databases_summary();
    let selectors: Vec<RespFrame> = user
        .selectors
        .iter()
        .map(build_getuser_selector_reply)
        .collect();

    RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_bytes(b"flags")),
        RespFrame::array(flag_items),
        RespFrame::bulk(RedisString::from_bytes(b"passwords")),
        RespFrame::array(pass_items),
        RespFrame::bulk(RedisString::from_bytes(b"commands")),
        RespFrame::bulk(RedisString::from_vec(commands_str)),
        RespFrame::bulk(RedisString::from_bytes(b"keys")),
        RespFrame::bulk(RedisString::from_vec(keys_str)),
        RespFrame::bulk(RedisString::from_bytes(b"channels")),
        RespFrame::bulk(RedisString::from_vec(channels_str)),
        RespFrame::bulk(RedisString::from_bytes(b"databases")),
        RespFrame::bulk(RedisString::from_vec(databases_str)),
        RespFrame::bulk(RedisString::from_bytes(b"selectors")),
        RespFrame::array(selectors),
    ])
}

pub fn build_getuser_selector_reply(selector: &AclUser) -> RespFrame {
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"commands")),
            RespFrame::bulk(RedisString::from_vec(selector.commands_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"keys")),
            RespFrame::bulk(RedisString::from_vec(selector.keys_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"channels")),
            RespFrame::bulk(RedisString::from_vec(selector.channels_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"databases")),
            RespFrame::bulk(RedisString::from_vec(selector.databases_summary())),
        ),
    ])
}

/// Return command names belonging to a given ACL category bitmask bit.
/// Scans the generated `COMMANDS` registry for entries whose `acl_categories`
/// include the requested bit and collects their names (deduplicated).
pub fn commands_in_category(bit: u64) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for spec in crate::generated::COMMANDS.iter() {
        let matches = spec.acl_categories.iter().any(|&cat| {
            let cat_bit = generated_acl_category_bit(cat);
            cat_bit & bit != 0
        });
        if matches {
            let name = spec.name.as_bytes().to_ascii_lowercase();
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    if bit == acl_category::SCRIPTING {
        for name in [
            b"function|delete" as &[u8],
            b"function|dump",
            b"function|flush",
            b"function|kill",
            b"function|list",
            b"function|load",
            b"function|restore",
            b"function|stats",
        ] {
            let name = name.to_vec();
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    out
}

pub fn generated_acl_category_bit(cat: crate::generated::AclCategory) -> u64 {
    use crate::generated::AclCategory;
    match cat {
        AclCategory::KEYSPACE => acl_category::KEYSPACE,
        AclCategory::READ => acl_category::READ,
        AclCategory::WRITE => acl_category::WRITE,
        AclCategory::SET => acl_category::SET,
        AclCategory::SORTEDSET => acl_category::SORTEDSET,
        AclCategory::LIST => acl_category::LIST,
        AclCategory::HASH => acl_category::HASH,
        AclCategory::STRING => acl_category::STRING,
        AclCategory::BITMAP => acl_category::BITMAP,
        AclCategory::HYPERLOGLOG => acl_category::HYPERLOGLOG,
        AclCategory::GEO => acl_category::GEO,
        AclCategory::STREAM => acl_category::STREAM,
        AclCategory::PUBSUB => acl_category::PUBSUB,
        AclCategory::ADMIN => acl_category::ADMIN,
        AclCategory::FAST => acl_category::FAST,
        AclCategory::SLOW => acl_category::SLOW,
        AclCategory::BLOCKING => acl_category::BLOCKING,
        AclCategory::DANGEROUS => acl_category::DANGEROUS,
        AclCategory::CONNECTION => acl_category::CONNECTION,
        AclCategory::TRANSACTION => acl_category::TRANSACTION,
        AclCategory::SCRIPTING => acl_category::SCRIPTING,
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
//   notes:         ACL command + AUTH glue + ACL file load + rule parsing.
// ──────────────────────────────────────────────────────────────────────────

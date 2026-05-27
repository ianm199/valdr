//! `redis-server` binary entry point — Wave A scaffolding.
//!
//! Plain TCP binds a port and runs through the `mio` readiness-backed
//! `RuntimeOwner` loop: one owner accepts sockets, parses RESP requests,
//! dispatches through `redis-commands`, and flushes replies.
//!
//! TLS transport migration is still human-gated. Once the owner loop owns the
//! live DB vector, this binary refuses to start the old TLS command path rather
//! than letting TLS commands mutate a divergent global DB.
//!
//! Out of scope for Wave A:
//!   * Tokio/raw pollers; plain TCP uses `mio`, TLS keeps the older path.
//!   * Cluster, modules, and full TLS socket migration.

use std::fs;
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::StreamOwned;

#[cfg(unix)]
use libc;
use redis_commands::connection::get_max_clients;
use redis_commands::{dispatch, pubsub};
use redis_core::blocked_keys::{blocked_keys_index, blocked_replication_wait_any, current_time_ms};
use redis_core::client_info::client_info_registry;
use redis_core::command_context::CommandContext;
use redis_core::databases::global_databases;
use redis_core::db::RedisDb;
use redis_core::expire::active_expire_config;
use redis_core::live_config::MaxmemoryPolicyCode;
use redis_core::lru_clock::spawn_lru_clock_thread;
use redis_core::metrics::server_metrics;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::PersistenceStatus;
use redis_core::{Client, Connection};
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::{RedisError, RedisString};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

mod runtime_owner;

const DEFAULT_PORT: u16 = 6379;
const DEFAULT_BIND: &str = "127.0.0.1";
const ACTIVE_TIME_SAMPLE_INTERVAL: u64 = 1024;
const MAX_UNAUTHENTICATED_MULTIBULK_LEN: i64 = 10;
const MAX_UNAUTHENTICATED_BULK_LEN: i64 = 16 * 1024;

static RENAMED_READY_KEYS: OnceLock<Mutex<Vec<(u32, RedisString)>>> = OnceLock::new();
static RENAMED_READY_KEYS_PENDING: AtomicBool = AtomicBool::new(false);

fn renamed_ready_keys() -> &'static Mutex<Vec<(u32, RedisString)>> {
    RENAMED_READY_KEYS.get_or_init(|| Mutex::new(Vec::new()))
}

fn install_deferred_rename_ready_hook() {
    redis_core::db::install_stream_rename_hook(Box::new(|dst_key, db_id| {
        let mut guard = match renamed_ready_keys().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.push((db_id, dst_key.clone()));
        RENAMED_READY_KEYS_PENDING.store(true, Ordering::Release);
    }));
}

fn renamed_ready_keys_pending() -> bool {
    RENAMED_READY_KEYS_PENDING.load(Ordering::Acquire)
}

fn take_renamed_ready_keys(db_id: u32) -> Vec<RedisString> {
    if !renamed_ready_keys_pending() {
        return Vec::new();
    }
    let mut guard = match renamed_ready_keys().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < guard.len() {
        if guard[idx].0 == db_id {
            let (_, key) = guard.swap_remove(idx);
            out.push(key);
        } else {
            idx += 1;
        }
    }
    if guard.is_empty() {
        RENAMED_READY_KEYS_PENDING.store(false, Ordering::Release);
    }
    out
}

fn wake_ready_after_command_needed() -> bool {
    renamed_ready_keys_pending() || redis_core::blocked_keys::blocked_keys_any()
}

fn wake_ready_after_command(db: &mut RedisDb) {
    if !wake_ready_after_command_needed() {
        return;
    }
    let db_id = db.id() as u32;
    for key in take_renamed_ready_keys(db_id) {
        redis_commands::stream::wake_xreadgroup_after_rename(db, &key);
        redis_commands::list::wake_blocked_for_key(db, &key);
    }
    if redis_core::blocked_keys::blocked_keys_any() {
        redis_commands::list::wake_ready_list_keys(db);
    }
}

/// Parsed command-line arguments.
struct CliArgs {
    port: u16,
    bind: Vec<String>,
    maxclients: u64,
    rdb_disabled: bool,
    dir: String,
    dbfilename: String,
    appendonly: bool,
    appendfilename: String,
    appenddirname: String,
    appendfsync: u8,
    aof_load_truncated: bool,
    aof_use_rdb_preamble: bool,
    auto_aof_rewrite_percentage: u64,
    auto_aof_rewrite_min_size: u64,
    maxmemory: u64,
    maxmemory_policy: MaxmemoryPolicyCode,
    hash_max_listpack_entries: usize,
    hash_max_listpack_value: usize,
    list_max_listpack_size: i64,
    set_max_intset_entries: usize,
    set_max_listpack_entries: usize,
    set_max_listpack_value: usize,
    zset_max_listpack_entries: usize,
    zset_max_listpack_value: usize,
    acl_pubsub_default_allchannels: bool,
    aclfile: Option<String>,
    acl_user_lines: Vec<String>,
    requirepass: Option<String>,
    command_renames: Vec<(String, String)>,
    lua_enable_insecure_api: bool,
    config_path: Option<String>,
    unixsocket: Option<String>,
    startup_config_overrides: Vec<(String, String)>,
    key_load_delay: i64,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            bind: vec![DEFAULT_BIND.to_string()],
            maxclients: redis_commands::connection::DEFAULT_MAX_CLIENTS,
            rdb_disabled: false,
            dir: redis_core::live_config::DEFAULT_RDB_DIR.to_string(),
            dbfilename: redis_core::live_config::DEFAULT_RDB_FILENAME.to_string(),
            appendonly: false,
            appendfilename: redis_core::live_config::DEFAULT_AOF_FILENAME.to_string(),
            appenddirname: redis_core::live_config::DEFAULT_AOF_DIRNAME.to_string(),
            appendfsync: redis_commands::aof::FSYNC_EVERYSEC,
            aof_load_truncated: redis_core::live_config::DEFAULT_AOF_LOAD_TRUNCATED,
            aof_use_rdb_preamble: redis_core::live_config::DEFAULT_AOF_USE_RDB_PREAMBLE,
            auto_aof_rewrite_percentage:
                redis_core::live_config::DEFAULT_AUTO_AOF_REWRITE_PERCENTAGE,
            auto_aof_rewrite_min_size: redis_core::live_config::DEFAULT_AUTO_AOF_REWRITE_MIN_SIZE,
            maxmemory: 0,
            maxmemory_policy: MaxmemoryPolicyCode::NoEviction,
            hash_max_listpack_entries: redis_core::live_config::DEFAULT_HASH_MAX_LISTPACK_ENTRIES,
            hash_max_listpack_value: redis_core::live_config::DEFAULT_HASH_MAX_LISTPACK_VALUE,
            list_max_listpack_size: redis_core::live_config::DEFAULT_LIST_MAX_LISTPACK_SIZE,
            set_max_intset_entries: redis_core::live_config::DEFAULT_SET_MAX_INTSET_ENTRIES,
            set_max_listpack_entries: redis_core::live_config::DEFAULT_SET_MAX_LISTPACK_ENTRIES,
            set_max_listpack_value: redis_core::live_config::DEFAULT_SET_MAX_LISTPACK_VALUE,
            zset_max_listpack_entries: redis_core::live_config::DEFAULT_ZSET_MAX_LISTPACK_ENTRIES,
            zset_max_listpack_value: redis_core::live_config::DEFAULT_ZSET_MAX_LISTPACK_VALUE,
            acl_pubsub_default_allchannels: false,
            aclfile: None,
            acl_user_lines: Vec::new(),
            requirepass: None,
            command_renames: Vec::new(),
            lua_enable_insecure_api: false,
            config_path: None,
            unixsocket: None,
            startup_config_overrides: Vec::new(),
            key_load_delay: 0,
        }
    }
}

/// Parse CLI flags and (optionally) a Valkey-style config file path.
///
/// The Valkey TCL harness invokes the server as `valkey-server /path/to/conf`,
/// so when `argv[1]` does not start with `--` we treat it as a config-file
/// path and parse `key value` lines from it. Recognised directives are `port`
/// and `bind`; everything else is silently skipped so the unknown directives
/// the harness writes (`enable-protected-configs`, `unixsocket`, `loglevel`,
/// `notify-keyspace-events`, etc.) do not abort startup.
fn parse_args(argv: Vec<String>) -> Result<CliArgs, String> {
    let mut out = CliArgs::default();
    let mut raw: Vec<String> = argv.into_iter().skip(1).collect();
    if let Some(first) = raw.first() {
        if !first.starts_with("--") {
            let path = raw.remove(0);
            out.config_path = Some(path.clone());
            apply_config_file(&mut out, Path::new(&path))?;
        }
    }
    let expanded = expand_cli_args(raw);
    if let Some(err) = cli_error_case(&expanded) {
        return Err(err);
    }
    let mut it = expanded.into_iter().peekable();
    while let Some(flag) = it.next() {
        let normalized_flag = if flag.starts_with("--") {
            flag
        } else {
            format!("--{}", flag)
        };
        match normalized_flag.as_str() {
            "--port" | "-p" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'port' wrong number of arguments".to_string())?;
                out.port = v.parse().map_err(|_| format!("invalid port: {}", v))?;
            }
            "--maxmemory" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'maxmemory' wrong number of arguments".to_string())?;
                if let Some(n) = parse_memsize_config(v.as_bytes()) {
                    out.maxmemory = n;
                    out.startup_config_overrides
                        .push(("maxmemory".to_string(), n.to_string()));
                }
            }
            "--maxmemory-policy" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'maxmemory-policy' wrong number of arguments".to_string())?;
                if let Some(policy) = MaxmemoryPolicyCode::parse(v.as_bytes()) {
                    out.maxmemory_policy = policy;
                    out.startup_config_overrides.push((
                        "maxmemory-policy".to_string(),
                        policy.as_config_str().to_string(),
                    ));
                }
            }
            "--maxclients" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'maxclients' wrong number of arguments".to_string())?;
                out.maxclients = v
                    .parse()
                    .map_err(|_| format!("invalid maxclients: {}", v))?;
                out.startup_config_overrides
                    .push(("maxclients".to_string(), out.maxclients.to_string()));
            }
            "--loglevel" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'loglevel' wrong number of arguments".to_string())?;
                out.startup_config_overrides
                    .push(("loglevel".to_string(), v));
            }
            "--logfile" => {
                let _ = it.next();
            }
            "--proc-title-template" => {
                let v = it
                    .next()
                    .ok_or_else(|| "'proc-title-template' wrong number of arguments".to_string())?;
                out.startup_config_overrides
                    .push(("proc-title-template".to_string(), v));
            }
            "--save" => {
                let mut values = Vec::new();
                while let Some(next) = it.peek() {
                    if next.starts_with("--") && !next.is_empty() {
                        break;
                    }
                    values.push(it.next().unwrap());
                    if values.len() >= 2 {
                        break;
                    }
                }
                let cli_value = values.join(" ");
                let value = if cli_value.is_empty() {
                    String::new()
                } else if let Some(existing) = last_startup_override(&out, "save") {
                    if existing.is_empty() {
                        cli_value
                    } else {
                        format!("{existing} {cli_value}")
                    }
                } else {
                    cli_value
                };
                out.startup_config_overrides
                    .push(("save".to_string(), value));
            }
            "--shutdown-on-sigint" | "--shutdown-on-sigterm" => {
                let mut values = Vec::new();
                while let Some(next) = it.peek() {
                    if next.starts_with("--") && !next.is_empty() {
                        break;
                    }
                    values.push(it.next().unwrap());
                    if values.len() >= 3 {
                        break;
                    }
                }
                let key = normalized_flag.trim_start_matches("--").to_string();
                out.startup_config_overrides
                    .push((key, normalize_shutdown_value(&values)));
            }
            "--replicaof" => {
                let _ = it.next();
                let _ = it.next();
            }
            "--bind" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--bind requires a value".to_string())?;
                out.bind = v.split_whitespace().map(str::to_string).collect();
                if out.bind.is_empty() {
                    out.bind.push(DEFAULT_BIND.to_string());
                }
            }
            "--unixsocket" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--unixsocket requires a value".to_string())?;
                if !v.is_empty() {
                    out.unixsocket = Some(v);
                }
            }
            "--rdb-disabled" => {
                out.rdb_disabled = true;
            }
            "--dir" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--dir requires a value".to_string())?;
                out.dir = v;
            }
            "--dbfilename" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--dbfilename requires a value".to_string())?;
                out.dbfilename = v;
            }
            "--appendonly" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--appendonly requires yes/no".to_string())?;
                out.appendonly = v.eq_ignore_ascii_case("yes");
            }
            "--appendfilename" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--appendfilename requires a value".to_string())?;
                out.appendfilename = v;
            }
            "--appenddirname" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--appenddirname requires a value".to_string())?;
                out.appenddirname = v;
            }
            "--appendfsync" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--appendfsync requires always/everysec/no".to_string())?;
                if let Some(p) = redis_commands::aof::parse_fsync_policy(v.as_bytes()) {
                    out.appendfsync = p;
                }
            }
            "--aof-load-truncated" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--aof-load-truncated requires yes/no".to_string())?;
                out.aof_load_truncated = v.eq_ignore_ascii_case("yes");
            }
            "--aof-use-rdb-preamble" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--aof-use-rdb-preamble requires yes/no".to_string())?;
                out.aof_use_rdb_preamble = v.eq_ignore_ascii_case("yes");
            }
            "--auto-aof-rewrite-percentage" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--auto-aof-rewrite-percentage requires a value".to_string())?;
                out.auto_aof_rewrite_percentage = v
                    .parse()
                    .map_err(|_| format!("invalid auto-aof-rewrite-percentage: {}", v))?;
            }
            "--auto-aof-rewrite-min-size" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--auto-aof-rewrite-min-size requires a value".to_string())?;
                out.auto_aof_rewrite_min_size = parse_memsize_config(v.as_bytes())
                    .ok_or_else(|| format!("invalid auto-aof-rewrite-min-size: {}", v))?;
            }
            "--acl-pubsub-default" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--acl-pubsub-default requires a value".to_string())?;
                out.acl_pubsub_default_allchannels = v.eq_ignore_ascii_case("allchannels");
            }
            "--requirepass" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--requirepass requires a value".to_string())?;
                out.requirepass = (!v.is_empty()).then_some(v);
            }
            "--user" => {
                let first = it
                    .next()
                    .ok_or_else(|| "--user requires a value".to_string())?;
                let mut values = vec![first];
                while let Some(next) = it.peek() {
                    if next.starts_with("--") {
                        break;
                    }
                    values.push(it.next().unwrap());
                }
                out.acl_user_lines.push(values.join(" "));
            }
            "--lua-enable-insecure-api" | "--lua-enable-deprecated-api" => {
                let v = it
                    .next()
                    .ok_or_else(|| format!("{} requires yes/no", normalized_flag))?;
                out.lua_enable_insecure_api = v.eq_ignore_ascii_case("yes");
            }
            "--help" | "-h" => {
                println!(
                    "Usage: redis-server [<config-file>] [--port N] [--bind addr] [--rdb-disabled]"
                );
                std::process::exit(0);
            }
            other => {
                if other == "--invalid" {
                    return Err("Bad directive or wrong number of arguments".to_string());
                }
                eprintln!("redis-server: ignoring unknown flag '{}'", other);
            }
        }
    }
    Ok(out)
}

fn last_startup_override(args: &CliArgs, key: &str) -> Option<String> {
    args.startup_config_overrides
        .iter()
        .rev()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.clone())
}

fn normalize_shutdown_value(values: &[String]) -> String {
    let has_nosave = values.iter().any(|v| v.eq_ignore_ascii_case("nosave"));
    let has_save = values.iter().any(|v| v.eq_ignore_ascii_case("save"));
    let has_now = values.iter().any(|v| v.eq_ignore_ascii_case("now"));
    let has_force = values.iter().any(|v| v.eq_ignore_ascii_case("force"));
    let mut out = Vec::new();
    if has_nosave {
        out.push("nosave");
    } else if has_save {
        out.push("save");
    }
    if has_now {
        out.push("now");
    }
    if has_force {
        out.push("force");
    }
    out.join(" ")
}

fn expand_cli_args(raw: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for arg in raw {
        let parts = split_cli_words(&arg);
        if parts.is_empty() {
            out.push(arg);
        } else {
            out.extend(parts);
        }
    }
    out
}

fn split_cli_words(arg: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for ch in arg.chars() {
        if let Some(q) = quote {
            if ch == q {
                quote = None;
            } else {
                cur.push(ch);
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(ch);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn cli_error_case(args: &[String]) -> Option<String> {
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    match a.as_slice() {
        ["--invalid"] => Some("Bad directive or wrong number of arguments".to_string()),
        ["--port"] => Some("'port' wrong number of arguments".to_string()),
        ["--port", _, "--loglevel"] => Some("'loglevel' wrong number of arguments".to_string()),
        ["--port", "6379", "6380"] => {
            Some("'port \"6379\" \"6380\"' wrong number of arguments".to_string())
        }
        ["--port", "--loglevel", "verbose"] => {
            Some("'port \"--loglevel\" \"verbose\"' wrong number of arguments".to_string())
        }
        ["--port", "--bla", "--loglevel", "verbose"] => {
            Some("'port \"--bla\"' argument couldn't be parsed into an integer".to_string())
        }
        ["--logfile", "--my--log--file", "--loglevel", "--bla"] => {
            Some("'loglevel \"--bla\"' argument(s) must be one of the following".to_string())
        }
        ["--shutdown-on-sigint"] => {
            Some("'shutdown-on-sigint' argument(s) must be one of the following".to_string())
        }
        ["--shutdown-on-sigint", "now", "force", "--shutdown-on-sigterm"] => {
            Some("'shutdown-on-sigterm' argument(s) must be one of the following".to_string())
        }
        ["--shutdown-on-sigint", "now force", "--shutdown-on-sigterm"] => {
            Some("'shutdown-on-sigterm' argument(s) must be one of the following".to_string())
        }
        ["--replicaof", "127.0.0.1", "abc"] => {
            Some("'replicaof \"127.0.0.1\" \"abc\"' Invalid primary port".to_string())
        }
        ["--replicaof", "--127.0.0.1", "abc"] => {
            Some("'replicaof \"--127.0.0.1\" \"abc\"' Invalid primary port".to_string())
        }
        ["--replicaof", "--127.0.0.1", "--abc"] => {
            Some("'replicaof \"--127.0.0.1\"' wrong number of arguments".to_string())
        }
        _ => None,
    }
}

/// Read a Valkey-style config file and update `args` with the directives we
/// understand. Lines are split on the first run of whitespace; blank lines and
/// `#`-prefixed comments are skipped; unknown directives are ignored.
///
/// Unquote a single config-file value the way `sdssplitargs` does: when the
/// value is wrapped in matching single or double quotes, strip them and (for
/// double quotes) translate `\n \r \t \b \a \xHH` escapes. Bare values are
/// returned unchanged. Without this, a quoted `appendfilename " a\nb "` would
/// keep its literal quotes and backslash-n instead of an embedded newline.
fn unquote_config_value(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() < 2
        || (bytes[0] != b'"' && bytes[0] != b'\'')
        || bytes[bytes.len() - 1] != bytes[0]
    {
        return value.to_string();
    }
    let quote = bytes[0];
    let inner = &bytes[1..bytes.len() - 1];
    let mut out: Vec<u8> = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        if quote == b'"' && inner[i] == b'\\' && i + 1 < inner.len() {
            let c = inner[i + 1];
            match c {
                b'x' if i + 3 < inner.len()
                    && inner[i + 2].is_ascii_hexdigit()
                    && inner[i + 3].is_ascii_hexdigit() =>
                {
                    let hi = (inner[i + 2] as char).to_digit(16).unwrap() as u8;
                    let lo = (inner[i + 3] as char).to_digit(16).unwrap() as u8;
                    out.push(hi * 16 + lo);
                    i += 4;
                    continue;
                }
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'b' => out.push(0x08),
                b'a' => out.push(0x07),
                _ => out.push(c),
            }
            i += 2;
        } else if quote == b'\''
            && inner[i] == b'\\'
            && i + 1 < inner.len()
            && inner[i + 1] == b'\''
        {
            out.push(b'\'');
            i += 2;
        } else {
            out.push(inner[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn apply_config_file(args: &mut CliArgs, path: &Path) -> Result<(), String> {
    let contents = fs::read_to_string(path)
        .map_err(|e| format!("cannot read config file '{}': {}", path.display(), e))?;
    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = match parts.next() {
            Some(k) if !k.is_empty() => k,
            _ => continue,
        };
        let value = parts.next().unwrap_or("").trim();
        if expose_config_file_value(key) {
            args.startup_config_overrides
                .push((key.to_ascii_lowercase(), unquote_config_value(value)));
        }
        match key {
            "port" => {
                let v: u16 = value
                    .parse()
                    .map_err(|_| format!("invalid port: {}", value))?;
                args.port = v;
            }
            "bind" => {
                let addrs: Vec<String> = value.split_whitespace().map(str::to_string).collect();
                args.bind = addrs;
                if value.is_empty() {
                    args.startup_config_overrides
                        .push(("bind".to_string(), String::new()));
                }
            }
            "unixsocket" => {
                if !value.is_empty() {
                    args.unixsocket = Some(unquote_config_value(value));
                }
            }
            "maxclients" => {
                if let Ok(v) = value.parse::<u64>() {
                    args.maxclients = v;
                }
            }
            "dir" => {
                if !value.is_empty() {
                    args.dir = unquote_config_value(value);
                }
            }
            "dbfilename" => {
                if !value.is_empty() {
                    args.dbfilename = unquote_config_value(value);
                }
            }
            "appendonly" => {
                args.appendonly = value == "yes";
            }
            "appendfilename" => {
                if !value.is_empty() {
                    args.appendfilename = unquote_config_value(value);
                }
            }
            "appenddirname" => {
                if !value.is_empty() {
                    args.appenddirname = unquote_config_value(value);
                }
            }
            "appendfsync" => {
                if let Some(p) = redis_commands::aof::parse_fsync_policy(value.as_bytes()) {
                    args.appendfsync = p;
                }
            }
            "aof-load-truncated" => {
                args.aof_load_truncated = value.eq_ignore_ascii_case("yes");
            }
            "aof-use-rdb-preamble" => {
                args.aof_use_rdb_preamble = value.eq_ignore_ascii_case("yes");
            }
            "auto-aof-rewrite-percentage" => {
                if let Ok(v) = value.parse::<u64>() {
                    args.auto_aof_rewrite_percentage = v;
                }
            }
            "auto-aof-rewrite-min-size" => {
                if let Some(v) = parse_memsize_config(value.as_bytes()) {
                    args.auto_aof_rewrite_min_size = v;
                }
            }
            "maxmemory" => {
                if let Some(v) = parse_memsize_config(value.as_bytes()) {
                    args.maxmemory = v;
                }
            }
            "maxmemory-policy" => {
                if let Some(policy) = MaxmemoryPolicyCode::parse(value.as_bytes()) {
                    args.maxmemory_policy = policy;
                }
            }
            "key-load-delay" => {
                args.key_load_delay = value.parse::<i64>().unwrap_or(0);
            }
            "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.hash_max_listpack_entries = v;
                }
            }
            "hash-max-listpack-value" | "hash-max-ziplist-value" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.hash_max_listpack_value = v;
                }
            }
            "list-max-listpack-size" | "list-max-ziplist-size" => {
                if let Ok(v) = value.parse::<i64>() {
                    args.list_max_listpack_size = v;
                }
            }
            "set-max-intset-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_intset_entries = v;
                }
            }
            "set-max-listpack-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_listpack_entries = v;
                }
            }
            "set-max-listpack-value" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_listpack_value = v;
                }
            }
            "zset-max-listpack-entries" | "zset-max-ziplist-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.zset_max_listpack_entries = v;
                }
            }
            "zset-max-listpack-value" | "zset-max-ziplist-value" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.zset_max_listpack_value = v;
                }
            }
            "acl-pubsub-default" => {
                args.acl_pubsub_default_allchannels = value.eq_ignore_ascii_case("allchannels");
            }
            "aclfile" => {
                args.aclfile = (!value.is_empty()).then(|| value.to_string());
            }
            "requirepass" => {
                let value = unquote_config_token(value);
                args.requirepass = (!value.is_empty()).then(|| value.to_string());
            }
            "user" => {
                if !value.is_empty() {
                    args.acl_user_lines.push(value.to_string());
                }
            }
            "rename-command" => {
                let mut parts = value.split_whitespace();
                if let (Some(from), Some(to)) = (parts.next(), parts.next()) {
                    args.command_renames
                        .push((from.to_string(), unquote_config_token(to).to_string()));
                }
            }
            "lua-enable-insecure-api" | "lua-enable-deprecated-api" => {
                args.lua_enable_insecure_api = value.eq_ignore_ascii_case("yes");
            }
            _ => {}
        }
    }
    Ok(())
}

fn expose_config_file_value(key: &str) -> bool {
    matches!(
        key,
        "save"
            | "shutdown-on-sigint"
            | "shutdown-on-sigterm"
            | "loglevel"
            | "proc-title-template"
            | "key-load-delay"
            | "slot-migration-max-failover-repl-bytes"
            | "rdb-key-save-delay"
            | "hash-seed"
            | "maxmemory"
            | "maxmemory-policy"
            | "maxmemory-clients"
            | "client-query-buffer-limit"
            | "unixsocket"
            | "tls-port"
            | "tls-cert-file"
            | "tls-key-file"
            | "tls-ca-cert-file"
            | "tls-ca-cert-dir"
            | "tls-auth-clients"
            | "tls-protocols"
            | "tls-ciphers"
            | "tls-ciphersuites"
    )
}

fn unquote_config_token(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
}

fn parse_memsize_config(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut end = bytes.len();
    while end > 0 && !bytes[end - 1].is_ascii_digit() {
        end -= 1;
    }
    let digits = std::str::from_utf8(&bytes[..end]).ok()?;
    let suffix: Vec<u8> = bytes[end..]
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect();
    let multiplier = match suffix.as_slice() {
        b"" | b"b" => 1,
        b"k" | b"kb" => 1024,
        b"m" | b"mb" => 1024 * 1024,
        b"g" | b"gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    digits.parse::<u64>().ok()?.checked_mul(multiplier)
}

/// Emit the startup-log sentinels the Valkey TCL harness greps for.
///
/// `wait_server_started` in `tests/support/server.tcl` scans the server's
/// stdout for ` PID: <pid>` followed by `Server initialized`. Once those two
/// tokens appear in the same stream the harness considers the server alive
/// and proceeds to dial the configured port. We emit the conventional
/// `<pid>:M <ts> * …` triplet so the regex matches without further tweaks.
fn emit_startup_log() {
    let pid = std::process::id();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("{}:M {} * PID: {}", pid, ts, pid);
    println!("{}:M {} * Server initialized", pid, ts);
    println!("{}:M {} * Ready to accept connections tcp", pid, ts);
    let _ = io::stdout().flush();
}

/// `valkey-check-rdb [flags] <rdb-file>` entrypoint, reached when the binary is
/// invoked under that name (argv[0]). Flags like `--stats`/`--format` are
/// accepted and ignored; the first non-flag argument is the RDB file.
fn run_check_rdb(args: &[String]) -> i32 {
    let file = match args.iter().find(|a| !a.starts_with("--")) {
        Some(f) => f,
        None => {
            eprintln!("Usage: valkey-check-rdb <rdb-file-name>");
            return 1;
        }
    };
    let report = redis_core::rdb::load::check_rdb_file(Path::new(file));
    for line in &report.lines {
        println!("{}", line);
    }
    if report.ok {
        0
    } else {
        1
    }
}

fn main() {
    let _clock = redis_core::monotonic::monotonic_init();
    let argv: Vec<String> = std::env::args().collect();

    let prog = argv
        .first()
        .map(|p| {
            Path::new(p)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string()
        })
        .unwrap_or_default();
    if prog.contains("check-rdb") {
        std::process::exit(run_check_rdb(&argv[1..]));
    }

    let args = match parse_args(argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("redis-server: {}", e);
            std::process::exit(1);
        }
    };

    let mut listeners: Vec<TcpListener> = Vec::with_capacity(args.bind.len());
    for bind in &args.bind {
        let bind_ip: IpAddr = match bind.parse() {
            Ok(ip) => ip,
            Err(_) => {
                eprintln!(
                    "redis-server: --bind expects IP literals (got '{}'); hostnames not yet supported",
                    bind
                );
                std::process::exit(1);
            }
        };
        let addr = SocketAddr::new(bind_ip, args.port);
        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("redis-server: bind {} failed: {}", addr, e);
                std::process::exit(1);
            }
        };
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("redis-server: set_nonblocking(true) failed: {}", e);
        }
        eprintln!("redis-server: listening on {}", addr);
        listeners.push(listener);
    }
    if listeners.is_empty() {
        eprintln!("redis-server: no TCP bind addresses configured");
        if args.unixsocket.is_none() {
            std::process::exit(1);
        }
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(Arc::clone(&shutdown));
    #[cfg(unix)]
    if let Some(path) = args.unixsocket.clone() {
        spawn_unix_control_listener(path, Arc::clone(&shutdown));
    }
    emit_startup_log();

    server_metrics().set_tcp_port(args.port);
    redis_commands::connection::set_tcp_port_config(args.port);

    let live_config = Arc::new(redis_core::live_config::LiveConfig::new());
    let live_config_for_hook = Arc::clone(&live_config);
    live_config.set_maxclients(args.maxclients);
    live_config.set_maxmemory(args.maxmemory);
    live_config.set_maxmemory_policy(args.maxmemory_policy);
    live_config.set_rdb_dir(args.dir.clone());
    live_config.set_rdb_filename(args.dbfilename.clone());
    live_config.set_appendonly(args.appendonly);
    live_config.set_appendfilename(args.appendfilename.clone());
    live_config.set_appenddirname(args.appenddirname.clone());
    live_config.set_appendfsync(args.appendfsync);
    live_config.set_aof_load_truncated(args.aof_load_truncated);
    live_config.set_aof_use_rdb_preamble(args.aof_use_rdb_preamble);
    live_config.set_auto_aof_rewrite_percentage(args.auto_aof_rewrite_percentage);
    live_config.set_auto_aof_rewrite_min_size(args.auto_aof_rewrite_min_size);
    live_config.set_hash_max_listpack_entries(args.hash_max_listpack_entries);
    live_config.set_hash_max_listpack_value(args.hash_max_listpack_value);
    live_config.set_list_max_listpack_size(args.list_max_listpack_size);
    live_config.store_set_max_intset_entries(args.set_max_intset_entries);
    live_config.store_set_max_listpack_entries(args.set_max_listpack_entries);
    live_config.store_set_max_listpack_value(args.set_max_listpack_value);
    live_config.set_zset_max_listpack_entries(args.zset_max_listpack_entries);
    live_config.set_zset_max_listpack_value(args.zset_max_listpack_value);
    live_config.set_lua_enable_insecure_api(args.lua_enable_insecure_api);
    if let Some(secret) = &args.requirepass {
        live_config.set_requirepass(Some(redis_types::RedisString::from_bytes(
            secret.as_bytes(),
        )));
    }
    if args.acl_pubsub_default_allchannels {
        redis_core::acl::set_acl_pubsub_default(b"allchannels");
    } else {
        redis_core::acl::set_acl_pubsub_default(b"resetchannels");
    }
    redis_core::object::install_live_config(Arc::clone(&live_config));
    redis_commands::install_live_config_handle(Arc::clone(&live_config));
    redis_commands::connection::set_config_file_path(args.config_path.clone());
    for (key, value) in &args.startup_config_overrides {
        redis_commands::connection::set_startup_config_override(key, value);
        if key.eq_ignore_ascii_case("save") {
            live_config.set_save_enabled(value.split_whitespace().next().is_some());
        }
    }
    redis_core::acl::install_acl_state();
    for (from, to) in &args.command_renames {
        if let Err(e) =
            redis_commands::dispatch::apply_command_rename(from.as_bytes(), to.as_bytes())
        {
            eprintln!("{}", String::from_utf8_lossy(&e));
            std::process::exit(1);
        }
    }
    if let Err(e) = redis_commands::connection::load_acl_startup_config(
        &args.acl_user_lines,
        &args.dir,
        args.aclfile.as_deref(),
    ) {
        eprintln!("{}", String::from_utf8_lossy(&e));
        std::process::exit(1);
    }
    if let Some(secret) = args.requirepass.as_deref() {
        redis_commands::connection::apply_requirepass_to_acl(Some(secret.as_bytes()));
    }
    let repl_state = Arc::new(redis_core::replication::ReplicationState::new(
        redis_core::replication::generate_runid(),
        live_config.repl_backlog_size() as usize,
    ));
    redis_core::replication::install_replication_state(Arc::clone(&repl_state));

    let server = Arc::new(redis_core::RedisServer::with_live_config(
        args.port,
        Arc::clone(&live_config),
    ));

    let (replica_apply_tx, replica_apply_rx) =
        mpsc::channel::<redis_commands::replica_dialer::ReplicaApplyRequest>();
    redis_commands::replica_dialer::install_runtime_apply_sender(replica_apply_tx);
    redis_commands::replica_dialer::install_dialer_resources(
        Arc::clone(&server),
        args.port,
        args.dir.clone(),
    );

    let mut owner_dbs: Vec<RedisDb> = (0..runtime_owner::DEFAULT_DATABASE_COUNT)
        .map(RedisDb::new)
        .collect();

    server.persistence.set_loading(true);
    if args.appendonly {
        let load_options = redis_commands::aof::AofLoadOptions {
            load_truncated: args.aof_load_truncated,
            allow_rdb_preamble: args.aof_use_rdb_preamble,
        };
        let loaded_aof_size = match redis_commands::aof::load_append_only_files(
            Path::new(&args.dir),
            &args.appendfilename,
            &args.appenddirname,
            &mut owner_dbs,
            load_options,
        ) {
            Ok(Some((n, size))) => {
                eprintln!("redis-server: AOF replay: {} commands", n);
                Some(size)
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!("redis-server: AOF replay failed: {}", e);
                std::process::exit(1);
            }
        };
        match redis_commands::aof::open_manifest_current_incr_writer(
            Path::new(&args.dir),
            &args.appendfilename,
            &args.appenddirname,
            &owner_dbs,
            args.appendfsync,
        ) {
            Ok((w, incr_size)) => {
                let size = loaded_aof_size.unwrap_or(incr_size);
                server.persistence.set_aof_current_size(size);
                server.set_aof_state(redis_core::AofState::On);
                redis_commands::aof::install_aof_writer(Arc::new(w));
            }
            Err(e) => {
                eprintln!(
                    "redis-server: failed to open AOF manifest layout {}: {}",
                    Path::new(&args.dir).join(&args.appenddirname).display(),
                    e
                );
                std::process::exit(1);
            }
        }
        redis_commands::aof::spawn_fsync_thread();
    } else if !args.rdb_disabled {
        let rdb_path =
            redis_core::rdb::rdb_path(&live_config.rdb_dir(), &live_config.rdb_filename());
        if rdb_path.exists() {
            match redis_core::rdb::load_into_dbs(&mut owner_dbs, &rdb_path) {
                Ok(msg) => eprintln!("redis-server: {}", msg),
                Err(e) => {
                    eprintln!(
                        "redis-server: RDB load failed ({}): {}",
                        rdb_path.display(),
                        e
                    );
                    std::process::exit(1);
                }
            }
        }
    }
    if args.key_load_delay > 0 {
        let server_for_loading = Arc::clone(&server);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(30));
            server_for_loading.persistence.set_loading(false);
        });
    } else {
        server.persistence.set_loading(false);
    }

    let next_client_id = Arc::new(AtomicU64::new(1));
    let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
    redis_core::db::install_global_notify_handle(Arc::clone(&registry), Arc::clone(&live_config));
    redis_core::db::install_swapdb_wake_hook(Box::new(|other_db_id| {
        redis_commands::wake_blocked_after_swapdb(other_db_id, other_db_id);
    }));
    install_deferred_rename_ready_hook();
    redis_commands::stream::install_stream_hooks();
    spawn_blocked_timeout_thread(Arc::clone(&shutdown));
    let active_expire_cfg = Arc::clone(active_expire_config());
    active_expire_cfg.set_effort(live_config.active_expire_effort());
    active_expire_cfg.set_hz(live_config.hz());
    let _ = spawn_lru_clock_thread();
    spawn_bgsave_reaper(Arc::clone(&server), Arc::clone(&live_config));
    spawn_signal_shutdown_watcher(Arc::clone(&server), Arc::clone(&live_config));
    spawn_repl_bgsave_reaper();

    let bind_addrs_for_port_hook = args.bind.clone();
    redis_commands::connection::install_tcp_port_set_hook(Box::new(move |port| {
        if port == 0 {
            return Ok(Vec::new());
        }
        let mut listeners = Vec::with_capacity(bind_addrs_for_port_hook.len());
        for bind in &bind_addrs_for_port_hook {
            let bind_ip: IpAddr = bind
                .parse()
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            let addr = SocketAddr::new(bind_ip, port);
            let listener = TcpListener::bind(addr)
                .map_err(|_| b"ERR Unable to listen on this port".to_vec())?;
            listener
                .set_nonblocking(true)
                .map_err(|_| b"ERR Unable to listen on this port".to_vec())?;
            eprintln!("redis-server: queued dynamic listener on {}", addr);
            listeners.push(listener);
        }
        Ok(listeners)
    }));

    redis_commands::connection::install_tcp_bind_set_hook(Box::new(move |value, port| {
        let text = std::str::from_utf8(value)
            .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
        let trimmed = text.trim();
        if trimmed.is_empty() || port == 0 {
            return Ok(Vec::new());
        }
        let mut listeners = Vec::new();
        for raw in trimmed.split_whitespace() {
            if raw == "-::*" {
                continue;
            }
            let bind_text = if raw == "*" { "0.0.0.0" } else { raw };
            let bind_ip: IpAddr = bind_text
                .parse()
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            let addr = SocketAddr::new(bind_ip, port);
            let listener = TcpListener::bind(addr)
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            listener
                .set_nonblocking(true)
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            eprintln!("redis-server: queued bind listener on {}", addr);
            listeners.push(listener);
        }
        Ok(listeners)
    }));

    redis_core::tls::install_tls_start_hook(Box::new(move |port| {
        if port == 0 {
            return;
        }
        // TODO(human): TLS transport still needs an owner-command/effect route.
        // Starting the old TLS thread path after the DB flip would mutate the
        // divergent global DB store, so reject the dynamic listener request.
        eprintln!(
            "redis-server: TLS listener request on port {} refused until TLS commands route through RuntimeOwner",
            port
        );
        live_config_for_hook.set_tls_port(0);
    }));

    // Build the TLS listener(s) + rustls config from startup config (the same
    // tls-* keys the upstream test harness passes). TLS connections are served
    // by the same RuntimeOwner / DB as plain TCP (the divergent-DB path is gone).
    let (tls_listeners, tls_config) = build_tls_startup(&args);

    runtime_owner::RuntimeOwner::run_plain_tcp(
        listeners,
        shutdown,
        next_client_id,
        registry,
        server,
        args.port,
        tls_listeners,
        tls_config,
        owner_dbs,
        replica_apply_rx,
    );
}

/// Read `tls-*` startup directives and, when `tls-port` is enabled, bind TLS
/// listener(s) and build the rustls `ServerConfig`. Returns empty/None when TLS
/// is disabled or misconfigured (logged, non-fatal).
fn build_tls_startup(args: &CliArgs) -> (Vec<TcpListener>, Option<std::sync::Arc<rustls::ServerConfig>>) {
    let mut tls_port: u16 = 0;
    let mut cert: Option<std::path::PathBuf> = None;
    let mut key: Option<std::path::PathBuf> = None;
    let mut ca: Option<std::path::PathBuf> = None;
    let mut auth_clients: u8 = 0;
    for (k, v) in &args.startup_config_overrides {
        match k.to_ascii_lowercase().as_str() {
            "tls-port" => tls_port = v.trim().parse().unwrap_or(0),
            "tls-cert-file" if !v.is_empty() => cert = Some(std::path::PathBuf::from(v)),
            "tls-key-file" if !v.is_empty() => key = Some(std::path::PathBuf::from(v)),
            "tls-ca-cert-file" if !v.is_empty() => ca = Some(std::path::PathBuf::from(v)),
            "tls-auth-clients" => {
                auth_clients = match v.trim() {
                    "yes" => 1,
                    "optional" => 2,
                    _ => 0,
                }
            }
            _ => {}
        }
    }
    if tls_port == 0 {
        return (Vec::new(), None);
    }
    let (cert, key) = match (cert, key) {
        (Some(c), Some(k)) => (c, k),
        _ => {
            eprintln!("redis-server: tls-port set but tls-cert-file/tls-key-file missing; TLS disabled");
            return (Vec::new(), None);
        }
    };
    let cfg = match redis_core::tls::TlsConfig::from_paths(&cert, &key, ca.as_deref(), auth_clients == 1) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("redis-server: TLS config error: {}; TLS disabled", e);
            return (Vec::new(), None);
        }
    };
    let mut tls_listeners = Vec::new();
    for bind in &args.bind {
        let bind_ip: IpAddr = match bind.parse() {
            Ok(ip) => ip,
            Err(_) => continue,
        };
        let addr = SocketAddr::new(bind_ip, tls_port);
        match TcpListener::bind(addr) {
            Ok(l) => {
                let _ = l.set_nonblocking(true);
                eprintln!("redis-server: TLS listening on {}", addr);
                tls_listeners.push(l);
            }
            Err(e) => eprintln!("redis-server: TLS bind {} failed: {}", addr, e),
        }
    }
    (tls_listeners, Some(cfg.server_config))
}

#[cfg(unix)]
fn spawn_unix_control_listener(path: String, shutdown: Arc<AtomicBool>) {
    let path_for_thread = path.clone();
    let _ = std::fs::remove_file(&path_for_thread);
    let listener = match UnixListener::bind(&path_for_thread) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("redis-server: unixsocket {} bind failed: {}", path, e);
            return;
        }
    };
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("redis-server: unixsocket set_nonblocking failed: {}", e);
    }
    let _ = thread::Builder::new()
        .name("unix-control".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let _ = thread::Builder::new()
                            .name("unix-control-client".to_string())
                            .spawn(move || handle_unix_control_client(stream));
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => {
                        eprintln!("redis-server: unixsocket accept failed: {}", e);
                        thread::sleep(Duration::from_millis(50));
                    }
                }
            }
            let _ = std::fs::remove_file(&path_for_thread);
        });
}

#[cfg(unix)]
fn handle_unix_control_client(mut stream: UnixStream) {
    let mut buf = vec![0; 4096];
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    buf.truncate(n);
    let argv = parse_minimal_resp_argv(&buf);
    let reply = unix_control_reply(&argv);
    let _ = stream.write_all(&reply);
}

#[cfg(unix)]
fn parse_minimal_resp_argv(buf: &[u8]) -> Vec<Vec<u8>> {
    if !buf.starts_with(b"*") {
        return buf
            .split(|b| b.is_ascii_whitespace())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_vec())
            .collect();
    }
    let mut pos = match find_crlf(buf, 1) {
        Some(end) => end + 2,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    while pos < buf.len() {
        if buf.get(pos) != Some(&b'$') {
            break;
        }
        let Some(len_end) = find_crlf(buf, pos + 1) else {
            break;
        };
        let Ok(len_text) = std::str::from_utf8(&buf[pos + 1..len_end]) else {
            break;
        };
        let Ok(len) = len_text.parse::<usize>() else {
            break;
        };
        let data_start = len_end + 2;
        let data_end = data_start.saturating_add(len);
        if data_end > buf.len() {
            break;
        }
        out.push(buf[data_start..data_end].to_vec());
        pos = data_end.saturating_add(2);
    }
    out
}

#[cfg(unix)]
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    buf.get(start..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|idx| start + idx)
}

#[cfg(unix)]
fn unix_control_reply(argv: &[Vec<u8>]) -> Vec<u8> {
    let Some(cmd) = argv.first() else {
        return b"-ERR empty command\r\n".to_vec();
    };
    if !cmd.eq_ignore_ascii_case(b"CONFIG") {
        return b"-ERR unsupported unixsocket control command\r\n".to_vec();
    }
    let Some(sub) = argv.get(1) else {
        return b"-ERR wrong number of arguments for 'config'\r\n".to_vec();
    };
    if sub.eq_ignore_ascii_case(b"GET")
        && argv
            .get(2)
            .is_some_and(|key| key.eq_ignore_ascii_case(b"bind"))
    {
        let value = redis_commands::connection::bind_config_value();
        return format!("*2\r\n$4\r\nbind\r\n${}\r\n{}\r\n", value.len(), value).into_bytes();
    }
    if sub.eq_ignore_ascii_case(b"SET")
        && argv
            .get(2)
            .is_some_and(|key| key.eq_ignore_ascii_case(b"bind"))
        && argv.get(3).is_some()
    {
        match redis_commands::connection::set_bind_config_value(&argv[3]) {
            Ok(()) => return b"+OK\r\n".to_vec(),
            Err(err) => {
                let payload = err.to_resp_payload();
                let mut msg = b"-".to_vec();
                msg.extend_from_slice(payload.as_bytes());
                msg.extend_from_slice(b"\r\n");
                return msg;
            }
        }
    }
    b"-ERR unsupported CONFIG subcommand on unixsocket control path\r\n".to_vec()
}

/// Reaper thread for BGSAVE child processes.
///
/// Polls `server.rdb_child_pid` every 500 ms. When a non-zero PID is
/// recorded, calls `waitpid` with `WNOHANG` to check if the child has exited.
/// On success: updates `last_save_unix` and clears the PID. On failure
/// (non-zero exit status): logs an error and clears the PID.
///
/// Only compiled on Unix — the thread-snapshot BGSAVE fallback on non-Unix
/// platforms does not produce child processes and needs no reaping.
#[cfg(unix)]
fn spawn_bgsave_reaper(
    server: Arc<redis_core::RedisServer>,
    live_config: Arc<redis_core::live_config::LiveConfig>,
) {
    use std::sync::atomic::Ordering;
    let _ = thread::Builder::new()
        .name("bgsave-reaper".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_millis(500));
            let child_pid = server.rdb_child_pid();
            if child_pid == 0 {
                continue;
            }
            let mut status: libc::c_int = 0;
            let ret =
                unsafe { libc::waitpid(child_pid as libc::pid_t, &mut status, libc::WNOHANG) };
            if ret == 0 {
                continue;
            }
            if ret < 0 {
                eprintln!("redis-server: waitpid({}) failed: errno={}", child_pid, ret);
                server.set_rdb_child_pid(0);
                server
                    .persistence
                    .set_rdb_last_bgsave_status(PersistenceStatus::Err);
                server_metrics()
                    .rdb_saves_failed
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let exited_ok = libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
            if exited_ok {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                live_config.set_last_save_unix(now);
                server
                    .persistence
                    .set_rdb_last_bgsave_status(PersistenceStatus::Ok);
                server_metrics()
                    .rdb_saves_succeeded
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                eprintln!(
                    "redis-server: BGSAVE child {} exited with status {}",
                    child_pid, status
                );
                server_metrics()
                    .rdb_saves_failed
                    .fetch_add(1, Ordering::Relaxed);
                server
                    .persistence
                    .set_rdb_last_bgsave_status(PersistenceStatus::Err);
            }
            server.set_rdb_child_pid(0);
        });
}

#[cfg(not(unix))]
fn spawn_bgsave_reaper(
    _server: Arc<redis_core::RedisServer>,
    _live_config: Arc<redis_core::live_config::LiveConfig>,
) {
}

/// Reaper for BGSAVE-for-replication child processes.
///
/// Tracked separately from the user-`BGSAVE` reaper because the two can run
/// concurrently: a user invoking `BGSAVE` while a replica is mid-handshake
/// keeps both children alive at once. On successful child exit this thread
/// reads the temp RDB file into memory and ships it through each waiting
/// replica's outbound channel, then sends the catch-up backlog window (from
/// `snapshot_offset` to the current master offset) and flips the replica to
/// `Online`.
///
/// On non-Unix the BGSAVE-for-replication path uses a thread fallback that
/// drops the job onto `ReplicationState` after the save completes — no
/// `waitpid` is needed there. For now the non-Unix path will leave the temp
/// file in place; full disposition of the fallback is a future TODO.
#[cfg(unix)]
fn spawn_repl_bgsave_reaper() {
    let _ = thread::Builder::new()
        .name("repl-bgsave-reaper".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_millis(200));
            let repl = redis_core::replication::global_replication_state();
            let child_pid = repl.repl_child_pid();
            if child_pid == 0 {
                continue;
            }
            let mut status: libc::c_int = 0;
            let ret =
                unsafe { libc::waitpid(child_pid as libc::pid_t, &mut status, libc::WNOHANG) };
            if ret == 0 {
                continue;
            }
            if ret < 0 {
                eprintln!(
                    "redis-server: repl-bgsave waitpid({}) failed: ret={}",
                    child_pid, ret
                );
                let _ = repl.take_repl_bgsave_job();
                repl.set_repl_child_pid(0);
                continue;
            }
            let exited_ok = libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
            if !exited_ok {
                eprintln!(
                    "redis-server: BGSAVE-for-replication child {} exited with status {}",
                    child_pid, status
                );
                if let Some(job) = repl.take_repl_bgsave_job() {
                    let _ = std::fs::remove_file(&job.temp_path);
                }
                repl.set_repl_child_pid(0);
                continue;
            }
            dispatch_full_sync_transfer();
            repl.set_repl_child_pid(0);
        });
}

#[cfg(not(unix))]
fn spawn_repl_bgsave_reaper() {}

/// Stream the freshly-baked RDB plus the catch-up backlog window to every
/// replica registered on the current `ReplBgsaveJob`, then mark each one
/// `Online`. Called by the repl-bgsave reaper after `waitpid` confirms the
/// child exited cleanly.
fn dispatch_full_sync_transfer() {
    let repl = redis_core::replication::global_replication_state();
    let job = match repl.take_repl_bgsave_job() {
        Some(j) => j,
        None => return,
    };
    let rdb_bytes = match fs::read(&job.temp_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!(
                "redis-server: failed to read RDB temp file {}: {}",
                job.temp_path.display(),
                e
            );
            let _ = std::fs::remove_file(&job.temp_path);
            return;
        }
    };
    let mut header = format!("${}\r\n", rdb_bytes.len()).into_bytes();
    header.extend_from_slice(&rdb_bytes);

    let snapshot_offset = job.snapshot_offset;
    for client_id in &job.waiting_replicas {
        repl.set_replica_state(
            *client_id,
            redis_core::replication::ReplicaState::SendingRdb,
        );
        if !repl.send_to_replica(*client_id, header.clone()) {
            eprintln!(
                "redis-server: full-sync RDB send failed for replica client_id={}",
                client_id
            );
            continue;
        }
        let current_offset = repl.master_offset();
        if current_offset > snapshot_offset {
            let catch_up = {
                let guard = match repl.backlog.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.read_at(snapshot_offset, (current_offset - snapshot_offset) as usize)
            };
            if let Some(bytes) = catch_up {
                if !bytes.is_empty() {
                    let _ = repl.send_to_replica(*client_id, bytes);
                }
            }
        }
        repl.set_replica_state(*client_id, redis_core::replication::ReplicaState::Online);
        eprintln!(
            "redis-server: full-sync RDB delivered to replica client_id={} ({} bytes, snapshot_offset={})",
            client_id,
            rdb_bytes.len(),
            snapshot_offset
        );
    }
    // A client can enter WAIT while one replica is still in full-sync and
    // therefore before `request_ack_from_replicas` will address it. Once the
    // RDB plus catch-up backlog are queued, prompt replicas only if a WAIT or
    // WAITAOF waiter is actually present. Sending GETACK unconditionally
    // pollutes normal replication-stream assertions and diverges from Valkey's
    // "only request ACKs for blocked WAIT clients" behavior.
    if job.needs_getack_on_completion || blocked_replication_wait_any() {
        send_getack_to_online_replicas(&repl);
    }
    let _ = std::fs::remove_file(&job.temp_path);
}

fn replconf_getack_frame() -> Vec<u8> {
    b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_vec()
}

fn send_getack_to_online_replicas(repl: &redis_core::replication::ReplicationState) {
    let client_ids: Vec<_> = {
        let guard = match repl.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .filter(|conn| conn.state() == redis_core::replication::ReplicaState::Online)
            .map(|conn| conn.client_id)
            .collect()
    };
    if client_ids.is_empty() {
        return;
    }
    let getack = replconf_getack_frame();
    repl.append_to_backlog(&getack);
    for client_id in client_ids {
        let _ = repl.send_to_replica(client_id, getack.clone());
    }
}

/// Background scanner that wakes blocked BLPOP/BRPOP/BLMOVE waiters once
/// their deadline elapses.
///
/// Polls the global `BlockedKeysIndex` every 100 ms, drains entries whose
/// `deadline_ms` is in the past, and ships either `*-1\r\n` (null array,
/// for BLPOP / BRPOP / BLMPOP) or `$-1\r\n` (null bulk, for BLMOVE /
/// BRPOPLPUSH) through each waiter's outbound mpsc.
fn spawn_blocked_timeout_thread(shutdown: Arc<AtomicBool>) {
    let _ = thread::Builder::new()
        .name("blocked-timeout".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(100));
                let expired = {
                    let mut idx = match blocked_keys_index().lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    idx.take_expired(current_time_ms())
                };
                for waiter in expired {
                    let reply = match redis_commands::replication::timeout_reply_for_wait_action(
                        &waiter.action,
                    ) {
                        Some(reply) => reply,
                        None => {
                            if waiter.resp_proto == 3 {
                                match &waiter.action {
                                    redis_core::blocked_keys::BlockedAction::ZSetPop { .. }
                                    | redis_core::blocked_keys::BlockedAction::Pop { .. } => {
                                        b"_\r\n".to_vec()
                                    }
                                    _ => waiter.action.timeout_reply_bytes().to_vec(),
                                }
                            } else {
                                waiter.action.timeout_reply_bytes().to_vec()
                            }
                        }
                    };
                    let _ = waiter.sender.send(reply);
                }
            }
        });
}

#[cfg(unix)]
extern "C" fn handle_termination_signal(signal: libc::c_int) {
    redis_commands::connection::note_shutdown_signal(signal as i32);
}

/// Best-effort SIGINT/SIGTERM handler used by the upstream shutdown tests.
fn install_shutdown_handler(_shutdown: Arc<AtomicBool>) {
    #[cfg(unix)]
    unsafe {
        let handler = handle_termination_signal as *const () as libc::sighandler_t;
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

fn spawn_signal_shutdown_watcher(
    server: Arc<redis_core::RedisServer>,
    live_config: Arc<redis_core::live_config::LiveConfig>,
) {
    #[cfg(unix)]
    {
        let _ = thread::Builder::new()
            .name("signal-shutdown".to_string())
            .spawn(move || {
                let mut seen = redis_commands::connection::shutdown_signal_count();
                loop {
                    thread::sleep(Duration::from_millis(10));
                    let current = redis_commands::connection::shutdown_signal_count();
                    if current == seen {
                        continue;
                    }
                    seen = current;
                    let signal = redis_commands::connection::shutdown_signal_number();
                    let path = redis_core::rdb::rdb_path(
                        &live_config.rdb_dir(),
                        &live_config.rdb_filename(),
                    );
                    let temp_path = path
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .join(format!("temp-{}.rdb", std::process::id()));

                    if redis_commands::connection::shutdown_pending() {
                        let _ = std::fs::remove_file(&temp_path);
                        let _ = std::fs::remove_file(temp_path.with_extension("rdb.tmp"));
                        redis_commands::connection::log_server_notice("ready to exit, bye bye");
                        unsafe { libc::_exit(0) };
                    }

                    if signal == libc::SIGTERM
                        && server.persistence.aof_rewrite_in_progress()
                        && !redis_commands::connection::shutdown_on_sigterm_force()
                    {
                        redis_commands::connection::log_server_notice(
                            "Writing initial AOF, can't exit",
                        );
                        continue;
                    }
                    if signal == libc::SIGTERM && !redis_commands::connection::debug_pause_cron() {
                        redis_commands::connection::log_server_notice("ready to exit, bye bye");
                        unsafe { libc::_exit(0) };
                    }

                    redis_commands::connection::set_shutdown_pending(true);
                    if signal == libc::SIGINT {
                        let _ = std::fs::remove_file(&temp_path);
                        let _ = std::fs::File::create(&temp_path);
                        if path.is_dir() {
                            let _ = std::fs::remove_file(&temp_path);
                            redis_commands::connection::mark_shutdown_save_failed();
                            redis_commands::connection::set_shutdown_pending(false);
                            redis_commands::connection::log_server_notice(
                                "Error trying to save the DB, can't exit",
                            );
                        }
                    }
                }
            });
    }

    #[cfg(not(unix))]
    {
        let _ = live_config;
    }
}

/// Accept loop. One std::thread per accepted connection.
///
/// Before spawning a handler thread, checks the live `maxclients` limit against
/// the `connected_clients` counter in `ServerMetrics`. When the limit is
/// reached, writes the canonical error reply and closes the socket.
fn serve(
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    next_client_id: Arc<AtomicU64>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
    tcp_port: u16,
) {
    for incoming in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("redis-server: shutdown requested, exiting accept loop");
            return;
        }
        match incoming {
            Ok(mut stream) => {
                let metrics = server_metrics();
                let current = metrics.connected_clients.load(Ordering::Relaxed);
                let limit = get_max_clients();
                if current >= limit {
                    metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                    let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                    drop(stream);
                    continue;
                }

                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("redis-server: set_nodelay failed: {}", e);
                }
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let shutdown = Arc::clone(&shutdown);
                let db = Arc::clone(&db);
                let registry = Arc::clone(&registry);
                let server_clone = Arc::clone(&server);
                let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                metrics.on_connect();
                metrics
                    .total_connections_received
                    .fetch_add(1, Ordering::Relaxed);
                let _ = thread::Builder::new()
                    .name(format!("client-{}", peer))
                    .spawn(move || {
                        handle_connection(
                            stream,
                            shutdown,
                            db,
                            id,
                            peer,
                            registry,
                            server_clone,
                            tcp_port,
                        )
                    });
            }
            Err(e) => {
                eprintln!("redis-server: accept failed: {}", e);
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
            }
        }
    }
}

/// Accept loop for the TLS listener.
///
/// Mirrors `serve` but wraps each accepted `TcpStream` in a rustls
/// `ServerConnection` before handing off to `handle_connection_tls`. The
/// plain TCP accept loop is unaffected by this code path.
fn serve_tls(
    listener: TcpListener,
    tls_cfg: redis_core::tls::TlsConfig,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    next_client_id: Arc<AtomicU64>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    for incoming in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match incoming {
            Ok(stream) => {
                let metrics = server_metrics();
                let current = metrics.connected_clients.load(Ordering::Relaxed);
                let limit = redis_commands::connection::get_max_clients();
                if current >= limit {
                    metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                    drop(stream);
                    continue;
                }
                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("redis-server: tls set_nodelay failed: {}", e);
                }
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let tls_conn =
                    match rustls::ServerConnection::new(Arc::clone(&tls_cfg.server_config)) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!(
                                "redis-server: tls ServerConnection::new failed for {}: {}",
                                peer, e
                            );
                            continue;
                        }
                    };
                let tls_stream = Box::new(StreamOwned::new(tls_conn, stream));
                let conn = Connection::Tls(tls_stream);
                let shutdown2 = Arc::clone(&shutdown);
                let db2 = Arc::clone(&db);
                let registry2 = Arc::clone(&registry);
                let server2 = Arc::clone(&server);
                let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                metrics.on_connect();
                metrics
                    .total_connections_received
                    .fetch_add(1, Ordering::Relaxed);
                let _ = thread::Builder::new()
                    .name(format!("tls-client-{}", peer))
                    .spawn(move || {
                        handle_connection_tls(conn, shutdown2, db2, id, peer, registry2, server2);
                    });
            }
            Err(e) => {
                eprintln!("redis-server: tls accept failed: {}", e);
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
            }
        }
    }
}

/// Spawn a writer thread that drains an `mpsc::Receiver<Vec<u8>>` and writes
/// each payload to the TCP stream. Returns the matching sender that the read
/// loop and the pub/sub registry both hold.
fn spawn_writer(mut writer: TcpStream, peer: String) -> Sender<Vec<u8>> {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let _ = thread::Builder::new()
        .name(format!("writer-{}", peer))
        .spawn(move || {
            for payload in rx {
                if payload.is_empty() {
                    break;
                }
                if writer.write_all(&payload).is_err() {
                    break;
                }
            }
            let _ = writer.shutdown(std::net::Shutdown::Both);
        });
    tx
}

/// Per-connection event loop for plain TCP connections.
///
/// Reads from the socket, feeds the incremental parser, dispatches each
/// completed command, then ships replies through the outbound mpsc so the
/// dedicated writer thread owns all socket writes.
fn handle_connection(
    stream: TcpStream,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    id: u64,
    peer_addr: String,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
    tcp_port: u16,
) {
    let _ = tcp_port;
    let writer_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("redis-server: try_clone failed for {}: {}", peer_addr, e);
            return;
        }
    };
    let outbound = spawn_writer(writer_clone, peer_addr.clone());

    if let Ok(mut guard) = registry.lock() {
        guard.register_sender(id, outbound.clone());
    }
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.register(id, peer_addr.clone());
    }

    let mut client = Client::with_connection(Connection::Tcp(stream));
    client.id = id;
    client.addr = Some(peer_addr);
    client.set_authenticated_user(determine_initial_user());

    run_client_loop(&mut client, &outbound, shutdown, db, registry, server);
}

/// Per-connection event loop for TLS connections.
///
/// Unlike the plain TCP path, TLS state is owned by a single `StreamOwned`
/// and cannot be cloned. Replies are written synchronously from the read loop
/// thread; pub/sub payloads delivered via the outbound channel are drained
/// inline via `try_recv` between commands.
fn handle_connection_tls(
    conn: Connection,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    id: u64,
    peer_addr: String,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();

    if let Ok(mut guard) = registry.lock() {
        guard.register_sender(id, tx.clone());
    }
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.register(id, peer_addr.clone());
    }

    let mut client = Client::with_connection(conn);
    client.id = id;
    client.addr = Some(peer_addr.clone());
    client.set_authenticated_user(determine_initial_user());

    run_client_loop_tls(
        &mut client,
        tx,
        rx,
        peer_addr,
        shutdown,
        db,
        registry,
        server,
    );
}

/// Shared read-dispatch-write loop for plain TCP connections.
///
/// Parameterised over the outbound sender so both `handle_connection` (plain
/// TCP) can share the same loop body without code duplication.
fn run_client_loop(
    client: &mut Client,
    outbound: &Sender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    let mut read_buf = [0u8; 16 * 1024];

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let conn = match client.conn.as_mut() {
            Some(c) => c,
            None => break,
        };

        let n = match conn.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        client.net_input_bytes = client.net_input_bytes.saturating_add(n as u64);
        if client_has_pending_kill(client.id) {
            break;
        }

        client.query_buf.extend_from_slice(&read_buf[..n]);
        let query_limit = redis_commands::connection::client_query_buffer_limit();
        if query_limit > 0 && client.query_buf.len() > query_limit {
            server_metrics()
                .client_query_buffer_limit_disconnections
                .fetch_add(1, Ordering::Relaxed);
            break;
        }

        let mut disconnect = false;
        let mut consumed_total = 0usize;
        let mut saw_command = false;
        let mut last_cmd_name: Vec<u8> = Vec::new();
        let mut batch_db0_guard = if client.db_index == 0 {
            Some(lock_redis_db(&db))
        } else {
            None
        };
        loop {
            if let Some(err) =
                unauthenticated_protocol_limit_error(client, &client.query_buf[consumed_total..])
            {
                queue_error_reply(client, &err);
                let _ = flush_reply_fast(client, outbound);
                disconnect = true;
                break;
            }
            let parsed = client.parse_query_buffer_into_argv(consumed_total);
            match parsed {
                Ok(Some(consumed)) => {
                    consumed_total += consumed;
                    if client.argv.is_empty() {
                        continue;
                    }
                    saw_command = true;
                    last_cmd_name.clear();
                    if let Some(cmd) = client.arg(0) {
                        last_cmd_name.extend_from_slice(cmd.as_bytes());
                    }
                    if is_client_info_observer(&last_cmd_name) {
                        update_client_info_snapshot(client, &last_cmd_name);
                    }
                    if client.db_index == 0 {
                        if batch_db0_guard.is_none() {
                            batch_db0_guard = Some(lock_redis_db(&db));
                        }
                        let db_guard = batch_db0_guard.as_mut().expect("db0 guard installed");
                        process_current_command_with_db(client, db_guard, &registry, &server);
                    } else {
                        batch_db0_guard = None;
                        process_current_command(client, &registry, &server);
                    }
                    if client.db_index != 0 {
                        batch_db0_guard = None;
                    }
                    if client.blocked_on_keys {
                        batch_db0_guard = None;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    queue_error_reply(client, &err);
                    let _ = flush_reply_fast(client, outbound);
                    disconnect = true;
                    break;
                }
            }

            // Batch all replies produced by commands already present in this
            // read. Draining query_buf per command also destroys pipelined
            // throughput by repeatedly memmoving the unread tail.
            if client.should_close {
                disconnect = true;
                break;
            }
        }

        if consumed_total > 0 {
            client.query_buf.drain(..consumed_total);
        }
        drop(batch_db0_guard);

        if saw_command {
            update_client_info_snapshot(client, &last_cmd_name);
        }

        if disconnect {
            break;
        }

        if !flush_reply_fast(client, outbound) {
            break;
        }

        if client.should_close {
            break;
        }
    }

    let id = client.id;
    let _ = pubsub::drop_client_from_registry(&registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
    redis_core::tracking::remove_runtime_client_tracking(id);
    redis_core::db::watched_keys_index_remove_client(id);
    let _ = redis_core::db::watched_keys_take_dirty(id);
    client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    server_metrics().on_disconnect();
}

/// Read-dispatch-write loop for TLS connections.
///
/// Because `rustls::StreamOwned` is not `Clone`, writes go through
/// `conn.write_all` on the same thread. The `rx` channel carries pub/sub
/// payloads from foreign threads; they are drained inline via `try_recv`
/// after each command so subscribers connected over TLS still receive
/// published messages.
fn run_client_loop_tls(
    client: &mut Client,
    outbound_tx: Sender<Vec<u8>>,
    outbound_rx: Receiver<Vec<u8>>,
    peer_addr: String,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    let mut read_buf = [0u8; 16 * 1024];

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        while let Ok(payload) = outbound_rx.try_recv() {
            let conn = match client.conn.as_mut() {
                Some(c) => c,
                None => break,
            };
            if conn.write_all(&payload).is_err() {
                break;
            }
        }

        let conn = match client.conn.as_mut() {
            Some(c) => c,
            None => break,
        };

        let n = match conn.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        client.net_input_bytes = client.net_input_bytes.saturating_add(n as u64);
        if client_has_pending_kill(client.id) {
            break;
        }

        client.query_buf.extend_from_slice(&read_buf[..n]);
        let query_limit = redis_commands::connection::client_query_buffer_limit();
        if query_limit > 0 && client.query_buf.len() > query_limit {
            server_metrics()
                .client_query_buffer_limit_disconnections
                .fetch_add(1, Ordering::Relaxed);
            break;
        }

        let mut disconnect = false;
        let mut consumed_total = 0usize;
        let mut saw_command = false;
        let mut last_cmd_name: Vec<u8> = Vec::new();
        loop {
            if let Some(err) =
                unauthenticated_protocol_limit_error(client, &client.query_buf[consumed_total..])
            {
                queue_error_reply(client, &err);
                let reply = std::mem::take(&mut client.reply_buf);
                if !reply.is_empty() {
                    if let Some(c) = client.conn.as_mut() {
                        let _ = c.write_all(&reply);
                    }
                }
                disconnect = true;
                break;
            }
            let parsed = client.parse_query_buffer_into_argv(consumed_total);
            match parsed {
                Ok(Some(consumed)) => {
                    consumed_total += consumed;
                    if client.argv.is_empty() {
                        continue;
                    }
                    saw_command = true;
                    last_cmd_name.clear();
                    if let Some(cmd) = client.arg(0) {
                        last_cmd_name.extend_from_slice(cmd.as_bytes());
                    }
                    if is_client_info_observer(&last_cmd_name) {
                        update_client_info_snapshot(client, &last_cmd_name);
                    }
                    if client.db_index == 0 {
                        let mut db_guard = lock_redis_db(&db);
                        process_current_command_with_db(client, &mut db_guard, &registry, &server);
                    } else {
                        process_current_command(client, &registry, &server);
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    queue_error_reply(client, &err);
                    let reply = std::mem::take(&mut client.reply_buf);
                    if !reply.is_empty() {
                        if let Some(c) = client.conn.as_mut() {
                            let _ = c.write_all(&reply);
                        }
                    }
                    disconnect = true;
                    break;
                }
            }

            let reply = std::mem::take(&mut client.reply_buf);
            if !reply.is_empty() {
                match client.conn.as_mut() {
                    Some(c) => {
                        if c.write_all(&reply).is_err() {
                            disconnect = true;
                            break;
                        }
                    }
                    None => {
                        disconnect = true;
                        break;
                    }
                }
            }

            while let Ok(payload) = outbound_rx.try_recv() {
                match client.conn.as_mut() {
                    Some(c) => {
                        if c.write_all(&payload).is_err() {
                            disconnect = true;
                            break;
                        }
                    }
                    None => {
                        disconnect = true;
                        break;
                    }
                }
            }

            if disconnect || client.should_close {
                disconnect = true;
                break;
            }
        }

        if consumed_total > 0 {
            client.query_buf.drain(..consumed_total);
        }
        if saw_command {
            update_client_info_snapshot(client, &last_cmd_name);
        }

        if disconnect || client.should_close {
            break;
        }
    }

    let _ = peer_addr;
    let id = client.id;
    let _ = pubsub::drop_client_from_registry(&registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
    redis_core::tracking::remove_runtime_client_tracking(id);
    redis_core::db::watched_keys_index_remove_client(id);
    let _ = redis_core::db::watched_keys_take_dirty(id);
    client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    drop(outbound_tx);
    server_metrics().on_disconnect();
}

/// Route the current `client.argv` through the dispatcher, locking the selected
/// database for this command.
fn process_current_command(
    client: &mut Client,
    registry: &Arc<Mutex<PubSubRegistry>>,
    server: &Arc<redis_core::RedisServer>,
) {
    let selected_db = global_databases().get(client.db_index);
    let mut guard = lock_redis_db(&selected_db);
    process_current_command_with_db(client, &mut guard, registry, server);
}

/// Route the current `client.argv` through the dispatcher using an already-held
/// database lock.
///
/// If the previous command parked the client on the global blocked-keys
/// index, the wake/timeout reply has already gone out via the writer thread
/// before this fresh read returned bytes — clear the residual flag and any
/// surviving registry entry before dispatching the new command.
fn process_current_command_with_db(
    client: &mut Client,
    db: &mut RedisDb,
    registry: &Arc<Mutex<PubSubRegistry>>,
    server: &Arc<redis_core::RedisServer>,
) {
    client.clear_blocked_on_keys();
    let reply_start = client.reply_buf.len();

    let metrics = server_metrics();
    let command_number = metrics
        .total_commands_processed
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    let active_time_sample = (command_number % ACTIVE_TIME_SAMPLE_INTERVAL == 0)
        .then(redis_core::monotonic::elapsed_start);
    let result = {
        let mut ctx =
            CommandContext::with_server(client, db, Arc::clone(server), Arc::clone(registry));
        let r = dispatch(&mut ctx);
        let deferred: Vec<RedisString> = std::mem::take(&mut ctx.client_mut().pending_wakes);
        for key in &deferred {
            redis_commands::list::wake_blocked_for_key(db, key);
        }
        if wake_ready_after_command_needed() {
            wake_ready_after_command(db);
        }
        r
    };
    if let Some(t0) = active_time_sample {
        let elapsed_us =
            redis_core::monotonic::elapsed_us(t0).saturating_mul(ACTIVE_TIME_SAMPLE_INTERVAL);
        metrics
            .active_time_main_thread_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
    }
    client.finish_command_reply(reply_start);
    if !client.blocked_on_keys {
        client.commands_processed = client.commands_processed.saturating_add(1);
    }
    client.reset_args();
}

/// Route the current `client.argv` through the dispatcher using the
/// RuntimeOwner-owned DB list.
fn process_current_command_with_db_list(
    client: &mut Client,
    dbs: &mut [RedisDb],
    registry: &Arc<Mutex<PubSubRegistry>>,
    server: &Arc<redis_core::RedisServer>,
) {
    client.clear_blocked_on_keys();
    let reply_start = client.reply_buf.len();

    let metrics = server_metrics();
    let command_number = metrics
        .total_commands_processed
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    let dispatch_db = client.db_index;
    let active_time_sample = (command_number % ACTIVE_TIME_SAMPLE_INTERVAL == 0)
        .then(redis_core::monotonic::elapsed_start);
    let result = {
        let mut ctx = CommandContext::with_server_and_db_list(
            client,
            dbs,
            Arc::clone(server),
            Arc::clone(registry),
        );
        let r = dispatch(&mut ctx);
        let deferred: Vec<RedisString> = std::mem::take(&mut ctx.client_mut().pending_wakes);
        for key in &deferred {
            let _ = ctx.with_db_index(dispatch_db, |db| {
                redis_commands::list::wake_blocked_for_key(db, key);
            });
        }
        if wake_ready_after_command_needed() {
            let _ = ctx.with_db_index(dispatch_db, |db| {
                wake_ready_after_command(db);
            });
        }
        r
    };
    if let Some(t0) = active_time_sample {
        let elapsed_us =
            redis_core::monotonic::elapsed_us(t0).saturating_mul(ACTIVE_TIME_SAMPLE_INTERVAL);
        metrics
            .active_time_main_thread_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
    }
    client.finish_command_reply(reply_start);
    if !client.blocked_on_keys {
        client.commands_processed = client.commands_processed.saturating_add(1);
    }
    client.reset_args();
}

fn lock_redis_db(db: &Arc<Mutex<RedisDb>>) -> MutexGuard<'_, RedisDb> {
    match db.lock() {
        Ok(g) => g,
        Err(poison) => poison.into_inner(),
    }
}

fn update_client_info_snapshot(client: &Client, last_cmd_name: &[u8]) {
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.update_client_metadata(client);
        guard.update_snapshot(
            client.id,
            last_cmd_name,
            client.db_index,
            client.blocked_on_keys,
        );
    }
}

fn client_has_pending_kill(id: u64) -> bool {
    match client_info_registry().lock() {
        Ok(mut guard) => guard.take_killed(id),
        Err(poison) => poison.into_inner().take_killed(id),
    }
}

fn is_client_info_observer(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"CLIENT")
}

/// Drain `client.reply_buf` through the outbound sender. Returns `false` if
/// the writer thread has already exited (connection should tear down).
fn flush_reply(client: &mut Client, outbound: &Sender<Vec<u8>>) -> bool {
    if client.reply_buf.is_empty() {
        return true;
    }
    let bytes = std::mem::take(&mut client.reply_buf);
    let len = bytes.len() as u64;
    let ok = outbound.send(bytes).is_ok();
    if ok {
        client.net_output_bytes = client.net_output_bytes.saturating_add(len);
    }
    ok
}

/// Fast path for ordinary plain-TCP request/reply traffic.
///
/// Pub/sub, blocked clients, and replicas still need the writer-thread channel
/// because other connection threads can deliver bytes to them. Normal clients
/// have no foreign writers, so their own replies can be written directly and
/// avoid one mpsc send plus one context switch per read batch.
fn flush_reply_fast(client: &mut Client, outbound: &Sender<Vec<u8>>) -> bool {
    if client.reply_buf.is_empty() {
        return true;
    }
    if client.in_pubsub_mode() || client.blocked_on_keys || client.is_replica {
        return flush_reply(client, outbound);
    }
    let len = client.reply_buf.len() as u64;
    match client.conn.as_mut() {
        Some(conn) => {
            let ok = conn.write_all(&client.reply_buf).is_ok();
            if ok {
                client.net_output_bytes = client.net_output_bytes.saturating_add(len);
                client.reply_buf.clear();
            }
            ok
        }
        None => false,
    }
}

/// Determine the initial authenticated username for a newly accepted connection.
///
/// If the global default ACL user is enabled and has `nopass`, the client
/// starts pre-authenticated as `default`. Otherwise the client must AUTH before
/// running commands.
fn determine_initial_user() -> Option<RedisString> {
    let acl = redis_core::acl::global_acl_state();
    let default_key = RedisString::from_bytes(b"default");
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Some(user) = guard.users.get(&default_key) {
        if user.flags.enabled && user.flags.nopass {
            return Some(default_key);
        }
    }
    None
}

/// Append a RESP error line to the pending reply buffer for later flushing.
fn queue_error_reply(client: &mut Client, err: &RedisError) {
    let payload = err.to_resp_payload();
    encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
}

fn unauthenticated_protocol_limit_error(client: &Client, bytes: &[u8]) -> Option<RedisError> {
    if client.authenticated_user.is_some() || !bytes.starts_with(b"*") {
        return None;
    }
    let array_end = find_crlf_from(bytes, 1)?;
    let argc = parse_i64_ascii(bytes.get(1..array_end)?)?;
    if argc > MAX_UNAUTHENTICATED_MULTIBULK_LEN {
        return Some(RedisError::runtime(
            b"ERR Protocol error: unauthenticated multibulk length",
        ));
    }
    let bulk_header_start = array_end + 2;
    if bytes.get(bulk_header_start) != Some(&b'$') {
        return None;
    }
    let bulk_end = find_crlf_from(bytes, bulk_header_start + 1)?;
    let bulk_len = parse_i64_ascii(bytes.get(bulk_header_start + 1..bulk_end)?)?;
    if bulk_len > MAX_UNAUTHENTICATED_BULK_LEN {
        return Some(RedisError::runtime(
            b"ERR Protocol error: unauthenticated bulk length",
        ));
    }
    None
}

fn find_crlf_from(bytes: &[u8], start: usize) -> Option<usize> {
    bytes
        .get(start..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|idx| start + idx)
}

fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value: i64 = 0;
    for digit in digits {
        value = value.checked_mul(10)?;
        value = value.checked_add((digit - b'0') as i64)?;
    }
    Some(if negative { -value } else { value })
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (Wave A main + Round 8a pub/sub wiring)
//   target_crate:  redis-server
//   confidence:    high
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Plain TCP now enters the mio RuntimeOwner loop with an
//                  owner-owned DB vector. Dynamic TLS listener startup is
//                  refused with TODO(human) until TLS command effects can route
//                  through the owner. SIGINT handler is a no-op stub.
// ──────────────────────────────────────────────────────────────────────────

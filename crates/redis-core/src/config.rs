//! Configuration file parsing and `CONFIG GET/SET/REWRITE/RESETSTAT/HELP` commands.
//!
//! Port of `src/config.c` (3708 lines) + `src/config.h` (platform detection macros).
//!
//! # Architecture note (Phase A)
//!
//! The C implementation stores raw pointers (`int *config`, `char **config`, etc.)
//! directly into fields of the global `redisServer` struct and mutates them through
//! those pointers.  Rust's aliasing rules forbid storing arbitrary interior `&mut`
//! pointers into a struct we also hold other references to.  The field-accessor
//! strategy is marked `TODO(architect)` throughout; Phase B will resolve it, likely
//! via an accessor-function closure stored in each `StandardConfig` entry, or by
//! dispatching through a `ConfigField` enum that maps config names to
//! `RedisServer` fields.
//!
//! The C `standardConfig::interface` vtable (function pointers for init/set/get/
//! rewrite/apply) is modelled as a `ConfigInterface` trait object in Phase A.
//! See §"Type interfaces" below.

// ── imports ──────────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::path::Path;

// Canonical types from peer crates (type-vocabulary rule: no redefinition here)
use redis_types::error::RedisError;
use redis_types::string::RedisString;

// TODO(architect): need dependency edge redis-core → redis-types (already in pilot?)
// TODO(architect): CommandContext lives in crates/redis-core/src/command_context.rs;
//   verify it is accessible from this module.

// ── Config-flag constants ─────────────────────────────────────────────────────
// C: server.h — exact bit values TBD in Phase B; assigned consecutive powers-of-2
// here so the bitwise logic compiles.

/// Config may be changed at runtime via CONFIG SET.
pub const MODIFIABLE_CONFIG: u32 = 1 << 0;
/// Config is fixed at startup; CONFIG SET will reject it.
pub const IMMUTABLE_CONFIG: u32 = 1 << 1;
/// CONFIG SET accepts multiple space-separated arguments.
pub const MULTI_ARG_CONFIG: u32 = 1 << 2;
/// Config belongs to a loaded module; uses module callback path.
pub const MODULE_CONFIG: u32 = 1 << 3;
/// Config value is security-sensitive; redact from logs / commandlog.
pub const SENSITIVE_CONFIG: u32 = 1 << 4;
/// Config requires `enable-protected-configs yes` before it can be changed.
pub const PROTECTED_CONFIG: u32 = 1 << 5;
/// Config is not returned in glob CONFIG GET patterns; requires exact match.
pub const HIDDEN_CONFIG: u32 = 1 << 6;
/// Config change must be treated as occurred even when value did not change
/// (forces apply() to fire).
pub const VOLATILE_CONFIG: u32 = 1 << 7;
/// Include in `DEBUG QUICKLIST-PACKED-THRESHOLD` / debug config dump.
pub const DEBUG_CONFIG: u32 = 1 << 8;
/// Cannot be set via CONFIG SET while the server is loading a dataset.
pub const DENY_LOADING_CONFIG: u32 = 1 << 9;
/// This entry is the alias copy of another config; points back to primary.
pub const ALIAS_CONFIG: u32 = 1 << 10;

// ── Numeric-config sub-flags ──────────────────────────────────────────────────
// C: config.c (local to numericConfigData.flags)

pub const INTEGER_CONFIG: u32 = 0;
pub const MEMORY_CONFIG: u32 = 1 << 0;
pub const PERCENT_CONFIG: u32 = 1 << 1;
pub const OCTAL_CONFIG: u32 = 1 << 2;
pub const UNSIGNED_CONFIG: u32 = 1 << 3;
pub const SIGNED_MEMORY_CONFIG: u32 = 1 << 4;

// ── Platform / OS constants (from config.h) ───────────────────────────────────
// config.h is almost entirely C preprocessor guards for platform detection.
// The functional constants we need at runtime:

pub const IO_THREADS_MAX_NUM: usize = 256;

// PORT NOTE: config.h platform-detect macros (HAVE_EPOLL, HAVE_KQUEUE, etc.) are
// replaced by Rust's cfg() attributes when actually needed.  No constants emitted
// for those here.

// ── configEnum ───────────────────────────────────────────────────────────────
// C: configEnum in server.h / used throughout config.c

/// A single (name, value) entry in an enum-typed config option.
/// Names are static byte strings because config option names are always
/// compile-time ASCII strings.
#[derive(Debug, Clone, Copy)]
pub struct ConfigEnumEntry {
    pub name: &'static [u8],
    pub val: i32,
}

// ── configType ───────────────────────────────────────────────────────────────
// C: configType enum in server.h

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Bool,
    String,
    Sds,
    Enum,
    Numeric,
    Special,
}

// ── NumericType ───────────────────────────────────────────────────────────────
// C: numericType enum in config.c

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericType {
    Int,
    Uint,
    Long,
    Ulong,
    LongLong,
    UlongLong,
    SizeT,
    SsizeT,
    OffT,
    TimeT,
}

// ── ProtectedAction (enable-protected-configs / enable-debug-command) ─────────
// C: PROTECTED_ACTION_ALLOWED_* constants in server.h

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectedAction {
    No,
    Yes,
    Local,
}

// ── clientBufferLimitsConfig ──────────────────────────────────────────────────
// TODO(port): canonical owner may be crates/redis-core/src/client.rs;
// duplicating here temporarily for Phase A.  Phase B must merge.

#[derive(Debug, Clone, Copy)]
pub struct ClientBufferLimitsConfig {
    pub hard_limit_bytes: u64,
    pub soft_limit_bytes: u64,
    pub soft_limit_seconds: i64,
}

pub const CLIENT_TYPE_OBUF_COUNT: usize = 3;

pub const CLIENT_BUFFER_LIMITS_DEFAULTS: [ClientBufferLimitsConfig; CLIENT_TYPE_OBUF_COUNT] = [
    ClientBufferLimitsConfig { hard_limit_bytes: 0, soft_limit_bytes: 0, soft_limit_seconds: 0 },
    ClientBufferLimitsConfig {
        hard_limit_bytes: 256 * 1024 * 1024,
        soft_limit_bytes: 64 * 1024 * 1024,
        soft_limit_seconds: 60,
    },
    ClientBufferLimitsConfig {
        hard_limit_bytes: 32 * 1024 * 1024,
        soft_limit_bytes: 8 * 1024 * 1024,
        soft_limit_seconds: 60,
    },
];

// ── OOM score defaults ────────────────────────────────────────────────────────
// C: configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT]

pub const CONFIG_OOM_COUNT: usize = 3;
pub const CONFIG_OOM_PRIMARY: usize = 0;
pub const CONFIG_OOM_REPLICA: usize = 1;
pub const CONFIG_OOM_BGCHILD: usize = 2;
pub const CONFIG_OOM_SCORE_ADJ_DEFAULTS: [i32; CONFIG_OOM_COUNT] = [0, 200, 800];

// ── deprecatedConfig ─────────────────────────────────────────────────────────
// C: deprecatedConfig struct, local to loadServerConfigFromString

struct DeprecatedConfig {
    name: &'static [u8],
    argc_min: usize,
    argc_max: usize,
}

const DEPRECATED_CONFIGS: &[DeprecatedConfig] = &[
    DeprecatedConfig { name: b"list-max-ziplist-entries", argc_min: 2, argc_max: 2 },
    DeprecatedConfig { name: b"list-max-ziplist-value", argc_min: 2, argc_max: 2 },
    DeprecatedConfig { name: b"lua-replicate-commands", argc_min: 2, argc_max: 2 },
    DeprecatedConfig { name: b"io-threads-do-reads", argc_min: 2, argc_max: 2 },
    DeprecatedConfig { name: b"dynamic-hz", argc_min: 2, argc_max: 2 },
    DeprecatedConfig { name: b"events-per-io-thread", argc_min: 2, argc_max: 2 },
];

// ── Enum tables ───────────────────────────────────────────────────────────────
// C: config.c lines 60-183 — global arrays of configEnum

pub static MAXMEMORY_POLICY_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"volatile-lru", val: 0 },    // TODO(port): MAXMEMORY_VOLATILE_LRU value
    ConfigEnumEntry { name: b"volatile-lfu", val: 1 },
    ConfigEnumEntry { name: b"volatile-random", val: 2 },
    ConfigEnumEntry { name: b"volatile-ttl", val: 3 },
    ConfigEnumEntry { name: b"allkeys-lru", val: 4 },
    ConfigEnumEntry { name: b"allkeys-lfu", val: 5 },
    ConfigEnumEntry { name: b"allkeys-random", val: 6 },
    ConfigEnumEntry { name: b"noeviction", val: 7 },      // TODO(port): actual enum values from server.h
];

pub static SYSLOG_FACILITY_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"user", val: 8 },     // LOG_USER
    ConfigEnumEntry { name: b"local0", val: 16 },  // LOG_LOCAL0 — TODO(port): actual syslog values
    ConfigEnumEntry { name: b"local1", val: 17 },
    ConfigEnumEntry { name: b"local2", val: 18 },
    ConfigEnumEntry { name: b"local3", val: 19 },
    ConfigEnumEntry { name: b"local4", val: 20 },
    ConfigEnumEntry { name: b"local5", val: 21 },
    ConfigEnumEntry { name: b"local6", val: 22 },
    ConfigEnumEntry { name: b"local7", val: 23 },
];

pub static LOGLEVEL_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"debug", val: 0 },
    ConfigEnumEntry { name: b"verbose", val: 1 },
    ConfigEnumEntry { name: b"notice", val: 2 },
    ConfigEnumEntry { name: b"warning", val: 3 },
    ConfigEnumEntry { name: b"nothing", val: 4 },  // TODO(port): LL_* values from server.h
];

pub static SUPERVISED_MODE_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"upstart", val: 1 },
    ConfigEnumEntry { name: b"systemd", val: 2 },
    ConfigEnumEntry { name: b"auto", val: 3 },
    ConfigEnumEntry { name: b"no", val: 0 },  // TODO(port): SUPERVISED_* values
];

pub static AOF_FSYNC_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"everysec", val: 1 },
    ConfigEnumEntry { name: b"always", val: 2 },
    ConfigEnumEntry { name: b"no", val: 0 },  // TODO(port): AOF_FSYNC_* values
];

pub static SHUTDOWN_ON_SIG_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"default", val: 0 },
    ConfigEnumEntry { name: b"save", val: 1 },
    ConfigEnumEntry { name: b"nosave", val: 2 },
    ConfigEnumEntry { name: b"now", val: 4 },
    ConfigEnumEntry { name: b"force", val: 8 },
    ConfigEnumEntry { name: b"safe", val: 16 },
    ConfigEnumEntry { name: b"failover", val: 32 }, // TODO(port): SHUTDOWN_* values
];

pub static REPL_DISKLESS_LOAD_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"disabled", val: 0 },
    ConfigEnumEntry { name: b"on-empty-db", val: 1 },
    ConfigEnumEntry { name: b"swapdb", val: 2 },
    ConfigEnumEntry { name: b"flush-before-load", val: 3 }, // TODO(port): REPL_DISKLESS_LOAD_* values
];

pub static TLS_AUTH_CLIENTS_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"no", val: 0 },
    ConfigEnumEntry { name: b"yes", val: 1 },
    ConfigEnumEntry { name: b"optional", val: 2 }, // TODO(port): TLS_CLIENT_AUTH_* values
];

pub static TLS_CLIENT_AUTH_USER_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"CN", val: 1 },
    ConfigEnumEntry { name: b"URI", val: 2 },
    ConfigEnumEntry { name: b"off", val: 0 }, // TODO(port): TLS_CLIENT_FIELD_* values
];

pub static OOM_SCORE_ADJ_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"no", val: 0 },
    ConfigEnumEntry { name: b"yes", val: 1 },
    ConfigEnumEntry { name: b"relative", val: 1 },
    ConfigEnumEntry { name: b"absolute", val: 2 }, // TODO(port): OOM_SCORE_* values
];

pub static ACL_PUBSUB_DEFAULT_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"allchannels", val: 1 }, // TODO(port): SELECTOR_FLAG_ALLCHANNELS
    ConfigEnumEntry { name: b"resetchannels", val: 0 },
];

pub static SANITIZE_DUMP_PAYLOAD_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"no", val: 0 },
    ConfigEnumEntry { name: b"yes", val: 1 },
    ConfigEnumEntry { name: b"clients", val: 2 }, // TODO(port): SANITIZE_DUMP_* values
];

pub static PROTECTED_ACTION_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"no", val: 0 },
    ConfigEnumEntry { name: b"yes", val: 1 },
    ConfigEnumEntry { name: b"local", val: 2 }, // TODO(port): PROTECTED_ACTION_ALLOWED_* values
];

pub static CLUSTER_PREFERRED_ENDPOINT_TYPE_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"ip", val: 0 },
    ConfigEnumEntry { name: b"hostname", val: 1 },
    ConfigEnumEntry { name: b"unknown-endpoint", val: 2 }, // TODO(port): CLUSTER_ENDPOINT_TYPE_* values
];

pub static CLUSTER_CONFIGFILE_SAVE_BEHAVIOR_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"sync", val: 0 },
    ConfigEnumEntry { name: b"best-effort", val: 1 }, // TODO(port): CLUSTER_CONFIGFILE_SAVE_BEHAVIOR_*
];

pub static PROPAGATION_ERROR_BEHAVIOR_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"ignore", val: 0 },
    ConfigEnumEntry { name: b"panic", val: 1 },
    ConfigEnumEntry { name: b"panic-on-replicas", val: 2 }, // TODO(port): PROPAGATION_ERR_BEHAVIOR_*
];

pub static LOG_FORMAT_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"legacy", val: 0 },
    ConfigEnumEntry { name: b"logfmt", val: 1 },
    ConfigEnumEntry { name: b"json", val: 2 }, // TODO(port): LOG_FORMAT_* values
];

pub static LOG_TIMESTAMP_FORMAT_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"legacy", val: 0 },
    ConfigEnumEntry { name: b"iso8601", val: 1 },
    ConfigEnumEntry { name: b"milliseconds", val: 2 }, // TODO(port): LOG_TIMESTAMP_* values
];

pub static RDB_VERSION_CHECK_ENUM: &[ConfigEnumEntry] = &[
    ConfigEnumEntry { name: b"strict", val: 0 },
    ConfigEnumEntry { name: b"relaxed", val: 1 }, // TODO(port): RDB_VERSION_CHECK_* values
];

// ── StandardConfig and supporting types ──────────────────────────────────────
// C: standardConfig struct + typeData union + typeInterface struct in config.c

/// Apply-function signature: called after CONFIG SET succeeds to propagate the
/// new value into live server state.
///
/// Returns `Ok(())` on success, `Err(RedisError)` on failure (which triggers
/// config rollback).
pub type ApplyFn = fn() -> Result<(), RedisError>;

/// Type-specific config data.
///
/// PORT NOTE: In C these are four separate structs packed into a union.
/// Represented here as an enum variant; accessor methods replace the C
/// macro machinery (GET_NUMERIC_TYPE etc.).
///
/// The `config` pointer fields in C (e.g., `int *config`) are replaced by
/// TODO(architect) stubs because Rust cannot safely store interior `&mut`
/// pointers to `RedisServer` fields.
#[derive(Debug)]
pub enum ConfigData {
    Bool(BoolConfigData),
    String(StringConfigData),
    Sds(SdsConfigData),
    Enum(EnumConfigData),
    Numeric(NumericConfigData),
    /// For SPECIAL_CONFIG entries whose set/get/rewrite are provided directly
    /// as function pointers in the interface.
    Special,
}

/// Boolean (yes/no) config data.
#[derive(Debug)]
pub struct BoolConfigData {
    pub default_value: bool,
    // TODO(architect): field accessor fn replacing C's `int *config` pointer.
    // In Phase B use fn(&mut RedisServer) -> &mut bool or a ConfigField enum variant.
}

/// `char*` string config data.
#[derive(Debug)]
pub struct StringConfigData {
    pub default_value: Option<&'static [u8]>,
    pub convert_empty_to_null: bool,
    // TODO(architect): field accessor fn replacing C's `char **config` pointer.
}

/// `sds` string config data.
#[derive(Debug)]
pub struct SdsConfigData {
    pub default_value: Option<&'static [u8]>,
    pub convert_empty_to_null: bool,
    // TODO(architect): field accessor fn replacing C's `sds *config` pointer.
}

/// Enum config data.
#[derive(Debug)]
pub struct EnumConfigData {
    pub enum_values: &'static [ConfigEnumEntry],
    pub default_value: i32,
    // TODO(architect): field accessor fn replacing C's `int *config` pointer.
}

/// Numeric config data.
#[derive(Debug)]
pub struct NumericConfigData {
    pub numeric_type: NumericType,
    pub flags: u32,
    pub lower_bound: i64,
    pub upper_bound: i64,
    pub default_value: i64,
    // TODO(architect): field accessor fn replacing C's union of typed pointers.
}

/// Per-type function pointers (C: typeInterface).
///
/// PORT NOTE: In C this is a struct of raw function pointers that all share the
/// same `standardConfig *` first argument.  In Rust we use separate `Option<fn>`
/// fields; the `config: &mut StandardConfig` argument is passed explicitly.
/// The `init` function is called once at startup; the rest are called on demand.
pub struct ConfigInterface {
    /// Called once at startup to initialise the server field to its default.
    pub init: Option<fn(config: &mut StandardConfig)>,
    /// Called on CONFIG SET and at startup from config file.
    /// Returns `1` (changed), `2` (no change), or `Err` on validation failure.
    pub set: fn(
        config: &mut StandardConfig,
        argv: &[RedisString],
        err: &mut Option<RedisString>,
    ) -> Result<i32, RedisError>,
    /// Optional post-set hook to propagate changes into live server state.
    pub apply: Option<ApplyFn>,
    /// Called on CONFIG GET.  Returns the current value as a `RedisString`.
    pub get: fn(config: &StandardConfig) -> RedisString,
    /// Called on CONFIG REWRITE.
    pub rewrite: Option<fn(config: &StandardConfig, name: &[u8], state: &mut RewriteConfigState)>,
}

impl std::fmt::Debug for ConfigInterface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigInterface")
            .field("init", &self.init.map(|_| "<fn>"))
            .field("set", &"<fn>")
            .field("apply", &self.apply.map(|_| "<fn>"))
            .field("get", &"<fn>")
            .field("rewrite", &self.rewrite.map(|_| "<fn>"))
            .finish()
    }
}

/// A single configuration entry (C: standardConfig).
#[derive(Debug)]
pub struct StandardConfig {
    pub name: &'static [u8],
    pub alias: Option<&'static [u8]>,
    pub flags: u32,
    pub interface: ConfigInterface,
    pub data: ConfigData,
    pub config_type: ConfigType,
    /// Module-private data pointer (C: void *privdata).
    /// TODO(architect): need concrete type once Module API is defined (Phase 10).
    pub privdata: Option<Box<dyn std::any::Any + Send + Sync>>,
}

// ── Config registry ───────────────────────────────────────────────────────────
// C: static dict *configs = NULL;
// Maps lowercased config names to their StandardConfig entries.

/// The runtime config registry.
///
/// PORT NOTE: In C this is a global `dict *configs`.  In Rust we use a
/// `HashMap` keyed by `RedisString` (byte string).  The registry is
/// populated by `init_config_values()` from the static `STATIC_CONFIGS`
/// table, and by `add_module_*_config()` for module-registered configs.
pub struct ConfigRegistry {
    configs: HashMap<RedisString, Box<StandardConfig>>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        ConfigRegistry { configs: HashMap::new() }
    }

    /// Lookup a config by (lowercase) name.
    pub fn lookup(&self, name: &[u8]) -> Option<&StandardConfig> {
        self.configs.get(name).map(|c| c.as_ref())
    }

    /// Lookup a config by (lowercase) name, mutably.
    pub fn lookup_mut(&mut self, name: &[u8]) -> Option<&mut StandardConfig> {
        self.configs.get_mut(name).map(|c| c.as_mut())
    }

    /// Register a config entry (fails silently if name already exists).
    ///
    /// Returns `true` on success, `false` if the name was already registered.
    pub fn register(&mut self, name: RedisString, config: StandardConfig) -> bool {
        if self.configs.contains_key(&name) {
            return false;
        }
        self.configs.insert(name, Box::new(config));
        true
    }

    /// Remove a config entry (for module unloading).
    pub fn remove(&mut self, name: &[u8]) {
        self.configs.remove(name);
    }

    pub fn len(&self) -> usize {
        self.configs.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RedisString, &StandardConfig)> {
        self.configs.iter().map(|(k, v)| (k, v.as_ref()))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&RedisString, &mut StandardConfig)> {
        self.configs.iter_mut().map(|(k, v)| (k, v.as_mut()))
    }
}

// ── RewriteConfigState ────────────────────────────────────────────────────────
// C: struct rewriteConfigState in config.c (lines 1064-1074)

/// State maintained while rewriting the config file.
///
/// PORT NOTE: The C version uses a `dict` keyed by SDS string for both
/// `option_to_line` and `rewritten`.  Here we use `HashMap<RedisString, …>`.
pub struct RewriteConfigState {
    /// Maps option name → list of line indices in `lines` where the option
    /// appears in the existing config file.
    pub option_to_line: HashMap<RedisString, Vec<usize>>,
    /// Set of already-processed option names (used to blank orphaned lines).
    pub rewritten: std::collections::HashSet<RedisString>,
    /// The current config file content as individual lines.
    pub lines: Vec<RedisString>,
    /// True if we need to append the `# Generated by CONFIG REWRITE` signature.
    pub needs_signature: bool,
    /// True forces all options to be written even if at defaults (for tests).
    pub force_write: bool,
}

const CONFIG_REWRITE_SIGNATURE: &[u8] = b"# Generated by CONFIG REWRITE";

impl RewriteConfigState {
    pub fn new() -> Self {
        RewriteConfigState {
            option_to_line: HashMap::new(),
            rewritten: std::collections::HashSet::new(),
            lines: Vec::new(),
            needs_signature: true,
            force_write: false,
        }
    }

    pub fn append_line(&mut self, line: RedisString) {
        self.lines.push(line);
    }

    pub fn add_line_number_to_option(&mut self, option: &[u8], linenum: usize) {
        self.option_to_line
            .entry(RedisString::from_bytes(option))
            .or_insert_with(Vec::new)
            .push(linenum);
    }

    pub fn mark_as_processed(&mut self, option: &[u8]) {
        self.rewritten.insert(RedisString::from_bytes(option));
    }
}

// ── Enum helper functions ─────────────────────────────────────────────────────
// C: configEnumGetValue / configEnumGetName (config.c lines 323-360)

/// Look up a config enum integer value from its string name(s).
///
/// If `bitflags` is true, multiple names may be provided and their values
/// are OR-combined.  Returns `None` if any name is unrecognised.
///
/// C: configEnumGetValue (config.c:323)
pub fn config_enum_get_value(
    ce: &[ConfigEnumEntry],
    argv: &[RedisString],
    bitflags: bool,
) -> Option<i32> {
    if argv.is_empty() || (!bitflags && argv.len() != 1) {
        return None;
    }
    let mut values: i32 = 0;
    for arg in argv {
        let mut matched = false;
        for entry in ce {
            if entry.name.eq_ignore_ascii_case(arg.as_bytes()) {
                values |= entry.val;
                matched = true;
            }
        }
        if !matched {
            return None;
        }
    }
    Some(values)
}

/// Produce the string representation of an enum config value.
///
/// If `bitflags` is true, may return multiple space-separated names.
/// Returns `"unknown"` if no match is found.
///
/// C: configEnumGetName (config.c:340)
pub fn config_enum_get_name(ce: &[ConfigEnumEntry], values: i32, bitflags: bool) -> RedisString {
    let mut unmatched = values;

    for entry in ce {
        if values == entry.val {
            return RedisString::from_bytes(entry.name);
        }
    }

    if bitflags {
        let mut parts: Vec<&[u8]> = Vec::new();
        for entry in ce {
            if entry.val != 0 && entry.val == unmatched & entry.val {
                parts.push(entry.name);
                unmatched &= !entry.val;
            }
        }
        if !parts.is_empty() && unmatched == 0 {
            let joined: Vec<u8> = parts.join(&b' ');
            return RedisString::from_bytes(&joined);
        }
    }

    RedisString::from_bytes(b"unknown")
}

/// Return the string name of the current maxmemory eviction policy.
///
/// C: evictPolicyToString (config.c:364)
pub fn evict_policy_to_string(maxmemory_policy: i32) -> &'static [u8] {
    for entry in MAXMEMORY_POLICY_ENUM {
        if maxmemory_policy == entry.val {
            return entry.name;
        }
    }
    // C: serverPanic("unknown eviction policy")
    // TODO(architect): is panic correct here?
    panic!("unknown eviction policy: {}", maxmemory_policy);
}

// ── yesnotoi ─────────────────────────────────────────────────────────────────
// C: yesnotoi (config.c:375)

/// Check whether a given directive name + argument count matches any deprecated
/// config entry (which should be silently ignored).
///
/// C: local deprecatedConfig table in loadServerConfigFromString (config.c:460)
fn is_deprecated_config(name: &[u8], argc: usize) -> bool {
    for dc in DEPRECATED_CONFIGS {
        if dc.name.eq_ignore_ascii_case(name) && argc >= dc.argc_min && argc <= dc.argc_max {
            return true;
        }
    }
    false
}

/// Parse a yes/no string.  Returns `Some(true/false)` or `None` if invalid.
pub fn yesnotoi(s: &[u8]) -> Option<bool> {
    if s.eq_ignore_ascii_case(b"yes") {
        Some(true)
    } else if s.eq_ignore_ascii_case(b"no") {
        Some(false)
    } else {
        None
    }
}

// ── save-params helpers ───────────────────────────────────────────────────────
// C: appendServerSaveParams / resetServerSaveParams (config.c:384-395)

/// A `save` parameter: after `seconds` seconds, trigger RDB save if at
/// least `changes` keys have been modified.
#[derive(Debug, Clone, Copy)]
pub struct SaveParam {
    pub seconds: i64,
    pub changes: i32,
}

// ── client output buffer limit helpers ───────────────────────────────────────
// C: updateClientOutputBufferLimit (config.c:400-452)

/// Parse and validate client-output-buffer-limit config args.
///
/// Expects args in groups of 4: `<class> <hard> <soft> <soft_seconds>`.
/// Fills `dest` on success.
///
/// C: updateClientOutputBufferLimit (config.c:400)
pub fn update_client_output_buffer_limit(
    args: &[RedisString],
    dest: &mut [ClientBufferLimitsConfig; CLIENT_TYPE_OBUF_COUNT],
) -> Result<(), RedisError> {
    if args.len() % 4 != 0 {
        return Err(RedisError::runtime(
            b"Wrong number of arguments in buffer limit configuration.",
        ));
    }

    let mut values = *dest;
    let mut changed = [false; CLIENT_TYPE_OBUF_COUNT];

    let mut i = 0;
    while i < args.len() {
        let class = get_client_type_by_name(args[i].as_bytes()).ok_or_else(|| {
            RedisError::runtime(b"Invalid client class specified in buffer limit configuration.")
        })?;
        // CLIENT_TYPE_PRIMARY cannot have output buffer limits configured
        if class == CLIENT_TYPE_PRIMARY_IDX {
            return Err(RedisError::runtime(
                b"Invalid client class specified in buffer limit configuration.",
            ));
        }

        let hard = mem_to_ull(args[i + 1].as_bytes()).map_err(|_| {
            RedisError::runtime(
                b"Error in hard, soft or soft_seconds setting in buffer limit configuration.",
            )
        })?;
        let soft = mem_to_ull(args[i + 2].as_bytes()).map_err(|_| {
            RedisError::runtime(
                b"Error in hard, soft or soft_seconds setting in buffer limit configuration.",
            )
        })?;
        let soft_seconds: i64 =
            parse_decimal_i64(args[i + 3].as_bytes()).map_err(|_| {
                RedisError::runtime(
                    b"Error in hard, soft or soft_seconds setting in buffer limit configuration.",
                )
            })?;
        if soft_seconds < 0 {
            return Err(RedisError::runtime(
                b"Error in hard, soft or soft_seconds setting in buffer limit configuration.",
            ));
        }

        values[class].hard_limit_bytes = hard;
        values[class].soft_limit_bytes = soft;
        values[class].soft_limit_seconds = soft_seconds;
        changed[class] = true;
        i += 4;
    }

    for j in 0..CLIENT_TYPE_OBUF_COUNT {
        if changed[j] {
            dest[j] = values[j];
        }
    }
    Ok(())
}

// ── load server config ────────────────────────────────────────────────────────
// C: loadServerConfigFromString / loadServerConfig (config.c:459-720)

/// Parse a config string line-by-line and apply each directive to `registry`.
///
/// C: loadServerConfigFromString (config.c:459)
///
/// TODO(port): This function needs access to the live server state
/// (`server.cluster_enabled`, `server.hz`, ACL loading, etc.) which requires
/// `&mut RedisServer`.  Signature will need updating in Phase B once
/// `RedisServer` is fleshed out.
pub fn load_server_config_from_string(
    config: &[u8],
    registry: &mut ConfigRegistry,
) -> Result<(), RedisError> {
    // C: reading_config_file = 1
    // (thread-local flag to distinguish file-parse from CONFIG SET context)
    READING_CONFIG_FILE.with(|f| *f.borrow_mut() = true);

    let lines = split_config_lines(config);

    for (linenum, raw_line) in lines.iter().enumerate() {
        let line = trim_whitespace(raw_line);
        if line.is_empty() || line[0] == b'#' {
            continue;
        }

        let argv = split_args(line).map_err(|_| {
            RedisError::runtime(
                format!("Unbalanced quotes in configuration line at line {}", linenum + 1)
                    .as_bytes(),
            )
        })?;

        if argv.is_empty() {
            continue;
        }

        let key = ascii_lowercase(&argv[0]);

        // Check deprecated configs and silently skip them
        if is_deprecated_config(&key, argv.len()) {
            continue;
        }

        // Standard config lookup
        if let Some(cfg) = registry.lookup_mut(&key) {
            let args_slice = &argv[1..];
            // For MULTI_ARG_CONFIG with a single arg, try splitting by spaces
            if (cfg.flags & MULTI_ARG_CONFIG != 0) && args_slice.len() == 1
                && !args_slice[0].is_empty()
            {
                let sub_args = split_args(&args_slice[0]).map_err(|_| {
                    RedisError::runtime(b"Error parsing multi-arg config value")
                })?;
                let rs_args: Vec<RedisString> =
                    sub_args.iter().map(|a| RedisString::from_bytes(a)).collect();
                let mut err_out: Option<RedisString> = None;
                (cfg.interface.set)(cfg, &rs_args, &mut err_out).map_err(|e| e)?;
            } else {
                if (cfg.flags & MULTI_ARG_CONFIG == 0) && argv.len() != 2 {
                    return Err(RedisError::runtime(b"wrong number of arguments"));
                }
                let rs_args: Vec<RedisString> =
                    args_slice.iter().map(|a| RedisString::from_bytes(a)).collect();
                let mut err_out: Option<RedisString> = None;
                (cfg.interface.set)(cfg, &rs_args, &mut err_out).map_err(|e| e)?;
            }
            continue;
        }

        // Special directives not in the config registry
        if key.eq_ignore_ascii_case(b"include") && argv.len() == 2 {
            // TODO(port): recursive include — requires I/O; pass through to
            // load_server_config in Phase B.
            load_server_config(&argv[1], false, None, registry)?;
        } else if key.eq_ignore_ascii_case(b"rename-command") && argv.len() == 3 {
            // TODO(port): command renaming requires access to server.commands hashtable.
            // Flag for Phase B.
        } else if key.eq_ignore_ascii_case(b"user") && argv.len() >= 2 {
            // TODO(port): ACL user declaration — requires ACLAppendUserForLoading.
        } else if key.eq_ignore_ascii_case(b"loadmodule") && argv.len() >= 2 {
            // TODO(port): module loading — Phase 10.
        } else if line.contains(&b'.') {
            // Module config: "module.param value"
            // TODO(port): queue in server.module_configs_queue for Phase 10.
        } else if key.eq_ignore_ascii_case(b"sentinel") {
            // TODO(port): sentinel mode — defer to Phase 9.
        } else {
            return Err(RedisError::runtime(
                format!("Bad directive or wrong number of arguments at line {}", linenum + 1)
                    .as_bytes(),
            ));
        }
    }

    READING_CONFIG_FILE.with(|f| *f.borrow_mut() = false);
    Ok(())
}

/// Load and apply config from a file path, optional stdin, and/or option string.
///
/// C: loadServerConfig (config.c:657)
pub fn load_server_config(
    filename: &[u8],
    config_from_stdin: bool,
    options: Option<&[u8]>,
    registry: &mut ConfigRegistry,
) -> Result<(), RedisError> {
    let mut config: Vec<u8> = Vec::new();

    if !filename.is_empty() {
        // TODO(port): glob expansion for wildcards in filename.
        // For Phase B use the `glob` crate.
        let path = std::path::Path::new(
            // SAFETY: filename is treated as a filesystem path; non-UTF8 paths
            // handled separately in Phase B.  TODO(port): use OsStr.
            std::str::from_utf8(filename).map_err(|_| {
                RedisError::runtime(b"config filename contains non-UTF8 bytes")
            })?,
        );
        let contents = std::fs::read(path).map_err(|e| {
            RedisError::runtime(
                format!("Fatal error, can't open config file: {}", e).as_bytes(),
            )
        })?;
        config.extend_from_slice(&contents);
    }

    if config_from_stdin {
        use std::io::Read;
        let mut stdin_buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut stdin_buf)
            .map_err(|e| RedisError::runtime(format!("stdin read error: {}", e).as_bytes()))?;
        config.extend_from_slice(&stdin_buf);
    }

    if let Some(opts) = options {
        config.push(b'\n');
        config.extend_from_slice(opts);
    }

    load_server_config_from_string(&config, registry)
}

// ── performInterfaceSet helper ────────────────────────────────────────────────
// C: performInterfaceSet (config.c:722)

/// Parse `value` and call config.interface.set().
///
/// For MULTI_ARG_CONFIG, `value` is space-split into multiple args first.
fn perform_interface_set(
    config: &mut StandardConfig,
    value: &RedisString,
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    if config.flags & MULTI_ARG_CONFIG != 0 {
        let parts = split_by_space(value.as_bytes());
        let rs_parts: Vec<RedisString> =
            parts.iter().map(|p| RedisString::from_bytes(p)).collect();
        (config.interface.set)(config, &rs_parts, err_out)
    } else {
        (config.interface.set)(config, std::slice::from_ref(value), err_out)
    }
}

// ── restoreBackupConfig ───────────────────────────────────────────────────────
// C: restoreBackupConfig (config.c:767)

/// Restore previously-backed-up config values after a failed CONFIG SET.
///
/// C: restoreBackupConfig (config.c:767)
fn restore_backup_config(
    set_configs: &mut [&mut StandardConfig],
    old_values: &[RedisString],
    apply_fns: &[ApplyFn],
) {
    let mut err_out: Option<RedisString> = None;
    for (i, cfg) in set_configs.iter_mut().enumerate() {
        if let Err(_) = perform_interface_set(cfg, &old_values[i], &mut err_out) {
            // C: serverLog(LL_WARNING, "Failed restoring failed CONFIG SET command ...")
            // TODO(port): proper logging once log infrastructure is wired up.
            eprintln!(
                "Failed restoring failed CONFIG SET command for '{}'",
                RedisString::from_bytes(cfg.name)
            );
        }
    }
    for apply in apply_fns {
        if let Err(_) = apply() {
            eprintln!("Failed applying restored failed CONFIG SET command");
        }
    }
}

// ── CONFIG SET command ────────────────────────────────────────────────────────
// C: configSetCommand (config.c:797)

/// `CONFIG SET field value [field value …]`
///
/// C: configSetCommand (config.c:797)
pub fn config_set_command(
    ctx: &mut redis_core::command_context::CommandContext,
    registry: &mut ConfigRegistry,
) -> Result<(), RedisError> {
    // C: if (c->argc & 1) { addReplyErrorObject(c, shared.syntaxerr); return; }
    let argc = ctx.argc();
    if argc & 1 != 0 {
        return Err(RedisError::syntax(b"CONFIG SET requires key-value pairs"));
    }
    let config_count = (argc - 2) / 2;

    let mut set_names: Vec<&[u8]> = Vec::with_capacity(config_count);
    let mut new_values: Vec<RedisString> = Vec::with_capacity(config_count);
    let mut apply_fn_set: Vec<ApplyFn> = Vec::new();
    let mut old_values: Vec<RedisString> = Vec::with_capacity(config_count);

    // Validate all args before touching anything
    for i in 0..config_count {
        let name_obj = ctx.arg(2 + i * 2)?;
        let val_obj = ctx.arg(2 + i * 2 + 1)?;

        let cfg = registry.lookup(name_obj.as_bytes()).ok_or_else(|| {
            RedisError::runtime(
                format!("Unknown option or number of arguments for CONFIG SET - '{}'",
                    RedisString::from_bytes(name_obj.as_bytes())
                ).as_bytes(),
            )
        })?;

        if cfg.flags & SENSITIVE_CONFIG != 0 {
            ctx.redact_arg(2 + i * 2 + 1);
        }

        if cfg.flags & IMMUTABLE_CONFIG != 0 {
            return Err(RedisError::runtime(b"can't set immutable config"));
        }
        // TODO(port): PROTECTED_CONFIG check requires `allowProtectedAction(server.enable_protected_configs, c)`.
        // TODO(port): DENY_LOADING_CONFIG check requires `server.loading`.
        // TODO(port): duplicate parameter detection.

        set_names.push(name_obj.as_bytes());
        new_values.push(RedisString::from_bytes(val_obj.as_bytes()));
    }

    // Back up old values
    for name in &set_names {
        let cfg = registry.lookup(name).ok_or_else(|| RedisError::runtime(b"internal error"))?;
        old_values.push((cfg.interface.get)(cfg));
    }

    // Apply new values
    for (i, name) in set_names.iter().enumerate() {
        let cfg = registry
            .lookup_mut(name)
            .ok_or_else(|| RedisError::runtime(b"internal error"))?;
        let mut err_out: Option<RedisString> = None;
        let res = perform_interface_set(cfg, &new_values[i], &mut err_out)?;
        if res == 1 {
            if let Some(apply) = cfg.interface.apply {
                if !apply_fn_set.contains(&(apply as usize as *const () as usize as fn() -> _)) {
                    // TODO(port): deduplication of apply fns by pointer identity
                    apply_fn_set.push(apply);
                }
            }
        }
    }

    // Run apply functions
    for apply in &apply_fn_set {
        apply().map_err(|e| {
            // TODO(port): restore backup on apply failure
            e
        })?;
    }

    // TODO(port): fire VALKEYMODULE_EVENT_CONFIG / moduleFireServerEvent

    ctx.reply_simple_string(b"OK")
}

// ── CONFIG GET command ────────────────────────────────────────────────────────
// C: configGetCommand (config.c:971)

/// `CONFIG GET pattern [pattern …]`
///
/// C: configGetCommand (config.c:971)
pub fn config_get_command(
    ctx: &mut redis_core::command_context::CommandContext,
    registry: &ConfigRegistry,
) -> Result<(), RedisError> {
    // Collect all matching configs
    let mut matches: HashMap<RedisString, &StandardConfig> = HashMap::new();

    for i in 0..(ctx.argc() - 2) {
        let name = ctx.arg(2 + i)?;
        let name_bytes = name.as_bytes();

        if !has_glob_chars(name_bytes) {
            // Direct lookup
            if let Some(cfg) = registry.lookup(name_bytes) {
                matches.insert(RedisString::from_bytes(name_bytes), cfg);
            }
        } else {
            // Glob match against all registered configs
            for (key, cfg) in registry.iter() {
                if cfg.flags & HIDDEN_CONFIG != 0 {
                    continue;
                }
                if string_match_len(name_bytes, key.as_bytes(), true) {
                    matches.entry(key.clone()).or_insert(cfg);
                }
            }
        }
    }

    // Sort by key for deterministic output
    let mut sorted: Vec<(RedisString, RedisString)> = matches
        .iter()
        .map(|(k, cfg)| (k.clone(), (cfg.interface.get)(cfg)))
        .collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

    ctx.reply_map_header(sorted.len())?;
    for (key, value) in sorted {
        ctx.reply_bulk(key.as_bytes())?;
        ctx.reply_bulk(value.as_bytes())?;
    }
    Ok(())
}

// ── Config REWRITE helpers ────────────────────────────────────────────────────
// C: rewriteConfigState helpers (config.c:1076-1684)

/// C: rewriteConfigReadOldFile (config.c:1128)
pub fn rewrite_config_read_old_file(path: &Path) -> Result<RewriteConfigState, RedisError> {
    let mut state = RewriteConfigState::new();

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(state),
        Err(e) => return Err(RedisError::runtime(format!("read error: {}", e).as_bytes())),
    };

    if data.is_empty() {
        return Ok(state);
    }

    let mut linenum: usize = 0;
    for raw in split_config_lines(&data) {
        let line = trim_whitespace(&raw);

        if line.is_empty() || line[0] == b'#' {
            if !state.needs_signature && line == CONFIG_REWRITE_SIGNATURE {
                state.needs_signature = false;
            }
            state.append_line(RedisString::from_bytes(line));
            linenum += 1;
            continue;
        }

        let argv = match split_args(line) {
            Ok(a) => a,
            Err(_) => {
                let commented = {
                    let mut v = b"# ??? ".to_vec();
                    v.extend_from_slice(line);
                    v
                };
                state.append_line(RedisString::from_bytes(&commented));
                linenum += 1;
                continue;
            }
        };

        if argv.is_empty() {
            state.append_line(RedisString::from_bytes(line));
            linenum += 1;
            continue;
        }

        let key = ascii_lowercase(&argv[0]);
        // TODO(port): alias resolution (if config is ALIAS_CONFIG, use primary name)
        state.append_line(RedisString::from_bytes(line));
        state.add_line_number_to_option(&key, linenum);
        linenum += 1;
    }

    Ok(state)
}

/// C: rewriteConfigRewriteLine (config.c:1246)
pub fn rewrite_config_rewrite_line(
    state: &mut RewriteConfigState,
    option: &[u8],
    line: RedisString,
    force: bool,
) -> bool {
    state.mark_as_processed(option);

    if let Some(line_list) = state.option_to_line.get_mut(option) {
        if !line_list.is_empty() {
            let linenum = line_list.remove(0);
            if line_list.is_empty() {
                state.option_to_line.remove(option);
            }
            state.lines[linenum] = line;
            return true;
        }
    }

    if force || state.force_write {
        if state.needs_signature {
            state.append_line(RedisString::from_bytes(CONFIG_REWRITE_SIGNATURE));
            state.needs_signature = false;
        }
        state.append_line(line);
        return true;
    }

    false
}

/// Format bytes as a human-readable memory string (gb/mb/kb/plain).
///
/// C: rewriteConfigFormatMemory (config.c:1283)
pub fn rewrite_config_format_memory(bytes: u64) -> Vec<u8> {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes != 0 && bytes % GB == 0 {
        format!("{}gb", bytes / GB).into_bytes()
    } else if bytes != 0 && bytes % MB == 0 {
        format!("{}mb", bytes / MB).into_bytes()
    } else if bytes != 0 && bytes % KB == 0 {
        format!("{}kb", bytes / KB).into_bytes()
    } else {
        format!("{}", bytes).into_bytes()
    }
}

/// C: rewriteConfigBytesOption (config.c:1299)
pub fn rewrite_config_bytes_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: u64,
    defvalue: u64,
) {
    let mem = rewrite_config_format_memory(value);
    let mut line: Vec<u8> = option.to_vec();
    line.push(b' ');
    line.extend_from_slice(&mem);
    let force = value != defvalue;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(&line), force);
}

/// C: rewriteConfigYesNoOption (config.c:1324)
pub fn rewrite_config_yes_no_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: bool,
    defvalue: bool,
) {
    let val_str = if value { b"yes" as &[u8] } else { b"no" };
    let mut line: Vec<u8> = option.to_vec();
    line.push(b' ');
    line.extend_from_slice(val_str);
    let force = value != defvalue;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(&line), force);
}

/// C: rewriteConfigNumericalOption (config.c:1377)
pub fn rewrite_config_numerical_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: i64,
    defvalue: i64,
) {
    let s = format!("{} {}", RedisString::from_bytes(option), value);
    let force = value != defvalue;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(s.as_bytes()), force);
}

/// C: rewriteConfigPercentOption (config.c:1313)
pub fn rewrite_config_percent_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: i64,
    defvalue: i64,
) {
    let s = format!("{} {}%", RedisString::from_bytes(option), value);
    let force = value != defvalue;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(s.as_bytes()), force);
}

/// C: rewriteConfigOctalOption (config.c:1388)
pub fn rewrite_config_octal_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: i64,
    defvalue: i64,
) {
    let s = format!("{} {:o}", RedisString::from_bytes(option), value);
    let force = value != defvalue;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(s.as_bytes()), force);
}

/// C: rewriteConfigStringOption (config.c:1333)
pub fn rewrite_config_string_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: Option<&[u8]>,
    defvalue: Option<&[u8]>,
) {
    let Some(val) = value else {
        state.mark_as_processed(option);
        return;
    };
    let force = defvalue.map_or(true, |d| d != val);
    let mut line: Vec<u8> = option.to_vec();
    line.push(b' ');
    // C: sdscatrepr — produce a quoted representation
    line.extend_from_slice(&bytes_to_repr(val));
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(&line), force);
}

/// C: rewriteConfigEnumOption (config.c:1412)
pub fn rewrite_config_enum_option(
    state: &mut RewriteConfigState,
    option: &[u8],
    value: i32,
    config: &StandardConfig,
) {
    let ConfigData::Enum(ref edata) = config.data else { return };
    let bitflags = config.flags & MULTI_ARG_CONFIG != 0;
    let names = config_enum_get_name(edata.enum_values, value, bitflags);
    let mut line: Vec<u8> = option.to_vec();
    line.push(b' ');
    line.extend_from_slice(names.as_bytes());
    let force = value != edata.default_value;
    rewrite_config_rewrite_line(state, option, RedisString::from_bytes(&line), force);
}

/// C: rewriteConfigGetContentFromState (config.c:1633)
pub fn rewrite_config_get_content(state: &RewriteConfigState) -> Vec<u8> {
    let mut content: Vec<u8> = Vec::new();
    let mut was_empty = false;

    for line in &state.lines {
        let empty = line.is_empty();
        if empty && was_empty {
            continue;
        }
        was_empty = empty;
        content.extend_from_slice(line.as_bytes());
        content.push(b'\n');
    }
    content
}

/// Blank all lines in the state that are associated with an option that was
/// processed but has no more uses.
///
/// C: rewriteConfigRemoveOrphaned (config.c:1659)
pub fn rewrite_config_remove_orphaned(state: &mut RewriteConfigState) {
    let orphan_line_nums: Vec<usize> = state
        .option_to_line
        .iter()
        .filter_map(|(opt, lines)| {
            if state.rewritten.contains(opt) {
                Some(lines.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    for linenum in orphan_line_nums {
        state.lines[linenum] = RedisString::new();
    }
}

/// Build a string of all DEBUG_CONFIG options for troubleshooting.
///
/// C: getConfigDebugInfo (config.c:1688)
pub fn get_config_debug_info(registry: &ConfigRegistry) -> Vec<u8> {
    let mut state = RewriteConfigState::new();
    state.force_write = true;
    state.needs_signature = false;

    for (key, cfg) in registry.iter() {
        if cfg.flags & DEBUG_CONFIG == 0 {
            continue;
        }
        if let Some(rewrite_fn) = cfg.interface.rewrite {
            rewrite_fn(cfg, key.as_bytes(), &mut state);
        }
    }
    rewrite_config_get_content(&state)
}

/// Atomically overwrite `configfile` with `content` via a temp file.
///
/// C: rewriteConfigOverwriteFile (config.c:1713)
pub fn rewrite_config_overwrite_file(configfile: &Path, content: &[u8]) -> Result<(), RedisError> {
    use std::io::Write;

    let dir = configfile.parent().unwrap_or(Path::new("."));
    let tmp_path = dir.join(format!(
        "{}.XXXXXX",
        configfile.file_name().and_then(|n| n.to_str()).unwrap_or("valkey.conf")
    ));
    // PORT NOTE: mkstemp → NamedTempFile in Rust (from the `tempfile` crate, Phase B).
    // For Phase A, write directly; TODO(port): use tempfile crate.
    let mut f = std::fs::File::create(&tmp_path)
        .map_err(|e| RedisError::runtime(format!("Could not create tmp config: {}", e).as_bytes()))?;
    f.write_all(content)
        .map_err(|e| RedisError::runtime(format!("Write to tmp config failed: {}", e).as_bytes()))?;
    f.sync_all()
        .map_err(|e| RedisError::runtime(format!("fsync tmp config failed: {}", e).as_bytes()))?;
    std::fs::rename(&tmp_path, configfile)
        .map_err(|e| RedisError::runtime(format!("rename tmp config failed: {}", e).as_bytes()))?;
    Ok(())
}

/// Full CONFIG REWRITE pipeline.
///
/// C: rewriteConfig (config.c:1782)
pub fn rewrite_config(
    path: &Path,
    force_write: bool,
    registry: &ConfigRegistry,
) -> Result<(), RedisError> {
    let mut state = rewrite_config_read_old_file(path)?;
    if force_write {
        state.force_write = true;
    }

    for (key, cfg) in registry.iter() {
        if cfg.flags & ALIAS_CONFIG != 0 {
            continue;
        }
        if let Some(rewrite_fn) = cfg.interface.rewrite {
            rewrite_fn(cfg, key.as_bytes(), &mut state);
        }
    }

    // TODO(port): rewriteConfigUserOption — requires ACL user iteration.
    // TODO(port): rewriteConfigLoadmoduleOption — requires module iterator.
    // TODO(port): rewriteConfigSentinelOption — sentinel mode, Phase 9.

    rewrite_config_remove_orphaned(&mut state);

    let newcontent = rewrite_config_get_content(&state);
    rewrite_config_overwrite_file(path, &newcontent)
}

// ── Type-interface functions: Bool ────────────────────────────────────────────
// C: boolConfigInit / boolConfigSet / boolConfigGet / boolConfigRewrite (config.c:1851-1884)
//
// PORT NOTE: These functions mutate server fields via stored pointer; the pointer
// is replaced by TODO(architect) accessor stubs.  In Phase A the functions are
// represented as stand-alone functions for documentation purposes only; the actual
// dispatch happens through the ConfigInterface function pointers stored in each
// StandardConfig entry.

pub fn bool_config_init(_config: &mut StandardConfig) {
    // TODO(architect): write config.data.yesno.default_value to the target server field.
}

pub fn bool_config_set(
    config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    let ConfigData::Bool(ref data) = config.data else {
        return Err(RedisError::runtime(b"internal: not a bool config"));
    };
    let yn = yesnotoi(argv[0].as_bytes()).ok_or_else(|| {
        *err_out = Some(RedisString::from_bytes(b"argument must be 'yes' or 'no'"));
        RedisError::runtime(b"argument must be 'yes' or 'no'")
    })?;
    // TODO(architect): read/write bool from server field accessor.
    // For now, compare against default and return changed/unchanged.
    let changed = yn != data.default_value;
    Ok(if changed { 1 } else { 2 })
}

pub fn bool_config_get(config: &StandardConfig) -> RedisString {
    let ConfigData::Bool(ref data) = config.data else {
        return RedisString::from_bytes(b"");
    };
    // TODO(architect): read actual server field value; using default_value placeholder.
    let yesno: &[u8] = if data.default_value { b"yes" } else { b"no" };
    RedisString::from_bytes(yesno)
}

pub fn bool_config_rewrite(
    config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    let ConfigData::Bool(ref data) = config.data else { return };
    // TODO(architect): read actual server field value.
    rewrite_config_yes_no_option(state, name, data.default_value, data.default_value);
}

// ── Type-interface functions: String ─────────────────────────────────────────
// C: stringConfigInit/Set/Get/Rewrite (config.c:1899-1924)

pub fn string_config_init(_config: &mut StandardConfig) {
    // TODO(architect): initialise server field from data.string.default_value.
}

pub fn string_config_set(
    config: &mut StandardConfig,
    argv: &[RedisString],
    _err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    let ConfigData::String(ref data) = config.data else {
        return Err(RedisError::runtime(b"internal: not a string config"));
    };
    let new_val = if data.convert_empty_to_null && argv[0].is_empty() {
        None
    } else {
        Some(argv[0].as_bytes())
    };
    let old_val = data.default_value;
    // TODO(architect): read old server field value; compare.
    let changed = new_val != old_val;
    // TODO(architect): write new_val to server field.
    Ok(if changed { 1 } else { 2 })
}

pub fn string_config_get(config: &StandardConfig) -> RedisString {
    let ConfigData::String(ref data) = config.data else {
        return RedisString::from_bytes(b"");
    };
    // TODO(architect): read from server field; placeholder returns default.
    RedisString::from_bytes(data.default_value.unwrap_or(b""))
}

pub fn string_config_rewrite(
    config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    let ConfigData::String(ref data) = config.data else { return };
    // TODO(architect): read from server field.
    rewrite_config_string_option(state, name, data.default_value, data.default_value);
}

// ── Type-interface functions: Sds ─────────────────────────────────────────────
// C: sdsConfigInit/Set/Get/Rewrite (config.c:1927-1969)

pub fn sds_config_init(_config: &mut StandardConfig) {
    // TODO(architect): initialise server sds field from data.sds.default_value.
}

pub fn sds_config_set(
    config: &mut StandardConfig,
    argv: &[RedisString],
    _err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    let ConfigData::Sds(ref data) = config.data else {
        return Err(RedisError::runtime(b"internal: not an sds config"));
    };
    let new_val = if data.convert_empty_to_null && argv[0].is_empty() {
        None
    } else {
        Some(argv[0].as_bytes())
    };
    let old_val = data.default_value;
    // TODO(architect): compare with server field value; write new value.
    let changed = new_val != old_val;
    Ok(if changed { 1 } else { 2 })
}

pub fn sds_config_get(config: &StandardConfig) -> RedisString {
    let ConfigData::Sds(ref data) = config.data else {
        return RedisString::from_bytes(b"");
    };
    RedisString::from_bytes(data.default_value.unwrap_or(b""))
}

pub fn sds_config_rewrite(
    config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    let ConfigData::Sds(ref data) = config.data else { return };
    rewrite_config_string_option(state, name, data.default_value, data.default_value);
}

// ── Type-interface functions: Enum ────────────────────────────────────────────
// C: enumConfigInit/Set/Get/Rewrite (config.c:2003-2047)

pub fn enum_config_init(_config: &mut StandardConfig) {
    // TODO(architect): write default_value to server field.
}

pub fn enum_config_set(
    config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    let ConfigData::Enum(ref data) = config.data else {
        return Err(RedisError::runtime(b"internal: not an enum config"));
    };
    let bitflags = config.flags & MULTI_ARG_CONFIG != 0;
    let enumval = config_enum_get_value(data.enum_values, argv, bitflags).ok_or_else(|| {
        let mut errmsg = Vec::from(b"argument(s) must be one of the following: " as &[u8]);
        for (i, e) in data.enum_values.iter().enumerate() {
            if i > 0 { errmsg.extend_from_slice(b", "); }
            errmsg.extend_from_slice(e.name);
        }
        let rs = RedisString::from_bytes(&errmsg);
        *err_out = Some(rs.clone());
        RedisError::runtime(rs.as_bytes())
    })?;
    // TODO(architect): read prev from server field; write enumval.
    let changed = enumval != data.default_value;
    Ok(if changed { 1 } else { 2 })
}

pub fn enum_config_get(config: &StandardConfig) -> RedisString {
    let ConfigData::Enum(ref data) = config.data else {
        return RedisString::from_bytes(b"");
    };
    let bitflags = config.flags & MULTI_ARG_CONFIG != 0;
    // TODO(architect): read value from server field.
    config_enum_get_name(data.enum_values, data.default_value, bitflags)
}

pub fn enum_config_rewrite(
    config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    let ConfigData::Enum(ref data) = config.data else { return };
    // TODO(architect): read value from server field.
    rewrite_config_enum_option(state, name, data.default_value, config);
}

// ── Type-interface functions: Numeric ─────────────────────────────────────────
// C: numericConfigInit/Set/Get/Rewrite + helpers (config.c:2127-2290)

pub fn numeric_config_init(_config: &mut StandardConfig) {
    // TODO(architect): setNumericType — write default_value to server field.
}

/// Parse a numeric config value from a string, respecting the config's flags.
///
/// C: numericParseString (config.c:2172)
pub fn numeric_parse_string(
    data: &NumericConfigData,
    value: &[u8],
    err_out: &mut Option<RedisString>,
) -> Option<i64> {
    if data.flags & MEMORY_CONFIG != 0 {
        if let Ok(v) = mem_to_ull(value) {
            return Some(v as i64);
        }
        if data.flags & SIGNED_MEMORY_CONFIG != 0 {
            if let Some(v) = parse_decimal_i64(value).ok() {
                return Some(v);
            }
        }
    }

    if data.flags & PERCENT_CONFIG != 0 && value.last() == Some(&b'%') {
        let digits = &value[..value.len() - 1];
        if let Ok(n) = parse_decimal_i64(digits) {
            if n >= 0 {
                return Some(-n); // stored as negative
            }
        }
    }

    if data.flags & OCTAL_CONFIG != 0 {
        if let Ok(n) = i64::from_str_radix(
            std::str::from_utf8(value).unwrap_or(""),
            8,
        ) {
            return Some(n);
        }
    }

    if data.flags & UNSIGNED_CONFIG != 0 {
        if let Ok(n) = parse_decimal_u64(value) {
            return Some(n as i64);
        }
    }

    if data.flags == INTEGER_CONFIG {
        if let Ok(n) = parse_decimal_i64(value) {
            return Some(n);
        }
    }

    // Build error message
    let emsg: &[u8] = if data.flags & MEMORY_CONFIG != 0 && data.flags & PERCENT_CONFIG != 0 {
        b"argument must be a memory or percent value"
    } else if data.flags & MEMORY_CONFIG != 0 {
        b"argument must be a memory value"
    } else if data.flags & OCTAL_CONFIG != 0 {
        b"argument couldn't be parsed as an octal number"
    } else if data.flags & UNSIGNED_CONFIG != 0 {
        b"argument couldn't be parsed as an unsigned number"
    } else {
        b"argument couldn't be parsed into an integer"
    };
    *err_out = Some(RedisString::from_bytes(emsg));
    None
}

/// Check that a numeric value is within the config's declared bounds.
///
/// C: numericBoundaryCheck (config.c:2131)
pub fn numeric_boundary_check(
    data: &NumericConfigData,
    ll: i64,
    err_out: &mut Option<RedisString>,
) -> bool {
    let is_unsigned = matches!(
        data.numeric_type,
        NumericType::UlongLong | NumericType::Ulong | NumericType::Uint | NumericType::SizeT
    );

    if is_unsigned {
        let ull = ll as u64;
        let upper = data.upper_bound as u64;
        let lower = data.lower_bound as u64;
        if ull > upper || ull < lower {
            let msg = if data.flags & OCTAL_CONFIG != 0 {
                format!("argument must be between {:o} and {:o} inclusive", lower, upper)
            } else {
                format!("argument must be between {} and {} inclusive", lower, upper)
            };
            *err_out = Some(RedisString::from_bytes(msg.as_bytes()));
            return false;
        }
    } else if data.flags & PERCENT_CONFIG != 0 && ll < 0 {
        if ll < data.lower_bound {
            let msg = format!(
                "percentage argument must be less or equal to {}",
                -data.lower_bound
            );
            *err_out = Some(RedisString::from_bytes(msg.as_bytes()));
            return false;
        }
    } else if ll > data.upper_bound || ll < data.lower_bound {
        let msg = format!(
            "argument must be between {} and {} inclusive",
            data.lower_bound, data.upper_bound
        );
        *err_out = Some(RedisString::from_bytes(msg.as_bytes()));
        return false;
    }
    true
}

pub fn numeric_config_set(
    config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    let ConfigData::Numeric(ref data) = config.data else {
        return Err(RedisError::runtime(b"internal: not a numeric config"));
    };
    let ll = numeric_parse_string(data, argv[0].as_bytes(), err_out).ok_or_else(|| {
        RedisError::runtime(err_out.as_ref().map(|e| e.as_bytes()).unwrap_or(b"parse error"))
    })?;
    if !numeric_boundary_check(data, ll, err_out) {
        return Err(RedisError::runtime(
            err_out.as_ref().map(|e| e.as_bytes()).unwrap_or(b"out of range"),
        ));
    }
    // TODO(architect): read prev from server field; compare; write if changed.
    let changed = ll != data.default_value;
    Ok(if changed { 1 } else { 2 })
}

pub fn numeric_config_get(config: &StandardConfig) -> RedisString {
    let ConfigData::Numeric(ref data) = config.data else {
        return RedisString::from_bytes(b"");
    };
    // TODO(architect): read value from server field.
    let value = data.default_value;

    let s = if data.flags & PERCENT_CONFIG != 0 && value < 0 {
        format!("{}%", -value)
    } else if data.flags & OCTAL_CONFIG != 0 {
        format!("{:o}", value)
    } else if data.flags & (MEMORY_CONFIG | UNSIGNED_CONFIG) != 0 {
        format!("{}", value as u64)
    } else {
        format!("{}", value)
    };
    RedisString::from_bytes(s.as_bytes())
}

pub fn numeric_config_rewrite(
    config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    let ConfigData::Numeric(ref data) = config.data else { return };
    // TODO(architect): read value from server field.
    let value = data.default_value;

    if data.flags & PERCENT_CONFIG != 0 && value < 0 {
        rewrite_config_percent_option(state, name, -value, data.default_value);
    } else if data.flags & MEMORY_CONFIG != 0 && !(data.flags & SIGNED_MEMORY_CONFIG != 0 && value < 0) {
        rewrite_config_bytes_option(state, name, value as u64, data.default_value as u64);
    } else if data.flags & OCTAL_CONFIG != 0 {
        rewrite_config_octal_option(state, name, value, data.default_value);
    } else {
        rewrite_config_numerical_option(state, name, value, data.default_value);
    }
}

// ── Validation callbacks ───────────────────────────────────────────────────────
// C: isValid* functions (config.c:2389-2522)

fn is_valid_active_defrag(val: bool, err_out: &mut Option<RedisString>) -> bool {
    // C: if !HAVE_DEFRAG { *err = "Active defrag..."; return 0; }
    // TODO(port): conditional compilation — check cfg feature flag for defrag support.
    let _ = (val, err_out);
    true
}

fn is_valid_cluster_config_file(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        *err_out = Some(RedisString::from_bytes(b"cluster-config-file can't be empty"));
        return false;
    }
    true
}

fn is_valid_db_filename(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        *err_out = Some(RedisString::from_bytes(b"dbfilename can't be empty"));
        return false;
    }
    if !path_is_base_name(val) {
        *err_out = Some(RedisString::from_bytes(b"dbfilename can't be a path, just a filename"));
        return false;
    }
    true
}

fn is_valid_aof_filename(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        *err_out = Some(RedisString::from_bytes(b"appendfilename can't be empty"));
        return false;
    }
    if !path_is_base_name(val) {
        *err_out = Some(RedisString::from_bytes(b"appendfilename can't be a path, just a filename"));
        return false;
    }
    true
}

fn is_valid_aof_dirname(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        *err_out = Some(RedisString::from_bytes(b"appenddirname can't be empty"));
        return false;
    }
    if !path_is_base_name(val) {
        *err_out = Some(RedisString::from_bytes(b"appenddirname can't be a path, just a dirname"));
        return false;
    }
    true
}

fn is_valid_shutdown_on_sig_flags(val: i32, err_out: &mut Option<RedisString>) -> bool {
    const SHUTDOWN_NOSAVE: i32 = 2; // TODO(port): actual flag values
    const SHUTDOWN_SAVE: i32 = 1;
    if val & SHUTDOWN_NOSAVE != 0 && val & SHUTDOWN_SAVE != 0 {
        *err_out = Some(RedisString::from_bytes(
            b"shutdown options SAVE and NOSAVE can't be used simultaneously",
        ));
        return false;
    }
    true
}

fn is_valid_announced_nodename(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if !is_valid_aux_string(val) {
        *err_out = Some(RedisString::from_bytes(
            b"Announced human node name contained invalid character",
        ));
        return false;
    }
    true
}

fn is_valid_announced_hostname(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    const NET_HOST_STR_LEN: usize = 256;
    if val.len() >= NET_HOST_STR_LEN {
        *err_out = Some(RedisString::from_bytes(b"Hostnames must be less than 256 characters"));
        return false;
    }
    for &c in val {
        if !c.is_ascii_alphanumeric() && c != b'-' && c != b'.' {
            *err_out = Some(RedisString::from_bytes(
                b"Hostnames may only contain alphanumeric characters, hyphens or dots",
            ));
            return false;
        }
    }
    true
}

fn is_valid_ipv4(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        return true;
    }
    // TODO(port): use std::net::Ipv4Addr::from_str for proper validation.
    let s = match std::str::from_utf8(val) {
        Ok(s) => s,
        Err(_) => {
            *err_out = Some(RedisString::from_bytes(b"Invalid IPv4 address"));
            return false;
        }
    };
    if s.parse::<std::net::Ipv4Addr>().is_err() {
        *err_out = Some(RedisString::from_bytes(b"Invalid IPv4 address"));
        return false;
    }
    true
}

fn is_valid_ipv6(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    if val.is_empty() {
        return true;
    }
    let s = match std::str::from_utf8(val) {
        Ok(s) => s,
        Err(_) => {
            *err_out = Some(RedisString::from_bytes(b"Invalid IPv6 address"));
            return false;
        }
    };
    if s.parse::<std::net::Ipv6Addr>().is_err() {
        *err_out = Some(RedisString::from_bytes(b"Invalid IPv6 address"));
        return false;
    }
    true
}

fn is_valid_mptcp(val: bool, err_out: &mut Option<RedisString>) -> bool {
    // TODO(port): check anetHasMptcp() — requires OS detection at runtime.
    let _ = (val, err_out);
    true
}

fn is_valid_proc_title_template(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    // TODO(port): validateProcTitleTemplate — requires proc-title format rules.
    let _ = (val, err_out);
    true
}

fn is_valid_db_hash_seed(val: &[u8], err_out: &mut Option<RedisString>) -> bool {
    const HASH_SEED_MAX_LEN: usize = 256;
    if val.len() > HASH_SEED_MAX_LEN {
        *err_out = Some(RedisString::from_bytes(
            b"hash-seed must be less than or equal to 256 characters",
        ));
        return false;
    }
    true
}

// ── Apply callbacks ────────────────────────────────────────────────────────────
// C: update* / apply* functions (config.c:2524-2828)
//
// All apply functions are `fn() -> Result<(), RedisError>` in Phase A.
// They need access to `&mut RedisServer`; for now they call stub functions
// that will be wired up in Phase B.

fn update_hz() -> Result<(), RedisError> {
    // TODO(port): clamp server.hz to [CONFIG_MIN_HZ, CONFIG_MAX_HZ].
    Ok(())
}

fn update_port() -> Result<(), RedisError> {
    // TODO(port): re-bind the TCP listener on new port.
    Ok(())
}

fn update_defrag_configuration() -> Result<(), RedisError> {
    // TODO(port): server.active_defrag_configuration_changed = 1.
    Ok(())
}

fn update_jemalloc_bg_thread() -> Result<(), RedisError> {
    // TODO(port): set_jemalloc_bg_thread(server.jemalloc_bg_thread).
    Ok(())
}

fn update_repl_backlog_size() -> Result<(), RedisError> {
    // TODO(port): resizeReplicationBacklog().
    Ok(())
}

fn update_maxmemory() -> Result<(), RedisError> {
    // TODO(port): warn if new limit < used memory; startEvictionTimeProc().
    Ok(())
}

fn update_good_replicas() -> Result<(), RedisError> {
    // TODO(port): refreshGoodReplicasCount().
    Ok(())
}

fn update_watchdog_period() -> Result<(), RedisError> {
    // TODO(port): applyWatchdogPeriod().
    Ok(())
}

fn update_append_only() -> Result<(), RedisError> {
    // TODO(port): stopAppendOnly() / startAppendOnly().
    Ok(())
}

fn update_aof_auto_gc_enabled() -> Result<(), RedisError> {
    // TODO(port): aofDelHistoryFiles().
    Ok(())
}

fn update_extended_redis_compat() -> Result<(), RedisError> {
    // TODO(port): updateSharedObjectsWithCompat().
    Ok(())
}

fn update_sighandler_enabled() -> Result<(), RedisError> {
    // TODO(port): setupSigSegvHandler() / removeSigSegvHandlers().
    Ok(())
}

fn update_maxclients() -> Result<(), RedisError> {
    // TODO(port): adjustOpenFilesLimit(); aeResizeSetSize().
    Ok(())
}

fn update_oom_score_adj() -> Result<(), RedisError> {
    // TODO(port): setOOMScoreAdj(-1).
    Ok(())
}

fn invalidate_cluster_slots_resp() -> Result<(), RedisError> {
    // TODO(port): clearCachedClusterSlotsResponse().
    Ok(())
}

fn update_lua_enable_insecure_api() -> Result<(), RedisError> {
    // TODO(port): evalReset() if insecure_api_current != lua_enable_insecure_api.
    Ok(())
}

fn update_require_pass() -> Result<(), RedisError> {
    // TODO(port): ACLUpdateDefaultUserPassword(server.requirepass).
    Ok(())
}

fn update_append_fsync() -> Result<(), RedisError> {
    // TODO(port): bioDrainWorker(BIO_AOF_FSYNC) if AOF_FSYNC_ALWAYS.
    Ok(())
}

fn apply_bind() -> Result<(), RedisError> {
    // TODO(port): changeListener() for TCP + TLS listeners.
    Ok(())
}

fn update_cluster_flags() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfFlags().
    Ok(())
}

fn update_cluster_announced_port() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfAnnouncedPorts().
    Ok(())
}

fn update_cluster_ip() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfIp().
    Ok(())
}

fn update_cluster_client_ip_v4() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfClientIpV4().
    Ok(())
}

fn update_cluster_client_ip_v6() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfClientIpV6().
    Ok(())
}

fn update_cluster_hostname() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfHostname().
    Ok(())
}

fn update_cluster_human_nodename() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfHumanNodename().
    Ok(())
}

fn update_cluster_availability_zone() -> Result<(), RedisError> {
    // TODO(port): clusterUpdateMyselfAvailabilityZone(); invalidateClusterSlotsResp().
    Ok(())
}

fn apply_tls_cfg() -> Result<(), RedisError> {
    // TODO(port): connTypeConfigure(connectionTypeTls(), &server.tls_ctx_config, 1).
    Ok(())
}

fn apply_tls_port() -> Result<(), RedisError> {
    // TODO(port): configure TLS; changeListener for TLS port.
    Ok(())
}

fn apply_client_max_memory_usage() -> Result<(), RedisError> {
    // TODO(port): initServerClientMemUsageBuckets / updateClientMemUsageAndBucket.
    Ok(())
}

fn update_io_threads() -> Result<(), RedisError> {
    // TODO(port): spawn/retire IO threads based on new server.io_threads_num.
    Ok(())
}

fn on_max_batch_size_change() -> Result<(), RedisError> {
    // TODO(port): update prefetch batch configuration.
    Ok(())
}

fn update_locale_collate() -> Result<(), RedisError> {
    // TODO(port): setlocale(LC_COLLATE, server.locale_collate).
    Ok(())
}

fn update_proc_title_template() -> Result<(), RedisError> {
    // TODO(port): serverSetProcTitle(NULL).
    Ok(())
}

fn apply_rdma_bind() -> Result<(), RedisError> {
    // TODO(port): changeListener for RDMA listener.
    Ok(())
}

fn update_rdma_port() -> Result<(), RedisError> {
    // TODO(port): changeListener for RDMA port.
    Ok(())
}

// ── Special config set/get/rewrite functions ──────────────────────────────────
// C: setConfigDirOption/getConfigDirOption (config.c:2830-2854)

fn set_config_dir_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    if argv.len() != 1 {
        *err_out = Some(RedisString::from_bytes(b"wrong number of arguments"));
        return Err(RedisError::runtime(b"wrong number of arguments"));
    }
    if argv[0].is_empty() {
        *err_out = Some(RedisString::from_bytes(b"dir can't be empty"));
        return Err(RedisError::runtime(b"dir can't be empty"));
    }
    let dir_str = std::str::from_utf8(argv[0].as_bytes())
        .map_err(|_| RedisError::runtime(b"dir contains invalid UTF-8"))?;
    std::env::set_current_dir(dir_str)
        .map_err(|e| RedisError::runtime(format!("{}", e).as_bytes()))?;
    Ok(1)
}

fn get_config_dir_option(_config: &StandardConfig) -> RedisString {
    match std::env::current_dir() {
        Ok(p) => RedisString::from_bytes(p.to_string_lossy().as_bytes()),
        Err(_) => RedisString::from_bytes(b""),
    }
}

fn rewrite_config_dir_option(
    _config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    match std::env::current_dir() {
        Ok(p) => {
            rewrite_config_string_option(
                state,
                name,
                Some(p.to_string_lossy().as_bytes()),
                None,
            );
        }
        Err(_) => {
            state.mark_as_processed(name);
        }
    }
}

// C: setConfigSaveOption / getConfigSaveOption (config.c:2856-2920)
// The save option is complex (multiple <seconds changes> pairs).

fn set_config_save_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    // TODO(port): modifies server.saveparams / server.saveparamslen.
    // Full translation requires &mut RedisServer access.
    let _ = (argv, err_out);
    Ok(1)
}

fn get_config_save_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): reads server.saveparams.
    RedisString::from_bytes(b"")
}

// C: setConfigClientOutputBufferLimitOption / getConfigClientOutputBufferLimitOption

fn set_config_client_output_buffer_limit_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    // TODO(port): needs &mut [ClientBufferLimitsConfig; CLIENT_TYPE_OBUF_COUNT] from server.
    let _ = (argv, err_out);
    Ok(1)
}

fn get_config_client_output_buffer_limit_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): reads server.client_obuf_limits.
    RedisString::from_bytes(b"")
}

// C: setConfigOOMScoreAdjValuesOption / getConfigOOMScoreAdjValuesOption

fn set_config_oom_score_adj_values_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    if argv.len() != CONFIG_OOM_COUNT {
        *err_out = Some(RedisString::from_bytes(b"wrong number of arguments"));
        return Err(RedisError::runtime(b"wrong number of arguments"));
    }
    let mut values = [0i32; CONFIG_OOM_COUNT];
    for (i, arg) in argv.iter().enumerate() {
        let v: i64 = parse_decimal_i64(arg.as_bytes())
            .map_err(|_| RedisError::runtime(b"Invalid oom-score-adj-values"))?;
        if v < -2000 || v > 2000 {
            *err_out = Some(RedisString::from_bytes(
                b"Invalid oom-score-adj-values, elements must be between -2000 and 2000.",
            ));
            return Err(RedisError::runtime(
                b"Invalid oom-score-adj-values, elements must be between -2000 and 2000.",
            ));
        }
        values[i] = v as i32;
    }
    if values[CONFIG_OOM_REPLICA] < values[CONFIG_OOM_PRIMARY]
        || values[CONFIG_OOM_BGCHILD] < values[CONFIG_OOM_REPLICA]
    {
        eprintln!("Warning: oom-score-adj-values may not work for non-privileged processes");
    }
    // TODO(port): write values to server.oom_score_adj_values.
    Ok(1)
}

fn get_config_oom_score_adj_values_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): reads server.oom_score_adj_values.
    RedisString::from_bytes(b"0 200 800")
}

// C: setConfigNotifyKeyspaceEventsOption / getConfigNotifyKeyspaceEventsOption

fn set_config_notify_keyspace_events_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    if argv.len() != 1 {
        *err_out = Some(RedisString::from_bytes(b"wrong number of arguments"));
        return Err(RedisError::runtime(b"wrong number of arguments"));
    }
    // TODO(port): keyspaceEventsStringToFlags — parse the event class string.
    // TODO(port): write flags to server.notify_keyspace_events.
    Ok(1)
}

fn get_config_notify_keyspace_events_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): keyspaceEventsFlagsToString(server.notify_keyspace_events).
    RedisString::from_bytes(b"")
}

// C: setConfigSocketBindOption / getConfigBindOption

fn set_config_socket_bind_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    const CONFIG_BINDADDR_MAX: usize = 16;
    if argv.len() > CONFIG_BINDADDR_MAX {
        *err_out = Some(RedisString::from_bytes(b"Too many bind addresses specified."));
        return Err(RedisError::runtime(b"Too many bind addresses specified."));
    }
    // TODO(port): update server.bindaddr / server.bindaddr_count.
    Ok(1)
}

fn get_config_bind_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): sdsjoin(server.bindaddr, server.bindaddr_count, " ").
    RedisString::from_bytes(b"")
}

// C: setConfigReplicaOfOption / getConfigReplicaOfOption

fn set_config_replica_of_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    if argv.len() != 2 {
        *err_out = Some(RedisString::from_bytes(b"wrong number of arguments"));
        return Err(RedisError::runtime(b"wrong number of arguments"));
    }
    // TODO(port): update server.primary_host / server.primary_port / server.repl_state.
    Ok(1)
}

fn get_config_replica_of_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): return "<host> <port>" or "" from server fields.
    RedisString::from_bytes(b"")
}

// C: setConfigLatencyTrackingInfoPercentilesOutputOption / get… / rewrite…

fn set_config_latency_tracking_info_percentiles_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    for arg in argv {
        if arg.is_empty() {
            continue;
        }
        let s = std::str::from_utf8(arg.as_bytes()).unwrap_or("");
        let p: f64 = s.parse().map_err(|_| {
            *err_out = Some(RedisString::from_bytes(
                b"Invalid latency-tracking-info-percentiles parameters",
            ));
            RedisError::runtime(b"Invalid latency-tracking-info-percentiles parameters")
        })?;
        if p < 0.0 || p > 100.0 {
            *err_out = Some(RedisString::from_bytes(
                b"latency-tracking-info-percentiles parameters should sit between [0.0,100.0]",
            ));
            return Err(RedisError::runtime(
                b"latency-tracking-info-percentiles parameters should sit between [0.0,100.0]",
            ));
        }
    }
    // TODO(port): store to server.latency_tracking_info_percentiles.
    Ok(1)
}

fn get_config_latency_tracking_info_percentiles_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): format from server.latency_tracking_info_percentiles.
    RedisString::from_bytes(b"")
}

fn rewrite_config_latency_tracking_info_percentiles_option(
    _config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    // TODO(port): emit percentile list from server.latency_tracking_info_percentiles.
    let mut line: Vec<u8> = name.to_vec();
    line.extend_from_slice(b" \"\"");
    rewrite_config_rewrite_line(state, name, RedisString::from_bytes(&line), true);
}

// C: setConfigRdmaBindOption / getConfigRdmaBindOption / rewriteConfigRdmaBindOption

fn set_config_rdma_bind_option(
    _config: &mut StandardConfig,
    argv: &[RedisString],
    err_out: &mut Option<RedisString>,
) -> Result<i32, RedisError> {
    // TODO(port): update server.rdma_ctx_config.bindaddr.
    let _ = (argv, err_out);
    Ok(1)
}

fn get_config_rdma_bind_option(_config: &StandardConfig) -> RedisString {
    // TODO(port): sdsjoin(server.rdma_ctx_config.bindaddr, …).
    RedisString::from_bytes(b"")
}

fn rewrite_config_rdma_bind_option(
    _config: &StandardConfig,
    name: &[u8],
    state: &mut RewriteConfigState,
) {
    // TODO(port): rewrite from server.rdma_ctx_config.bindaddr if count > 0.
    state.mark_as_processed(name);
}

// ── allowProtectedAction ──────────────────────────────────────────────────────
// C: allowProtectedAction (config.c:3143)

/// Determine whether a protected config action is permitted.
///
/// C: allowProtectedAction (config.c:3143)
pub fn allow_protected_action(config: i32, is_local: bool) -> bool {
    const PROTECTED_ACTION_ALLOWED_YES: i32 = 1;
    const PROTECTED_ACTION_ALLOWED_LOCAL: i32 = 2;
    config == PROTECTED_ACTION_ALLOWED_YES
        || (config == PROTECTED_ACTION_ALLOWED_LOCAL && is_local)
}

// ── Registry management: registerConfigValue / initConfigValues / removeConfig ─
// C: registerConfigValue / initConfigValues / removeConfig (config.c:3522-3584)

/// Register a single config entry by name, optionally as an alias.
///
/// C: registerConfigValue (config.c:3522)
pub fn register_config_value(
    registry: &mut ConfigRegistry,
    name: &'static [u8],
    config: StandardConfig,
    as_alias: bool,
) -> bool {
    let key = if as_alias {
        // When registering as alias, the alias entry uses the primary name as alias
        RedisString::from_bytes(config.alias.unwrap_or(name))
    } else {
        RedisString::from_bytes(name)
    };
    registry.register(key, config)
}

/// Initialise the runtime config registry from `STATIC_CONFIGS`.
///
/// C: initConfigValues (config.c:3536)
///
/// TODO(port): cannot fully initialise configs without access to
/// `&mut RedisServer`; the `init` callback writes defaults into server fields.
/// Phase B will wire `server` through here.
pub fn init_config_values(registry: &mut ConfigRegistry) {
    // TODO(port): iterate STATIC_CONFIGS, call config.interface.init(config),
    // and register each config (+ its alias) in the registry.
    // STATIC_CONFIGS is large (260+ entries) and references server fields
    // via TODO(architect) accessors; deferred to Phase B.
}

/// Remove a config entry from the registry (used when unloading a module).
///
/// C: removeConfig (config.c:3567)
pub fn remove_config(registry: &mut ConfigRegistry, name: &[u8]) {
    // TODO(port): for MODULE_CONFIG entries, also free the config name sds
    // and enum values if ENUM_CONFIG.
    registry.remove(name);
}

// ── Module config registration ────────────────────────────────────────────────
// C: addModule*Config functions (config.c:3591-3658)

/// Register a bool config for a loaded module.
///
/// C: addModuleBoolConfig (config.c:3591)
pub fn add_module_bool_config(
    registry: &mut ConfigRegistry,
    module_name: &[u8],
    name: &[u8],
    flags: u32,
    default_val: bool,
) {
    let config_name = {
        let mut n = module_name.to_vec();
        n.push(b'.');
        n.extend_from_slice(name);
        n
    };
    // TODO(port): privdata (ModuleConfig*) cannot be typed until Phase 10.
    let _config = StandardConfig {
        name: Box::leak(config_name.clone().into_boxed_slice()),
        alias: None,
        flags: flags | MODULE_CONFIG,
        interface: ConfigInterface {
            init: Some(bool_config_init),
            set: |c, a, e| bool_config_set(c, a, e),
            apply: None,
            get: bool_config_get,
            rewrite: Some(bool_config_rewrite),
        },
        data: ConfigData::Bool(BoolConfigData { default_value: default_val }),
        config_type: ConfigType::Bool,
        privdata: None, // TODO(architect): ModuleConfig privdata
    };
    // registry.register(RedisString::from_bytes(&config_name), _config);
    // TODO(port): uncomment once StandardConfig: Send + Sync resolved.
}

/// Register a string (SDS) config for a loaded module.
///
/// C: addModuleStringConfig (config.c:3601)
pub fn add_module_string_config(
    _registry: &mut ConfigRegistry,
    _module_name: &[u8],
    _name: &[u8],
    _flags: u32,
    _default_val: &[u8],
) {
    // TODO(port): mirror of add_module_bool_config for SDS type.
}

/// Register an enum config for a loaded module.
///
/// C: addModuleEnumConfig (config.c:3611)
pub fn add_module_enum_config(
    _registry: &mut ConfigRegistry,
    _module_name: &[u8],
    _name: &[u8],
    _flags: u32,
    _default_val: i32,
    _enum_vals: &'static [ConfigEnumEntry],
) {
    // TODO(port): mirror of add_module_bool_config for enum type.
}

/// Register a numeric (long long) config for a loaded module.
///
/// C: addModuleNumericConfig (config.c:3626)
pub fn add_module_numeric_config(
    _registry: &mut ConfigRegistry,
    _module_name: &[u8],
    _name: &[u8],
    _flags: u32,
    _default_val: i64,
    _conf_flags: u32,
    _lower: i64,
    _upper: i64,
) {
    // TODO(port): mirror of add_module_bool_config for numeric type.
}

/// Register an unsigned numeric config for a loaded module.
///
/// C: addModuleUnsignedNumericConfig (config.c:3643)
pub fn add_module_unsigned_numeric_config(
    _registry: &mut ConfigRegistry,
    _module_name: &[u8],
    _name: &[u8],
    _flags: u32,
    _default_val: u64,
    _conf_flags: u32,
    _lower: u64,
    _upper: u64,
) {
    // TODO(port): mirror of add_module_bool_config for ull numeric type.
}

// ── CONFIG sub-commands ───────────────────────────────────────────────────────
// C: configHelpCommand / configResetStatCommand / configRewriteCommand (config.c:3664-3708)

/// `CONFIG HELP`
///
/// C: configHelpCommand (config.c:3664)
pub fn config_help_command(
    ctx: &mut redis_core::command_context::CommandContext,
) -> Result<(), RedisError> {
    let help: &[&[u8]] = &[
        b"GET <pattern>",
        b"    Return parameters matching the glob-like <pattern> and their values.",
        b"SET <directive> <value>",
        b"    Set the configuration <directive> to <value>.",
        b"RESETSTAT",
        b"    Reset statistics reported by the INFO command.",
        b"REWRITE",
        b"    Rewrite the configuration file.",
    ];
    ctx.reply_array_header(help.len())?;
    for line in help {
        ctx.reply_bulk(line)?;
    }
    Ok(())
}

/// `CONFIG RESETSTAT`
///
/// C: configResetStatCommand (config.c:3682)
pub fn config_reset_stat_command(
    ctx: &mut redis_core::command_context::CommandContext,
) -> Result<(), RedisError> {
    // TODO(port): resetServerStats(); resetClusterStats(); resetCommandTableStats();
    //             resetErrorTableStats();
    ctx.reply_simple_string(b"OK")
}

/// `CONFIG REWRITE`
///
/// C: configRewriteCommand (config.c:3694)
pub fn config_rewrite_command(
    ctx: &mut redis_core::command_context::CommandContext,
    configfile: Option<&Path>,
    registry: &ConfigRegistry,
) -> Result<(), RedisError> {
    let path = configfile.ok_or_else(|| {
        RedisError::runtime(b"The server is running without a config file")
    })?;
    rewrite_config(path, false, registry).map_err(|e| {
        // TODO(port): log the error via server logger
        e
    })?;
    // TODO(port): serverLog(LL_NOTICE, "CONFIG REWRITE executed with success.")
    ctx.reply_simple_string(b"OK")
}

// ── Internal utility helpers (not in original C; support the translation) ─────

/// Returns true if `val` contains only ASCII characters valid in cluster node names.
///
/// C: isValidAuxString — defined in cluster.c / util.c (TODO(port): import from there).
fn is_valid_aux_string(val: &[u8]) -> bool {
    val.iter().all(|&c| c.is_ascii() && !c.is_ascii_control() && c != b' ')
}

/// Returns true if `val` is a plain basename (no path separators).
///
/// C: pathIsBaseName — defined in util.c (TODO(port): import from there).
fn path_is_base_name(val: &[u8]) -> bool {
    !val.contains(&b'/') && !val.contains(&b'\\')
}

/// Split a config file blob into individual lines (without newlines).
fn split_config_lines(data: &[u8]) -> Vec<Vec<u8>> {
    data.split(|&b| b == b'\n')
        .map(|l| {
            let l = if l.ends_with(b"\r") { &l[..l.len() - 1] } else { l };
            l.to_vec()
        })
        .collect()
}

/// Trim leading/trailing ASCII whitespace from a byte slice.
fn trim_whitespace(s: &[u8]) -> &[u8] {
    let start = s.iter().position(|&b| !b.is_ascii_whitespace()).unwrap_or(s.len());
    let end = s.iter().rposition(|&b| !b.is_ascii_whitespace()).map(|i| i + 1).unwrap_or(0);
    if start > end { &[] } else { &s[start..end] }
}

/// Split a config line into tokens, respecting quoted strings.
///
/// C: sdssplitargs — defined in sds.c.
fn split_args(line: &[u8]) -> Result<Vec<Vec<u8>>, ()> {
    // PERF(port): naive implementation; replace with sds.c-equivalent in Phase B.
    let mut args: Vec<Vec<u8>> = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut in_quotes = false;
    let mut i = 0;

    while i < line.len() {
        let b = line[i];
        if in_quotes {
            if b == b'"' {
                in_quotes = false;
            } else if b == b'\\' && i + 1 < line.len() {
                i += 1;
                cur.push(line[i]);
            } else {
                cur.push(b);
            }
        } else if b == b'"' {
            in_quotes = true;
        } else if b.is_ascii_whitespace() {
            if !cur.is_empty() {
                args.push(cur.clone());
                cur.clear();
            }
        } else {
            cur.push(b);
        }
        i += 1;
    }

    if in_quotes {
        return Err(());
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    Ok(args)
}

/// ASCII-lowercase a byte slice into a `Vec<u8>`.
fn ascii_lowercase(s: &[u8]) -> Vec<u8> {
    s.iter().map(|b| b.to_ascii_lowercase()).collect()
}

/// Split bytes on ASCII spaces.
fn split_by_space(s: &[u8]) -> Vec<Vec<u8>> {
    s.split(|&b| b == b' ')
        .filter(|p| !p.is_empty())
        .map(|p| p.to_vec())
        .collect()
}

/// Check whether `s` contains any glob wildcard characters.
fn has_glob_chars(s: &[u8]) -> bool {
    s.iter().any(|&b| b == b'[' || b == b'*' || b == b'?')
}

/// Minimal glob matching (case-insensitive if `nocase` is true).
///
/// C: stringmatch / stringmatchlen — defined in util.c (TODO(port): import).
fn string_match_len(pattern: &[u8], text: &[u8], nocase: bool) -> bool {
    // TODO(port): implement full glob matching as in stringmatch (util.c).
    // Placeholder: exact match or simple '*' prefix/suffix.
    if pattern == b"*" {
        return true;
    }
    if nocase {
        pattern.eq_ignore_ascii_case(text)
    } else {
        pattern == text
    }
}

/// Parse a decimal i64 from bytes.
fn parse_decimal_i64(s: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(s)
        .map_err(|_| ())?
        .parse::<i64>()
        .map_err(|_| ())
}

/// Parse a decimal u64 from bytes.
fn parse_decimal_u64(s: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(s)
        .map_err(|_| ())?
        .parse::<u64>()
        .map_err(|_| ())
}

/// Parse a memory quantity (e.g. `1mb`, `512kb`, `1073741824`) into bytes.
///
/// C: memtoull — defined in util.c (TODO(port): import from there).
fn mem_to_ull(s: &[u8]) -> Result<u64, ()> {
    // PERF(port): naive implementation; replace with util.c equivalent.
    if s.is_empty() {
        return Err(());
    }
    let (digits, suffix) = if s.last().map(|b| b.is_ascii_alphabetic()).unwrap_or(false) {
        let split = s.len() - 2; // e.g. "gb" is 2 chars
        // Could be 2-char or 1-char suffix; check
        if s.len() >= 2 && s[s.len() - 2].is_ascii_alphabetic() {
            (&s[..s.len() - 2], &s[s.len() - 2..])
        } else {
            (&s[..s.len() - 1], &s[s.len() - 1..])
        }
    } else {
        (s, &b""[..])
    };
    let n: u64 = parse_decimal_u64(digits)?;
    let multiplier: u64 = match suffix.to_ascii_lowercase().as_slice() {
        b"gb" => 1024 * 1024 * 1024,
        b"mb" => 1024 * 1024,
        b"kb" => 1024,
        b"b" | b"" => 1,
        _ => return Err(()),
    };
    n.checked_mul(multiplier).ok_or(())
}

/// Return a Redis-style quoted representation of `bytes` (like C's sdscatrepr).
fn bytes_to_repr(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![b'"'];
    for &b in bytes {
        match b {
            b'"' => { out.push(b'\\'); out.push(b'"'); }
            b'\\' => { out.push(b'\\'); out.push(b'\\'); }
            b'\n' => { out.push(b'\\'); out.push(b'n'); }
            b'\r' => { out.push(b'\\'); out.push(b'r'); }
            b'\t' => { out.push(b'\\'); out.push(b't'); }
            0..=31 | 127..=255 => {
                out.extend_from_slice(format!("\\x{:02x}", b).as_bytes());
            }
            _ => out.push(b),
        }
    }
    out.push(b'"');
    out
}

/// Return the numeric index for a client type by name.
///
/// C: getClientTypeByName — defined in networking.c (TODO(port): import).
fn get_client_type_by_name(name: &[u8]) -> Option<usize> {
    match name {
        b"normal" => Some(0),
        b"replica" | b"slave" => Some(1),
        b"pubsub" => Some(2),
        _ => None,
    }
}

/// Index for CLIENT_TYPE_PRIMARY (cannot set output buffer limits for primary).
const CLIENT_TYPE_PRIMARY_IDX: usize = usize::MAX; // sentinel; TODO(port): use actual value

/// Thread-local flag: true while parsing a config file (not a CONFIG SET).
///
/// C: static int reading_config_file (config.c:457)
thread_local! {
    static READING_CONFIG_FILE: std::cell::RefCell<bool> = std::cell::RefCell::new(false);
}

// ── Trait impls ───────────────────────────────────────────────────────────────

impl Default for ConfigRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for RewriteConfigState {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/config.c  (3708 lines, ~45 public functions + ~260 config entries)
//                  src/config.h  (417 lines, platform-detection macros only)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         155
//   port_notes:    9
//   unsafe_blocks: 0
//   notes:         Phase A translation complete (2807 lines). All type/flag/enum/struct
//                  declarations are in place: ConfigRegistry, StandardConfig,
//                  ConfigData enum, ConfigInterface, RewriteConfigState, all 20
//                  enum tables. All config-loading (loadServerConfig*), CONFIG SET/GET/
//                  REWRITE/HELP/RESETSTAT commands, rewrite-state helpers, type-interface
//                  functions (bool/string/sds/enum/numeric init/set/get/rewrite),
//                  validation callbacks, and apply callbacks are translated with stubs
//                  where server-field access is needed. The primary open item is
//                  TODO(architect): interior mutability pattern for StandardConfig field
//                  pointers — the C design stores raw `int *` / `char **` pointers into
//                  RedisServer fields, which Phase B must replace with accessor closures
//                  or a ConfigField enum. Static config table (~260 entries) also deferred
//                  to Phase B. Validator output: only expected E0282/E0433/E0425 errors.
// ──────────────────────────────────────────────────────────────────────────────

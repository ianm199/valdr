//! ACL (Access Control List) subsystem.
//!
//! Provides user management, command/key/channel/db permission checking,
//! ACL rule parsing, ACL file loading/saving, and the ACL security log.
//!
//! C source: `src/acl.c` (3504 lines, ~104 functions)
//!
//! PORT NOTE: Global mutable state (Users, DefaultUser, ACLLog, etc.) is
//! collected into `AclState` wrapped in a `Mutex`. In C these are bare
//! global pointers. The Mutex approach is correct for Phase A; Phase 3+
//! may move state inside `RedisServer`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Write as IoWrite};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use redis_types::RedisString;
use redis_types::error::RedisError;

// TODO(architect): need CommandContext from crate::command_context for command handlers.
// TODO(architect): need Client from crate::client for ACL permission checks that receive a client.
// TODO(architect): ServerCommand type (see ServerCommand stub below) needs to
//                  migrate to its canonical owner once server.rs defines it fully.
// TODO(architect): sha2 / sha256 crate dependency needed for ACLHashPassword.

// ─── Constants ────────────────────────────────────────────────────────────────

/// Maximum number of ACL command categories (default + module-defined).
pub const ACL_MAX_CATEGORIES: usize = 64;

/// Length of a SHA-256 password hash in lowercase hex characters.
/// C: `HASH_PASSWORD_LEN = SHA256_BLOCK_SIZE * 2`
pub const HASH_PASSWORD_LEN: usize = 64;

/// Maximum entries in the allowed-command bitmask.
/// TODO(port): confirm from server.h — typically 1024 in Valkey.
pub const USER_COMMAND_BITS_COUNT: usize = 1024;

/// Number of u64 words in the command bitmask (`USER_COMMAND_BITS_COUNT / 64`).
pub const COMMAND_BITS_WORDS: usize = USER_COMMAND_BITS_COUNT / 64;

/// Maximum ACL log grouping window in milliseconds.
const ACL_LOG_GROUPING_MAX_TIME_DELTA: i64 = 60_000;

/// Maximum bits for `ACL GENPASS`.
const GENPASS_MAX_BITS: i64 = 4096;

// ─── ACL result codes ─────────────────────────────────────────────────────────

pub const ACL_OK: i32 = 0;
pub const ACL_DENIED_CMD: i32 = 1;
pub const ACL_DENIED_KEY: i32 = 2;
pub const ACL_DENIED_CHANNEL: i32 = 3;
pub const ACL_DENIED_AUTH: i32 = 4;
pub const ACL_DENIED_DB: i32 = 5;
pub const ACL_INVALID_TLS_CERT_AUTH: i32 = 6;

// ─── Auth result codes ────────────────────────────────────────────────────────

pub const AUTH_OK: i32 = 0;
pub const AUTH_ERR: i32 = 1;
pub const AUTH_BLOCKED: i32 = 2;
pub const AUTH_NOT_HANDLED: i32 = 3;

// ─── ACL log context codes ────────────────────────────────────────────────────

pub const ACL_LOG_CTX_TOPLEVEL: i32 = 0;
pub const ACL_LOG_CTX_MULTI: i32 = 1;
pub const ACL_LOG_CTX_LUA: i32 = 2;
pub const ACL_LOG_CTX_MODULE: i32 = 3;
pub const ACL_LOG_CTX_SCRIPT: i32 = 4;

// ─── Key permission flags ─────────────────────────────────────────────────────

/// Key may be read (`%R~` or `~`).
pub const ACL_READ_PERMISSION: i32 = 1 << 0;
/// Key may be written (`%W~` or `~`).
pub const ACL_WRITE_PERMISSION: i32 = 1 << 1;
/// Full read+write permission (the `~` form).
pub const ACL_ALL_PERMISSION: i32 = ACL_READ_PERMISSION | ACL_WRITE_PERMISSION;

// ─── User flags ───────────────────────────────────────────────────────────────
// TODO(port): confirm bit positions against server.h; these are educated guesses.

pub const USER_FLAG_ENABLED: u64 = 1 << 0;
pub const USER_FLAG_DISABLED: u64 = 1 << 1;
pub const USER_FLAG_NOPASS: u64 = 1 << 2;
pub const USER_FLAG_SANITIZE_PAYLOAD: u64 = 1 << 3;
pub const USER_FLAG_SANITIZE_PAYLOAD_SKIP: u64 = 1 << 4;

// ─── Selector flags ───────────────────────────────────────────────────────────
// TODO(port): confirm bit positions against server.h.

pub const SELECTOR_FLAG_ROOT: u32 = 1 << 0;
pub const SELECTOR_FLAG_ALLKEYS: u32 = 1 << 1;
pub const SELECTOR_FLAG_ALLCOMMANDS: u32 = 1 << 2;
pub const SELECTOR_FLAG_ALLCHANNELS: u32 = 1 << 3;
pub const SELECTOR_FLAG_ALLDBS: u32 = 1 << 4;

// ─── ACL category flags ───────────────────────────────────────────────────────
// TODO(port): confirm bit positions against server.h.

pub const ACL_CATEGORY_KEYSPACE: u64 = 1 << 0;
pub const ACL_CATEGORY_READ: u64 = 1 << 1;
pub const ACL_CATEGORY_WRITE: u64 = 1 << 2;
pub const ACL_CATEGORY_SET: u64 = 1 << 3;
pub const ACL_CATEGORY_SORTEDSET: u64 = 1 << 4;
pub const ACL_CATEGORY_LIST: u64 = 1 << 5;
pub const ACL_CATEGORY_HASH: u64 = 1 << 6;
pub const ACL_CATEGORY_STRING: u64 = 1 << 7;
pub const ACL_CATEGORY_BITMAP: u64 = 1 << 8;
pub const ACL_CATEGORY_HYPERLOGLOG: u64 = 1 << 9;
pub const ACL_CATEGORY_GEO: u64 = 1 << 10;
pub const ACL_CATEGORY_STREAM: u64 = 1 << 11;
pub const ACL_CATEGORY_PUBSUB: u64 = 1 << 12;
pub const ACL_CATEGORY_ADMIN: u64 = 1 << 13;
pub const ACL_CATEGORY_FAST: u64 = 1 << 14;
pub const ACL_CATEGORY_SLOW: u64 = 1 << 15;
pub const ACL_CATEGORY_BLOCKING: u64 = 1 << 16;
pub const ACL_CATEGORY_DANGEROUS: u64 = 1 << 17;
pub const ACL_CATEGORY_CONNECTION: u64 = 1 << 18;
pub const ACL_CATEGORY_TRANSACTION: u64 = 1 << 19;
pub const ACL_CATEGORY_SCRIPTING: u64 = 1 << 20;

// ─── Command flags used in ACL permission checks ──────────────────────────────
// TODO(port): confirm against server.h.

pub const CMD_NO_AUTH: u64 = 1 << 14;
pub const CMD_MODULE: u64 = 1 << 10;
pub const CMD_ALL_DBS: u64 = 1 << 24;

pub const CMD_KEY_ACCESS: i32 = 1 << 0;
pub const CMD_KEY_INSERT: i32 = 1 << 1;
pub const CMD_KEY_DELETE: i32 = 1 << 2;
pub const CMD_KEY_UPDATE: i32 = 1 << 3;

pub const CMD_CHANNEL_PUBLISH: i32 = 1 << 0;
pub const CMD_CHANNEL_SUBSCRIBE: i32 = 1 << 1;
pub const CMD_CHANNEL_PATTERN: i32 = 1 << 2;

// ─── Default ACL category table ──────────────────────────────────────────────

/// Static default command category entries; loaded into `AclState` at `acl_init()`.
/// C: `ACLDefaultCommandCategories[]`
static DEFAULT_COMMAND_CATEGORIES: &[(&[u8], u64)] = &[
    (b"keyspace", ACL_CATEGORY_KEYSPACE),
    (b"read", ACL_CATEGORY_READ),
    (b"write", ACL_CATEGORY_WRITE),
    (b"set", ACL_CATEGORY_SET),
    (b"sortedset", ACL_CATEGORY_SORTEDSET),
    (b"list", ACL_CATEGORY_LIST),
    (b"hash", ACL_CATEGORY_HASH),
    (b"string", ACL_CATEGORY_STRING),
    (b"bitmap", ACL_CATEGORY_BITMAP),
    (b"hyperloglog", ACL_CATEGORY_HYPERLOGLOG),
    (b"geo", ACL_CATEGORY_GEO),
    (b"stream", ACL_CATEGORY_STREAM),
    (b"pubsub", ACL_CATEGORY_PUBSUB),
    (b"admin", ACL_CATEGORY_ADMIN),
    (b"fast", ACL_CATEGORY_FAST),
    (b"slow", ACL_CATEGORY_SLOW),
    (b"blocking", ACL_CATEGORY_BLOCKING),
    (b"dangerous", ACL_CATEGORY_DANGEROUS),
    (b"connection", ACL_CATEGORY_CONNECTION),
    (b"transaction", ACL_CATEGORY_TRANSACTION),
    (b"scripting", ACL_CATEGORY_SCRIPTING),
];

/// User flag name → flag value table used by `ACLDescribeUser`.
/// C: `ACLUserFlags[]`
static USER_FLAGS_TABLE: &[(&[u8], u64)] = &[
    (b"on", USER_FLAG_ENABLED),
    (b"off", USER_FLAG_DISABLED),
    (b"nopass", USER_FLAG_NOPASS),
    (b"skip-sanitize-payload", USER_FLAG_SANITIZE_PAYLOAD_SKIP),
    (b"sanitize-payload", USER_FLAG_SANITIZE_PAYLOAD),
];

// ─── Type definitions ─────────────────────────────────────────────────────────

/// An ACL category entry: human-readable name and its bitmask.
/// C: `struct ACLCategoryItem`
#[derive(Debug, Clone)]
pub struct AclCategoryItem {
    /// Lowercase ASCII name (e.g. `b"keyspace"`).
    pub name: RedisString,
    /// Bitmask bit for this category.
    pub flag: u64,
}

/// A key pattern with associated read/write permission flags.
/// C: `typedef struct { int flags; sds pattern; } keyPattern;`
#[derive(Debug, Clone)]
pub struct KeyPattern {
    /// `ACL_READ_PERMISSION | ACL_WRITE_PERMISSION` combination.
    pub flags: i32,
    /// Glob-style pattern matched against key names (byte string).
    pub pattern: RedisString,
}

/// An ACL selector providing fine-grained permissions.
///
/// Every user has a root selector (created with `SELECTOR_FLAG_ROOT`) plus
/// optional additional selectors created with `(...)` notation.
/// C: `typedef struct { ... } aclSelector;`
#[derive(Debug, Clone)]
pub struct AclSelector {
    /// Behaviour flags (`SELECTOR_FLAG_*`).
    pub flags: u32,
    /// Command bitmask: bit `i` is set if command with id `i` is allowed.
    /// C: `uint64_t allowed_commands[USER_COMMAND_BITS_COUNT / 64]`
    pub allowed_commands: [u64; COMMAND_BITS_WORDS],
    /// Per-command first-argument allowlist.
    /// `allowed_firstargs[cmd_id]` holds Vec of allowed argv[1] values.
    /// `None` when no first-arg restrictions are in effect.
    /// C: `sds **allowed_firstargs`
    pub allowed_firstargs: Option<Vec<Option<Vec<RedisString>>>>,
    /// Allowed key patterns.
    /// C: `list *patterns`
    pub patterns: Vec<KeyPattern>,
    /// Allowed pub/sub channel patterns.
    /// C: `list *channels`
    pub channels: Vec<RedisString>,
    /// Serialised rule string used for display and recomputation.
    /// C: `sds command_rules`
    pub command_rules: RedisString,
    /// Allowed database IDs. Empty (not None) when `ALLDBS` flag is set.
    /// C: `intset *dbs`
    pub dbs: BTreeSet<i64>,
}

/// A Redis ACL user.
/// C: `user` struct (defined in server.h, implemented in acl.c)
#[derive(Debug, Clone)]
pub struct User {
    /// Username — byte string, no spaces or null bytes.
    /// C: `sds name`
    pub name: RedisString,
    /// Behaviour flags (`USER_FLAG_*`).
    /// C: `uint64_t flags`
    pub flags: u64,
    /// SHA-256 hex hashes of accepted passwords.
    /// C: `list *passwords`
    pub passwords: Vec<RedisString>,
    /// Selectors: index 0 is always the root selector.
    /// C: `list *selectors`
    pub selectors: Vec<AclSelector>,
    /// Cached serialised ACL description; invalidated on any mutation.
    /// C: `robj *acl_string`
    pub acl_string: Option<Vec<u8>>,
}

/// An ACL security log entry.
/// C: `typedef struct ACLLogEntry { ... } ACLLogEntry;`
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: i32,
    pub context: i32,
    pub object: RedisString,
    pub username: RedisString,
    pub ctime: i64,
    pub cinfo: RedisString,
    pub entry_id: i64,
    pub timestamp_created: i64,
}

/// Cache used to avoid re-computing key positions across multiple selector checks.
/// C: `typedef struct { int keys_init; getKeysResult keys; } aclKeyResultCache;`
struct AclKeyResultCache {
    initialized: bool,
    /// Cached key references (index + flags pairs).
    /// TODO(port): replace with actual getKeysResult type when server key-specs are ported.
    keys: Vec<KeyRef>,
}

/// A reference to one key in a command's argument vector.
/// C: `keyReference` from server.h.
#[derive(Debug, Clone, Copy)]
pub struct KeyRef {
    /// Argument index into argv.
    pub pos: usize,
    /// Access flags (`CMD_KEY_*`).
    pub flags: i32,
}

/// Stub for the C `serverCommand` type.
/// TODO(architect): replace with canonical type from redis-core::server once server.rs
///                  defines the full command table.
#[derive(Debug, Clone)]
pub struct ServerCommand {
    pub id: u64,
    pub fullname: RedisString,
    pub flags: u64,
    pub acl_categories: u64,
    /// `true` when this is a subcommand.
    pub parent: bool,
    /// Subcommands keyed by lowercase name.
    pub subcommands: HashMap<RedisString, Box<ServerCommand>>,
    // TODO(port): get_dbid_args, proc, arity function pointers
}

/// Stub for the C `ValkeyModule` type.
/// TODO(architect): replace with canonical type from redis-modules crate (Phase 10).
#[derive(Debug)]
pub struct ValkeyModule {
    pub name: RedisString,
}

// ─── Global ACL state ─────────────────────────────────────────────────────────

/// All global mutable ACL state, collected into a single struct.
///
/// PORT NOTE: In C these are bare module-level globals (`Users`, `DefaultUser`,
/// `UsersToLoad`, `ACLLog`, etc.). In Rust they live in a `Mutex<AclState>`
/// accessed via `ACL_STATE`. Phase 3 may move this inside `RedisServer`.
pub struct AclState {
    /// All users, keyed by lowercase username bytes. Iteration order is
    /// lexicographic (BTreeMap mirrors rax's sorted iteration).
    /// C: `rax *Users`
    pub users: BTreeMap<Vec<u8>, User>,

    /// Pending users from valkey.conf, loaded at `acl_load_users_at_startup`.
    /// Each entry is `[username, rule0, rule1, ..., ruleN]`.
    /// C: `list *UsersToLoad`
    pub users_to_load: Vec<Vec<RedisString>>,

    /// ACL security log (newest entry first).
    /// C: `list *ACLLog`
    pub log: Vec<AclLogEntry>,

    /// Total ACL log entries created (used as unique entry id).
    /// C: `long long ACLLogEntryCount`
    pub log_entry_count: i64,

    /// Command-name → command-id mapping (lowercase keys).
    /// C: `static rax *commandId`
    pub command_id_map: HashMap<Vec<u8>, usize>,

    /// Next command id to assign.
    /// C: `static unsigned long nextid`
    pub next_command_id: usize,

    /// Dynamic ACL category table (default + module-defined entries).
    /// C: `static struct ACLCategoryItem *ACLCommandCategories`
    pub command_categories: Vec<AclCategoryItem>,

    /// Global pub/sub default selector flag (ALLCHANNELS vs resetchannels).
    /// C: `server.acl_pubsub_default`
    pub pubsub_default: u32,

    /// Max ACL log length (0 = disabled). C: `server.acllog_max_len`
    pub acllog_max_len: usize,

    /// Path to the ACL file (empty = not configured). C: `server.acl_filename`
    pub acl_filename: Vec<u8>,

    /// Number of databases (for DB-range checks). C: `server.dbnum`
    pub dbnum: i64,
}

impl Default for AclState {
    fn default() -> Self {
        Self {
            users: BTreeMap::new(),
            users_to_load: Vec::new(),
            log: Vec::new(),
            log_entry_count: 0,
            command_id_map: HashMap::new(),
            next_command_id: 0,
            command_categories: Vec::new(),
            pubsub_default: 0,
            acllog_max_len: 128,
            acl_filename: Vec::new(),
            dbnum: 16,
        }
    }
}

/// Module-level ACL state singleton.
pub static ACL_STATE: OnceLock<Mutex<AclState>> = OnceLock::new();

/// Convenience: acquire the ACL state lock, panicking on poison.
/// PORT NOTE: using `unwrap()` on Mutex::lock is acceptable here — a poisoned
/// Mutex means a thread panicked while holding it, which is a fatal condition.
fn acl_state() -> std::sync::MutexGuard<'static, AclState> {
    ACL_STATE
        .get()
        .expect("ACL_STATE not initialized; call acl_init() first")
        .lock()
        .expect("ACL_STATE mutex poisoned")
}

// ─── §1: Helper functions ─────────────────────────────────────────────────────

/// Constant-time byte comparison preventing timing side-channels.
///
/// Returns `0` if the two slices are identical, non-zero otherwise.
/// Both slices must be the same length.
/// C: `static int time_independent_strcmp(char *a, char *b, int len)`
fn time_independent_strcmp(a: &[u8], b: &[u8]) -> u8 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y))
}

/// Compute the SHA-256 hash of `cleartext` and return it as a 64-byte
/// lowercase hex `RedisString`.
/// C: `static sds ACLHashPassword(unsigned char *cleartext, size_t len)`
///
/// TODO(port): requires `sha2` crate (or equivalent). Placeholder returns
/// zeros for now so the module compiles without the crate.
fn acl_hash_password(cleartext: &[u8]) -> RedisString {
    // TODO(port): replace with real sha2::Sha256 computation.
    // C equivalent:
    //   sha256_init(&ctx);
    //   sha256_update(&ctx, cleartext, len);
    //   sha256_final(&ctx, hash);
    //   hex-encode hash into a 64-byte string.
    let _ = cleartext;
    RedisString::from_bytes(b"0000000000000000000000000000000000000000000000000000000000000000")
}

/// Validate that `hash` is a valid 64-char lowercase hex SHA-256 hash.
/// Returns `Ok(())` on success, `Err` on invalid format.
/// C: `static int ACLCheckPasswordHash(unsigned char *hash, int hashlen)`
fn acl_check_password_hash(hash: &[u8]) -> Result<(), AclSetError> {
    if hash.len() != HASH_PASSWORD_LEN {
        return Err(AclSetError::BadMsg);
    }
    for &b in hash {
        if !(b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(AclSetError::BadMsg);
        }
    }
    Ok(())
}

/// Return `true` if the byte slice contains any ASCII whitespace or null bytes.
/// C: `static int ACLStringHasSpaces(const char *s, size_t len)`
pub fn acl_string_has_spaces(s: &[u8]) -> bool {
    s.iter().any(|&b| b == 0 || b.is_ascii_whitespace())
}

/// Return the category flag for the given lowercase name, or `0` if not found.
/// C: `uint64_t ACLGetCommandCategoryFlagByName(const char *name)`
pub fn acl_get_command_category_flag_by_name(name: &[u8]) -> u64 {
    let state = acl_state();
    for cat in &state.command_categories {
        if cat.name.as_bytes().eq_ignore_ascii_case(name) {
            return cat.flag;
        }
    }
    0
}

// ─── §2: Category management ──────────────────────────────────────────────────

/// Add a new ACL command category to the dynamic table.
/// `flag == 0` means "assign the next available bit".
/// Returns `true` on success, `false` when the table is full.
/// C: `int ACLAddCommandCategory(const char *name, uint64_t flag)`
pub fn acl_add_command_category(state: &mut AclState, name: &[u8], flag: u64) -> bool {
    if state.command_categories.len() >= ACL_MAX_CATEGORIES {
        return false;
    }
    let actual_flag = if flag != 0 {
        flag
    } else {
        1u64 << state.command_categories.len()
    };
    state.command_categories.push(AclCategoryItem {
        name: RedisString::from_bytes(name),
        flag: actual_flag,
    });
    true
}

/// Populate the category table with the built-in default categories.
/// C: `void ACLInitCommandCategories(void)`
pub fn acl_init_command_categories(state: &mut AclState) {
    for &(name, flag) in DEFAULT_COMMAND_CATEGORIES {
        let ok = acl_add_command_category(state, name, flag);
        debug_assert!(ok, "ACL category table overflowed during init");
    }
}

/// Remove the last `count` categories from the table.
/// Used to roll back categories added by a module that failed to load.
/// C: `void ACLCleanupCategoriesOnFailure(size_t num_acl_categories_added)`
pub fn acl_cleanup_categories_on_failure(state: &mut AclState, count: usize) {
    let new_len = state.command_categories.len().saturating_sub(count);
    state.command_categories.truncate(new_len);
}

// ─── §3: Selector helpers ─────────────────────────────────────────────────────

impl KeyPattern {
    fn new(pattern: RedisString, flags: i32) -> Self {
        Self { flags, pattern }
    }
}

impl AclSelector {
    /// Create an empty selector with the given flags.
    /// C: `static aclSelector *ACLCreateSelector(int flags)`
    fn create(flags: u32, pubsub_default: u32) -> Self {
        Self {
            flags: flags | pubsub_default | SELECTOR_FLAG_ALLDBS,
            allowed_commands: [0u64; COMMAND_BITS_WORDS],
            allowed_firstargs: None,
            patterns: Vec::new(),
            channels: Vec::new(),
            command_rules: RedisString::new(),
            dbs: BTreeSet::new(),
        }
    }

    /// Reset the first-arg allowlist for a specific command id.
    /// C: `static void ACLResetFirstArgsForCommand(aclSelector *selector, unsigned long id)`
    fn reset_first_args_for_command(&mut self, id: usize) {
        if let Some(ref mut fa) = self.allowed_firstargs {
            if id < fa.len() {
                fa[id] = None;
            }
        }
    }

    /// Reset the entire first-arg allowlist.
    /// C: `static void ACLResetFirstArgs(aclSelector *selector)`
    fn reset_first_args(&mut self) {
        self.allowed_firstargs = None;
    }

    /// Add a first-arg allowlist entry for command `id`.
    /// C: `static void ACLAddAllowedFirstArg(aclSelector *selector, unsigned long id, const char *sub)`
    fn add_allowed_first_arg(&mut self, id: usize, sub: &[u8]) {
        let fa = self.allowed_firstargs.get_or_insert_with(|| {
            vec![None; USER_COMMAND_BITS_COUNT]
        });
        if id >= fa.len() {
            fa.resize(id + 1, None);
        }
        let slot = fa[id].get_or_insert_with(Vec::new);
        // Avoid duplicates (case-insensitive).
        if slot.iter().any(|existing: &RedisString| existing.as_bytes().eq_ignore_ascii_case(sub)) {
            return;
        }
        slot.push(RedisString::from_bytes(sub));
    }

    /// Get the command bit at position `id`.
    /// C: `static int ACLGetSelectorCommandBit(const aclSelector *selector, unsigned long id)`
    fn get_command_bit(&self, id: usize) -> bool {
        if id >= USER_COMMAND_BITS_COUNT {
            return false;
        }
        let word = id / 64;
        let bit = 1u64 << (id % 64);
        (self.allowed_commands[word] & bit) != 0
    }

    /// Set or clear the command bit at position `id`.
    /// C: `static void ACLSetSelectorCommandBit(aclSelector *selector, unsigned long id, int value)`
    fn set_command_bit(&mut self, id: usize, value: bool) {
        if id >= USER_COMMAND_BITS_COUNT {
            return;
        }
        let word = id / 64;
        let bit = 1u64 << (id % 64);
        if value {
            self.allowed_commands[word] |= bit;
        } else {
            self.allowed_commands[word] &= !bit;
            self.flags &= !SELECTOR_FLAG_ALLCOMMANDS;
        }
    }

    /// Check whether the selector allows future (not-yet-loaded) commands.
    /// C: `static int ACLSelectorCanExecuteFutureCommands(aclSelector *selector)`
    fn can_execute_future_commands(&self) -> bool {
        self.get_command_bit(USER_COMMAND_BITS_COUNT - 1)
    }

    /// Build the canonical text representation of key/channel/db permissions.
    /// C: `static sds ACLDescribeSelector(aclSelector *selector)` (partial; command rules appended)
    fn describe(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        // Key patterns.
        if self.flags & SELECTOR_FLAG_ALLKEYS != 0 {
            res.extend_from_slice(b"~* ");
        } else {
            for pat in &self.patterns {
                cat_pattern_string(&mut res, pat);
                res.push(b' ');
            }
        }

        // Pub/sub channel patterns.
        if self.flags & SELECTOR_FLAG_ALLCHANNELS != 0 {
            res.extend_from_slice(b"&* ");
        } else {
            res.extend_from_slice(b"resetchannels ");
            for chan in &self.channels {
                res.push(b'&');
                res.extend_from_slice(chan.as_bytes());
                res.push(b' ');
            }
        }

        // Database permissions.
        if self.flags & SELECTOR_FLAG_ALLDBS != 0 {
            res.extend_from_slice(b"alldbs ");
        } else if self.dbs.is_empty() {
            res.extend_from_slice(b"resetdbs ");
        } else {
            res.extend_from_slice(b"db=");
            for (i, dbid) in self.dbs.iter().enumerate() {
                if i > 0 {
                    res.push(b',');
                }
                res.extend_from_slice(dbid.to_string().as_bytes());
            }
            res.push(b' ');
        }

        // Command rules (built separately).
        let cmd_rules = self.describe_command_rules();
        res.extend_from_slice(&cmd_rules);

        res
    }

    /// Build the command rules portion of an ACL selector description.
    /// C: `static sds ACLDescribeSelectorCommandRules(aclSelector *selector)`
    ///
    /// TODO(port): The sanity-check in C (`memcmp` of bitmaps) is omitted here
    /// because it requires running ACLSetSelector on a fake selector. Add back
    /// once SetSelector is stable.
    fn describe_command_rules(&self) -> Vec<u8> {
        let mut rules: Vec<u8> = Vec::new();
        if self.can_execute_future_commands() {
            rules.extend_from_slice(b"+@all");
        } else {
            rules.extend_from_slice(b"-@all");
        }
        if !self.command_rules.as_bytes().is_empty() {
            rules.push(b' ');
            rules.extend_from_slice(self.command_rules.as_bytes());
        }
        rules
    }

    /// Remove a rule from `command_rules` by exact (or prefix-subcommand) match.
    /// C: `static void ACLSelectorRemoveCommandRule(aclSelector *selector, sds new_rule)`
    fn remove_command_rule(&mut self, new_rule: &[u8]) {
        // Work on a copy, then replace in-place.
        let existing = self.command_rules.as_bytes().to_vec();
        let mut result: Vec<u8> = Vec::with_capacity(existing.len());
        let mut i = 0usize;
        while i < existing.len() {
            // Each rule starts with +/-.
            let rule_start = i;
            i += 1; // skip +/-
            let rule_name_start = i;

            // Find end of this rule (space or end of string).
            let space_pos = existing[i..].iter().position(|&b| b == b' ');
            let rule_end = space_pos.map(|p| i + p).unwrap_or(existing.len());
            let rule_name = &existing[rule_name_start..rule_end];

            // Check match: exact or this is a subcommand of new_rule.
            let is_match = if rule_name.len() == new_rule.len() {
                rule_name.eq_ignore_ascii_case(new_rule)
            } else if rule_name.len() > new_rule.len()
                && rule_name[..new_rule.len()].eq_ignore_ascii_case(new_rule)
                && rule_name[new_rule.len()] == b'|'
            {
                true
            } else {
                false
            };

            let copy_end = if rule_end < existing.len() { rule_end + 1 } else { rule_end };

            if is_match {
                // Skip this rule; if it was the last and result is non-empty,
                // we may have a trailing space — trimmed below.
            } else {
                result.extend_from_slice(&existing[rule_start..copy_end]);
            }

            i = copy_end;
        }
        // Trim trailing space.
        while result.last() == Some(&b' ') {
            result.pop();
        }
        self.command_rules = RedisString::from_bytes(&result);
    }

    /// Update `command_rules` to reflect allowing/denying `rule`.
    /// C: `static void ACLUpdateCommandRules(aclSelector *selector, const char *rule, int allow)`
    fn update_command_rules(&mut self, rule: &[u8], allow: bool) {
        let lower: Vec<u8> = rule.iter().map(|b| b.to_ascii_lowercase()).collect();
        self.remove_command_rule(&lower);
        if !self.command_rules.as_bytes().is_empty() {
            let mut cr = self.command_rules.as_bytes().to_vec();
            cr.push(b' ');
            cr.push(if allow { b'+' } else { b'-' });
            cr.extend_from_slice(&lower);
            self.command_rules = RedisString::from_bytes(&cr);
        } else {
            let mut cr = Vec::new();
            cr.push(if allow { b'+' } else { b'-' });
            cr.extend_from_slice(&lower);
            self.command_rules = RedisString::from_bytes(&cr);
        }
    }

    /// Set or clear database permissions from a comma-separated list of IDs.
    /// C: `static int ACLSetSelectorDatabasePermissions(aclSelector *selector, const char *dbs_str)`
    fn set_database_permissions(&mut self, dbs_str: &[u8]) -> Result<(), AclSetError> {
        if dbs_str.is_empty() || dbs_str.first() == Some(&b',') || dbs_str.last() == Some(&b',') {
            return Err(AclSetError::InvalidSyntax);
        }
        let mut new_dbs: BTreeSet<i64> = BTreeSet::new();
        for token in dbs_str.split(|&b| b == b',') {
            if token.is_empty() {
                return Err(AclSetError::InvalidSyntax);
            }
            // Parse decimal integer from bytes.
            let s = token;
            let dbid = parse_i64_bytes(s).ok_or(AclSetError::OutOfRange)?;
            if dbid < 0 || dbid > i64::from(i32::MAX) {
                return Err(AclSetError::OutOfRange);
            }
            new_dbs.insert(dbid);
        }
        self.flags &= !SELECTOR_FLAG_ALLDBS;
        self.dbs = new_dbs;
        Ok(())
    }

    /// Check whether this selector allows access to `dbid`.
    /// C: `static inline int ACLSelectorCanAccessDb(aclSelector *selector, long long dbid)`
    fn can_access_db(&self, dbid: i64, total_dbs: i64) -> bool {
        if self.flags & SELECTOR_FLAG_ALLDBS != 0 {
            return true;
        }
        if dbid < 0 || dbid >= total_dbs {
            return false;
        }
        self.dbs.contains(&dbid)
    }

    /// Check whether the selector allows `key` access with the given flags.
    /// C: `static int ACLSelectorCheckKey(aclSelector *selector, const char *key, int keylen, int keyspec_flags, bool is_prefix)`
    fn check_key(&self, key: &[u8], keyspec_flags: i32, is_prefix: bool) -> i32 {
        if self.flags & SELECTOR_FLAG_ALLKEYS != 0 {
            return ACL_OK;
        }
        let mut key_flags = 0i32;
        if keyspec_flags & CMD_KEY_ACCESS != 0 {
            key_flags |= ACL_READ_PERMISSION;
        }
        if keyspec_flags & (CMD_KEY_INSERT | CMD_KEY_DELETE | CMD_KEY_UPDATE) != 0 {
            key_flags |= ACL_WRITE_PERMISSION;
        }
        for pat in &self.patterns {
            if (pat.flags & key_flags) != key_flags {
                continue;
            }
            let pattern = pat.pattern.as_bytes();
            let matched = if is_prefix {
                // TODO(port): implement prefixmatchlen equivalent.
                prefix_match_len(pattern, key, false)
            } else {
                string_match_len(pattern, key, false)
            };
            if matched {
                return ACL_OK;
            }
        }
        ACL_DENIED_KEY
    }

    /// Check whether this selector has unrestricted access with the given flags.
    /// C: `static int ACLSelectorHasUnrestrictedKeyAccess(aclSelector *selector, int flags)`
    fn has_unrestricted_key_access(&self, flags: i32) -> bool {
        if self.flags & SELECTOR_FLAG_ALLKEYS != 0 {
            return true;
        }
        let mut access_flags = 0i32;
        if flags & CMD_KEY_ACCESS != 0 {
            access_flags |= ACL_READ_PERMISSION;
        }
        if flags & (CMD_KEY_INSERT | CMD_KEY_DELETE | CMD_KEY_UPDATE) != 0 {
            access_flags |= ACL_WRITE_PERMISSION;
        }
        for pat in &self.patterns {
            if (pat.flags & access_flags) != access_flags {
                continue;
            }
            if pat.pattern.as_bytes() == b"*" {
                return true;
            }
        }
        false
    }
}

// ─── §4: User helpers ─────────────────────────────────────────────────────────

impl User {
    /// Create a new user with the given name, default-disabled.
    /// C: `static user *ACLCreateUser(const char *name, size_t namelen)` (partial)
    fn new(name: &[u8]) -> Self {
        let root = AclSelector::create(SELECTOR_FLAG_ROOT, 0);
        Self {
            name: RedisString::from_bytes(name),
            flags: USER_FLAG_DISABLED | USER_FLAG_SANITIZE_PAYLOAD,
            passwords: Vec::new(),
            selectors: vec![root],
            acl_string: None,
        }
    }

    /// Get a reference to the root (first) selector.
    /// C: `static aclSelector *ACLUserGetRootSelector(user *u)`
    pub fn root_selector(&self) -> &AclSelector {
        debug_assert!(!self.selectors.is_empty(), "user has no root selector");
        debug_assert!(
            self.selectors[0].flags & SELECTOR_FLAG_ROOT != 0,
            "first selector is not root"
        );
        &self.selectors[0]
    }

    /// Get a mutable reference to the root (first) selector.
    pub fn root_selector_mut(&mut self) -> &mut AclSelector {
        debug_assert!(!self.selectors.is_empty(), "user has no root selector");
        &mut self.selectors[0]
    }

    /// Produce the full ACL description string for this user.
    /// C: `robj *ACLDescribeUser(user *u)`
    pub fn describe(&mut self) -> Vec<u8> {
        if let Some(ref cached) = self.acl_string {
            return cached.clone();
        }
        let mut res: Vec<u8> = Vec::new();

        // Flags.
        for &(name, flag) in USER_FLAGS_TABLE {
            if self.flags & flag != 0 {
                res.extend_from_slice(name);
                res.push(b' ');
            }
        }

        // Passwords.
        for pass in &self.passwords {
            res.push(b'#');
            res.extend_from_slice(pass.as_bytes());
            res.push(b' ');
        }

        // Selectors.
        for (i, selector) in self.selectors.iter().enumerate() {
            let desc = selector.describe();
            if selector.flags & SELECTOR_FLAG_ROOT != 0 {
                res.extend_from_slice(&desc);
            } else {
                res.push(b'(');
                res.extend_from_slice(&desc);
                res.push(b')');
            }
            if i + 1 < self.selectors.len() {
                res.push(b' ');
            }
        }

        self.acl_string = Some(res.clone());
        res
    }

    /// Invalidate the cached `acl_string`.
    fn invalidate_cache(&mut self) {
        self.acl_string = None;
    }
}

// ─── §5: Error type for ACL set operations ────────────────────────────────────

/// Fine-grained error codes from ACL set operations.
/// These drive `acl_set_user_string_error()` error messages.
/// C: errno values set by ACLSetUser / ACLSetSelector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclSetError {
    /// Unknown command or category (ENOENT).
    NotFound,
    /// Invalid syntax or pattern (EINVAL).
    InvalidSyntax,
    /// Adding a key pattern after `*` was already added (EEXIST).
    KeyPatternAfterStar,
    /// Adding a channel pattern after `*` was already added (EISDIR).
    ChanPatternAfterStar,
    /// Password to remove not found (ENODEV).
    PasswordNotFound,
    /// Invalid hash format (EBADMSG).
    BadMsg,
    /// Duplicate user (EALREADY).
    DuplicateUser,
    /// First-arg of a subcommand (ECHILD).
    SubcommandFirstArg,
    /// DB id out of range (ERANGE).
    OutOfRange,
}

impl AclSetError {
    /// Human-readable error message matching the C ACLSetUserStringError() output.
    pub fn message(self) -> &'static [u8] {
        match self {
            AclSetError::NotFound => b"Unknown command or category name in ACL",
            AclSetError::InvalidSyntax => b"Syntax error",
            AclSetError::KeyPatternAfterStar =>
                b"Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid \
                  and does not have any effect. Try 'resetkeys' to start with an empty list of patterns",
            AclSetError::ChanPatternAfterStar =>
                b"Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid \
                  and does not have any effect. Try 'resetchannels' to start with an empty list of channels",
            AclSetError::PasswordNotFound =>
                b"The password you are trying to remove from the user does not exist",
            AclSetError::BadMsg =>
                b"The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters",
            AclSetError::DuplicateUser =>
                b"Duplicate user found. A user can only be defined once in config files",
            AclSetError::SubcommandFirstArg =>
                b"Allowing first-arg of a subcommand is not supported",
            AclSetError::OutOfRange =>
                b"The provided database ID is out of range",
        }
    }
}

// ─── §6: Glob / prefix matching stubs ────────────────────────────────────────

/// Glob-style pattern match (case-sensitive).
/// C: `stringmatchlen(pattern, plen, string, slen, nocase)`
///
/// TODO(port): implement full glob semantics (*, ?, [...]).
/// Currently only handles the trivial `*` pattern.
/// PERF(port): C uses a recursive implementation; a Rust iterative version will be faster.
fn string_match_len(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    // TODO(port): full glob implementation needed.
    if pattern == b"*" {
        return true;
    }
    if nocase {
        pattern.eq_ignore_ascii_case(string)
    } else {
        pattern == string
    }
}

/// Prefix-glob match: returns `true` if any prefix of `string` matches `pattern`.
/// C: `prefixmatchlen(pattern, plen, string, slen, nocase)`
///
/// TODO(port): implement full prefix-glob semantics.
fn prefix_match_len(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    // TODO(port): full prefix-glob implementation needed.
    string_match_len(pattern, string, nocase)
}

// ─── §7: Pattern string builder ───────────────────────────────────────────────

/// Append the canonical text form of `pat` to `base`.
/// C: `static sds sdsCatPatternString(sds base, keyPattern *pat)`
fn cat_pattern_string(base: &mut Vec<u8>, pat: &KeyPattern) {
    if pat.flags == ACL_ALL_PERMISSION {
        base.push(b'~');
    } else if pat.flags == ACL_READ_PERMISSION {
        base.extend_from_slice(b"%R~");
    } else if pat.flags == ACL_WRITE_PERMISSION {
        base.extend_from_slice(b"%W~");
    } else {
        // TODO(architect): is panic correct here? The C code calls serverPanic.
        debug_assert!(false, "invalid key pattern flag");
    }
    base.extend_from_slice(pat.pattern.as_bytes());
}

// ─── §8: Integer parsing helper ──────────────────────────────────────────────

/// Parse a decimal integer from ASCII bytes, returning `None` on parse error.
fn parse_i64_bytes(s: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(s).ok()?;
    s.parse::<i64>().ok()
}

// ─── §9: Selector set operation ──────────────────────────────────────────────

/// Apply a single ACL operation string to a selector.
///
/// C: `static int ACLSetSelector(aclSelector *selector, const char *op, size_t oplen)`
///
/// Supported operations (documented in detail in the C source):
/// - `allkeys`, `~*`, `resetkeys`
/// - `allchannels`, `&*`, `resetchannels`
/// - `alldbs`, `resetdbs`, `db=<ids>`
/// - `allcommands`, `+@all`, `nocommands`, `-@all`
/// - `~<pattern>`, `%R~<pattern>`, `%W~<pattern>`
/// - `&<pattern>`
/// - `+<cmd>`, `-<cmd>`, `+@<cat>`, `-@<cat>`
/// - `+<cmd>|<firstarg>`
pub fn acl_set_selector(
    selector: &mut AclSelector,
    op: &[u8],
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) -> Result<(), AclSetError> {
    if op.eq_ignore_ascii_case(b"allkeys") || op == b"~*" {
        selector.flags |= SELECTOR_FLAG_ALLKEYS;
        selector.patterns.clear();
    } else if op.eq_ignore_ascii_case(b"resetkeys") {
        selector.flags &= !SELECTOR_FLAG_ALLKEYS;
        selector.patterns.clear();
    } else if op.eq_ignore_ascii_case(b"allchannels") || op == b"&*" {
        selector.flags |= SELECTOR_FLAG_ALLCHANNELS;
        selector.channels.clear();
    } else if op.eq_ignore_ascii_case(b"resetchannels") {
        selector.flags &= !SELECTOR_FLAG_ALLCHANNELS;
        selector.channels.clear();
    } else if op.eq_ignore_ascii_case(b"alldbs") {
        selector.flags |= SELECTOR_FLAG_ALLDBS;
        selector.dbs.clear();
    } else if op.eq_ignore_ascii_case(b"resetdbs") {
        selector.flags &= !SELECTOR_FLAG_ALLDBS;
        selector.dbs.clear();
    } else if op.len() > 3 && op[..3].eq_ignore_ascii_case(b"db=") {
        selector.set_database_permissions(&op[3..])?;
    } else if op.eq_ignore_ascii_case(b"allcommands") || op.eq_ignore_ascii_case(b"+@all") {
        selector.allowed_commands = [0xFFFF_FFFF_FFFF_FFFFu64; COMMAND_BITS_WORDS];
        selector.flags |= SELECTOR_FLAG_ALLCOMMANDS;
        selector.command_rules = RedisString::new();
        selector.reset_first_args();
    } else if op.eq_ignore_ascii_case(b"nocommands") || op.eq_ignore_ascii_case(b"-@all") {
        selector.allowed_commands = [0u64; COMMAND_BITS_WORDS];
        selector.flags &= !SELECTOR_FLAG_ALLCOMMANDS;
        selector.command_rules = RedisString::new();
        selector.reset_first_args();
    } else if op.first() == Some(&b'~') || op.first() == Some(&b'%') {
        if selector.flags & SELECTOR_FLAG_ALLKEYS != 0 {
            return Err(AclSetError::KeyPatternAfterStar);
        }
        let (flags, pat_offset) = if op[0] == b'%' {
            parse_key_permission_prefix(op)?
        } else {
            (ACL_ALL_PERMISSION, 1usize)
        };
        let pattern_bytes = &op[pat_offset..];
        if acl_string_has_spaces(pattern_bytes) {
            return Err(AclSetError::InvalidSyntax);
        }
        let new_pat = KeyPattern::new(RedisString::from_bytes(pattern_bytes), flags);
        if let Some(existing) = selector.patterns.iter_mut().find(|p| p.pattern == new_pat.pattern) {
            existing.flags |= flags;
        } else {
            selector.patterns.push(new_pat);
        }
        selector.flags &= !SELECTOR_FLAG_ALLKEYS;
    } else if op.first() == Some(&b'&') {
        if selector.flags & SELECTOR_FLAG_ALLCHANNELS != 0 {
            return Err(AclSetError::ChanPatternAfterStar);
        }
        let pattern_bytes = &op[1..];
        if acl_string_has_spaces(pattern_bytes) {
            return Err(AclSetError::InvalidSyntax);
        }
        let new_pat = RedisString::from_bytes(pattern_bytes);
        if !selector.channels.contains(&new_pat) {
            selector.channels.push(new_pat);
        }
        selector.flags &= !SELECTOR_FLAG_ALLCHANNELS;
    } else if op.first() == Some(&b'+') && op.get(1) != Some(&b'@') {
        // Allow command or command|firstarg.
        let cmd_part = &op[1..];
        if !cmd_part.contains(&b'|') {
            // Simple command allow.
            let cmd = cmd_lookup(cmd_part).ok_or(AclSetError::NotFound)?;
            acl_change_selector_perm(selector, &cmd, true);
            selector.update_command_rules(cmd.fullname.as_bytes(), true);
        } else {
            // Allow with first-arg restriction: `+cmd|firstarg`.
            let sep = cmd_part.iter().rposition(|&b| b == b'|').unwrap();
            let base_name = &cmd_part[..sep];
            let first_arg = &cmd_part[sep + 1..];

            let cmd = cmd_lookup(base_name).ok_or(AclSetError::NotFound)?;
            if cmd.parent {
                return Err(AclSetError::SubcommandFirstArg);
            }
            if first_arg.is_empty() {
                return Err(AclSetError::InvalidSyntax);
            }

            if !cmd.subcommands.is_empty() {
                // Treat as a real subcommand.
                let sub_cmd = cmd_lookup(cmd_part).ok_or(AclSetError::NotFound)?;
                acl_change_selector_perm(selector, &sub_cmd, true);
            } else {
                // Legacy first-arg allowlist mechanism.
                log::warn!(
                    "Deprecation warning: Allowing a first arg of an otherwise blocked command \
                     is a misuse of ACL and may get disabled in the future (offender: +{})",
                    // Safety: only used for logging; non-UTF-8 shows as replacement char.
                    String::from_utf8_lossy(cmd_part)
                );
                selector.add_allowed_first_arg(cmd.id as usize, first_arg);
            }
            selector.update_command_rules(cmd_part, true);
        }
    } else if op.first() == Some(&b'-') && op.get(1) != Some(&b'@') {
        // Deny command.
        let cmd_part = &op[1..];
        let cmd = cmd_lookup(cmd_part).ok_or(AclSetError::NotFound)?;
        acl_change_selector_perm(selector, &cmd, false);
        selector.update_command_rules(cmd.fullname.as_bytes(), false);
    } else if (op.first() == Some(&b'+') || op.first() == Some(&b'-')) && op.get(1) == Some(&b'@') {
        // Category allow/deny.
        let allow = op[0] == b'+';
        let cat_name = &op[2..]; // skip +@ or -@
        let cflag = acl_get_command_category_flag_by_name(cat_name);
        if cflag == 0 {
            return Err(AclSetError::NotFound);
        }
        selector.update_command_rules(
            // Pass the full `@category` token as the rule name.
            &op[1..], // strip leading +/- for the rule name
            allow,
        );
        acl_set_selector_command_bits_for_category(selector, orig_commands, cflag, allow);
    } else {
        return Err(AclSetError::InvalidSyntax);
    }
    Ok(())
}

/// Parse the `%R~`, `%W~`, or `%RW~` permission prefix from a key-pattern op.
/// Returns `(flags, byte_offset_to_pattern)` on success.
fn parse_key_permission_prefix(op: &[u8]) -> Result<(i32, usize), AclSetError> {
    let mut flags = 0i32;
    let mut offset = 1usize; // skip leading '%'
    loop {
        if offset >= op.len() {
            return Err(AclSetError::InvalidSyntax);
        }
        match op[offset].to_ascii_uppercase() {
            b'R' if flags & ACL_READ_PERMISSION == 0 => {
                flags |= ACL_READ_PERMISSION;
                offset += 1;
            }
            b'W' if flags & ACL_WRITE_PERMISSION == 0 => {
                flags |= ACL_WRITE_PERMISSION;
                offset += 1;
            }
            b'~' => {
                offset += 1;
                break;
            }
            _ => return Err(AclSetError::InvalidSyntax),
        }
    }
    if flags == 0 {
        return Err(AclSetError::InvalidSyntax);
    }
    Ok((flags, offset))
}

/// Allow or deny a command (and all its subcommands) in a selector.
/// C: `static void ACLChangeSelectorPerm(aclSelector *selector, struct serverCommand *cmd, int allow)`
pub fn acl_change_selector_perm(selector: &mut AclSelector, cmd: &ServerCommand, allow: bool) {
    selector.set_command_bit(cmd.id as usize, allow);
    selector.reset_first_args_for_command(cmd.id as usize);
    for sub in cmd.subcommands.values() {
        selector.set_command_bit(sub.id as usize, allow);
    }
}

/// Set command bits for all commands in the given category.
/// C: `static void ACLSetSelectorCommandBitsForCategory(...)`
pub fn acl_set_selector_command_bits_for_category(
    selector: &mut AclSelector,
    commands: &HashMap<RedisString, Box<ServerCommand>>,
    cflag: u64,
    allow: bool,
) {
    for cmd in commands.values() {
        if cmd.acl_categories & cflag != 0 {
            acl_change_selector_perm(selector, cmd, allow);
        }
        // Recurse into subcommands.
        for sub in cmd.subcommands.values() {
            if sub.acl_categories & cflag != 0 {
                acl_change_selector_perm(selector, sub, allow);
            }
        }
    }
}

// ─── §10: User set operation ──────────────────────────────────────────────────

/// Apply a single ACL operation to a `User`.
///
/// C: `int ACLSetUser(user *u, const char *op, ssize_t oplen)`
pub fn acl_set_user(
    user: &mut User,
    op: &[u8],
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
    pubsub_default: u32,
) -> Result<(), AclSetError> {
    user.invalidate_cache();
    if op.is_empty() {
        return Ok(());
    }
    if op.eq_ignore_ascii_case(b"on") {
        user.flags |= USER_FLAG_ENABLED;
        user.flags &= !USER_FLAG_DISABLED;
    } else if op.eq_ignore_ascii_case(b"off") {
        user.flags |= USER_FLAG_DISABLED;
        user.flags &= !USER_FLAG_ENABLED;
    } else if op.eq_ignore_ascii_case(b"skip-sanitize-payload") {
        user.flags |= USER_FLAG_SANITIZE_PAYLOAD_SKIP;
        user.flags &= !USER_FLAG_SANITIZE_PAYLOAD;
    } else if op.eq_ignore_ascii_case(b"sanitize-payload") {
        user.flags &= !USER_FLAG_SANITIZE_PAYLOAD_SKIP;
        user.flags |= USER_FLAG_SANITIZE_PAYLOAD;
    } else if op.eq_ignore_ascii_case(b"nopass") {
        user.flags |= USER_FLAG_NOPASS;
        user.passwords.clear();
    } else if op.eq_ignore_ascii_case(b"resetpass") {
        user.flags &= !USER_FLAG_NOPASS;
        user.passwords.clear();
    } else if op.first() == Some(&b'>') || op.first() == Some(&b'#') {
        // Add password: `>cleartext` or `#hash`.
        let new_pass = if op[0] == b'>' {
            acl_hash_password(&op[1..])
        } else {
            acl_check_password_hash(&op[1..])?;
            RedisString::from_bytes(&op[1..])
        };
        if !user.passwords.contains(&new_pass) {
            user.passwords.push(new_pass);
        }
        user.flags &= !USER_FLAG_NOPASS;
    } else if op.first() == Some(&b'<') || op.first() == Some(&b'!') {
        // Remove password: `<cleartext` or `!hash`.
        let del_pass = if op[0] == b'<' {
            acl_hash_password(&op[1..])
        } else {
            acl_check_password_hash(&op[1..])?;
            RedisString::from_bytes(&op[1..])
        };
        let pos = user.passwords.iter().position(|p| *p == del_pass);
        match pos {
            Some(i) => { user.passwords.remove(i); }
            None => return Err(AclSetError::PasswordNotFound),
        }
    } else if op.first() == Some(&b'(') && op.last() == Some(&b')') {
        // New sub-selector: `(<options>)`.
        let inner = &op[1..op.len() - 1];
        let mut selector = AclSelector::create(0, pubsub_default);
        for part in split_acl_args(inner) {
            acl_set_selector(&mut selector, part.as_bytes(), cmd_lookup, orig_commands)?;
        }
        user.selectors.push(selector);
    } else if op.eq_ignore_ascii_case(b"clearselectors") {
        // Keep only the root selector.
        debug_assert!(!user.selectors.is_empty(), "user must have root selector");
        user.selectors.truncate(1);
    } else if op.eq_ignore_ascii_case(b"reset") {
        // Reset to freshly-created state.
        acl_set_user(user, b"resetpass", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"resetkeys", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"resetchannels", cmd_lookup, orig_commands, pubsub_default)?;
        if pubsub_default & SELECTOR_FLAG_ALLCHANNELS != 0 {
            acl_set_user(user, b"allchannels", cmd_lookup, orig_commands, pubsub_default)?;
        }
        acl_set_user(user, b"alldbs", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"off", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"sanitize-payload", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"clearselectors", cmd_lookup, orig_commands, pubsub_default)?;
        acl_set_user(user, b"-@all", cmd_lookup, orig_commands, pubsub_default)?;
    } else {
        // Delegate to root selector for all other ops.
        let root = user.root_selector_mut();
        acl_set_selector(root, op, cmd_lookup, orig_commands)?;
    }
    Ok(())
}

/// Tokenize ACL argument string (respects quoted tokens).
/// C: `sdssplitargs` / `sdsnsplitargs`
///
/// TODO(port): implement full sdssplitargs semantics including quoting.
fn split_acl_args(s: &[u8]) -> Vec<RedisString> {
    s.split(|&b| b == b' ')
        .filter(|t| !t.is_empty())
        .map(RedisString::from_bytes)
        .collect()
}

// ─── §11: User management ─────────────────────────────────────────────────────

/// Create a new user in the global user table, returning `Err` if name is taken.
/// C: `static user *ACLCreateUser(const char *name, size_t namelen)`
pub fn acl_create_user(state: &mut AclState, name: &[u8]) -> Result<(), ()> {
    let key = name.to_ascii_lowercase();
    if state.users.contains_key(&key) {
        return Err(());
    }
    state.users.insert(key, User::new(name));
    Ok(())
}

/// Look up a user by name, returning a reference or `None`.
/// C: `user *ACLGetUserByName(const char *name, size_t namelen)`
pub fn acl_get_user_by_name<'a>(state: &'a AclState, name: &[u8]) -> Option<&'a User> {
    state.users.get(&name.to_ascii_lowercase())
}

/// Look up a user by name (mutable), returning a reference or `None`.
pub fn acl_get_user_by_name_mut<'a>(state: &'a mut AclState, name: &[u8]) -> Option<&'a mut User> {
    state.users.get_mut(&name.to_ascii_lowercase())
}

/// Create the default user with all permissions open.
/// C: `static user *ACLCreateDefaultUser(void)`
fn acl_create_default_user(state: &mut AclState) {
    let _ = acl_create_user(state, b"default");
    // Safe: we just created it.
    let u = state.users.get_mut(b"default".as_ref()).unwrap();
    // Apply default user settings — we can't call acl_set_user without
    // a cmd_lookup, so set flags directly.
    // TODO(port): wire up cmd_lookup to apply "+@all ~* &* on nopass alldbs" properly.
    u.flags = USER_FLAG_ENABLED | USER_FLAG_NOPASS;
    let root = u.root_selector_mut();
    root.flags |= SELECTOR_FLAG_ALLKEYS | SELECTOR_FLAG_ALLCOMMANDS | SELECTOR_FLAG_ALLCHANNELS | SELECTOR_FLAG_ALLDBS;
    root.allowed_commands = [0xFFFF_FFFF_FFFF_FFFFu64; COMMAND_BITS_WORDS];
}

/// Initialize the ACL subsystem.
/// C: `void ACLInit(void)`
pub fn acl_init() {
    let mut state = AclState::default();
    acl_init_command_categories(&mut state);
    acl_create_default_user(&mut state);
    ACL_STATE
        .set(Mutex::new(state))
        .ok(); // idempotent in tests
}

/// Get the next available command id, reusing ids for known names.
/// C: `unsigned long ACLGetCommandID(sds cmdname)`
pub fn acl_get_command_id(state: &mut AclState, cmdname: &[u8]) -> usize {
    let lower = cmdname.to_ascii_lowercase();
    if let Some(&id) = state.command_id_map.get(&lower) {
        return id;
    }
    let id = state.next_command_id;
    state.command_id_map.insert(lower, id);
    state.next_command_id += 1;
    // Reserve the last bit for the "future commands" sentinel.
    if state.next_command_id == USER_COMMAND_BITS_COUNT - 1 {
        state.next_command_id += 1;
    }
    id
}

// ─── §12: Credential checking ─────────────────────────────────────────────────

/// Check username + password against the user table.
///
/// Returns `Ok(())` on success.
/// On failure returns:
/// - `Err(AclSetError::NotFound)` — no such user.
/// - `Err(AclSetError::InvalidSyntax)` — user is disabled or password mismatch.
///
/// C: `int ACLCheckUserCredentials(robj *username, robj *password)`
pub fn acl_check_user_credentials(state: &AclState, username: &[u8], password: &[u8]) -> Result<(), AclSetError> {
    let u = state.users.get(&username.to_ascii_lowercase()).ok_or(AclSetError::NotFound)?;
    if u.flags & USER_FLAG_DISABLED != 0 {
        return Err(AclSetError::InvalidSyntax);
    }
    if u.flags & USER_FLAG_NOPASS != 0 {
        return Ok(());
    }
    let hashed = acl_hash_password(password);
    for stored in &u.passwords {
        if time_independent_strcmp(hashed.as_bytes(), stored.as_bytes()) == 0 {
            return Ok(());
        }
    }
    Err(AclSetError::InvalidSyntax)
}

// ─── §13: Permission checks ───────────────────────────────────────────────────

/// Check whether a user has access to the given key.
/// Returns `ACL_OK` or `ACL_DENIED_KEY`.
/// C: `int ACLUserCheckKeyPerm(user *u, const char *key, int keylen, int flags, bool is_prefix)`
pub fn acl_user_check_key_perm(u: &User, key: &[u8], flags: i32, is_prefix: bool) -> i32 {
    for selector in &u.selectors {
        if selector.check_key(key, flags, is_prefix) == ACL_OK {
            return ACL_OK;
        }
    }
    ACL_DENIED_KEY
}

/// Check whether a user has permission to use the given pub/sub channel.
/// Returns `ACL_OK` or `ACL_DENIED_CHANNEL`.
/// C: `int ACLUserCheckChannelPerm(user *u, sds channel, int is_pattern)`
pub fn acl_user_check_channel_perm(u: &User, channel: &[u8], is_pattern: bool) -> i32 {
    for selector in &u.selectors {
        if selector.flags & SELECTOR_FLAG_ALLCHANNELS != 0 {
            return ACL_OK;
        }
        if acl_check_channel_against_list(&selector.channels, channel, is_pattern) == ACL_OK {
            return ACL_OK;
        }
    }
    ACL_DENIED_CHANNEL
}

/// Check a channel pattern against a list of allowed channel patterns.
/// C: `static int ACLCheckChannelAgainstList(list *reference, const char *channel, int channellen, int is_pattern)`
fn acl_check_channel_against_list(list: &[RedisString], channel: &[u8], is_pattern: bool) -> i32 {
    for pat in list {
        if is_pattern {
            // Pattern-subscriptions must match literally.
            if pat.as_bytes() == channel {
                return ACL_OK;
            }
        } else {
            // Regular channels: match via glob.
            if string_match_len(pat.as_bytes(), channel, false) {
                return ACL_OK;
            }
        }
    }
    ACL_DENIED_CHANNEL
}

/// Check all permissions for a user/command pair.
/// Returns `ACL_OK` or the first denied code (preference: DB < CMD < KEY < CHANNEL).
///
/// C: `int ACLCheckAllUserCommandPerm(user *u, struct serverCommand *cmd, robj **argv, int argc, int dbid, int *idxptr)`
///
/// TODO(port): key/channel extraction requires `getKeysFromCommandWithSpecs` and
/// `getChannelsFromCommand` which are not yet ported. This is a simplified version.
pub fn acl_check_all_user_command_perm(
    u: &User,
    cmd: &ServerCommand,
    argv: &[RedisString],
    dbid: i64,
    total_dbs: i64,
    idxptr: &mut usize,
) -> i32 {
    let mut relevant_error = ACL_DENIED_DB;
    let mut last_idx = 0usize;

    for selector in &u.selectors {
        let ret = acl_selector_check_cmd(selector, cmd, argv, dbid, total_dbs, idxptr);
        if ret == ACL_OK {
            return ACL_OK;
        }
        if ret > relevant_error || (ret == relevant_error && *idxptr > last_idx) {
            relevant_error = ret;
            last_idx = *idxptr;
        }
    }
    *idxptr = last_idx;
    relevant_error
}

/// Low-level check of a single selector against a command.
/// C: `static int ACLSelectorCheckCmd(aclSelector *selector, struct serverCommand *cmd, ...)`
///
/// TODO(port): DB-args lookup via `cmd->get_dbid_args` not yet implemented.
/// TODO(port): Key/channel extraction via `getKeysFromCommandWithSpecs` not yet implemented.
fn acl_selector_check_cmd(
    selector: &AclSelector,
    cmd: &ServerCommand,
    argv: &[RedisString],
    dbid: i64,
    total_dbs: i64,
    keyidxptr: &mut usize,
) -> i32 {
    // Database-level permission check.
    // TODO(port): cmd->get_dbid_args not yet implemented; use simple dbid check.
    if selector.flags & SELECTOR_FLAG_ALLKEYS == 0 {
        // If the command touches the keyspace, check current db.
        if cmd.acl_categories & (ACL_CATEGORY_KEYSPACE | ACL_CATEGORY_READ | ACL_CATEGORY_WRITE) != 0 {
            if !selector.can_access_db(dbid, total_dbs) {
                *keyidxptr = 0;
                return ACL_DENIED_DB;
            }
        }
    }

    // Command-level permission check.
    if selector.flags & SELECTOR_FLAG_ALLCOMMANDS == 0 && cmd.flags & CMD_NO_AUTH == 0 {
        if !selector.get_command_bit(cmd.id as usize) {
            // Check first-arg allowlist.
            if argv.len() < 2 {
                return ACL_DENIED_CMD;
            }
            let fa = match &selector.allowed_firstargs {
                Some(fa) if (cmd.id as usize) < fa.len() => &fa[cmd.id as usize],
                _ => return ACL_DENIED_CMD,
            };
            let allowed = match fa {
                Some(list) => list,
                None => return ACL_DENIED_CMD,
            };
            let check_idx = if cmd.parent { 2 } else { 1 };
            let given = argv.get(check_idx).map(|s| s.as_bytes()).unwrap_or(b"");
            if !allowed.iter().any(|a: &RedisString| a.as_bytes().eq_ignore_ascii_case(given)) {
                return ACL_DENIED_CMD;
            }
        }
    }

    // TODO(port): key-level and channel-level checks require getKeysFromCommandWithSpecs
    // and getChannelsFromCommand which are not yet ported.

    ACL_OK
}

/// Check whether the user can execute the command and also has the given
/// key-access level on all keys in the keyspace.
/// C: `int ACLUserCheckCmdWithUnrestrictedKeyAccess(...)`
pub fn acl_user_check_cmd_with_unrestricted_key_access(
    u: &User,
    cmd: &ServerCommand,
    argv: &[RedisString],
    dbid: i64,
    total_dbs: i64,
    flags: i32,
) -> bool {
    let mut local_idx = 0usize;
    for selector in &u.selectors {
        let ret = acl_selector_check_cmd(selector, cmd, argv, dbid, total_dbs, &mut local_idx);
        if ret == ACL_OK && selector.has_unrestricted_key_access(flags) {
            return true;
        }
    }
    false
}

// ─── §14: ACL loading / saving ────────────────────────────────────────────────

/// Merge selector operations split across multiple arguments into single tokens.
/// C: `static sds *ACLMergeSelectorArguments(sds *argv, int argc, int *merged_argc, int *invalid_idx)`
///
/// Returns `Ok(merged)` where each element is a merged token, or `Err(idx)` if
/// there is an unmatched opening parenthesis at `idx`.
pub fn acl_merge_selector_arguments(argv: &[RedisString]) -> Result<Vec<RedisString>, usize> {
    let mut result: Vec<RedisString> = Vec::new();
    let mut open_start: Option<usize> = None;
    let mut current_selector: Option<Vec<u8>> = None;

    for (j, arg) in argv.iter().enumerate() {
        let a = arg.as_bytes();
        if open_start.is_none() && a.first() == Some(&b'(') && a.last() != Some(&b')') {
            current_selector = Some(a.to_vec());
            open_start = Some(j);
            continue;
        }
        if open_start.is_some() {
            let sel = current_selector.as_mut().unwrap();
            sel.push(b' ');
            sel.extend_from_slice(a);
            if a.last() == Some(&b')') {
                open_start = None;
                result.push(RedisString::from_bytes(current_selector.take().unwrap().as_slice()));
            }
            continue;
        }
        result.push(arg.clone());
    }

    if let Some(open) = open_start {
        return Err(open);
    }
    Ok(result)
}

/// Apply an ACL argument list (excluding username) to a user, staging via a
/// temporary copy and applying atomically on success.
///
/// C: `sds ACLStringSetUser(user *u, sds username, sds *argv, int argc)`
///
/// Returns `Ok(())` on success or `Err(error_message_bytes)` on failure.
pub fn acl_string_set_user(
    state: &mut AclState,
    username: &[u8],
    argv: &[RedisString],
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) -> Result<(), Vec<u8>> {
    let merged = acl_merge_selector_arguments(argv).map_err(|idx| {
        let s = argv.get(idx).map(|a| a.as_bytes()).unwrap_or(b"?");
        let mut msg = b"Unmatched parenthesis in acl selector starting at '".to_vec();
        msg.extend_from_slice(s);
        msg.extend_from_slice(b"'.");
        msg
    })?;

    // Stage changes on a temporary user.
    let existing = acl_get_user_by_name(state, username);
    let mut temp = match existing {
        Some(u) => u.clone(),
        None => User::new(username),
    };

    let pubsub_default = state.pubsub_default;
    for op in &merged {
        acl_set_user(&mut temp, op.as_bytes(), cmd_lookup, orig_commands, pubsub_default)
            .map_err(|e| {
                let mut msg = b"Error in ACL SETUSER modifier '".to_vec();
                msg.extend_from_slice(op.as_bytes());
                msg.extend_from_slice(b"': ");
                msg.extend_from_slice(e.message());
                msg
            })?;
    }

    // Commit: create user if it doesn't exist, then copy.
    let key = username.to_ascii_lowercase();
    if !state.users.contains_key(&key) {
        state.users.insert(key.clone(), User::new(username));
    }
    *state.users.get_mut(&key).unwrap() = temp;
    Ok(())
}

/// Validate and enqueue a user definition from the config file for later loading.
/// C: `int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err)`
///
/// `argv` starts with the `"user"` keyword, then username, then rules.
/// Returns `Ok(())` or `Err(arg_index)` on syntax error.
pub fn acl_append_user_for_loading(
    state: &mut AclState,
    argv: &[RedisString],
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) -> Result<(), usize> {
    if argv.len() < 2 || !argv[0].as_bytes().eq_ignore_ascii_case(b"user") {
        return Err(0);
    }
    let username = argv[1].as_bytes();

    // Duplicate user check.
    if state.users_to_load.iter().any(|entry| {
        entry.first().map(|n| n.as_bytes()) == Some(username)
    }) {
        return Err(1);
    }

    let rules = &argv[2..];
    let merged = acl_merge_selector_arguments(rules).map_err(|idx| idx + 2)?;

    // Validate rules against a fake user.
    let pubsub_default = state.pubsub_default;
    let mut fake = User::new(b"__validate__");
    for (j, op) in merged.iter().enumerate() {
        if let Err(e) = acl_set_user(&mut fake, op.as_bytes(), cmd_lookup, orig_commands, pubsub_default) {
            if e != AclSetError::NotFound {
                return Err(j + 2);
            }
            // ENOENT (command not found) is tolerated — modules may load later.
        }
    }

    // Enqueue: [username, rule0, rule1, ...]
    let mut entry = vec![argv[1].clone()];
    entry.extend(merged);
    state.users_to_load.push(entry);
    Ok(())
}

/// Load users previously enqueued by `acl_append_user_for_loading`.
/// C: `static int ACLLoadConfiguredUsers(void)`
pub fn acl_load_configured_users(
    state: &mut AclState,
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) -> Result<(), Vec<u8>> {
    let entries = state.users_to_load.clone();
    let pubsub_default = state.pubsub_default;
    for entry in &entries {
        let username = entry[0].as_bytes();
        if acl_string_has_spaces(username) {
            return Err(b"Spaces not allowed in ACL usernames".to_vec());
        }
        let key = username.to_ascii_lowercase();
        if !state.users.contains_key(&key) {
            state.users.insert(key.clone(), User::new(username));
        }
        let mut u = state.users[&key].clone();
        if u.name.as_bytes().eq_ignore_ascii_case(b"default") {
            acl_set_user(&mut u, b"reset", cmd_lookup, orig_commands, pubsub_default)
                .map_err(|e| e.message().to_vec())?;
        }
        for rule in &entry[1..] {
            acl_set_user(&mut u, rule.as_bytes(), cmd_lookup, orig_commands, pubsub_default)
                .map_err(|e| {
                    let mut msg = b"Error loading ACL rule '".to_vec();
                    msg.extend_from_slice(rule.as_bytes());
                    msg.extend_from_slice(b"' for user '");
                    msg.extend_from_slice(username);
                    msg.extend_from_slice(b"': ");
                    msg.extend_from_slice(e.message());
                    msg
                })?;
        }
        if u.flags & USER_FLAG_DISABLED != 0 {
            log::info!(
                "User '{}' is disabled (no 'on' modifier). Make sure this is not a configuration error.",
                String::from_utf8_lossy(username)
            );
        }
        *state.users.get_mut(&key).unwrap() = u;
    }
    Ok(())
}

/// Load ACL rules from `filename`, replacing the current user table atomically.
/// C: `static sds ACLLoadFromFile(const char *filename)`
///
/// Returns `Ok(())` if the file was loaded cleanly, or `Err(error_message)` if
/// any errors were found (in which case the existing rules are unchanged).
pub fn acl_load_from_file(
    state: &mut AclState,
    filename: &Path,
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) -> Result<(), Vec<u8>> {
    let content = std::fs::read_to_string(filename).map_err(|e| {
        format!("Error loading ACLs, opening file '{}': {}", filename.display(), e)
            .into_bytes()
    })?;

    let mut new_users: BTreeMap<Vec<u8>, User> = BTreeMap::new();
    let mut errors: Vec<u8> = Vec::new();
    let pubsub_default = state.pubsub_default;

    for (i, raw_line) in content.lines().enumerate() {
        let linenum = i + 1;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        // Split into tokens.
        let tokens: Vec<&[u8]> = line.as_bytes().split(|&b| b == b' ')
            .filter(|t| !t.is_empty())
            .collect();

        if tokens.is_empty() {
            continue;
        }

        if !tokens[0].eq_ignore_ascii_case(b"user") || tokens.len() < 2 {
            errors.extend_from_slice(
                format!("{}:{} should start with user keyword followed by the username. ", filename.display(), linenum).as_bytes()
            );
            continue;
        }

        let username = tokens[1];
        if acl_string_has_spaces(username) {
            errors.extend_from_slice(
                format!("{}:{}: username '{}' contains invalid characters. ",
                    filename.display(), linenum, String::from_utf8_lossy(username)).as_bytes()
            );
            continue;
        }

        let key = username.to_ascii_lowercase();
        if new_users.contains_key(&key) {
            errors.extend_from_slice(
                format!("WARNING: Duplicate user '{}' found on line {}. ",
                    String::from_utf8_lossy(username), linenum).as_bytes()
            );
            continue;
        }

        let rule_args: Vec<RedisString> = tokens[2..].iter().map(|t| RedisString::from_bytes(t)).collect();
        let merged = match acl_merge_selector_arguments(&rule_args) {
            Ok(m) => m,
            Err(_) => {
                errors.extend_from_slice(
                    format!("{}:{}: Unmatched parenthesis in selector definition.",
                        filename.display(), linenum).as_bytes()
                );
                continue;
            }
        };

        let mut u = User::new(username);
        let mut syntax_error = false;
        for op in &merged {
            if let Err(e) = acl_set_user(&mut u, op.as_bytes(), cmd_lookup, orig_commands, pubsub_default) {
                if e == AclSetError::NotFound {
                    errors.extend_from_slice(
                        format!("{}:{}: Error in applying operation '{}': {}. ",
                            filename.display(), linenum,
                            String::from_utf8_lossy(op.as_bytes()),
                            String::from_utf8_lossy(e.message())).as_bytes()
                    );
                } else if !syntax_error {
                    errors.extend_from_slice(
                        format!("{}:{}: {}. ",
                            filename.display(), linenum,
                            String::from_utf8_lossy(e.message())).as_bytes()
                    );
                    syntax_error = true;
                }
            }
        }

        if errors.is_empty() {
            new_users.insert(key, u);
        }
    }

    if !errors.is_empty() {
        errors.extend_from_slice(
            b"WARNING: ACL errors detected, no change to the previously active ACL rules was performed"
        );
        return Err(errors);
    }

    // Atomically swap in the new user table.
    // The default user is re-created if not present in the file.
    if !new_users.contains_key(b"default".as_ref()) {
        let mut def = User::new(b"default");
        def.flags = USER_FLAG_ENABLED | USER_FLAG_NOPASS;
        let root = def.root_selector_mut();
        root.flags |= SELECTOR_FLAG_ALLKEYS | SELECTOR_FLAG_ALLCOMMANDS | SELECTOR_FLAG_ALLCHANNELS | SELECTOR_FLAG_ALLDBS;
        root.allowed_commands = [0xFFFF_FFFF_FFFF_FFFFu64; COMMAND_BITS_WORDS];
        new_users.insert(b"default".to_vec(), def);
    }
    state.users = new_users;
    Ok(())
}

/// Save the current ACL rules to `filename` atomically (write-tmp, rename).
/// C: `static int ACLSaveToFile(const char *filename)`
pub fn acl_save_to_file(state: &AclState, filename: &Path) -> Result<(), Vec<u8>> {
    let mut acl: Vec<u8> = Vec::new();
    for user in state.users.values() {
        // Clone so we can call describe() which takes &mut self for caching.
        let mut u = user.clone();
        acl.extend_from_slice(b"user ");
        acl.extend_from_slice(u.name.as_bytes());
        acl.push(b' ');
        let desc = u.describe();
        acl.extend_from_slice(&desc);
        acl.push(b'\n');
    }

    let tmp_path = filename.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp_path, &acl).map_err(|e| {
        format!("Opening temp ACL file for ACL SAVE: {}", e).into_bytes()
    })?;
    std::fs::rename(&tmp_path, filename).map_err(|e| {
        let _ = std::fs::remove_file(&tmp_path);
        format!("Renaming ACL file for ACL SAVE: {}", e).into_bytes()
    })?;
    Ok(())
}

/// Called after modules are loaded to load ACL rules from the config or file.
/// C: `void ACLLoadUsersAtStartup(void)`
pub fn acl_load_users_at_startup(
    state: &mut AclState,
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) {
    if !state.acl_filename.is_empty() && !state.users_to_load.is_empty() {
        log::warn!(
            "Configuring users in both valkey.conf and an ACL file is invalid. \
             Please define either an ACL file or declare users in your valkey.conf."
        );
        std::process::exit(1);
    }

    if let Err(e) = acl_load_configured_users(state, cmd_lookup, orig_commands) {
        log::error!("Critical error while loading ACLs: {}", String::from_utf8_lossy(&e));
        std::process::exit(1);
    }

    if !state.acl_filename.is_empty() {
        let path = Path::new(
            // TODO(port): path from bytes — assumes UTF-8 filename. Flag if non-UTF-8.
            std::str::from_utf8(&state.acl_filename).unwrap_or("acl_file")
        );
        if let Err(e) = acl_load_from_file(state, path, cmd_lookup, orig_commands) {
            log::error!("Aborting startup because of ACL errors: {}", String::from_utf8_lossy(&e));
            std::process::exit(1);
        }
    }
}

// ─── §15: ACL log ─────────────────────────────────────────────────────────────

/// Check whether two log entries are "similar" (should be merged rather than creating a new entry).
/// C: `static int ACLLogMatchEntry(ACLLogEntry *a, ACLLogEntry *b)`
fn acl_log_match_entry(a: &AclLogEntry, b: &AclLogEntry) -> bool {
    if a.reason != b.reason || a.context != b.context {
        return false;
    }
    let delta = (a.ctime - b.ctime).abs();
    if delta > ACL_LOG_GROUPING_MAX_TIME_DELTA {
        return false;
    }
    a.object == b.object && a.username == b.username
}

/// Trim the ACL log to `max_len` entries (dropping oldest from the tail).
/// C: `static void trimACLLogEntriesToMaxLen(void)`
fn trim_acl_log_to_max_len(state: &mut AclState) {
    while state.acllog_max_len > 0 && state.log.len() > state.acllog_max_len {
        state.log.pop();
    }
}

/// Update per-reason ACL info counters.
/// C: `static void ACLUpdateInfoMetrics(int reason)`
///
/// TODO(port): counters should live in `AclInfo` sub-struct of `RedisServer`.
fn acl_update_info_metrics(_state: &mut AclState, reason: i32) {
    // TODO(port): increment server.acl_info.user_auth_failures etc.
    let _ = reason;
}

/// Add an entry to the ACL security log, merging with recent similar entries.
/// C: `void addACLLogEntry(client *c, int reason, int context, int argpos, sds username, sds object)`
pub fn add_acl_log_entry(
    state: &mut AclState,
    reason: i32,
    context: i32,
    username: &[u8],
    object: &[u8],
    client_info: &[u8],
    now_ms: i64,
) {
    acl_update_info_metrics(state, reason);

    if state.acllog_max_len == 0 {
        trim_acl_log_to_max_len(state);
        return;
    }

    let entry = AclLogEntry {
        count: 1,
        reason,
        context,
        object: RedisString::from_bytes(object),
        username: RedisString::from_bytes(username),
        ctime: now_ms,
        cinfo: RedisString::from_bytes(client_info),
        entry_id: state.log_entry_count,
        timestamp_created: now_ms,
    };

    // Scan the first 10 entries for a match to merge into.
    let scan_limit = 10usize.min(state.log.len());
    let match_pos = state.log[..scan_limit].iter().position(|existing| {
        acl_log_match_entry(existing, &entry)
    });

    match match_pos {
        Some(pos) => {
            // Update existing entry.
            let existing = &mut state.log[pos];
            existing.cinfo = entry.cinfo;
            existing.ctime = entry.ctime;
            existing.count += 1;
            // Move to front.
            let updated = state.log.remove(pos);
            state.log.insert(0, updated);
        }
        None => {
            state.log_entry_count += 1;
            state.log.insert(0, entry);
            trim_acl_log_to_max_len(state);
        }
    }
}

/// Build a human-readable ACL denial error message.
/// C: `sds getAclErrorMessage(int acl_res, user *user, struct serverCommand *cmd, sds errored_val, int verbose)`
pub fn get_acl_error_message(
    acl_res: i32,
    username: &[u8],
    cmd_fullname: &[u8],
    errored_val: &[u8],
    verbose: bool,
) -> Vec<u8> {
    match acl_res {
        ACL_DENIED_CMD => {
            let mut msg = b"User ".to_vec();
            msg.extend_from_slice(username);
            msg.extend_from_slice(b" has no permissions to run the '");
            msg.extend_from_slice(cmd_fullname);
            msg.extend_from_slice(b"' command");
            msg
        }
        ACL_DENIED_KEY => {
            if verbose {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username);
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(errored_val);
                msg.extend_from_slice(b"' key");
                msg
            } else {
                b"No permissions to access a key".to_vec()
            }
        }
        ACL_DENIED_CHANNEL => {
            if verbose {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username);
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(errored_val);
                msg.extend_from_slice(b"' channel");
                msg
            } else {
                b"No permissions to access a channel".to_vec()
            }
        }
        ACL_DENIED_DB => {
            if verbose {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username);
                msg.extend_from_slice(b" has no permissions to access database ");
                msg.extend_from_slice(errored_val);
                msg
            } else {
                b"No permissions to access database".to_vec()
            }
        }
        _ => {
            // TODO(architect): is panic correct here? The C code calls serverPanic.
            b"Unknown ACL denial reason".to_vec()
        }
    }
}

// ─── §16: ACL command handlers ───────────────────────────────────────────────

/// `ACL` command dispatcher.
///
/// C: `void aclCommand(client *c)`
///
/// TODO(port): this function uses `CommandContext` which is not yet fully wired.
/// The function signature follows PORTING.md §4.1.
pub fn acl_command(ctx: &mut crate::command_context::CommandContext) -> Result<(), RedisError> {
    // TODO(port): implement full ACL subcommand dispatch.
    // Sub-commands: SETUSER, DELUSER, GETUSER, LIST, USERS, WHOAMI,
    //               LOAD, SAVE, CAT, GENPASS, LOG, DRYRUN, HELP.
    Err(RedisError::runtime(b"ACL command not yet fully ported"))
}

/// `AUTH [username] password` command handler.
///
/// C: `void authCommand(client *c)`
pub fn auth_command(ctx: &mut crate::command_context::CommandContext) -> Result<(), RedisError> {
    // TODO(port): implement AUTH with full ACLAuthenticateUser logic.
    // Needs: CommandContext::arg() for username/password, acl_state() for user lookup,
    // module auth callbacks (Phase 10), and reply helpers.
    Err(RedisError::runtime(b"AUTH command not yet fully ported"))
}

/// Recompute command bits for all users from their `command_rules` strings.
/// C: `void ACLRecomputeCommandBitsFromCommandRulesAllUsers(void)`
///
/// TODO(port): requires a live `cmd_lookup` and `orig_commands` reference.
pub fn acl_recompute_command_bits_from_command_rules_all_users(
    state: &mut AclState,
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) {
    let pubsub_default = state.pubsub_default;
    for user in state.users.values_mut() {
        for selector in user.selectors.iter_mut() {
            let rules_bytes = selector.command_rules.as_bytes().to_vec();
            let args: Vec<RedisString> = rules_bytes
                .split(|&b| b == b' ')
                .filter(|t| !t.is_empty())
                .map(RedisString::from_bytes)
                .collect();

            // Reset to +@all or -@all.
            if selector.can_execute_future_commands() {
                selector.allowed_commands = [0xFFFF_FFFF_FFFF_FFFFu64; COMMAND_BITS_WORDS];
                selector.flags |= SELECTOR_FLAG_ALLCOMMANDS;
            } else {
                selector.allowed_commands = [0u64; COMMAND_BITS_WORDS];
                selector.flags &= !SELECTOR_FLAG_ALLCOMMANDS;
            }
            selector.reset_first_args();

            for arg in &args {
                let _ = acl_set_selector(selector, arg.as_bytes(), cmd_lookup, orig_commands);
            }
        }
    }
}

/// Check if any ACL user references commands from the given module.
/// C: `int ACLModuleHasCommandRules(const struct ValkeyModule *module, sds *rule_out)`
///
/// TODO(port): requires module → command relationship which is Phase 10.
pub fn acl_module_has_command_rules(
    state: &AclState,
    _module: &ValkeyModule,
) -> Option<Vec<u8>> {
    // TODO(port): implement module command rule checking (Phase 10).
    let _ = state;
    None
}

/// Update the default user's password (implements `requirepass` config).
/// C: `void ACLUpdateDefaultUserPassword(sds password)`
pub fn acl_update_default_user_password(
    state: &mut AclState,
    password: Option<&[u8]>,
    cmd_lookup: &dyn Fn(&[u8]) -> Option<ServerCommand>,
    orig_commands: &HashMap<RedisString, Box<ServerCommand>>,
) {
    let pubsub_default = state.pubsub_default;
    if let Some(u) = acl_get_user_by_name_mut(state, b"default") {
        let _ = acl_set_user(u, b"resetpass", cmd_lookup, orig_commands, pubsub_default);
        match password {
            Some(pw) => {
                let mut op = vec![b'>'];
                op.extend_from_slice(pw);
                let _ = acl_set_user(u, &op, cmd_lookup, orig_commands, pubsub_default);
            }
            None => {
                let _ = acl_set_user(u, b"nopass", cmd_lookup, orig_commands, pubsub_default);
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/acl.c  (3504 lines, ~104 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         39
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         All major types, constants, and function signatures ported.
//                  Key gaps: sha256 hashing (placeholder; needs sha2 crate),
//                  full glob matching (string_match_len/prefix_match_len stubs),
//                  key/channel extraction (requires getKeysFromCommandWithSpecs),
//                  acl_command/auth_command bodies (need CommandContext reply API),
//                  module auth callbacks (Phase 10), client pubsub kill logic,
//                  flag constant values (educated guesses from server.h context).
//                  Global state uses Mutex<AclState> instead of bare C globals.
//                  BTreeMap used for Users to mirror rax lexicographic iteration.
// ──────────────────────────────────────────────────────────────────────────────

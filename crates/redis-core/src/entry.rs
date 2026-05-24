//! Entry — hash field/value pair with optional expiry.
//!
//! C: `src/entry.c` (548 lines, ~15 functions) + `entry.h` (76 lines)
//!
//! # Purpose
//!
//! An "entry" is the primitive unit of the Redis HASH data type with
//! hash-field expiration support.  Each entry holds a field name (byte
//! string), a value (owned or externally referenced), and an optional
//! millisecond-precision expiry timestamp.
//!
//! # C vs Rust representation
//!
//! The C implementation uses a four-variant single-allocation memory layout
//! distinguished by SDS aux bits in the field header:
//!
//! - **Type 1** (`SDS_TYPE_5` field): field + embedded value, no expiry.
//! - **Type 2** (`SDS_TYPE_8` field, embedded value): field + embedded value,
//!   optional expiry.
//! - **Type 3** (`SDS_TYPE_8+` field, value pointer): field + pointer to
//!   separately allocated sds, optional expiry.
//! - **Type 4** (`SDS_TYPE_8+` field, stringRef pointer): field + pointer to
//!   a `stringRef { buf, len }` struct; the entry does NOT own the buffer.
//!
//! In Phase A Rust this single-allocation optimisation is **not** replicated.
//! `Entry` is a plain struct with separately heap-allocated components.
//! Defrag and `dismiss_memory` are no-op stubs.  The public API surface is
//! preserved faithfully.
//!
//! PORT NOTE: C memory layout (single-allocation embedding + SDS aux bits) is
//! replaced by `Entry { field, value, expiry }`.  Behaviour is semantically
//! identical; per-entry memory footprint is larger.  Phase B can reintroduce
//! the embedding optimisation behind the same public API.

// TODO(architect): need dependency edge from redis-server to redis-types for
// RedisString + RedisError; both crates are already listed in
// crates/redis-server/Cargo.toml, so confirm import paths resolve at check time.

use redis_types::RedisError;
use redis_types::RedisString;
use std::sync::Arc;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Maximum allocation size the C code allows for a single-block entry
/// (field + embedded value + optional expiry ≤ 128 bytes).
///
/// Not functionally used by the Rust struct; preserved as a documentation
/// constant.  Phase B may use it to gate an inline-storage (`smallvec`)
/// optimisation for the embedded-value path.
///
/// C: `EMBED_VALUE_MAX_ALLOC_SIZE` (`entry.h:25`)
pub const EMBED_VALUE_MAX_ALLOC_SIZE: usize = 128;

/// Sentinel returned/accepted by C callers to indicate "no expiry".
///
/// The Rust public API uses `Option<MsTime>` (`None` = no expiry) instead.
/// This constant exists solely for cross-reference and for callers that
/// bridge C-sourced raw timestamp values.
///
/// C: `EXPIRY_NONE` (defined in `server.h`; value not visible in `entry.c`).
///
/// TODO(port): confirm the concrete value of `EXPIRY_NONE` from `server.h`
/// and replace this placeholder if it differs from -1.
pub const EXPIRY_NONE: MsTime = -1;

/// C SDS aux-bit indices used to encode entry type in the field SDS header.
///
/// Preserved as documentation constants; the `EntryValue` enum discriminant
/// and `Entry.expiry: Option<MsTime>` replace their role in Rust.
///
/// C: anonymous `enum` in `entry.c:85–100`.
pub mod c_aux_bits {
    pub const FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY: u8 = 0;
    pub const FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR: u8 = 1;
    pub const FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF: u8 = 2;
}

// ─── Types ────────────────────────────────────────────────────────────────────

/// Millisecond-precision Unix timestamp.  Maps to C `mstime_t` (`long long`).
pub type MsTime = i64;

/// The value side of a hash entry.
///
/// The C Types 1–4 (distinguished at runtime by SDS aux bits) are collapsed
/// into this safe Rust enum.  The enum discriminant replaces the aux-bit
/// encoding at zero extra memory cost on 64-bit targets.
#[derive(Clone, Debug)]
pub enum EntryValue {
    /// Owned byte-string value (C Types 1, 2, 3).
    ///
    /// Covers embedded values (Types 1/2, where C co-locates field + value in
    /// one block) and separately allocated SDS values (Type 3, where C stores
    /// a pointer to an independently allocated sds).  In Rust both are simply
    /// owned heap allocations inside `RedisString`.
    Owned(RedisString),

    /// Non-owning reference to an external buffer (C Type 4 — `stringRef`).
    ///
    /// In C the entry allocates a `stringRef { const char *buf; size_t len; }`
    /// struct but NOT the buffer it points to.  Freeing a Type 4 entry frees
    /// the `stringRef` struct but NOT `buf`.
    ///
    /// `Arc<[u8]>` is the closest safe approximation: it provides shared
    /// ownership so that the buffer stays alive as long as any `Arc` clone
    /// holds a reference, without requiring a lifetime parameter on `Entry`.
    ///
    /// TODO(architect): `Arc<[u8]>` shifts ownership model from "external
    /// lifetime managed by caller" (C semantics) to "ref-counted shared
    /// ownership" (Rust semantics).  Evaluate in Phase B whether the primary
    /// use-case (avoiding duplication between core and a module) is adequately
    /// served by `Arc`, or whether a raw-pointer + explicit lifetime wrapper
    /// is required.
    StringRef(Arc<[u8]>),
}

impl EntryValue {
    /// Returns the value as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            EntryValue::Owned(s) => s.as_bytes(),
            EntryValue::StringRef(arc) => arc.as_ref(),
        }
    }

    /// Returns `true` if this value is a non-owning string reference (C Type 4).
    pub fn is_string_ref(&self) -> bool {
        matches!(self, EntryValue::StringRef(_))
    }
}

/// A hash field/value pair with an optional expiration timestamp.
///
/// Maps to the C `entry` opaque type (`typedef struct _entry entry` in
/// `entry.h:21`).  In C the entry pointer IS the field SDS data pointer —
/// a deliberate aliasing trick that allows the field and value to share one
/// allocation.  In Rust we use a plain struct with separate fields.
///
/// # Ownership
///
/// - `field` and `value: Owned(...)` are fully owned by this struct.
/// - `value: StringRef(arc)` shares ownership of the backing buffer.
/// - `expiry: Option<MsTime>` is a plain `Copy` value.
///
/// `Drop` is handled automatically by the compiler; no explicit `entryFree`
/// is needed.
///
/// C: `entry.h:21` (`typedef struct _entry entry`) + `entry.c:104–547`
#[derive(Clone, Debug)]
pub struct Entry {
    /// The field name.
    ///
    /// C: the `entry` pointer IS the field SDS data pointer; the struct's
    /// location in memory is derived from pointer arithmetic on the field.
    pub field: RedisString,

    /// The value, owned or externally referenced.
    ///
    /// C: embedded after the field (Types 1/2), or referenced via a pointer
    /// stored immediately before the field allocation (Types 3/4).
    pub value: EntryValue,

    /// Expiration time (Unix milliseconds).
    ///
    /// `None` corresponds to C `EXPIRY_NONE` (no expiry set).
    ///
    /// C: optional `mstime_t` prepended to the allocation block.
    pub expiry: Option<MsTime>,
}

impl Entry {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Creates a new owned entry.
    ///
    /// `value: None` represents a field-only entry (no value stored).  In C
    /// this is signalled by passing `NULL` for the `value` sds, which causes
    /// `entryReqSize` to be called with `value_len = SIZE_MAX`.
    ///
    /// C: `entryCreate(field, value, expiry)` (`entry.c:350–357`)
    ///
    /// PORT NOTE: C takes `const_sds field` (no ownership transfer for field)
    /// and `sds value` (ownership transferred).  In Rust both are owned by
    /// value; callers should `clone()` if they need to retain the field.
    pub fn create(field: RedisString, value: Option<RedisString>, expiry: Option<MsTime>) -> Self {
        Entry {
            field,
            value: match value {
                Some(v) => EntryValue::Owned(v),
                None => EntryValue::Owned(RedisString::new()),
            },
            expiry,
        }
    }

    // ── Field / value accessors ───────────────────────────────────────────────

    /// Returns a reference to the field name.
    ///
    /// C: `entryGetField(entry)` (`entry.c:104–106`)
    ///
    /// PORT NOTE: C returns the field `sds` pointer (which equals the `entry`
    /// pointer itself due to the aliasing layout).  Here we return `&RedisString`.
    pub fn get_field(&self) -> &RedisString {
        &self.field
    }

    /// Returns the value as a byte slice.
    ///
    /// C: `entryGetValue(entry, &len)` (`entry.c:151–168`)
    ///
    /// PORT NOTE: C writes the length via an out-pointer `size_t *len` and
    /// returns `char *`.  Rust returns `&[u8]` which carries the length inline.
    pub fn get_value(&self) -> &[u8] {
        self.value.as_bytes()
    }

    /// Returns `true` if the value is embedded (i.e. owned by this entry).
    ///
    /// C: `entryHasEmbeddedValue(entry)` (`entry.c:116–118`)
    ///
    /// PORT NOTE: In C "embedded" means the value bytes live in the same
    /// allocation block as the field (Types 1/2).  A Type 3 entry has a
    /// separately allocated sds for the value but is still "owned" by the
    /// entry.  Type 4 (`stringRef`) is the only case the C code treats as
    /// "not embedded."
    ///
    /// In Rust, `Owned(...)` covers all owned cases (C Types 1–3) and
    /// `StringRef(...)` covers C Type 4.  This method returns `true` iff
    /// the value is `Owned`.
    ///
    /// TODO(port): revisit if the per-byte embedding optimisation is
    /// reintroduced in Phase B (e.g. distinguishing inline vs heap-allocated
    /// `Owned`).
    pub fn has_embedded_value(&self) -> bool {
        !self.value.is_string_ref()
    }

    /// Returns `true` if the value is a non-owning string reference (C Type 4).
    ///
    /// C: `entryHasStringRef(entry)` (`entry.c:122–124`)
    pub fn has_string_ref(&self) -> bool {
        self.value.is_string_ref()
    }

    // ── Expiry accessors / mutators ───────────────────────────────────────────

    /// Returns `true` if an expiration timestamp is set.
    ///
    /// C: `entryHasExpiry(entry)` (`entry.c:128–130`)
    pub fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }

    /// Returns the expiration timestamp, or `None` if not set.
    ///
    /// C: `entryGetExpiry(entry)` (`entry.c:211–214`) — returns the sentinel
    /// `EXPIRY_NONE` when no expiry is set.  Rust returns `Option<MsTime>`.
    ///
    /// PORT NOTE: call sites that compare against `EXPIRY_NONE` must be
    /// updated to pattern-match on `None`.
    pub fn get_expiry(&self) -> Option<MsTime> {
        self.expiry
    }

    /// Sets or clears the expiration timestamp.
    ///
    /// Pass `None` to clear the expiry (equivalent to C `EXPIRY_NONE`).
    ///
    /// C: `entrySetExpiry(e, expiry)` (`entry.c:217–227`)
    ///
    /// PORT NOTE: C may reallocate the entry here because the `mstime_t`
    /// slot is prepended to the allocation block; adding or removing an expiry
    /// changes the block layout and requires a new pointer.  The C function
    /// therefore returns `entry *` (potentially a different address).  In Rust,
    /// `Option<MsTime>` lives inside the struct and mutation is always
    /// in-place; the return type is `&mut self`.
    pub fn set_expiry(&mut self, expiry: Option<MsTime>) {
        self.expiry = expiry;
    }

    /// Returns `true` if the entry has an assigned expiration that has already
    /// elapsed compared to the current time.
    ///
    /// C: `entryIsExpired(entry)` (`entry.c:231–235`)
    ///
    /// TODO(port): The C implementation calls `timestampIsExpired(entry_expiry)`
    /// which checks against the server's command-time snapshot
    /// (`commandTimeSnapshot()`), NOT the wall clock.  Here we use
    /// `SystemTime::now()` as a placeholder.  Phase B must inject the server
    /// time snapshot to match C semantics.
    pub fn is_expired(&self) -> bool {
        let expiry_ms = match self.expiry {
            None => return false,
            Some(t) => t,
        };
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as MsTime)
            .unwrap_or(0);
        expiry_ms <= now_ms
    }

    // ── Value mutation ────────────────────────────────────────────────────────

    /// Updates the entry's value to a non-owning reference to an external
    /// buffer.  The entry stores the `Arc<[u8]>` but does not own the
    /// underlying bytes in the C sense (freeing the entry does not free them).
    ///
    /// C: `entryUpdateAsStringRef(e, buf, len, expiry)` (`entry.c:363–399`)
    ///
    /// PORT NOTE: C allocates a `stringRef { buf, len }` struct on the heap
    /// and stores a pointer to it inside the entry allocation.  In Rust the
    /// `Arc<[u8]>` is stored directly on the `Entry` struct.  If the entry
    /// needs to change from Owned→StringRef or add/remove an expiry slot, C
    /// reallocates the entire entry; Rust mutates in-place.
    pub fn update_as_string_ref(&mut self, buf: Arc<[u8]>, expiry: Option<MsTime>) {
        self.value = EntryValue::StringRef(buf);
        self.expiry = expiry;
    }

    /// Updates the entry's value and/or expiry in-place.
    ///
    /// - `value: None` — keep the existing value unchanged.
    /// - `new_expiry: None` — clear expiry (C `EXPIRY_NONE`).
    ///
    /// C: `entryUpdate(e, value, expiry)` (`entry.c:405–491`)
    ///
    /// PORT NOTE: In C, `entryUpdate` takes `sds value` (NULL = keep
    /// existing) and `mstime_t expiry` (EXPIRY_NONE = clear).  The C code may
    /// reallocate the entry if the layout changes (e.g. toggling expiry,
    /// crossing the `EMBED_VALUE_MAX_ALLOC_SIZE` threshold).  The C function
    /// returns `entry *` (potentially a new address).  In Rust, mutation is
    /// always in-place via `&mut self`; no reallocation occurs.
    ///
    /// PORT NOTE: The C code skips the update entirely if neither value nor
    /// expiry changed.  The Rust version is equivalent: assigning to
    /// `self.value` and `self.expiry` is idempotent.
    pub fn update(&mut self, value: Option<RedisString>, new_expiry: Option<MsTime>) {
        if let Some(v) = value {
            self.value = EntryValue::Owned(v);
        } else if self.has_string_ref() {
            // C: `entryHasStringRef(e) && !value` → delegate to
            // `entryUpdateAsStringRef` preserving existing buf/len.
            // In Rust we only need to update the expiry.
        }
        // Update expiry: None means "clear" (EXPIRY_NONE), Some(t) means set.
        // Only skip when both old and new are None (no-op case).
        if new_expiry.is_some() || self.expiry.is_some() {
            self.expiry = new_expiry;
        }
    }

    // ── Memory reporting ──────────────────────────────────────────────────────

    /// Returns an approximate lower-bound on the memory used by this entry.
    ///
    /// C: `entryMemUsage(entry)` (`entry.c:495–512`)
    ///
    /// PORT NOTE: The C implementation calls `zmalloc_usable_size()` on the
    /// raw allocation pointer to get the exact allocator-reported size
    /// (including any allocator padding).  In Rust, querying the actual
    /// allocated size requires `unsafe` interaction with the global allocator.
    /// This method returns a calculated lower bound instead.
    ///
    /// TODO(port): use a custom allocator hook or `std::alloc::Layout` if
    /// exact memory accounting is needed by `DEBUG MEMORY USAGE` or similar.
    pub fn mem_usage(&self) -> usize {
        let mut mem: usize = 0;

        mem += std::mem::size_of::<Self>();
        mem += self.field.len();

        mem += match &self.value {
            EntryValue::Owned(s) => s.len(),
            EntryValue::StringRef(_arc) => {
                // PERF(port): C reports zmalloc_usable_size of the stringRef
                // struct only (sizeof(stringRef) ≈ 16 bytes); Arc<[u8]> has
                // additional ref-count overhead (2 × usize).
                std::mem::size_of::<usize>() * 2
                // Note: we do NOT count arc.len() — the buffer is external.
            }
        };

        mem
    }

    // ── Defragmentation / memory advice ──────────────────────────────────────

    /// Attempts to defragment the entry's allocations.
    ///
    /// Returns `true` if the internal allocation was moved (caller must update
    /// its pointer to `self`), `false` if unchanged.
    ///
    /// C: `entryDefrag(e, defragfn, sdsdefragfn)` (`entry.c:521–540`)
    ///
    /// PORT NOTE: In C, defrag is performed by calling `defragfn` on the raw
    /// allocation pointer and `sdsdefragfn` on sds values; the entry may
    /// return a new pointer if the block was relocated.  In Rust, the allocator
    /// manages defragmentation transparently.  This is a no-op stub for Phase A.
    ///
    /// TODO(port): If explicit jemalloc defrag hooks are needed in Phase B,
    /// implement via a pluggable allocator wrapper in `redis-core`.
    pub fn defrag(&mut self) -> bool {
        false
    }

    /// Advises the OS to release memory pages used by the entry's value.
    ///
    /// C: `entryDismissMemory(entry)` (`entry.c:544–547`)
    ///
    /// PORT NOTE: C calls `dismissSds(*entryGetValueRef(entry))` which issues
    /// `madvise(MADV_DONTNEED)` on the value's backing pages — intended to
    /// reduce CoW pressure in a forked child.  In Rust there is no direct
    /// equivalent without `unsafe` platform calls.  No-op stub for Phase A.
    ///
    /// TODO(architect): If post-fork memory trimming is required, implement via
    /// a platform-specific `madvise` wrapper in `redis-core::zmalloc`.
    pub fn dismiss_memory(&self) {
        // No-op in Phase A.
    }
}

// ─── Free-function API (C-naming shims) ───────────────────────────────────────
//
// The C consumers call free functions (`entryCreate`, `entryFree`, …).
// These thin wrappers preserve the C call-site shape so that ported callers
// (hash.rs, db.rs, etc.) can be updated incrementally to the method API.
//
// PORT NOTE: These are transitional shims.  Phase B porting of call sites
// should migrate to the `Entry` method API directly.

/// Creates a new owned entry.
///
/// `value: None` = no value (field-only entry; C passes `NULL` sds).
///
/// C: `entryCreate(field, value, expiry)` — `entry.c:350–357`
pub fn entry_create(field: RedisString, value: Option<RedisString>, expiry: Option<MsTime>) -> Entry {
    Entry::create(field, value, expiry)
}

/// Returns a reference to the entry's field name.
///
/// C: `entryGetField(entry)` — `entry.c:104–106`
pub fn entry_get_field(entry: &Entry) -> &RedisString {
    entry.get_field()
}

/// Returns the entry's value as a byte slice.
///
/// C: `entryGetValue(entry, len)` — `entry.c:151–168`
pub fn entry_get_value(entry: &Entry) -> &[u8] {
    entry.get_value()
}

/// Returns the expiration timestamp, or `None` if not set.
///
/// C: `entryGetExpiry(entry)` — `entry.c:211–214`
pub fn entry_get_expiry(entry: &Entry) -> Option<MsTime> {
    entry.get_expiry()
}

/// Returns `true` if an expiration timestamp is set.
///
/// C: `entryHasExpiry(entry)` — `entry.c:128–130`
pub fn entry_has_expiry(entry: &Entry) -> bool {
    entry.has_expiry()
}

/// Returns `true` if the value is owned by the entry (not a string reference).
///
/// C: `entryHasEmbeddedValue(entry)` — `entry.c:116–118`
pub fn entry_has_embedded_value(entry: &Entry) -> bool {
    entry.has_embedded_value()
}

/// Returns `true` if the value is a non-owning string reference (C Type 4).
///
/// C: `entryHasStringRef(entry)` — `entry.c:122–124`
pub fn entry_has_string_ref(entry: &Entry) -> bool {
    entry.has_string_ref()
}

/// Sets or clears the expiration timestamp.
///
/// C: `entrySetExpiry(e, expiry)` — `entry.c:217–227`
///
/// PORT NOTE: C returns `entry *` because the entry may be reallocated.
/// Rust mutates in-place; callers retain the same `&mut Entry` reference.
pub fn entry_set_expiry(entry: &mut Entry, expiry: Option<MsTime>) {
    entry.set_expiry(expiry);
}

/// Returns `true` if the entry's expiry has elapsed.
///
/// C: `entryIsExpired(entry)` — `entry.c:231–235`
pub fn entry_is_expired(entry: &Entry) -> bool {
    entry.is_expired()
}

/// Drops the entry, freeing all owned memory.
///
/// In C, `entryFree` manually frees the value pointer (if any) and the
/// allocation block.  In Rust, `Drop` is automatic; this function simply
/// moves the entry and lets it drop.
///
/// C: `entryFree(entry)` — `entry.c:238–241`
pub fn entry_free(entry: Entry) {
    drop(entry);
}

/// Updates the entry's value to a non-owning string reference.
///
/// C: `entryUpdateAsStringRef(e, buf, len, expiry)` — `entry.c:363–399`
///
/// PORT NOTE: C takes `(const char *buf, size_t len)`.  Rust takes
/// `Arc<[u8]>` for safe shared ownership.  See `EntryValue::StringRef` docs.
pub fn entry_update_as_string_ref(entry: &mut Entry, buf: Arc<[u8]>, expiry: Option<MsTime>) {
    entry.update_as_string_ref(buf, expiry);
}

/// Updates the entry's value and/or expiry.
///
/// `value: None` — keep existing value.
/// `new_expiry: None` — clear expiry.
///
/// C: `entryUpdate(e, value, expiry)` — `entry.c:405–491`
///
/// PORT NOTE: C returns `entry *` because the entry may be reallocated.
/// Rust mutates in-place.
pub fn entry_update(entry: &mut Entry, value: Option<RedisString>, new_expiry: Option<MsTime>) {
    entry.update(value, new_expiry);
}

/// Returns an approximate lower-bound on the entry's total memory usage.
///
/// C: `entryMemUsage(entry)` — `entry.c:495–512`
pub fn entry_mem_usage(entry: &Entry) -> usize {
    entry.mem_usage()
}

/// Defragments the entry.  No-op in Phase A.
///
/// C: `entryDefrag(e, defragfn, sdsdefragfn)` — `entry.c:521–540`
pub fn entry_defrag(entry: &mut Entry) -> bool {
    entry.defrag()
}

/// Advises the OS to release memory pages used by the entry's value.
/// No-op in Phase A.
///
/// C: `entryDismissMemory(entry)` — `entry.c:544–547`
pub fn entry_dismiss_memory(entry: &Entry) {
    entry.dismiss_memory();
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/entry.c (548 lines, ~15 functions) + entry.h (76 lines)
//   target_crate:  redis-server
//   confidence:    medium
//   todos:         7
//   port_notes:    9
//   unsafe_blocks: 0
//   notes:         C single-allocation memory layout with SDS aux-bit typing
//                  replaced by a safe Entry struct; defrag/dismiss are no-ops;
//                  is_expired() uses wall clock instead of server time snapshot.
//                  Phase B must wire in server time and verify memory-reporting
//                  numbers against C benchmarks.
// ──────────────────────────────────────────────────────────────────────────

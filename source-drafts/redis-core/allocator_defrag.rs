//! Port of `allocator_defrag.c` + `allocator_defrag.h`.
//!
//! Allocator-specific defragmentation logic invoked from [`crate::defrag`].
//!
//! Three compile-time paths, selected by Cargo features:
//!
//! | Cargo feature | C condition | Behaviour |
//! |---|---|---|
//! | `jemalloc-defrag` | `HAVE_DEFRAG && USE_JEMALLOC` | Full jemalloc implementation |
//! | `debug-force-defrag` | `DEBUG_FORCE_DEFRAG` | Always-defrag debug stub |
//! | (neither) | `else` | No-op; `allocator_defrag_init` returns `Err` |
//!
//! Architecture (from C file comment):
//!
//! ```text
//!              Application code
//!                 /       \
//!     allocation /         \ defrag
//!               /           \
//!          zmalloc    allocator_defrag
//!           /  |   \       /     \
//!     libc  tcmalloc  jemalloc   other
//! ```
//!
//! C source: `reference/valkey/src/allocator_defrag.c` (488 lines, 11 functions)

// TODO(architect): The `jemalloc-defrag` feature path calls jemalloc C functions
// (`je_mallocx`, `je_sdallocx`, `je_mallctl`, `je_mallctlnametomib`,
// `je_mallctlbymib`). Every such call requires an `unsafe` block. Pilot crates
// have a zero-unsafe budget. Before Phase B compiles this module:
//   1. Approve an unsafe budget increase for `redis-core/allocator_defrag.rs`.
//   2. Add `tikv-jemalloc-sys` (or a hand-written `extern "C"` block) as a
//      dependency in `crates/redis-core/Cargo.toml`.
//   3. Replace the stub returns in the `jemalloc-defrag` section with real FFI
//      calls wrapped in `// SAFETY: <invariant>` comments.
// TODO(architect): Add `log` crate to `crates/redis-core/Cargo.toml` for the
// `log::debug!` call inside `get_allocator_fragmentation` (jemalloc path).

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use redis_types::error::RedisError;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Words returned per queried pointer by the jemalloc batch-query interface.
// C: #define BATCH_QUERY_ARGS_OUT 3
const BATCH_QUERY_ARGS_OUT: usize = 3;

/// Utilization threshold expressed as milli-units (denominator 1000).
/// Equals 12.5 %. A slab below this fraction of the average utilization is
/// always a defrag candidate; one above average × 1.125 is never defragged.
// C: #define UTILIZATION_THRESHOLD_FACTOR_MILLI (125)
const UTILIZATION_THRESHOLD_FACTOR_MILLI: u64 = 125;

// ─── Batch-query output accessors (replace C macros) ─────────────────────────
//
// The batch-query output is a flat `[usize; BATCH_QUERY_ARGS_OUT * N]` array.
// Each queried pointer `i` occupies three consecutive words.

// C: #define SLAB_NFREE(out, i)    out[(i) * BATCH_QUERY_ARGS_OUT]
#[inline(always)]
fn slab_nfree(out: &[usize], i: usize) -> usize {
    out[i * BATCH_QUERY_ARGS_OUT]
}

// C: #define SLAB_LEN(out, i)      out[(i) * BATCH_QUERY_ARGS_OUT + 2]
#[inline(always)]
fn slab_len(out: &[usize], i: usize) -> usize {
    out[i * BATCH_QUERY_ARGS_OUT + 2]
}

// C: #define SLAB_NUM_REGS(out, i) out[(i) * BATCH_QUERY_ARGS_OUT + 1]
#[inline(always)]
fn slab_num_regs(out: &[usize], i: usize) -> usize {
    out[i * BATCH_QUERY_ARGS_OUT + 1]
}

// ─── Types ───────────────────────────────────────────────────────────────────
//
// Defined unconditionally so downstream callers can import the types without
// needing to mirror feature flags; the types are only *populated* in the
// jemalloc path.

/// Precomputed MIB (Management Information Base) key for jemalloc.
///
/// Obtained once from a string name via `je_mallctlnametomib`; used afterwards
/// with `je_mallctlbymib` to avoid per-query hash-table look-ups.
///
/// C equivalent: `jeMallctlKey` in `allocator_defrag.c`.
#[derive(Debug, Default, Clone)]
pub struct JeMallctlKey {
    /// Numeric MIB key array; only indices `0..keylen` are valid.
    pub key: [usize; 6],
    /// Number of valid entries in `key`.
    pub keylen: usize,
}

/// Precomputed MIB keys for the three per-bin statistics fields queried live.
///
/// C equivalent: `jeBinInfoKeys` in `allocator_defrag.c`.
#[derive(Debug, Default, Clone)]
pub struct JeBinInfoKeys {
    /// Key for `stats.arenas.<ARENA>.bins.<B>.curslabs`.
    pub curr_slabs: JeMallctlKey,
    /// Key for `stats.arenas.<ARENA>.bins.<B>.nonfull_slabs`.
    pub nonfull_slabs: JeMallctlKey,
    /// Key for `stats.arenas.<ARENA>.bins.<B>.curregs`.
    pub curr_regs: JeMallctlKey,
}

/// Static metadata for one jemalloc size-class bin.
///
/// Populated once during [`allocator_defrag_init`] and read-only afterwards.
///
/// C equivalent: `jeBinInfo` in `allocator_defrag.c`.
#[derive(Debug, Default, Clone)]
pub struct JeBinInfo {
    /// Byte size of each region in this bin.
    pub reg_size: usize,
    /// Number of regions per slab.
    pub nregs: u32,
    /// Precomputed MIB keys for fast live-statistics queries.
    pub info_keys: JeBinInfoKeys,
}

/// Jemalloc defragmentation control block.
///
/// Initialized exactly once by [`allocator_defrag_init`] and immutable
/// afterwards. Stored in a [`OnceLock`] so it can be shared without locks.
///
/// C equivalent: `jemallocCB` in `allocator_defrag.c`.
#[derive(Debug)]
pub struct JemallocCb {
    /// Number of size-class bins in the jemalloc configuration.
    pub nbins: u32,
    /// Per-bin metadata; `bin_info.len() == nbins as usize`.
    pub bin_info: Vec<JeBinInfo>,
    /// MIB key for `experimental.utilization.batch_query`.
    pub util_batch_query: JeMallctlKey,
    /// MIB key for `epoch` — writing to it forces cross-thread stats sync.
    pub epoch: JeMallctlKey,
}

/// Live usage snapshot for one jemalloc bin.
///
/// Refreshed by [`allocator_defrag_get_frag_smallbins`] before each defrag
/// scan, then consumed by [`allocator_should_defrag`] for per-pointer decisions.
///
/// C equivalent: `jemallocBinUsageData` in `allocator_defrag.c`.
#[derive(Debug, Default, Clone)]
pub struct JemallocBinUsageData {
    /// Current total number of slabs in this bin.
    pub curr_slabs: usize,
    /// Current number of non-full slabs (slabs with at least one free region).
    pub curr_nonfull_slabs: usize,
    /// Current total allocated regions across all slabs in this bin.
    pub curr_regs: usize,
}

// ─── Module-level state ──────────────────────────────────────────────────────

/// Set to `true` after [`allocator_defrag_init`] completes successfully.
/// Every public entry point in the jemalloc path asserts this before proceeding.
// C: static int defrag_supported = 0;
#[cfg(feature = "jemalloc-defrag")]
static DEFRAG_SUPPORTED: AtomicBool = AtomicBool::new(false);

/// Jemalloc control block; written exactly once during init.
// C: static jemallocCB je_cb = {0, NULL, {{0}, 0}, {{0}, 0}};
#[cfg(feature = "jemalloc-defrag")]
static JE_CB: OnceLock<JemallocCb> = OnceLock::new();

/// Per-bin live usage data; updated on every call to
/// [`allocator_defrag_get_frag_smallbins`].
// C: static jemallocBinUsageData *je_usage_info = NULL;
#[cfg(feature = "jemalloc-defrag")]
static JE_USAGE_INFO: Mutex<Vec<JemallocBinUsageData>> = Mutex::new(Vec::new());

// ─── jemalloc internal helpers ────────────────────────────────────────────────

/// Map a jemalloc region size (bytes) to its bin index, assuming lg-quantum = 3.
///
/// Sizes ≤ 64 bytes use linear binning (8-byte steps, bins 0–7).
/// Larger sizes use exponential binning with 4 sub-classes per power-of-two.
///
/// This is the inverse of `arenas.bin.<N>.size`, needed because the
/// `experimental.utilization.batch_query` interface returns region size rather
/// than bin index.
///
/// C: `jeSize2BinIndexLgQ3` — static inline, `allocator_defrag.c:166–192`.
// C: allocator_defrag.c:166-192
#[cfg(feature = "jemalloc-defrag")]
#[inline]
fn je_size_to_bin_index_lg_q3(sz: usize) -> u32 {
    const SIZE_CLASS_GROUP_SIZE: u32 = 4;
    const LG_QUANTUM_3_FIRST_POW2: u32 = 3;
    const LG_QUANTUM_3_OFFSET: u32 = (64 >> LG_QUANTUM_3_FIRST_POW2) - 1;

    if sz <= 64 {
        return ((sz >> 3) - 1) as u32;
    }

    // C: unsigned leading_zeros = __builtin_clzll(sz - 1);
    // PERF(port): __builtin_clzll operates on u64; cast to u64 to preserve
    // behaviour on 32-bit targets where usize is 32 bits.
    let leading_zeros = ((sz as u64) - 1).leading_zeros();
    let exp = 64u32 - leading_zeros;

    // C: within_group_offset = size_class_group_size -
    //      (((1ULL << exp) - sz) >> (exp - lg_quantum_3_first_pow2));
    let within_group_offset = SIZE_CLASS_GROUP_SIZE
        - ((((1u64 << exp) as usize).wrapping_sub(sz) >> (exp - LG_QUANTUM_3_FIRST_POW2) as usize)
            as u32);

    within_group_offset
        + ((exp - (LG_QUANTUM_3_FIRST_POW2 + 3)) - 1) * SIZE_CLASS_GROUP_SIZE
        + LG_QUANTUM_3_OFFSET
}

/// Advance the jemalloc epoch to force cross-thread statistics synchronisation.
///
/// C: `jeRefreshStats` — static inline, `allocator_defrag.c:198–203`.
// C: allocator_defrag.c:198-203
#[cfg(feature = "jemalloc-defrag")]
fn je_refresh_stats(je_cb: &JemallocCb) {
    // TODO(architect): unsafe needed — je_mallctlbymib is an FFI call.
    // Real implementation:
    //   let mut epoch: u64 = 1;
    //   let mut sz = std::mem::size_of::<u64>();
    //   unsafe {
    //       je_mallctlbymib(
    //           je_cb.epoch.key.as_ptr(), je_cb.epoch.keylen,
    //           &mut epoch as *mut _ as *mut c_void, &mut sz,
    //           &epoch as *const _ as *mut c_void, sz,
    //       );
    //   }
    let _ = je_cb;
}

/// Translate a jemalloc control-knob name string to its numeric MIB key.
///
/// Called once per key during initialisation. `key_name` must be
/// NUL-terminated when passed to the underlying C function.
///
/// C: `jeQueryKeyInit` — static inline, `allocator_defrag.c:206–212`.
// C: allocator_defrag.c:206-212
#[cfg(feature = "jemalloc-defrag")]
fn je_query_key_init(key_name: &[u8], key_info: &mut JeMallctlKey) -> Result<(), RedisError> {
    // TODO(architect): unsafe needed — je_mallctlnametomib is an FFI call.
    // Real implementation (sketch):
    //   key_info.keylen = key_info.key.len();
    //   let res = unsafe {
    //       je_mallctlnametomib(
    //           key_name.as_ptr() as *const c_char,
    //           key_info.key.as_mut_ptr(),
    //           &mut key_info.keylen,
    //       )
    //   };
    //   debug_assert!(key_info.keylen <= key_info.key.len());
    //   if res != 0 {
    //       return Err(RedisError::runtime(b"je_mallctlnametomib failed"));
    //   }
    let _ = (key_name, key_info);
    Ok(())
}

/// Query a jemalloc control knob using a precomputed MIB key, writing the
/// result (a `usize`) into `value`.
///
/// Faster than the name-based `je_mallctl` path because it skips the
/// string dictionary look-up.
///
/// C: `jeQueryCtlInterface` — static inline, `allocator_defrag.c:215–219`.
// C: allocator_defrag.c:215-219
#[cfg(feature = "jemalloc-defrag")]
fn je_query_ctl_interface(key_info: &JeMallctlKey, value: &mut usize) -> Result<(), RedisError> {
    // TODO(architect): unsafe needed — je_mallctlbymib is an FFI call.
    // Real implementation (sketch):
    //   let mut sz = std::mem::size_of::<usize>();
    //   let res = unsafe {
    //       je_mallctlbymib(
    //           key_info.key.as_ptr(), key_info.keylen,
    //           value as *mut _ as *mut c_void, &mut sz,
    //           std::ptr::null_mut(), 0,
    //       )
    //   };
    //   if res != 0 { Err(RedisError::runtime(b"je_mallctlbymib failed")) } else { Ok(()) }
    let _ = (key_info, value);
    Ok(())
}

/// Initialise the fast MIB query keys for one bin's three statistics fields.
///
/// `bin_index` is the zero-based index into the jemalloc bin array.
///
/// C: `binQueryHelperInitialization` — static inline, `allocator_defrag.c:221–235`.
// C: allocator_defrag.c:221-235
#[cfg(feature = "jemalloc-defrag")]
fn bin_query_helper_initialization(
    helper: &mut JeBinInfoKeys,
    bin_index: u32,
) -> Result<(), RedisError> {
    // PORT NOTE: ARENA_TO_QUERY is MALLCTL_ARENAS_ALL, a jemalloc compile-time
    // constant (typically 4096). In C it is stringified at compile time via the
    // STRINGIFY macro. Here we use the integer directly in format!; verify the
    // value against the linked jemalloc headers in Phase B.
    // TODO(port): confirm the numeric value of MALLCTL_ARENAS_ALL from the
    // jemalloc headers used by this build (4096 is the common value).
    let arena_str = "4096";

    let curregs_name = format!(
        "stats.arenas.{}.bins.{}.curregs\0",
        arena_str, bin_index
    );
    je_query_key_init(curregs_name.as_bytes(), &mut helper.curr_regs)?;

    let curslabs_name = format!(
        "stats.arenas.{}.bins.{}.curslabs\0",
        arena_str, bin_index
    );
    je_query_key_init(curslabs_name.as_bytes(), &mut helper.curr_slabs)?;

    let nonfull_name = format!(
        "stats.arenas.{}.bins.{}.nonfull_slabs\0",
        arena_str, bin_index
    );
    je_query_key_init(nonfull_name.as_bytes(), &mut helper.nonfull_slabs)?;

    Ok(())
}

/// Decide whether the allocation living in a particular slab should be moved.
///
/// Returns `true` if defragmentation is recommended.
///
/// Decision criteria (see C comment at `allocator_defrag.c:339`):
/// 1. Skip if the slab is full (`nalloced == nregs`) — moving gains nothing.
/// 2. Skip if `curr_nonfull_slabs < 2` — no destination slab available.
/// 3. Always defrag if the slab is < 12.5 % full (absolute low-use threshold).
/// 4. Defrag if per-slab utilisation < average non-full utilisation × 1.125.
///
/// C: `makeDefragDecision` — static inline, `allocator_defrag.c:358–373`.
// C: allocator_defrag.c:358-373
#[cfg(feature = "jemalloc-defrag")]
#[inline]
fn make_defrag_decision(
    bin_info: &JeBinInfo,
    bin_usage: &JemallocBinUsageData,
    nalloced: u64,
) -> bool {
    let curr_full_slabs = bin_usage.curr_slabs.saturating_sub(bin_usage.curr_nonfull_slabs);

    // PORT NOTE: The C code performs unsigned subtraction of size_t values
    // without an underflow guard. We use saturating_sub to preserve the
    // intent without triggering debug-mode panics.
    let allocated_nonfull = (bin_usage.curr_regs as u64)
        .saturating_sub(curr_full_slabs as u64 * bin_info.nregs as u64);

    if bin_info.nregs as u64 == nalloced || bin_usage.curr_nonfull_slabs < 2 {
        return false;
    }

    if 1000 * nalloced < bin_info.nregs as u64 * UTILIZATION_THRESHOLD_FACTOR_MILLI {
        return true;
    }

    if 1000 * nalloced * bin_usage.curr_nonfull_slabs as u64
        > (1000 + UTILIZATION_THRESHOLD_FACTOR_MILLI) * allocated_nonfull
    {
        return false;
    }

    true
}

// ─── Public API: jemalloc-defrag path ────────────────────────────────────────

/// Allocate `size` bytes bypassing the jemalloc thread cache, going directly
/// to the arena bin.
///
/// Used during online defragmentation to place relocated objects into
/// well-utilised slabs.
///
/// C: `allocatorDefragAlloc` — `allocator_defrag.c:145–148`.
// C: allocator_defrag.c:145-148
#[cfg(feature = "jemalloc-defrag")]
pub fn allocator_defrag_alloc(size: usize) -> *mut c_void {
    // TODO(architect): unsafe needed — je_mallocx is an FFI call.
    // Real implementation:
    //   unsafe { je_mallocx(size, MALLOCX_TCACHE_NONE) }
    let _ = size;
    std::ptr::null_mut()
}

/// Free `ptr` (of known allocation size `size`), bypassing the thread cache.
///
/// C: `allocatorDefragFree` — `allocator_defrag.c:149–152`.
// C: allocator_defrag.c:149-152
#[cfg(feature = "jemalloc-defrag")]
pub fn allocator_defrag_free(ptr: *mut c_void, size: usize) {
    if ptr.is_null() {
        return;
    }
    // TODO(architect): unsafe needed — je_sdallocx is an FFI call.
    // Real implementation:
    //   unsafe { je_sdallocx(ptr, size, MALLOCX_TCACHE_NONE) }
    let _ = (ptr, size);
}

/// Initialise the defragmentation subsystem.
///
/// Must be called exactly once before any other function in this module.
/// Returns `Ok(())` on success; `Err` if jemalloc does not expose the
/// required experimental interfaces or if any query fails.
///
/// C: `allocatorDefragInit` — `allocator_defrag.c:255–311`.
// C: allocator_defrag.c:255-311
#[cfg(feature = "jemalloc-defrag")]
pub fn allocator_defrag_init() -> Result<(), RedisError> {
    debug_assert!(!DEFRAG_SUPPORTED.load(Ordering::Relaxed), "allocator_defrag_init called twice");

    let mut util_batch_query = JeMallctlKey::default();
    let mut epoch_key = JeMallctlKey::default();

    je_query_key_init(b"experimental.utilization.batch_query\0", &mut util_batch_query)?;
    je_query_key_init(b"epoch\0", &mut epoch_key)?;

    let mut cb = JemallocCb {
        nbins: 0,
        bin_info: Vec::new(),
        util_batch_query,
        epoch: epoch_key,
    };

    je_refresh_stats(&cb);

    // Verify lg-quantum == 3, i.e. jemalloc quantum == 8 bytes.
    // TODO(architect): unsafe needed — je_mallctl is an FFI call.
    // Real implementation:
    //   let mut quantum: usize = 0;
    //   let mut sz = std::mem::size_of::<usize>();
    //   unsafe {
    //       je_mallctl(
    //           b"arenas.quantum\0".as_ptr() as *const c_char,
    //           &mut quantum as *mut _ as *mut c_void, &mut sz,
    //           std::ptr::null_mut(), 0,
    //       );
    //   }
    //   debug_assert_eq!(quantum, 8, "lg-quantum must be 3");
    let jemalloc_quantum: usize = 8; // Phase A stub — FFI populates this in Phase B.
    debug_assert_eq!(jemalloc_quantum, 8, "lg-quantum must be 3 (quantum = 8 bytes)");

    // Retrieve the total number of size-class bins.
    // TODO(architect): unsafe needed — je_mallctl is an FFI call.
    // Real implementation:
    //   let mut sz = std::mem::size_of::<u32>();
    //   unsafe {
    //       je_mallctl(
    //           b"arenas.nbins\0".as_ptr() as *const c_char,
    //           &mut cb.nbins as *mut _ as *mut c_void, &mut sz,
    //           std::ptr::null_mut(), 0,
    //       );
    //   }
    //   debug_assert!(cb.nbins != 0);
    // Phase A: nbins stays 0; loop below is a no-op.

    cb.bin_info = vec![JeBinInfo::default(); cb.nbins as usize];

    for j in 0..cb.nbins {
        let bin_info = &mut cb.bin_info[j as usize];

        // Query region size: "arenas.bin.<j>.size"
        // TODO(architect): unsafe needed — je_mallctl is an FFI call.
        // Real: write result into bin_info.reg_size.

        // Query regions per slab: "arenas.bin.<j>.nregs"
        // TODO(architect): unsafe needed — je_mallctl is an FFI call.
        // Real: write result into bin_info.nregs.

        bin_query_helper_initialization(&mut bin_info.info_keys, j)?;

        debug_assert_eq!(
            je_size_to_bin_index_lg_q3(bin_info.reg_size) as u32,
            j,
            "size-to-bin-index round-trip failed for bin {}",
            j,
        );
    }

    let nbins = cb.nbins;
    JE_CB
        .set(cb)
        .map_err(|_| RedisError::runtime(b"allocator_defrag_init called twice"))?;

    {
        let mut usage = JE_USAGE_INFO
            .lock()
            .map_err(|_| RedisError::runtime(b"JE_USAGE_INFO mutex poisoned during init"))?;
        *usage = vec![JemallocBinUsageData::default(); nbins as usize];
    }

    DEFRAG_SUPPORTED.store(true, Ordering::Release);
    Ok(())
}

/// Compute total external-fragmentation bytes across all small bins.
///
/// Refreshes the jemalloc epoch first to synchronise cross-thread statistics,
/// then for each bin computes:
///
/// ```text
/// frag += (nregs * curr_slabs - curr_regs) * reg_size
/// ```
///
/// Also updates [`JE_USAGE_INFO`] so subsequent [`allocator_should_defrag`]
/// calls have fresh per-bin utilisation data.
///
/// C: `allocatorDefragGetFragSmallbins` — `allocator_defrag.c:318–337`.
// C: allocator_defrag.c:318-337
#[cfg(feature = "jemalloc-defrag")]
pub fn allocator_defrag_get_frag_smallbins() -> u64 {
    debug_assert!(DEFRAG_SUPPORTED.load(Ordering::Acquire));

    let je_cb = match JE_CB.get() {
        Some(cb) => cb,
        None => return 0,
    };

    je_refresh_stats(je_cb);

    let mut usage = match JE_USAGE_INFO.lock() {
        Ok(g) => g,
        Err(_) => return 0,
    };

    let mut frag: u64 = 0;
    for j in 0..(je_cb.nbins as usize) {
        let bin_info = &je_cb.bin_info[j];
        let bin_usage = &mut usage[j];

        je_query_ctl_interface(&bin_info.info_keys.curr_regs, &mut bin_usage.curr_regs).ok();
        je_query_ctl_interface(&bin_info.info_keys.curr_slabs, &mut bin_usage.curr_slabs).ok();
        je_query_ctl_interface(
            &bin_info.info_keys.nonfull_slabs,
            &mut bin_usage.curr_nonfull_slabs,
        )
        .ok();

        // PORT NOTE: saturating_sub guards against data races between the
        // jemalloc stats epoch and the live bin counters; the C code allows
        // silent wrapping via unsigned arithmetic.
        let capacity = bin_info.nregs as u64 * bin_usage.curr_slabs as u64;
        let used = bin_usage.curr_regs as u64;
        frag += capacity.saturating_sub(used) * bin_info.reg_size as u64;
    }
    frag
}

/// Determine whether the allocation at `ptr` should be relocated to reduce
/// fragmentation.
///
/// Issues a single jemalloc batch-query to learn the slab layout around `ptr`,
/// then calls [`make_defrag_decision`] using the most recently cached bin
/// utilisation from [`JE_USAGE_INFO`].
///
/// Returns `true` if defrag is recommended, `false` otherwise.
///
/// C: `allocatorShouldDefrag` — `allocator_defrag.c:382–411`.
// C: allocator_defrag.c:382-411
#[cfg(feature = "jemalloc-defrag")]
pub fn allocator_should_defrag(ptr: *mut c_void) -> bool {
    debug_assert!(DEFRAG_SUPPORTED.load(Ordering::Acquire));

    let je_cb = match JE_CB.get() {
        Some(cb) => cb,
        None => return false,
    };

    // Sentinel value mirrors C's `out[j] = -1` initialisation (size_t wraps).
    let mut out = [usize::MAX; BATCH_QUERY_ARGS_OUT];

    // TODO(architect): unsafe needed — je_mallctlbymib is an FFI call.
    // Real implementation:
    //   let mut out_sz = std::mem::size_of_val(&out);
    //   let in_sz = std::mem::size_of::<*mut c_void>();
    //   unsafe {
    //       je_mallctlbymib(
    //           je_cb.util_batch_query.key.as_ptr(),
    //           je_cb.util_batch_query.keylen,
    //           out.as_mut_ptr() as *mut c_void, &mut out_sz,
    //           &ptr as *const _ as *mut c_void, in_sz,
    //       );
    //   }
    let _ = ptr;

    debug_assert!(slab_num_regs(&out, 0) > 0);
    debug_assert!(slab_len(&out, 0) > 0);
    debug_assert!(
        slab_nfree(&out, 0) != usize::MAX,
        "batch query sentinel still set — FFI call did not execute"
    );

    let num_regs = slab_num_regs(&out, 0);
    if num_regs == 0 {
        return false;
    }
    let region_size = slab_len(&out, 0) / num_regs;

    if region_size > je_cb.bin_info[je_cb.nbins as usize - 1].reg_size {
        return false;
    }

    let binind = je_size_to_bin_index_lg_q3(region_size) as usize;
    debug_assert!(binind < je_cb.nbins as usize);
    debug_assert_eq!(region_size, je_cb.bin_info[binind].reg_size);

    let nalloced = (je_cb.bin_info[binind].nregs as u64)
        .saturating_sub(slab_nfree(&out, 0) as u64);

    let usage = match JE_USAGE_INFO.lock() {
        Ok(g) => g,
        Err(_) => return false,
    };

    make_defrag_decision(&je_cb.bin_info[binind], &usage[binind], nalloced)
}

/// Compute the allocator fragmentation ratio as a percentage of total allocated
/// memory (not just small bins), to avoid inflating the percentage when most
/// memory is in large bins.
///
/// Also writes the absolute small-bin fragmentation byte count into
/// `out_frag_bytes` if it is `Some`.
///
/// C: `getAllocatorFragmentation` — `allocator_defrag.c:419–434`.
// C: allocator_defrag.c:419-434
#[cfg(feature = "jemalloc-defrag")]
pub fn get_allocator_fragmentation(out_frag_bytes: Option<&mut usize>) -> f32 {
    // TODO(port): crate::zmalloc::get_allocator_info() must be called here to
    // populate `allocated`, `active`, and `resident`. The function lives in
    // crates/redis-core/src/zmalloc.rs (Phase A stub; wire up in Phase B).
    let allocated: usize = 0;
    let active: usize = 0;
    let resident: usize = 0;

    let frag_smallbins_bytes = allocator_defrag_get_frag_smallbins();

    let frag_pct = if allocated == 0 {
        0.0f32
    } else {
        frag_smallbins_bytes as f32 / allocated as f32 * 100.0
    };

    let rss_pct = if allocated == 0 {
        0.0f32
    } else {
        resident as f32 / allocated as f32 * 100.0 - 100.0
    };
    let rss_bytes = resident.saturating_sub(allocated);

    // C: serverLog(LL_DEBUG, "allocated=%zu, active=%zu, resident=%zu, frag=…")
    // TODO(architect): add `log` crate to Cargo.toml for redis-core.
    log::debug!(
        "allocated={}, active={}, resident={}, frag={:.2}% ({:.2}% rss), frag_bytes={} ({} rss)",
        allocated,
        active,
        resident,
        frag_pct,
        rss_pct,
        frag_smallbins_bytes,
        rss_bytes,
    );

    if let Some(out) = out_frag_bytes {
        *out = frag_smallbins_bytes as usize;
    }

    frag_pct
}

// ─── Public API: debug-force-defrag path ─────────────────────────────────────

/// Always succeed: debug stub never fails to initialise.
///
/// C: `allocatorDefragInit` (DEBUG_FORCE_DEFRAG branch) — `allocator_defrag.c:437`.
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn allocator_defrag_init() -> Result<(), RedisError> {
    Ok(())
}

/// Allocate via the global Rust allocator (debug path, bypasses nothing).
///
/// C: `allocatorDefragAlloc` (DEBUG_FORCE_DEFRAG) — `allocator_defrag.c:444`.
// C: allocator_defrag.c:444-447
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn allocator_defrag_alloc(size: usize) -> *mut c_void {
    // TODO(architect): unsafe needed — allocating via the global allocator and
    // casting the resulting Box to a raw void pointer requires unsafe.
    // Real: Box::into_raw(vec![0u8; size].into_boxed_slice()) as *mut c_void
    let _ = size;
    std::ptr::null_mut()
}

/// Free a pointer obtained from [`allocator_defrag_alloc`] (debug path).
///
/// C: `allocatorDefragFree` (DEBUG_FORCE_DEFRAG) — `allocator_defrag.c:440`.
// C: allocator_defrag.c:440-443
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn allocator_defrag_free(ptr: *mut c_void, _size: usize) {
    // TODO(architect): unsafe needed — reconstituting a Box from a raw pointer
    // to drop it requires unsafe.
    // Real: equivalent of zfree(ptr); drop(Box::from_raw(ptr as *mut u8))
    let _ = ptr;
}

/// No tracked fragmentation in the debug path.
///
/// C: `allocatorDefragGetFragSmallbins` (DEBUG_FORCE_DEFRAG) — `allocator_defrag.c:448`.
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn allocator_defrag_get_frag_smallbins() -> u64 {
    0
}

/// Always recommend defragmentation in the debug path.
///
/// C: `allocatorShouldDefrag` (DEBUG_FORCE_DEFRAG) — `allocator_defrag.c:452`.
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn allocator_should_defrag(_ptr: *mut c_void) -> bool {
    true
}

/// Report fragmentation using server config thresholds (debug path).
///
/// C: `getAllocatorFragmentation` (DEBUG_FORCE_DEFRAG) — `allocator_defrag.c:457–460`.
// C: allocator_defrag.c:457-460
#[cfg(all(not(feature = "jemalloc-defrag"), feature = "debug-force-defrag"))]
pub fn get_allocator_fragmentation(out_frag_bytes: Option<&mut usize>) -> f32 {
    // C: *out_frag_bytes = server.active_defrag_ignore_bytes + 1;
    //    return server.active_defrag_threshold_upper;
    // TODO(port): needs access to RedisServer fields `active_defrag_ignore_bytes`
    // and `active_defrag_threshold_upper`. Thread RedisServer through in Phase B.
    // Returning sentinel values that guarantee the defrag threshold is exceeded.
    if let Some(out) = out_frag_bytes {
        *out = usize::MAX;
    }
    100.0f32
}

// ─── Public API: no-op (unsupported allocator) path ──────────────────────────

/// Defragmentation is not supported by the active allocator.
///
/// C: `allocatorDefragInit` (else branch) — `allocator_defrag.c:463`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn allocator_defrag_init() -> Result<(), RedisError> {
    Err(RedisError::runtime(
        b"defragmentation is not supported by the active memory allocator",
    ))
}

/// No-op allocation (defrag unsupported); returns null.
///
/// C: `allocatorDefragAlloc` (else branch) — `allocator_defrag.c:470`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn allocator_defrag_alloc(_size: usize) -> *mut c_void {
    std::ptr::null_mut()
}

/// No-op free (defrag unsupported).
///
/// C: `allocatorDefragFree` (else branch) — `allocator_defrag.c:466`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn allocator_defrag_free(_ptr: *mut c_void, _size: usize) {}

/// No tracked fragmentation when defrag is unsupported.
///
/// C: `allocatorDefragGetFragSmallbins` (else branch) — `allocator_defrag.c:473`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn allocator_defrag_get_frag_smallbins() -> u64 {
    0
}

/// Never recommend defragmentation when the allocator does not support it.
///
/// C: `allocatorShouldDefrag` (else branch) — `allocator_defrag.c:477`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn allocator_should_defrag(_ptr: *mut c_void) -> bool {
    false
}

/// Return zero fragmentation when defrag is unsupported.
///
/// C: `getAllocatorFragmentation` (else branch) — `allocator_defrag.c:481`.
#[cfg(not(any(feature = "jemalloc-defrag", feature = "debug-force-defrag")))]
pub fn get_allocator_fragmentation(out_frag_bytes: Option<&mut usize>) -> f32 {
    if let Some(out) = out_frag_bytes {
        *out = 0;
    }
    0.0
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/allocator_defrag.c  (488 lines, 11 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         18
//   port_notes:    3
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         All jemalloc FFI call-sites are stubs (TODO(architect));
//                  the three-way C preprocessor split is modelled as Cargo
//                  features `jemalloc-defrag` / `debug-force-defrag` / default.
//                  Arithmetic that could silently wrap in C (unsigned size_t
//                  subtraction) uses saturating_sub in Rust. The
//                  MALLCTL_ARENAS_ALL constant needs verification against the
//                  linked jemalloc headers in Phase B.
// ──────────────────────────────────────────────────────────────────────────

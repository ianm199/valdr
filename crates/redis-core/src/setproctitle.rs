//! Process-title manipulation for Linux and macOS.
// Deferred feature: process-title (ps/cmdline) override; wired at startup once
// the libc/unsafe budget for argv[0] manipulation is approved.
#![allow(dead_code)]
//!
//! Ports `src/setproctitle.c` (332 lines, 6 functions), which implements
//! `setproctitle(3)` for platforms that lack a native version. BSD-family OSes
//! (NetBSD, FreeBSD, OpenBSD, DragonFly) ship a native `setproctitle(3)` and
//! do not need this implementation.
//!
//! ## Technique
//!
//! The C implementation overwrites `argv[0]` in-place. The kernel exposes the
//! null-terminated string at `argv[0]` as the process command line (visible in
//! `ps`, `/proc/<pid>/cmdline`, etc.). Writing a new string into that memory
//! changes what the OS shows without any additional kernel call.
//!
//! This approach requires manipulating raw pointers into process memory and
//! cannot be expressed in safe Rust. All such portions are stubbed with
//! `TODO(architect)`. A practical Phase B alternative is the `proctitle` crate
//! (safe wrapper over the same unsafe) or `prctl(PR_SET_NAME, …)` via `libc`
//! on Linux (16-byte limit, but sufficient for server process names).
//!
//! ## Platform dispatch
//!
//! | Platform                                    | Strategy                                             |
//! |---------------------------------------------|------------------------------------------------------|
//! | Linux                                       | Overwrite `argv[0]` memory (unsafe / libc required)  |
//! | macOS                                       | `setprogname` + overwrite `argv[0]` (unsafe / libc)  |
//! | FreeBSD / NetBSD / OpenBSD / DragonFly      | Native `setproctitle(3)` — OS handles it             |
//! | Other                                       | No-op                                                |
//!
//! C source: `src/setproctitle.c` — 332 lines, 6 functions.

// PORT NOTE: C preprocessor platform guards (`#if !HAVE_SETPROCTITLE`,
// `#if defined __linux || defined __APPLE__`) become `#[cfg(...)]` attributes
// in Rust. They are left as prose comments here for Phase B to wire up.
//
// PORT NOTE: The C file's `#include` directives (stddef, stdarg, stdlib, stdio,
// string, errno) collapse to `use std::...` statements in Rust.
//
// PORT NOTE: The variadic `setproctitle(const char *fmt, ...)` API becomes
// `set_proc_title(title: Option<&[u8]>)`. Callers must pre-format the string;
// Rust has no variadic functions.

use std::ffi::OsString;
use std::sync::Mutex;

/// Maximum byte length of a settable process title.
///
/// C: `#define SPT_MAXTITLE 255`
const SPT_MAX_TITLE: usize = 255;

/// Stand-in for the POSIX `EINVAL` error code (22 on Linux/macOS).
///
/// TODO(architect): replace with `libc::EINVAL` once `libc` is declared as a
/// dependency of `redis-core`. Do not hard-code OS errno values in production
/// code.
const ERRNO_EINVAL: i32 = 22;

/// Stand-in for the POSIX `ENOMEM` error code (12 on Linux/macOS).
///
/// TODO(architect): replace with `libc::ENOMEM` — same rationale as ERRNO_EINVAL.
const ERRNO_ENOMEM: i32 = 12;

/// Internal state for the process-title override mechanism.
///
/// In C this is a module-level static struct (`static struct { ... } SPT`).
/// In Rust it is wrapped in [`SPT`] as `Mutex<Option<SptState>>` to allow safe
/// lazy initialisation and shared access.
///
/// TODO(architect): unsafe needed — the three raw-pointer fields commented out
/// below cannot be stored in a `static Mutex<...>` in safe Rust. They must
/// become `*mut u8` with an `unsafe impl Send` (justified by the invariant that
/// they are written once during `spt_init` while single-threaded, then only
/// read). Options once the unsafe budget is approved:
///   (a) `struct RawTitleRegion(*mut u8, *mut u8, *mut u8)` with
///       `unsafe impl Send + Sync`.
///   (b) Store byte offsets rather than pointers, recomputed each call from a
///       stable address anchor obtained via `libc::argc`/`libc::argv`.
///   (c) Delegate entirely to the `proctitle` crate.
struct SptState {
    /// Byte-string copy of the original `argv[0]` captured at init time.
    ///
    /// C: `const char *arg0;`
    arg0: Vec<u8>,

    /// Whether the title region has been zeroed at least once (first write
    /// clears the entire region; subsequent writes only clear `SPT_MAX_TITLE`
    /// bytes to avoid exposing stale data).
    ///
    /// C: `_Bool reset;`
    reset: bool,

    /// Last OS error code captured from a failed init or title-set operation.
    ///
    /// C: `int error;`
    error: i32,
    // TODO(architect): unsafe needed — raw pointer fields:
    //
    //   base: *mut u8  — start of the writable title region (= original argv[0])
    //   end:  *mut u8  — one-past-end of the writable region
    //   nul:  *mut u8  — pointer to the original NUL terminator inside `base`
    //
    // These point into process memory whose address is only knowable through
    // the raw C `argv` array passed to `main`. There is no safe Rust API to
    // obtain them.
}

/// Module-level singleton for the SPT state, guarded by a `Mutex`.
///
/// C: `static struct { ... } SPT;`
static SPT: Mutex<Option<SptState>> = Mutex::new(None);

/// Returns the smaller of two `usize` values.
///
/// C: `static inline size_t spt_min(size_t a, size_t b)` / `SPT_MIN` macro.
///
/// PORT NOTE: Call-sites in this module use `a.min(b)` directly; this function
/// is retained as a named alias matching the C symbol for diff-review clarity.
#[inline]
fn spt_min(a: usize, b: usize) -> usize {
    a.min(b)
}

/// Clears the process environment so its strings no longer occupy the memory
/// region immediately following `argv[]` (which we want to reuse as title
/// space).
///
/// On glibc this calls `clearenv(3)`. On non-glibc platforms the C code
/// manually replaces `environ` with a freshly `malloc`'d one-element NULL
/// array.
///
/// C: `int spt_clearenv(void)` — setproctitle.c:87-102
///
/// Returns `Ok(())` on success, `Err(errno_code)` on failure.
///
/// TODO(architect): unsafe needed — `clearenv(3)` and direct `extern "C"
/// char **environ` manipulation have no safe Rust equivalents in `std`. Use
/// `libc::clearenv()` (glibc) or implement the malloc/replace trick via
/// `libc::malloc` + `libc::environ` raw access. Until resolved, this function
/// returns `Err(ERRNO_EINVAL)` making `spt_init` a no-op.
fn spt_clear_env() -> Result<(), i32> {
    Err(ERRNO_EINVAL)
}

/// Deep-copies environment variables from the original `environ` block so the
/// strings no longer reside in the argv-adjacent memory region.
///
/// After this call, the process `environ` pointer is redirected to
/// heap-allocated storage, freeing the original block for title use.
///
/// C: `static int spt_copyenv(int envc, char *oldenv[])` —
/// setproctitle.c:105-161
///
/// Returns `Ok(())` on success, `Err(errno_code)` on failure.
///
/// TODO(architect): unsafe needed — the C function begins with a pointer
/// equality check (`environ != oldenv`) to detect whether `environ` has already
/// been redirected and return early. This check requires access to the raw
/// `environ` and `oldenv` pointers, which are not available in safe Rust.
/// Without this guard we always attempt the copy-and-reset, which is a
/// correctness divergence.
///
/// TODO(port): propagate `std::env::set_var` failures as `Err(errno_code)`.
/// The C code checks the `setenv(3)` return value and restores the previous
/// environment on error; the Rust version below silently continues.
fn spt_copy_env() -> Result<(), i32> {
    // C: setproctitle.c:105-161, spt_copyenv
    //
    // Snapshot the current environment before clearing it.
    let vars: Vec<(OsString, OsString)> = std::env::vars_os().collect();

    // Clear environ so its strings no longer back the argv-adjacent memory.
    // This returns Err until the TODO(architect) above is resolved, which
    // makes spt_init a no-op on all platforms until then.
    spt_clear_env()?;

    // Re-populate from the snapshot via the safe std::env API.
    // C: `setenv(envcopy[i], eq + 1, 1)` for each key=value pair.
    for (key, val) in &vars {
        std::env::set_var(key, val);
    }

    Ok(())
}

/// Duplicates each `argv[i]` (i ≥ 1) onto the heap so the original argv
/// memory can later be overwritten with the process title.
///
/// C: `static int spt_copyargs(int argc, char *argv[])` —
/// setproctitle.c:164-179
///
/// PORT NOTE: In Rust, `std::env::args_os()` already returns owned,
/// heap-allocated strings; the raw `argv` pointers are never exposed by the
/// standard library. This function is therefore a **no-op** in the Rust port —
/// Rust's args iterator has already made the necessary copies.
///
/// TODO(architect): if direct access to the raw C `argv` array is ever required
/// (e.g., for the pointer-arithmetic in `spt_init`), it must arrive via a
/// foreign-function entry point (`extern "C" fn redis_main(argc, argv)`). No
/// safe std API exposes raw argv pointers.
fn spt_copy_args() -> Result<(), i32> {
    Ok(())
}

/// Initialises the process-title subsystem.
///
/// Must be called exactly once at process start (before any threads are
/// spawned), passing the original `argc` and `argv` from `main`. It:
///
///   1. Records a heap copy of `argv[0]` as the original program name.
///   2. Walks `argv` and `environ` to determine the largest contiguous block
///      of memory starting at `argv[0]` that can be repurposed as title space.
///   3. Deep-copies `argv` and `environ` so those strings no longer occupy
///      the title region.
///   4. On Linux: re-points `program_invocation_name` /
///      `program_invocation_short_name` to heap copies.
///      On macOS: calls `setprogname(strdup(getprogname()))`.
///
/// After this call, [`set_proc_title`] may be used to change the visible
/// process title.
///
/// C: `void spt_init(int argc, char *argv[])` — setproctitle.c:191-268
///
/// TODO(architect): unsafe needed — this function:
///   (a) reads `argv[0]` through a raw C pointer (`char *base = argv[0]`),
///   (b) compares raw pointer values to map the contiguous title region
///       (`end >= argv[i] && end <= argv[i] + strlen(argv[i])`),
///   (c) stores raw pointers into `SPT.base`, `SPT.end`, `SPT.nul`,
///   (d) on Linux accesses `program_invocation_name` /
///       `program_invocation_short_name` via `libc`,
///   (e) on macOS calls `libc::getprogname` / `libc::setprogname`.
///
///   A safe entry point shim:
///   ```ignore
///   pub unsafe extern "C" fn spt_init_entry(argc: i32, argv: *mut *mut u8) {
///       // SAFETY: caller guarantees argc/argv are the original main() args.
///       spt_init_inner(argc, argv);
///   }
///   ```
///   with the unsafe confined to that shim.
pub fn spt_init() {
    // C: setproctitle.c:191-268
    //
    // Step 1 — obtain argv[0] via the safe std API (no raw pointer).
    // The pointer-arithmetic steps 2–4 cannot proceed in safe Rust and are
    // omitted; the TODO(architect) above must be resolved first.
    let arg0 = current_argv0();

    let mut guard = match SPT.lock() {
        Ok(g) => g,
        Err(_) => return,
    };

    if guard.is_some() {
        return;
    }

    *guard = Some(SptState {
        arg0,
        reset: false,
        error: 0,
        // base, end, nul: omitted — TODO(architect) above
    });

    // spt_copy_env and spt_copy_args are called here in the C source (steps
    // 7 and 8) but both reduce to no-ops or early errors until the unsafe
    // TODO(architect) items are resolved.
    if let Some(state) = guard.as_mut() {
        if let Err(e) = spt_copy_env() {
            state.error = e;
            return;
        }
        if let Err(e) = spt_copy_args() {
            state.error = e;
        }
    }
}

/// Sets the process title visible in `ps` and `/proc/<pid>/cmdline`.
///
/// Accepts an optional pre-formatted title byte string. Passing `None`
/// restores the original `argv[0]` string captured during [`spt_init`].
///
/// The title is silently truncated to [`SPT_MAX_TITLE`] bytes if longer.
///
/// On platforms with a native `setproctitle(3)` (BSDs), this function should
/// delegate to the OS implementation. On Linux and macOS it overwrites the
/// `argv[0]` memory region established by [`spt_init`].
///
/// C: `void setproctitle(const char *fmt, ...)` — setproctitle.c:275-316
///
/// The C function takes a `printf`-style format string plus varargs. The Rust
/// equivalent takes a pre-formatted `Option<&[u8]>` because Rust has no
/// variadic functions. Callers must format before calling.
///
/// Returns `Ok(())` on success, `Err(errno_code)` on failure.
///
/// TODO(architect): unsafe needed — the body must write `buf[..len]` into
/// the raw pointer region `SPT.base..SPT.end` using `memset` + `memcpy`
/// (or equivalent `ptr::write_bytes` / `ptr::copy_nonoverlapping`). This
/// cannot be done in safe Rust. Until resolved, the title change is a no-op.
///
/// TODO(port): on FreeBSD / NetBSD / OpenBSD / DragonFly, gate on
/// `#[cfg(any(target_os = "freebsd", target_os = "netbsd", ...))]` and call
/// `libc::setproctitle(…)` instead of the argv-overwrite path.
pub fn set_proc_title(title: Option<&[u8]>) -> Result<(), i32> {
    // C: setproctitle.c:275-316
    //
    // C logic (condensed):
    //   buf[SPT_MAXTITLE+1]
    //   if fmt:  len = vsnprintf(buf, sizeof buf, fmt, ap)
    //   else:    len = snprintf(buf, sizeof buf, "%s", SPT.arg0)
    //   if len <= 0: record errno, return
    //   if !SPT.reset: memset(base, 0, end-base); reset=1
    //   else:          memset(base, 0, min(sizeof buf, end-base))
    //   len = min(len, min(sizeof buf, end-base) - 1)
    //   memcpy(base, buf, len)
    //   nul = &base[len]
    //   if nul < SPT.nul: *SPT.nul = '.'
    //   elif nul == SPT.nul && &nul[1] < end: *SPT.nul = ' '; *++nul = '\0'

    let mut guard = SPT.lock().map_err(|_| ERRNO_EINVAL)?;

    let state = match guard.as_mut() {
        Some(s) => s,
        None => return Ok(()),
    };

    // Build the title buffer — a fixed-size stack array matching C's `buf`.
    let mut buf = [0u8; SPT_MAX_TITLE + 1];
    let src: &[u8] = match title {
        Some(t) => t,
        None => &state.arg0,
    };
    let copy_len = spt_min(src.len(), SPT_MAX_TITLE);
    buf[..copy_len].copy_from_slice(&src[..copy_len]);
    let len = copy_len;

    if len == 0 {
        state.error = 0;
        return Ok(());
    }

    // TODO(architect): unsafe needed — write buf[..len] into the raw pointer
    // region [state.base, state.end) using ptr::write_bytes (memset) followed
    // by ptr::copy_nonoverlapping (memcpy). The NUL/sentinel fixup logic
    // (comparing nul vs SPT.nul to decide whether to write '.' or ' ')
    // likewise requires raw pointer comparison and dereference.
    //
    // Once resolved, also update `state.reset = true` after the first write.
    let _ = (buf, len);

    Ok(())
}

/// Returns the current process executable path as a byte string.
///
/// Used by [`spt_init`] to capture `argv[0]` via the safe `std::env` API.
/// This is a best-effort fallback; the full implementation requires the raw C
/// `argv[0]` pointer for the title-region arithmetic.
#[cfg(unix)]
fn current_argv0() -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    std::env::args_os()
        .next()
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_default()
}

#[cfg(not(unix))]
fn current_argv0() -> Vec<u8> {
    Vec::new()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/setproctitle.c  (332 lines, 6 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         8
//   port_notes:    5
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         All raw argv/environ pointer manipulation stubbed with
//                  TODO(architect). The safe portions (env snapshot via
//                  std::env, SptState struct, title-buffer construction) are
//                  translated faithfully. The in-place argv[0] overwrite
//                  requires libc raw pointer access to be resolved in Phase B.
//                  spt_clear_env returns Err until that TODO is resolved,
//                  making spt_init a functional no-op at runtime.
// ──────────────────────────────────────────────────────────────────────────

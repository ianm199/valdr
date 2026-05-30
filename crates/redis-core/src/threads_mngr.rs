//! Thread manager — signal-based cross-thread callback dispatch.
//! Sends `SIGUSR2` (`THREADS_SIGNAL`) to a list of Linux kernel thread-IDs
//! via `tgkill(2)`, causing each target thread to invoke a registered callback
//! inside its signal handler. The API is Linux-only; non-Linux builds compile
//! to no-op stubs with identical signatures.
//! # Design (PORT NOTE)
//! The C implementation relies on POSIX signal mechanics — `sigaction(2)`
//! register the handler and `tgkill(2)` to target individual threads — both
//! which require `unsafe` in Rust. Because the pilot-crate budget for
//! `redis-core` is zero unsafe blocks, all signal-dispatch machinery is marked
//! with `TODO(architect)`.
//! The _structure_ of the state machine (atomic in-progress flag, done counter,
//! global callback, cleanup) is faithfully ported using safe Rust atomics and a
//! `Mutex`. The signal handler body (`invoke_callback`) cannot use `Mutex`
//! (not async-signal-safe per POSIX 2018 §2.4.3); see the in-code
//! `TODO(architect)` for the approved replacement pattern.

// ─── Public type alias (shared across all platforms) ──────────────────────────

/// Callback invoked by each thread upon receiving `THREADS_SIGNAL`.
/// Raw function pointers (`fn`) are `Send + Sync` in Rust, so this type
/// can safely be stored in a global.
pub type RunOnThreadCallback = fn();

// ─── Shared constant ──────────────────────────────────────────────────────────

/// The signal sent to target threads: `SIGUSR2`.
/// TODO(port): replace the literal `12` with `libc::SIGUSR2` once the `libc`
/// dependency is confirmed present in `redis-core`'s `Cargo.toml`.
/// SIGUSR2 == 12 on Linux x86-64 and arm64.
pub const THREADS_SIGNAL: i32 = 12;

// ──────────────────────────────────────────────────────────────────────────────
// Linux-only implementation
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod imp {
    use super::RunOnThreadCallback;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

 // ─── Constants ────────────────────────────────────────────────────────────

 /// Maximum seconds to wait for all threads to complete the callback.
    const RUN_ON_THREADS_TIMEOUT_SECS: u64 = 2;

 // ─── Global state ─────────────────────────────────────────────────────────

 /// The callback to be invoked by each signaled thread.
    /// TODO(architect): reading this from inside a signal handler via `Mutex::lock`
 /// is not async-signal-safe (POSIX 2018 §2.4.3). The C code performs a plain
 /// pointer-width load, which is correct because all writes are sequenced before
 /// the signals are sent (and no write races with the signal handler).
 /// In Rust the architect must replace this `Mutex` with an `AtomicUsize` that
 /// stores the function-pointer value cast to `usize`, loaded/stored with
 /// `Ordering::Release` / `Ordering::Acquire`. That cast requires an approved
 /// `unsafe` block; raise the budget in `harness/unsafe-budgets.toml` or move
 /// the transmute to a dedicated `unsafe`-permitted helper.
    static G_CALLBACK: Mutex<Option<RunOnThreadCallback>> = Mutex::new(None);

 /// Number of kernel thread-IDs targeted in the current round.
 /// PORT NOTE: `volatile` in C provides no ordering guarantees beyond
 /// preventing the compiler from caching the value. `AtomicUsize` with
 /// `Relaxed` ordering is equivalent for this single-writer use-case.
    static G_TIDS_LEN: AtomicUsize = AtomicUsize::new(0);

 /// Count of threads that have completed the callback in this round.
    static G_NUM_THREADS_DONE: AtomicUsize = AtomicUsize::new(0);

 /// Guards against re-entrant calls to `run_on_threads`.
    static G_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

 // ─── Internal helpers ─────────────────────────────────────────────────────

 /// Atomically marks the manager as in-progress.
 /// Returns `true` if it was *already* in-progress (the caller must abort).
 /// `atomic_exchange_explicit(&g_in_progress, 1,
 /// memory_order_relaxed)`; returns `IN_PROGRESS` (1) if already set.
    fn test_and_start() -> bool {
        G_IN_PROGRESS.swap(true, Ordering::Relaxed)
    }

 /// Signal handler body: invoke the registered callback and increment
 /// done counter.
 /// `__attribute__((noinline))`.
    /// TODO(architect): the real registration must use `sigaction(2)` with an
 /// `unsafe extern "C" fn(libc::c_int)` trampoline. Architect options:
 /// (a) Raise the unsafe budget for `redis-core/src/threads_mngr.rs`
 /// `harness/unsafe-budgets.toml`.
 /// (b) Introduce the `signal-hook` crate and evaluate whether its safe
 /// handler API meets the async-signal-safety requirements here.
 /// (c) Extract signal setup to a small `unsafe`-permitted shim module
 /// (e.g. `redis-core::signal_setup`) and call it from here.
    /// TODO(port): `Mutex::lock` inside a signal handler is not async-signal-safe.
 /// Must be replaced with an atomic function-pointer load (see `G_CALLBACK`
 /// docstring for the replacement pattern).
    fn invoke_callback(_sig: i32) {
        let callback: Option<RunOnThreadCallback> = G_CALLBACK.lock().ok().and_then(|guard| *guard);

        match callback {
            Some(cb) => {
                cb();
                G_NUM_THREADS_DONE.fetch_add(1, Ordering::Relaxed);
            }
            None => {
 // "tid %ld: ThreadsManager g_callback is NULL",
 // syscall(SYS_gettid));
                // TODO(port): `serverLogFromHandler` uses `write(2)` for async-signal
 // safety. `eprintln!` is not async-signal-safe; this is a Phase A
 // placeholder only. Replace with a `write(2)`-based log path before
 // Phase C.
                eprintln!("ThreadsManager: g_callback is NULL in signal handler");
            }
        }
    }

 /// Poll until all threads have invoked the callback, or until the timeout
 /// elapses.
 /// The C implementation uses `select(0, NULL, NULL, NULL, &{tv_sec:0, tv_usec:10})`
 /// for a 10 µs yield, because `usleep(3)` is not listed as async-signal-safe.
 /// `wait_threads` is called from the main thread (not a signal handler), so
 /// `std::thread::sleep` is acceptable here.
 /// The C timeout check compares only `tv_sec` fields (whole-second granularity).
 /// This port uses `Instant`, which is strictly more precise.
 /// PERF(port): spinning with 10 µs sleeps is identical to the C implementation;
 /// profile in Phase B to verify.
    fn wait_threads() {
        let deadline = Instant::now() + Duration::from_secs(RUN_ON_THREADS_TIMEOUT_SECS);

        loop {
            std::thread::sleep(Duration::from_micros(10));

            let done = G_NUM_THREADS_DONE.load(Ordering::Relaxed);
            let expected = G_TIDS_LEN.load(Ordering::Relaxed);
            let now = Instant::now();

            if done >= expected || now >= deadline {
                if now >= deadline {
 // "wait_threads: waiting threads timed out")
                    // TODO(port): replace with server-level logger.
                    eprintln!("wait_threads(): waiting threads timed out");
                }
                break;
            }
        }
    }

 /// Reset all global state after a round of `run_on_threads` completes.
 /// Must only be called while `G_IN_PROGRESS` is set (i.e. from within
 /// `run_on_threads` before the flag is released).
 /// comment: "not a thread-safe function".
    fn cleanups() {
        if let Ok(mut guard) = G_CALLBACK.lock() {
            *guard = None;
        }
        G_TIDS_LEN.store(0, Ordering::Relaxed);
        G_NUM_THREADS_DONE.store(0, Ordering::Relaxed);
 // This must be last — it releases the in-progress lock for future callers.
        G_IN_PROGRESS.store(false, Ordering::Relaxed);
    }

 // ─── Public API (Linux) ───────────────────────────────────────────────────

 /// Register the process-wide `SIGUSR2` signal handler.
 /// Conceptual C translation (cannot be expressed safely here):
 /// ```c
 /// struct sigaction act;
 /// sigemptyset(&act.sa_mask);
 /// act.sa_flags = 0; // no SA_RESTART → EINTR on blocked syscalls
 /// act.sa_handler = invoke_callback;
 /// sigaction(SIGUSR2, &act, NULL);
 /// ```
    /// TODO(architect): `libc::sigaction` requires `unsafe`, which exceeds the
 /// zero-unsafe budget for `redis-core`. Architect must choose one:
 /// (a) Raise the unsafe budget for this file in `harness/unsafe-budgets.toml`.
 /// (b) Introduce the `signal-hook` crate and verify it covers this use-case.
 /// (c) Extract the `sigaction` call to a small `unsafe`-permitted shim
 /// `redis-core::signal_setup` (or a new `redis-sys` crate).
    pub fn init() {
 // Body intentionally empty — signal registration requires unsafe.
        // See TODO(architect) above.
    }

 /// Invoke `callback` on each thread in `tids` and wait for completion.
 /// Sends `THREADS_SIGNAL` (`SIGUSR2`) to each kernel thread ID via `tgkill(2)`,
 /// waits for all threads to finish the callback (or until a 2-second timeout),
 /// then resets global state.
 /// Returns `true` on success, `false` if another invocation is already
 /// progress.
 /// — `__attribute__((noinline))`.
    /// TODO(architect): the `tgkill` loop (see comment below) requires two
 /// `unsafe` calls — `libc::getpid` and `libc::syscall(SYS_tgkill,...)` —
 /// which are both blocked by the zero-unsafe budget. Provide either approved
 /// wrappers in a `redis-core::syscall` helper or raise the budget for this file.
    pub fn run_on_threads(tids: &[i32], callback: RunOnThreadCallback) -> bool {
        if test_and_start() {
            return false;
        }

        if let Ok(mut guard) = G_CALLBACK.lock() {
            *guard = Some(callback);
        }

        G_TIDS_LEN.store(tids.len(), Ordering::Relaxed);

 // Reset before signaling: handles the case where a prior run timed out
 // and cleanups executed before some threads incremented the counter.
        G_NUM_THREADS_DONE.store(0, Ordering::Relaxed);

 // for (size_t i = 0; i < tids_len; ++i)
 // syscall(SYS_tgkill, pid, tids[i], THREADS_SIGNAL);
        // TODO(architect): `libc::getpid()` (to obtain the process ID for tgkill)
 // and `libc::syscall(libc::SYS_tgkill, pid, tid, THREADS_SIGNAL)` both
 // require `unsafe`. The placeholder loop below must be replaced once
 // unsafe budget is approved or a safe wrapper is provided.
        for _tid in tids {
            // TODO(port): send THREADS_SIGNAL to *_tid via tgkill — requires
 // unsafe syscall. Placeholder only.
        }

        wait_threads();

        cleanups();

        true
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-Linux stubs
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::RunOnThreadCallback;

 /// No-op on non-Linux platforms.
    pub fn init() {}

 /// No-op on non-Linux platforms; always returns `true`.
 /// ```c
 /// int ThreadsManager_runOnThreads(pid_t *tids, size_t tids_len,
 /// run_on_thread_cb callback) {
 /// UNUSED(tids); UNUSED(tids_len); UNUSED(callback);
 /// return 1;
 /// }
 /// ```
    pub fn run_on_threads(_tids: &[i32], _callback: RunOnThreadCallback) -> bool {
        true
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Platform-independent public surface
// ──────────────────────────────────────────────────────────────────────────────

/// Register the `SIGUSR2` signal handler for thread-based callback dispatch.
/// Must be called once at server startup before any call to [`run_on_threads`].
/// No-op on non-Linux platforms.
pub fn init() {
    imp::init();
}

/// Invoke `callback` on each thread in `tids` by sending SIGUSR2 via `tgkill(2)`.
/// Blocks until all threads have invoked the callback (or a 2-second timeout
/// expires), then resets state for the next call.
/// Returns `true` on success, `false` if another invocation is already
/// progress. Always returns `true` on non-Linux platforms.
pub fn run_on_threads(tids: &[i32], callback: RunOnThreadCallback) -> bool {
    imp::run_on_threads(tids, callback)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  + src/threads_mngr.h  (70 lines)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         10  (5 TODO(port) + 5 TODO(architect))
//   port_notes:    2
//   unsafe_blocks: 0
//   notes: State machine (atomic flag, done counter, global callback, cleanup
//          loop) faithfully ported with safe Rust atomics and Mutex.  The two
//          syscall sites (sigaction in init, tgkill loop in run_on_threads) and
//          the async-signal-safe callback storage pattern all require unsafe and
//          are stubbed with TODO(architect).  invoke_callback body is logically
//          correct but its Mutex lock is not async-signal-safe (TODO(architect)
//          + TODO(port)).  Non-Linux stubs are complete no-ops matching the C
//          #else branch.  Phase B must wire up the syscall wrappers and raise
//          the unsafe budget (or introduce signal-hook / a safe shim crate).
// ──────────────────────────────────────────────────────────────────────────────

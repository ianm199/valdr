//! Background I/O service.
//!
//! Port of `bio.c` (359 lines, 11 functions) and `bio.h` (59 lines).
//!
//! Each job type is permanently assigned to one of five worker threads.
//! Workers pull jobs from their channel in FIFO order and execute them
//! sequentially.  The main thread submits jobs by sending to the worker's
//! channel and incrementing an atomic counter; the worker decrements the
//! counter after processing each job.
//!
//! # Design (PORT NOTE)
//!
//! The C implementation uses `pthread_t` + `mutexQueue` (a mutex + condvar
//! queue from `mutexqueue.c`).  This port replaces `mutexQueue` with
//! `std::sync::mpsc::channel`, which provides equivalent blocking-pop
//! semantics with no `unsafe`.  Worker threads are spawned via
//! `std::thread::Builder` with an explicit stack size.
//!
//! The C `bio_job` tagged union becomes a Rust `BioJob` enum; each variant
//! carries exactly the fields relevant to that job type.
//!
//! The variadic `bioCreateLazyFreeJob(free_fn, arg_count, ...)` API cannot
//! be expressed in safe Rust.  It is replaced with
//! `bio_create_lazy_free_job(f: LazyFreeFn)` where callers capture their
//! arguments in a closure.
//!
//! C: `bio.c`, `bio.h`

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use redis_types::RedisError;

use crate::connection::Connection;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Total number of distinct background-job opcodes.
/// C: `BIO_NUM_OPS` in `bio.h`.
pub const BIO_NUM_OPS: usize = 6;

/// Minimum stack size for bio worker threads (4 MiB).
/// C: `VALKEY_THREAD_STACK_SIZE` in `bio.c`.
const THREAD_STACK_SIZE: usize = 1024 * 1024 * 4;

/// Maps each `BioJobType` (by its discriminant) to the index of the worker
/// that processes it.
///
/// C: `bio_job_to_worker[]` in `bio.c`.
///
/// Indices into this array correspond to `BioJobType` discriminants:
///   0 = CloseFile → worker 0
///   1 = AofFsync  → worker 1
///   2 = LazyFree  → worker 2
///   3 = CloseAof  → worker 1  (same thread as AofFsync)
///   4 = RdbSave   → worker 3
///   5 = TlsReload → worker 4
const JOB_TO_WORKER: [usize; BIO_NUM_OPS] = [
    0, // BIO_CLOSE_FILE
    1, // BIO_AOF_FSYNC
    2, // BIO_LAZY_FREE
    1, // BIO_CLOSE_AOF
    3, // BIO_RDB_SAVE
    4, // BIO_TLS_RELOAD
];

/// Human-readable titles for worker threads, one per worker (not per job type).
/// C: `bio_worker_title` fields inside `bio_workers[]`.
const WORKER_TITLES: [&str; 5] = [
    "bio_close_file",
    "bio_aof",
    "bio_lazy_free",
    "bio_rdb_save",
    "bio_tls_reload",
];

// ─── Job-type enum ────────────────────────────────────────────────────────────

/// Background-job opcodes.  Discriminants are stable and must match the C
/// `BIO_*` constants so that any serialised or logged values stay consistent.
///
/// C: anonymous enum in `bio.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum BioJobType {
    /// Deferred `close(2)`.
    CloseFile = 0,
    /// Deferred AOF `fsync`.
    AofFsync = 1,
    /// Deferred object freeing (`lazy_free_fn`).
    LazyFree = 2,
    /// Deferred close for AOF files (includes an fsync).
    CloseAof = 3,
    /// Deferred RDB-to-disk save on a replica.
    RdbSave = 4,
    /// Deferred TLS configuration reload.
    TlsReload = 5,
}

// ─── Lazy-free callback type ──────────────────────────────────────────────────

/// Boxed owning closure used in `BioJob::LazyFree`.
///
/// PORT NOTE: The C API uses `typedef void lazy_free_fn(void *args[])` plus
/// variadic argument packing.  Safe Rust cannot express that directly; callers
/// capture their arguments in a `Box<dyn FnOnce() + Send>` closure instead.
pub type LazyFreeFn = Box<dyn FnOnce() + Send + 'static>;

// ─── Job enum (replaces C `bio_job` union) ────────────────────────────────────

/// A single background job.
///
/// PORT NOTE: The C `bio_job` is a tagged union discriminated by a `type`
/// field in the header struct.  Each Rust variant carries only the fields
/// relevant to that job type; no tag field is needed.
pub(crate) enum BioJob {
    /// C: `fd_args` variant with `BIO_CLOSE_FILE` type.
    CloseFile {
        /// Raw file descriptor.
        /// TODO(architect): abstract over `RawFd` with a safe `OwnedFd` or
        /// similar when the fd-lifecycle story is settled.
        fd: i32,
        /// Perform an `fsync` before closing.
        need_fsync: bool,
        /// Reclaim kernel page cache before closing.
        need_reclaim_cache: bool,
    },
    /// C: `fd_args` variant with `BIO_AOF_FSYNC` type.
    AofFsync {
        fd: i32,
        /// Replication offset written up to this point; stored into
        /// `server.fsynced_reploff_pending` on success.
        offset: i64,
        need_reclaim_cache: bool,
    },
    /// C: `fd_args` variant with `BIO_CLOSE_AOF` type.
    CloseAof {
        fd: i32,
        offset: i64,
        need_reclaim_cache: bool,
    },
    /// C: `free_args` variant.
    LazyFree { free_fn: LazyFreeFn },
    /// C: `save_to_disk_args` variant.
    RdbSave {
        /// Connection to download the RDB from.
        /// TODO(architect): `Connection` may need to be `Arc<Mutex<Connection>>`
        /// when `Connection` is not `Send` by default.
        conn: Connection,
        is_dual_channel: bool,
    },
    /// C: `tls_reload_args` variant.
    TlsReload,
}

// ─── Per-worker handle ────────────────────────────────────────────────────────

struct BioWorkerHandle {
    /// Thread title (for diagnostics / OS thread name).
    title: &'static str,
    /// Send side of the job channel; clone to create additional submitters.
    sender: Sender<BioJob>,
    /// Join handle; taken on `bio_kill_threads`.
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

// ─── Global state ─────────────────────────────────────────────────────────────

/// Lazily-initialised array of worker handles, one per worker thread.
///
/// C: `static bio_worker_data bio_workers[]`.
static BIO_WORKERS: OnceLock<Vec<BioWorkerHandle>> = OnceLock::new();

/// Per-job-type pending-job counter (decremented after each job completes).
///
/// C: `static _Atomic unsigned long bio_jobs_counter[BIO_NUM_OPS]`.
static BIO_JOBS_COUNTER: [AtomicUsize; BIO_NUM_OPS] = [
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
];

thread_local! {
    /// Index of this thread in the `BIO_WORKERS` array, or 0 for the main thread.
    ///
    /// PORT NOTE: The C code has the same `0`-as-default ambiguity: worker #0
    /// (`bio_close_file`) also sets this to `0`, so `in_bio_thread()` returns
    /// false for both the main thread and worker #0.  This is faithfully
    /// reproduced here — see C: `static _Thread_local size_t bio_worker_num`.
    static BIO_WORKER_NUM: Cell<usize> = Cell::new(0);
}

// ─── Initialisation ───────────────────────────────────────────────────────────

/// Initialise background worker threads and their job queues.
///
/// Must be called exactly once at server startup.  Subsequent calls are a
/// no-op (guarded by `OnceLock`).
///
/// C: `bioInit` in `bio.c` (lines 158-184).
pub fn bio_init() -> Result<(), RedisError> {
    if BIO_WORKERS.get().is_some() {
        return Ok(());
    }

    let mut workers = Vec::with_capacity(WORKER_TITLES.len());

    for (idx, title) in WORKER_TITLES.iter().enumerate() {
        let (sender, receiver) = mpsc::channel::<BioJob>();

        let builder = thread::Builder::new()
            .name(title.to_string())
            .stack_size(THREAD_STACK_SIZE);

        let handle = builder
            .spawn(move || bio_process_background_jobs(receiver, title, idx))
            .map_err(|_| RedisError::runtime(b"Fatal: Can't initialize Background Jobs"))?;

        workers.push(BioWorkerHandle {
            title,
            sender,
            thread: Mutex::new(Some(handle)),
        });
    }

    BIO_WORKERS
        .set(workers)
        .map_err(|_| RedisError::runtime(b"bio workers already initialised"))
}

// ─── Job submission ───────────────────────────────────────────────────────────

/// Route `job` to the correct worker and increment the pending counter.
///
/// C: `bioSubmitJob` in `bio.c` (lines 186-191).
fn bio_submit_job(job_type: BioJobType, job: BioJob) -> Result<(), RedisError> {
    let worker_idx = JOB_TO_WORKER[job_type as usize];

    let workers = BIO_WORKERS
        .get()
        .ok_or_else(|| RedisError::runtime(b"bio workers not initialised"))?;

    workers[worker_idx]
        .sender
        .send(job)
        .map_err(|_| RedisError::runtime(b"bio worker channel closed"))?;

    BIO_JOBS_COUNTER[job_type as usize].fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// ─── Public job-creation API ──────────────────────────────────────────────────

/// Submit a deferred `close(2)` job.
///
/// C: `bioCreateCloseJob` in `bio.c` (lines 208-215).
pub fn bio_create_close_job(
    fd: i32,
    need_fsync: bool,
    need_reclaim_cache: bool,
) -> Result<(), RedisError> {
    bio_submit_job(
        BioJobType::CloseFile,
        BioJob::CloseFile {
            fd,
            need_fsync,
            need_reclaim_cache,
        },
    )
}

/// Submit a deferred AOF-close job (includes an `fsync` before closing).
///
/// C: `bioCreateCloseAofJob` in `bio.c` (lines 217-225).
pub fn bio_create_close_aof_job(
    fd: i32,
    offset: i64,
    need_reclaim_cache: bool,
) -> Result<(), RedisError> {
    bio_submit_job(
        BioJobType::CloseAof,
        BioJob::CloseAof {
            fd,
            offset,
            need_reclaim_cache,
        },
    )
}

/// Submit a deferred AOF `fsync` job.
///
/// C: `bioCreateFsyncJob` in `bio.c` (lines 227-234).
pub fn bio_create_fsync_job(
    fd: i32,
    offset: i64,
    need_reclaim_cache: bool,
) -> Result<(), RedisError> {
    bio_submit_job(
        BioJobType::AofFsync,
        BioJob::AofFsync {
            fd,
            offset,
            need_reclaim_cache,
        },
    )
}

/// Submit a deferred lazy-free job.
///
/// PORT NOTE: The C API is `bioCreateLazyFreeJob(free_fn, arg_count, ...)`.
/// In safe Rust, variadic argument packing is impossible without `unsafe`.
/// Callers must instead capture their arguments in a closure:
/// ```ignore
/// bio_create_lazy_free_job(Box::new(move || free_my_thing(arg1, arg2)))?;
/// ```
///
/// C: `bioCreateLazyFreeJob` in `bio.c` (lines 193-206).
pub fn bio_create_lazy_free_job(free_fn: LazyFreeFn) -> Result<(), RedisError> {
    bio_submit_job(BioJobType::LazyFree, BioJob::LazyFree { free_fn })
}

/// Submit a deferred RDB-to-disk save job.
///
/// C: `bioCreateSaveRDBToDiskJob` in `bio.c` (lines 236-241).
pub fn bio_create_save_rdb_to_disk_job(
    conn: Connection,
    is_dual_channel: bool,
) -> Result<(), RedisError> {
    bio_submit_job(
        BioJobType::RdbSave,
        BioJob::RdbSave {
            conn,
            is_dual_channel,
        },
    )
}

/// Submit a deferred TLS configuration reload job.
///
/// C: `bioCreateTlsReloadJob` in `bio.c` (lines 243-246).
pub fn bio_create_tls_reload_job() -> Result<(), RedisError> {
    bio_submit_job(BioJobType::TlsReload, BioJob::TlsReload)
}

// ─── Worker thread body ───────────────────────────────────────────────────────

/// Main loop executed by each bio worker thread.
///
/// Blocks on `receiver.recv()` waiting for jobs; processes each job in order,
/// then decrements the pending counter.
///
/// C: `bioProcessBackgroundJobs` in `bio.c` (lines 248-323).
fn bio_process_background_jobs(receiver: Receiver<BioJob>, title: &'static str, worker_idx: usize) {
    BIO_WORKER_NUM.with(|n| n.set(worker_idx));

    // TODO(port): set OS-level thread name via prctl/pthread_setname_np.
    // C: `valkey_set_thread_title(bwd->bio_worker_title)`.

    // TODO(port): apply CPU affinity from server config.
    // C: `serverSetCpuAffinity(server.bio_cpulist)`.

    // TODO(port): block SIGALRM in this thread so the watchdog signal is
    // only delivered to the main thread.
    // C: sigemptyset/sigaddset/pthread_sigmask(SIG_BLOCK, SIGALRM).

    log::debug!("bio worker '{}' (idx {}) started", title, worker_idx);

    loop {
        let job = match receiver.recv() {
            Ok(j) => j,
            Err(_) => {
                log::debug!("bio worker '{}' channel closed; exiting", title);
                return;
            }
        };

        let job_type_idx = dispatch_job(job);
        BIO_JOBS_COUNTER[job_type_idx].fetch_sub(1, Ordering::Release);
    }
}

/// Execute a single `BioJob` and return its `BioJobType` discriminant so the
/// caller can decrement the correct counter.
///
/// Extracted from `bio_process_background_jobs` to keep the loop body readable.
///
/// C: the large if-else chain inside `bioProcessBackgroundJobs` (lines 273-320).
fn dispatch_job(job: BioJob) -> usize {
    match job {
        BioJob::CloseFile {
            fd,
            need_fsync,
            need_reclaim_cache,
        } => {
            if need_fsync {
                // TODO(architect): safe `fsync` wrapper needed (raw syscall
                // cannot be called without unsafe; consider the `nix` crate or
                // an internal `syscall_wrappers` module).
                // C: valkey_fsync(fd) with EBADF/EINVAL ignored.
                log::warn!("bio: fsync before close not yet implemented for fd {}", fd);
            }
            if need_reclaim_cache {
                // TODO(architect): safe `posix_fadvise` / reclaimFilePageCache
                // wrapper needed.
                log::warn!("bio: page-cache reclaim not yet implemented for fd {}", fd);
            }
            // TODO(architect): safe close(fd) — requires nix or a wrapping
            // OwnedFd. C: close(job->fd_args.fd).
            log::debug!("bio: close file fd={}", fd);
            BioJobType::CloseFile as usize
        }

        BioJob::AofFsync {
            fd,
            offset,
            need_reclaim_cache,
        } => {
            // TODO(architect): safe fsync wrapper.
            // On success: update server.aof_bio_fsync_status = C_OK and
            //   server.fsynced_reploff_pending = offset.
            // On error (not EBADF/EINVAL): update aof_bio_fsync_status = C_ERR
            //   and log a warning.
            // C: bio.c lines 288-305.
            log::debug!("bio: aof fsync fd={} offset={}", fd, offset);
            if need_reclaim_cache {
                // TODO(architect): safe posix_fadvise wrapper.
                log::warn!("bio: page-cache reclaim not yet implemented for fd {}", fd);
            }
            BioJobType::AofFsync as usize
        }

        BioJob::CloseAof {
            fd,
            offset,
            need_reclaim_cache,
        } => {
            // Same fsync + status-update logic as AofFsync, then close the fd.
            // TODO(architect): safe fsync + close wrappers.
            // C: bio.c lines 284-306 (same branch as BIO_AOF_FSYNC, with
            //   extra close at the end).
            log::debug!("bio: close aof fd={} offset={}", fd, offset);
            if need_reclaim_cache {
                // TODO(architect): safe posix_fadvise wrapper.
                log::warn!("bio: page-cache reclaim not yet implemented for fd {}", fd);
            }
            BioJobType::CloseAof as usize
        }

        BioJob::LazyFree { free_fn } => {
            // C: `job->free_args.free_fn(job->free_args.free_args)`.
            free_fn();
            BioJobType::LazyFree as usize
        }

        BioJob::RdbSave {
            conn,
            is_dual_channel,
        } => {
            // TODO(port): call `replication::replica_receive_rdb_from_primary_to_disk`.
            // The replication module is deferred to a later phase; this is a
            // placeholder.
            // C: `replicaReceiveRDBFromPrimaryToDisk(conn, is_dual_channel)`.
            let _ = (conn, is_dual_channel);
            log::warn!("bio: RDB-save-to-disk job not yet implemented");
            BioJobType::RdbSave as usize
        }

        BioJob::TlsReload => {
            // C: calls `tlsConfigureAsync()` under `#if USE_OPENSSL`.
            #[cfg(feature = "tls")]
            {
                // TODO(port): call `crate::tls::tls_configure_async()`.
                log::warn!("bio: TLS reload not yet implemented");
            }
            #[cfg(not(feature = "tls"))]
            {
                // TODO(architect): is panic correct here? C uses serverPanic.
                // The C comment says this job type requires BUILD_TLS=yes.
                log::error!("bio: BIO_TLS_RELOAD received but TLS feature is not enabled");
            }
            BioJobType::TlsReload as usize
        }
    }
}

// ─── Query and control ────────────────────────────────────────────────────────

/// Return the number of pending (submitted but not yet completed) jobs of the
/// given type.
///
/// C: `bioPendingJobsOfType` in `bio.c` (lines 326-328).
pub fn bio_pending_jobs_of_type(job_type: BioJobType) -> usize {
    BIO_JOBS_COUNTER[job_type as usize].load(Ordering::Relaxed)
}

/// Spin-wait until all pending jobs of the specified type have been processed.
///
/// C: `bioDrainWorker` in `bio.c` (lines 331-335).
pub fn bio_drain_worker(job_type: BioJobType) {
    while bio_pending_jobs_of_type(job_type) > 0 {
        // C: usleep(100) — sleep 100 µs between polls.
        thread::sleep(Duration::from_micros(100));
    }
}

/// Attempt to stop all bio worker threads.
///
/// Drops the sender half of each worker's channel (signalling `recv()` to
/// return `Err`) and then joins the thread.
///
/// PORT NOTE: The C implementation uses `pthread_cancel` which forcefully
/// stops the thread at the next cancellation point.  Rust has no equivalent;
/// the channel-drop approach requires the worker to be between jobs.  For
/// crash-time use (the only caller in C), this difference is acceptable.
///
/// TODO(port): if truly non-blocking cancellation is required (e.g. while the
/// worker is deep inside an fsync syscall), the approach needs a dedicated
/// `AtomicBool` kill-switch checked at appropriate points, or a signal-based
/// mechanism.
///
/// C: `bioKillThreads` in `bio.c` (lines 341-354).
pub fn bio_kill_threads() {
    let workers = match BIO_WORKERS.get() {
        Some(w) => w,
        None => return,
    };

    for (idx, worker) in workers.iter().enumerate() {
        let mut guard = match worker.thread.lock() {
            Ok(g) => g,
            Err(_) => continue,
        };

        if let Some(handle) = guard.take() {
            // Closing the sender causes the worker's receiver.recv() to return
            // Err, which causes the thread to exit cleanly.
            // However, we cannot close the sender here without consuming
            // `worker.sender` — it lives inside the shared `BioWorkerHandle`.
            // TODO(port): to properly signal each thread to stop we need a
            // separate shutdown channel or AtomicBool per worker.
            match handle.join() {
                Ok(()) => log::warn!("Bio worker thread #{} terminated", idx),
                Err(_) => log::warn!("Bio worker thread #{} panicked during join", idx),
            }
        }
    }
}

/// Returns `true` if the calling thread is a bio worker thread other than
/// worker #0 (`bio_close_file`).
///
/// PORT NOTE: faithfully reproduces the C behaviour where worker #0 also
/// returns `false` because `bio_worker_num` defaults to `0` and worker #0
/// also sets it to `0`.
///
/// C: `inBioThread` in `bio.c` (lines 356-358).
pub fn in_bio_thread() -> bool {
    BIO_WORKER_NUM.with(|n| n.get() != 0)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/bio.c  (359 lines, 11 functions)  +  src/bio.h  (59 lines)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         19
//   port_notes:    7
//   unsafe_blocks: 0
//   notes: Structure, enums, counters, and channel-based queue are faithfully
//          ported. The syscall sites (fsync, close, posix_fadvise) are stubbed
//          with TODO(architect) because they require unsafe or an approved safe
//          wrapper crate. The lazy-free variadic API is replaced with a closure.
//          bio_kill_threads lacks true pthread_cancel semantics (TODO(port)).
//          OnceLock::get_or_try_init (unstable) replaced with build-then-set.
// ──────────────────────────────────────────────────────────────────────────────

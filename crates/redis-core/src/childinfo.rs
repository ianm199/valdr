//! Child-info pipe — communicates RDB/AOF/replication progress from a
// Deferred feature: BGSAVE COW-memory reporting pipe; wired when safe Unix pipe
// abstraction (nix/os_pipe/std::pipe) is approved for the crate.
#![allow(dead_code, private_interfaces)]
//! forked child process to the parent via a non-blocking Unix pipe.
//!
//! C: `src/childinfo.c` (189 lines, 6 functions)
//!
//! PORT NOTE: The C code stores throttle state as `static` locals inside
//! `sendChildInfoGeneric` and keeps the partial-read buffer as a `static`
//! local inside `readChildInfo`. Rust promotes those to explicit structs
//! (`ChildInfoSender` for the child side, `ChildInfoChannel` for the
//! parent side) so that ownership is explicit and state is not hidden in
//! module-level singletons.
//!
//! PORT NOTE: Raw Unix pipe I/O (`read(2)` / `write(2)`) requires either
//! `unsafe` or a crate such as `nix`. Since pilot crates have an unsafe
//! budget of 0, the pipe-end accessors are gated behind
//! `TODO(architect)` comments and compile-time stub types.

use std::io::{Read, Write};

use redis_types::RedisError;

use crate::server::RedisServer;

// TODO(architect): need a safe Unix pipe abstraction. Options:
//   (a) add `nix` crate dep (safe wrappers around POSIX read/write/pipe2),
//   (b) add `os_pipe` crate dep,
//   (c) use `std::pipe` once MSRV reaches Rust 1.87+,
//   (d) uplift unsafe budget for this module after architect sign-off.
// Until resolved, `PipeReader` / `PipeWriter` are placeholder stubs.

/// Opaque reader end of the child-info pipe (parent holds this).
///
/// TODO(architect): replace with real non-blocking pipe reader once the
/// safe-pipe abstraction above is resolved.
pub struct PipeReader {
    fd: i32,
}

/// Opaque writer end of the child-info pipe (child holds this).
///
/// TODO(architect): replace with real non-blocking pipe writer once the
/// safe-pipe abstraction above is resolved.
pub struct PipeWriter {
    fd: i32,
}

impl Read for PipeReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        // TODO(architect): implement with nix::unistd::read or equivalent
        // safe wrapper.  Cannot use raw read(2) here without unsafe budget.
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "PipeReader::read — awaiting safe-pipe architect decision",
        ))
    }
}

impl Write for PipeWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        // TODO(architect): implement with nix::unistd::write or equivalent.
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "PipeWriter::write — awaiting safe-pipe architect decision",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ── Constants ────────────────────────────────────────────────────────────────

/// Throttle factor: CoW measurement runs at most once per
/// `cow_update_cost * CHILD_COW_DUTY_CYCLE` microseconds.
///
/// C: server.h `#define CHILD_COW_DUTY_CYCLE 100`
pub const CHILD_COW_DUTY_CYCLE: u64 = 100;

/// Monotonic microsecond counter.  Matches C `typedef uint64_t monotime`.
///
/// C: monotonic.h
pub type MonoTime = u64;

// ── ChildInfoType ────────────────────────────────────────────────────────────

/// Which kind of information a child-info message carries.
///
/// C: server.h `typedef enum childInfoType { … } childInfoType;`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChildInfoType {
    /// Periodic in-progress update (CoW size, keys processed, etc.)
    #[default]
    CurrentInfo,
    /// Final CoW peak after AOF rewrite child exits.
    AofCowSize,
    /// Final CoW peak after RDB save child exits.
    RdbCowSize,
    /// Final CoW peak after module child exits.
    ModuleCowSize,
    /// Final CoW peak after slot-migration child exits.
    SlotMigrationCowSize,
    /// Bytes written to replication output buffer by the child.
    ReplOutputBytes,
}

// ── ChildInfoData ────────────────────────────────────────────────────────────

/// Wire-format payload exchanged over the child-info pipe.
///
/// C: childinfo.c `typedef struct { … } child_info_data;`
///
/// PORT NOTE: In C this struct is written/read as raw bytes via
/// `write(2)` / `read(2)`.  Rust serialisation is left as
/// `TODO(architect)` pending the safe-pipe resolution.
#[derive(Debug, Clone, Default)]
struct ChildInfoData {
    /// Keys processed so far.
    keys: usize,
    /// Copy-on-write bytes measured in the child address space.
    cow: usize,
    /// Timestamp (µs) when `cow` was last measured.
    cow_updated: MonoTime,
    /// Fractional save progress in [0.0, 1.0]; −1.0 means "unknown".
    progress: f64,
    /// Bytes written to the replication output buffer.
    repl_output_bytes: usize,
    /// Discriminant — what kind of information this packet carries.
    information_type: ChildInfoType,
}

impl ChildInfoData {
    /// Serialise to a fixed-size byte buffer for pipe transport.
    ///
    /// TODO(architect): choose a stable wire encoding.  Options:
    ///   (a) `bytemuck::bytes_of` for repr(C) + repr(packed) structs,
    ///   (b) manual little-endian encoding (portable, no dep),
    ///   (c) keep the C struct layout with `zerocopy`.
    /// For now this is a stub that returns an error.
    fn encode(&self) -> Result<Vec<u8>, RedisError> {
        // TODO(port): implement stable binary encoding of ChildInfoData.
        Err(RedisError::runtime(
            b"ChildInfoData::encode - not yet implemented",
        ))
    }

    /// Deserialise from a fixed-size byte buffer received from the pipe.
    ///
    /// TODO(architect): must use the same encoding chosen in `encode`.
    fn decode(_bytes: &[u8]) -> Result<Self, RedisError> {
        // TODO(port): implement decode to match encode.
        Err(RedisError::runtime(
            b"ChildInfoData::decode - not yet implemented",
        ))
    }

    /// Number of bytes in the wire representation.
    ///
    /// Mirrors `sizeof(child_info_data)` in C.  Must match `encode` output.
    ///
    /// TODO(port): fix this once encode/decode are implemented.
    fn wire_len() -> usize {
        // Placeholder — real value depends on the chosen encoding.
        // C struct is approximately 5 * 8 + 8 + 4 = 52 bytes on 64-bit,
        // but alignment/padding make it target-dependent.
        56
    }
}

// ── ChildInfoChannel (parent side) ──────────────────────────────────────────

/// Parent-side state for the child-info pipe.
///
/// Holds the reader file descriptor and accumulates partial reads across
/// `receive_child_info` calls, mirroring the C static-local buffer in
/// `readChildInfo`.
///
/// PORT NOTE: The C code stores `child_info_pipe[2]` and `child_info_nread`
/// directly on `server`.  TODO(architect): decide whether these fields move
/// onto `RedisServer` or stay in a separate `ChildInfoChannel` that `RedisServer`
/// owns.  For Phase A they live here.
pub struct ChildInfoChannel {
    /// Reader end of the pipe (parent reads, child writes).
    reader: Option<PipeReader>,
    /// Writer end (held here temporarily until child inherits it after fork).
    writer: Option<PipeWriter>,
    /// Bytes accumulated in `buffer` so far (handles short reads).
    nread: usize,
    /// Partial-read accumulation buffer.
    buffer: Vec<u8>,
}

impl ChildInfoChannel {
    /// Construct a closed (no pipe) channel.
    pub fn new() -> Self {
        Self {
            reader: None,
            writer: None,
            nread: 0,
            buffer: vec![0u8; ChildInfoData::wire_len()],
        }
    }

    /// Returns `true` if the read end is open.
    pub fn is_open(&self) -> bool {
        self.reader.is_some()
    }
}

impl Default for ChildInfoChannel {
    fn default() -> Self {
        Self::new()
    }
}

// ── open_child_info_pipe ─────────────────────────────────────────────────────

/// Create the child-info pipe and initialise the channel.
///
/// C: childinfo.c `openChildInfoPipe` (lines 46–54)
///
/// PORT NOTE: The C implementation calls `anetPipe(server.child_info_pipe,
/// O_NONBLOCK, 0)` and falls back to `closeChildInfoPipe()` on error.
/// Rust must construct a non-blocking pipe without `unsafe`; see
/// `TODO(architect)` on `PipeReader` / `PipeWriter` above.
pub fn open_child_info_pipe(channel: &mut ChildInfoChannel) -> Result<(), RedisError> {
    // TODO(architect): call the safe pipe-creation primitive here.
    // On success set channel.reader / channel.writer and reset channel.nread.
    // On failure call close_child_info_pipe (mirrors the C fallback path).
    close_child_info_pipe(channel);
    Err(RedisError::runtime(
        b"open_child_info_pipe - awaiting safe-pipe architect decision",
    ))
}

// ── close_child_info_pipe ────────────────────────────────────────────────────

/// Close both ends of the child-info pipe and reset state.
///
/// C: childinfo.c `closeChildInfoPipe` (lines 57–65)
///
/// In C, only closes if at least one fd is not −1; Rust simply drops the
/// `Option` values which triggers `Drop` on the underlying OS resource.
pub fn close_child_info_pipe(channel: &mut ChildInfoChannel) {
    // C: if (server.child_info_pipe[0] != -1 || server.child_info_pipe[1] != -1)
    if channel.reader.is_some() || channel.writer.is_some() {
        channel.reader = None; // drops PipeReader → closes fd
        channel.writer = None; // drops PipeWriter → closes fd
        channel.nread = 0;
    }
}

// ── ChildInfoSender (child side) ─────────────────────────────────────────────

/// Child-side state that throttles CoW-size measurements.
///
/// PORT NOTE: The C `sendChildInfoGeneric` uses `static` local variables
/// for throttle state; Rust makes this explicit as a struct so the child
/// process owns and mutates it through normal borrow mechanics.
pub struct ChildInfoSender {
    /// Timestamp of the last CoW measurement (0 = never measured).
    cow_updated: MonoTime,
    /// Wall-clock cost of the last `zmalloc_get_private_dirty` call (µs).
    cow_update_cost: u64,
    /// Most recent CoW byte count.
    cow: usize,
    /// Peak CoW byte count across all measurements.
    peak_cow: usize,
    /// Running sum of all CoW measurements (for average logging).
    sum_cow: u64,
    /// Number of CoW measurements taken.
    update_count: u64,
    /// Writer end of the pipe.
    writer: PipeWriter,
}

impl ChildInfoSender {
    /// Construct a sender owning the write end of the pipe.
    pub fn new(writer: PipeWriter) -> Self {
        Self {
            cow_updated: 0,
            cow_update_cost: 0,
            cow: 0,
            peak_cow: 0,
            sum_cow: 0,
            update_count: 0,
            writer,
        }
    }
}

// ── send_child_info_generic ───────────────────────────────────────────────────

/// Send a child-info packet to the parent process.
///
/// Throttles the expensive `zmalloc_get_private_dirty` call: the next
/// measurement is deferred for at least
/// `cow_update_cost × CHILD_COW_DUTY_CYCLE` microseconds.
///
/// C: childinfo.c `sendChildInfoGeneric` (lines 68–117)
///
/// # Parameters
/// - `sender`           — mutable sender state (replaces C `static` locals).
/// - `info_type`        — kind of info being reported.
/// - `keys`             — number of keys processed.
/// - `repl_output_bytes`— bytes written to replication output buffer.
/// - `progress`         — fractional progress [0.0, 1.0]; −1.0 = unknown.
/// - `pname`            — name of the child process for log messages.
/// - `now_us`           — current monotonic timestamp (µs); injected to
///                        avoid calling `getMonotonicUs` inside this fn.
///
/// PORT NOTE: The C function calls `getMonotonicUs()` twice (before and
/// after `zmalloc_get_private_dirty`).  Rust passes `now_us` as a
/// parameter so callers can use `crate::monotonic::get_monotonic_us()`
/// (or a test stub) without a global function pointer.
pub fn send_child_info_generic(
    sender: &mut ChildInfoSender,
    info_type: ChildInfoType,
    keys: usize,
    repl_output_bytes: usize,
    progress: f64,
    pname: &[u8],
    now_us: MonoTime,
) -> Result<(), RedisError> {
    // C: childinfo.c:86-101 — CoW throttle + measurement block
    //
    // Measure CoW if:
    //   - this is a final-report (not CURRENT_INFO), OR
    //   - we have never measured, OR
    //   - enough time has elapsed since the last measurement.
    let should_measure = info_type != ChildInfoType::CurrentInfo
        || sender.cow_updated == 0
        || now_us.saturating_sub(sender.cow_updated)
            > sender.cow_update_cost.saturating_mul(CHILD_COW_DUTY_CYCLE);

    if should_measure {
        // C: cow = zmalloc_get_private_dirty(-1);
        // TODO(port): call the Rust equivalent of zmalloc_get_private_dirty.
        // This reads /proc/self/smaps on Linux to tally private-dirty pages.
        // For now, substitute 0 until the zmalloc module is ported.
        let cow: usize = 0; // TODO(port): replace with zmalloc_get_private_dirty()

        let after_us: MonoTime = now_us; // TODO(port): call get_monotonic_us() after measurement
        let cost = after_us.saturating_sub(now_us);

        sender.cow = cow;
        sender.cow_updated = after_us;
        sender.cow_update_cost = cost;
        if cow > sender.peak_cow {
            sender.peak_cow = cow;
        }
        sender.sum_cow = sender.sum_cow.saturating_add(cow as u64);
        sender.update_count = sender.update_count.saturating_add(1);

        // C: childinfo.c:96-100 — log CoW size
        // C: serverLog(cow_info ? LL_NOTICE : LL_VERBOSE, "Fork CoW for %s: …", pname, …)
        let is_final = info_type != ChildInfoType::CurrentInfo;
        if cow > 0 || is_final {
            let avg_cow = if sender.update_count > 0 {
                (sender.sum_cow / sender.update_count) >> 20
            } else {
                0
            };
            // PORT NOTE: log crate macros used; maps to C serverLog().
            // PERF(port): format! allocation — profile in Phase B.
            let _ = (pname, cow >> 20, sender.peak_cow >> 20, avg_cow, is_final);
            // TODO(port): emit actual log message via log crate once server
            // logging infrastructure is wired up.
        }
    }

    // C: childinfo.c:103-108 — fill data struct
    let data = ChildInfoData {
        information_type: info_type,
        keys,
        repl_output_bytes,
        cow: sender.cow,
        cow_updated: sender.cow_updated,
        progress,
    };

    // C: childinfo.c:110-116 — write to pipe; exit on failure
    let bytes = data.encode()?;
    match sender.writer.write_all(&bytes) {
        Ok(()) => Ok(()),
        Err(_io_err) => {
            // C: childinfo.c:114-116
            //   serverLog(LL_WARNING, "Child failed reporting info to parent, exiting. %s", strerror(errno));
            //   exitFromChild(1);
            // TODO(architect): decide whether this should std::process::exit(1)
            // (matching C behaviour) or propagate as an error.  On child-side
            // pipe failure the C code always exits; propagating as Result gives
            // callers a chance to clean up first.
            Err(RedisError::runtime(
                b"child failed reporting info to parent",
            ))
        }
    }
}

// ── update_child_info ────────────────────────────────────────────────────────

/// Update server statistics from a decoded child-info packet.
///
/// C: childinfo.c `updateChildInfo` (lines 120–139)
///
/// TODO(architect): `RedisServer` (stub) is missing all `stat_*` fields
/// referenced here.  Add the following fields to `RedisServer`:
///   - `stat_current_cow_peak:  usize`
///   - `stat_current_cow_bytes: usize`
///   - `stat_current_cow_updated: MonoTime`
///   - `stat_current_save_keys_processed: usize`
///   - `stat_module_progress: f64`
///   - `stat_aof_cow_bytes:  usize`
///   - `stat_rdb_cow_bytes:  usize`
///   - `stat_module_cow_bytes: usize`
///   - `stat_slot_migration_cow_bytes: usize`
///   - `stat_net_repl_output_bytes: i64`
pub fn update_child_info(
    _server: &mut RedisServer,
    information_type: ChildInfoType,
    cow: usize,
    cow_updated: MonoTime,
    keys: usize,
    repl_output_bytes: usize,
    progress: f64,
) {
    // TODO(port): all stat_* assignments below are no-ops until RedisServer
    // gains these fields (see TODO(architect) above).

    // C: if (cow > server.stat_current_cow_peak) server.stat_current_cow_peak = cow;
    let _ = cow; // TODO(port): server.stat_current_cow_peak = cow.max(current_peak)

    match information_type {
        ChildInfoType::CurrentInfo => {
            // C: server.stat_current_cow_bytes = cow;
            // C: server.stat_current_cow_updated = cow_updated;
            // C: server.stat_current_save_keys_processed = keys;
            // C: if (progress != -1) server.stat_module_progress = progress;
            let _ = (cow_updated, keys);
            if progress != -1.0 {
                let _ = progress; // TODO(port): server.stat_module_progress = progress;
            }
        }
        ChildInfoType::AofCowSize => {
            // C: server.stat_aof_cow_bytes = server.stat_current_cow_peak;
            // TODO(port): server.stat_aof_cow_bytes = stat_current_cow_peak;
        }
        ChildInfoType::RdbCowSize => {
            // C: server.stat_rdb_cow_bytes = server.stat_current_cow_peak;
            // TODO(port): server.stat_rdb_cow_bytes = stat_current_cow_peak;
        }
        ChildInfoType::ModuleCowSize => {
            // C: server.stat_module_cow_bytes = server.stat_current_cow_peak;
            // TODO(port): server.stat_module_cow_bytes = stat_current_cow_peak;
        }
        ChildInfoType::SlotMigrationCowSize => {
            // C: server.stat_slot_migration_cow_bytes = server.stat_current_cow_peak;
            // TODO(port): server.stat_slot_migration_cow_bytes = stat_current_cow_peak;
        }
        ChildInfoType::ReplOutputBytes => {
            // C: server.stat_net_repl_output_bytes += (long long)repl_output_bytes;
            let _ = repl_output_bytes; // TODO(port): server.stat_net_repl_output_bytes += repl_output_bytes as i64;
        }
    }
}

// ── read_child_info ──────────────────────────────────────────────────────────

/// Attempt to read one complete child-info packet from the pipe.
///
/// Returns `Ok(Some(ChildInfoData))` when a full packet is available,
/// `Ok(None)` when the read was short (caller should try again later),
/// and `Err(…)` on a genuine I/O error.
///
/// C: childinfo.c `readChildInfo` (lines 145–171)
///
/// PORT NOTE: The C function uses output pointer parameters; Rust returns
/// a structured `Option<ChildInfoData>` instead, which is idiomatically
/// cleaner and avoids uninitialised-value footguns.
///
/// PORT NOTE: The C function stores a `static child_info_data buffer` and
/// a `server.child_info_nread` counter to handle short reads.  Both are
/// now fields on `ChildInfoChannel`.
pub fn read_child_info(
    channel: &mut ChildInfoChannel,
) -> Result<Option<ChildInfoData>, RedisError> {
    let wire_len = ChildInfoData::wire_len();

    // C: if (server.child_info_nread == wlen) server.child_info_nread = 0;
    if channel.nread == wire_len {
        channel.nread = 0;
    }

    let reader = match channel.reader.as_mut() {
        Some(r) => r,
        None => return Ok(None),
    };

    // C: nread = read(pipe[0], (char*)&buffer + nread, wlen - nread);
    let buf_slice = &mut channel.buffer[channel.nread..wire_len];
    match reader.read(buf_slice) {
        Ok(n) if n > 0 => {
            channel.nread += n;
        }
        Ok(_) => {} // 0-byte read (EAGAIN / non-blocking) — leave nread unchanged
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {} // non-blocking pipe, not ready
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {} // EINTR — retry on next call
        Err(_e) => {
            // TODO(port): log the I/O error before returning.
            return Err(RedisError::runtime(b"read_child_info: pipe read error"));
        }
    }

    // C: if (server.child_info_nread == wlen) { populate outputs; return 1; }
    if channel.nread == wire_len {
        let data = ChildInfoData::decode(&channel.buffer[..wire_len])?;
        Ok(Some(data))
    } else {
        Ok(None)
    }
}

// ── receive_child_info ───────────────────────────────────────────────────────

/// Drain all pending child-info packets from the pipe and apply each to
/// server statistics.
///
/// Called from the parent's event loop after the child wakes the pipe.
/// Processes packets in order so the final packet (exit status) wins.
///
/// C: childinfo.c `receiveChildInfo` (lines 174–188)
pub fn receive_child_info(
    server: &mut RedisServer,
    channel: &mut ChildInfoChannel,
) -> Result<(), RedisError> {
    // C: if (server.child_info_pipe[0] == -1) return;
    if !channel.is_open() {
        return Ok(());
    }

    // C: while (readChildInfo(&type, &cow, &cow_updated, &keys, &repl, &progress))
    //      updateChildInfo(type, cow, cow_updated, keys, repl, progress);
    loop {
        match read_child_info(channel)? {
            Some(data) => {
                update_child_info(
                    server,
                    data.information_type,
                    data.cow,
                    data.cow_updated,
                    data.keys,
                    data.repl_output_bytes,
                    data.progress,
                );
            }
            None => break,
        }
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/childinfo.c  (189 lines, 6 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         34
//   port_notes:    5
//   unsafe_blocks: 0
//   notes:         Logic is faithful. Two blockers: (1) safe pipe I/O
//                  abstraction (TODO(architect) × 1, stub PipeReader/PipeWriter);
//                  (2) RedisServer missing stat_* fields (TODO(architect) × 1).
//                  ChildInfoData encode/decode is stubbed (TODO(port) × 2).
//                  zmalloc_get_private_dirty and logging are stubbed.
// ──────────────────────────────────────────────────────────────────────────

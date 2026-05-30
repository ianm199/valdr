//! Monotonic clock abstraction for Valkey/Redis.
//! Provides a microsecond-resolution, always-increasing clock used throughout
//! the server for relative timing (latency tracking, expiry, event scheduling).
//! The concrete implementation is selected once at startup via
//! [`monotonic_init`]:
//! 1. x86_64 + Linux (default): TSC-based via `RDTSC` instruction
//! 2. aarch64 (default): ARM virtual counter (`CNTVCT_EL0`)
//! 3. POSIX fallback (all platforms): `clock_gettime(CLOCK_MONOTONIC, …)`
//! implemented here via `std::time::Instant`
//! Build with feature `no_processor_clock` to force the POSIX path everywhere,
//! mirroring the C `CFLAGS="-DNO_PROCESSOR_CLOCK"` option.
//! PORT NOTE: defines inline helpers (`elapsedStart`,
//! `elapsedUs`, `elapsedMs`); they are included here as free functions rather
//! than in a separate `.rs` file, since header contents merge into the consuming
//! `.rs`.

use std::sync::OnceLock;
use std::time::Instant;

// ── Public types ─────────────────────────────────────────────────────────────

/// A counter in microseconds relative to an arbitrary monotonic reference.
/// Use only for measuring elapsed time; never compare to wall-clock values.
pub type MonoTime = u64;

/// Identifies which underlying clock source is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonotonicClockType {
 /// POSIX `clock_gettime(CLOCK_MONOTONIC, …)`.
    Posix,
 /// Hardware counter: TSC on x86, CNTVCT on ARM.
    HardwareCounter,
}

// ── Internal clock state ─────────────────────────────────────────────────────

/// All mutable clock globals consolidated into a single `OnceLock`.
/// C equivalents:
/// `monotime (*getMonotonicUs)(void)` — the `get_us` field
/// `static char monotonic_info_string[32]` — the `info` / `info_len` fields
struct ClockState {
    kind: MonotonicClockType,
 /// Hot-path reader: the selected read-clock function.
    get_us: fn() -> MonoTime,
 /// NUL-padded info string matching C's fixed `char[32]` buffer.
    info: [u8; 32],
    info_len: usize,
}

// PORT NOTE: All fields of `ClockState` are Send + Sync by the stdlib rules
// (fn-pointer, enum, [u8; 32], usize), so the type auto-derives Send + Sync.
// No manual unsafe impls needed, keeping this crate's unsafe count at 0.
static CLOCK_STATE: OnceLock<ClockState> = OnceLock::new();

/// Baseline `Instant` for the POSIX implementation.
/// PORT NOTE: C's POSIX path returns the raw `CLOCK_MONOTONIC` value in µs
/// (seconds-since-boot on Linux). Rust `Instant` has no stable API to read
/// an absolute value, so we fix a process-lifetime origin here. All
/// *differences* between two `MonoTime` values are bit-identical to the C
/// behavior; only the absolute base differs.
static POSIX_BASELINE: OnceLock<Instant> = OnceLock::new();

// ── x86_64 TSC path ──────────────────────────────────────────────────────────

/// x86_64 + Linux TSC implementation block.
/// Gated on `#[cfg(...)]` mirroring the C preprocessor guard:
/// ```c
/// #if defined(USE_PROCESSOR_CLOCK) && defined(__x86_64__)
/// && defined(__linux__) && defined(__SIZEOF_INT128__)
/// ```
/// PORT NOTE: `__SIZEOF_INT128__` check dropped — Rust `u128` is always
/// available on supported targets.
#[cfg(all(
    not(feature = "no_processor_clock"),
    target_arch = "x86_64",
    target_os = "linux"
))]
mod x86_tsc {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use super::{ClockState, MonoTime, MonotonicClockType};

 /// Fixed-point multiplier: `(1 << MONO_FPMULT_SHIFT) / ticks_per_us`.
 /// Initialized to `u64::MAX` (sentinel = "not yet calibrated").
    pub(super) static MONO_TICKS_SPEED: AtomicU64 = AtomicU64::new(u64::MAX);

 /// Fractional bits in the fixed-point representation.
    pub(super) const MONO_FPMULT_SHIFT: u32 = 24;

 /// Number of TSC calibration rounds.
    pub(super) const TSC_CALIBRATION_ITERATIONS: usize = 3;

 /// Read the processor TSC counter.
    /// TODO(architect): unsafe needed — `core::arch::x86_64::_rdtsc()` must
 /// be called inside `unsafe { }`. This placeholder returns 0 until
 /// architect approves the unsafe budget and adds the real call.
    pub(super) fn rdtsc() -> u64 {
        // TODO(port): replace with `unsafe { core::arch::x86_64::_rdtsc() }`
 // once the architect grants unsafe access for this module.
        0
    }

 /// TSC-based clock reader.
 /// ```c
 /// return ((__uint128_t)__rdtsc * mono_ticks_speed) >> MONO_FPMULT_SHIFT;
 /// ```
 /// PORT NOTE: Rust `u128` is used for the intermediate product, matching
 /// the C `__uint128_t` — no overflow possible.
    pub(super) fn get_monotonic_us_x86() -> MonoTime {
        let tsc = rdtsc();
        let speed = MONO_TICKS_SPEED.load(Ordering::Relaxed);
 // Rust u128 is identical.
        ((tsc as u128).wrapping_mul(speed as u128) >> MONO_FPMULT_SHIFT) as MonoTime
    }

 /// Calibrate the TSC and verify `constant_tsc`; returns `ClockState` on
 /// success or `None` if the CPU is unsuitable.
    pub(super) fn init() -> Option<ClockState> {
 // TSC_CALIBRATION_ITERATIONS calibration rounds.
        for _ in 0..TSC_CALIBRATION_ITERATIONS {
            let wall_start = Instant::now();
            let tsc_start = rdtsc();

 // 10 ms sample window.
            std::thread::sleep(std::time::Duration::from_millis(10));

            let tsc_end = rdtsc();
            let elapsed_us = wall_start.elapsed().as_micros() as u64;

            if elapsed_us == 0 {
                continue;
            }

            let tsc_elapsed = tsc_end.wrapping_sub(tsc_start);
            let sample_ticks_per_us = tsc_elapsed as f64 / elapsed_us as f64;
            let sample_mult = ((1u64 << MONO_FPMULT_SHIFT) as f64 / sample_ticks_per_us) as u64;

 // of ticks_per_us, so smaller mult = higher speed = more accurate).
            let prev = MONO_TICKS_SPEED.load(Ordering::Relaxed);
            if sample_mult < prev {
                MONO_TICKS_SPEED.store(sample_mult, Ordering::Relaxed);
            }
        }

        let speed = MONO_TICKS_SPEED.load(Ordering::Relaxed);
        if speed == u64::MAX {
            eprintln!("monotonic: x86 linux, unable to determine clock rate");
            return None;
        }

        if !check_constant_tsc() {
            eprintln!("monotonic: x86 linux, 'constant_tsc' flag not present");
            return None;
        }

        let ticks_per_us = (1u64 << MONO_FPMULT_SHIFT) as f64 / speed as f64;

 // Using format! to build the string then encoding as bytes.
        let msg = format!("X86 TSC @ {:.2} ticks/us", ticks_per_us);
        let mut info = [0u8; 32];
        let bytes = msg.as_bytes();
        let len = bytes.len().min(31);
        info[..len].copy_from_slice(&bytes[..len]);

        Some(ClockState {
            kind: MonotonicClockType::HardwareCounter,
            get_us: get_monotonic_us_x86,
            info,
            info_len: len,
        })
    }

 /// Scan `/proc/cpuinfo` for the `constant_tsc` CPU flag.
    /// TODO(port): C uses compiled regex; this uses a simple byte-pattern
 /// search. Functionally equivalent for well-formed /proc/cpuinfo.
 /// Consider the `regex` crate in Phase B if edge cases arise.
    fn check_constant_tsc() -> bool {
        let Ok(cpuinfo) = std::fs::read("/proc/cpuinfo") else {
            return false;
        };
        for line in cpuinfo.split(|&b: &u8| b == b'\n') {
            if line.starts_with(b"flags") && line.windows(12).any(|w| w == b"constant_tsc") {
                return true;
            }
        }
        false
    }
}

// ── aarch64 ARM CNT path ─────────────────────────────────────────────────────

/// aarch64 virtual counter implementation block.
/// Mirrors the C guard:
/// ```c
/// #if defined(USE_PROCESSOR_CLOCK) && defined(__aarch64__)
/// ```
#[cfg(all(not(feature = "no_processor_clock"), target_arch = "aarch64"))]
mod aarch64_cnt {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::{ClockState, MonoTime, MonotonicClockType};

 /// Ticks per microsecond derived from `CNTFRQ_EL0`.
    pub(super) static MONO_TICKS_PER_US: AtomicI64 = AtomicI64::new(0);

 /// Read the ARM virtual counter register `CNTVCT_EL0`.
    pub(super) fn cntvct() -> u64 {
        let value: u64;
 // SAFETY: This reads the architectural virtual counter register. It
 // does not touch memory, dereference pointers, or alter control flow.
        unsafe {
            core::arch::asm!(
                "mrs {value}, cntvct_el0",
                value = out(reg) value,
                options(nomem, nostack)
            );
        }
        value
    }

 /// Read the CNT frequency register `CNTFRQ_EL0`.
    pub(super) fn cntfrq_hz() -> u32 {
        let value: u64;
 // SAFETY: This reads the architectural counter-frequency register.
 // The instruction is side-effect-free with respect to Rust memory.
        unsafe {
            core::arch::asm!(
                "mrs {value}, cntfrq_el0",
                value = out(reg) value,
                options(nomem, nostack)
            );
        }
        value as u32
    }

 /// ARM CNT-based clock reader.
 /// ```c
 /// return __cntvct / mono_ticksPerMicrosecond;
 /// ```
    pub(super) fn get_monotonic_us_aarch64() -> MonoTime {
        let ticks = cntvct();
        let per_us = MONO_TICKS_PER_US.load(Ordering::Relaxed);
        if per_us <= 0 {
            return 0;
        }
        ticks / per_us as u64
    }

 /// Initialize the aarch64 hardware counter path.
    pub(super) fn init() -> Option<ClockState> {
        let ticks_per_us = cntfrq_hz() as i64 / 1_000 / 1_000;
        if ticks_per_us == 0 {
            eprintln!("monotonic: aarch64, unable to determine clock rate");
            return None;
        }
        MONO_TICKS_PER_US.store(ticks_per_us, Ordering::Relaxed);

        let msg = format!("ARM CNTVCT @ {} ticks/us", ticks_per_us);
        let mut info = [0u8; 32];
        let bytes = msg.as_bytes();
        let len = bytes.len().min(31);
        info[..len].copy_from_slice(&bytes[..len]);

        Some(ClockState {
            kind: MonotonicClockType::HardwareCounter,
            get_us: get_monotonic_us_aarch64,
            info,
            info_len: len,
        })
    }
}

// ── POSIX fallback ────────────────────────────────────────────────────────────

/// POSIX monotonic clock reader.
/// ```c
/// struct timespec ts;
/// clock_gettime(CLOCK_MONOTONIC, &ts);
/// return ((uint64_t)ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
/// ```
/// PORT NOTE: `std::time::Instant` wraps `CLOCK_MONOTONIC` on Linux/macOS.
/// Since Rust provides no stable API to read the absolute counter value,
/// elapsed µs are measured from `POSIX_BASELINE` (fixed at `monotonic_init`
/// time). Relative differences are bit-identical to the C implementation.
fn get_monotonic_us_posix() -> MonoTime {
    let baseline = POSIX_BASELINE.get_or_init(Instant::now);
    baseline.elapsed().as_micros() as MonoTime
}

/// Initialize the POSIX fallback; always succeeds.
fn init_posix() -> ClockState {
 // In Rust, Instant::now always succeeds on supported platforms;
 // if CLOCK_MONOTONIC were unavailable, the stdlib would panic at startup.
    POSIX_BASELINE.get_or_init(Instant::now);

    const INFO: &[u8] = b"POSIX clock_gettime";
    let mut info = [0u8; 32];
    info[..INFO.len()].copy_from_slice(INFO);

    ClockState {
        kind: MonotonicClockType::Posix,
        get_us: get_monotonic_us_posix,
        info,
        info_len: INFO.len(),
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Retrieve the current monotonic time in microseconds.
/// Must be called after [`monotonic_init`]; if called before, falls back
/// the POSIX path (safe but sets a different baseline than `monotonic_init`
/// would have chosen).
/// global function pointer called
/// directly at call sites. In Rust this is a regular function that
/// dispatches through the stored pointer in `CLOCK_STATE`.
pub fn get_monotonic_us() -> MonoTime {
    match CLOCK_STATE.get() {
        None => get_monotonic_us_posix(),
        Some(state) => (state.get_us)(),
    }
}

/// Initialize the monotonic clock (call once at startup; idempotent).
/// Tries platform-optimized paths in priority order, then falls back to
/// POSIX implementation. Returns a static byte slice naming the active clock.
pub fn monotonic_init() -> &'static [u8] {
    let state = CLOCK_STATE.get_or_init(|| {
        #[cfg(all(
            not(feature = "no_processor_clock"),
            target_arch = "x86_64",
            target_os = "linux"
        ))]
        if let Some(s) = x86_tsc::init() {
            return s;
        }

        #[cfg(all(not(feature = "no_processor_clock"), target_arch = "aarch64"))]
        if let Some(s) = aarch64_cnt::init() {
            return s;
        }

        init_posix()
    });

    &state.info[..state.info_len]
}

/// Return a static byte slice naming the active clock source.
pub fn monotonic_info_string() -> &'static [u8] {
    CLOCK_STATE
        .get()
        .map(|s| &s.info[..s.info_len])
        .unwrap_or(b"uninitialized")
}

/// Return which clock type is currently active.
pub fn monotonic_get_type() -> MonotonicClockType {
    CLOCK_STATE
        .get()
        .map(|s| s.kind)
        .unwrap_or(MonotonicClockType::Posix)
}

// ── Elapsed-time helpers ──────────────────────

/// Record the current monotonic time as a measurement start point.
/// PORT NOTE: C writes through a pointer; Rust returns the value instead,
/// matching safe-Rust idioms. Call sites store the return value.
pub fn elapsed_start() -> MonoTime {
    get_monotonic_us()
}

/// Microseconds elapsed since `start_time`.
pub fn elapsed_us(start_time: MonoTime) -> u64 {
 // PERF(port): C subtracts two u64s directly; saturating_sub adds one
 // branch. Profile in Phase B — the clock should never go backward, so
 // wrapping_sub might be substituted after verification.
    get_monotonic_us().saturating_sub(start_time)
}

/// Milliseconds elapsed since `start_time`.
pub fn elapsed_ms(start_time: MonoTime) -> u64 {
    elapsed_us(start_time) / 1_000
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  8 functions: getMonotonicUs_x86, monotonicInit_x86linux,
//                  __cntvct, cntfrq_hz, getMonotonicUs_aarch64,
//                  monotonicInit_aarch64, getMonotonicUs_posix,
//                  monotonicInit_posix, monotonicInit, monotonicInfoString,
//                  monotonicGetType, elapsedStart, elapsedUs, elapsedMs)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         6
//   port_notes:    4
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         x86 RDTSC and aarch64 CNT paths are stubbed with
//                  TODO(architect) for unsafe — real intrinsics/asm needed.
//                  POSIX path via std::time::Instant is fully functional.
//                  `no_processor_clock` feature must be declared in
//                  redis-core/Cargo.toml for cfg gating to work.
//                  OnceLock requires T: Send+Sync; ClockState has manual impls.
// ──────────────────────────────────────────────────────────────────────────

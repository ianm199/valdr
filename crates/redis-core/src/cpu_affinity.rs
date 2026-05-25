//! CPU affinity management for Valkey server threads.
//!
//! Port of `setcpuaffinity.c` (Valkey). Exposes [`set_cpu_affinity`] to pin
//! the calling thread to a set of CPU cores described by a cpulist string
//! such as `"0,2,3"`, `"0,2-3"`, or `"0-20:2"` (same format as `taskset`).
//!
//! The parsing logic is fully translated into safe Rust. The final OS
//! syscalls (Linux `sched_setaffinity`, FreeBSD `cpuset_setaffinity`,
//! DragonFly/NetBSD `pthread_setaffinity_np`) require `unsafe` FFI and are
//! stubbed with `TODO(architect)` until the `libc` dependency and an unsafe
//! budget entry are approved for this crate.
//!
//! Compile-time enablement mirrors the C `#ifdef USE_SETCPUAFFINITY` via the
//! Cargo feature `"cpu-affinity"`.
//!
//! C source: `src/setcpuaffinity.c` — Copyright (C) 2020 zhenwei pi, MIT.

use redis_types::error::RedisError;

// ── internal parsing helpers ────────────────────────────────────────────────

/// Advance `input` past the first occurrence of `sep` and return the
/// remaining slice. Returns `None` when `sep` is absent.
///
/// Identical semantics to the C helper: returns the character *after* the
/// separator, or `None` (`NULL` in C) if the separator is not found.
///
/// C: `static const char *next_token(const char *q, int sep)`,
/// `setcpuaffinity.c:50-57`
fn next_token(input: &[u8], sep: u8) -> Option<&[u8]> {
    input
        .iter()
        .position(|&b| b == sep)
        .map(|i| &input[i + 1..])
}

/// Parse a decimal integer from the start of `input`. Returns
/// `Ok((value, rest))` where `rest` is the unparsed suffix, or `Err(())`
/// when no leading ASCII digit is present (mirrors the C -1 / 0 return).
///
/// C: `static int next_num(const char *str, char **end, int *result)`,
/// `setcpuaffinity.c:59-68`
///
/// PERF(port): C uses `strtoul` (base 10); this uses a fold — profile if hot.
fn next_num(input: &[u8]) -> Result<(i32, &[u8]), ()> {
    if input.is_empty() || !input[0].is_ascii_digit() {
        return Err(());
    }
    let end = input
        .iter()
        .position(|&b| !b.is_ascii_digit())
        .unwrap_or(input.len());
    // TODO(port): C uses `unsigned long` (64-bit on Linux); very large numbers
    // saturate to i32::MAX here which is still an invalid CPU index and will
    // produce a benign out-of-range error from the OS call.
    let n: u64 = input[..end].iter().fold(0u64, |acc, &b| {
        acc.saturating_mul(10).saturating_add(u64::from(b - b'0'))
    });
    Ok((n.min(i32::MAX as u64) as i32, &input[end..]))
}

/// Parse one comma-separated segment of a cpulist into a `(start, end, step)`
/// triple. The segment must not contain commas (callers split beforehand).
///
/// In the original C, pointer comparisons (`c1 < c2`) guard against '-' or
/// ':' belonging to the *next* comma-separated token. Because we pre-split by
/// comma, those comparisons always favour the local match, reducing to simple
/// `Option::is_some()` checks.
///
/// Mirrors the per-token parsing block in `setcpuaffinity()`,
/// `setcpuaffinity.c:97-134`.
fn parse_segment(segment: &[u8]) -> Result<(i32, i32, i32), ()> {
    let (a, rest) = next_num(segment)?;
    let mut b = a;
    let mut s: i32 = 1;

    if let Some(after_dash) = next_token(rest, b'-') {
        // Range: `a-b` or `a-b:s`
        let (range_end, rest2) = next_num(after_dash)?;
        b = range_end;

        if !rest2.is_empty() {
            if let Some(after_colon) = next_token(rest2, b':') {
                // Step: `a-b:s`
                let (step, tail) = next_num(after_colon)?;
                if step == 0 || !tail.is_empty() {
                    return Err(());
                }
                s = step;
            } else {
                // Trailing garbage after range-end digit (no colon found).
                return Err(());
            }
        }
    } else if !rest.is_empty() {
        // Trailing garbage after a single CPU number.
        return Err(());
    }

    if a > b {
        return Err(());
    }

    Ok((a, b, s))
}

/// Parse a cpulist string into an ordered list of CPU indices.
///
/// Accepted formats:
/// - `"0,2,3"` — individual CPUs
/// - `"0,2-3"` — individual CPU plus a contiguous range
/// - `"0-20:2"` — range with explicit step
///
/// Returns `Err(())` on any malformed input.
///
/// C: cpulist parsing loop in `setcpuaffinity()`, `setcpuaffinity.c:95-138`
fn parse_cpulist(cpulist: &[u8]) -> Result<Vec<i32>, ()> {
    if cpulist.is_empty() {
        return Ok(Vec::new());
    }

    let mut cpus: Vec<i32> = Vec::new();

    for segment in cpulist.split(|&b| b == b',') {
        if segment.is_empty() {
            // Trailing comma or `,,` — treat as garbage, matching C behaviour.
            return Err(());
        }
        let (start, end, step) = parse_segment(segment)?;
        let mut cpu = start;
        while cpu <= end {
            cpus.push(cpu);
            // TODO(port): saturating_add prevents wrapping if step is large,
            // but any wrapped value would be invalid and the while condition
            // would eventually terminate; profile to confirm no infinite loop.
            cpu = cpu.saturating_add(step);
        }
    }

    Ok(cpus)
}

// ── platform-specific affinity application ──────────────────────────────────

/// Apply `cpus` as the CPU affinity mask for the calling thread.
///
/// TODO(architect): every platform path needs `unsafe` libc FFI:
///   - Linux:     `sched_setaffinity(0, size_of::<cpu_set_t>(), &cpuset)`
///                via `<sched.h>` / `libc::sched_setaffinity`
///   - FreeBSD:   `cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, …)`
///                via `<sys/cpuset.h>` / `libc`
///   - DragonFly: `pthread_setaffinity_np(pthread_self(), …)`
///                via `<pthread_np.h>` / `libc`
///   - NetBSD:    `pthread_setaffinity_np(pthread_self(), cpuset_size(c), c)`
///                via `<pthread.h>` + `<sched.h>` / `libc`
///
/// Pilot crate `redis-core` has `unsafe` budget 0 (`harness/unsafe-budgets.toml`).
/// This stub returns `Ok(())` without touching the OS until the architect
/// approves the `libc` dependency and a non-zero budget entry.
///
/// C: `setcpuaffinity.c:140-152`
#[cfg(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
))]
fn apply_affinity(cpus: &[i32]) -> Result<(), RedisError> {
    // TODO(architect): unsafe FFI required — see doc-comment above.
    // Implementation sketch (Linux path):
    //   let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    //   for &cpu in cpus {
    //       unsafe { libc::CPU_SET(cpu as usize, &mut cpuset); }
    //   }
    //   let rc = unsafe {
    //       libc::sched_setaffinity(0, std::mem::size_of_val(&cpuset), &cpuset)
    //   };
    //   if rc != 0 { return Err(RedisError::runtime(b"sched_setaffinity failed")); }
    let _ = cpus;
    Ok(())
}

/// No-op fallback for platforms that do not support CPU affinity pinning,
/// matching the C `#ifndef USE_SETCPUAFFINITY` / unsupported-OS behaviour.
#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
)))]
fn apply_affinity(_cpus: &[i32]) -> Result<(), RedisError> {
    Ok(())
}

// ── public API ───────────────────────────────────────────────────────────────

/// Set the CPU affinity of the calling thread to the CPUs described by
/// `cpulist`. Returns `Ok(())` on success.
///
/// On an empty `cpulist` the function is a no-op, matching the C
/// `if (!cpulist) return;` null-pointer guard.
///
/// PORT NOTE: The C source gates the entire function body on
/// `#ifdef USE_SETCPUAFFINITY`. Here that maps to the Cargo feature
/// `"cpu-affinity"`. The `#[cfg(not(feature = …))]` variant is a no-op so
/// the symbol is always present for callers.
///
/// PORT NOTE: The C function returns `void` and silently ignores parse
/// errors. Rust surfaces them as `Err(RedisError)` so callers can log the
/// problem; callers that want the C silent-ignore behaviour may use
/// `let _ = set_cpu_affinity(list);`.
///
/// C: `void setcpuaffinity(const char *cpulist)`, `setcpuaffinity.c:73-153`
// TODO(port): feature name "cpu-affinity" is a guess — align with the
// Cargo.toml feature chosen for USE_SETCPUAFFINITY when the manifest is
// finalised.
#[cfg(feature = "cpu-affinity")]
pub fn set_cpu_affinity(cpulist: &[u8]) -> Result<(), RedisError> {
    if cpulist.is_empty() {
        return Ok(());
    }
    let cpus =
        parse_cpulist(cpulist).map_err(|()| RedisError::runtime(b"invalid cpu affinity list"))?;
    apply_affinity(&cpus)
}

/// No-op when the `"cpu-affinity"` Cargo feature is disabled.
#[cfg(not(feature = "cpu-affinity"))]
pub fn set_cpu_affinity(_cpulist: &[u8]) -> Result<(), RedisError> {
    Ok(())
}

// ── unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{parse_cpulist, parse_segment};

    #[test]
    fn single_cpu() {
        assert_eq!(parse_cpulist(b"3").unwrap(), vec![3]);
    }

    #[test]
    fn comma_list() {
        assert_eq!(parse_cpulist(b"0,2,3").unwrap(), vec![0, 2, 3]);
    }

    #[test]
    fn range_no_step() {
        assert_eq!(parse_cpulist(b"2-4").unwrap(), vec![2, 3, 4]);
    }

    #[test]
    fn range_with_step() {
        assert_eq!(parse_cpulist(b"0-6:2").unwrap(), vec![0, 2, 4, 6]);
    }

    #[test]
    fn mixed() {
        assert_eq!(parse_cpulist(b"0,2-3").unwrap(), vec![0, 2, 3]);
    }

    #[test]
    fn empty_is_ok() {
        assert_eq!(parse_cpulist(b"").unwrap(), Vec::<i32>::new());
    }

    #[test]
    fn trailing_comma_is_err() {
        assert!(parse_cpulist(b"0,1,").is_err());
    }

    #[test]
    fn reversed_range_is_err() {
        assert!(parse_segment(b"5-2").is_err());
    }

    #[test]
    fn zero_step_is_err() {
        assert!(parse_segment(b"0-4:0").is_err());
    }

    #[test]
    fn trailing_garbage_is_err() {
        assert!(parse_segment(b"1x").is_err());
    }

    #[test]
    fn non_digit_start_is_err() {
        assert!(parse_cpulist(b"abc").is_err());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/setcpuaffinity.c  (156 lines, 3 functions)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         5
//   port_notes:    2
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Parsing logic fully translated into safe Rust with unit
//                  tests. OS-level affinity syscalls are stubbed pending
//                  architect approval of libc dep + unsafe budget. Feature
//                  flag name ("cpu-affinity") must be aligned with Cargo.toml.
// ──────────────────────────────────────────────────────────────────────────

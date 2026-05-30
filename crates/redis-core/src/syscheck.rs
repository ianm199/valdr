//! System health checks run at startup to warn operators about suboptimal
// Deferred feature: startup system-health warnings (clocksource, overcommit,
// THP, arm64 fork bug); to be wired at server startup in Phase B.
#![allow(dead_code)]
//! kernel and VM configuration (clocksource, overcommit, THP, arm64 fork bug).
//!
//! Corresponds to `src/syscheck.c` + `src/syscheck.h` (374 lines, 6 functions).
//! Both the `.c` and its header are merged into this module per PORTING.md §"File location".
//!
//! All public check functions return [`CheckOutcome`] rather than the C int tri-state
//! (`-1` / `0` / `1`).  Error message bytes exactly reproduce the C string literals so
//! operator-visible output is identical to Valkey.
//!
//! Linux-only functionality is gated with `#[cfg(target_os = "linux")]`.
//! The arm64 MADV_FREE bug check is additionally gated on `#[cfg(target_arch = "aarch64")]`.

// ── Public types ──────────────────────────────────────────────────────────────

/// The result of a single system check.
///
/// Maps to the C tri-state return convention:
/// * `1`  → [`CheckOutcome::Pass`]
/// * `0`  → [`CheckOutcome::Skip`]  (check could not be completed)
/// * `-1` → [`CheckOutcome::Fail`]  (check detected a problem; includes message bytes)
#[derive(Debug)]
pub enum CheckOutcome {
    Pass,
    Skip,
    Fail(Vec<u8>),
}

// ── Linux-only helpers and checks ─────────────────────────────────────────────

/// Read the first line from a sysfs/procfs pseudo-file and trim leading and
/// trailing ASCII spaces and newlines.  Returns `None` if the file cannot be
/// opened or contains no data.
///
/// C: `syscheck.c:50-62`, `read_sysfs_line` (static, Linux only).
///
/// PORT NOTE: `path` is `&str` (not `&[u8]`) because sysfs paths are Rust-side
/// system path literals, not Redis data, and `std::fs::File::open` requires
/// a `Path`-compatible type.
#[cfg(target_os = "linux")]
fn read_sysfs_line(path: &str) -> Option<Vec<u8>> {
    let file = File::open(path).ok()?;
    let mut reader = BufReader::new(file);
    let mut line: Vec<u8> = Vec::new();
    let n = reader.read_until(b'\n', &mut line).ok()?;
    if n == 0 {
        return None;
    }
    // sdstrim(res, " \n") — strip leading and trailing spaces / newlines
    let start = line
        .iter()
        .position(|&b| b != b' ' && b != b'\n')
        .unwrap_or(line.len());
    let end = line
        .iter()
        .rposition(|&b| b != b' ' && b != b'\n')
        .map(|i| i + 1)
        .unwrap_or(0);
    if start >= end {
        return Some(Vec::new());
    }
    Some(line[start..end].to_vec())
}

/// Verify that the system clocksource does not go through a kernel system call
/// (i.e., it is backed by the vDSO).  A syscall-backed clocksource degrades
/// server performance measurably.
///
/// The check busy-loops for `5 × (1_000_000 / system_hz)` µs while recording
/// `getrusage` system-time before and after.  If more than 10% of elapsed
/// process time was in kernel mode, the clocksource is considered slow.
///
/// C: `syscheck.c:66-118`, `checkClocksource` (static, Linux only).
///
/// TODO(architect): `sysconf(_SC_CLK_TCK)` and `getrusage(RUSAGE_SELF, …)` are
/// POSIX libc calls with no safe Rust stdlib equivalent.  A `libc` dependency
/// and a non-zero unsafe budget for `redis-core` are required to implement
/// this check faithfully.  Until approved, returns [`CheckOutcome::Skip`].
#[cfg(target_os = "linux")]
pub(crate) fn check_clocksource() -> CheckOutcome {
    CheckOutcome::Skip
}

/// Verify that the current clocksource is not `xen`.
///
/// The Xen hypervisor's default clocksource is slow; AWS ec2 recommends
/// switching to `tsc` for Xen-based instances.
///
/// C: `syscheck.c:123-137`, `checkXenClocksource` (Linux only).
#[cfg(target_os = "linux")]
pub fn check_xen_clocksource() -> CheckOutcome {
    // C: syscheck.c:123-137
    let curr =
        match read_sysfs_line("/sys/devices/system/clocksource/clocksource0/current_clocksource") {
            Some(v) => v,
            None => return CheckOutcome::Skip,
        };

    if curr == b"xen" {
        CheckOutcome::Fail(
            b"Your system is configured to use the 'xen' clocksource which might lead to \
degraded performance. Check the result of the [slow-clocksource] system check: run \
'valkey-server --check-system' to check if the system's clocksource isn't degrading \
performance."
                .to_vec(),
        )
    } else {
        CheckOutcome::Pass
    }
}

/// Verify that `vm.overcommit_memory` is set to `1`.
///
/// When overcommit is disabled, Linux may OOM-kill the `bgsave` child process
/// even though it only needs copy-on-write pages, not a full second copy of
/// all server memory.
///
/// C: `syscheck.c:143-168`, `checkOvercommit` (Linux only).
#[cfg(target_os = "linux")]
pub fn check_overcommit() -> CheckOutcome {
    // C: syscheck.c:143-168
    let file = match File::open("/proc/sys/vm/overcommit_memory") {
        Ok(f) => f,
        Err(_) => return CheckOutcome::Skip,
    };
    let mut reader = BufReader::new(file);
    let mut line: Vec<u8> = Vec::new();
    match reader.read_until(b'\n', &mut line) {
        Ok(0) | Err(_) => return CheckOutcome::Skip,
        _ => {}
    }

    // strtol(buf, NULL, 10) != 1
    // The file contains a single ASCII decimal integer.
    let value: i64 = line
        .iter()
        .take_while(|&&b| b.is_ascii_digit())
        .fold(0i64, |acc, &b| acc * 10 + i64::from(b - b'0'));

    if value != 1 {
        CheckOutcome::Fail(
            b"Memory overcommit must be enabled! Without it, a background save or replication \
may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to \
/etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this \
to take effect."
                .to_vec(),
        )
    } else {
        CheckOutcome::Pass
    }
}

/// Verify that Transparent Huge Pages (THP) are not set to `always`.
///
/// When THP is `always`, copy-on-write during `fork` can double memory
/// consumption and significantly hurt tail latency.
///
/// C: `syscheck.c:172-194`, `checkTHPEnabled` (Linux only).
#[cfg(target_os = "linux")]
pub fn check_thp_enabled() -> CheckOutcome {
    // C: syscheck.c:172-194
    let file = match File::open("/sys/kernel/mm/transparent_hugepage/enabled") {
        Ok(f) => f,
        Err(_) => return CheckOutcome::Skip,
    };
    let mut reader = BufReader::new(file);
    let mut buf: Vec<u8> = Vec::new();
    match reader.read_until(b'\n', &mut buf) {
        Ok(0) | Err(_) => return CheckOutcome::Skip,
        _ => {}
    }

    // strstr(buf, "[always]") != NULL
    if buf.windows(b"[always]".len()).any(|w| w == b"[always]") {
        CheckOutcome::Fail(
            b"You have Transparent Huge Pages (THP) support enabled in your kernel. \
This will create latency and memory usage issues with Valkey. \
To fix this issue run the command \
'echo madvise > /sys/kernel/mm/transparent_hugepage/enabled' as root, \
and add it to your /etc/rc.local in order to retain the setting after a reboot. \
Valkey must be restarted after THP is disabled (set to 'madvise' or 'never')."
                .to_vec(),
        )
    } else {
        CheckOutcome::Pass
    }
}

// ── arm64 + Linux only ────────────────────────────────────────────────────────

/// Parse a hexadecimal `usize` from an ASCII byte slice.
/// Returns `None` if the slice is empty or contains a non-hex character.
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn parse_hex_usize(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut result: usize = 0;
    for &b in bytes {
        let digit = match b {
            b'0'..=b'9' => usize::from(b - b'0'),
            b'a'..=b'f' => usize::from(b - b'a') + 10,
            b'A'..=b'F' => usize::from(b - b'A') + 10,
            _ => return None,
        };
        result = result.wrapping_mul(16).wrapping_add(digit);
    }
    Some(result)
}

/// Return the `Shared_Dirty` value (in kB) from `/proc/self/smaps` for the
/// virtual-memory area that contains `addr`, or `None` on failure.
///
/// C: `syscheck.c:199-223`, `smapsGetSharedDirty` (static, arm64 + Linux only).
///
/// PORT NOTE: The C return is `int` with `-1` for error; Rust uses `Option<u32>`
/// since valid values are always non-negative.
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn smaps_get_shared_dirty(addr: usize) -> Option<u32> {
    // C: syscheck.c:199-223
    let file = File::open("/proc/self/smaps").ok()?;
    let reader = BufReader::new(file);
    let mut in_mapping = false;

    for raw in reader.split(b'\n') {
        let line = raw.ok()?;

        // VMA header lines always start with a hex digit: "address-address perms ..."
        // Try to parse "from-to" hex pair to update in_mapping.
        if line.first().map(|b| b.is_ascii_hexdigit()).unwrap_or(false) {
            if let Some(dash_pos) = line.iter().position(|&b| b == b'-') {
                let from_hex = parse_hex_usize(&line[..dash_pos]);
                let rest = &line[dash_pos + 1..];
                let to_end = rest.iter().position(|&b| b == b' ').unwrap_or(rest.len());
                let to_hex = parse_hex_usize(&rest[..to_end]);
                if let (Some(from), Some(to)) = (from_hex, to_hex) {
                    in_mapping = from <= addr && addr < to;
                    continue;
                }
            }
        }

        // "Shared_Dirty:   <value> kB"  — only relevant inside the target mapping
        if in_mapping && line.starts_with(b"Shared_Dirty:") {
            let rest = &line[b"Shared_Dirty:".len()..];
            let digits_start = rest
                .iter()
                .position(|b| b.is_ascii_digit())
                .unwrap_or(rest.len());
            let value: u32 = rest[digits_start..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .fold(0u32, |acc, &b| acc * 10 + u32::from(b - b'0'));
            return Some(value);
        }
    }
    None
}

/// Check whether the arm64 kernel has the MADV_FREE + copy-on-write dirty-bit
/// bug that can cause data corruption during background save.
///
/// The bug was fixed in kernel commit ff1712f9 ("arm64: pgtable: Ensure dirty
/// bit is preserved across pte_wrprotect()").
///
/// C: `syscheck.c:231-321`, `checkLinuxMadvFreeForkBug` (arm64 + Linux only).
///
/// TODO(architect): the C implementation requires `mmap`, `mprotect`,
/// `madvise`, `fork`, `pipe`, and `waitpid` — all POSIX syscalls that have no
/// safe Rust stdlib equivalent and require `unsafe` blocks.  Until the
/// architect approves a `libc`/`nix` dependency and a non-zero unsafe budget
/// for `redis-core`, this check returns [`CheckOutcome::Skip`].
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub fn check_linux_madv_free_fork_bug() -> CheckOutcome {
    CheckOutcome::Skip
}

// ── Dispatch helper ───────────────────────────────────────────────────────────

/// Print the result of one check to stdout and update `all_passed`.
///
/// Output format mirrors the C `syscheck()` printf calls exactly:
/// `[<name>]...OK`, `[<name>]...skipped`, or `[<name>]...WARNING:\n<message>`.
///
/// PORT NOTE: `name` and message bytes are printed via `String::from_utf8_lossy`
/// for display purposes only.  These are ASCII diagnostic labels and operator
/// messages — not Redis data — so the display conversion is acceptable here.
fn run_check(name: &[u8], outcome: CheckOutcome, all_passed: &mut bool) {
    print!("[{}]...", String::from_utf8_lossy(name));
    match outcome {
        CheckOutcome::Skip => println!("skipped"),
        CheckOutcome::Pass => println!("OK"),
        CheckOutcome::Fail(msg) => {
            println!("WARNING:");
            println!("{}", String::from_utf8_lossy(&msg));
            *all_passed = false;
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Run all applicable system checks, printing results to stdout.
///
/// Returns `true` if every check either passed or was skipped, `false` if any
/// check reported a warning.  The caller (startup code) typically prints an
/// additional advisory and continues anyway; the return value is informational.
///
/// C: `syscheck.c:353-374`, `syscheck`.
///
/// PORT NOTE: The C implementation uses a statically-initialised function-pointer
/// table (`check checks[]`).  Rust has no idiomatic equivalent for
/// conditionally-populated static slices, so this function dispatches directly.
/// Behavior is identical.
pub fn syscheck() -> bool {
    // C: syscheck.c:353-374
    let all_passed = true;

    #[cfg(target_os = "linux")]
    {
        run_check(b"slow-clocksource", check_clocksource(), &mut all_passed);
        run_check(b"xen-clocksource", check_xen_clocksource(), &mut all_passed);
        run_check(b"overcommit", check_overcommit(), &mut all_passed);
        run_check(b"THP", check_thp_enabled(), &mut all_passed);

        #[cfg(target_arch = "aarch64")]
        run_check(
            b"madvise-free-fork-bug",
            check_linux_madv_free_fork_bug(),
            &mut all_passed,
        );
    }

    all_passed
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/syscheck.c  (374 lines, 6 functions) + src/syscheck.h
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         2
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         check_clocksource and check_linux_madv_free_fork_bug are
//                  stubbed as Skip — both require libc (getrusage / mmap /
//                  fork) which needs an unsafe budget.  All safe checks
//                  (xen-clocksource, overcommit, THP, smaps parsing) are
//                  faithfully translated.  lib.rs needs `pub mod syscheck;`.
// ──────────────────────────────────────────────────────────────────────────────

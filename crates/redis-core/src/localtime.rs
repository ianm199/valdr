//! Lock-free local-time decomposition.
//!
//! This is a direct port of `localtime.c` from Valkey. It provides a
//! re-entrant, fork-safe alternative to `localtime(3)` by avoiding the
//! internal mutex that POSIX `localtime` / `localtime_r` hold. The
//! implementation is intentionally restricted: it only handles dates
//! >= 1970-01-01 (Unix epoch), which is all that server-side logging
//! needs.
//!
//! # Usage pattern (mirrors the C caller convention)
//!
//! ```rust,ignore
//! // In main(), before forking:
//! //   1. Call tzset() (or capture `timezone`/`tm_isdst` from an initial
//! //      localtime() call) to obtain the UTC offset and DST flag.
//! //   2. Pass those values to nolocks_localtime() throughout the
//! //      process lifetime, refreshing dst at safe points.
//! let t: i64 = /* unix timestamp from time(2) */;
//! let tz_offset: i64 = /* seconds west of UTC (e.g. timezone global) */;
//! let dst_active: i32 = /* 1 if DST is in effect, 0 otherwise */;
//! let bd = nolocks_localtime(t, tz_offset, dst_active);
//! ```

// C: localtime.c (128 lines, 2 functions)

/// Broken-down calendar time, matching the fields of POSIX `struct tm`
/// that are populated by `nolocks_localtime`.
///
/// Field semantics are identical to POSIX `struct tm`:
/// - `tm_year` is years **since 1900** (e.g. 2024 → 124).
/// - `tm_mon` is months since January (0–11).
/// - `tm_mday` is day of month (1–31).
/// - `tm_wday` is day of week: Sunday = 0, Saturday = 6.
/// - `tm_yday` is day of year (0–365).
/// - `tm_hour`, `tm_min`, `tm_sec` are the obvious time-of-day fields.
/// - `tm_isdst` mirrors the `dst` argument that was passed in; this
///   struct does not derive DST independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenDownTime {
    pub tm_year: i32,
    pub tm_mon: i32,
    pub tm_mday: i32,
    pub tm_hour: i32,
    pub tm_min: i32,
    pub tm_sec: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_isdst: i32,
}

/// Returns `true` when `year` is a Gregorian leap year.
///
/// C: localtime.c:52-61, `is_leap_year`
fn is_leap_year(year: i64) -> bool {
    if year % 4 != 0 {
        false // not divisible by 4 → not leap
    } else if year % 100 != 0 {
        true // divisible by 4 but not 100 → leap
    } else if year % 400 != 0 {
        false // divisible by 100 but not 400 → not leap
    } else {
        true // divisible by 400 → leap
    }
}

/// Decomposes a Unix timestamp into calendar fields without acquiring any
/// system lock, making it safe to call after `fork(2)` and from signal
/// handlers.
///
/// # Parameters
/// - `t`   — Unix timestamp (seconds since 1970-01-01 00:00:00 UTC).
/// - `tz`  — Seconds **west** of UTC (the value of the C `timezone` global
///            after `tzset()`). Positive for timezones behind UTC (e.g.
///            US/East = +18000), negative for those ahead.
/// - `dst` — 1 if daylight saving time is currently active, 0 otherwise.
///            The caller must obtain this independently (e.g. from a prior
///            `localtime()` call in `main()` before any `fork()`).
///
/// # Returns
/// A [`BrokenDownTime`] with all fields filled in. Returns the epoch
/// (1970-01-01 00:00:00) for any `t` that, after timezone adjustment,
/// would be negative.
///
/// # Limitations
/// Does not handle dates before 1970-01-01. The function is designed
/// exclusively for server logging of recent timestamps.
///
/// C: localtime.c:63-108, `nolocks_localtime`
pub fn nolocks_localtime(t: i64, tz: i64, dst: i32) -> BrokenDownTime {
    const SECS_MIN: i64 = 60;
    const SECS_HOUR: i64 = 3_600;
    const SECS_DAY: i64 = 3_600 * 24;

    // Adjust timestamp for timezone and DST.
    // C: t -= tz; t += 3600 * dst;
    let t = t - tz + SECS_HOUR * dst as i64;

    // Split into whole days and intra-day seconds.
    // Guard against negative adjusted timestamps so indexing stays sane.
    let days = if t >= 0 { t / SECS_DAY } else { 0 };
    let seconds = if t >= 0 { t % SECS_DAY } else { 0 };

    let tm_hour = (seconds / SECS_HOUR) as i32;
    let tm_min = ((seconds % SECS_HOUR) / SECS_MIN) as i32;
    let tm_sec = ((seconds % SECS_HOUR) % SECS_MIN) as i32;

    // 1970-01-01 was a Thursday (weekday index 4 when Sunday = 0).
    // C: tmp->tm_wday = (days + 4) % 7;
    let tm_wday = ((days + 4) % 7) as i32;

    // Walk forward from 1970 consuming whole years until the remainder fits
    // within the current year.
    // C: localtime.c:84-91
    let mut year: i64 = 1970;
    let mut remaining_days = days;
    loop {
        let days_this_year: i64 = 365 + i64::from(is_leap_year(year));
        if days_this_year > remaining_days {
            break;
        }
        remaining_days -= days_this_year;
        year += 1;
    }

    let tm_yday = remaining_days as i32;

    // Build the per-month day table; February gets an extra day in leap years.
    // C: localtime.c:97-98
    let mut mdays: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if is_leap_year(year) {
        mdays[1] += 1;
    }

    // Walk forward through months until the remainder fits within the current
    // month.
    // C: localtime.c:100-104
    let mut tm_mon: i32 = 0;
    let mut month_days = remaining_days;
    while month_days >= mdays[tm_mon as usize] {
        month_days -= mdays[tm_mon as usize];
        tm_mon += 1;
    }

    // Add 1 because 'month_days' is zero-based; subtract 1900 to match the
    // tm_year convention.
    // C: localtime.c:106-107
    let tm_mday = month_days as i32 + 1;
    let tm_year = (year - 1900) as i32;

    BrokenDownTime {
        tm_isdst: dst,
        tm_hour,
        tm_min,
        tm_sec,
        tm_wday,
        tm_year,
        tm_mon,
        tm_mday,
        tm_yday,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_zero_utc() {
        // 1970-01-01 00:00:00 UTC with no timezone offset.
        let bd = nolocks_localtime(0, 0, 0);
        assert_eq!(bd.tm_year, 70); // 1970 - 1900
        assert_eq!(bd.tm_mon, 0); // January
        assert_eq!(bd.tm_mday, 1);
        assert_eq!(bd.tm_wday, 4); // Thursday
        assert_eq!(bd.tm_yday, 0);
        assert_eq!(bd.tm_hour, 0);
        assert_eq!(bd.tm_min, 0);
        assert_eq!(bd.tm_sec, 0);
    }

    #[test]
    fn known_date_2024_01_15_utc() {
        // 2024-01-15 12:34:56 UTC
        // Computed externally: 1705322096
        let t: i64 = 1_705_322_096;
        let bd = nolocks_localtime(t, 0, 0);
        assert_eq!(bd.tm_year, 124); // 2024 - 1900
        assert_eq!(bd.tm_mon, 0); // January
        assert_eq!(bd.tm_mday, 15);
        assert_eq!(bd.tm_hour, 12);
        assert_eq!(bd.tm_min, 34);
        assert_eq!(bd.tm_sec, 56);
    }

    #[test]
    fn leap_year_detection() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
        assert!(is_leap_year(1972));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/localtime.c  (128 lines, 2 functions)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Pure arithmetic port; output-param struct tm becomes
//                  BrokenDownTime return value. Negative adjusted timestamps
//                  are clamped to epoch (C has UB there). Tests cover epoch,
//                  a known date, and leap-year logic.
// ──────────────────────────────────────────────────────────────────────────

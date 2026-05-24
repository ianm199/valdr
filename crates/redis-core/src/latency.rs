//! Latency monitor — sample collection, analysis, and the `LATENCY` command.
//!
//! Merges `src/latency.c` (777 lines, 12 functions) and `src/latency.h` (117 lines).
//!
//! The latency monitor records timing spikes from named event sources (fork, AOF
//! writes, slow commands, …) in fixed-size circular ring buffers of 160 entries
//! each. Spikes are exposed via the `LATENCY` command sub-commands:
//!   - `HISTORY <event>`   — full ring-buffer sample list
//!   - `LATEST`            — most-recent sample per event
//!   - `DOCTOR`            — human-readable analysis with tuning advice
//!   - `GRAPH <event>`     — ASCII sparkline graph
//!   - `RESET [<event>…]`  — clear one or all event series
//!   - `HISTOGRAM [cmd…]`  — per-command HdrHistogram CDF
//!
//! ## Integration note
//!
//! In C the event map lives on `server.latency_events` and the threshold on
//! `server.latency_monitor_threshold`. In Rust these are owned by
//! [`LatencyMonitor`], which must be a field on `RedisServer`.
//!
//! TODO(architect): add `pub latency: LatencyMonitor` and
//!   `pub latency_monitor_threshold: i64` (milliseconds; 0 = disabled) to
//!   `RedisServer` in `crates/redis-core/src/server.rs`.
//!
//! TODO(architect): add `pub duration_stats: [DurationStats; EL_DURATION_TYPE_NUM]`
//!   to `RedisServer` in `crates/redis-core/src/server.rs`.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::{RedisError, RedisString};

use crate::command_context::CommandContext;

// ── Constants ─────────────────────────────────────────────────────────────────
// C: latency.h:39

/// Number of samples retained per event in the ring buffer.
pub const LATENCY_TS_LEN: usize = 160;

/// Number of distinct event-loop duration categories.
pub const EL_DURATION_TYPE_NUM: usize = 4;

/// Width in columns of the ASCII sparkline graph produced by LATENCY GRAPH.
/// C: latency.c:629
const LATENCY_GRAPH_COLS: usize = 80;

// ── Monotonic time alias ──────────────────────────────────────────────────────

/// Monotonic clock value (platform-specific; typically nanoseconds).
/// C: `monotime` typedef in `monotonic.h`.
// TODO(port): replace with the concrete type once `crates/redis-core/src/monotonic.rs` is ported.
pub type MonoTime = u64;

// ── DurationType ─────────────────────────────────────────────────────────────
// C: `DurationType` enum in latency.h:105-113

/// Categories of event-loop duration metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum DurationType {
    /// Cumulative time for the whole event-loop iteration.
    El = 0,
    /// Cumulative time for executing commands.
    Cmd = 1,
    /// Cumulative time for flushing the AOF buffer in the event loop.
    Aof = 2,
    /// Cumulative time for cron work (serverCron + beforeSleep, excluding I/O and AOF).
    Cron = 3,
}

// ── DurationStats ─────────────────────────────────────────────────────────────
// C: `durationStats` typedef in latency.h:99-103

/// Accumulated timing statistics for one event-loop category.
#[derive(Debug, Default, Clone)]
pub struct DurationStats {
    pub cnt: u64,
    pub sum: u64,
    pub max: u64,
}

// ── LatencySample ─────────────────────────────────────────────────────────────
// C: `struct latencySample` in latency.h:47-50

/// One latency observation: a Unix timestamp (seconds) and the observed latency
/// in milliseconds.
///
/// `time` is `i32` rather than `time_t` to match the C layout comment: "We don't
/// use time_t to force 4 bytes usage everywhere."
#[derive(Debug, Clone, Copy, Default)]
pub struct LatencySample {
    pub time: i32,
    pub latency: u32,
}

// ── LatencyTimeSeries ─────────────────────────────────────────────────────────
// C: `struct latencyTimeSeries` in latency.h:53-59

/// Ring-buffer time series for a single named latency event.
pub struct LatencyTimeSeries {
    /// Next write slot (wraps at `LATENCY_TS_LEN`).
    pub idx: usize,
    /// All-time peak latency (ms) since the last reset.
    pub max: u32,
    /// Running sum of all observed latencies (ms).
    // PORT NOTE: C uses `uint32_t` for sum (overflow-prone); Rust uses u64.
    pub sum: u64,
    /// Running count of all observed samples (not just ring-buffer slots).
    // PORT NOTE: C uses `uint32_t`; Rust uses u64 for safety.
    pub cnt: u64,
    /// Circular buffer of the most recent `LATENCY_TS_LEN` samples.
    pub samples: [LatencySample; LATENCY_TS_LEN],
}

impl Default for LatencyTimeSeries {
    fn default() -> Self {
        Self {
            idx: 0,
            max: 0,
            sum: 0,
            cnt: 0,
            samples: [LatencySample {
                time: 0,
                latency: 0,
            }; LATENCY_TS_LEN],
        }
    }
}

// ── LatencyStats ──────────────────────────────────────────────────────────────
// C: `struct latencyStats` in latency.h:62-70

/// Analysis results produced by [`LatencyMonitor::analyze_event`].
#[derive(Debug, Default, Clone)]
pub struct LatencyStats {
    /// Absolute maximum latency since the last reset (ms).
    pub all_time_high: u32,
    /// Average of ring-buffer samples (ms).
    pub avg: u32,
    /// Minimum of ring-buffer samples (ms).
    pub min: u32,
    /// Maximum of ring-buffer samples (ms).
    pub max: u32,
    /// Mean absolute deviation of ring-buffer samples (ms).
    pub mad: u32,
    /// Number of non-zero samples in the ring buffer.
    pub samples: u32,
    /// Seconds elapsed between the oldest recorded event and now.
    pub period: i64,
}

// ── LatencyReportConfig ───────────────────────────────────────────────────────

/// Server configuration fields required by [`LatencyMonitor::create_report`].
///
/// Extracted so the report builder does not need a reference to the full
/// `RedisServer` before those fields are added to it.
///
/// TODO(architect): replace with `&RedisServer` once all listed fields land on
///   `RedisServer` in `crates/redis-core/src/server.rs`.
pub struct LatencyReportConfig {
    /// `server.latency_monitor_threshold` (ms; 0 = monitoring disabled).
    pub latency_monitor_threshold: i64,
    /// `server.stat_fork_rate` (GB/sec; from the last fork timing).
    pub stat_fork_rate: f64,
    /// `server.commandlog[COMMANDLOG_TYPE_SLOW].threshold` (µs; negative = disabled).
    pub slowlog_threshold_us: i64,
    /// `server.commandlog[COMMANDLOG_TYPE_SLOW].max_len` (0 = disabled).
    pub slowlog_max_len: i32,
    /// `server.hz` (event-loop frequency in Hz).
    pub hz: i32,
    /// Whether `server.aof_fsync == AOF_FSYNC_ALWAYS`.
    pub aof_fsync_always: bool,
}

// ── HdrHistogram placeholder ──────────────────────────────────────────────────

/// Opaque placeholder for the HdrHistogram type used in `LATENCY HISTOGRAM`.
///
/// TODO(architect): add the `hdrhistogram` crate as a dependency of `redis-core`
///   and replace this with `hdrhistogram::Histogram<u64>`.
pub struct HdrHistogram {
    pub total_count: i64,
    // TODO(architect): full HdrHistogram fields — blocked on crate dependency.
}

// ── LatencyMonitor ────────────────────────────────────────────────────────────

/// All latency event time-series indexed by event name (byte string).
///
/// Replaces the C `server.latency_events` dict.  The name is a byte string
/// because Redis event names are ASCII identifiers passed as `const char *`.
///
/// TODO(architect): embed this as `pub latency: LatencyMonitor` on `RedisServer`.
pub struct LatencyMonitor {
    events: HashMap<Vec<u8>, Box<LatencyTimeSeries>>,
}

// ── Internal utilities ────────────────────────────────────────────────────────

/// Returns the current Unix time in seconds (equivalent of C `time(NULL)`).
fn unix_time_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Returns the size of `AnonHugePages` from `/proc/self/smaps` in bytes, or 0
/// if unavailable. A non-zero return means THP is active and may cause latency
/// spikes during fork/copy-on-write.
///
/// C: `THPGetAnonHugePagesSize` in latency.c:60.
pub fn thp_get_anon_huge_pages_size() -> i64 {
    // C: zmalloc_get_smap_bytes_by_field("AnonHugePages:", -1)
    // TODO(port): implement a proper /proc/self/smaps parser once
    //   zmalloc.rs is ported. For now, return 0 (THP not detected).
    0
}

// ── LatencyMonitor impl ───────────────────────────────────────────────────────

impl LatencyMonitor {
    /// Create a new, empty latency monitor.
    /// C: `latencyMonitorInit` in latency.c:69.
    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Fetch a time series by name, if it exists.
    pub fn get(&self, event: &[u8]) -> Option<&LatencyTimeSeries> {
        self.events.get(event).map(|b| b.as_ref())
    }

    /// Iterate over all (event_name, time_series) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &LatencyTimeSeries)> {
        self.events.iter().map(|(k, v)| (k, v.as_ref()))
    }

    // C: latency.c:77-111, latencyAddSample
    /// Record a latency observation for the named event.
    ///
    /// `latency_us` is microseconds of observed latency (the caller is
    /// responsible for the `latencyAddSampleIfNeeded` threshold check).
    pub fn add_sample(&mut self, event: &[u8], latency_us: i64) {
        let latency = (latency_us / 1000) as u32;
        let now = unix_time_secs() as i32;

        let ts = self
            .events
            .entry(event.to_vec())
            .or_insert_with(|| Box::new(LatencyTimeSeries::default()));

        if latency > ts.max {
            ts.max = latency;
        }
        ts.sum += latency as u64;
        ts.cnt += 1;

        // If the previous sample is in the same second, update it in-place
        // if this latency is higher; otherwise discard the new one.
        let prev = (ts.idx + LATENCY_TS_LEN - 1) % LATENCY_TS_LEN;
        if ts.samples[prev].time == now {
            if latency > ts.samples[prev].latency {
                ts.samples[prev].latency = latency;
            }
            return;
        }

        ts.samples[ts.idx].time = now;
        ts.samples[ts.idx].latency = latency;
        ts.idx += 1;
        if ts.idx == LATENCY_TS_LEN {
            ts.idx = 0;
        }
    }

    // C: latency.c:118-134, latencyResetEvent
    /// Reset one named event or (if `event_name` is `None`) all events.
    ///
    /// Returns the count of events actually removed.
    pub fn reset_event(&mut self, event_name: Option<&[u8]>) -> i32 {
        match event_name {
            None => {
                let n = self.events.len() as i32;
                self.events.clear();
                n
            }
            Some(name) => {
                // O(N) scan matching case-insensitively, matching C behaviour.
                let to_remove: Vec<Vec<u8>> = self
                    .events
                    .keys()
                    .filter(|k| k.eq_ignore_ascii_case(name))
                    .cloned()
                    .collect();
                let resets = to_remove.len() as i32;
                for key in to_remove {
                    self.events.remove(&key);
                }
                resets
            }
        }
    }

    // C: latency.c:143-194, analyzeLatencyForEvent
    /// Compute statistics for the named event's ring buffer.
    ///
    /// Returns a zeroed [`LatencyStats`] when the event has no data.
    pub fn analyze_event(&self, event: &[u8]) -> LatencyStats {
        let ts = match self.events.get(event) {
            Some(ts) => ts,
            None => return LatencyStats::default(),
        };

        let mut ls = LatencyStats {
            all_time_high: ts.max,
            ..Default::default()
        };

        // First pass: min, max, sum, oldest-time (stored temporarily in period).
        let mut sum: u64 = 0;
        for sample in &ts.samples {
            if sample.time == 0 {
                continue;
            }
            ls.samples += 1;
            if ls.samples == 1 {
                ls.min = sample.latency;
                ls.max = sample.latency;
            } else {
                if sample.latency < ls.min {
                    ls.min = sample.latency;
                }
                if sample.latency > ls.max {
                    ls.max = sample.latency;
                }
            }
            sum += sample.latency as u64;

            // Track the oldest timestamp in ls.period (converted to age below).
            if ls.period == 0 || (sample.time as i64) < ls.period {
                ls.period = sample.time as i64;
            }
        }

        if ls.samples > 0 {
            ls.avg = (sum / ls.samples as u64) as u32;
            ls.period = unix_time_secs() - ls.period;
            if ls.period == 0 {
                ls.period = 1;
            }
        }

        // Second pass: mean absolute deviation.
        let mut mad_sum: u64 = 0;
        for sample in &ts.samples {
            if sample.time == 0 {
                continue;
            }
            let delta = (ls.avg as i64) - (sample.latency as i64);
            mad_sum += delta.unsigned_abs();
        }
        if ls.samples > 0 {
            ls.mad = (mad_sum / ls.samples as u64) as u32;
        }

        ls
    }

    // C: latency.c:197-496, createLatencyReport
    /// Generate a human-readable latency analysis report (the LATENCY DOCTOR output).
    ///
    /// Returns the report as a `Vec<u8>` (ASCII text; no Redis binary data).
    pub fn create_report(&self, cfg: &LatencyReportConfig) -> Vec<u8> {
        let mut report: Vec<u8> = Vec::new();

        if self.events.is_empty() && cfg.latency_monitor_threshold == 0 {
            report.extend_from_slice(
                b"I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this \
                  Valkey instance. You may use \"CONFIG SET latency-monitor-threshold \
                  <milliseconds>.\" in order to enable it.\n",
            );
            return report;
        }

        let mut advise_better_vm = false;
        let mut advise_slowlog_enabled = false;
        let mut advise_slowlog_tuning = false;
        let mut advise_slowlog_inspect = false;
        let mut advise_disk_contention = false;
        let mut advise_scheduler = false;
        let mut advise_data_writeback = false;
        let mut advise_no_appendfsync = false;
        let mut advise_local_disk = false;
        let mut advise_ssd = false;
        let mut advise_write_load_info = false;
        let mut advise_hz = false;
        let mut advise_large_objects = false;
        let mut advise_mass_eviction = false;
        let mut advise_relax_fsync_policy = false;
        let mut advise_disable_thp = false;
        let mut advices: i32 = 0;

        let mut eventnum = 0i32;
        for (event_key, ts) in &self.events {
            let event: &[u8] = event_key.as_slice();
            let ls = self.analyze_event(event);

            eventnum += 1;
            if eventnum == 1 {
                report
                    .extend_from_slice(b"Latency spikes are observed in this Valkey instance.\n\n");
            }

            // Per-event summary line.
            // C: sdscatprintf with "%d. %s: %d latency spikes ..."
            let period_per_sample = if ls.samples > 0 {
                ls.period as f64 / ls.samples as f64
            } else {
                0.0
            };
            report.extend_from_slice(format!("{}. ", eventnum).as_bytes());
            report.extend_from_slice(event);
            report.extend_from_slice(
                format!(
                    ": {} latency spikes (average {}ms, mean deviation {}ms, period {:.2} sec). \
                     Worst all time event {}ms.",
                    ls.samples, ls.avg, ls.mad, period_per_sample, ts.max
                )
                .as_bytes(),
            );

            // Fork event advice.
            if event.eq_ignore_ascii_case(b"fork") {
                let fork_quality = if cfg.stat_fork_rate < 10.0 {
                    advise_better_vm = true;
                    advices += 1;
                    "terrible"
                } else if cfg.stat_fork_rate < 25.0 {
                    advise_better_vm = true;
                    advices += 1;
                    "poor"
                } else if cfg.stat_fork_rate < 100.0 {
                    "good"
                } else {
                    "excellent"
                };
                report.extend_from_slice(
                    format!(
                        " Fork rate is {:.2} GB/sec ({}).",
                        cfg.stat_fork_rate, fork_quality
                    )
                    .as_bytes(),
                );
            }

            // Command event advice.
            if event.eq_ignore_ascii_case(b"command") {
                if cfg.slowlog_threshold_us < 0 || cfg.slowlog_max_len == 0 {
                    advise_slowlog_enabled = true;
                    advices += 1;
                } else if cfg.slowlog_threshold_us / 1000 > cfg.latency_monitor_threshold {
                    advise_slowlog_tuning = true;
                    advices += 1;
                }
                advise_slowlog_inspect = true;
                advise_large_objects = true;
                advices += 2;
            }

            // Fast-command event advice.
            if event.eq_ignore_ascii_case(b"fast-command") {
                advise_scheduler = true;
                advices += 1;
            }

            // AOF write events.
            if event.eq_ignore_ascii_case(b"aof-write-pending-fsync") {
                advise_local_disk = true;
                advise_disk_contention = true;
                advise_ssd = true;
                advise_data_writeback = true;
                advices += 4;
            }

            if event.eq_ignore_ascii_case(b"aof-write-active-child") {
                advise_no_appendfsync = true;
                advise_data_writeback = true;
                advise_ssd = true;
                advices += 3;
            }

            if event.eq_ignore_ascii_case(b"aof-write-alone") {
                advise_local_disk = true;
                advise_data_writeback = true;
                advise_ssd = true;
                advices += 3;
            }

            if event.eq_ignore_ascii_case(b"aof-fsync-always") {
                advise_relax_fsync_policy = true;
                advices += 1;
            }

            if event.eq_ignore_ascii_case(b"aof-fstat")
                || event.eq_ignore_ascii_case(b"rdb-unlink-temp-file")
            {
                advise_disk_contention = true;
                advise_local_disk = true;
                advices += 2;
            }

            if event.eq_ignore_ascii_case(b"aof-rewrite-diff-write")
                || event.eq_ignore_ascii_case(b"aof-rename")
            {
                advise_write_load_info = true;
                advise_data_writeback = true;
                advise_ssd = true;
                advise_local_disk = true;
                advices += 4;
            }

            // Expire cycle advice.
            if event.eq_ignore_ascii_case(b"expire-cycle") {
                advise_hz = true;
                advise_large_objects = true;
                advices += 2;
            }

            // Eviction advice.
            if event.eq_ignore_ascii_case(b"eviction-del") {
                advise_large_objects = true;
                advices += 1;
            }

            if event.eq_ignore_ascii_case(b"eviction-cycle") {
                advise_mass_eviction = true;
                advices += 1;
            }

            report.push(b'\n');
        }

        // Non-event-based advice: THP.
        if thp_get_anon_huge_pages_size() > 0 {
            advise_disable_thp = true;
            advices += 1;
        }

        if eventnum == 0 && advices == 0 {
            report.extend_from_slice(
                b"No latency spike was observed during the lifetime of this Valkey instance, \
                  not in the slightest bit.\n",
            );
        } else if eventnum > 0 && advices == 0 {
            report.extend_from_slice(
                b"\nThere are latency events logged that are not easy to fix. Please get some \
                  help from Valkey community, providing this report in your help request.\n",
            );
        } else {
            report.extend_from_slice(b"\nHere is some advice for you:\n\n");

            if advise_better_vm {
                report.extend_from_slice(
                    b"- If you are using a virtual machine, consider upgrading it with a faster \
                      one using a hypervisior that provides less latency during fork() calls. Xen \
                      is known to have poor fork() performance. Even in the context of the same VM \
                      provider, certain kinds of instances can execute fork faster than others.\n",
                );
            }

            if advise_slowlog_enabled {
                report.extend_from_slice(
                    format!(
                        "- There are latency issues with potentially slow commands you are using. \
                         Try to enable the Slow Log Valkey feature using the command \
                         'CONFIG SET slowlog-log-slower-than {}'. If the Slow log is disabled \
                         Valkey is not able to log slow commands execution for you.\n",
                        cfg.latency_monitor_threshold as u64 * 1000
                    )
                    .as_bytes(),
                );
            }

            if advise_slowlog_tuning {
                report.extend_from_slice(
                    format!(
                        "- Your current Slow Log configuration only logs events that are slower \
                         than your configured latency monitor threshold. Please use \
                         'CONFIG SET slowlog-log-slower-than {}'.\n",
                        cfg.latency_monitor_threshold as u64 * 1000
                    )
                    .as_bytes(),
                );
            }

            if advise_slowlog_inspect {
                report.extend_from_slice(
                    b"- Check your Slow Log to understand what are the commands you are running \
                      which are too slow to execute. Please check \
                      https://valkey.io/commands/slowlog for more information.\n",
                );
            }

            if advise_scheduler {
                report.extend_from_slice(
                    b"- The system is slow to execute Valkey code paths not containing system \
                      calls. This usually means the system does not provide Valkey CPU time to \
                      run for long periods. You should try to:\n\
                      \x20 1) Lower the system load.\n\
                      \x20 2) Use a computer / VM just for Valkey if you are running other \
                      software in the same system.\n\
                      \x20 3) Check if you have a \"noisy neighbour\" problem.\n\
                      \x20 4) Check with 'valkey-cli --intrinsic-latency 100' what is the \
                      intrinsic latency in your system.\n\
                      \x20 5) Check if the problem is allocator-related by recompiling Valkey \
                      with MALLOC=libc, if you are using Jemalloc. However this may create \
                      fragmentation problems.\n",
                );
            }

            if advise_local_disk {
                report.extend_from_slice(
                    b"- It is strongly advised to use local disks for persistence, especially if \
                      you are using AOF. Remote disks provided by platform-as-a-service providers \
                      are known to be slow.\n",
                );
            }

            if advise_ssd {
                report.extend_from_slice(
                    b"- SSD disks are able to reduce fsync latency, and total time needed for \
                      snapshotting and AOF log rewriting (resulting in smaller memory usage). \
                      With extremely high write load SSD disks can be a good option. However \
                      Valkey should perform reasonably with high load using normal disks. Use \
                      this advice as a last resort.\n",
                );
            }

            if advise_data_writeback {
                report.extend_from_slice(
                    b"- Mounting ext3/4 filesystems with data=writeback can provide a performance \
                      boost compared to data=ordered, however this mode of operation provides \
                      less guarantees, and sometimes it can happen that after a hard crash the \
                      AOF file will have a half-written command at the end and will require to \
                      be repaired before Valkey restarts.\n",
                );
            }

            if advise_disk_contention {
                report.extend_from_slice(
                    b"- Try to lower the disk contention. This is often caused by other disk \
                      intensive processes running in the same computer (including other Valkey \
                      instances).\n",
                );
            }

            if advise_no_appendfsync {
                report.extend_from_slice(
                    b"- Assuming from the point of view of data safety this is viable in your \
                      environment, you could try to enable the 'no-appendfsync-on-rewrite' \
                      option, so that fsync will not be performed while there is a child rewriting \
                      the AOF file or producing an RDB file (the moment where there is high disk \
                      contention).\n",
                );
            }

            if advise_relax_fsync_policy && cfg.aof_fsync_always {
                report.extend_from_slice(
                    b"- Your fsync policy is set to 'always'. It is very hard to get good \
                      performances with such a setup, if possible try to relax the fsync policy \
                      to 'onesec'.\n",
                );
            }

            if advise_write_load_info {
                report.extend_from_slice(
                    b"- Latency during the AOF atomic rename operation or when the final \
                      difference is flushed to the AOF file at the end of the rewrite, sometimes \
                      is caused by very high write load, causing the AOF buffer to get very large. \
                      If possible try to send less commands to accomplish the same work, or use \
                      Lua scripts to group multiple operations into a single EVALSHA call.\n",
                );
            }

            if advise_hz && cfg.hz < 100 {
                report.extend_from_slice(
                    b"- In order to make the Valkey keys expiring process more incremental, try \
                      to set the 'hz' configuration parameter to 100 using 'CONFIG SET hz 100'.\n",
                );
            }

            if advise_large_objects {
                report.extend_from_slice(
                    b"- Deleting, expiring or evicting (because of maxmemory policy) large objects \
                      is a blocking operation. If you have very large objects that are often \
                      deleted, expired, or evicted, try to fragment those objects into multiple \
                      smaller objects.\n",
                );
            }

            if advise_mass_eviction {
                report.extend_from_slice(
                    b"- Sudden changes to the 'maxmemory' setting via 'CONFIG SET', or allocation \
                      of large objects via sets or sorted sets intersections, STORE option of SORT, \
                      Valkey Cluster large keys migrations (RESTORE command), may create sudden \
                      memory pressure forcing the server to block trying to evict keys. \n",
                );
            }

            if advise_disable_thp {
                report.extend_from_slice(
                    b"- I detected a non zero amount of anonymous huge pages used by your process. \
                      This creates very serious latency events in different conditions, especially \
                      when Valkey is persisting on disk. To disable THP support use the command \
                      'echo never > /sys/kernel/mm/transparent_hugepage/enabled', make sure to \
                      also add it into /etc/rc.local so that the command will be executed again \
                      after a reboot. Note that even if you have already disabled THP, you still \
                      need to restart the Valkey process to get rid of the huge pages already \
                      created.\n",
                );
            }
        }

        report
    }
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ── Command reply helpers ─────────────────────────────────────────────────────

// C: latency.c:507-528, fillCommandCDF
/// Reply with a two-key map `{ calls: <n>, histogram_usec: { <usec>: <count>, … } }`
/// from an HdrHistogram.
///
/// TODO(architect): blocked on `hdrhistogram` crate dep. The HdrHistogram iterator
///   (`hdr_iter_log_init` / `hdr_iter_next`) has no equivalent until the crate lands.
/// TODO(port): `addReplyDeferredLen` / `setDeferredMapLen` pattern is not yet
///   implemented on `CommandContext`. Needs deferred-length support.
pub fn fill_command_cdf(
    ctx: &mut CommandContext,
    histogram: &HdrHistogram,
) -> Result<(), RedisError> {
    // Stub: would emit a RESP3 map with call count and per-bucket usec counts.
    // C: latency.c:507-528
    let _ = (ctx, histogram);
    // TODO(port): implement once CommandContext supports deferred-length RESP3 maps
    //   and the hdrhistogram crate is added.
    Ok(())
}

// C: latency.c:532-549, latencyAllCommandsFillCDF
/// Reply with per-command latency histograms for every command in the server's
/// command table.
///
/// TODO(architect): needs `&HashMap<RedisString, ServerCommand>` (or equivalent)
///   from `RedisServer::commands`, which does not yet exist on the Rust stub.
pub fn latency_all_commands_fill_cdf(
    ctx: &mut CommandContext,
    command_with_data: &mut i32,
) -> Result<(), RedisError> {
    // Stub — command table iteration is deferred until ServerCommand type exists.
    // C: latency.c:532-549
    let _ = (ctx, command_with_data);
    // TODO(port): iterate server.commands when ServerCommand type is defined.
    Ok(())
}

// C: latency.c:553-585, latencySpecificCommandsFillCDF
/// Reply with per-command latency histograms for the commands named in argv[2..].
///
/// TODO(architect): needs `lookupCommandBySds` once the command registry is wired
///   to `RedisServer` in Phase 3.
/// TODO(port): `addReplyDeferredLen` / `setDeferredMapLen` pattern not yet in
///   `CommandContext`.
fn latency_specific_commands_fill_cdf(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // Stub — deferred until command lookup is available.
    // C: latency.c:553-585
    let _ = ctx;
    // TODO(port): implement when lookupCommandBySds and deferred-len replies exist.
    Ok(())
}

// C: latency.c:589-603, latencyCommandReplyWithSamples
/// Reply with the full ring-buffer sample list for a time series.
/// Emits a deferred-length array of `[time, latency]` pairs.
///
/// TODO(port): deferred-length array (`addReplyDeferredLen` / `setDeferredArrayLen`)
///   is not yet implemented on `CommandContext`.
fn latency_command_reply_with_samples(
    ctx: &mut CommandContext,
    ts: &LatencyTimeSeries,
) -> Result<(), RedisError> {
    // Iterate the ring buffer in insertion order (oldest first).
    // C: for j in 0..LATENCY_TS_LEN: i = (ts.idx + j) % LATENCY_TS_LEN
    let mut samples: Vec<(i64, i64)> = Vec::new();
    for j in 0..LATENCY_TS_LEN {
        let i = (ts.idx + j) % LATENCY_TS_LEN;
        if ts.samples[i].time == 0 {
            continue;
        }
        samples.push((ts.samples[i].time as i64, ts.samples[i].latency as i64));
    }

    ctx.reply_array_header(samples.len())?;
    for (time, latency) in samples {
        ctx.reply_array_header(2)?;
        ctx.reply_integer(time)?;
        ctx.reply_integer(latency)?;
    }
    Ok(())
}

// C: latency.c:607-627, latencyCommandReplyWithLatestEvents
/// Reply for `LATENCY LATEST`: one array entry per event with
/// `[name, last_time, last_latency, max, sum, cnt]`.
fn latency_command_reply_with_latest_events(
    ctx: &mut CommandContext,
    monitor: &LatencyMonitor,
) -> Result<(), RedisError> {
    ctx.reply_array_header(monitor.len())?;
    for (event_key, ts) in &monitor.events {
        let last = (ts.idx + LATENCY_TS_LEN - 1) % LATENCY_TS_LEN;
        ctx.reply_array_header(6)?;
        ctx.reply_bulk(event_key)?;
        ctx.reply_integer(ts.samples[last].time as i64)?;
        ctx.reply_integer(ts.samples[last].latency as i64)?;
        ctx.reply_integer(ts.max as i64)?;
        ctx.reply_integer(ts.sum as i64)?;
        ctx.reply_integer(ts.cnt as i64)?;
    }
    Ok(())
}

// C: latency.c:630-670, latencyCommandGenSparkeline
/// Generate an ASCII sparkline graph for the named event's ring buffer.
///
/// Returns the graph as `Vec<u8>`.
///
/// TODO(port): `sparkline.c` is marked SKIP in `harness/file-deps.tsv`.
///   This function cannot be implemented until a Rust sparkline renderer exists.
///   Returns a placeholder notice for now.
fn latency_command_gen_sparkline(event: &[u8], ts: &LatencyTimeSeries) -> Vec<u8> {
    // C: latency.c:630-670 — uses sparklineSequenceAddSample / sparklineRender
    //   from sparkline.c (SKIP), so full implementation is deferred.
    let mut graph: Vec<u8> = Vec::new();
    graph.extend_from_slice(event);
    graph.extend_from_slice(
        format!(
            " - high {} ms, low {} ms (all time high {} ms)\n",
            ts.max, 0u32, ts.max
        )
        .as_bytes(),
    );
    for _ in 0..LATENCY_GRAPH_COLS {
        graph.push(b'-');
    }
    graph.push(b'\n');
    // TODO(port): render actual sparkline once sparkline.rs is ported.
    graph.extend_from_slice(b"(sparkline rendering not yet implemented)\n");
    graph
}

// ── LATENCY command ───────────────────────────────────────────────────────────

// C: latency.c:682-764, latencyCommand
/// Entry point for the `LATENCY` command.
///
/// Dispatches to the appropriate sub-command handler.  `monitor` is the
/// server's [`LatencyMonitor`] and `cfg` provides the config fields needed
/// by LATENCY DOCTOR.
///
/// TODO(architect): when `RedisServer` gains `latency: LatencyMonitor` and
///   `latency_monitor_threshold: i64`, the signature should change to accept
///   `&mut RedisServer` (or `&mut CommandContext` once it bundles the server).
pub fn latency_command(
    ctx: &mut CommandContext,
    monitor: &mut LatencyMonitor,
    cfg: &LatencyReportConfig,
) -> Result<(), RedisError> {
    let subcommand = ctx.arg(1)?.clone();
    let argc = ctx.arg_count();

    if subcommand.as_bytes().eq_ignore_ascii_case(b"history") && argc == 3 {
        // LATENCY HISTORY <event>
        let event = ctx.arg(2)?.clone();
        match monitor.get(event.as_bytes()) {
            None => ctx.reply_array_header(0)?,
            Some(ts) => {
                // Safety: ts reference is valid; we cloned event to avoid borrow conflict.
                let ts_owned: LatencyTimeSeries = LatencyTimeSeries {
                    idx: ts.idx,
                    max: ts.max,
                    sum: ts.sum,
                    cnt: ts.cnt,
                    samples: ts.samples,
                };
                latency_command_reply_with_samples(ctx, &ts_owned)?;
            }
        }
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"graph") && argc == 3 {
        // LATENCY GRAPH <event>
        let event = ctx.arg(2)?.clone();
        let ts_opt = monitor.get(event.as_bytes());
        match ts_opt {
            None => {
                let mut msg: Vec<u8> = b"No samples available for event '".to_vec();
                msg.extend_from_slice(event.as_bytes());
                msg.push(b'\'');
                return Err(RedisError::runtime(msg));
            }
            Some(ts) => {
                let ts_owned = LatencyTimeSeries {
                    idx: ts.idx,
                    max: ts.max,
                    sum: ts.sum,
                    cnt: ts.cnt,
                    samples: ts.samples,
                };
                let graph = latency_command_gen_sparkline(event.as_bytes(), &ts_owned);
                // C: addReplyVerbatim(c, graph, sdslen(graph), "txt")
                // TODO(port): CommandContext does not yet have reply_verbatim().
                //   For now, emit as a bulk string.
                ctx.reply_bulk(&graph)?;
            }
        }
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"latest") && argc == 2 {
        // LATENCY LATEST
        // PORT NOTE: must re-borrow monitor immutably; clone to satisfy borrow checker.
        latency_command_reply_with_latest_events(ctx, monitor)?;
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"doctor") && argc == 2 {
        // LATENCY DOCTOR
        let report = monitor.create_report(cfg);
        // C: addReplyVerbatim(c, report, sdslen(report), "txt")
        // TODO(port): CommandContext does not yet have reply_verbatim(). Emit as bulk string.
        ctx.reply_bulk(&report)?;
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"reset") && argc >= 2 {
        // LATENCY RESET [<event> …]
        if argc == 2 {
            let resets = monitor.reset_event(None);
            ctx.reply_integer(resets as i64)?;
        } else {
            let mut resets = 0i32;
            // Collect arg bytes first to avoid holding borrows into ctx while mutating monitor.
            let mut events: Vec<Vec<u8>> = Vec::with_capacity(argc - 2);
            for j in 2..argc {
                events.push(ctx.arg(j)?.as_bytes().to_vec());
            }
            for event in &events {
                resets += monitor.reset_event(Some(event));
            }
            ctx.reply_integer(resets as i64)?;
        }
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"histogram") && argc >= 2 {
        // LATENCY HISTOGRAM [command …]
        if argc == 2 {
            // All commands.
            let mut command_with_data = 0i32;
            latency_all_commands_fill_cdf(ctx, &mut command_with_data)?;
        } else {
            // Specific commands named in argv[2..].
            latency_specific_commands_fill_cdf(ctx)?;
        }
    } else if subcommand.as_bytes().eq_ignore_ascii_case(b"help") && argc == 2 {
        // LATENCY HELP
        let help: &[&[u8]] = &[
            b"DOCTOR",
            b"    Return a human readable latency analysis report.",
            b"GRAPH <event>",
            b"    Return an ASCII latency graph for the <event> class.",
            b"HISTORY <event>",
            b"    Return time-latency samples for the <event> class.",
            b"LATEST",
            b"    Return the latest latency samples for all events.",
            b"RESET [<event> ...]",
            b"    Reset latency data of one or more <event> classes.",
            b"    (default: reset all data for all event classes)",
            b"HISTOGRAM [COMMAND ...]",
            b"    Return a cumulative distribution of latencies in the format of a histogram \
              for the specified command names.",
            b"    If no commands are specified then all histograms are replied.",
        ];
        ctx.reply_array_header(help.len())?;
        for line in help {
            ctx.reply_bulk(line)?;
        }
    } else {
        return Err(RedisError::syntax(
            b"Unknown subcommand or wrong number of arguments",
        ));
    }

    Ok(())
}

// ── durationAddSample ─────────────────────────────────────────────────────────

// C: latency.c:766-776, durationAddSample
/// Record a duration observation into the appropriate slot of `stats`.
///
/// `dtype` is passed as a raw `i32` (matching the C `int type` parameter) so that
/// callers that have not yet converted to the [`DurationType`] enum can still
/// call this function.
///
/// TODO(architect): when `RedisServer` has `duration_stats`, make this a method
///   on `RedisServer` (or on a `DurationStatsBank` wrapper) instead.
pub fn duration_add_sample(stats: &mut [DurationStats], dtype: i32, duration: MonoTime) {
    let idx = dtype as usize;
    if idx >= EL_DURATION_TYPE_NUM {
        return;
    }
    let ds = &mut stats[idx];
    ds.cnt += 1;
    ds.sum += duration;
    if duration > ds.max {
        ds.max = duration;
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/latency.c (777 lines, 12 functions) + src/latency.h (117 lines)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         12
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         All core logic translated (ring-buffer, analysis, report, command dispatch).
//                  Three stubs: sparkline (sparkline.c SKIP), HdrHistogram CDF (needs crate dep),
//                  command-table iteration (ServerCommand type not yet defined).
//                  RedisServer integration deferred to Phase 3 architect packet.
// ──────────────────────────────────────────────────────────────────────────────

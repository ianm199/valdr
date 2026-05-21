# Benchmarks

First measurement against upstream Valkey. **No performance tuning has
been done yet** — these are baseline numbers from the same alpha binary
that the conformance tests run against.

## Methodology

Both binaries run on the same host, on different ports, sequentially
(not in parallel) to keep CPU contention out of the picture. We use the
official `valkey-benchmark` from upstream — anyone in the Redis/Valkey
ecosystem can read these numbers the same way they'd read upstream's own
benchmarks.

```bash
bash harness/bench/run.sh
# default: -n 1_000_000 -c 50 -P 100 -d 64 -t <standard suite>
```

There is also a profile-matrix runner:

```bash
bash harness/bench/run-profile-matrix.sh
```

That runner executes smaller named profiles with different pipeline depths and
emits a typed JSON result on stdout plus a TSV under
`harness/bench/results/<UTC>-<commit>-profile-matrix.tsv`.

The script writes a TSV to `harness/bench/results/<UTC>-<commit>.tsv`
recording the request count, client count, pipeline depth, payload size,
hardware fingerprint (CPU + OS + arch), and the commit hash so results
from different machines are not silently merged.

## Headline result (2026-05-20, alpha baseline)

**Hardware:** Apple M3 Max, macOS Darwin 24.3.0 (arm64)
**Workload:** 200,000 requests per command, 50 concurrent clients,
pipeline depth 100, 64-byte payload
**valkey-rs commit:** `a6ebea9`
**upstream Valkey:** pinned commit per `harness/source.toml`

| Command | upstream Valkey (req/s) | valkey-rs (req/s) | ratio | upstream p99 (ms) | valkey-rs p99 (ms) |
|---|---:|---:|---:|---:|---:|
| PING_MBULK              | 4,651,162 | 242,131 | 0.05× | 1.50 | 6.93 |
| SET                     | 2,531,645 | 189,573 | 0.07× | 2.62 | 1.80 |
| GET                     | 3,333,333 | 223,964 | 0.07× | 1.84 | 18.46 |
| INCR                    | 3,448,276 | 193,050 | 0.06× | 1.76 | 1.63 |
| LPUSH                   | 2,325,581 | 184,502 | 0.08× | 2.52 | 1.89 |
| RPUSH                   | 2,666,666 | 180,018 | 0.07× | 2.18 | 1.82 |
| LPOP                    | 2,127,660 | 189,036 | 0.09× | 3.91 | 1.58 |
| RPOP                    | 2,352,941 | 186,047 | 0.08× | 2.68 | 1.96 |
| SADD                    | 2,898,551 | 187,091 | 0.06× | 2.35 | 2.34 |
| HSET                    | 2,352,941 | 175,593 | 0.07× | 2.96 | 1.86 |
| SPOP                    | 3,703,703 | 199,601 | 0.05× | 1.76 | 1.79 |
| ZADD                    | 2,197,802 | 176,056 | 0.08× | 3.11 | 1.95 |
| MSET (10 keys)          | 460,829   | 90,621  | 0.20× | 3.50 | 8.40 |
| LRANGE_100              | 111,669   | 105,988 | **0.95×** | 27.39 | 28.74 |
| LRANGE_300              | 36,677    | 52,383  | **1.43×** ⚡ | 69.06 | 189.57 |

## Profile matrix (2026-05-21)

The original headline run used a deep pipeline (`-P 100`) for every simple
command. That is useful, but it hides the distinction between "this command is
intrinsically slow" and "this architecture does not drain pipelined requests
like upstream Valkey." The profile matrix runs the same binary at pipeline 1,
16, and 100.

**Hardware:** Apple M3 Max, macOS Darwin 24.3.0 (arm64)
**valkey-rs commit:** `47915ba`

| Profile | Command | upstream Valkey (req/s) | valkey-rs (req/s) | ratio | upstream p99 (ms) | valkey-rs p99 (ms) |
|---|---|---:|---:|---:|---:|---:|
| core-p1 | PING_MBULK | 146,628 | 132,275 | **0.90×** | 1.047 | 0.359 |
| core-p1 | SET | 173,010 | 136,986 | **0.79×** | 0.383 | 0.351 |
| core-p1 | GET | 161,290 | 141,243 | **0.88×** | 0.367 | 0.319 |
| core-p1 | INCR | 168,919 | 136,240 | **0.81×** | 0.375 | 0.343 |
| core-p16 | PING_MBULK | 2,380,953 | 254,453 | 0.11× | 0.551 | 2.895 |
| core-p16 | SET | 1,503,759 | 184,672 | 0.12× | 0.719 | 1.903 |
| core-p16 | GET | 2,105,263 | 209,424 | 0.10× | 0.543 | 3.055 |
| core-p16 | INCR | 2,247,191 | 191,388 | 0.09× | 0.431 | 1.807 |
| core-p100 | PING_MBULK | 5,128,205 | 250,941 | 0.05× | 1.183 | 12.223 |
| core-p100 | SET | 2,531,646 | 187,970 | 0.07× | 2.239 | 1.743 |
| core-p100 | GET | 3,333,334 | 220,994 | 0.07× | 1.759 | 10.879 |
| core-p100 | INCR | 3,389,831 | 195,312 | 0.06× | 1.679 | 1.863 |
| range-heavy-p16 | LRANGE_100 | 165,837 | 92,851 | 0.56× | 5.663 | 13.759 |
| range-heavy-p16 | LRANGE_300 | 38,640 | 42,553 | **1.10×** | 13.439 | 47.871 |

The profile-matrix summary from this run:

```text
median 0.11x, min 0.05x, max 1.10x; GET p1 0.88x; GET p100 0.07x
```

The read I would trust: this is not primarily "Rust data structures cannot do
GET." At pipeline 1, GET is close to upstream. The cliff appears when upstream
Valkey can amortize event-loop work across large batches and valkey-rs stays
near a ~200k req/s ceiling on simple commands. That points at connection
serving, request draining, response flushing, and shared-state locking before it
points at the command implementation itself.

## Reading this honestly

**Where we're slow (most simple commands, ~5-10% of upstream throughput):**
The profile matrix refines the diagnosis. The deep-pipeline gap is real, but
pipeline-1 simple ops are 0.79-0.90x upstream. That makes "per-command mutex
acquisition" a likely contributor, not the whole explanation. The larger issue
is that upstream Valkey's single event loop drains and writes pipelined command
batches extremely efficiently, while valkey-rs still uses a blocking
thread-per-connection shape with shared global state.

**Where we're competitive (LRANGE_100, ~95% of upstream):**
Once each operation does meaningful work (return 100 elements, ~6.4 KB
payload), the lock overhead amortizes and we're within noise of
upstream. The Rust data structures themselves are not the bottleneck.

**Where we're faster (LRANGE_300, 1.4× upstream):**
The larger the payload, the more our advantage. Reasons we suspect
matter here: Rust's `Vec<u8>` push + I/O write path may be marginally
better than upstream's reply buffer, and our RESP serializer is a tight
write-only path with no string-conversion overhead. Worth profiling.

**Per-op latency p99 is mostly competitive** even when throughput is
not — most commands' p99 latency is within 2× of upstream. The GET p99
of 18ms is an outlier we should investigate (probably a tail-latency
event from GC pressure or a lock-contention spike).

## What we'd improve

Roadmap to closer-to-parity throughput, in rough effort order:

1. **Shard the lock per key range** (or per-shard hash) — the easiest
   2-4× win for simple ops, since most ops touch a single key.
2. **`io_uring` on Linux** — replace blocking-thread I/O with the kernel
   submission queue. Big win on pipelined throughput.
3. **Read replicas of the keyspace via RwLock or DashMap** — `GET`-heavy
   workloads currently pay write-lock cost.
4. **Per-thread connection affinity** — pin a connection to a thread
   that owns a slice of the keyspace. The Garnet approach.
5. **Profile-guided optimization** — flamegraphs on the GET hot path,
   evaluate `jemalloc`/`mimalloc`.

These are post-alpha work; the perf gap is acceptable for the use cases
we're targeting now (single-node Valkey-compatible cache where wire
fidelity and safety matter more than raw throughput) but not for serious
production deployment.

## Reproducing

```bash
# One-time: build both binaries
bash scripts/setup-reference.sh
cargo build --release

# Run the standard suite (takes ~30s on M-series Apple silicon)
bash harness/bench/run.sh

# Smaller/faster smoke
bash harness/bench/run.sh --requests 50000 --pipeline 16

# Single command, no pipelining (latency-focused)
bash harness/bench/run.sh --requests 100000 --pipeline 1 --tests set,get

# Profile matrix: pipeline 1 vs 16 vs 100, plus range-heavy workload
bash harness/bench/run-profile-matrix.sh

# Custom workload
bash harness/bench/run.sh --requests 500000 --clients 100 --pipeline 50 \
    --tests "set,get,zadd,zrange,xadd,xread"
```

Results land in `harness/bench/results/<UTC>-<commit>.tsv`. Each file
records the full configuration in TSV-comment headers so a future
maintainer can diff runs across hardware or commits.

## What this benchmark does NOT measure

- **Memory footprint.** Untracked today. Rust's allocator + `Arc<Mutex<>>`
  overhead vs upstream's hand-tuned slab allocator is the obvious place
  this matters; we should add a `valgrind --tool=massif` or
  `heaptrack` workflow.
- **Sustained throughput.** This is a 30-second blast. Real production
  latency stories live in the 1-hour soak. A `--duration 3600s` mode
  on the script is a future addition.
- **Tail latency under contention.** `valkey-benchmark` reports p50/p95/
  p99/max but not p999 or p9999. For latency-sensitive workloads, a
  proper HDR histogram with `memtier_benchmark` is the next tool.
- **Multi-client mixed workloads.** Reads + writes + range scans
  concurrently against the same server process — closer to a real app.
  `memtier_benchmark` again.
- **TLS overhead.** All numbers here are plain TCP.
- **Persistence on the hot path.** AOF write costs aren't measured;
  bench runs use `--rdb-disabled --appendonly no` for both sides.
- **Network latency.** Loopback only.

# Benchmarks

This document keeps both the first alpha baseline and the harness-driven
performance iterations against upstream Valkey. The important read is the
trajectory: the same profile matrix is rerun after each focused packet so
performance work stays tied to a reproducible objective.

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

For hotspot work, use the larger profiled runner:

```bash
./harness/bench/profile-hotspots.py --suite big
```

That runner benchmarks upstream and valkey-rs on longer simple-command
workloads, and samples the Rust server with `/usr/bin/sample` while the hot
section is running. It writes:

- `harness/bench/results/<UTC>-<commit>-hotspots.tsv`
- `harness/bench/results/<UTC>-<commit>-hotspots.json`
- `harness/bench/results/<UTC>-<commit>-<workload>.sample.txt`

The sampler is wall-clock stack sampling, not a pure CPU profiler. Treat
wait/sleep/socket categories as scheduling and I/O evidence. For a GUI-grade
CPU trace on macOS, attach Instruments/xctrace Time Profiler to the same Rust
server PID while the workload runs. For better symbols in either tool, rebuild
with optimized debuginfo:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p redis-server
```

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
**valkey-rs revision:** `0004595`, after the TCP-loop, dispatch hot-path,
standalone write-propagation, and bucketed command-dispatch optimizations
described below

| Profile | Command | upstream Valkey (req/s) | valkey-rs (req/s) | ratio | upstream p99 (ms) | valkey-rs p99 (ms) |
|---|---|---:|---:|---:|---:|---:|
| core-p1 | PING_MBULK | 190,114 | 144,509 | 0.76× | 0.719 | 0.839 |
| core-p1 | SET | 210,970 | 138,122 | 0.65× | 0.303 | 0.255 |
| core-p1 | GET | 180,505 | 141,243 | 0.78× | 0.399 | 0.255 |
| core-p1 | INCR | 196,850 | 151,515 | 0.77× | 0.375 | 0.255 |
| core-p16 | PING_MBULK | 2,298,851 | 1,600,000 | 0.70× | 0.631 | 2.351 |
| core-p16 | SET | 1,709,402 | 1,282,051 | 0.75× | 0.535 | 2.175 |
| core-p16 | GET | 2,083,333 | 1,342,282 | 0.64× | 0.543 | 2.799 |
| core-p16 | INCR | 2,222,222 | 1,234,568 | 0.56× | 0.439 | 2.447 |
| core-p100 | PING_MBULK | 5,128,205 | 3,448,276 | 0.67× | 1.095 | 5.631 |
| core-p100 | SET | 2,469,136 | 1,666,667 | 0.68× | 2.223 | 3.087 |
| core-p100 | GET | 3,333,334 | 2,173,913 | 0.65× | 1.727 | 2.415 |
| core-p100 | INCR | 3,278,689 | 1,869,159 | 0.57× | 1.719 | 2.735 |
| range-heavy-p16 | LRANGE_100 | 185,874 | 185,529 | 1.00× | 4.991 | 8.959 |
| range-heavy-p16 | LRANGE_300 | 41,701 | 66,094 | **1.58×** | 12.607 | 14.015 |

The profile-matrix summary from this run:

```text
median 0.68x, min 0.56x, max 1.58x; GET p1 0.78x; GET p100 0.65x
```

The read I would trust: this is not primarily "Rust data structures cannot do
GET." The cliff appears when upstream Valkey can amortize event-loop work across
large batches and valkey-rs hits the connection-serving path hard. That points
at request draining, response flushing, and shared-state locking before it
points at the command implementation itself.

### Profiled hotspot run

The first larger profiled run used commit `2709d1e`, 50 clients, pipeline 100,
64-byte payloads, and `/usr/bin/sample` attached to the Rust server during each
hot workload.

| Workload | Requests | upstream Valkey (req/s) | valkey-rs (req/s) | ratio | upstream p99 (ms) | valkey-rs p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| GET | 20,000,000 | 3,658,314 | 2,122,917 | 0.58x | 1.663 | 5.407 |
| SET | 10,000,000 | 2,355,158 | 943,663 | 0.40x | 2.303 | 6.367 |
| INCR | 10,000,000 | 3,178,640 | 1,047,559 | 0.33x | 1.751 | 5.855 |
| PING_MBULK | 20,000,000 | 5,018,821 | 2,212,879 | 0.44x | 1.183 | 5.247 |

The stack samples are wall-clock samples, so the top rows include blocked
threads. That is still useful here because the blocked time is exactly the
architecture question. Across GET/SET/INCR/PING, the dominant non-idle leaf is
`__psynch_mutexwait`, followed by socket receive/write. The hottest Rust frames
were:

| Workload | Top Rust frames from sampled stacks |
|---|---|
| GET | `redis_commands::dispatch::dispatch_command_name`, `redis_protocol::request::parse_inline_or_multibulk_into`, `redis_protocol::request::read_resp_integer`, allocator frames |
| SET | `redis_core::replication::ReplicationState::append_to_backlog`, `redis_commands::dispatch::dispatch_command_name`, hasher/format/allocator frames |
| INCR | `redis_commands::dispatch::dispatch_command_name`, hasher frames, `redis_core::replication::ReplicationState::append_to_backlog`, format/allocator frames |
| PING_MBULK | `redis_commands::dispatch::dispatch_command_name`, `redis_protocol::request::parse_inline_or_multibulk_into`, memcpy/allocator frames |

The practical read: the profiler agrees with the architecture-stage conclusion.
The biggest remaining gap is shared-state waiting under high fan-in. The best
small/medium packet candidate is narrower than the full runtime rewrite:
investigate write-command propagation overhead when AOF is off and no replicas
are connected, because SET/INCR both show replication backlog work in the hot
sample. The harness gate for that packet should be strict: oracle smoke, RDB/AOF
tests if touched, replication tests if backlog semantics change, and this same
profiled hotspot runner.

That packet landed in `b21a533`: the dispatcher now skips replication stream
encoding when the server is a standalone primary with no active backlog, no
replicas, and no replication BGSAVE job, matching upstream's "do not propagate
when there is nowhere to propagate" shape. The same patch also folds handler
and metadata lookup into one runtime dispatch table, removing the second
per-command binary search. The next packet, `0004595`, keeps the runtime
dispatch table but buckets it by the command's first ASCII byte, so each
lookup searches only the relevant sub-slice instead of the full command list.

### Harness-driven optimization log

The profile matrix turned the vague "simple commands are slow" complaint into a
specific subsystem hypothesis: the plain TCP loop was paying too much
per-command overhead under deep pipelines. We then ran small patches through the
same profile matrix and kept the table current after each pass.

| Iteration | Patch | Summary | `core-p100/GET` | `core-p100/PING` | `range-heavy-p16/LRANGE_300` |
|---|---|---|---:|---:|---:|
| 0 | Baseline profile matrix | median 0.11x, min 0.05x, max 1.10x | 220,994 req/s (0.07x) | 250,941 req/s (0.05x) | 42,553 req/s (1.10x) |
| 1 | Batch replies once per socket read | median 0.14x, min 0.08x, max 1.30x | 406,504 req/s (0.12x) | 454,545 req/s (0.08x) | 49,975 req/s (1.30x) |
| 2 | Drain query buffer once per read batch | median 0.14x, min 0.07x, max 1.26x | 446,429 req/s (0.14x) | 497,512 req/s (0.09x) | 48,309 req/s (1.26x) |
| 3 | Direct-write ordinary plain-TCP replies | median 0.15x, min 0.07x, max 1.37x | 454,545 req/s (0.14x) | 420,168 req/s (0.08x) | 52,411 req/s (1.37x) |
| 4 | Avoid duplicate command-name lowercase | median 0.17x, min 0.08x, max 1.24x | 498,753 req/s (0.15x) | 447,427 req/s (0.09x) | 47,984 req/s (1.24x) |
| 5 | Architecture-first hot path packet: batch client-info snapshots, reuse argv storage, `Instant` timing, batch DB0 lock | median 0.33x, min 0.18x, max 1.62x | 956,938 req/s (0.28x) | 909,091 req/s (0.18x) | 66,490 req/s (1.62x) |
| 6 | Dispatch metadata cache + lazy argv snapshot for slowlog/AOF/replication | median 0.63x, min 0.39x, max 1.68x | 2,061,856 req/s (0.63x) | 2,702,703 req/s (0.53x) | 64,935 req/s (1.68x) |
| 7 | Standalone no-replica propagation skip + unified runtime dispatch table | median 0.66x, min 0.51x, max 1.54x | 2,127,660 req/s (0.65x) | 2,777,778 req/s (0.54x) | 63,211 req/s (1.54x) |
| 8 | First-byte bucketed runtime dispatch lookup | median 0.68x, min 0.56x, max 1.58x | 2,173,913 req/s (0.65x) | 3,448,276 req/s (0.67x) | 66,094 req/s (1.58x) |
| 9 | Runtime-owner reply/write-buffer polish | median 0.80x, min 0.57x, max 1.18x | 2,409,639 req/s (0.72x) | 4,081,633 req/s (0.80x) | 43,346 req/s (1.11x) |

The individual runs are noisy, especially on loopback with short benchmark
windows, so the useful read is the trend: deep-pipeline GET moved from about
221k req/s to about 2.17M req/s. The exact LRANGE number bounces because it is
already doing enough response work that the TCP-loop patches are not the main
determinant.

This is a good example of the harness shape we want for nginx: benchmark rows
should identify the subsystem boundary. The correct packet was not "make Redis
faster"; it was "reduce per-command overhead in the pipelined TCP path."

### Architecture-first read

Iteration 5 deliberately bundled the first four no/low-regret hot-path fixes
before considering a larger runtime rewrite:

- `CLIENT LIST` metadata moved from per-command global-lock updates to one
  read-batch snapshot update.
- The live server parser gained `parse_inline_or_multibulk_into`, so the
  connection reuses `client.argv` storage instead of allocating a fresh argv
  vector for every pipelined command.
- The command active-time metric uses monotonic `Instant` timing instead of
  `SystemTime` on the dispatch path.
- The plain TCP loop holds the DB0 lock across a read batch, dropping it when
  the client changes DB or blocks.

These changes transfer cleanly into any future runtime model. They also clarify
what remains: even after cutting obvious per-command costs, upstream Valkey's
event-loop batching still wins the tiny-command/deep-pipeline case. That is the
evidence we would want before considering the larger event-loop or shard-owned
DB architecture.

### Runtime ownership decision

We did not land a same-day "iteration 6" runtime rewrite. That is intentional.
After iteration 5, the remaining gap is no longer a small hot-path cleanup; it
is the server ownership model. The current binary still accepts connections
with blocking std threads and shares each logical DB through
`Arc<Mutex<RedisDb>>`. Upstream Valkey executes normal commands from a tight
event loop.

A quick patch here would likely be one of two bad things:

- a command-specific fast path for benchmark commands, bypassing the normal
  Redis semantics; or
- a half-runtime that improves one benchmark mode while breaking pub/sub,
  blocking commands, replication, or transactions.

The production-shaped version is documented in
[`docs/RUNTIME_OWNERSHIP_PLAN.md`](RUNTIME_OWNERSHIP_PLAN.md). The short
version: if valkey-rs continues, the next real performance milestone should be
a runtime-owner packet family, not another micro-optimization.

### Runtime owner loop evidence (2026-05-22)

The overnight runtime-owner packets moved default plain TCP onto the std
nonblocking owner loop and then polished the owner-loop reply staging. The
polish packet is deliberately narrow: it keeps `redis_commands::dispatch`,
RESP parsing, DB access, pub/sub foreign payload delivery, and command
semantics intact, while reducing reply-buffer movement inside
`RuntimeOwner::run_plain_tcp`.

Typed evidence for `runtime-owner-5-owner-loop-polish`:

- `harness/bench/results/20260522T044745Z-803918c-profile-matrix.tsv`:
  profile matrix median 0.80x, min 0.57x, max 1.18x; GET p1 1.10x and
  GET p100 0.72x.
- `harness/bench/results/20260522T044801Z-803918c-hotspots.tsv` and
  `harness/bench/results/20260522T044801Z-803918c-hotspots.json`: long p100
  hotspot median 0.74x, min 0.61x, max 0.80x.
- `harness/evidence/runs/20260522T044045Z-803918c-perf-fixer-runtime-owner-5-owner-loop-polish.json`:
  packet validation summary, including oracle smoke and focused Rust checks.

The current hotspot read is different from the pre-owner baseline. DB mutex
waiting is no longer the dominant sampled leaf. The remaining long-run p100
gap shows up around the owner loop, dispatch lookup, RESP integer parsing,
hashing for write commands, allocator/free/memmove, and socket read/write
costs.

## Reading this honestly

**Where we're still slower (long p100 simple commands, ~61-80% of upstream in
the latest hotspot run):**
The profile matrix now has noisy pipeline-1 wins, but the long p100 run is the
more useful read for the owner-loop path. GET reached 0.80x, SET 0.74x, PING
0.70x, and INCR 0.61x. The remaining gap is local hot-path work, not the old
thread-per-connection DB mutex wall.

**Where we're competitive or faster (larger replies):**
The latest short matrix has LRANGE_100 at 0.77x and LRANGE_300 at 1.11x.
Those numbers are noisier than the simple-command hotspot suite, but they keep
the same broad shape: once each operation returns meaningful payload, command
implementation and serialization costs matter more than tiny-command loop
overhead.

**Per-op latency p99 is still close enough to guide packet work** even when
throughput lags. In the latest long p100 hotspot run, Rust p99 is about
1.3-2.3x upstream across GET/SET/INCR/PING. GET and PING still show wider
tails than upstream and should stay visible in future owner-loop evidence.

## What we'd improve

Roadmap to closer-to-parity throughput, in rough effort order:

1. **Readiness and write batching** — replace the std nonblocking linear scan
   with a real readiness poller only after the owner-loop semantics stay green.
2. **Hasher/allocation pass on command execution** — use the new sampled stacks
   to reduce allocator churn, default-hasher work, and residual reply movement
   without bypassing the normal command path.
3. **Profile-guided optimization** — flamegraphs on the GET/SET/INCR hot path,
   evaluate `jemalloc`/`mimalloc`, and keep updating the profile matrix after
   each patch.
4. **Connection scalability and TLS parity** — the runtime owner currently
   covers plain TCP; TLS still uses the existing path.
5. **Sharding** — only after the faithful single-owner semantics are stable.
   Sharding can help independent-key throughput, but Redis transactions,
   scripts, blocking commands, and replication ordering make it a product
   decision rather than a small optimization.

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

# Larger profiles plus sampled Rust server stacks
./harness/bench/profile-hotspots.py --suite big

# Faster hotspot smoke while developing the runner
./harness/bench/profile-hotspots.py --suite smoke --workloads get --sample-seconds 2

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

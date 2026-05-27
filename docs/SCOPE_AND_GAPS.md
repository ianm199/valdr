# Scope and gaps — what valkey-rs does and does not do today

This document is the **honest counterpart** to the headline numbers in the
README. The scoped TCL, wire-diff, RDB, and benchmark numbers are real, but
they describe quality *within the scope we've built*. They do not describe how
much of Valkey we've built. That's what this page is for.

There are two separate goals:

- **Current claim:** accurately describe the scoped single-node behavior that
  is already backed by oracles.
- **Conformance target:** grow the denominator to the full upstream Valkey TCL
  suite, currently 4,299 `test` blocks across 245 `.tcl` files. See
  [`TCL_FULL_SUITE_GOAL_20260523.md`](TCL_FULL_SUITE_GOAL_20260523.md).

If you're evaluating whether valkey-rs can replace `redis-server` /
`valkey-server` in your stack, **read this before the README**.

## TL;DR

- **What works**: a single-node Redis/Valkey server speaking RESP2/RESP3
  over TCP and Unix sockets, with the standard data types (string, list,
  hash, set, zset, stream), transactions (MULTI/EXEC/WATCH), basic pub/sub,
  RDB persistence (bidirectional with upstream), EVAL/EVALSHA scripting, and
  native RedisJSON/RedisBloom-compatible command subsets. Existing Redis
  clients connect without changes.
- **What is not release-grade yet**: clustering, replica/Sentinel HA
  conformance, loadable C modules, in-process TLS, I/O threads, AOF parity,
  deeper ACL sweeps, and a long tail of newer Valkey 9.0 / Redis 7+ features
  (HGETDEL, SET IFEQ, LCS, HELLO availability-zone, import-mode, etc.).
- **Drop-in readiness**: it can replace a *single-node, basic-types,
  no-replication* Redis. It cannot replace a Redis Cluster, a
  replica/sentinel HA setup, or anything using modules.

## Where we are by the numbers

Three independent views of "how close are we" tell three different stories,
and you should look at all three.

### View 1 — Test pass rate (within surveyed scope)

| Oracle | Result |
|---|---|
| Upstream Tcl, historical scoped core survey | **~877 pass / ~73 fail** across the cleanup-wave core unit slice |
| Upstream Tcl, latest focused frontier telemetry | **266 counted pass / 0 counted fail**, 1 file without summary |
| Wire-diff RESP corpus | **23 / 23 byte-exact** |
| RDB bidirectional (we save → C loads; C saves → we load) | **378 / 378** |

This is the strong story. Within the slice we've decided to build, the
behavior matches upstream Valkey closely enough to satisfy upstream's own
test harness. Source: [`docs/CONFORMANCE.md`](CONFORMANCE.md).

### View 2 — Coverage of upstream's tests

Upstream Valkey has **4,299 individual `test "..." { }` blocks** across
245 `.tcl` files in this checkout. The old headline number was from a scoped
single-node survey, not the full suite. The focused frontier runner we just
use for packet work is smaller still.

| Slice | Tests | Share of upstream |
|---|---:|---:|
| Full upstream inventory | 4,299 | 100% |
| Historical scoped core survey | ~950 counted | ~22% |
| Pass within historical scoped survey | ~877 | ~20% |
| Latest focused frontier telemetry | 266 counted passes | ~6% |
| `tcl-survey-core` source inventory | 1,160 source test blocks | ~27% |

The remaining upstream surface is not supposed to disappear from the
accounting. Some areas need implementation work; some need multi-node or
specialized runners; some may become explicit product-decision exclusions. The
goal is to make those rows visible against the full-suite denominator instead
of only reporting the green scoped subset.

### View 3 — Code surface

| | LoC |
|---|---:|
| Upstream Valkey C source (`reference/valkey/src/`, `.c` + `.h`) | 180,348 |
| valkey-rs Rust source (`crates/`) | 82,732 |
| Ratio | **~46%** |

After excluding subsystems we haven't ported (modules, separate CLI,
clustering, sentinel, replication, ACL, RDB/AOF, TLS, streams, HLL, debug,
benchmark, fuzzer), the comparable scope is roughly 101k C vs 83k Rust —
**~82% of the equivalent C surface**, where the small reduction comes
from no headers in Rust, RAII replacing manual alloc/free, and pattern
matching replacing tag dispatch.

The headline-misleading lesson: the 180k → 83k ratio is *not* a 2× win
for Rust. It's mostly "we haven't built it yet."

## What subsystems are entirely missing

Numbers are upstream `src/` LoC and signal how much surface area remains.

| Subsystem | Upstream LoC | Status here | Impact if you need it |
|---|---:|---|---|
| **Clustering** (`cluster_*.c`) | ~11,200 | Out of scope by design (single-node only) | Cannot drop in for a Redis Cluster deployment. |
| **Modules API** (`module.c` + header) | ~18,200 | Out of scope by design | Loadable `.so` modules cannot load. Native JSON/Bloom subsets exist; RedisSearch/RedisGraph/etc. require separate native ports. |
| **Replication** (`replication.c`) | ~5,800 | Backbone exists; not release-gated as HA | No replica/Sentinel HA conformance. Don't use as primary or replica in an HA setup. |
| **Sentinel** (`sentinel.c`) | ~5,400 | Not ported (separate process upstream) | Sentinel-managed failover is not supported. |
| **ACL** (`acl.c`) | ~3,500 | Partial / not swept | If you rely on user-based authz beyond the legacy `requirepass`, expect gaps. |
| **AOF** (`aof.c`) | ~2,900 | Partial | RDB persistence works (378/378 bidirectional); AOF is not gated to the same standard. |
| **TLS** (`tls.c`) | ~2,000 | Deferred post-1.0 | Plain TCP only. Put a TLS terminator in front (haproxy, envoy, nginx) if you need it. |
| **I/O threads** (`io-threads`) | n/a | Deferred post-1.0 | Single-threaded I/O. Adequate for many workloads, not all. |
| **Streams consumer groups, newer edges** | ~4,000 (`t_stream.c`) | Partial — `stream-cgroups` is 36 pass / 28 fail | XADD/XREAD/XACK/XCLAIM work; newer consumer-group lifecycle edges fail. |
| **HyperLogLog** | ~2,100 | Commands present, focused frontier is green | PFADD/PFCOUNT/PFMERGE are covered by wire/focused TCL evidence; still not part of a full-suite claim. |
| **DEBUG command** | ~2,600 | Partial — Tcl tests using `needs:debug` are filtered out | Affects only Tcl-suite expansion, not application code. |
| **Valkey 9.0 / Redis 7.4+ additions** | scattered | Mostly missing | HGETDEL, SET IFEQ, LCS, HELLO availability-zone, MSETEX edge semantics — listed as deliberate gaps in CONFORMANCE.md. |

## Drop-in readiness — decision table

| You're running... | Can valkey-rs replace it today? |
|---|---|
| Single-node Redis as a cache (GET/SET/INCR + TTLs) | **Likely yes** — exercise your real workload first; this is the strongest slice. |
| Single-node with hashes / sets / zsets / lists | **Likely yes** — `unit/type/set` and `unit/type/zset` are full pass; `string`/`list`/`hash` have specific documented gaps. |
| Streams (XADD/XREAD/consumer groups) | **Maybe** — basic ops work; consumer-group lifecycle edges fail. Test your specific group workflows. |
| Transactions (MULTI/EXEC/WATCH) | **Mostly yes** — `unit/multi` is 12 pass / 5 fail. |
| Pub/Sub | **Mostly yes** — `unit/pubsub` is 22 pass / 6 fail. Sharded pub/sub (`SSUBSCRIBE`) supported on single-node only. |
| Scripting (EVAL / EVALSHA / Lua) | **Partial** — basic scripts work; `unit/scripting.tcl` not in the gated sweep. Don't rely on advanced Lua features. |
| RDB-based persistence | **Yes** — 378/378 bidirectional pass. |
| AOF-based persistence | **Not yet** — partial implementation, not gated. |
| Redis Cluster (multi-shard, slot-routed) | **No.** Out of scope by design. |
| Replication / Sentinel HA | **No.** Backbone exists; conformance not established. |
| RedisJSON / RedisBloom command subsets | **Maybe** — native Rust subsets exist; validate the exact commands and paths you use. |
| RedisSearch / RedisGraph / loadable C modules | **No.** Module ABI is not exposed. Won't happen pre-1.0. |
| TLS-terminated client connections | **Not in-process.** Use a TLS terminator. |

The honest framing: **valkey-rs today is roughly "Redis 2.6 + most of Redis
6/7's data-type surface, single-node, no extension API."** That covers a
surprising amount of real-world usage, but it is not the same thing as
"drop-in for Redis everywhere."

## What this means for the roadmap

The priority order is set by the harness's pilot strategy, not by any
single user's adoption blockers:

1. **Full upstream TCL-suite accounting** — move from focused packet runners
   to a dashboard whose denominator is all 4,299 upstream test blocks.
2. **Performance evidence and soak** (in progress — see the dashboard at
   `harness/bench/history/` and `docs/BENCHMARKS.md`). The surveyed default
   matrix is around parity, but broader workloads and long-duration soak still
   need publication-grade evidence.
3. **Replication conformance** — backbone exists; needs a multi-node
   integration sweep before it's claimable.
4. **AOF parity** to match the RDB story.
5. **Wider Tcl sweep** — next frontiers are `hyperloglog`, `scripting`,
   `slowlog`, `sort`, and meaningful `info` cases, then the rest of unit and
   integration coverage.
6. **TLS, I/O threads, ACL deeper, clustering, modules, Sentinel** — not in
   the current scoped product claim, but they belong in full-suite accounting
   as red, skipped-by-policy, or product-decision rows until resolved.

The wider harness may use Valkey as a pilot for other ports, but that does not
make Valkey's upstream-suite accounting optional. The conformance target for
this port is the full upstream TCL suite.

## How to verify all of this yourself

```bash
# 1. Wire-diff smoke (23/23)
bash harness/oracle/smoke.sh --skip-build

# 2. RDB bidirectional (378/378)
python3 harness/oracle/rdb-diff --direction=all

# 3. Run any single upstream Tcl unit file against our binary
bash harness/oracle/setup_tcl_runner.sh --skip-build
cd reference/valkey && VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/zset \
  --clients 1 --skip-leaks --tags "-needs:repl -needs:debug" \
  --durable --quiet

# 4. LoC breakdown (the numbers in this doc)
find reference/valkey/src -maxdepth 1 \( -name '*.c' -o -name '*.h' \) -print0 \
  | xargs -0 wc -l | tail -1
find crates -name '*.rs' -print0 | xargs -0 wc -l | tail -1

# 5. Individual upstream test count
grep -rE '^\s*test\s+\{|^\s*test\s+"' reference/valkey/tests --include='*.tcl' | wc -l
```

If any of those numbers disagree with this doc, this doc is wrong and
should be updated — not the other way around.

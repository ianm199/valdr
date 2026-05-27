# valkey-rs

A Redis/Valkey server written in Rust.

`valkey-rs` is a from-scratch Rust implementation of the Redis server
interface, targeting Valkey compatibility for single-node deployments. It runs
as a normal `redis-server` binary, speaks RESP2/RESP3, and works with existing
Redis clients without client-side changes.

No C bindings. No shim process. BSD-3-Clause, matching upstream Valkey.

[![License: BSD-3](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#status)
[![TCL: 877 pass](https://img.shields.io/badge/upstream%20TCL-877%20pass-brightgreen.svg)](docs/CONFORMANCE.md)

## Compatibility

The current compatibility picture:

```text
Scoped TCL survey: ~877 pass / ~73 known fail
RESP wire diff:   23 / 23 byte-exact
RDB load/save:    378 / 378 bidirectional
```

[Run the same comparison](docs/CONFORMANCE.md#reproducing), or read the full
[conformance matrix](docs/CONFORMANCE.md).

> **Thinking about adopting?** The TCL number above is a scoped single-node
> survey, not coverage of all upstream Valkey behavior.
> Before you wire valkey-rs into anything load-bearing, read
> [**Scope and gaps**](docs/SCOPE_AND_GAPS.md) — it spells out what is
> and isn't here today (no clustering, no modules, no in-process TLS,
> replication backbone not gated, …) and gives a decision table for
> common deployment shapes.

Other useful numbers:

| Check | Result |
|---|---:|
| Source size | **~80k Rust LoC** vs upstream's ~187k C LoC |
| `unsafe` blocks | **14** in first-party Rust: OS/process control, AArch64 CPU-timer reads, and the FUNCTION/FCALL Lua callback bridge |

What works today:

- Strings, lists, hashes, sets, sorted sets, streams, HyperLogLog, bitmaps, geo.
- Pub/sub, transactions, Lua scripting, ACL, multi-DB, eviction.
- RDB v11 persistence, gated by bidirectional load/save tests against Valkey.
- AOF and replication basics, including PSYNC and WAIT, with alpha-level
  coverage rather than production HA conformance.
- Native RedisJSON-compatible `JSON.*` commands.
- Native RedisBloom-compatible `BF.*` commands.

Not done yet:

- Cluster mode.
- Loadable C-ABI modules.
- In-process TLS termination (rustls scaffold present but not wired to the runtime owner; TLS listener requests are currently refused).
- Production-grade AOF/replication/Sentinel/HA conformance.
- A handful of Valkey 9.0 commands and edge cases.
- Sustained production soak and performance tuning.

## Run it

```bash
git clone https://github.com/ianm199/valkey-rs
cd valkey-rs
cargo build --release
./target/release/redis-server --port 6379 --bind 127.0.0.1
```

Then point an ordinary Redis client at it:

```python
import redis

r = redis.Redis(host="localhost", port=6379)
r.set("hello", "world")
print(r.get("hello"))
```

```javascript
import Redis from "ioredis";

const r = new Redis(6379);
await r.set("hello", "world");
console.log(await r.get("hello"));
```

## Docker

```bash
docker pull ghcr.io/ianm199/valkey-rs:alpha &&
docker run --rm -p 6379:6379 -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
```

One-copy smoke test with only Docker installed:

```bash
docker network create valkey-rs-try >/dev/null 2>&1 || true
docker rm -f valkey-rs-try >/dev/null 2>&1 || true
docker pull ghcr.io/ianm199/valkey-rs:alpha
docker run -d --name valkey-rs-try --network valkey-rs-try -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try PING
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try SET hello world
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try GET hello
docker rm -f valkey-rs-try
docker network rm valkey-rs-try
```

Or build locally:

```bash
docker build -t valkey-rs:local .
docker run --rm -p 6379:6379 -v valkey-rs-data:/data valkey-rs:local
```

See [Docker](docs/DOCKER.md) for tags, Compose, persistence, the container
smoke test, and a Docker-only benchmark wrapper.

```bash
IMAGE=ghcr.io/ianm199/valkey-rs:alpha PIPELINE=100 TESTS=get,set,incr,ping_mbulk \
  bash harness/docker/bench.sh
```

## Status

Alpha. The server is functional and verified against independent compatibility
oracles on every commit, but it is not a drop-in replacement for every Valkey
deployment yet.

The intended near-term target is: non-clustered, single-node Redis/Valkey
workloads where the safety and auditability of a Rust implementation matter
more than perfect coverage of every upstream extension.

## Performance

Current release-candidate telemetry vs upstream Valkey on an Apple M3 Max.
Both servers are run sequentially on the same host with 50 clients and 64-byte
payloads. These are benchmark signals, not production soak claims.

| Benchmark | Workload | upstream Valkey | valkey-rs | ratio |
|---|---|---:|---:|---:|
| Default suite, P=1 | GET | 194,932 req/s | 194,553 req/s | 1.00× |
| Default suite, P=1 | SET | 190,840 req/s | 211,417 req/s | 1.11× |
| Default suite, P=1 | MSET (10 keys) | 185,874 req/s | 207,900 req/s | 1.12× |
| Default suite, P=1 | LRANGE_300 | 43,611 req/s | 45,956 req/s | 1.05× |
| JSON document mix, P=1 | 80% GET / 15% SET / 5% MGET | 36,629 req/s | 36,163 req/s | 0.99× |
| Pipeline smoke, P=100 | GET | 3.51M req/s | 4.55M req/s | 1.30× |
| Pipeline smoke, P=100 | SET | 2.38M req/s | 3.70M req/s | 1.56× |

The representative non-function default suite is 21/21 pass with median
`1.060x` vs upstream and weakest row `0.986x`. The JSON document mix is 3/3
pass with median `0.994x`. The pipeline smoke is 12/12 pass with median
`1.133x`; it is useful for catching batching regressions, but loopback
high-pipeline numbers can be noisy.

The optimization log moved deep-pipeline GET from about 221k req/s in the
first alpha baseline to 4.55M req/s in the latest smoke run. See
[`docs/BENCHMARKS.md`][bench] and
[`docs/RUST_PERFORMANCE_IMPROVEMENT_PLAYBOOK_20260526.md`](docs/RUST_PERFORMANCE_IMPROVEMENT_PLAYBOOK_20260526.md)
for methodology, artifact paths, and the optimization roadmap.

[bench]: docs/BENCHMARKS.md
[runtime]: docs/RUNTIME_OWNERSHIP_PLAN.md

## Supported surface

| Surface | Status |
|---|---|
| RESP2 / RESP3 wire protocol | Full, with one minor HELLO edge remaining |
| Core data types | Strings, lists, hashes, sets, sorted sets |
| Streams | Entries, consumer groups, blocking variants |
| Scripting | `EVAL` / `EVALSHA` through Lua 5.1 |
| Persistence | RDB v11 load/save is gated; AOF exists but is alpha/not gated to the same standard |
| Replication | PSYNC, full sync, WAIT basics; no production HA conformance yet |
| Security | ACL, AUTH (TLS scaffold present via rustls, not yet wired to the runtime) |
| Memory policies | 8 maxmemory eviction policies |
| Modules | Native RedisJSON + RedisBloom command subsets |
| Cluster | Not implemented |
| Loadable C modules | Not implemented |

## Test oracles

The project is gated by three external compatibility checks:

```bash
bash scripts/setup-reference.sh
cargo build -p redis-server
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
```

The TCL suite uses upstream Valkey's own test harness against our binary:

```bash
bash harness/oracle/run-single-node-tcl-suite.sh
```

For focused iteration, use the same wrapper with `--skip-build --files
unit/maxmemory`. See [TCL test suite runbook](docs/TCL_TEST_SUITE_RUNBOOK.md)
for the official profile, port rules, and contained safe-survey variant.

## Safety

Most crates are safe Rust. The first-party crates currently contain 14
`unsafe` blocks, tracked by [`harness/unsafe-budgets.toml`](harness/unsafe-budgets.toml).
The parser, command dispatch table, and Rust data-structure ports remain
unsafe-free.

| Crate | `unsafe` blocks | Reason |
|---|---:|---|
| `redis-types` | 0 | Shared byte strings, RESP values, and errors |
| `redis-protocol` | 0 | RESP parsing and serialization |
| `redis-ds` | 0 | Rust-native data structures |
| `redis-core` | 2 | AArch64 `mrs` reads of CPU timer registers (`cntvct_el0`, `cntfrq_el0`) |
| `redis-commands` | 7 | `fork`/`_exit` BGSAVE paths, shutdown `kill`/`waitpid`/`_exit`, and the cached `mlua` FUNCTION/FCALL active-context bridge |
| `redis-server` | 5 | `waitpid` child reapers, SIGTERM/SIGINT `signal` install, and immediate shutdown `_exit` calls |

Most current unsafe is boundary work around primitives the Rust standard
library does not expose directly: Unix process control, signal handling, and
AArch64 timer registers. The one application-level exception is the cached
`mlua` FUNCTION/FCALL active-context bridge, which should be revisited when
FUNCTION performance and correctness work resumes.

The near-term safety plan is to keep new unsafe out of hot-path protocol,
dispatch, and data-structure code; centralize Unix process-control calls behind
small audited wrappers; and add or tighten explicit `SAFETY` comments for each
remaining block. The unsafe-budget hook fails changes that exceed the crate
ceilings.

## Architecture

```text
crates/
├── redis-types/      shared values, RESP values, errors
├── redis-protocol/   RESP2 / RESP3 parser and serializer
├── redis-ds/         listpack, intset, skiplist, compact structures
├── redis-core/       server state, DBs, objects, ACL, eviction, blocking
├── redis-commands/   command handlers, RDB/AOF, Lua bridge, replication
└── redis-server/     binary, TCP/TLS accept loop, config, dispatch
```

## How it was built

This port was produced with an AI-assisted porting harness: bounded agents,
small commits, safety hooks, and compatibility oracles after each change. The
important claim is not that AI wrote code; it is that the port is continuously
checked against the real upstream behavior.

## Roadmap

To reach upstream-suite parity:

- HGETDEL / HEXPIRE family.
- LCS.
- SET ... IFEQ.
- HELLO availability-zone / no-protover variants.
- Remaining stream consumer-group wakeup edges.
- Per-DB blocked-key indexing.
- Sustained-load tuning and broader public benchmark coverage.

Longer term:

- Cluster mode.
- More native replacements for popular loadable modules.

## License

[BSD-3-Clause](LICENSE), matching upstream Valkey.

## Acknowledgments

- The [Valkey project](https://github.com/valkey-io/valkey) for the reference
  implementation and upstream test suite.
- [Redis Ltd.](https://redis.com) for the original Redis project that Valkey was
  forked from.

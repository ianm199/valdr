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
TCL unit survey: [███████████████████▋] 877 / 896 (97.9%)
RESP wire diff:  [████████████████████] 21 / 21
RDB load/save:   [████████████████████] 378 / 378
```

[Run the same comparison](docs/CONFORMANCE.md#reproducing), or read the full
[conformance matrix](docs/CONFORMANCE.md).

Other useful numbers:

| Check | Result |
|---|---:|
| Source size | **~80k Rust LoC** vs upstream's ~187k C LoC |
| `unsafe` blocks | **5**, all `fork(2)` / `waitpid(2)` wrappers |

What works today:

- Strings, lists, hashes, sets, sorted sets, streams, HyperLogLog, bitmaps, geo.
- Pub/sub, transactions, Lua scripting, ACL, multi-DB, eviction, TLS.
- Persistence through RDB v11 and AOF.
- Replication basics, including PSYNC and WAIT.
- Native RedisJSON-compatible `JSON.*` commands.
- Native RedisBloom-compatible `BF.*` commands.

Not done yet:

- Cluster mode.
- Loadable C-ABI modules.
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
docker run --rm -p 6379:6379 -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
```

Or build locally:

```bash
docker build -t valkey-rs:local .
docker run --rm -p 6379:6379 -v valkey-rs-data:/data valkey-rs:local
```

See [Docker](docs/DOCKER.md) for tags, Compose, persistence, and the container
smoke test.

## Status

Alpha. The server is functional and verified against independent compatibility
oracles on every commit, but it is not a drop-in replacement for every Valkey
deployment yet.

The intended near-term target is: non-clustered, single-node Redis/Valkey
workloads where the safety and auditability of a Rust implementation matter
more than perfect coverage of every upstream extension.

## Performance

First-baseline numbers vs upstream Valkey on an Apple M3 Max (50 clients,
pipeline 100, 64-byte payload):

| Command | upstream Valkey | valkey-rs | ratio |
|---|---:|---:|---:|
| SET / GET / INCR (simple ops)  | ~2.5–3.5M req/s | ~190–225k req/s | ~6–9% |
| LRANGE_100 (100-elem range)    | 111k req/s      | 106k req/s      | **95%** |
| LRANGE_300 (300-elem range)    | 36.7k req/s     | 52.4k req/s     | **143%** ⚡ |

Per-op latency p99 is mostly competitive (within 2× of upstream) even
when throughput is not. The gap on simple ops is mostly the connection-serving
path and shared-state ownership model; the cost amortizes away on commands
that do real work.

A newer profile-matrix benchmark makes the architecture cliff clearer and gives
the harness a performance objective it can optimize. The first tuning passes
focused on the plain-TCP loop and then on the per-command hot path: batch
replies, drain the query buffer once per read batch, direct-write ordinary
request/reply traffic, batch client-info snapshots, reuse argv storage, use
monotonic timing, hold the DB0 lock across safe read batches, cache generated
command metadata, and avoid argv snapshots unless slowlog/AOF/replication need
them.

| Profile | Command | upstream Valkey | valkey-rs | ratio |
|---|---|---:|---:|---:|
| 50 clients, pipeline 1 | GET | 188k req/s | 138k req/s | 0.73× |
| 50 clients, pipeline 16 | GET | 2.08M req/s | 1.30M req/s | 0.62× |
| 50 clients, pipeline 100 | GET | 3.28M req/s | 2.06M req/s | 0.63× |
| 50 clients, pipeline 16 | LRANGE_300 | 38.6k req/s | 64.9k req/s | **1.68×** |

The optimization log moved deep-pipeline GET from about 221k req/s to about
2.06M req/s. See [`docs/BENCHMARKS.md`][bench] for full methodology, each
iteration's table, and the optimization roadmap.

The remaining throughput gap is architectural. The current server is still a
blocking thread-per-connection runtime sharing DB state through
`Arc<Mutex<RedisDb>>`; upstream Valkey's tiny-command pipeline advantage comes
from event-loop ownership and batching. The proposed next performance milestone
is documented in [`docs/RUNTIME_OWNERSHIP_PLAN.md`][runtime].

[bench]: docs/BENCHMARKS.md
[runtime]: docs/RUNTIME_OWNERSHIP_PLAN.md

## Supported surface

| Surface | Status |
|---|---|
| RESP2 / RESP3 wire protocol | Full, with one minor HELLO edge remaining |
| Core data types | Strings, lists, hashes, sets, sorted sets |
| Streams | Entries, consumer groups, blocking variants |
| Scripting | `EVAL` / `EVALSHA` through Lua 5.1 |
| Persistence | RDB v11 load/save, AOF |
| Replication | PSYNC, full sync, WAIT |
| Security | ACL, AUTH, TLS through rustls |
| Memory policies | 8 maxmemory eviction policies |
| Modules | Native RedisJSON + RedisBloom command subsets |
| Cluster | Not implemented |
| Loadable C modules | Not implemented |

## Test oracles

The project is gated by three external compatibility checks:

```bash
bash scripts/setup-reference.sh
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
```

The TCL suite uses upstream Valkey's own test harness against our binary:

```bash
bash harness/oracle/setup_tcl_runner.sh --skip-build
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/zset \
  --clients 1 --skip-leaks --tags "-needs:repl -needs:debug" \
  --durable --quiet
```

## Safety

Most crates are safe Rust. The only `unsafe` blocks are in process-management
code that wraps Unix `fork(2)` and `waitpid(2)` for background save / rewrite
flows:

| Crate | `unsafe` blocks | Reason |
|---|---:|---|
| `redis-types` | 0 | |
| `redis-protocol` | 0 | |
| `redis-ds` | 0 | |
| `redis-core` | 0 | |
| `redis-commands` | 3 | `fork` / `_exit` for BGSAVE, BGREWRITEAOF, full sync |
| `redis-server` | 2 | `waitpid` child reapers |

Each block has a `// SAFETY:` invariant. The hook
`harness/unsafe-budget.sh` fails changes that exceed the crate budgets.

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
- Performance benchmarks and sustained-load tuning.

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

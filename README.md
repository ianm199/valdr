# valkey-rs

> A Valkey-compatible cache/store, reimplemented in **safe Rust** — drop-in for
> non-clustered single-node deployments, ~98% of the upstream Valkey TCL test
> suite passing.

`valkey-rs` speaks the RESP2 and RESP3 wire protocols byte-for-byte against
existing Redis/Valkey clients. Your unmodified `redis-py`, `ioredis`,
`go-redis`, `jedis`, `redis-rs`, or any other RESP client will connect, run
the standard command set, and observe the same replies they'd see from
upstream Valkey 7.2.4.

[![License: BSD-3](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#status)
[![Unsafe: documented](https://img.shields.io/badge/unsafe-2_blocks_documented-green.svg)](#safety)
[![TCL: ~98%](https://img.shields.io/badge/upstream%20TCL-~98%25-brightgreen.svg)](docs/CONFORMANCE.md)

## Status

**Alpha.** Suitable for non-production workloads, evaluation, and
single-node Valkey replacement in development environments. Not yet
benchmarked or run under sustained production load.

What works today:

- **All major data types** — strings, lists, hashes, sets, sorted sets,
  streams (incl. consumer groups), HyperLogLog, bitmap, geo
- **Persistence** — RDB v11 (bidirectional with upstream), AOF write +
  replay
- **Replication** — primary/replica state machine with PSYNC, full-sync
  RDB transfer, REPLCONF/WAIT, READONLY enforcement
- **Eviction** — all 8 maxmemory policies (noeviction / allkeys-lru /
  allkeys-lfu / allkeys-random / volatile-lru / volatile-lfu /
  volatile-random / volatile-ttl)
- **Scripting** — EVAL / EVALSHA via embedded Lua 5.1
- **Multi-user ACL** — SHA-256 password hashing, command categories
- **Transactions** — MULTI / EXEC / DISCARD / WATCH
- **Pub/sub** — channel + pattern subscribers
- **Blocking ops** — BLPOP / BRPOP / BLMOVE / BLMPOP / BZPOPMIN / BZPOPMAX /
  BZMPOP / XREAD BLOCK
- **Multi-DB** — `SELECT 0..15`, MOVE, COPY DB, SWAPDB, INFO keyspace
- **TLS** — rustls
- **Native module commands** — RedisJSON-style `JSON.*` (17 commands,
  JSONPath subset) and RedisBloom-style `BF.*` (7 commands)

See [`docs/CONFORMANCE.md`](docs/CONFORMANCE.md) for the detailed
capability matrix and known gaps.

## Quick start

### From source

```bash
git clone https://github.com/ianm199/valkey-rs
cd valkey-rs
cargo build --release
./target/release/redis-server --port 6379 --bind 127.0.0.1
```

### Running the test suite (optional)

The wire-diff and TCL oracles need a built copy of upstream Valkey for
comparison. A helper script clones the pinned commit and builds it:

```bash
bash scripts/setup-reference.sh           # one-time, ~1 min
bash harness/oracle/smoke.sh --skip-build # 21/21 PASS
```

In a separate terminal, point any RESP2/RESP3 client at it:

```bash
redis-cli -p 6379 PING                          # PONG
redis-cli -p 6379 SET hello world               # OK
redis-cli -p 6379 GET hello                     # "world"
```

### With your existing client library

```python
# Python (redis-py)
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('hello', 'world')
print(r.get('hello'))
```

```javascript
// Node.js (ioredis)
const Redis = require('ioredis');
const r = new Redis(6379, '127.0.0.1');
await r.set('hello', 'world');
console.log(await r.get('hello'));
```

## Why

Most "Redis-in-Rust" projects are clients. This is a complete
server-side reimplementation:

- **Memory safety.** Zero `unsafe` outside two documented blocks (fork
  + waitpid in the persistence path). The entire RESP protocol, every
  command handler, every data structure, the replication state machine
  — all in safe Rust. See [Safety](#safety).
- **BSD-3-Clause licensing.** Tracks upstream Valkey's licensing (not
  Redis 8+ AGPL/SSPL/RSAL) so the port can be freely redistributed.
- **Single-node simplicity.** No cluster, no sharding. Drop it in
  where you'd put a single-node Valkey today.
- **Compatibility-first.** Behavior is validated against upstream's
  own test suite, not a hand-rolled compat checklist.

## Conformance

This port is verified against **three independent oracles** at every
commit:

1. **Wire-diff oracle** — 21 hand-curated RESP corpus scripts compared
   byte-for-byte against real Valkey. 21/21 PASS.
2. **RDB bidirectional oracle** — saves a corpus with us, loads with
   upstream; saves with upstream, loads with us. 378/378 PASS.
3. **Upstream Valkey TCL test suite** — the same test infrastructure
   Valkey uses internally, pointed at our binary. ~98% pass rate on
   the surveyed unit files. See [CONFORMANCE.md](docs/CONFORMANCE.md)
   for the per-file breakdown.

## How this was built

The translation work was done by an AI-driven porting harness that
combines bounded subagent roles (translator / compiler-fixer / test-fixer
/ verifier), pre-computed analyses (type vocabulary, macro mappings,
header dependency graph), enforcement hooks (unsafe budget, forbidden
patterns, vocabulary registry), and a three-oracle verification stack.
The harness is a sibling project at [`../port-harness/`](../port-harness)
and is intended to be the durable artifact — `valkey-rs` is one of two
proofs (alongside `lua-rs-port`) that the methodology works on
non-trivial real-world C codebases.

The port spans **109 atomic commits** on the main branch, each one
representing a single agent invocation with the same hooks gating
every change.

## Safety

`unsafe` accounting (`harness/unsafe-budgets.toml`):

| Crate | `unsafe` blocks | Reason |
|---|---|---|
| `redis-types` | 0 | |
| `redis-protocol` | 0 | |
| `redis-core` | 0 | |
| `redis-commands` | 1 | `libc::fork` + `_exit` in `persist.rs::bgsave_fork` |
| `redis-server` | 1 | `libc::waitpid` in `main.rs` BGSAVE reaper |
| `redis-ds` | 0 | |

Every `unsafe` block has a `// SAFETY:` comment documenting the
invariant. Both are wrapping POSIX `fork(2)` semantics that have no
safe equivalent in `std`.

The chassis enforces the budget with a `Stop`-event hook
(`harness/unsafe-budget.sh`) that fails the commit if any crate exceeds
its declared ceiling.

## Architecture overview

```
crates/
├── redis-types/          ByteString, RespValue, RedisError — no I/O
├── redis-protocol/       RESP2 + RESP3 frame parser + serializer
├── redis-ds/             ListPack, IntSet, SkipList — bulk-data structures
├── redis-core/           RedisServer, Client, RedisDb, RedisObject, GC, eviction,
│                         replication, ACL, BlockedKeysIndex, GlobalDatabases
├── redis-commands/       all command handlers, AOF writer, RDB save/load,
│                         BGSAVE fork wrapper, EVAL/Lua bridge
└── redis-server/         the binary — accept loop, TLS, command dispatch
```

## Project layout

```
valkey-rs/
├── README.md             this file
├── docs/
│   ├── CONFORMANCE.md    capability matrix, TCL pass rates, gaps
│   ├── DOCKER.md         containerization (planned)
│   ├── ADR_001_LUA_RUNTIME.md
│   └── ...
├── crates/               see Architecture above
├── reference/valkey/     pinned upstream source (BSD-3) for oracle
├── harness/
│   ├── oracle/           wire-diff, rdb-diff, TCL runner setup
│   ├── corpus/           21 wire-diff scripts (smoke) + edge-case regression
│   ├── rdb-corpus/       7 RDB serialization scripts (bidirectional)
│   └── unsafe-budgets.toml
└── PORTING.md            agent-facing translation rules (the "playbook")
```

## Roadmap to 1.0

To reach **100% non-clustered drop-in**:

- HGETDEL / HEXPIRE family (Valkey 9.0 hash extensions)
- LCS command (longest-common-subsequence)
- SET … IFEQ conditional (Valkey 9.0)
- HELLO with no protover (Valkey 9.0 negotiation)
- `unit/type/stream-cgroups` remaining XREADGROUP edge cases (~28 tests)
- Performance benchmarking + tuning under sustained load
- Per-DB BlockedKeysIndex (currently one BLPOP-watcher key namespace
  across DBs — rare bug, deferred)
- CI workflow

Beyond 1.0:

- Cluster mode (deliberate gap today; not on the alpha roadmap)
- Loadable C-ABI modules (we provide native equivalents for RedisJSON +
  RedisBloom only)

## License

BSD-3-Clause, matching upstream Valkey. See [LICENSE](LICENSE).

## Acknowledgments

- The [Valkey project](https://github.com/valkey-io/valkey) for the
  reference implementation and the TCL test suite that makes this
  port verifiable.
- [Redis Ltd.](https://redis.com) for the original Redis project, on
  whose source Valkey is based (BSD-3 era through Redis 7.2.4).

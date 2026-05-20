# valkey-rs

`valkey-rs` is a Valkey-compatible cache/store written in Rust. The goal
is simple: full upstream-test-suite compatibility, shipped as a memory-safe
single-node deployment.

| | upstream Valkey | valkey-rs |
|---|---|---|
| TCL tests on surveyed unit files | 896 / 896 | **877 / 896 (97.9%)** |
| RESP wire-diff oracle (byte-exact) | n/a — reference | **21 / 21** |
| RDB bidirectional oracle | n/a — reference | **378 / 378** |
| Source size | ~187,000 lines of C | **~80,000 lines of Rust** |
| `unsafe` blocks | n/a | **5** (all `fork(2)`/`waitpid(2)`) |
| License | BSD-3-Clause | BSD-3-Clause |

`valkey-rs` is not a drop-in upstream-Valkey replacement *yet* — clustering
and a handful of Valkey 9.0 extensions are deliberately unimplemented (see
[Compatibility](#compatibility) and [`docs/CONFORMANCE.md`](docs/CONFORMANCE.md)).
For non-clustered single-node deployments, unmodified `redis-py`,
`ioredis`, `go-redis`, `jedis`, and `redis-rs` clients connect and behave
the same as they would against Valkey 7.2.4.

[![License: BSD-3](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#status)
[![TCL: 877/896](https://img.shields.io/badge/upstream%20TCL-877%2F896-brightgreen.svg)](docs/CONFORMANCE.md)

[Get started](#get-started) • [Conformance](docs/CONFORMANCE.md) • [GitHub](https://github.com/ianm199/valkey-rs)

## Highlights

- **From-scratch port.** No bindings, no shim, no C dependency. ~80,000
  lines of Rust against ~187,000 lines of upstream Valkey C.
- **Wire-compatible.** RESP2 and RESP3 verified byte-for-byte against
  upstream Valkey on every commit.
- **All major data types.** Strings, lists, hashes, sets, sorted sets,
  streams (incl. consumer groups + blocking variants), HyperLogLog,
  bitmap, geo. Plus pub/sub, transactions, scripting (Lua 5.1), ACL,
  replication (PSYNC + WAIT), persistence (RDB v11 + AOF), TLS, multi-DB.
- **Tested against the upstream's own tests.** The same TCL test
  infrastructure Valkey uses internally, pointed at our binary. 877
  passing of 896 tests across the 13 surveyed unit files.
- **5 `unsafe` blocks total** in the entire codebase, all wrapping POSIX
  `fork(2)` / `waitpid(2)` in the background-save path. Every block
  carries a `// SAFETY:` invariant. A pre-commit hook fails any change
  that exceeds the per-crate budget.

## Status

**Alpha.** Functional, verified against three independent oracles on
every commit, but not yet performance-benchmarked or run under sustained
production load. Use for development, evaluation, and CI-style workloads.

## Compatibility

| Surface | Status |
|---|---|
| RESP2 / RESP3 wire protocol | ✅ full (one minor HELLO edge missing) |
| Strings | ✅ |
| Lists (incl. BLPOP / BLMPOP / BLMOVE) | ✅ |
| Hashes | ✅ — HGETDEL family deferred (Valkey 9.0) |
| Sets | ✅ |
| Sorted sets (incl. BZPOPMIN / BZMPOP) | ✅ |
| Streams (incl. consumer groups + XREADGROUP BLOCK) | ✅ |
| HyperLogLog • Bitmap • Geo | ✅ |
| Pub/sub (channels • patterns • sharded) | ✅ |
| Transactions (MULTI / EXEC / WATCH) | ✅ |
| Scripting (EVAL / EVALSHA via Lua 5.1) | ✅ |
| ACL (multi-user, SHA-256 hashes) | ✅ |
| Persistence (RDB v11 bidirectional + AOF) | ✅ |
| Replication (PSYNC + WAIT) | ✅ |
| Eviction (8 maxmemory policies) | ✅ |
| TLS (rustls) | ✅ |
| Multi-DB (`SELECT 0..15`, MOVE / COPY / SWAPDB) | ✅ |
| Native RedisJSON-compat (`JSON.*`, 17 cmds) | ✅ |
| Native RedisBloom-compat (`BF.*`, 7 cmds) | ✅ |
| Clustering | ❌ single-node only (deliberate) |
| Loadable C-ABI modules | ❌ — native impls only |
| Valkey 9.0 extensions (LCS, SET IFEQ, …) | ❌ — roadmap |

Per-unit-file TCL pass tally + full command coverage in
[`docs/CONFORMANCE.md`](docs/CONFORMANCE.md).

## Get started

```bash
git clone https://github.com/ianm199/valkey-rs
cd valkey-rs
cargo build --release
./target/release/redis-server --port 6379 --bind 127.0.0.1
```

Point any unmodified RESP client at port 6379:

```python
# Python (redis-py)
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('hello', 'world')
print(r.get('hello'))
```

```javascript
// Node (ioredis)
const Redis = require('ioredis');
const r = new Redis(6379);
await r.set('hello', 'world');
console.log(await r.get('hello'));
```

### Running the tests

The wire-diff and TCL oracles need a built copy of upstream Valkey for
comparison. A helper script clones the pinned commit and builds it:

```bash
bash scripts/setup-reference.sh             # one-time, ~1 min
bash harness/oracle/smoke.sh --skip-build   # 21/21 wire-diff PASS
```

For the upstream TCL suite against your `valkey-rs` build:

```bash
bash harness/oracle/setup_tcl_runner.sh --skip-build
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/zset \
  --clients 1 --skip-leaks --tags "-needs:repl -needs:debug" \
  --durable --quiet
```

## Safety

| Crate | `unsafe` blocks | Reason |
|---|---|---|
| `redis-types` | 0 | |
| `redis-protocol` | 0 | |
| `redis-ds` | 0 | |
| `redis-core` | 0 | |
| `redis-commands` | 3 | `libc::fork` + `_exit` for BGSAVE / BGREWRITEAOF / replication full-sync |
| `redis-server` | 2 | `libc::waitpid` for BGSAVE + AOF-rewrite child reapers |

Every block carries a documented `// SAFETY:` invariant. A pre-commit
hook (`harness/unsafe-budget.sh`) fails any change exceeding the per-
crate ceiling.

## Architecture

```
crates/
├── redis-types/          ByteString, RespValue, RedisError — no I/O
├── redis-protocol/       RESP2 + RESP3 frame parser + serializer
├── redis-ds/             ListPack, IntSet, SkipList — bulk-data structures
├── redis-core/           RedisServer, Client, RedisDb, RedisObject, GC,
│                         eviction, replication, ACL, BlockedKeysIndex,
│                         GlobalDatabases
├── redis-commands/       command handlers, AOF writer, RDB save/load,
│                         BGSAVE fork wrapper, EVAL/Lua bridge
└── redis-server/         the binary — accept loop, TLS, command dispatch
```

## How this was built

The translation was done by an AI-driven porting harness — bounded
subagent roles (translator / compiler-fixer / test-fixer / verifier),
per-commit hooks enforcing safety budgets and forbidden patterns, and
three independent oracles gating every change. The harness lives in a
sibling project at [`../port-harness/`](../port-harness) and is intended
to be the durable artifact; `valkey-rs` is one of two proofs (alongside
[`lua-rs-port`](../lua-rs-port), a port of PUC-Rio Lua 5.4 to safe Rust)
that the methodology works on non-trivial real-world C codebases.

The port currently spans **111 atomic commits** on main, each one a
single agent invocation under the same hook gating.

## Roadmap

To reach **full upstream-test-suite parity**:

- HGETDEL / HEXPIRE family (Valkey 9.0 hash extensions)
- LCS (longest-common-subsequence)
- SET … IFEQ conditional (Valkey 9.0)
- HELLO availability-zone / no-protover variants
- `unit/type/stream-cgroups` remaining XREADGROUP edge cases
- Per-DB BlockedKeysIndex (currently keyed by `RedisString` only)
- Performance benchmarking and tuning under sustained load

Beyond parity:

- Cluster mode (not on the alpha roadmap)
- Loadable C-ABI modules (we provide native impls of RedisJSON +
  RedisBloom only)

## License

[BSD-3-Clause](LICENSE), matching upstream Valkey.

## Acknowledgments

- The [Valkey project](https://github.com/valkey-io/valkey) for the
  reference implementation and the TCL test suite that makes this port
  verifiable.
- [Redis Ltd.](https://redis.com) for the original Redis project, on
  whose source Valkey is based (BSD-3 era through Redis 7.2.4).

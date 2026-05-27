# redis-core

Core single-node server state for the Valkey port — the live data model every
command reads and mutates. Sits above `redis-types`, `redis-protocol`, and
`redis-ds`; consumed by `redis-commands`.

## Owns (canonical types — `harness/type-vocabulary.tsv`, mode=enforce)
`RedisServer`, `RedisDb`, `RedisObject` (+ `ObjectKind`, `StringEncoding`,
`ListEncoding`, `SetEncoding`, `ZSetEncoding`, `HashEncoding`, `ObjectType`),
`Client` (+ `MultiState`/`MultiCmd`/`WatchedKey`), `CommandContext`.

## Module map (66 files, by subsystem)
- keyspace & values   db, databases, object, entry, expire, eviction, lrulfu,
                      lru_clock, defrag, lazyfree
- clients & I/O       client, client_info, connection, networking, transport,
                      timeout, blocked_keys, tracking
- server lifecycle    server, persistence, rdb, replication, bio, threads_mngr,
                      childinfo, syscheck, setproctitle, cpu_affinity
- pub/sub & events    pubsub_registry, notify
- config & introspect live_config, acl, commandlog, latency, metrics, memory,
                      memory_prefetch, logreqres
- primitives          rand, mt19937, siphash, strtod, monotonic, localtime,
                      fifo, queues, mutexqueue

## Does NOT own
- command implementations  → `redis-commands` (this crate is the state they run
                             against; it does not dispatch them)
- RESP wire encode/decode  → `redis-protocol`
- value encodings (listpack/quicklist/intset/dict/rax/…) → `redis-ds`
- `RedisString`, `RedisError` → `redis-types`

A type owned by another crate must not be redeclared here —
`pretooluse-type-vocab.sh` blocks it.

## Footguns (read before touching state ownership)
- The live DB vector is owned by `redis-server`'s `RuntimeOwner` accept loop on
  the plain-TCP path — `global_databases()` is NOT authoritative there. The
  legacy TLS path is divergent and the server refuses to start it.
- Cross-connection WATCH uses a global `OnceLock` dirty-bit index
  (`WatchedKeysIndex` in db.rs), not per-client state.
- MULTI/EXEC state lives in `client.rs` (salvaged from the old `multi.rs`).
- replication: command propagation is wired, but PSYNC *partial* resync is not —
  a dropped replica link triggers a full re-sync and `sync_partial_ok/err` stay
  0 (see integration/replication-psync).

## Common tasks / where to look
- key read/write semantics, expiry-on-access → db.rs, expire.rs
- object encoding & type checks               → object.rs
- a new server config field                   → live_config.rs (+ server.rs)
- why a command blocks / unblocks             → blocked_keys.rs, timeout.rs
- eviction / maxmemory behavior               → eviction.rs, lrulfu.rs

## Ports (upstream C — never edit `reference/`)
server.c, db.c, object.c, expire.c, evict.c, networking.c, replication.c,
rdb.c, tracking.c, acl.c, bio.c, lazyfree.c, … Authoritative per-file map:
`harness/file-deps.tsv`.

## Invariants (hook-enforced)
- every `.rs` ends with a PORT STATUS trailer       — `trailer-required.sh`
- `unsafe` blocks stay under this crate's budget     — `unsafe-budget.sh`
- banned patterns (raw `*mut` outside GC/alloc)      — `forbidden-import.sh`
- cross-crate type ownership                         — `pretooluse-type-vocab.sh`

## Build / behavior
`cargo build -p redis-core`. Behavior is proven by the **oracle, not the build**.
After a behavior change, re-run the matching upstream TCL file:
`bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/<x>`

## Heads up
`lib.rs` module doc-comments still say "STUB / Phase 2-3" — stale; the crate is
full-featured. Trust the code and the oracle, not those.

Strategy, the oracle/phase model, and agent roles live in the parent `CLAUDE.md`
files — not duplicated here.

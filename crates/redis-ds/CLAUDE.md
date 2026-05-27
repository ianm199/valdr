# redis-ds

Redis value-encoding data structures — the low-level containers Redis objects
are stored in. Depends only on `redis-types`. Encodings only: this crate does
**not** decide which encoding an object uses (that's `RedisObject` in
`redis-core`) and holds no command logic.

## Owns (canonical types — `harness/type-vocabulary.tsv`, mode=enforce)
`ListPack` (listpack.rs), `QuickList` (quicklist.rs), `IntSet` (intset.rs),
`RadixTree` (rax.rs), `Dict` (dict.rs), `HashTable` (hashtable.rs),
`ZSkiplist` (zskiplist.rs), `LinkedList` (adlist.rs), `Kvstore` (kvstore.rs),
`Ziplist` (ziplist.rs), `StreamId` (stream.rs).

## Module map (14 files)
- compact byte encodings   listpack, ziplist, zipmap, intset
- list backing             quicklist (listpack of nodes), adlist (linked list)
- maps & sets              dict, hashtable, kvstore (sharded dict)
- ordered                  zskiplist
- trees / streams          rax (radix tree), stream (StreamId + stream radix)
- helpers                  pqsort

## Does NOT own
- which encoding an object uses / the `*Encoding` enums → `redis-core::object`
- command behavior (LPUSH, ZADD, …)                     → `redis-commands`
- byte strings / errors                                 → `redis-types`

## Footguns
- This crate is the main `unsafe` pressure point in the workspace — the C
  encodings are pointer arithmetic over packed byte buffers. Keep `unsafe` in
  the encoding internals; never let a raw pointer escape the module boundary.
- Encoding-conversion *thresholds* (listpack→hashtable, intset→listpack, etc.)
  live in `redis-core::object`/command logic, NOT here. This crate provides the
  containers; it doesn't decide when to switch them.
- Byte layout must match C exactly — the RDB oracle compares logical keyspace
  across the C and Rust binaries, so a subtly-wrong encoding fails there, not in
  a unit test.

## Common tasks / where to look
- port/repair an encoding → `reference/valkey/src/<x>.c`, plus the playbook
  `harness/PORTING_REDIS_DS.md`.
- verify a change         → the RDB bidirectional oracle (below), not cargo.
- stream entry IDs        → stream.rs (`StreamId`); group/consumer logic is in
  `redis-commands::stream`, not here.

## Ports (upstream C — never edit `reference/`)
listpack.c, quicklist.c, intset.c, rax.c, dict.c, adlist.c, hashtable.c,
kvstore.c, ziplist.c, zipmap.c, zskiplist (t_zset.c), StreamId (t_stream.c).
Authoritative map: `harness/file-deps.tsv`. Porting notes:
`harness/PORTING_REDIS_DS.md`.

## Invariants (hook-enforced)
- every `.rs` ends with a PORT STATUS trailer  — `trailer-required.sh`
- `unsafe` under crate budget                  — `unsafe-budget.sh`
- banned patterns (raw `*mut` discipline)      — `forbidden-import.sh`
- type ownership                               — `pretooluse-type-vocab.sh`

## Build / behavior
`cargo build -p redis-ds`. Encoding correctness is gated by the RDB bidirectional
oracle (C-saves/we-load and we-save/C-loads):
`python3 harness/oracle/rdb-diff --direction=all`.

## Heads up
`lib.rs` says "all modules are skeleton stubs" — stale; these are implemented
(the RDB corpus round-trips every type). Trust the code + oracle.

Project strategy & roles live in the parent `CLAUDE.md` files — not duplicated.

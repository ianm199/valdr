# Redis / Valkey Cheap-Bulk Translation Triage — 2026-05-24

## Why this exists

`port-harness/templates/c-to-rust/cheap-bulk-translate/` is a fast Phase-A
drafting strategy. It is useful only when the source slice has a natural Rust
shape and objective downstream gates. Redis/Valkey now has enough conformance
surface that blindly generating replacement command handlers would be harmful:
most high-traffic command code is already tested, already integrated, and
would be easy to regress.

The useful cheap-bulk target is narrower and stronger:

1. Bulk-draft source-shaped Redis data-structure primitives.
2. Repair them into `redis-ds` canonical owners.
3. Wire those owners into `redis-core::object` and existing command iterators.
4. Prove behavior through current RDB, wire, and focused TCL runners.

This is the Redis equivalent of the “source-shaped subsystem first, idiomatic
integration second” strategy.

## Current Rust crate structure vs upstream Valkey

| Rust crate | Upstream Valkey source shape | Current fit |
|---|---|---|
| `redis-types` | Shared byte strings, errors, RESP-ish value vocabulary from `server.h` and helpers | Keep small; not a bulk target. |
| `redis-protocol` | RESP parser/serializer from `resp_parser.c`, `networking.c` protocol paths | Already source-shaped enough; no bulk target unless adding parser fuzz cases. |
| `redis-ds` | `intset.c`, `listpack.c`, `quicklist.c`, `rax.c`, `ziplist.c`, `hashtable.c`, `kvstore.c`, `adlist.c`, zskiplist portion of `t_zset.c` | Best bulk target. Several files are still 25-line skeletons. |
| `redis-core` | `server.c`, `db.c`, `object.c`, `config.c`, `expire.c`, `evict.c`, `acl.c`, runtime state | Architecture-sensitive. Use architect packets, not cheap bulk, except isolated utility files. |
| `redis-commands` | `t_*.c`, `eval.c`, `functions.c`, `pubsub.c`, `multi.c`, command dispatch | Mostly integrated. Use harness test-fixers, not fresh drafts, except untouched leaf helpers. |
| `redis-server` | `server.c`, `networking.c`, `ae.c`, process/runtime wiring | Class C for cheap bulk. Runtime ownership is a design problem, not a translation throughput problem. |

## Size / completeness signal

The current high-signal gaps are in `redis-ds`:

| Subsystem | Upstream source | Upstream LOC | Current Rust owner | Rust LOC | Read |
|---|---:|---:|---|---:|---|
| `ListPack` | `listpack.c` | 1412 | `redis-ds/src/listpack.rs` | 28 | Skeleton, but a full draft exists in `listpack_listpack.rs`. |
| `IntSet` | `intset.c` | 319 | `redis-ds/src/intset.rs` | 26 | Skeleton. Strong cheap-bulk candidate. |
| `QuickList` | `quicklist.c` | 1492 | `redis-ds/src/quicklist.rs` | 26 | Skeleton. Good after ListPack lands. |
| `RadixTree` | `rax.c` | 1761 | `redis-ds/src/rax.rs` | 26 | Skeleton. Important, but more complex. |
| `Dict` / hash table | `hashtable.c` | 2378 | `redis-ds/src/hashtable.rs` / `dict.rs` | mixed / 31 | Large and core-state-sensitive. Not first. |
| `Kvstore` | `kvstore.c` | 853 | `redis-ds/src/kvstore.rs` | 25 | Depends on dict/hashtable decision. Not first. |
| `Ziplist` | `ziplist.c` | 1490 | `redis-ds/src/ziplist.rs` | 25 | Good read-only legacy RDB target. |
| `Zipmap` | `zipmap.c` | 209 | `redis-ds/src/zipmap.rs` | 513 | Already ported. Use as pattern. |
| `PQ sort` | `pqsort.c` | 164 | `redis-ds/src/pqsort.rs` | 355 | Already ported. |

The key discovery: `crates/redis-ds/src/listpack_listpack.rs` is a full
source-shaped port of `listpack.c`, but it is not the canonical `ListPack`
owner. `redis-ds/src/listpack.rs` is still a skeleton, and `redis-ds/src/lib.rs`
does not export `listpack_listpack`. That is not a new bulk-generation job; it
is a repair-and-integrate job.

## Cheap-bulk applicability by subsystem

### Class A — use the cheap path or repair an existing draft

These are byte formats or pure algorithms with contained state and objective
tests.

1. **ListPack repair**
   - Source: `reference/valkey/src/listpack.c`, `listpack.h`
   - Current: full draft in `crates/redis-ds/src/listpack_listpack.rs`; canonical
     `crates/redis-ds/src/listpack.rs` is still a stub.
   - Work: merge/rename the full draft into the canonical owner, add the
     `data: Vec<u8>` field, export public helper types, compile `redis-ds`.
   - Proof: focused Rust unit tests for append/get/delete/seek/validate plus
     full RDB oracle and `unit/sort`/encoding TCL smoke.
   - Why first: it unlocks hash/list/set/zset compact encoding work and
     removes a ghost abstraction.

2. **IntSet**
   - Source: `reference/valkey/src/intset.c`, `intset.h`
   - Work: sorted `Vec<i64>` or byte-encoding-faithful buffer with API parity:
     add, remove, find, get, random, min/max, blob len, integrity validation.
   - Proof: Rust tests generated from C behavior; then set encoding TCL cases.
   - Why: small, pure, high leverage for SET `OBJECT ENCODING intset`.

3. **Ziplist read-only**
   - Source: `reference/valkey/src/ziplist.c`, `ziplist.h`
   - Work: decoder/validator/iterator sufficient for legacy RDB load. Avoid
     fully wiring new writes to ziplist because modern Valkey writes listpack.
   - Proof: legacy RDB fixture load and unit tests over raw ziplist blobs.
   - Why: old-RDB compatibility surface with a contained byte oracle.

4. **Small utility algorithms**
   - Sources: `crc16.c`, `sha1.c`, `sha256.c`, `lzf_c.c`, `lzf_d.c`,
     `mt19937-64.c`, `rand.c`, `localtime.c`
   - Work: translate only if a current runner needs them. Several may already
     have local equivalents.
   - Proof: deterministic vector tests.
   - Why: safe bulk filler, but lower conformance leverage than encodings.

### Class B — use cheap drafts as scaffolding, then careful repair

These are source-shaped enough to draft, but the integration is architectural.

1. **QuickList**
   - Source: `reference/valkey/src/quicklist.c`, `quicklist.h`
   - Needs ListPack first.
   - Recommended shape: `VecDeque<QuickListNode>` where each node owns either a
     plain byte value or a `ListPack`; do not preserve C pointer layout.
   - Proof: list command unit tests, `OBJECT ENCODING quicklist`, RDB list
     round-trip.
   - Risk: LZF compression/bookmarks can be deferred behind explicit TODOs if
     no current runner needs them.

2. **RadixTree / Rax**
   - Source: `reference/valkey/src/rax.c`, `rax.h`
   - Recommended shape: start with a behavior-faithful ordered byte-key map API,
     not packed-node memory layout. Preserve lexicographic iteration and prefix
     semantics.
   - Proof: stream consumer-group/Pending Entry List cases and tracking cases.
   - Risk: a literal C-shaped node allocator in Rust is not worth it unless a
     benchmark proves memory layout matters.

3. **ZSkiplist**
   - Source: skiplist portion of `reference/valkey/src/t_zset.c`
   - Current sorted-set commands already pass many tests with simpler storage.
   - Use only when performance or encoding fidelity requires it.
   - Proof: zset rank/range tests plus benchmark telemetry.

4. **AdList**
   - Source: `adlist.c`, `adlist.h`
   - Low risk, but low direct product leverage. A safe `VecDeque`/linked wrapper
     is enough for most uses.

### Class C — do not bulk translate now

These are large, already integrated, or design-sensitive.

- `module.c` / RedisModule ABI: product decision first.
- `cluster*.c`: currently out of single-node claim.
- `sentinel.c`: depends on replication/HA product scope.
- `server.c`, `networking.c`, `ae*.c`: runtime ownership and event loop
  architecture. Bulk translation would generate dead or wrong code.
- `replication.c`: backbone exists; conformance needs multi-node runners and
  careful state-machine work.
- `aof.c`, `rdb.c`: already partly strong and runner-driven. Use source-shaped
  packets, not fresh bulk drafts over live code.
- `t_hash.c`, `t_list.c`, `t_set.c`, `t_zset.c`, `t_stream.c`: command behavior
  is already live and tested. Do not overwrite with bulk output. Pull small
  helper routines only when integrating real encodings.

## Proposed “rip a ton done” wave

### Wave 1 — `redis-ds` source-shaped encoding core

Goal: turn skeletons into real canonical data structures without changing the
live command behavior yet.

Packets:

1. `ds-listpack-canonicalize-v1`
   - Inputs: `listpack.c`, `listpack.h`, existing `listpack_listpack.rs`
   - Targets: `crates/redis-ds/src/listpack.rs`,
     `crates/redis-ds/src/lib.rs`, tests
   - Gate: `cargo test -p redis-ds listpack`

2. `ds-intset-source-port-v1`
   - Inputs: `intset.c`, `intset.h`
   - Targets: `crates/redis-ds/src/intset.rs`, tests
   - Gate: `cargo test -p redis-ds intset`

3. `ds-ziplist-readonly-v1`
   - Inputs: `ziplist.c`, `ziplist.h`
   - Targets: `crates/redis-ds/src/ziplist.rs`, tests
   - Gate: `cargo test -p redis-ds ziplist`

4. `ds-quicklist-mvp-v1`
   - Inputs: `quicklist.c`, `quicklist.h`
   - Targets: `crates/redis-ds/src/quicklist.rs`, tests
   - Depends on: `ds-listpack-canonicalize-v1`
   - Gate: `cargo test -p redis-ds quicklist`

Expected production Rust: ~3k–5k LOC. Most of it is leaf code with low risk to
current conformance until it is wired into `redis-core::object`.

### Wave 2 — object-model integration

Goal: stop pretending compact encodings exist while all live commands use
inline Rust collections.

Packets:

1. `object-listpack-hash-zset-basic-v1`
   - Wire `HashEncoding::ListPack` and `ZSetEncoding::ListPack` to real
     `redis_ds::ListPack` for small values.
   - Gates: hash/zset unit tests, RDB oracle, focused TCL encoding checks.

2. `object-intset-set-basic-v1`
   - Wire `SetEncoding::IntSet` to real `redis_ds::IntSet`.
   - Gates: set unit tests and `OBJECT ENCODING intset` cases.

3. `object-quicklist-list-basic-v1`
   - Wire list objects to real `redis_ds::QuickList`.
   - Gates: list unit tests, sort STORE encoding cases, RDB list oracle.

Expected product impact: fewer encoding lies, better DUMP/RESTORE and SORT
coverage, and a cleaner foundation for memory/performance work.

### Wave 3 — stream/tracking data structure fidelity

Goal: replace inline stream/Pel maps with a real ordered byte-key tree where it
matters.

Packets:

1. `ds-rax-behavior-map-v1`
2. `stream-rax-pel-basic-v1`
3. `tracking-rax-prefix-basic-v1`

Do this only after Wave 1 and after current stream TCL frontiers are measured.

## How to use `cheap-bulk-translate` here

Do not point it at all of Valkey. Use one source cluster per spec and write
drafts under a scratch directory, not directly into live crate files.

Example specs to create when running the cheap path:

```text
porting_md=harness/PORTING_REDIS_DS.md
source=reference/valkey/src/intset.c
source=reference/valkey/src/intset.h
output_dir=drafts/redis-ds-intset
target=crates/redis-ds/src/intset.rs|IntSet source-shaped safe Rust implementation
```

```text
porting_md=harness/PORTING_REDIS_DS.md
source=reference/valkey/src/ziplist.c
source=reference/valkey/src/ziplist.h
output_dir=drafts/redis-ds-ziplist
target=crates/redis-ds/src/ziplist.rs|read-only Ziplist decoder, iterator, integrity validator
```

Then the normal harness should run the repair/integration packet. Cheap drafts
are input evidence, not trusted code.

## Immediate recommendation

Start with `ds-listpack-canonicalize-v1`. It is the best first test because:

- no cheap API key or new generation is needed;
- the full draft already exists;
- the canonical owner is currently a skeleton;
- it is pure byte-structure code;
- it has obvious local tests;
- it unblocks the rest of the encoding wave.

If that packet repairs cleanly, run `ds-intset-source-port-v1` next. If it does
not, that is useful evidence that the existing full draft quality is too low
and the cheap-bulk strategy should stay out of live Redis until the repair loop
gets better.


# Redis-DS Source-Shaped Porting Guide

This guide is for cheap-bulk Phase-A drafts and follow-up integration packets
for `redis-ds`. It is narrower than the workspace `PORTING.md`.

## Goal

Port Valkey's compact data-structure encodings into canonical Rust owners:

- `reference/valkey/src/listpack.c` -> `crates/redis-ds/src/listpack.rs`
- `reference/valkey/src/intset.c` -> `crates/redis-ds/src/intset.rs`
- `reference/valkey/src/ziplist.c` -> `crates/redis-ds/src/ziplist.rs`
- `reference/valkey/src/quicklist.c` -> `crates/redis-ds/src/quicklist.rs`
- `reference/valkey/src/rax.c` -> `crates/redis-ds/src/rax.rs`
- `reference/valkey/src/adlist.c` -> `crates/redis-ds/src/adlist.rs`

These are mostly byte formats and pure algorithms. Do not touch command
semantics in the draft phase.

## Rust Shape

- Keep Redis data byte-oriented: `Vec<u8>`, `&[u8]`, `RedisString` only when
  crossing into `redis-core` / `redis-commands`.
- Do not invent duplicate public types. The canonical public type names already
  exist in `crates/redis-ds/src/*.rs` and `harness/type-vocabulary.tsv`.
- Prefer safe owned structures over literal C pointer graphs unless byte layout
  itself is the product behavior.
- Use source anchors near tricky logic, for example:
  `// upstream: listpack.c lpEncodeBacklen`.
- No `unsafe`.
- No new external dependencies.

## Integration Policy

Cheap-bulk output is a draft, not product code. A successful integration packet
must:

1. Move or merge the useful draft into the canonical owner file.
2. Keep the canonical public type name and module path stable.
3. Add focused unit tests in the same module.
4. Pass `cargo test -p redis-ds <module>`.
5. Pass `cargo check --workspace`.
6. Preserve the `PORT STATUS` trailer.

## Deferrals

The first wave should not wire these encodings into live Redis objects unless a
packet explicitly says so. Object-model wiring is a second wave after the leaf
types compile and have tests.


# redis-types

The foundation crate — canonical cross-cutting types every other port crate
depends on. **No dependencies on any other port crate**; deps only flow *into*
here. Keep it that way.

## Owns (canonical types — `harness/type-vocabulary.tsv`, mode=enforce)
- `RedisString` — the byte-string type for all Redis keys/values — `src/string.rs`
- `RedisError`, `RedisResult` — `src/error.rs`

## Module map (2 modules)
- string   `RedisString` — owned byte buffer; the Rust stand-in for C `sds`
- error    `RedisError` (the error reply taxonomy) + `RedisResult<T>` alias

## Does NOT own
- RESP frames (`RespFrame`)     → `redis-protocol`
- value encodings               → `redis-ds`
- server/db/object/client state → `redis-core`

A type owned elsewhere must not be redeclared here — `pretooluse-type-vocab.sh`
blocks it.

## Footguns
- `RedisString`, never Rust `String`/`str`, for any key/value data — Redis
  values are arbitrary bytes, not guaranteed UTF-8. `forbidden-import.sh` bans
  the wrong choice in data paths.
- This crate is the foundation: a change here recompiles the entire workspace.
  Keep the surface small and stable; don't add convenience types that belong in
  a higher crate.
- `RedisError` variants must serialize to the exact C error-reply prefixes
  (`ERR`, `WRONGTYPE`, `NOSCRIPT`, …) — the TCL oracle matches error strings.

## Common tasks / where to look
- add/adjust an error reply → error.rs (mind the exact prefix/text vs C).
- byte-string operations    → string.rs.
- "where does type X live?" → it's NOT here unless it's `RedisString`/`RedisError`;
  check `harness/type-vocabulary.tsv`.

## Ports (upstream C — never edit `reference/`)
Byte-string semantics mirror C `sds` (sds.c). Not a 1:1 file port — this crate
is the Rust-native shared vocabulary the rest of the port is built on.

## Invariants (hook-enforced)
- every `.rs` ends with a PORT STATUS trailer  — `trailer-required.sh`
- the no-other-port-crate-dependency rule above (foundation crate)
- banned patterns (`harness/forbidden-patterns.sh`) — `forbidden-import.sh`

## Build
`cargo build -p redis-types`. Recompiles the whole workspace on change.

## Heads up
The `lib.rs` PORT STATUS still says "scaffolding; awaiting first translation
packet" — stale; these types are foundational and in use everywhere.

Project strategy & roles live in the parent `CLAUDE.md` files — not duplicated.

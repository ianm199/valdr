# redis-protocol

The wire boundary: RESP2/RESP3 encode and decode. Pure protocol — turns
bytes ⇄ frames with no socket, client, or command knowledge. Depends only on
`redis-types`.

## Owns (canonical types — `harness/type-vocabulary.tsv`, mode=enforce)
`RespFrame` — `src/frame.rs`.

## Module map (3 modules)
- frame    `RespFrame` + all encoders (`encode_resp2`, `encode_resp3`,
           `encode_for_proto`, maps/sets/pushes, doubles, verbatim, big-number)
- parser   incremental byte parser (`ParserCursor`, `ParserCallbacks`)
- request  inline / multibulk request parsing (the client read path)

## Does NOT own
- socket reads/writes & client buffers   → `redis-core::networking` + `redis-server`
- command dispatch / what a reply *means* → `redis-commands`
- the values being encoded                → `redis-core` / `redis-ds`

## Footguns
- RESP2 and RESP3 encode differently for the same frame (maps, sets, doubles,
  booleans, nulls, push). Encode through `encode_for_proto` against the client's
  negotiated protocol — never hardcode one. A RESP3-only frame sent to a RESP2
  client is a wire bug the smoke oracle will catch.
- Parsing is incremental: `Ok(None)` means "partial command, keep the buffer."
  For live client scratch use `parse_inline_or_multibulk_into_retaining_partial`
  (retains partial argv); the plain variant clears it. Mixing them up corrupts
  pipelined / fragmented reads.

## Common tasks / where to look
- add a new reply shape → frame.rs: extend `RespFrame` + both `encode_resp2`
  and `encode_resp3` (or `encode_for_proto`).
- a client read bug      → request.rs (inline vs multibulk, partial handling).
- verify wire output     → the RESP smoke oracle (below), byte-for-byte vs C.

## Ports (upstream C — never edit `reference/`)
The RESP parsing and `addReply*` encoding paths from networking.c
(`processInlineBuffer`, `processMultibulkBuffer`, the reply helpers).
Authoritative map: `harness/file-deps.tsv`.

## Invariants (hook-enforced)
- every `.rs` ends with a PORT STATUS trailer  — `trailer-required.sh`
- `unsafe` under crate budget                  — `unsafe-budget.sh`
- banned patterns                              — `forbidden-import.sh`
- type ownership                               — `pretooluse-type-vocab.sh`

## Build / behavior
`cargo build -p redis-protocol`. Wire correctness is proven byte-for-byte by the
RESP smoke oracle: `bash harness/oracle/smoke.sh --skip-build`.

## Heads up
`lib.rs` still says "Phase 2 of the pilot" — stale; RESP2/3 is fully landed.

Project strategy & roles live in the parent `CLAUDE.md` files — not duplicated.

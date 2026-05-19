# Harness task modes — A (translation) vs B (greenfield infra)

Captured during Session N+ work on Valkey port. Surfaced as a real
gap when stress-testing the harness on a mixed-mode parallel wave
(EVAL bridge + GEO + ACL + BGSAVE-fork + LFU eviction).

## The two modes

**Mode A — Translation**
> "Read this C file, produce Rust that behaves the same on the wire."

Examples we've done:
- t_string.c → string.rs
- t_zset.c → zset.rs
- RDB per-encoding serializers (hash/list/set/zset/stream)
- bitops.c → bitops.rs
- hyperloglog.c → hyperloglog.rs

What works for Mode A:
- Translator agent template
- Wire-diff oracle catches behavioral divergence
- rdb-diff oracle for persistence
- Hooks enforce porting hygiene (RedisString, no unsafe, vocab ownership)
- Token cost is predictable: ~$8-15 per file

**Mode B — Greenfield infrastructure**
> "Design and implement a Rust subsystem that satisfies this
> behavioral contract."

Examples we've done (successfully but with heavy operator prompting):
- LiveConfig spine (15a)
- BlockedKeysIndex + timeout thread (R12b)
- pubsub_registry + writer-thread architecture (8a)
- Active-expire cycle (OV-3)
- maxmemory pre-command gate + LRU sampling evictor (16b)
- RDB framework: varint/header/CRC/save/load top-level (R18)
- The Rust `notify_keyspace_event` helper bridging pubsub + config (15a)

Tomorrow's Mode B work:
- EVAL/EVALSHA via mlua FFI bridge
- BGSAVE real fork (Unix-specific)
- Replication primary-side state machine (when we get there)
- AOF append-only-file writer + rewrite

## Why Mode B strains the harness

The harness was optimized for translation. Mode B uses the same
primitives (Edit OCC, hooks, oracle, parallel dispatch) but the
ergonomic story is different:

1. **No Mode-B agent template.** The translator template asks
   "what C file?". Mode B has no C file. Operator writes a much
   heavier prompt to compensate.

2. **Oracle catches behavior, not architectural drift.** The
   `stub_server` bug in 15a's preconditions was caught only because
   the operator read the code. Wire-diff would have passed even
   with the bug.

3. **Type vocabulary tracks ownership, not invariants.** It says
   "RedisServer lives here." It can't say "RedisServer must be
   reachable from CommandContext via Arc."

4. **Token cost is unpredictable.** Mode B rounds run 150-300k
   tokens vs Mode A's 50-100k. The variance is operator-prompting
   thoroughness rather than agent capability.

## What would make the harness better at Mode B

(Ideas. Not all of them obviously right.)

### a. A Mode-B (architect-implementer) agent template
- Input: a behavioral contract + an architectural spec
- Output: Rust subsystem + property tests that codify the contract
- The operator's job is writing the spec; the template provides the
  scaffolding (test patterns, file-layout conventions, invariant
  documentation requirements)

### b. Property/invariant tests as a second oracle
- wire-diff = behavior
- rdb-diff = persistence
- property-tests = architectural invariants (e.g., "every write
  command propagates through the AOF writer", "every config field
  is reachable from a live handler")
- These would have caught `stub_server` automatically

### c. Static generation of hot files
- `dispatch.rs` is a singleton hot file — every command-adding
  round contends here. Extract from `command-registry.json` →
  generate `dispatch.rs`. Agents only add the *handler function*,
  the dispatch row is mechanical.
- Similar candidates: `lib.rs` `pub mod` lists, `Cargo.toml` deps
  for new modules

### d. ADR (architecture decision record) discipline
- For each Mode-B round, the agent writes a short ADR documenting:
  - What architectural choice was made
  - What was rejected
  - What constraint the choice satisfies
- Lives at `docs/ADR_NNN_<name>.md`
- Future agents read ADRs as part of "context"
- Started this in EVAL's ADR_001_LUA_RUNTIME.md

### e. Cross-cutting type invariants in machine-readable form
- Beyond type-vocabulary.tsv's ownership column, add invariant tags:
  - `RedisServer: Arc-shared, no &mut at runtime, fields are atomics`
  - `Client: per-connection, not Send across threads after construction`
  - `RedisDb: held behind Arc<Mutex<>>, accessed via CommandContext`
- Hooks can check these on Edit/Write

## Status / next action

This doc is a placeholder for harness-side thinking, NOT a Valkey
port deliverable. The Valkey port continues to use the harness as-is
with operator prompting to bridge Mode-B gaps.

Harness improvements live in the `port-harness/` repo. Don't try to
build them while in the middle of a Valkey port wave — they need
their own focused session.

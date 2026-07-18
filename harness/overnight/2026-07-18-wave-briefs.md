# Overnight wave briefs — 2026-07-18 (agent-console night shift)

Task briefs for CAMPAIGN_BACKLOG Phase 1 waves 9/10/8/12, queued as
night-shift tasks. Each task's queue prompt points at exactly one section
here. Written by the coordinating session; see AGENT_COORDINATION_BOARD.md
in the parent tree for the claim.

## Wave 9 — Transactions

Implement CAMPAIGN_BACKLOG.md Phase 1 **Wave 9 — Transactions** in the valdr edge engine: MULTI, EXEC, DISCARD, WATCH, UNWATCH in `crates/valdr-engine/src/lib.rs`.

READ FIRST, in order:
1. `harness/VALDR_ENGINE_COMMAND_PLAYBOOK.md` — the pre-computed contract for adding engine commands. Follow it exactly; do not re-derive.
2. The Wave 9 entry in `CAMPAIGN_BACKLOG.md` (context only — see the "never edit" rule below).
3. The transaction semantics in the already-correct Rust port: `crates/redis-commands/src/multi.rs` (and its dispatch wiring) — mirror its reply shapes, error strings, and check ordering byte-for-byte. The pinned C is at `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/multi.c` (NOTE: `reference/` is untracked, so it does NOT exist in this worktree — use that absolute path into the main checkout, read-only).

Scope notes:
- The engine is single-connection (a Durable Object serializes callers), so this wave is command-queueing + WATCH/CAS bookkeeping, not concurrency. Semantics that ARE testable single-connection and must be covered: +QUEUED replies while in MULTI; EXEC returning the array of queued results in order; DISCARD; EXEC/DISCARD without MULTI (error); nested MULTI (error); a queue-time-invalid command (unknown command / wrong arity) poisons the transaction so EXEC fails with EXECABORT; a runtime error inside EXEC appears as an error element in the array while later commands still run; WATCH then EXEC untouched (succeeds); WATCH then the same client writes the watched key before EXEC → EXEC returns null (self-touch aborts, exactly as in Redis); UNWATCH clears watches; EXEC clears watches either way.
- Place the new dispatch branches adjacent to `script_command`'s area rather than at the very tail of `execute_inner`, to reduce textual merge conflicts with sibling waves.
- Remember `note_write` discipline per the playbook: EXEC's executed commands account writes exactly as they would standalone; MULTI/DISCARD/WATCH/UNWATCH themselves never call `note_write`.

Fixtures: new `harness/oracle/valdr-fixtures/multi.jsonl` with a file-unique key namespace. Fixtures in one file share state in order — build the MULTI...EXEC sequences as consecutive lines.

The gate (must pass before you are done, run from the worktree root):
```
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/valkey-server --strict
```
must print **0 diverge** and exit 0 (pass count must rise; never any diverge), plus `cargo test -p valdr-engine` green. Build success is not signal; the oracle is the only truth-teller. The differential runner respects CARGO_TARGET_DIR when locating valdr-fixture-runner.

Hard rules:
- Do NOT edit `CAMPAIGN_BACKLOG.md` (the operator updates wave status at merge; sibling tasks run tonight and that file is a conflict magnet).
- Do NOT touch anything outside `crates/valdr-engine/` + the new fixture file. The single-node server, replication code, and other crates are out of scope.
- Commit your work with a clear conventional message including the final oracle counts (e.g. "feat(valdr-engine): Wave 9 transactions — oracle NNNN/0/NN").

## Wave 10 — Scan + introspection

Implement CAMPAIGN_BACKLOG.md Phase 1 **Wave 10 — Scan + introspection** in the valdr edge engine: SCAN, HSCAN, SSCAN, ZSCAN (cursor model), KEYS, DBSIZE in `crates/valdr-engine/src/lib.rs`. This also clears the SSCAN/ZSCAN stragglers deferred from Waves 5/6.

READ FIRST, in order:
1. `harness/VALDR_ENGINE_COMMAND_PLAYBOOK.md` — the pre-computed contract. Follow exactly.
2. The scan implementations in the already-correct Rust port: `crates/redis-commands/src/` (grep `b"SCAN"` in `dispatch.rs` for the handlers) — mirror reply shapes, error strings, MATCH/COUNT/TYPE/NOVALUES option parsing, and check ordering byte-for-byte. Pinned C at `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/db.c` (scanGenericCommand) — absolute path into the main checkout; `reference/` does not exist in this worktree.

THE CENTRAL TRAP — nondeterministic reply ordering:
The differential oracle compares raw RESP frames byte-for-byte against a real valkey-server. Iteration order over multi-element collections WILL differ between the engine's data structures and valkey's, so a fixture like "SCAN over a 5-key db" can never pass. Before writing any fixture, study how the existing fixture files handled nondeterministic replies (HRANDFIELD in `hash.jsonl` from Wave 2, RANDOMKEY in Wave 3's files) and restrict fixtures to order-deterministic cases:
- empty db / empty collection scans (exact empty reply, cursor "0"),
- single-element db/hash/set/zset scans (exactly one reply shape possible),
- MATCH patterns that narrow the result to exactly 0 or 1 element,
- TYPE filter narrowing to 0 or 1 key, HSCAN NOVALUES on a 1-field hash,
- cursor termination semantics (returned cursor is "0" when the collection fits in one pass),
- KEYS with a pattern matching exactly 0 or 1 key; DBSIZE (fully deterministic — cover it well, including after DEL/expiry),
- arity errors, wrong-type errors, invalid-cursor behavior (mirror the reference: non-numeric cursor error text), negative/zero COUNT error handling.
That restricted surface is still a real, honest wave: the cursor model, option parsing, and error paths all get proven. Do not "fix" ordering by sorting replies in the engine — byte-parity with the reference is the contract, and valkey does not sort.

Fixtures: new `harness/oracle/valdr-fixtures/scan.jsonl`, file-unique key namespace. Place dispatch branches near the keyspace/generic handlers (TYPE/RANDOMKEY area), not at the tail of `execute_inner`.

The gate (must pass before you are done):
```
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/valkey-server --strict
```
must print **0 diverge** (pass count rises), plus `cargo test -p valdr-engine` green.

Hard rules:
- Do NOT edit `CAMPAIGN_BACKLOG.md`.
- Do NOT touch anything outside `crates/valdr-engine/` + the new fixture file.
- Commit with a conventional message including the final oracle counts.

## Wave 8 — HyperLogLog

Implement CAMPAIGN_BACKLOG.md Phase 1 **Wave 8 — HyperLogLog** in the valdr edge engine: PFADD, PFCOUNT, PFMERGE in `crates/valdr-engine/src/lib.rs`.

READ FIRST, in order:
1. `harness/VALDR_ENGINE_COMMAND_PLAYBOOK.md` — the pre-computed contract. Follow exactly.
2. **`crates/redis-commands/src/hyperloglog.rs`** — the already-correct, Tcl-suite-passing Rust port of valkey's HLL. This is your source of truth for the algorithm: the sketch header ("HYLL"), dense/sparse encodings, MurmurHash64A, register updates, the cardinality estimator and its constants, and the sparse→dense promotion rules. Pinned C for cross-reference: `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/hyperloglog.c` (absolute path into the main checkout — `reference/` does not exist in this worktree).

Architecture constraint: valdr-engine deliberately does NOT depend on redis-commands/redis-core/redis-ds (wasm-safe, minimal deps — see its Cargo.toml). Port the needed HLL core into the engine (translate from hyperloglog.rs, trimming to what PFADD/PFCOUNT/PFMERGE need). Do NOT add a dependency on redis-commands. Keep the engine's single-file structure per the playbook.

Why byte-parity matters here and is achievable: an HLL value is a String at the keyspace level — the oracle's fixtures can (and should) assert `STRLEN`/`TYPE`/`OBJECT ENCODING`-visible behavior only where the reference guarantees it, but the critical assertions are PFADD's 0/1 modified replies, PFCOUNT's exact estimates, and PFMERGE results. Valkey's estimates are deterministic for a given input sequence, and hyperloglog.rs already reproduces them exactly (it passes upstream's own HLL tests) — so translating faithfully gives you exact-match PFCOUNT values against the real valkey-server. If a fixture's PFCOUNT diverges, your port has a real bug; do not band-aid the fixture.

Fixture guidance (`harness/oracle/valdr-fixtures/hll.jsonl`, file-unique namespace):
- PFADD new key → 1; PFADD existing elements → 0; PFADD no-element form (creates empty HLL, reply semantics per reference); PFCOUNT empty/missing key → 0; PFCOUNT single key exact values for a few dozen deterministic elements; PFCOUNT multi-key (union estimate); PFMERGE into new and existing destinations, overlapping sources; WRONGTYPE errors both directions (PFADD on a plain string that is not a valid HLL → the reference's exact error; GET-visible sparse encoding growth only if the reference behavior is deterministic — check hyperloglog.rs); sparse→dense promotion by adding enough distinct elements (stay bounded — a few thousand elements max via many fixture lines is too many; promotion can also be triggered by sparse-size overflow with fewer, well-chosen elements — check `hll_sparse_max_bytes` default in the reference and mirror what is reachable deterministically).
- `note_write` exactly when the stored sketch bytes changed (PFADD returning 0 must NOT note_write unless the reference mutates anyway — check hyperloglog.rs for cache-invalidation writes: the HLL header caches the cardinality, so PFCOUNT can MUTATE the value by updating the cache! Mirror the reference's behavior precisely, including whether your engine treats PFCOUNT's cache refresh as a snapshot-visible write — the per-key dirty-flush round-trip test in `cargo test -p valdr-engine` will catch it if you get this wrong.)

This is the meatiest open wave — take the time to get the translation right rather than rushing all three commands. PFADD+PFCOUNT with the oracle green is a legitimate stopping point if the window runs out; say so plainly in the commit rather than landing an unverified PFMERGE.

The gate (must pass before you are done):
```
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/valkey-server --strict
```
must print **0 diverge**, plus `cargo test -p valdr-engine` green (the dirty-flush round-trip test especially).

Hard rules:
- Do NOT edit `CAMPAIGN_BACKLOG.md`.
- Do NOT touch anything outside `crates/valdr-engine/` + the new fixture file.
- Commit with a conventional message including the final oracle counts.

## Wave 12 — DUMP / RESTORE

Implement CAMPAIGN_BACKLOG.md Phase 1 **Wave 12 — DUMP / RESTORE** in the valdr edge engine (`crates/valdr-engine/src/lib.rs`). This enables key migration in and out of the edge.

READ FIRST, in order:
1. `harness/VALDR_ENGINE_COMMAND_PLAYBOOK.md` — the pre-computed contract. Follow exactly.
2. The already-correct Rust port's DUMP/RESTORE: grep `b"DUMP"` and `b"RESTORE"` in `crates/redis-commands/src/dispatch.rs` and read the handlers plus the RDB serializers they call (`crates/redis-core/src/rdb/`). Pinned C: `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/cluster.c` (dumpCommand/restoreCommand) and `rdb.c` — absolute paths into the main checkout; `reference/` does not exist in this worktree.

Architecture constraint: valdr-engine does not depend on redis-core/redis-commands (wasm-safe, minimal deps). Port the minimal RDB value-serialization subset the engine needs for its own value types (string, hash, list, set, zset, stream if cheap — check which StoredValue variants the engine has and scope to those), the 2-byte RDB version footer, and the CRC64 (Jones polynomial) checksum. Translate from the Rust port, not from scratch.

Byte-parity strategy — be honest about what is provable:
- **RESTORE from reference-produced payloads is the primary, always-provable surface.** In fixtures you cannot embed raw binary easily; JSONL fixture args are strings. Check how the fixture format handles binary (look at existing fixtures for escaping; the fixture runner and oracle pass args as bytes — verify whether non-UTF8 payloads survive the JSONL path BEFORE building on it; if they cannot, restrict RESTORE fixtures to payloads that are valid UTF-8-safe byte sequences, e.g. simple string values dumped from short ASCII values — a DUMP of "hello" is deterministic bytes you can pre-compute with the real valkey-server via redis-cli and embed if escapable).
- **Round-trip fixtures are second**: DUMP a key the engine wrote, RESTORE it under a new name in the same file, then read the new key back — this proves self-consistency regardless of byte-level parity, because both DUMP and RESTORE run on both sides (engine and reference) and the final read-back must match.
- **Direct DUMP byte-parity** (engine DUMP output == valkey DUMP output for the same value) is the strongest claim — achievable when your serializer chooses the same encodings the reference chooses (e.g. small hash → listpack). Mirror the encoding-selection thresholds from redis-core's rdb serializer. Assert it for a few simple values (short string, small int-encoded string) where encoding choice is unambiguous. If a value type's encoding parity is uncertain, prefer round-trip fixtures over false confidence — never let a diverge land.
- RESTORE semantics to cover: BUSYKEY error without REPLACE; REPLACE; TTL argument (0 = no TTL; positive = ms TTL — verify with PTTL band or absolute EXPIRETIME pattern per playbook TTL guidance); ABSTTL flag; bad-checksum and bad-version payloads → the reference's exact error strings; IDLETIME/FREQ arguments parse-and-accept per reference.

Fixtures: new `harness/oracle/valdr-fixtures/dump.jsonl`, file-unique namespace.

The gate (must pass before you are done):
```
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey/src/valkey-server --strict
```
must print **0 diverge**, plus `cargo test -p valdr-engine` green.

Hard rules:
- Do NOT edit `CAMPAIGN_BACKLOG.md`.
- Do NOT touch anything outside `crates/valdr-engine/` + the new fixture file.
- Commit with a conventional message including the final oracle counts. If you prove only a subset (e.g. strings + hashes), say exactly that in the commit — an honest partial with 0 diverge beats a broad claim.

# Valdr overnight campaign backlog

Durable, self-replenishing queue for the overnight run started 2026-06-22.
The Task tracker holds the *active slice*; this file is the *exhaustive queue*
that outlives any one session. **Never run out of work**: when the active waves
empty, run the harvest step and refill from the live gap report.

## How this stays self-replenishing (do this, don't guess)

The valdr-engine surface backlog is **computed, not hand-maintained**:

```bash
bash harness/oracle/valdr-surface-gap.sh
```

It diffs the full production command table (`crates/redis-commands/src/dispatch.rs`)
against what the edge engine actually dispatches, minus an explicit
out-of-scope/subcommand exclusion list, and prints the in-scope commands still
missing. As commands land, the list shrinks on its own. When it prints
`0 in-scope commands remaining`, Phase 1 is exhaustive → advance to Phase 2.

**Harvest loop (run when the active Task queue is empty):**
1. `bash harness/oracle/valdr-surface-gap.sh` → current missing list.
2. Pick the next wave (recommended order below); create Tasks for it.
3. Implement per `harness/VALDR_ENGINE_COMMAND_PLAYBOOK.md`, gate on the
   differential oracle (`0 diverge`) + `cargo test -p valdr-engine`, commit.
4. Re-run the gap report; repeat. To pull a "scope-review" command into scope,
   delete it from the `exclude` list in `valdr-surface-gap.sh`.

Gate, every landing:
```bash
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin "$(pwd)/reference/valkey/src/valkey-server" --strict
```
Baseline 2026-06-22: **382 pass / 0 diverge / 19 known-unsupported**, engine
dispatches 27 of 200 in-scope commands (**173 missing**).

Status legend: `[ ]` queued · `[~]` in progress · `[x]` landed (oracle-green, committed) · `[?]` scope-review.

---

## Phase 1 — valdr-engine command surface (the overnight bulk)

Recommended wave order (value-at-edge × low effort first). The gap report is the
source of truth for what remains; this is the suggested sequencing.

### Wave 1 — String core  `[x]`  (3f3df9c — oracle 430/0/11)
APPEND, SETNX, GETDEL, MGET, STRLEN, DECR, DECRBY, GETSET, INCRBYFLOAT;
SET KEEPTTL / EXAT / PXAT options. Closes 8 known-unsupported. Fixtures: `strings.jsonl`.

### Wave 2 — Hash core  `[x]`  (9c361b5 — oracle 473/0/8)
HEXISTS, HLEN, HMGET, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HMSET,
HRANDFIELD. Fixtures: `hash.jsonl`.

### Wave 3 — Keyspace / generic / connection  `[x]`  (16d4c90 — oracle 608/0/3)
TYPE, PERSIST, EXPIREAT, PEXPIREAT, EXPIRETIME, PEXPIRETIME, EXPIRE/PEXPIRE
NX|XX|GT|LT options, RENAME, RENAMENX, COPY, RANDOMKEY, TOUCH, UNLINK, FLUSHALL,
PING, ECHO, TIME, OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ. Fixtures: `expiry.jsonl`,
new `keyspace.jsonl`, new `connection.jsonl`.

### Wave 4 — List value type  `[x]`  (525d1fb — oracle 696/0/3, 13 cmds, LPOS deferred)
LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP (+count), LLEN, LRANGE, LINDEX, LSET,
LINSERT, LREM, LTRIM, LPOS. Fixtures: new `list.jsonl`.

### Wave 5 — Set value type  `[x]`  (f9d6e3f — oracle 785/0/8, 14 cmds; SPOP/SRANDMEMBER/SSCAN deferred)
SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SPOP, SRANDMEMBER. Then the
multi-key set ops: SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE,
SINTERCARD, SMOVE. Fixtures: new `set.jsonl`.

### Wave 6 — ZSet surface fill-in  `[x]`  (d356364 — oracle 883/0/12; ZRANDMEMBER/ZSCAN/aggregates deferred → Wave 6b)
ZPOPMIN, ZPOPMAX, ZCOUNT, ZMSCORE, ZLEXCOUNT, ZRANGEBYLEX, ZREVRANGE,
ZREVRANGEBYSCORE, ZREVRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE,
ZREMRANGEBYLEX, ZRANDMEMBER. Then aggregates: ZUNION, ZINTER, ZDIFF,
ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, ZRANGESTORE, ZINTERCARD, ZMPOP. Fixtures: `zset.jsonl`.

### Wave 7 — Bitmaps + numeric strings  `[x]`  (b266130 — oracle 1036/0/17; INCRBYFLOAT/HINCRBYFLOAT/BITFIELD/BITOP deferred)
SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO; GETRANGE,
SETRANGE, SUBSTR, GETEX, PSETEX, MSET, MSETNX. Fixtures: `bitmap.jsonl`, `strings.jsonl`.

### Wave 8 — HyperLogLog  `[ ]`
PFADD, PFCOUNT, PFMERGE (dense/sparse parity vs reference). Fixtures: new `hll.jsonl`.

### Wave 9 — Transactions  `[ ]`
MULTI, EXEC, DISCARD, WATCH, UNWATCH. The DO already serializes, so this is
command-queue + WATCH/CAS semantics, not concurrency. High edge value (atomic
multi-command decisions). Fixtures: new `multi.jsonl`.

### Wave 10 — Scan + introspection  `[ ]`
SCAN, HSCAN, SSCAN, ZSCAN (cursor model), KEYS, DBSIZE (single-db). Fixtures: new `scan.jsonl`.

### Wave 11 — Streams  `[x]`  (core c1ce5a6 + groups c0c77be — feature-complete bar clock-dependent claim ops)
XADD, XLEN, XRANGE, XREVRANGE, XREAD (non-blocking), XDEL, XTRIM, XSETID, XINFO;
then consumer groups XGROUP/XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM.
Fixtures: new `stream.jsonl`. Split into sub-waves.

### Wave 12 — DUMP / RESTORE  `[ ]`
DUMP, RESTORE (RDB-serialization parity). Enables key migration in/out of the edge.

### Scope-review (in `exclude` or debatable) `[?]`
- Blocking: BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, BZMPOP —
  a request/response Durable Object can't block a client across the loop; decide
  immediate-only variants or defer.
- LMOVE, RPOPLPUSH, LMPOP, ZMPOP — non-blocking, fine; fold into Waves 4/6.
- SORT, SORT_RO — complex (BY/GET patterns); separate wave if wanted.
- GEO* — niche at edge; defer unless a demo needs it.
- DELIFEQ, MSETEX, hash-field-TTL (HEXPIRE/HTTL/HPERSIST/HGETEX/HGETDEL/HSETEX) —
  Valkey-specific / newer; pull in once core is done.

---

## Phase 2 — EdgeStash cold-start  `[x] implemented`  (live measurement = interactive)
**Option A lazy per-key loading IMPLEMENTED** (2a `d9c8fa6` mechanism + kit, 2b
`82c9a03` wired into demo + cloudflare, wasm-compiles). Cold cost O(touched) not
O(state). Only `wrangler deploy` + latency sweep remains (your session). Details:
`docs/EDGESTASH_COLDSTART_PREP.md`. Below = the original analysis.
Named open target: ~0.5s cold Durable Object start. **Analysis complete →
`docs/EDGESTASH_COLDSTART_PREP.md`.** Cost = wasm-instantiate(size) +
`storage.list()` + O(state) engine rebuild on first request (`edgestash-cloudflare/src/lib.rs`
`load_entries`). Two candidate wins: (A) **lazy per-key loading** — drop the
eager full `storage.list()`, fetch keys on touch via the per-key persistence
model → cold-start O(1) not O(state); (B) **shrink the wasm** (opt-z + lto +
panic=abort + strip + wasm-opt) → faster instantiate. Both need a live
`wrangler deploy` + cold-start latency sweep to validate — NOT overnight-safe.
NOTE: branch `perf/cold-start-eager-jemalloc` is single-node-server jemalloc/warmup,
a DIFFERENT cold-start (not the Worker). Deploy mechanism: `cloudflare-deploy-blocker` memory.

## Phase 3 — Single-node sub-parity perf  `[ ]`  (bench is noisy → confirm interactively)
Sub-parity commands: RPUSH 0.89×, RPOP 0.95×, XADD 0.81×, FCALL 0.71×.
FCALL = Lua-VM per-call overhead (roadmap names it the last sub-parity command).
Prep: profile hotpaths, stage fixes; gate any claim on a clean interactive bench re-run.

---

## Phase 4 — Large WIP to return to after Valdr (per `docs/roadmap.md` + lane branches)

**Assessed 2026-06-23 (NOT a fast-loop / overnight-safe pivot — different subsystem, slow Tcl gate):**
- **`lane/repl-observability` is ALREADY FULLY IN MAIN** (0 commits not in main) — no-op, ignore it.
- **`lane/replication-burn-down` = 41 unmerged commits** (merge-base `31c4433`, 2026-06-03)
  touching 48 files, of which **32 ALSO changed on `main`** since (dispatch.rs, db.rs,
  replication.rs, connection.rs, rdb/{load,save,mod}.rs, runtime_owner.rs, client.rs,
  metrics.rs, multi.rs, info.rs, persist.rs, tcl-survey.py, …). A real
  conflict-heavy reconciliation in the CORE single-node server, verifiable only by
  the slow/flaky Tcl suite — do this DELIBERATELY in a fresh session with the
  single-node oracle, file-by-file; do NOT auto-merge (high risk to `main`).
- **Replication out of alpha** — the goal those commits serve: PSYNC partial-resync
  instead of always full re-sync. Docs: `docs/REPL_OBSERVABILITY_OVERNIGHT_PLAN.md`,
  `docs/REPLICATION_INTEGRATION_DASHBOARD.md`.
- **HA / Cluster** — `docs/HA_CLUSTER_REPLICATION_ROADMAP.md` is the execution queue.
- **mlua → lua-rs migration** — `docs/MLUA_EXIT_PLAN.md`. Removes the last embedded
  C dependency in the data path and the `unsafe` blocks in `eval.rs`; gated on
  Lua 5.1 compat in the sibling `lua-rs-port`. **BLOCKED**: omnilua still has the
  GC use-after-sweep bug (omnilua#189) that the valdr pcall harness works around —
  don't start until that's fixed upstream (see `omnilua-gc-use-after-sweep` memory).
- **`#![forbid(unsafe_code)]`** on the zero-unsafe data crates once mlua is gone.
- **Bigger bets** — I/O threads (Valkey-style), then sharded execution; compact
  encodings (intset/listpack/skiplist) — also applicable to the edge engine.
- **C ABI decision** — support unsafe C modules vs full pure-Rust alternatives.

---

## Differential-testable surface COMPLETE (2026-06-23)

After Wave 20, **every in-scope command that the differential oracle can verify is
implemented** (engine 181 cmds, oracle 2007 fixtures / 1989 pass / 0 diverge / 18
known-unsupported, 56 cargo tests). The remaining **19 in-scope-missing are genuine
deferrals**, each blocked by a concrete reason — NOT by effort. Run
`bash harness/oracle/valdr-surface-gap.sh` to see them; categorized:

- **Blocking (8)** — BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH, BZPOPMIN, BZPOPMAX,
  BZMPOP. A request/response Durable Object can't block a client across the event
  loop; these need a different execution model.
- **Random / needs host RNG (5)** — SPOP, SRANDMEMBER, HRANDFIELD, ZRANDMEMBER,
  RANDOMKEY. NoopHost has no RNG and a random pick can't match valkey's RNG; only
  trivial cases (singleton / count≥card / empty) are differentially testable.
  Implement once a host-RNG-backed test harness exists.
- **Clock-dependent (3)** — XCLAIM, XAUTOCLAIM (consumer idle time), TIME (wall clock).
- **DUMP / RESTORE (2)** — require byte-identical RDB serialization (the full RDB
  encoder/CRC); large faithful port, separate effort.
- **OBJECT (1)** — ENCODING/etc. expose listpack/skiplist/intset encodings the engine
  doesn't model.

## Log (newest first)
- 2026-06-23 — Waves 22-24 landed: DUMP/RESTORE aggregate byte-parity — list/zset/
  intset (`d8a32c7`), then hash insertion-order fidelity + hash DUMP via IndexMap
  (`ae460f8`, tightened 4 HGETALL/HKEYS/HVALS fixtures set_equal→exact), then set
  insertion-order + non-int-set DUMP via IndexSet (`810c10f`, SMEMBERS tightened to
  exact with a faithful intset↔listpack encoding state machine). Oracle 2093/0/18,
  109 cargo tests. DUMP now covers strings + list/zset/set/hash (all non-stream,
  non-field-TTL types) byte-identically. Hash+Set now return valkey-EXACT order
  (was sorted — a real fidelity win). Deferred: stream DUMP, hash-with-field-TTL
  DUMP (valkey uses a different RDB_TYPE_HASH_2 encoding), large-collection
  encodings (hashtable/skiplist; fixtures stay small).
- 2026-06-23 — Wave 21 landed (`cf24ca8`): DUMP/RESTORE for STRING values with
  full byte-parity incl. LZF-compressed long strings (RDB ver 80, CRC64-Jones;
  validated vs 2282 captured valkey dumps, 0 diverge). RESTORE via 21 cargo unit
  tests (binary input can't ride JSON fixtures) incl. hardcoded real-valkey dumps.
  Oracle 2029/0/18, 77 cargo tests. Aggregate-type DUMP deferred (next: the
  order-preserving subset list/zset/intset; hash + non-int-set blocked by the
  engine's HashMap losing valkey's insertion order — a structural limit).
- 2026-06-23 — Wave 20 landed (`09a498e`): SCAN/HSCAN/SSCAN/ZSCAN single-pass,
  unlocked by an additive `scan_reply` oracle mode (`5c33f81`, cursor exact +
  elements set_equal — verified inert before use). Oracle 1989/0/18 (crossed 2000
  fixtures). Engine 181 cmds. SORT+GEO+SCAN (the requested list) all done; only
  DUMP/RESTORE (full RDB byte parity) remains a large separate effort.
- 2026-06-23 — Waves 17-19 landed: SORT/SORT_RO (`47b2dee`), GEO full surface with
  byte-for-byte GEOPOS/GEODIST float parity (`ac93324`), EVAL_RO/EVALSHA_RO (faithful
  write-command predicate from valkey command flags) + DELIFEQ + MSETEX (`b2351c8`).
  Oracle 1959/0/20, 56 cargo tests. Engine 177 cmds. Differential-testable surface complete.
- 2026-06-23 — Waves 14-16 landed: BITFIELD/BITFIELD_RO/BITOP (`5f977af`),
  HyperLogLog PFADD/PFCOUNT/PFMERGE with full estimation parity 1000→1002
  (`238a8c0`), hash-field TTL HEXPIRE/HTTL/HGETEX/HGETDEL/HSETEX (Wave 16,
  fix-forward after a sub-agent 500: get_value field-purge bug + deterministic
  fixtures). Oracle 1717/0/20. Engine 161 cmds, gap 39. Learned: `ttl_band` is
  scalar-only (see playbook) — use absolute HEXPIRETIME for array TTL asserts.
- 2026-06-23 — Wave 13 landed (`c0c77be`): stream consumer groups (XGROUP/
  XREADGROUP/XACK/XPENDING summary/XINFO STREAM+GROUPS) + PEL in snapshot codec;
  fixed a latent Wave-11 trim bug. Oracle 1500/0/20, 42 cargo tests. Gap 57.
  Streams feature-complete (XCLAIM/XAUTOCLAIM/idle-fields deferred = clock).
- 2026-06-23 — Wave 12 landed (`450dab8`): +4 INCRBYFLOAT/HINCRBYFLOAT/KEYS/LCS
  (incl. LCS IDX-map parity; long-double float format probed to byte parity).
  Oracle 1418/0/20. Gap 62. Engine 138 cmds. **Phase 1 core feature-complete**:
  all data types + aggregates + transactions + streams core. Verified.
  Remaining 62 = consumer groups, HLL, bitfield/bitop, scan, hash-field-TTL,
  dump/restore, sort, geo + genuine deferrals (blocking/random/object/time).
- 2026-06-23 — Wave 11 landed (`c1ce5a6`): Stream value type (cross-cutting) +
  non-blocking core (XADD/XLEN/XRANGE/XREVRANGE/XDEL/XTRIM/XSETID/XREAD). Oracle
  1341/0/22, 41 cargo tests. Gap 66. Consumer groups + blocking deferred (Wave 11b).
- 2026-06-23 — Wave 10 landed (`3b78b5e`): TRANSACTIONS (MULTI/EXEC/DISCARD/WATCH/
  UNWATCH) + per-key WATCH/CAS versioning; intercept at execute() so scripts stay
  atomic; tx state excluded from snapshots. Oracle 1264/0/22, 40 cargo tests. Gap 74.
  The EdgeStash atomic-decision headline. Verified.
- 2026-06-23 — Wave 9 landed (`92045ab`): +4 list completion (RPOPLPUSH/LMOVE/LMPOP/
  LPOS); blocking variants deferred. Oracle 1214/0/22. Gap 79.
- 2026-06-23 — Wave 8 landed (`ce13532`): +9 zset aggregates (ZUNION/ZINTER/ZDIFF/
  +STORE/ZRANGESTORE/ZINTERCARD/ZMPOP, WEIGHTS+AGGREGATE). Oracle 1135/0/17. Gap 83.
  Verified. Engine 117 cmds.
- 2026-06-23 — Wave 7 landed (`b266130`): +11 string-range + bitmap (GETRANGE/
  SUBSTR/SETRANGE/MSET/MSETNX/PSETEX/GETEX/SETBIT/GETBIT/BITCOUNT/BITPOS). Oracle
  1036/0/17 (crossed 1000 fixtures). Gap 92. Disk recovered to 18G. Next: aggregates.
- 2026-06-22 — Wave 6 landed (`d356364`): +12 zset single-key (ZPOPMIN/ZPOPMAX/
  ZMSCORE/ZCOUNT/ZLEXCOUNT/ZRANGEBYLEX/ZREVRANGE/ZREVRANGEBYSCORE/ZREVRANGEBYLEX/
  ZREMRANGEBY{RANK,SCORE,LEX}). Oracle 883/0/12. Gap 103. Verified. Disk dipped to
  1.3G → freed lua-rs-port/target (3.3G) → 4.6G. Aggregates deferred to Wave 6b.
- 2026-06-22 — Wave 5 landed (`f9d6e3f`): Set value type (cross-cutting) + 14
  commands (SADD/SREM/SCARD/SISMEMBER/SMISMEMBER/SMEMBERS/SMOVE/SINTER/SUNION/
  SDIFF/SINTERCARD/+STORE; SPOP/SRANDMEMBER/SSCAN deferred — RNG/cursor). Oracle
  785/0/8. Gap 115. Verified independently. NOTE: host hit ENOSPC (disk 100%)
  mid-wave — recovered to ~3.1G by clearing stale scratch-worktree targets; the
  57G `nginx-rs-port/target` is the big reclaim if more is needed (user's call).
- 2026-06-22 — Wave 4 landed (`525d1fb`): List value type (cross-cutting) + 13
  commands (LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX/LSET/LPUSHX/RPUSHX/LINSERT/
  LREM/LTRIM; LPOS deferred). Oracle 696/0/3. Gap 142→129. New snapshot-list-order
  test. Verified independently.
- 2026-06-22 — Wave 3 landed (`16d4c90`): +14 keyspace/generic/connection
  (PERSIST/EXPIREAT/EXPIRETIME/EXPIRE-opts/TYPE/RENAME/RENAMENX/COPY/TOUCH/UNLINK/
  PING/ECHO/FLUSHALL). Oracle 608/0/3. Original 19 known-unsupported → 3 genuine
  deferrals (OBJECT ENCODING, TIME, RANDOMKEY). Gap 156→142. Verified independently.
- 2026-06-22 — Wave 2 landed (`9c361b5`): +9 hash commands. Oracle 473/0/8
  (closed ku-hexists/hlen/hmget). Gap 165→156. Deferred HINCRBYFLOAT (float fmt),
  HRANDFIELD (RNG). Verified independently.
- 2026-06-22 — Wave 1 landed (`3f3df9c`): +8 string commands + SET KEEPTTL/EXAT/PXAT.
  Oracle 430/0/11 (closed 8 known-unsupported). Gap 173→165 in-scope missing.
- 2026-06-22 — Campaign opened. Baseline 382/0/19, 27/200 in-scope. Playbook +
  gap script + this backlog created. Wave 1 started.

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

### Wave 11 — Streams  `[ ]`  (cross-cutting: new `StoredValue::Stream`; large)
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

## Phase 2 — EdgeStash cold-start  `[ ]`  (needs live measurement → interactive)
Named open target: ~0.5s cold Durable Object start (new DO + snapshot restore +
wasm init). Branch `perf/cold-start-eager-jemalloc` has warmup/jemalloc toggles.
Overnight-safe prep only: identify wasm-init + storage.list() cold-load cost,
stage candidate optimizations + a measurement script. **Do not deploy
unattended** — gate real numbers on an interactive `wrangler deploy` + latency run.
See `harness/` memory `cloudflare-deploy-blocker` for the deploy mechanism.

## Phase 3 — Single-node sub-parity perf  `[ ]`  (bench is noisy → confirm interactively)
Sub-parity commands: RPUSH 0.89×, RPOP 0.95×, XADD 0.81×, FCALL 0.71×.
FCALL = Lua-VM per-call overhead (roadmap names it the last sub-parity command).
Prep: profile hotpaths, stage fixes; gate any claim on a clean interactive bench re-run.

---

## Phase 4 — Large WIP to return to after Valdr (per `docs/roadmap.md` + lane branches)

- **Replication out of alpha** — prove PSYNC partial-resync instead of always
  full re-sync. Unmerged lanes: `lane/replication-burn-down`, `lane/repl-observability`
  (155 commits ahead of `main`, last touched early June). Docs:
  `docs/REPL_OBSERVABILITY_OVERNIGHT_PLAN.md`, `docs/REPLICATION_INTEGRATION_DASHBOARD.md`.
  First step: reconcile those lanes vs current `main` (EdgeStash consolidation moved main).
- **HA / Cluster** — `docs/HA_CLUSTER_REPLICATION_ROADMAP.md` is the execution queue.
- **mlua → lua-rs migration** — `docs/MLUA_EXIT_PLAN.md`. Removes the last embedded
  C dependency in the data path and the 3 `unsafe` blocks in `eval.rs`; gated on
  Lua 5.1 compat (number model, setfenv/getfenv) in the sibling `lua-rs-port`.
- **`#![forbid(unsafe_code)]`** on the zero-unsafe data crates once mlua is gone.
- **Bigger bets** — I/O threads (Valkey-style), then sharded execution; compact
  encodings (intset/listpack/skiplist) — also applicable to the edge engine.
- **C ABI decision** — support unsafe C modules vs full pure-Rust alternatives.

---

## Log (newest first)
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

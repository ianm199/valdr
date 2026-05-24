# Persistence Frontier — 2026-05-24

Scout map of what remains before AOF is as **boring and credible as RDB**.
Supersedes the gap list in `PERSISTENCE_BORING_SPEC_20260523.md`, several items
of which are now fixed (noted below).

## How to run the scout

Self-contained — each scenario forks its own server in a private temp dir, so
it does **not** share `reference/valkey/tests/tmp/` and never races the breadth
runner.

```bash
python3 harness/oracle/persistence-frontier.py --skip-build        # 26 focused scenarios
python3 harness/oracle/persistence-cycle.py --mode rdb  --skip-build   # real process restart round-trip
python3 harness/oracle/persistence-cycle.py --mode aof  --skip-build
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite --skip-build
```

Current status (2026-05-24): **persistence-frontier 26/26 pass**,
**persistence-cycle rdb/aof/aof-rewrite all pass**.

## What is verified boring today

The scouts already exercise, end to end, and pass:

- **RDB**: real restart round-trip (cycle), `DEBUG RELOAD`, expires survive
  reload, **corrupt RDB → fatal startup** (new), **missing RDB → empty startup**
  (new), **BGSAVE forks a COW child, writes `dump.rdb`, reports ok status**
  (new).
- **AOF**: real restart round-trip (cycle), `DEBUG LOADAOF`, expires survive,
  load-truncated yes/no, unknown-command-fails, GETEX-does-not-append, SPOP /
  LMPOP / ZMPOP replay, `BGREWRITEAOF` digest round-trip, and the full
  multi-part manifest family (basic load, missing/duplicate/unknown-type/
  non-monotonic/blank-line/empty-file failures, discontinuous + empty incr
  loads, appendonly-enable layout, rewrite sequence advance).

### Fixed since the 05-23 spec
- Corrupt/unopenable RDB startup is now **fatal** (spec gap #1 — verified by
  `rdb-corrupt-file-fatal-startup`).
- BGSAVE genuinely **forks** (`libc::fork()` COW snapshot, tracked via
  `rdb_child_pid`; `_exit` in child) — not synchronous.
- Multi-part AOF (`appendonlydir` + manifest + BASE/INCR + rewrite seq) works
  (spec gap #7).
- `DEBUG RELOAD` / `DEBUG LOADAOF` actually reload (spec gap #4).

## Integration-test frontier classification

Both `integration/aof.tcl` (736 lines) and `integration/rdb.tcl` (598 lines)
are tagged `external:skip`, so a generic survey reports nothing. The scouts
source-shape the runnable cases. The remaining upstream cases, by blocker:

### A. Missing command / subcommand behavior

| Test (file:line) | Gap |
|---|---|
| `bgsave cancel aborts save`, `bgsave cancel schedulled request` (rdb.tcl:267,299) | `BGSAVE CANCEL` is unimplemented — it returns the normal `Background saving started` and does not kill the child. |
| `bgsave resets the change counter` (rdb.tcl:256) | `rdb_changes_since_last_save` is **always 0** — the server `dirty` counter is not tracked/reported, so there is nothing to reset. |
| `failed bgsave prevents writes` (rdb.tcl:522) | No `stop-writes-on-bgsave-error` gate; depends on the dirty counter + last-bgsave-status feeding a write deny path. |
| `Generate / load / truncate-to-timestamp AOF annotations` (aof.tcl:362-433) | AOF timestamp annotations (`#TS:` records) are not emitted or parsed. |

### B. Missing process / fork behavior

| Test (file:line) | Gap |
|---|---|
| `Test FLUSHALL aborts bgsave` (rdb.tcl:235) | FLUSHALL does not kill an in-progress BGSAVE child (the fork exists; the abort wiring does not). |
| `client freed during loading` (rdb.tcl:326) | No incremental loading event loop (`loading-process-events-interval-bytes`); load is a single blocking pass, so a client cannot be served/freed mid-load. |
| `Test child sending info` (rdb.tcl:399) | The BGSAVE child does not report COW/progress info back to the parent over a pipe. |
| `AOF fsync always barrier issue` (aof.tcl:214) | Needs the fsync-barrier ordering guarantee around the fork; likely `needs:debug`. |

### C. Wrong / missing file-format behavior

| Test (file:line) | Gap |
|---|---|
| `test old version rdb file`, `RDB encoding loading test` (rdb.tcl:25,49) | Needs corpus assets (`list-quicklist.rdb`, `encodings.rdb`) + loaders for legacy encodings. |
| `RDB future/foreign version loading, strict + relaxed` (rdb.tcl:56-111) | RDB version-check policy (strict reject vs relaxed accept) and unknown-type handling under relaxed mode. |
| `Test RDB stream encoding [- sanitize dump]` (rdb.tcl:128,146) | Stream object RDB encoding + sanitize-dump validation. |
| `RDB Load from incompatible version preserves data` (rdb.tcl:571) | Cross-version load that keeps data. |
| `script won't load anymore if it's in rdb` (rdb.tcl:517) | Lua script aux field in RDB. |

### D. Missing utility binary (largest single blocker)

| Test (file:line) | Gap |
|---|---|
| `valkey-check-aof` family (aof.tcl:110-135, 474-614) | No `valkey-check-aof` binary: confirm-invalid, show-abnormal-line, fix, and per-format checks (resp / rdb-preamble / multipart, format-error, truncate-to-timestamp). ~9 tests. |
| `valkey-check-rdb` (integration/valkey-check-rdb.tcl) | No `valkey-check-rdb` binary. |

### E. Replication / scripting coupling (out of single-node persistence scope)

| Test (file:line) | Gap |
|---|---|
| `EVAL timeout with slow verbatim Lua script from AOF` (aof.tcl:435) | scripting timeout during AOF load. |
| `EVAL can process writes from AOF in read-only replicas` (aof.tcl:459) | replica + AOF. |

### Already covered (no longer frontier)

`load-truncated` (aof.tcl:26-64,135-152), SPOP/EXPIRE/LMPOP/ZMPOP replay
(aof.tcl:155-360), GETEX-no-append (aof.tcl:245), empty/missing/corrupt RDB
startup (rdb.tcl:113-126,199-233), `DEL`→`EXPIREAT` for `EXPIRE -1`
(aof.tcl:207) — all green via the scouts.

## Success condition: what remains before AOF ≈ RDB credibility

RDB is credible today: byte-level oracle + restart cycle + corrupt-fatal +
BGSAVE-fork + status reporting all pass. **AOF reaches the same bar once these
land** (none requires the replication or utility-binary work, which is a
separate, larger track):

1. **Server dirty-counter spine** (unblocks A: `rdb_changes_since_last_save`,
   `bgsave resets change counter`, `failed bgsave prevents writes`). This is the
   single highest-leverage fix — it is shared by RDB and AOF credibility and is
   contained in `redis-core` + `info.rs`.
2. **BGSAVE CANCEL + FLUSHALL-aborts-bgsave** (A/B): kill the tracked
   `rdb_child_pid`, set `rdb_last_bgsave_status:err`/cancelled. The fork plumbing
   already exists.
3. **AOF timestamp annotations** (A): emit `#TS:` on rewrite/append and parse
   them on load; prerequisite for `truncate-to-timestamp`.

The bigger, separable tracks (not required for "boring AOF v1"):
- **`valkey-check-aof` / `valkey-check-rdb` utility binaries** (D) — the largest
  raw test count, but standalone tools, not server behavior.
- **RDB version/encoding compatibility** + corpus assets (C).
- **Incremental loading event loop** (B: `client freed during loading`, child
  info pipe).
- **AOF + scripting/replication** (E).

## Recommended next packet

`persistence-dirty-counter-spine-v1`: track `server.dirty` on every write,
reset on SAVE/BGSAVE success, surface it in `INFO persistence`
(`rdb_changes_since_last_save`), and gate writes on
`stop-writes-on-bgsave-error` when the last bgsave failed. It unblocks three
`rdb.tcl` tests directly and is the foundation the BGSAVE-CANCEL and
failed-bgsave behaviors build on. Add matching scout scenarios
(`rdb-changes-counter-tracks-and-resets`, `failed-bgsave-prevents-writes`) once
the behavior exists.

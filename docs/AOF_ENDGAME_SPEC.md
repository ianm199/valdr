# AOF Endgame Spec — from alpha persistence to release-grade durability

**Status:** implementation checkpoint / endgame roadmap. Packet G added
`appendfsync always` flush-boundary batching. Packet H moved multipart
`BGREWRITEAOF` to a two-phase, Valkey-shaped flow. Packet I hardens multipart
AOF publication with temp-file fsync plus directory fsync where available, adds
crash-window frontier coverage, and extends rewrite latency telemetry across
dataset sizes. Packet J adds successful-rewrite history cleanup, introduces the
`KeyspaceSnapshot` facade, and records large rewrite-start telemetry. Packet K
lands segmented-COW `KeyspaceMap` phase one, cutting rewrite-start snapshot
capture to root-clone scale while keeping AOF/RDB consumers on the same facade.
Packet L adds syscall-level AOF rewrite fault injection around BASE/manifest
sync and rename boundaries, then gates those failures through restart checks.
Packet M adds held-snapshot keyspace COW telemetry, samples it during
`BGREWRITEAOF`, and extends the reusable toy model with metadata/payload split
variants. Packet N adds conservative startup appenddir cleanup, hardens
multipart `valkey-check-aof`, expands the source-shaped persistence frontier to
cleanup/checker states, and turns focused upstream persistence TCL aborts into
parsed follow-up evidence.
**Date:** 2026-06-03.
**Scope:** Append-Only File persistence, replay, rewrite, manifests, durability
signals, and performance gates.

---

## 1. Ambitious End Goal

Make AOF a release-grade single-node durability feature rather than an alpha
surface.

The target state:

- `appendonly yes` is safe to recommend for single-node durability.
- Startup replays both legacy single-file AOF and Valkey-style multi-part AOF.
- Multi-part AOF manifest handling is strict enough to fail closed on corrupt
  state and permissive enough to load valid Valkey layouts.
- `BGREWRITEAOF` never loses acknowledged writes, even under concurrent write
  load, and does not rename over the active append stream.
- `appendfsync no/everysec/always` have measurable, documented latency and data
  loss behavior.
- `INFO persistence`, `CONFIG SET appendonly`, `WAITAOF`, and failure states
  report truthfully.
- AOF is covered by a fast in-memory kit, process-level restart cycles,
  source-shaped persistence frontier scenarios, and focused upstream TCL files.
- AOF overhead is measured against `appendonly no` and reference Valkey before
  it becomes a public performance claim.

The product-level goal is not "we have an AOF file." It is: Valdr can run a
write workload, acknowledge commands, crash/restart, rewrite the AOF, and
recover the same logical dataset with a bounded and measured durability window.

---

## 2. What AOF Is

AOF is a command log. Every write command that changes the dataset is serialized
as RESP and appended to disk. Restart replays those commands to reconstruct the
keyspace.

RDB is a point-in-time binary snapshot. AOF is a history of mutations. RDB is
compact and fast to load; AOF can reduce data loss because it records writes
between snapshots. Modern Valkey combines the two ideas with multi-part AOF:

- **BASE file:** compact representation of the dataset at rewrite time. This can
  be an AOF command stream or an RDB preamble.
- **INCR file:** append-only stream of writes after the BASE was cut.
- **Manifest:** text file naming the valid BASE plus ordered INCR files.

The correctness rule is simple and brutal: an acknowledged write must either be
in the active append stream, be included in the rewritten BASE, or be in a
surviving INCR file named by the manifest. There cannot be a gap.

---

## 3. Current Repo State

This is not blank slate. AOF already exists across the repo:

- CLI/config:
  - `crates/redis-server/src/cli.rs`
  - `crates/redis-server/src/main.rs`
  - `crates/redis-commands/src/config_cmd.rs`
- Append writer, replay, manifest parsing, rewrite helpers:
  - `crates/redis-commands/src/aof.rs`
- Command propagation into AOF:
  - `crates/redis-commands/src/dispatch.rs`
  - `crates/redis-commands/src/multi.rs`
- `BGREWRITEAOF` command:
  - `crates/redis-commands/src/persist.rs`
- `INFO persistence` state:
  - `crates/redis-core/src/persistence.rs`
  - `crates/redis-commands/src/info.rs`
- WAITAOF hooks:
  - `crates/redis-commands/src/replication.rs`
  - `crates/redis-core/src/blocked_keys.rs`
- Check utility:
  - `crates/redis-server/src/check_aof.rs`
- Fast AOF kit:
  - `crates/redis-commands/tests/aof_correctness_kit.rs`
- Process/oracle runners:
  - `harness/oracle/persistence-cycle.py`
  - `harness/oracle/persistence-frontier.py`
  - `harness/runners.toml`

Current labels still say alpha:

- `README.md`: AOF is alpha.
- `docs/coverage.md`: `unit/aofrw` is outside the gated core.
- `docs/TEST_AND_FEATURE_COVERAGE.md`: AOF is not release-gated.

### Important Current Behaviors

- `AofWriter` writes RESP command arrays and tracks selected DB.
- `appendfsync always` now has a RuntimeOwner flush-boundary staging path:
  propagated frames are staged for a client-slot drain and `sync_data()` runs
  before replies become observable. Direct helper calls still flush/sync in the
  append path.
- `appendfsync everysec` relies on a background `aof-fsync` thread.
- Startup can load a manifest layout or fall back to legacy `appendonly.aof`.
- Manifest BASE `.rdb` loads through the RDB loader with `aof_preamble = true`.
- Legacy single-file AOF still rejects RDB preambles even if
  `aof-use-rdb-preamble yes`; this is a known scope edge.
- `BGREWRITEAOF` now follows the important Valkey parent-side ordering: flush
  staged AOF bytes, deep-snapshot the DBs, open a fresh INCR, persist a
  preliminary manifest that keeps the old chain plus the new active INCR,
  install the new writer, and return while a background thread writes the BASE
  and publishes the final manifest.
- Manifest publication now mirrors Valkey's durability ordering more closely:
  write temp manifest, fsync it, rename it, then fsync the AOF directory where
  the platform supports directory fsync. Rewritten BASE files are fsynced before
  final manifest publication, and the BASE rename is followed by an AOF
  directory fsync.
- Successful multipart rewrite now publishes a replayable final manifest that
  temporarily marks superseded BASE/INCR files as history, deletes those history
  files only after durable final publication, and then best-effort compacts the
  manifest back to the live BASE plus current INCR. Failed rewrites leave the
  preliminary manifest and referenced files replayable.
- Rewrite is no longer fully synchronous. BASE generation is off the command
  path, and writes acknowledged during the rewrite window append to the new
  INCR. `KeyspaceSnapshot` centralizes the snapshot contract, and
  `snapshot_all_dbs()` now captures segmented-COW `KeyspaceMap` roots instead
  of deep-cloning every key/value on the owner thread. Saver-side
  materialization still happens behind the facade; value-payload sharing is not
  part of this phase.
- `INFO persistence` includes `aof_last_rewrite_snapshot_keys` and
  `aof_last_rewrite_snapshot_us` so rewrite-start cost is visible without
  re-running the benchmark harness.
- `CONFIG SET appendonly yes` creates the manifest/current-INCR layout and
  reports completed AOF sizes without a fake long-running rewrite window.

### Fresh Local Signal

Implementation checkpoint on 2026-06-02:

- The stale disk-full AOF kit failure was repaired by routing ordinary and
  transaction append outcomes through `record_aof_append_result`.
- `cargo test -p redis-commands --test aof_correctness_kit` passes 11/11 after
  Packet G added appendfsync-always batching, transaction-envelope batching,
  and lifecycle-barrier batching tests.
- `cargo test -p redis-commands --test repl_correctness_kit` passes 13/13 after
  the role-change-in-MULTI guard was made to model the production EXEC drain
  marker instead of top-level dispatch, and after adding fast WAITAOF guards for
  local fsync progress, appendonly-disabled rejection, and appendonly-disabled
  waiter unblocking.
- `cargo test -p redis-server` passes 8/8 after the RuntimeOwner AOF
  flush-boundary integration.
- `INFO persistence` now reports Valkey-shaped AOF sizes: `aof_current_size`
  includes BASE plus INCR bytes, not only the active INCR file, and
  `CONFIG SET appendonly yes` no longer leaves a fake rewrite-in-progress state
  after immediate BASE/INCR layout creation has completed.
- Packet H reruns:
  - `python3 harness/oracle/persistence-cycle.py --mode aof` passes
    (`20260602T181421Z`).
  - `python3 harness/oracle/persistence-cycle.py --mode aof-rewrite` passes
    (`20260602T181424Z`).
  - `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure`
    passes 27/27 scenarios (`20260602T181429Z`), including
    `multipart-aof-rewrite-window-survives-restart`.
  - The new rewrite-window scenario writes before/during/after
    `BGREWRITEAOF`, restarts from multipart AOF, and verifies every
    acknowledged key survived. Its observed final manifest names
    `appendonly.aof.2.base.rdb` plus `appendonly.aof.2.incr.aof`.
- `python3 harness/oracle/tcl-survey.py --runner-id aof-waitaof-focused ...`
  completed as telemetry with parsed summaries: `unit/wait` had 0 runnable tests
  under the current deny tags, and `unit/scripting` was 402/406 with the four
  failures in the known Lua error-prefix case rather than WAITAOF
  (`20260602T135621278962Z`).
- `python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused ...`
  completed as telemetry but still produced no parsed summaries for all four
  focused files, with `integration/rdb` timing out. Until that upstream harness
  surface is triaged, the source-shaped persistence frontier is the stronger
  release gate (`20260602T135646869135Z`).
- `python3 harness/bench/aof-matrix.py --quick` passes on a rebuilt release
  binary and records append/fsync overhead under
  `harness/bench/results/20260602T140115Z-6d52e9d-aof-matrix.*`.
- Packet G quick Rust rerun:
  `python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build`
  passes on a rebuilt release binary and records
  `harness/bench/results/20260602T171157Z-6d52e9d-aof-matrix.*`.
  Pipeline-16 `appendfsync always` moved from 243.072 to 3533.595 rps for SET
  and from 222.573 to 3786.690 rps for INCR. Latest reference quick telemetry
  under `harness/bench/results/20260602T142918Z-6d52e9d-aof-matrix.*` reports
  7467.443 rps for SET p16 always and 6367.804 rps for INCR p16 always. This
  confirms the flush-boundary model is real, but p1 remains a
  per-command-fsync workload.
- `python3 harness/bench/aof-rewrite-latency.py --quick --targets rust --skip-build`
  passes and records
  `harness/bench/results/20260602T181439Z-6d52e9d-aof-rewrite-latency.*`.
  The runner now separates `rewrite_command_wall_ms` from the actual
  rewrite-in-progress window. Packet H measured a 0.642 ms command round trip,
  a 12.461 ms rewrite window, during-write p99 0.069 ms, during-write p100
  0.637 ms, 400/400 acknowledged writes, and restart verification passed.
- `python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build`
  passes under
  `harness/bench/results/20260602T181027Z-6d52e9d-aof-matrix.*`. That full run
  had one noisy `INCR`/p16/`always` row at 1108.761 rps; a targeted rerun under
  `harness/bench/results/20260602T181116Z-6d52e9d-aof-matrix.*` measured
  3883.315 rps and p99 1.066 ms for the same cell, matching the Packet G shape.
- Packet I targeted frontier:
  `python3 harness/oracle/persistence-frontier.py --fail-on-failure --scenarios ...`
  passes 5/5 crash-window scenarios (`20260602T190040Z`). Covered states:
  preliminary manifest old chain plus new INCR; temp BASE ignored before final
  manifest; final BASE ignored before manifest; live failed rewrite remains
  replayable and reports `aof_last_bgrewrite_status:err`; corrupt final BASE
  fails closed at startup.
- Packet I full frontier:
  `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure`
  passes 32/32 scenarios (`20260602T190420Z`).
- Packet I scaling telemetry:
  `python3 harness/bench/aof-rewrite-latency.py --targets rust --skip-build --dataset-sizes 250,1000,5000 ...`
  records `harness/bench/results/20260602T190216Z-6d52e9d-aof-rewrite-latency.*`.
  Command-wall/start-block times were 8.502 ms, 9.968 ms, and 10.666 ms for
  dataset sizes 250, 1000, and 5000. Post-reply rewrite windows were 33.158 ms,
  36.824 ms, and 32.087 ms. RSS peaks were 7968 KiB, 9232 KiB, and 16832 KiB.
  Restart verification passed for all three rows.
- Packet I quick rewrite telemetry:
  `harness/bench/results/20260602T190441Z-6d52e9d-aof-rewrite-latency.*`
  measured command/start block 8.816 ms, post-reply rewrite 33.044 ms,
  rewrite wall 41.860 ms, during-write p99 0.111 ms, and restart passed.
- Packet I quick matrix:
  `harness/bench/results/20260602T190447Z-6d52e9d-aof-matrix.*` passed 16/16.
  Pipeline-16 `appendfsync always` was 3048.385 rps for SET and 3662.915 rps
  for INCR.
- Packet J targeted history frontier:
  `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure --scenarios multipart-aof-rewrite-success-deletes-history,multipart-aof-rewrite-failure-preserves-history-files`
  passes 2/2 (`20260602T195818Z`). Successful rewrite deletes superseded
  `1.base/1.incr` files after publishing `2.base/2.incr`; failed rewrite keeps
  every manifest-referenced file and restart preserves acknowledged writes.
- Packet J full frontier:
  `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure`
  passes 34/34 (`20260602T200700Z`).
- Packet J required tests and process gates:
  `aof_correctness_kit` 11/11 pass, `repl_correctness_kit` 13/13 pass,
  `redis-server` 8/8 pass, `persistence-cycle --mode aof` pass
  (`20260602T200654Z`), `persistence-cycle --mode aof-rewrite` pass
  (`20260602T200657Z`), and `cargo build --release -p redis-server` pass.
- Packet J large rewrite telemetry:
  `harness/bench/results/20260602T200339Z-1a9d679-aof-rewrite-latency.*`
  passes for datasets 5000, 25000, and 100000. Snapshot capture was
  1179 us / 7464 keys, 4503 us / 27896 keys, and 19319 us / 102735 keys.
  Command/start block was 10.458 ms, 16.318 ms, and 27.943 ms; post-reply
  rewrite wall was 58.018 ms, 60.007 ms, and 129.428 ms. Restart verification
  passed for all three rows.
- Packet J quick matrix:
  `harness/bench/results/20260602T200407Z-1a9d679-aof-matrix.*` passes 16/16.
  `appendonly` remains off by default. On this local quick run, `everysec` SET
  p16 was 219106.047 rps and `always` SET p16 was 3332.404 rps; `always`
  remains a deliberate fsync-heavy mode, not a default throughput path.
- Packet K structural-sharing model:
  `harness/models/keyspace-cow-model/results/keys100k-v64-fnv-incr-rss.tsv`
  and `harness/models/keyspace-cow-model/results/keys1m-v64-fnv-incr-rss.tsv`
  refresh the segmented-COW/HAMT/deep comparison with INCR and RSS columns.
  Model tests pass 5/5.
- Packet K production rewrite telemetry:
  `harness/bench/results/20260602T210203Z-1a9d679-aof-rewrite-latency.*`
  passes for datasets 5000, 25000, and 100000. Snapshot capture is 55 us,
  99 us, and 97 us respectively; command/start block is 10.058 ms, 9.114 ms,
  and 9.778 ms; restart verification passes for all three rows.
- Packet K throughput telemetry:
  `harness/bench/results/20260602T210228Z-1a9d679-profile-matrix.tsv` passes
  with median 1.02x, min 0.76x, max 1.35x, p1 `GET` 0.86x, p16 `GET` 0.99x,
  and p100 `GET` 1.17x. Focused ordered probes under
  `harness/bench/results/20260602T210252Z-1a9d679-default-suite-parts.*` and
  `harness/bench/results/20260602T210259Z-1a9d679-default-suite-parts.*`
  show p1 `SET`/`GET`/`INCR` at 1.011x/0.950x/1.032x and p16
  `SET`/`GET`/`INCR` at 1.323x/1.022x/0.950x.
- Packet L targeted syscall-fault frontier:
  `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure --scenarios ...`
  passes 6/6 (`20260602T205624Z`). Covered production hook points:
  preliminary manifest before rename; BASE before rename; BASE after rename
  before directory fsync; final manifest before fsync; final manifest before
  rename; final manifest after rename before directory fsync. Every scenario
  restarts and verifies acknowledged writes survived.
- Packet L final gates:
  `persistence-cycle --mode rdb` passes (`20260602T210155Z`),
  `persistence-cycle --mode aof` passes (`20260602T205929Z`),
  `persistence-cycle --mode aof-rewrite` passes (`20260602T205934Z`), full
  `persistence-frontier.py --skip-build --fail-on-failure` passes 40/40
  (`20260602T205938Z`), and quick Rust AOF matrix
  `harness/bench/results/20260602T210305Z-1a9d679-aof-matrix.*` passes 16/16.
- `harness/models/aof-batch-model` captures the first-principles
  `appendfsync always` finding: on this host, syncing every 16 commands instead
  of every command improved the toy model from roughly 237-240 commands/s to
  roughly 3.4k-3.8k commands/s. This points at an AOF staging/flush-boundary
  packet rather than command-encoder micro-optimizations.

Older loop telemetry under `harness/evidence/runs/` shows an AOF manifest wave
that reached:

- `persistence frontier: 23/23 scenarios passing`
- `persistence aof-rewrite cycle: pass`

Those evidence files were captured with dirty-tree context, so they are useful
as history and scenario inventory, not as current release proof.

---

## 4. Non-Negotiable Invariants

### 4.1 No acknowledged write loss

For every successful mutating command:

- If AOF is enabled, either the append succeeds or the server records an AOF
  write error truthfully.
- During rewrite, every write after the BASE cut is appended to the active INCR.
- Final manifest publication must never name a BASE while dropping the INCR that
  contains post-BASE acknowledged writes.

### 4.2 Strict replay

Replay must fail closed for corrupt input unless the configured mode explicitly
allows a truncated final command.

Expected behavior:

- `aof-load-truncated yes`: replay valid prefix, ignore incomplete final tail.
- `aof-load-truncated no`: startup fails on incomplete final command.
- Unknown command in AOF: startup fails.
- Invalid manifest: startup fails.
- Missing manifest target file: startup fails.
- Empty AOF dir with appendonly enabled: create a valid initial layout.

### 4.3 Propagation fidelity

AOF must record the command Valkey would propagate, not necessarily the literal
client argv.

Examples:

- `EXPIRE` family may propagate as `PEXPIREAT` or deletion.
- Commands that do not mutate must not append.
- Commands that mutate conditionally must append only on successful mutation.
- `MULTI`/`EXEC` must preserve transaction envelope semantics.
- Blocking wake commands must appear in causal order.
- Commands applied from AOF replay must not re-append to AOF.
- Commands applied from replication must not loop back into replication/AOF when
  marked as apply-only.

### 4.4 Rewrite digest parity

A rewritten AOF must recreate logical contents across supported object types:

- strings, including integer encodings after replay;
- lists;
- hashes;
- sets;
- sorted sets;
- streams and consumer-group metadata where supported;
- TTL presence and expiration times;
- multi-DB selection.

Unsupported or lossy types must be explicit, tested, and documented. Silent skip
of supported objects is not acceptable.

### 4.5 Truthful operator surface

These must mean what they say:

- `INFO persistence`:
  - `aof_enabled`
  - `aof_rewrite_in_progress`
  - `aof_rewrite_scheduled`
  - `aof_last_bgrewrite_status`
  - `aof_last_write_status`
  - `aof_current_size`
  - `aof_base_size`
- `CONFIG GET/SET appendonly`
- `CONFIG GET/SET appendfsync`
- `BGREWRITEAOF` replies and scheduling behavior
- `WAITAOF`
- `valkey-check-aof` / `redis-server` check-aof dispatch path

---

## 5. Architecture End State

### 5.1 AOF Writer

The AOF writer should become a clear durability component with explicit state:

```text
Disabled
  -> Enabled(CurrentIncr)
  -> Error(append/fsync failed)
  -> Rewriting(CurrentIncr + PendingBase)
  -> Enabled(NewBase + CurrentIncr)
```

Concrete properties:

- Owns the current INCR file handle.
- Tracks selected DB for emitted `SELECT`.
- Tracks pending bytes and fsync policy.
- Tracks the highest replication offset appended and highest offset fsynced.
- Updates `aof_last_write_status` on append and fsync failure.
- Can be replaced atomically during rewrite finalization without losing appends.

Open design question: whether the writer remains a global `Arc<AofWriter>` or is
routed through the RuntimeOwner. The ambitious end state should move toward
owner-owned persistence decisions, but that does not need to block correctness
work.

### 5.2 Manifest Model

Manifest parsing should be treated as a small formal language:

```text
file <filename> seq <positive-i64> type <b|i|h>
```

Rules:

- filenames are bare names, not paths;
- quoted and escaped names round-trip;
- at most one BASE;
- INCR sequence numbers are strictly increasing;
- invalid type fails startup;
- missing named files fail startup;
- empty manifest fails startup;
- comments are allowed only where Valkey allows them;
- blank lines fail if Valkey fails them.

The current implementation already has much of this. The spec-level goal is to
make these rules gated and documented as release behavior.

### 5.3 Replay Engine

Replay should use the real command dispatcher where possible, because AOF replay
is a compatibility surface. Hand-decoding a large command subset will drift.

Required controls:

- synthetic AOF client is authenticated and marked as AOF client;
- replay commands do not propagate to AOF or replicas;
- selected DB changes are honored;
- replay errors are fatal except allowed truncated final tail;
- startup loads all configured DBs, not only DB 0;
- command rewrites required for AOF are tested against replay.

### 5.4 Rewrite Engine

Packet H's two-phase rewrite is the current correctness base. The end state
should keep the background job shape while removing the remaining snapshot
clone and hardening filesystem publication:

```text
BGREWRITEAOF:
  1. snapshot keyspace at an exact instant;
  2. switch active appends to a new INCR;
  3. write temp BASE from snapshot;
  4. fsync temp BASE;
  5. rename BASE into appenddirname;
  6. persist manifest naming BASE + current INCR, plus superseded history;
  7. fsync manifest/dir where supported;
  8. clear rewrite state and update INFO stats;
  9. delete history files only after durable final publication;
 10. best-effort compact the manifest back to live BASE + current INCR.
```

This pairs naturally with the forkless snapshot work:

- Packet H/J first removed correctness ambiguity with the existing snapshot
  facade and background BASE generation.
- Packet K then moved rewrite-start capture to segmented-COW root cloning while
  keeping AOF/RDB callers on `KeyspaceSnapshot`.

Do not conflate those layers. Publication correctness, snapshot capture cost,
and saver-side serialization cost are separate gates.

### 5.5 RDB Preamble

End state:

- `aof-use-rdb-preamble yes` writes BASE `.rdb` during rewrite.
- Manifest replay loads `.rdb` BASE via the RDB loader, then INCR AOF files.
- Legacy single-file RDB-preamble AOF is either supported or explicitly
  documented as unsupported with a failing/skip gate. It should not be an
  accidental behavior.

### 5.6 Failure Handling

Failure behavior must be tested with injected or real filesystem faults:

- append ENOSPC;
- fsync error;
- manifest write error;
- temp BASE write error;
- crash before manifest rename;
- crash after BASE rename but before manifest persist;
- missing INCR;
- corrupted final INCR tail;
- chmod/read-only appenddir.

At minimum, the server must not report OK while silently losing acknowledged
writes.

---

## 6. Implementation Waves

### Wave 0 — Current-State Stabilization

Goal: make the evidence honest before changing behavior.

Work:

- Update `aof_correctness_kit` disk-full test to exercise current dispatch or a
  small production hook.
- Run the process AOF gates on clean `HEAD`.
- Record current pass/fail in a new evidence note.
- Do not call AOF release-grade until gates are clean on the current tree.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build
```

### Wave 1 — Append/Replay Contract

Goal: prove normal append and startup replay are correct.

Work:

- Lock propagation rules for common command families.
- Keep `MULTI`/`EXEC` AOF envelope tests green.
- Add red/green tests for any command that rewrites argv for propagation.
- Add explicit `aof-load-truncated yes/no` tests if current coverage is too
  process-only.
- Ensure startup errors are fatal for corrupt AOF.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
python3 harness/oracle/persistence-frontier.py --skip-build \
  --scenarios aof-load-truncated-yes-short-read,aof-load-truncated-no-fails,aof-unknown-command-fails-startup,getex-does-not-append-to-aof,aof-spop-count-replay,aof-lmpop-zmpop-replay \
  --fail-on-failure
```

### Wave 2 — Manifest Release Contract

Goal: make multi-part AOF layout a real compatibility surface.

Work:

- Freeze manifest grammar rules in tests.
- Verify load order: BASE then ordered INCR.
- Verify empty appenddir startup creates valid layout.
- Verify `CONFIG SET appendonly yes` creates manifest/current-INCR correctly.
- Ensure old legacy single-file fallback still works when no manifest exists.

Gates:

```bash
python3 harness/oracle/persistence-frontier.py --skip-build \
  --scenarios multipart-aof-manifest-basic-load,multipart-aof-manifest-missing-file-fails,multipart-aof-manifest-non-monotonic-incr-fails,multipart-aof-manifest-blank-line-fails,multipart-aof-manifest-empty-file-fails,multipart-aof-manifest-duplicate-base-fails,multipart-aof-manifest-unknown-type-fails,multipart-aof-empty-dir-startup,multipart-aof-manifest-discontinuous-incr-load,multipart-aof-manifest-empty-incr-load,multipart-aof-appendonly-enable-layout \
  --fail-on-failure
```

### Wave 3 — Rewrite Correctness Under Load

Goal: prove `BGREWRITEAOF` cannot lose acknowledged writes.

Work:

- Keep current-INCR switch before BASE write.
- Verify manifest finalization sequence advances.
- Verify rewrite digest across supported types.
- Add crash-window scenarios if possible.
- Separate rewrite correctness from snapshot/start-latency ambition.

Gates:

```bash
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build \
  --scenarios aof-rewrite-collections-digest,multipart-aof-rewrite-sequence-advance \
  --fail-on-failure
```

### Wave 4 — Background Rewrite / Forkless Snapshot

Goal: remove latency spikes without weakening correctness.

Work:

- Keep the real AOF rewrite job state introduced by Packet H.
- Keep BASE generation on a saver thread.
- Keep active writer on INCR during BASE write.
- Keep `KeyspaceSnapshot` as the only consumer contract.
- Keep segmented-COW capture gated by rewrite latency, GET/SET/INCR throughput,
  RSS, and persistence restart/frontier checks.
- Treat saver-side streaming and value-payload sharing as separate follow-up
  packets.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build
```

Performance gates begin here; see §8.

### Wave 5 — WAITAOF / Durability Acknowledgement

Goal: make local and replica AOF durability waits meaningful.

Work:

- Tie `fsynced_repl_offset` to append and fsync success.
- Wake `WAITAOF` waiters after everysec fsync.
- Ensure `appendonly no` unblocks local WAITAOF waiters with the correct error.
- Verify role changes unblock waiters.
- Keep scripting/function restrictions correct.

Gates:

```bash
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-commands --test aof_correctness_kit
python3 harness/oracle/tcl-survey.py --runner-id aof-waitaof-focused \
  --skip-build --timeout-s 180 --files unit/wait,unit/scripting
```

### Wave 6 — Upstream Persistence Surface

Goal: move AOF out of alpha docs.

Work:

- Run focused upstream persistence TCL files.
- Triage `unit/aofrw` no-summary into concrete failures.
- Decide which Valkey function/module rewrite cases are in scope.
- Update README/coverage only after evidence is clean.

Gates:

```bash
python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused \
  --skip-build --timeout-s 180 --no-default-deny-tags \
  --deny-tag needs:repl --deny-tag cluster \
  --files unit/other,unit/aofrw,integration/aof,integration/rdb
```

---

## 7. Correctness Kit Strategy

The AOF kit should be the fastest red/green loop. It should not try to prove
process lifecycle, but it should pin byte-level and command-level semantics.

Keep and expand:

- append -> replay for six plain types;
- fsync policies replay identically;
- truncated tail behavior;
- manifest grammar failures;
- manifest load order;
- `MULTI`/`EXEC` envelope replay;
- disk-full / append failure status;
- selected DB emission;
- propagation rewrites;
- RDB preamble BASE load, if feasible without starting a server.

The kit should avoid stale hand-copied production logic. If production behavior
needs injection, add narrow hooks or helper functions in production modules and
call them from the kit. A red test that reproduces an old control-flow snippet
after production has changed is noise.

---

## 8. Benchmark Plan

AOF performance needs its own benchmark packet. The existing profile matrix is
good for general command overhead, but it does not isolate AOF modes.

### 8.1 Benchmark Dimensions

Run each workload across:

- `appendonly no`
- `appendonly yes --appendfsync no`
- `appendonly yes --appendfsync everysec`
- `appendonly yes --appendfsync always`
- Valdr vs reference Valkey
- no rewrite vs rewrite in progress
- legacy single-file vs manifest current-INCR layout, if both remain supported

Workloads:

- `SET` fixed-size values, pipeline 1 and pipeline 16.
- `INCR` small command / small reply.
- `HSET` and `ZADD` medium command with several arguments.
- `RPUSH` batched list append.
- mixed write workload matching `persistence-cycle.py`.
- multi-DB writes to exercise `SELECT` insertion.
- `MULTI`/`EXEC` transaction writes.

Metrics:

- throughput;
- median, p95, p99, p100 latency;
- AOF bytes per command;
- `sync_data()` time for `always`;
- everysec fsync duration and jitter;
- pending bytes before fsync;
- rewrite duration;
- write latency during rewrite;
- startup replay time;
- recovered command count after crash tests;
- file size before and after rewrite;
- RSS during rewrite.

### 8.2 Runner: `bench-aof-matrix`

This benchmark runner keeps AOF overhead separate from the generic matrix.

Proposed command shape:

```bash
python3 harness/bench/aof-matrix.py \
  --commands set,incr,hset,zadd,rpush \
  --fsync-modes no,everysec,always \
  --pipelines 1,16 \
  --requests 50000 \
  --clients 50
```

Runner metadata:

```toml
[[runner]]
id = "bench-aof-matrix"
kind = "json_command"
surface = "performance"
method = "bench-load"
workload = "aof-fsync-and-append-overhead"
command = ["python3", "harness/bench/aof-matrix.py"]
timeout_s = 2400
resources = ["benchmark-host", "bench-results", "port:6379", "port:16379"]
claim_level = "telemetry"
capabilities = ["persistence-aof", "performance-aof"]
```

Acceptance is not "faster than Valkey" by default. Acceptance is:

- overhead relative to `appendonly no` is measured and stable;
- `everysec` does not introduce avoidable per-command fsync;
- `always` is slower for the obvious reason, not due to avoidable allocation or
  writer-lock pathologies;
- rewrite-in-progress latency is bounded and explained.

### 8.3 Runner: `bench-aof-rewrite-latency`

Purpose: isolate `BGREWRITEAOF` impact.

Scenario:

1. Load a dataset with strings, hashes, sets, zsets, lists, streams.
2. Start continuous write workload.
3. Trigger `BGREWRITEAOF`.
4. Continue writes through rewrite.
5. Stop, restart, verify digest.
6. Report write latency distribution before/during/after rewrite.

Metrics:

- rewrite wall time;
- max command latency while rewrite is active;
- p99 command latency while rewrite is active;
- final BASE size;
- active INCR size;
- manifest generation count;
- restart digest pass/fail.

This runner is `claim_level = "telemetry"` until repeated.

### 8.4 Proposed New Runner: `durability-crash-aof`

Purpose: measure actual data loss windows.

Scenario:

1. Start server with a selected fsync policy.
2. Write monotonic sequence keys or stream entries.
3. Kill the process with `SIGKILL` at controlled offsets.
4. Restart.
5. Measure highest recovered sequence and missing gaps.

Expected:

- `appendfsync always`: every acknowledged write before kill should recover, or
  any exception must be explained by OS/filesystem semantics.
- `appendfsync everysec`: recovered prefix may lag; lag should be bounded by the
  fsync cadence and measured.
- `appendfsync no`: OS decides; report only, do not claim a bound.

This is not a normal CI gate. It is a periodic evidence runner.

### 8.5 Profiling Follow-Up

If AOF overhead is high, use profiler runners in this order:

1. `bench-aof-matrix` to identify which mode/workload regressed.
2. `bench-profile-hotspots-smoke` if the issue appears in normal command path.
3. A new `bench-aof-hotspots` focused on append/encode/fsync if needed.
4. `bench-profile-calltree` only after a stable regression is measured.

Likely bottleneck classes:

- RESP encoding allocations in `encode_resp_command`.
- `Vec<RedisString>` argv snapshots in dispatch.
- writer mutex contention.
- `BufWriter` flush behavior.
- `sync_data()` cost in `always`.
- everysec thread wake cadence.
- manifest/rewrite file syncs.
- snapshot clone cost before rewrite.

### 8.6 Toy Model: `aof-batch-model`

The preserved model under `harness/models/aof-batch-model` isolates one
question: what happens if `appendfsync always` syncs once per command versus
once per event-loop batch?

Fresh local runs on 2026-06-02:

```bash
cd harness/models/aof-batch-model
cargo run --release -- --commands 2000 --frame set --batches 1,4,16,64,256 > results/set-2k.tsv
cargo run --release -- --commands 2000 --frame incr --batches 1,4,16,64,256 > results/incr-2k.tsv
```

Selected output:

- `SET`, per-command sync: ~237 commands/s.
- `SET`, batch 16: ~3.4k commands/s.
- `SET`, batch 256: ~48k commands/s.
- `INCR`, per-command sync: ~240 commands/s.
- `INCR`, batch 16: ~3.8k commands/s.
- `INCR`, batch 256: ~61.6k commands/s.

Interpretation: upstream-shaped batching matters more than frame-size or encode
micro-optimizations for the `always` cliff. Production should stage propagated
AOF frames and flush before replies become observable, while still preserving
operator status, `fsynced_repl_offset`, and WAITAOF wakeup semantics.

---

## 9. Release Gates

AOF can leave alpha only when these are true on a clean tree:

### Rust / Kit Gates

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-core
cargo test -p redis-server
```

### Process Gates

```bash
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
```

### Upstream Surface Gates

```bash
python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused \
  --skip-build --timeout-s 180 --no-default-deny-tags \
  --deny-tag needs:repl --deny-tag cluster \
  --files unit/other,unit/aofrw,integration/aof,integration/rdb
```

If upstream files remain no-summary due unrelated harness limitations, the
release note must say so and the source-shaped frontier must be the release
gate.

### Performance Gates

```bash
python3 harness/bench/aof-matrix.py
python3 harness/bench/aof-rewrite-latency.py
```

These runners exist in the current dirty tree. They are telemetry, not public
performance claims, until repeated on an isolated host.

Minimum acceptable evidence:

- append overhead measured for `no/everysec/always`;
- rewrite latency measured under write load;
- startup replay speed measured for legacy AOF, manifest BASE+INCR, and RDB
  preamble BASE if supported;
- benchmark artifacts committed or ledgered with commit ID and dirty state.

---

## 10. Packet Candidates

### Packet A — Repair AOF Kit Baseline

Goal: make `aof_correctness_kit` green or honestly red on current production
behavior.

Targets:

- `crates/redis-commands/tests/aof_correctness_kit.rs`
- possibly a narrow production helper in `dispatch.rs` or `aof.rs`

Gate:

```bash
cargo test -p redis-commands --test aof_correctness_kit
```

### Packet B — Current Clean AOF Frontier Baseline

Goal: rerun the old 23-scenario frontier on clean `HEAD`.

Targets:

- evidence only;
- maybe docs update.

Gate:

```bash
python3 harness/oracle/persistence-frontier.py --skip-build
```

### Packet C — BGREWRITEAOF Lifecycle Truth

Goal: replace simulated rewrite-in-progress clears with real job state and make
operator-visible lifecycle state correspond to real work.

Targets:

- `crates/redis-commands/src/persist.rs`
- `crates/redis-core/src/persistence.rs`
- `crates/redis-commands/src/info.rs`

Gates:

```bash
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build \
  --scenarios aof-rewrite-collections-digest,multipart-aof-rewrite-sequence-advance \
  --fail-on-failure
```

### Packet D — AOF Benchmark Runner

Goal: add `bench-aof-matrix` and establish append/fsync overhead.

Targets:

- `harness/bench/aof-matrix.py`
- `harness/runners.toml`
- docs or evidence note

Gates:

```bash
python3 harness/bench/aof-matrix.py --quick
```

### Packet E — Rewrite Latency Runner

Goal: add `bench-aof-rewrite-latency` and measure write latency during rewrite.

Targets:

- `harness/bench/aof-rewrite-latency.py`
- `harness/runners.toml`

Gates:

```bash
python3 harness/bench/aof-rewrite-latency.py --quick
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
```

### Packet F — Forkless Snapshot / Rewrite Start Latency

Goal: remove the remaining command-path snapshot cost now that Packet H runs
BASE generation on a background thread while active appends continue on INCR.

Targets:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/persist.rs`
- `crates/redis-core/src/persistence.rs`
- `KeyspaceSnapshot` / persistent keyspace snapshot work

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
python3 harness/bench/aof-rewrite-latency.py
```

### Packet G — AOF Flush-Boundary Batching

Goal: make `appendfsync always` behave more like upstream Valkey under
pipelined/event-loop workloads.

Status: implemented as a narrow `appendfsync always`
staging path. `appendfsync no/everysec` intentionally stays on the immediate
append path.

Targets:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/config_cmd.rs`
- `crates/redis-commands/src/connection.rs`
- `crates/redis-commands/src/dispatch.rs`
- `crates/redis-commands/src/multi.rs`
- `crates/redis-commands/src/persist.rs`
- `crates/redis-server/src/runtime_owner.rs`
- `harness/bench/aof-matrix.py`
- `harness/models/aof-batch-model`

Work:

- `AofWriter` can encode selected/raw command frames separately from writing.
- A thread-local RuntimeOwner batch stages encoded bytes plus the highest
  covered replication offset.
- `RuntimeOwner::dispatch_slot_commands` begins the batch before draining a
  client slot and flushes it before queued replies become observable.
- Successful propagated command frames stage instead of syncing immediately only
  when `appendfsync always` is active.
- The flush writes all staged bytes, `sync_data()`s once, updates
  `aof_last_write_status`, publishes `fsynced_repl_offset`, and wakes WAITAOF
  waiters after successful fsync.
- AOF lifecycle commands force a staged flush before writer swap/removal paths:
  dispatch barrier plus handler barriers for `CONFIG SET appendonly`,
  `BGREWRITEAOF`, and `SHUTDOWN`.
- Transaction propagation now attaches the known replication offset to the EXEC
  frame so the batched fsync publishes the transaction's covered offset.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
```

Performance acceptance:

- `appendfsync always` with pipeline 16 should move materially toward the local
  reference Valkey shape, not remain near one fsync per command.
- `appendfsync no/everysec` must not regress outside benchmark noise.

Measured result on 2026-06-02:

- Prior Rust quick, `20260602T140115Z`: SET p16 always 243.072 rps, INCR p16
  always 222.573 rps.
- Packet G Rust quick, `20260602T171157Z`: SET p16 always 3533.595 rps, INCR
  p16 always 3786.690 rps.
- Latest reference quick, `20260602T142918Z`: SET p16 always 7467.443 rps, INCR
  p16 always 6367.804 rps.

Acceptance read: Packet G recovers material throughput and validates the toy
model direction. It does not make `appendfsync always` cheap, and the remaining
gap versus reference still justifies a follow-up profile before any broad claim.

### Packet H — Two-Phase Multipart AOF Rewrite

Goal: move `BGREWRITEAOF` from a synchronous command-path rewrite to the
upstream Valkey shape without taking on fork/COW yet.

Status: implemented.

Targets:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/persist.rs`
- `harness/bench/aof-rewrite-latency.py`
- `harness/oracle/persistence-frontier.py`

Work:

- `begin_manifest_aof_rewrite` loads the current manifest, computes the next
  BASE/INCR sequence, opens a fresh active INCR, flushes the previous writer,
  persists a preliminary manifest that still names the old chain plus the new
  INCR, installs the new writer, and returns the planned rewrite state.
- `complete_manifest_aof_rewrite` writes the temp BASE, renames it into place,
  publishes the final manifest naming the new BASE plus active INCR, refreshes
  `aof_current_size`, and leaves the active writer untouched.
- `BGREWRITEAOF` now returns after the snapshot and writer switch. A named
  `aof-rewrite` thread performs BASE generation and final manifest publication.
- If completion fails, the preliminary manifest remains replayable: old
  BASE/INCR files plus the new active INCR preserve acknowledged writes.
- The rewrite-latency runner now reports both command round-trip time and the
  true rewrite-in-progress window.
- `persistence-frontier` includes a rewrite-window process scenario that writes
  before/during/after `BGREWRITEAOF`, restarts, and checks all acknowledged keys.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
cargo build --release -p redis-server
python3 harness/bench/aof-rewrite-latency.py --quick --targets rust --skip-build
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
```

Measured result on 2026-06-02:

- `aof_correctness_kit`: 11/11 pass.
- `repl_correctness_kit`: 13/13 pass.
- `redis-server`: 8/8 pass.
- `persistence-cycle --mode aof`: pass (`20260602T181421Z`).
- `persistence-cycle --mode aof-rewrite`: pass (`20260602T181424Z`).
- `persistence-frontier`: 27/27 pass (`20260602T181429Z`).
- `cargo build --release -p redis-server`: pass.
- Rewrite latency artifact:
  `harness/bench/results/20260602T181439Z-6d52e9d-aof-rewrite-latency.*`.
- Matrix artifact:
  `harness/bench/results/20260602T181027Z-6d52e9d-aof-matrix.*`, with the
  noisy `INCR`/p16/`always` cell checked by targeted rerun
  `harness/bench/results/20260602T181116Z-6d52e9d-aof-matrix.*`.

Acceptance read: Packet H materially improves rewrite shape. It proves that
writes acknowledged before, during, and after `BGREWRITEAOF` survive restart
while BASE generation happens outside the command handler. It does not yet
solve rewrite start latency for large datasets because `snapshot_all_dbs()` is
still a deep clone on the command path, and it does not yet implement Valkey's
history cleanup/fsync-hardening around manifest directories.

### Packet I — AOF Rewrite Crash/Fault Hardening

Goal: harden multipart AOF publication after Packet H and prove the main
rewrite crash windows with process-level frontier coverage.

Status: implemented.

Targets:

- `crates/redis-commands/src/aof.rs`
- `harness/oracle/persistence-frontier.py`
- `harness/bench/aof-rewrite-latency.py`
- `docs/AOF_ENDGAME_SPEC.md`

Work:

- `persist_aof_manifest` now writes a temp manifest, flushes and fsyncs it,
  renames it over the live manifest, and fsyncs the containing AOF directory
  where supported. This mirrors Valkey's `writeAofManifestFile` ordering.
- AOF BASE rewrite output now fsyncs file contents before final publication.
  RDB-preamble BASE output is explicitly fsynced after `save_rdb_databases`.
- Final BASE rename is followed by an AOF directory fsync before the final
  manifest is persisted.
- Failed finalization leaves the preliminary manifest replayable and keeps the
  active INCR as the append target; the background worker reports
  `aof_last_bgrewrite_status:err`.
- `persistence-frontier` now includes crash-window-shaped multipart rewrite
  states:
  - preliminary manifest with old BASE/INCR plus new INCR survives restart;
  - temp BASE exists but final manifest is not published;
  - final BASE exists but manifest still points to the preliminary chain;
  - live failed rewrite remains replayable and reports error status;
  - corrupt rewritten BASE named by the manifest fails closed at startup.
- `aof-rewrite-latency` now accepts `--dataset-sizes`, reports
  `rewrite_post_reply_wall_ms`, treats command wall as
  `rewrite_start_block_ms`, and samples server RSS before/during/after rewrite.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
cargo build --release -p redis-server
python3 harness/bench/aof-rewrite-latency.py --quick --targets rust --skip-build
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
```

Measured result on 2026-06-02:

- `aof_correctness_kit`: 11/11 pass.
- `repl_correctness_kit`: 13/13 pass.
- `redis-server`: 8/8 pass.
- `persistence-cycle --mode aof`: pass (`20260602T190407Z`).
- `persistence-cycle --mode aof-rewrite`: pass (`20260602T190411Z`).
- `persistence-frontier`: 32/32 pass (`20260602T190420Z`).
- `cargo build --release -p redis-server`: pass.
- Targeted crash-window frontier: 5/5 pass (`20260602T190040Z`).
- Scaling rewrite telemetry:
  `harness/bench/results/20260602T190216Z-6d52e9d-aof-rewrite-latency.*`.
- Dataset 250: command/start block 8.502 ms, post-reply rewrite 33.158 ms,
  RSS peak 7968 KiB, restart passed.
- Dataset 1000: command/start block 9.968 ms, post-reply rewrite 36.824 ms,
  RSS peak 9232 KiB, restart passed.
- Dataset 5000: command/start block 10.666 ms, post-reply rewrite 32.087 ms,
  RSS peak 16832 KiB, restart passed.
- Required quick rewrite telemetry:
  `harness/bench/results/20260602T190441Z-6d52e9d-aof-rewrite-latency.*`.
- Required quick matrix:
  `harness/bench/results/20260602T190447Z-6d52e9d-aof-matrix.*`.

Acceptance read: Packet I closes the most obvious durability-ordering gap
against upstream Valkey and gives the frontier concrete crash-window coverage.
The current small scaling run does not prove large-dataset behavior yet, but it
does confirm that command-wall/start-block time is now separately visible and
that RSS grows with dataset size. The next packet should run larger scale
telemetry and then remove the command-path deep clone with `KeyspaceSnapshot` or
another structural-sharing snapshot.

### Packet J — AOF History Cleanup / KeyspaceSnapshot Phase 0

Goal: close the successful-rewrite history-file lifecycle and create a stable
snapshot contract before replacing the live keyspace representation.

Status: implemented.

Targets:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/persist.rs`
- `crates/redis-core/src/keyspace_snapshot.rs`
- `crates/redis-core/src/command_context.rs`
- `crates/redis-core/src/persistence.rs`
- `crates/redis-commands/src/info.rs`
- `harness/oracle/persistence-frontier.py`
- `harness/bench/aof-rewrite-latency.py`
- `docs/AOF_ENDGAME_SPEC.md`

Work:

- `AofManifest` tracks history entries. Final rewrite publication first writes
  a durable manifest that names the new BASE/current INCR and marks superseded
  files as history. Only after that publication succeeds are the old files
  unlinked.
- Missing history files during cleanup are ignored, matching the fact that the
  durable final manifest is already replayable. Other cleanup/compaction errors
  are logged but do not turn a successful rewrite into data loss.
- Failed rewrite paths still keep the preliminary manifest replayable and do
  not delete old BASE/INCR files.
- `KeyspaceSnapshot` is now the shared snapshot shape for AOF/RDB consumers.
  It records DB count, key count, and capture duration. It is intentionally a
  facade over the current deep snapshot, not a HAMT, segmented COW table, or
  owner-owned live map replacement.
- Rewrite telemetry records snapshot key count and capture time, command/start
  block time, post-reply rewrite wall time, total rewrite wall time, RSS
  before/peak/after, BASE/INCR bytes, acknowledged writes, and restart status.

Gates:

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
cargo build --release -p redis-server
python3 harness/bench/aof-rewrite-latency.py --targets rust --skip-build --dataset-sizes 5000,25000,100000
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
```

Measured result on 2026-06-02:

- `aof_correctness_kit`: 11/11 pass.
- `repl_correctness_kit`: 13/13 pass.
- `redis-server`: 8/8 pass.
- `persistence-cycle --mode aof`: pass (`20260602T200654Z`).
- `persistence-cycle --mode aof-rewrite`: pass (`20260602T200657Z`).
- Targeted history frontier: 2/2 pass (`20260602T195818Z`).
- Full persistence frontier: 34/34 pass (`20260602T200700Z`).
- `cargo build --release -p redis-server`: pass.
- Rewrite latency artifact:
  `harness/bench/results/20260602T200339Z-1a9d679-aof-rewrite-latency.*`.
- Dataset 5000: 7464 snapshot keys, snapshot 1179 us, command/start block
  10.458 ms, post-reply rewrite 58.018 ms, RSS peak 20864 KiB, restart passed.
- Dataset 25000: 27896 snapshot keys, snapshot 4503 us, command/start block
  16.318 ms, post-reply rewrite 60.007 ms, RSS peak 56112 KiB, restart passed.
- Dataset 100000: 102735 snapshot keys, snapshot 19319 us, command/start block
  27.943 ms, post-reply rewrite 129.428 ms, RSS peak 146272 KiB, restart passed.
- Quick AOF matrix artifact:
  `harness/bench/results/20260602T200407Z-1a9d679-aof-matrix.*`, 16/16 pass.

Acceptance read: Packet J makes successful multipart rewrite cleanup safe and
observable without pretending the snapshot problem is solved. The 100k row shows
that the current deep snapshot accounts for a material fraction of the
operator-visible `BGREWRITEAOF` command stall. That is enough evidence to treat
structural sharing as the next architecture packet, while keeping this packet
bounded and mergeable.

### Packet K — Structural-Sharing Snapshot Phase 1

Goal: remove the command-path deep clone from rewrite snapshot capture while
preserving the `KeyspaceSnapshot` facade as the only AOF/RDB consumer contract.

Status: implemented.

Targets:

- `crates/redis-core/src/keyspace_map.rs`
- `crates/redis-core/src/keyspace_snapshot.rs`
- `crates/redis-core/src/db.rs`
- `crates/redis-core/src/command_context.rs`
- `harness/models/keyspace-cow-model`
- `docs/OPTION_D_PERSISTENT_KEYSPACE_SPEC.md`
- `docs/AOF_ENDGAME_SPEC.md`

Work:

- The toy model now includes hashed segmented routing, INCR-style mutation
  phases, held-snapshot mutation phases, and RSS samples.
- `RedisDb` now stores `KeyspaceMap`, a segmented copy-on-write table backed by
  `Vec<Arc<HashMap<RedisString, RedisObject>>>`.
- `KeyspaceMap::snapshot()` clones segment roots in O(segment count). The first
  live write to a shared segment clones that segment; misses avoid
  `Arc::make_mut`.
- `KeyspaceSnapshotDb` can hold either owned entries or a shared
  `KeyspaceMapSnapshot`, keeping AOF/RDB consumers behind one facade.
- `snapshot_all_dbs()` now captures keyspace roots instead of deep-cloning every
  key/value on the owner thread.
- LRU-touching read lookup now uses one mutable map lookup rather than
  `get_mut` followed by `get`, avoiding a doubled segment/hash lookup on the
  normal hit path.

Gates:

```bash
cargo check -p redis-core -p redis-commands
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
cargo test -p redis-core
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
cargo build --release -p redis-server
python3 harness/bench/aof-rewrite-latency.py --targets rust --skip-build --dataset-sizes 5000,25000,100000
VALKEY_BENCH_SKIP_BUILD=1 bash harness/bench/run-profile-matrix.sh
```

Measured result on 2026-06-02:

- `cargo check -p redis-core -p redis-commands`: pass.
- Keyspace model tests: 5/5 pass.
- `cargo test -p redis-core`, AOF correctness kit, replication correctness kit,
  and `cargo test -p redis-server`: pass.
- Restart/frontier gates: RDB cycle pass (`20260602T210155Z`), AOF cycle pass
  (`20260602T205929Z`), AOF rewrite cycle pass (`20260602T205934Z`), and full
  persistence frontier 40/40 pass (`20260602T205938Z`).
- Rewrite latency artifact:
  `harness/bench/results/20260602T210203Z-1a9d679-aof-rewrite-latency.*`.
- Dataset 5000: 7942 snapshot keys, snapshot 55 us, command/start block
  10.058 ms, post-reply rewrite 50.442 ms, RSS peak 20880 KiB, restart passed.
- Dataset 25000: 27794 snapshot keys, snapshot 99 us, command/start block
  9.114 ms, post-reply rewrite 69.123 ms, RSS peak 53984 KiB, restart passed.
- Dataset 100000: 102916 snapshot keys, snapshot 97 us, command/start block
  9.778 ms, post-reply rewrite 127.611 ms, RSS peak 168176 KiB, restart passed.
- Full profile matrix:
  `harness/bench/results/20260602T210228Z-1a9d679-profile-matrix.tsv`, median
  1.02x, min 0.76x, p1 `GET` 0.86x, p16 `GET` 0.99x, p100 `GET` 1.17x.
- Focused ordered probes:
  `harness/bench/results/20260602T210252Z-1a9d679-default-suite-parts.*`
  records p1 `SET` 1.011x, `GET` 0.950x, and `INCR` 1.032x;
  `harness/bench/results/20260602T210259Z-1a9d679-default-suite-parts.*`
  records p16 `SET` 1.323x, `GET` 1.022x, and `INCR` 0.950x.

Acceptance read: Packet K takes the 100k rewrite-start snapshot capture from
Packet J's 19319 us to 97 us and command/start block from 27.943 ms to
9.778 ms, without replacing the AOF/RDB snapshot facade. The win is command-path
capture latency, not total rewrite serialization time: BASE generation still
walks the snapshot in the background, and value-payload sharing is deliberately
deferred. Hot-path throughput is acceptable but mixed for this phase; p1 `GET`,
p16 `INCR`, and range-prep noise stay as follow-up telemetry rather than
blockers.

### Packet L — AOF Rewrite Syscall Fault Injection

Goal: move beyond modeled on-disk states by injecting failures at the production
rewrite publication calls and proving restart behavior.

Status: implemented.

Targets:

- `crates/redis-commands/src/aof.rs`
- `harness/oracle/persistence-frontier.py`
- `docs/AOF_ENDGAME_SPEC.md`

Work:

- Added inert-by-default AOF fault hooks gated by `VALDR_AOF_FAULT`.
- Hook points cover BASE AOF sync, RDB BASE sync, BASE rename, BASE
  post-rename directory sync, and manifest sync/rename/post-rename directory
  sync for current, preliminary, final, and compact manifest phases.
- The frontier runner can now start one server process with a scoped environment
  override, keeping restart checks on a clean process with no injected fault.
- Added six production-fault scenarios:
  - `multipart-aof-rewrite-fault-preliminary-manifest-before-rename`
  - `multipart-aof-rewrite-fault-base-before-rename`
  - `multipart-aof-rewrite-fault-base-after-rename-before-dir-sync`
  - `multipart-aof-rewrite-fault-manifest-final-before-sync`
  - `multipart-aof-rewrite-fault-manifest-final-before-rename`
  - `multipart-aof-rewrite-fault-manifest-final-after-rename-before-dir-sync`
- Each scenario starts appendonly with multipart AOF, writes before the rewrite,
  injects one failure during `BGREWRITEAOF`, writes after the failure path,
  restarts, and verifies acknowledged writes survived.

Gates:

```bash
python3 -m py_compile harness/oracle/persistence-frontier.py
cargo build -p redis-server
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure \
  --scenarios multipart-aof-rewrite-fault-preliminary-manifest-before-rename,multipart-aof-rewrite-fault-base-before-rename,multipart-aof-rewrite-fault-base-after-rename-before-dir-sync,multipart-aof-rewrite-fault-manifest-final-before-sync,multipart-aof-rewrite-fault-manifest-final-before-rename,multipart-aof-rewrite-fault-manifest-final-after-rename-before-dir-sync
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
```

Measured result on 2026-06-02:

- Targeted fault frontier:
  `harness/oracle/results/persistence-frontier/20260602T205624Z/`, 6/6 pass.
- Full persistence frontier:
  `harness/oracle/results/persistence-frontier/20260602T205938Z/`, 40/40 pass.
- Final quick Rust AOF matrix:
  `harness/bench/results/20260602T210305Z-1a9d679-aof-matrix.*`, 16/16 pass.
- Preliminary manifest pre-rename failure returns a `BGREWRITEAOF failed`
  error, leaves the old manifest intact, and restart preserves writes before
  and after the failed command.
- BASE pre-rename failure leaves a temp BASE and preliminary manifest; restart
  ignores the temp BASE and preserves old chain plus new INCR.
- BASE post-rename/pre-dir-sync failure leaves the final BASE file present but
  still unnamed by the manifest; restart ignores it and preserves old chain plus
  new INCR.
- Final manifest pre-sync and pre-rename failures leave the preliminary manifest
  replayable while the renamed BASE remains ignored.
- Final manifest post-rename/pre-dir-sync failure leaves a final manifest with
  history entries; restart loads the new BASE/current INCR and preserves
  acknowledged writes.

Acceptance read: Packet L does not prove filesystem physics after power loss,
but it does prove the production error paths at the syscall boundaries we can
control. The release confidence is stronger than the earlier state-model-only
frontier because the tests now exercise the actual `BGREWRITEAOF` code path,
status reporting, manifest state, and restart replay after injected publication
failures.

---

### Packet M — Held-Snapshot COW Telemetry / Payload Model

Goal: make forkless snapshot write-window cost observable and decide whether
payload sharing should be implemented broadly or narrowly.

Status: implemented in this packet.

Targets:

- `crates/redis-core/src/keyspace_cow.rs`
- `crates/redis-core/src/keyspace_map.rs`
- `crates/redis-commands/src/info.rs`
- `harness/bench/aof-rewrite-latency.py`
- `harness/models/keyspace-cow-model`
- `docs/KEYSPACE_COW_PAYLOAD_SHARING_SPEC.md`
- `docs/AOF_ENDGAME_SPEC.md`

Work:

- Add atomic keyspace COW counters for active snapshots, snapshot starts/drops,
  segment clone count, cloned key count, estimated clone bytes, and max
  clone-key/byte pressure.
- Hold a snapshot guard inside `KeyspaceMapSnapshot` so active snapshot counts
  track the actual lifetime of AOF/RDB snapshot consumers.
- Expose COW counters under `INFO persistence`.
- Extend `aof-rewrite-latency.py` to sample COW counters before, during, and
  after `BGREWRITEAOF`, with per-run deltas and peaks.
- Extend the standalone model with production-shaped owned-payload segmented
  COW and metadata/payload split variants.
- Keep `keyspace_cow_segment_clone_us` at zero for this packet. Clone timing
  would require either a pre-`strong_count` branch or an unconditional timer
  around every mutating key operation; the chosen hot path is one
  `Arc::make_mut` plus pointer comparison.

Gates:

```bash
cargo check -p redis-core -p redis-commands
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
cargo test -p redis-core
cargo test -p redis-commands info_persistence_exposes_keyspace_cow_fields -- --nocapture
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode rdb
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build
bash harness/bench/run-profile-matrix.sh
python3 harness/bench/aof-rewrite-latency.py --dataset-sizes 5000,25000,100000
```

Measured result on 2026-06-03:

- Model tests: 10/10 pass.
- `cargo check`, `redis-core`, AOF kit, replication kit, and `redis-server`:
  pass.
- RDB/AOF/AOF-rewrite restart cycles: pass.
- Full persistence frontier: 40/40 pass (`20260603T015103Z`).
- Rewrite latency artifact:
  `harness/bench/results/20260603T014824Z-787f63c-aof-rewrite-latency.*`.
- 100k rewrite row: 102,937 snapshot keys, 111 us snapshot capture,
  8.372 ms command wall, 439 COW segment clones, 43,076 cloned keys, and
  6,835,864 estimated clone bytes. Restart verification passed.
- Profile matrix artifact:
  `harness/bench/results/20260603T014731Z-787f63c-profile-matrix.tsv`,
  median 0.98x, min 0.84x, max 1.33x.
- Focused p1 `SET`/`GET`/`INCR`:
  1.020x / 1.051x / 1.008x.
- Focused p16 `SET`/`GET`/`INCR`:
  1.211x / 1.006x / 0.910x. The p16 `INCR` dip is consistent with a
  pre-packet baseline probe and is not treated as a new COW cliff.

Acceptance read: Packet M gives AOF rewrite windows an operator-visible COW
pressure surface without making AOF the default. The model shows large
held-snapshot wins from metadata/payload splitting, especially at 1 KiB and
64 KiB payload sizes, but the small-value live-operation risk is high enough
that broad production `RedisObject { Arc<ObjectPayload> }` migration is
deferred. The next payload-sharing work should be a narrower payload-handle
packet, not a whole-object-layout rewrite.

---

## 11. Next AOF Working Spec

The next AOF packet should move from "rewrite is structurally safe" toward
"AOF is a release-grade durability surface." The highest-leverage code work is
not another keyspace map rewrite; it is the AOF release-frontier cleanup and
validation layer around the hardened multipart rewrite path.

### 11.1 Packet N — AOF Release Frontier Cleanup / Checker / TCL Triage

Ambitious end state:

- Startup and successful rewrites do not leave unbounded temp/history debris in
  `appenddirname`.
- Cleanup never deletes a file named by the current manifest, even in a failed
  rewrite or crash-window state.
- `valkey-check-aof` is a real preflight tool for the layouts this server can
  create: legacy RESP AOF, multipart manifests, `.aof` BASE/INCR files, and
  manifest `.rdb` BASE preambles.
- Focused upstream persistence TCL files are either passing, converted into
  concrete bug packets, or clearly blocked by harness limitations.
- Docs can move from "AOF alpha" toward a precise release-gated status only
  after the source-shaped frontier, process cycles, checker cases, and
  benchmark telemetry agree.

Non-goals:

- Do not change AOF default enablement. `appendonly` stays off by default.
- Do not make filesystem power-loss claims beyond measured and injected-fault
  evidence.
- Do not rewrite RuntimeOwner or the global AOF writer as part of this packet.
- Do not broaden payload sharing inside this AOF packet.
- Do not delete files whose names are not recognized as this server's AOF temp,
  history, BASE, INCR, or manifest forms.

### 11.2 Code Work

1. **Recover exact upstream intent.**
   Read local Valkey `src/aof.c` for:
   `aofDelHistoryFiles`, temp rewrite file cleanup, `loadAppendOnlyFiles`,
   manifest validation, and `redis-check-aof` behavior.

2. **Inventory current Rust behavior.**
   Read:
   `crates/redis-commands/src/aof.rs`,
   `crates/redis-server/src/main.rs`,
   `crates/redis-server/src/check_aof.rs`,
   `harness/oracle/persistence-frontier.py`, and the latest AOF runner results.

3. **Add conservative cleanup primitives.**
   Add small production helpers that:
   - parse the current manifest into referenced filenames;
   - classify appenddir files as live, history, temp rewrite, unknown, or
     legacy;
   - delete only safe unreferenced temp/history files;
   - report cleanup counts and errors without blocking successful startup on
     harmless orphan cleanup failure.

4. **Wire cleanup at the right lifecycle points.**
   Candidate hooks:
   - after successful AOF startup load;
   - after successful final manifest publication and history deletion;
   - optionally after `CONFIG SET appendonly yes` creates the initial layout.

5. **Harden `valkey-check-aof`.**
   Ensure check-aof:
   - validates multipart manifest grammar and referenced files;
   - validates `.aof` BASE and INCR RESP streams;
   - validates manifest `.rdb` BASE preambles through the shared RDB checker;
   - reports truncated tails consistently with upstream expectations;
   - does not silently accept corrupt manifests or missing files.

6. **Extend frontier scenarios before trusting cleanup.**
   Add process scenarios for:
   - startup removes old temp rewrite files after successful load;
   - startup preserves every current-manifest-referenced file;
   - startup preserves failed-rewrite preliminary chain and active INCR;
   - successful rewrite cleanup is idempotent across restart;
   - check-aof accepts valid multipart layout;
   - check-aof fails missing/corrupt BASE or INCR;
   - check-aof validates RDB preamble BASE if `aof-use-rdb-preamble yes`.

7. **Triage focused upstream persistence TCL.**
   Rerun `tcl-persistence-focused`. If the runner still produces no summaries,
   fix the harness summary path first. Then split failures into:
   real AOF bugs, unsupported upstream surfaces, or unrelated harness limits.

8. **Update release docs only after gates pass.**
   Update `README.md`, `docs/coverage.md`, and
   `docs/TEST_AND_FEATURE_COVERAGE.md` with the exact AOF status, not a broad
   release claim.

### 11.3 Tool Iteration Loop

Start cheap and local:

```bash
git status --short
cargo check -p redis-core -p redis-commands -p redis-server
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-server check_aof
python3 -m py_compile harness/oracle/persistence-frontier.py
```

Then run targeted process evidence:

```bash
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure \
  --scenarios multipart-aof-rewrite-success-deletes-history,multipart-aof-rewrite-failure-preserves-history-files
python3 harness/oracle/persistence-cycle.py --mode aof --skip-build
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite --skip-build
```

After adding new scenarios, run the full frontier:

```bash
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
```

Then run upstream persistence telemetry:

```bash
python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused \
  --skip-build --timeout-s 180 --no-default-deny-tags \
  --deny-tag needs:repl --deny-tag cluster \
  --files unit/other,unit/aofrw,integration/aof,integration/rdb
```

Only after correctness is clean, run AOF performance telemetry:

```bash
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
python3 harness/bench/aof-rewrite-latency.py --targets rust --skip-build \
  --dataset-sizes 5000,25000,100000
```

If performance moves unexpectedly, use profiler runners in this order:

```bash
bash harness/bench/run-profile-matrix.sh
python3 harness/bench/profile-hotspots.py --suite smoke --sample-seconds 4
python3 harness/bench/profile-calltree.py --suite big --profile-seconds 8
```

Do not run benchmark/profiler agents concurrently; they share CPU, ports,
results directories, and profiler resources.

### 11.4 Acceptance Criteria

Packet N is accepted when:

- AOF kit and `redis-server` check-aof tests pass.
- RDB/AOF/AOF-rewrite process cycles pass.
- Full persistence frontier passes with new cleanup/checker scenarios included.
- Focused persistence TCL has a parsed summary; remaining failures are linked
  to concrete follow-up packets.
- Cleanup leaves all manifest-referenced files intact in failed and successful
  rewrite windows.
- Check-aof returns failure for missing/corrupt manifest targets and success for
  valid layouts this server creates.
- AOF matrix and rewrite-latency telemetry still pass in quick Rust mode.
- Docs record what changed, exact artifacts, remaining release gaps, and the
  next recommendation.

### 11.5 Packet N Landing Record

Packet N landed the release-frontier cleanup/checker layer without changing AOF
default enablement. `appendonly` remains off by default.

Implemented:

- `cleanup_aof_appenddir` loads the current manifest, preserves every BASE,
  HISTORY, and INCR filename it references, and removes only recognized safe
  debris: temp manifests, temp rewrite files, and unreferenced generated
  multipart AOF files for the configured `appendfilename`.
- AOF startup runs cleanup only after successful load/open/install of the active
  writer. Cleanup is best-effort and non-fatal; it reports inspected, preserved,
  removed, and error counts.
- `valkey-check-aof` now fails closed on empty or malformed multipart
  manifests, duplicate BASE entries, non-increasing INCR sequence numbers,
  missing manifest targets, corrupt RESP streams, and non-bare manifest paths.
  It accepts valid multipart layouts and manifest `.rdb` BASE preambles.
- The persistence frontier gained cleanup/checker scenarios for safe startup
  orphan deletion, referenced-file preservation, failed-rewrite preliminary
  chains, idempotent rewrite cleanup, valid multipart checking, missing/corrupt
  target failure, and RDB-preamble BASE checking.
- `tcl-survey.py` now records parsed failure-line counts plus abort/exception
  points when upstream TCL files do not emit normal pass/fail summaries.

Packet N gates run on 2026-06-03:

- `cargo check -p redis-commands -p redis-server`: pass.
- `cargo test -p redis-commands --test aof_correctness_kit`: 11/11 pass.
- `cargo test -p redis-server`: 8/8 pass.
- `python3 harness/oracle/persistence-cycle.py --mode rdb --skip-build`: pass,
  run timestamp `20260603T022026Z`.
- `python3 harness/oracle/persistence-cycle.py --mode aof --skip-build`: pass,
  run timestamp `20260603T022026Z`.
- `python3 harness/oracle/persistence-cycle.py --mode aof-rewrite --skip-build`:
  pass, run timestamp `20260603T022026Z`.
- `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure`:
  48/48 pass, run id `20260603T022034Z`.
- `cargo test -p redis-commands --test repl_correctness_kit`: 13/13 pass.
- `python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused ...`:
  4 files surveyed, 1 timeout, 4 without normal summary, 9 parsed failure
  lines, 3 abort/exception points, run id `20260603T022554572501Z`.
- `python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build`:
  16/16 pass, artifacts
  `harness/bench/results/20260603T023030Z-a3e82bf-aof-matrix.tsv` and `.json`.
- `python3 harness/bench/aof-rewrite-latency.py --targets rust --skip-build --dataset-sizes 5000,25000,100000`:
  3/3 pass, artifacts
  `harness/bench/results/20260603T023101Z-a3e82bf-aof-rewrite-latency.tsv`
  and `.json`.

Focused TCL is now actionable but still a release blocker:

- `integration/aof`: five parsed failures plus an abort at
  `AOF fsync always barrier issue` because `DEBUG aof-flush-sleep` is missing.
- `unit/aofrw`: one parsed failure plus an abort in `AOF rewrite functions`
  because function replay/rewrite support is missing.
- `unit/other`: three parsed persistence/expire failures and an unrelated
  unix-socket harness exception.
- `integration/rdb`: timed out at 180 seconds without a normal summary.

Performance read:

- The quick matrix still passes; `appendfsync always` remains fsync-bound and is
  expectedly expensive, especially p1 writes.
- Rewrite-start snapshot capture remains root-clone scale: 100k rewrite
  snapshot capture was 59 us, command wall/start block was 11.4006 ms, and
  post-reply rewrite wall was 132.7145 ms.
- Packet N did not broaden AOF's public claim. Release docs should stay
  conservative until the focused TCL blockers are resolved or explicitly
  classified as unsupported upstream surfaces.

## 12. Packet O Progress: Focused TCL Burn-Down

Packet O moved `integration/aof` from a broad persistence-red file to one
remaining runtime-yield blocker. The production changes are intentionally
source-shaped and covered by Rust/process/frontier checks before claiming TCL
movement.

Implemented:

- `DEBUG AOF-FLUSH-SLEEP <micros>` delays immediately before AOF bytes are
  written, matching upstream's fsync-barrier timing knob.
- AOF replay with `aof-load-truncated yes` physically truncates legacy and
  multipart files back to the last valid RESP boundary before accepting future
  appends.
- AOF replay now treats unfinished `MULTI` as an atomic truncated transaction:
  `load_truncated yes` cuts back before `MULTI`, while strict mode fails with
  an unexpected EOF.
- `valkey-check-aof` tracks committed offsets inside `MULTI` and refuses to
  truncate non-last multipart files, including timestamp truncation mode.
- AOF startup logs now expose upstream-shaped bad-format, unknown-command, and
  unexpected-EOF messages on the stdout stream the TCL harness watches.
- Replay preserves key expiry metadata across later collection mutations, and
  past `PEXPIREAT` values survive loading until normal post-load lookup deletes
  the key.
- Blocked `BLMPOP`/`BZMPOP` wake mutations append to AOF in the same owner
  batch as the triggering write, preserving replay order.
- `aof-timestamp-enabled` is live-configured, emits `#TS:<unix-seconds>` for
  incremental and rewritten AOF files when enabled, and remains off by default.
- `appendfsync everysec` now flushes AOF bytes to the file immediately while
  still deferring fsync; upstream tests that inspect the file after a write now
  see the command bytes.
- `valkey-check-aof --truncate-to-timestamp` truncates at the first future
  `#TS:` annotation and reports upstream-shaped abort/non-last-file messages.

Packet O gates run on 2026-06-03:

- `cargo check -p redis-core -p redis-commands -p redis-server`: pass.
- `cargo test -p redis-commands --test aof_correctness_kit`: 15/15 pass.
- `cargo test -p redis-server`: 8/8 pass.
- `python3 -m py_compile harness/oracle/persistence-frontier.py`: pass.
- Targeted frontier:
  `aof-blocked-lmpop-zmpop-wake-persists` passed, run id
  `20260603T031138Z`.
- Targeted timestamp frontier:
  `aof-timestamp-annotations-generated,check-aof-truncate-to-timestamp` passed,
  run id `20260603T032103Z`.
- Full persistence frontier:
  `python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure`
  passed 57/57, run id `20260603T032251Z`.
- Focused `integration/aof`:
  `python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused --skip-build --timeout-s 180 --no-default-deny-tags --deny-tag needs:repl --deny-tag cluster --files integration/aof`
  reports 42/43 pass, one remaining failure, run id
  `20260603T032108720990Z`.
- Focused four-file persistence survey:
  `unit/other`, `unit/aofrw`, `integration/aof`, and `integration/rdb` produced
  42 passed tests, one failed test, one timed-out file, three files without
  normal summaries, six parsed failure lines, and two abort/exception points;
  run id `20260603T032306533386Z`.

Current release blockers after Packet O:

- `integration/aof`: only `EVAL timeout with slow verbatim Lua script from AOF`
  remains. This is not an AOF parser/truncation issue; upstream serves other
  clients during a timed-out loading script through `processEventsWhileBlocked`,
  while this port's RuntimeOwner still executes `DEBUG LOADAOF` synchronously.
- `unit/aofrw`: still aborts at `AOF rewrite functions` with
  `ERR Function not found`; this is a function subsystem/rewrite surface, not a
  basic AOF manifest or truncation failure.
- `unit/other`: still records expire/reload failures and aborts later in a
  unix-socket fixture before a normal summary.
- `integration/rdb`: still times out at 180 seconds without a normal summary.

---

## 13. Risks

- **False confidence from old dirty-tree evidence.** The 23/23 persistence
  frontier is useful history, but clean current evidence must replace it.
- **Stale red kit tests.** AOF work should not start from a fake red test that
  no longer matches production code.
- **Rewrite serialization still scales with data.** Packet K removes the
  command-path deep snapshot clone, but BASE generation still walks and
  serializes the snapshot in the background.
- **Segmented-COW write-window cost.** Writes during a held snapshot can clone
  touched segment maps. Packet M makes this visible, but future segment tuning
  still needs repeated held-window evidence.
- **Value payloads are still owned.** Packet K shares index roots, not object
  payload internals. Large mutable values can still be expensive until metadata
  and payload ownership are split.
- **Crash/fault coverage is stronger but not physics-complete.** Packet L
  injects production-call failures at the key publication boundaries, but it
  still does not prove every filesystem's behavior after power loss.
- **Cleanup is conservative but still synchronous best-effort.** Packet N adds
  startup garbage collection for recognized orphan/temp AOF files, but there is
  still no background retry queue or operator-visible cleanup knob.
- **RDB preamble ambiguity.** Manifest `.rdb` BASE and legacy RDB-preamble AOF
  are different surfaces. Do not accidentally claim both.
- **AOF propagation drift.** Command handlers can mutate argv or suppress
  propagation. Every such command is a potential AOF bug.
- **Fsync is OS/filesystem-dependent.** Durability claims must be phrased as
  measured behavior under the tested environment, not universal physics.
- **Writer global state vs RuntimeOwner.** Current globals may be fine for a
  correctness wave but become architectural debt as owner-owned DB work deepens.
- **Performance benchmark contamination.** AOF/fsync tests are sensitive to disk,
  CPU, and background load. Use runner resource locks and do not parallelize
  benchmark agents.

---

## 14. Recommendation

After Packet O, the next high-leverage AOF work is no longer baseline repair,
basic background BASE generation, manifest fsync hardening, successful-rewrite
history deletion, command-path snapshot cloning, syscall-level rewrite
publication fault injection, held-snapshot COW visibility, startup cleanup, or
basic multipart checking. Those surfaces now have production code plus
frontier/process/bench evidence, and `integration/aof` is down to one remaining
runtime-yield failure.

The next pragmatic move is Packet P: runtime-yield and remaining focused TCL
classification.

Ambitious Packet P end state:

- `integration/aof`, `unit/aofrw`, `unit/other`, and `integration/rdb` either
  pass in focused survey or each remaining test is classified with a concrete
  unsupported-surface reason.
- `DEBUG LOADAOF` plus timed-out scripts either yield enough through
  RuntimeOwner to expose upstream `LOADING` behavior, or the missing
  `processEventsWhileBlocked` runtime work is specified as a separate release
  dependency.
- Function rewrite/replay support is implemented far enough that upstream AOF
  rewrite function tests no longer abort at `ERR Function not found`, or the
  missing function subsystem is documented as the gating non-AOF dependency.
- RDB integration timeouts are split into a server bug, a harness lifecycle
  bug, or an unsupported upstream fixture.

Packet P tool loop:

1. Run focused TCL once, then work one file at a time from earliest abort to
   first semantic failure:
   `python3 harness/oracle/tcl-survey.py --runner-id tcl-persistence-focused --skip-build --timeout-s 180 --no-default-deny-tags --deny-tag needs:repl --deny-tag cluster --files unit/other,unit/aofrw,integration/aof,integration/rdb`.
2. For each blocker, add the smallest Rust unit, process frontier scenario, or
   TCL-survey assertion that reproduces the failure outside the full TCL run.
3. Fix production behavior, rerun that small reproducer, then rerun the focused
   TCL file.
4. Keep `cargo test -p redis-commands --test aof_correctness_kit`,
   `cargo test -p redis-server`, and the 48-scenario persistence frontier green
   after every semantic fix.
5. Run the quick AOF matrix and rewrite-latency telemetry only after correctness
   moves, or immediately if a fix touches append, fsync, rewrite, manifest, RDB,
   or RuntimeOwner flush order.

Packet P should be considered complete only when the focused TCL status has
moved from "parsed and red" to either "passing" or "precisely classified with
source-backed gaps." That is the shortest path from alpha AOF toward a real
release gate.

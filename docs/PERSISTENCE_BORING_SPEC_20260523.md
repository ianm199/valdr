# Persistence Boring Spec - RDB + AOF restart/load/rewrite

Status: architecture handoff, 2026-05-23.

Audience: GPT-5.3 Codex Spark or a similar broad code-generation agent. This
is intentionally more concrete than a product note: it names source files,
current Rust files, upstream Valkey anchors, failure modes, packet boundaries,
and gates.

Goal: make persistence boring enough that a single-node user can trust:

- write workload -> restart -> data is still there
- write workload -> SAVE/BGSAVE -> restart from RDB -> data is still there
- appendonly yes -> restart from AOF -> data is still there
- appendonly yes -> BGREWRITEAOF while writes continue -> restart -> no writes
  lost
- upstream TCL persistence tests can be used as gates instead of being hidden
  behind `needs:debug`, `external:skip`, or stubbed `DEBUG RELOAD`/`LOADAOF`

This is not "implement every persistence-adjacent feature in Valkey." It is a
specific reliability milestone: persistence should stop being a special-case
partial implementation and become an operator-trustable subsystem.

---

## Executive summary

RDB is already the strong side of this port. The repo has a real RDB codec in
`crates/redis-core/src/rdb/`, and the harness reports 378/378 RDB
bidirectional oracle assertions passing:

```bash
python3 harness/oracle/rdb-diff --direction=all
```

AOF is the weak side. It exists, but it is not yet "boring":

- startup replay is best-effort and silently skips parse/command failures
- AOF rewrite can lose writes because the child rewrites a temp file and
  renames it over the live append file while the parent may have appended more
  commands to the old file
- `DEBUG RELOAD` and `DEBUG LOADAOF` are currently fake `+OK` shims in the
  command path
- `INFO persistence` is mostly hardcoded
- Valkey's modern multi-part AOF manifest layout is not implemented
- command propagation is "append original write argv after handler", which is
  simpler than Valkey's dirty/force/prevent/also-propagate model and breaks
  edge cases like `GETEX`, `SPOP count`, transactions, and commands rewritten
  into DEL/PEXPIREAT-style effects

The single most important correctness bug to avoid: do not ship
`BGREWRITEAOF` by atomically replacing the current append file in the child.
That loses writes that happen between fork and rename. Valkey avoids this with
a parent-owned finalization step plus a new incremental AOF file and manifest.

Recommended v1 architecture:

```text
redis-server
  startup load order
  process lifecycle
  child reapers for BGSAVE/BGREWRITEAOF
  shutdown SAVE/NOSAVE/flush

redis-commands
  command handlers: SAVE/BGSAVE/BGREWRITEAOF/DEBUG/CONFIG/INFO
  AOF writer, AOF replay, rewrite emission, propagation sink

redis-core
  RedisDb, RedisObject, RedisServer, LiveConfig
  pure RDB codec
  persistence status structs / metrics only
```

Do not put AOF disk lifecycle into `redis-core`: `redis-core` cannot depend on
command handlers, but AOF replay must execute commands. Keep pure codecs in
core; keep command replay and command propagation in `redis-commands`; keep
process/child lifecycle in `redis-server`.

---

## Source model in upstream Valkey

Persistence in Valkey is not one feature. It is the combination of:

1. RDB binary snapshot format.
2. AOF command log format.
3. Startup load selection.
4. Command propagation rules.
5. Background child lifecycle.
6. Rewrite finalization.
7. Runtime durability policy.
8. Debug/test hooks used by the TCL suite.

### RDB source anchors

Main upstream files:

- `reference/valkey/src/rdb.h`
- `reference/valkey/src/rdb.c`
- `reference/valkey/src/server.c`

Important functions and constants:

| Upstream anchor | What it owns | Current Rust analog |
|---|---|---|
| `rdb.h` `RDB_VERSION`, type bytes, opcodes | Binary vocabulary | `crates/redis-core/src/rdb/header.rs` |
| `rdb.c::rdbSaveKeyValuePair` | Per-key expire/type/key/value save | `crates/redis-core/src/rdb/save.rs` |
| `rdb.c::rdbSaveInfoAuxFields` | AUX metadata (`valkey-ver`, `aof-base`) | `write_aux_fields` |
| `rdb.c::rdbSaveDb` | SELECTDB/RESIZEDB + DB iteration | `save_rdb_databases` |
| `rdb.c::rdbSaveRio` | Whole RDB framing + EOF + CRC | `write_rdb_dbs_to_buf` |
| `rdb.c::rdbSaveBackground` | forked BGSAVE | `persist.rs::bgsave_command` |
| `rdb.c::rdbLoadRioWithLoadingCtx` | Whole RDB load state machine | `rdb/load.rs::load_into_dbs` |
| `rdb.c::rdbLoadObject` | Type-specific load | `rdb/load.rs::load_value_payload` |
| `server.c::loadDataFromDisk` | Startup chooses AOF vs RDB | `redis-server/src/main.rs` startup block |

The RDB file shape:

```text
magic header: VALKEY080 or REDIS0NNN
AUX fields
zero or more:
  SELECTDB dbid
  RESIZEDB key_count expire_count
  zero or more key records:
    optional EXPIRETIME_MS / EXPIRETIME
    optional IDLE / FREQ
    type byte
    key string
    value payload
EOF
CRC64
```

Current Rust RDB implementation mostly matches this and is already gated by
the bidirectional oracle. The remaining RDB work for "boring persistence" is
not "write an RDB codec from scratch." It is:

- make startup failure behavior strict instead of log-and-continue
- implement real `DEBUG RELOAD`
- close loader compatibility gaps for compact/legacy encodings used by the
  broader upstream tests
- make `INFO persistence` report true RDB state

### AOF source anchors

Main upstream files:

- `reference/valkey/src/aof.c`
- `reference/valkey/src/server.c`
- `reference/valkey/src/server.h`
- `reference/valkey/src/config.c`

Important functions and fields:

| Upstream anchor | What it owns | Current Rust analog |
|---|---|---|
| `server.h` AOF fields around `aof_state`, `aof_manifest`, sizes/statuses | Runtime state | partial fields in `LiveConfig`, global `AOF_WRITER`, missing stats |
| `aof.c::feedAppendOnlyFile` | SELECT insertion + append command to buffer | `aof.rs::AofWriter::append_selected` |
| `aof.c::flushAppendOnlyFile` | write/fsync before replies leave event loop | `AofWriter` writes immediately; fsync thread |
| `aof.c::createAOFClient` | fake client for replay | `dispatch_via_handler` creates minimal synthetic client |
| `aof.c::loadSingleAppendOnlyFile` | strict RESP/RDB-preamble load | `aof.rs::replay_aof_databases` |
| `aof.c::loadAppendOnlyFiles` | manifest/base/incr loader | missing |
| `aof.c::rewriteAppendOnlyFileRio` | command-form rewrite | `write_aof_rewrite_for_dbs` |
| `aof.c::rewriteAppendOnlyFile` | write base file, optionally RDB preamble | partial `do_aof_rewrite` |
| `aof.c::rewriteAppendOnlyFileBackground` | parent opens new INCR, child writes BASE | missing |
| `aof.c::backgroundRewriteDoneHandler` | parent finalizes manifest/renames | missing |
| `server.c::beforeSleep` | flush AOF before client writes | not modeled exactly |
| `server.c::call` + `alsoPropagate` | dirty/force/prevent/extra propagation | simplified in `dispatch.rs` |

The upstream AOF rewrite lifecycle is the key architecture shape:

```text
client calls BGREWRITEAOF
  parent verifies no active child
  parent opens a new temporary INCR AOF for new writes
  fork
    child writes a BASE file from the fork snapshot
    child exits
  parent keeps serving writes into the new INCR file
  reaper sees child success
  parent renames child temp BASE into official BASE name
  parent renames temp INCR into official INCR name
  parent persists manifest atomically
  parent marks old files as history and deletes them later
```

That is why writes are not lost. The snapshot is BASE, the post-fork writes are
INCR, and the manifest names both.

The current Rust implementation does this instead:

```text
client calls BGREWRITEAOF
  fork/thread
    child writes compact temp file
    child renames temp over live appendonly.aof
  parent may have appended writes to live appendonly.aof during rewrite
  child rename can discard those parent writes
```

That is not a performance issue. It is a correctness issue.

---

## Current Rust implementation map

### Files that already exist

| File | Current role | Keep / change |
|---|---|---|
| `crates/redis-core/src/rdb/mod.rs` | RDB module surface | keep |
| `crates/redis-core/src/rdb/save.rs` | RDB save, DUMP payloads | keep, add options for AOF preamble if needed |
| `crates/redis-core/src/rdb/load.rs` | RDB load, RESTORE payloads | keep, add load options and compact encodings |
| `crates/redis-core/src/rdb/{string,hash,list,set,zset,stream,listpack,lzf,crc,varint,header}.rs` | Type codecs | keep |
| `crates/redis-commands/src/aof.rs` | AOF writer/replay/rewrite | heavily refactor |
| `crates/redis-commands/src/persist.rs` | SAVE/BGSAVE/BGREWRITEAOF/DUMP/RESTORE | refactor BGREWRITEAOF, add status |
| `crates/redis-commands/src/dispatch.rs` | post-handler AOF append | add propagation sink |
| `crates/redis-commands/src/connection.rs` | CONFIG, DEBUG, SHUTDOWN | replace persistence shims |
| `crates/redis-commands/src/info.rs` | INFO/LASTSAVE | wire persistence fields |
| `crates/redis-server/src/main.rs` | startup load + child reapers | strict load, AOF reaper, shutdown |
| `crates/redis-core/src/live_config.rs` | live config | add missing AOF config |
| `crates/redis-core/src/server.rs` | server status atomics | add AOF rewrite/status fields |
| `harness/oracle/rdb-diff` | existing RDB oracle | keep as gate |
| `harness/runners.toml` | runner registry | add persistence runners later |

### What is good today

- RDB codec is real.
- RDB has bidirectional oracle coverage.
- `SAVE` snapshots all DBs via `CommandContext::snapshot_all_dbs`.
- `BGSAVE` uses `fork(2)` on Unix and a reaper updates save status.
- AOF writer can append RESP commands with `SELECT`.
- AOF rewrite emitter can reconstruct common types in command form.
- Startup has a hook for RDB load and AOF replay into owner DBs before the
  runtime loop starts.

### What is not boring today

1. **Startup load errors are not fatal.**

   In `redis-server/src/main.rs`, RDB/AOF load failures are printed to stderr
   and the server keeps running. Valkey exits on corrupt persistence files.

2. **AOF replay is best-effort.**

   `aof.rs::replay_aof_databases` skips parse errors by advancing to the next
   newline. `dispatch_replay_command` silently drops unknown commands. That is
   exactly the opposite of startup persistence correctness.

3. **AOF rewrite can lose writes.**

   `persist.rs::do_aof_rewrite` renames the rewritten file over the final AOF
   path from the child/thread. There is no parent finalization step and no
   incremental file for writes that happened during rewrite.

4. **`DEBUG RELOAD` and `DEBUG LOADAOF` are fake OKs.**

   Dispatch routes DEBUG to `redis-commands/src/connection.rs::debug_command`.
   That handler returns `+OK` for `RELOAD` and `LOADAOF` without reloading
   anything. There is also a deeper `redis-core/src/debug.rs` implementation
   with TODO errors, but the active dispatch table currently points at
   `redis-commands`.

5. **AOF command propagation is too simple.**

   `dispatch.rs` appends the original write argv after a successful handler.
   Valkey uses dirty counts, command flags, `forceCommandPropagation`,
   `preventCommandAOF`, and `alsoPropagate`. This matters for commands whose
   observable mutation is not exactly the original command vector.

6. **`INFO persistence` is mostly wrong.**

   `info.rs` has hardcoded or stale fields: `aof_enabled:0`,
   `rdb_bgsave_in_progress:0`, and last-save values that do not consistently
   reflect real persistence state.

7. **Modern multi-part AOF is missing.**

   Upstream tests assume `appenddirname appendonlydir`, manifest file, BASE and
   INCR file naming, and rewrite sequence numbers. Current Rust uses one
   append file.

---

## Definition of done: "boring persistence v1"

This milestone should be considered done when all of these are true.

### User-visible behavior

- `SAVE` writes an RDB, returns `+OK`, and restart loads it.
- `BGSAVE` starts a background save, `INFO persistence` reports it, reaper
  clears it, `LASTSAVE` updates on success.
- startup with a corrupt RDB or AOF fails loudly instead of serving an empty DB.
- if `appendonly yes`, startup loads AOF state rather than silently preferring
  an older RDB.
- AOF replay errors are fatal except for explicitly allowed truncation cases.
- `BGREWRITEAOF` does not lose writes made during rewrite.
- `DEBUG RELOAD` actually saves/reloads or reloads without saving according to
  its options.
- `DEBUG LOADAOF` actually reloads from AOF.
- `SHUTDOWN SAVE` does a foreground save/flush before exit; `SHUTDOWN NOSAVE`
  skips it.
- `CONFIG SET appendonly yes/no`, `appendfsync`, `appendfilename`,
  `appenddirname`, `aof-load-truncated`, and `aof-use-rdb-preamble` have live
  behavior or honest errors.

### Harness gates

- Existing RDB oracle still passes:

  ```bash
  python3 harness/oracle/rdb-diff --direction=all
  ```

- New restart cycle runner passes:

  ```bash
  python3 harness/oracle/persistence-cycle.py --mode rdb
  python3 harness/oracle/persistence-cycle.py --mode aof
  python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
  ```

- Focused upstream TCL persistence files are runnable:

  ```bash
  bash harness/oracle/setup_tcl_runner.sh --skip-build
  cd reference/valkey
  VALKEY_BIN_DIR=$(pwd)/../../target/debug \
    tclsh tests/test_helper.tcl --single unit/other \
    --clients 1 --skip-leaks --tags "-needs:repl -cluster"

  VALKEY_BIN_DIR=$(pwd)/../../target/debug \
    tclsh tests/test_helper.tcl --single unit/aofrw \
    --clients 1 --skip-leaks --tags "-needs:repl -cluster"

  VALKEY_BIN_DIR=$(pwd)/../../target/debug \
    tclsh tests/test_helper.tcl --single integration/aof \
    --clients 1 --skip-leaks --tags "-needs:repl -cluster"
  ```

The exact tag filters may need tuning, but the target is clear: persistence
tests should fail for real missing behavior, not because the runner cannot
start or because DEBUG commands lie.

---

## Architecture decision: use a persistence runtime spine

### Recommended shape

Add a typed persistence spine without doing a giant crate move:

```text
crates/redis-core/src/persistence.rs        # state structs/enums only
crates/redis-commands/src/aof.rs            # AOF codec/writer/replay/rewrite
crates/redis-commands/src/persistence.rs    # command-facing helpers
crates/redis-server/src/main.rs             # lifecycle/reapers/startup
```

The core state should be owned by `RedisServer` or `LiveConfig`, not scattered
as ad-hoc globals:

```rust
pub enum AofState {
    Off,
    On,
    WaitRewrite,
}

pub enum PersistenceStatus {
    Ok,
    Err { errno_or_msg: String },
}

pub struct PersistenceStats {
    pub rdb_bgsave_in_progress: bool,
    pub aof_rewrite_in_progress: bool,
    pub aof_rewrite_scheduled: bool,
    pub rdb_last_save_time: i64,
    pub rdb_last_bgsave_status: PersistenceStatus,
    pub aof_last_bgrewrite_status: PersistenceStatus,
    pub aof_last_write_status: PersistenceStatus,
    pub aof_current_size: u64,
    pub aof_base_size: u64,
}
```

Minimum config additions in `LiveConfig`:

- `appenddirname` (default `appendonlydir`)
- `aof_load_truncated` (default should match upstream)
- `aof_use_rdb_preamble` (default yes upstream)
- `auto_aof_rewrite_percentage`
- `auto_aof_rewrite_min_size`
- `rdb_checksum` if not already modeled
- `save` params if tests need dirty-counter behavior

Keep `AOF_WRITER` temporarily if needed, but treat it as an implementation
detail behind the persistence runtime. The long-term shape should not require
command handlers to know there is a global OnceLock.

### Why not a new crate immediately?

A new `redis-persistence` crate sounds clean, but it creates dependency
tension:

- RDB is already in `redis-core`.
- AOF replay needs command dispatch from `redis-commands`.
- server startup/reaping belongs in `redis-server`.

Moving all of that now is churn. For this milestone, add typed state and
correct lifecycle while preserving existing crate boundaries. Once behavior is
gated, a crate extraction is mechanical.

---

## Data-flow diagrams

### Startup load

Desired:

```text
process start
  parse config/CLI
  create LiveConfig + RedisServer
  create owner_dbs Vec<RedisDb>
  if appendonly yes:
      load AOF manifest or legacy AOF into owner_dbs
      fatal on corrupt/missing-required file
      open current incremental writer
  else if RDB exists and rdb not disabled:
      load RDB into owner_dbs
      fatal on corrupt file
  install AOF writer if appendonly yes
  start fsync/reaper threads
  start RuntimeOwner with owner_dbs
```

Current issue:

```text
RDB load error -> eprintln -> continue serving empty/stale DB
AOF replay error -> eprintln -> continue
if both RDB and AOF exist -> loads RDB then AOF, not Valkey's AOF-preferred path
```

### Command propagation

Desired:

```text
handler mutates DB
  handler may:
    prevent AOF
    force AOF
    add extra propagation commands
    rewrite propagation argv
dispatch finalizes execution unit
  if dirty or forced:
    append propagation events to AOF
    send same events to replication
```

Current:

```text
handler returns Ok and command metadata says write
  append original client argv to AOF
```

This works for simple `SET`, `HSET`, `SADD`. It is not enough for commands
with random pops, blocking pops, transactions, command rewrites, and commands
that are flagged write but do not mutate.

### BGREWRITEAOF

Correct shape:

```text
parent:
  snapshot DBs
  create/open new temporary INCR AOF for post-fork writes
  switch active writer to new INCR
  fork child

child:
  write BASE from snapshot
  fsync BASE
  exit

parent reaper:
  if child ok:
    finalize BASE name
    finalize INCR name
    atomically write manifest
    update stats
    keep writing to active INCR
  if child failed:
    discard temp BASE
    continue with old AOF path
```

Bad shape to avoid:

```text
child writes temp and renames over active appendonly.aof
```

That bad shape loses writes.

---

## Implementation phases and packets

The phases below are ordered. Spark should not implement AOF rewrite before
strict replay and status exist; otherwise tests will greenwash a data-loss
path.

### Phase 0 - persistence status spine

Purpose: create the state that future packets update and `INFO` reports.

Files:

- `crates/redis-core/src/persistence.rs` (new)
- `crates/redis-core/src/server.rs`
- `crates/redis-core/src/live_config.rs`
- `crates/redis-commands/src/info.rs`
- `crates/redis-commands/src/connection.rs`

Work:

1. Add typed AOF/RDB status enums and counters.
2. Add `appenddirname`, `aof-load-truncated`, `aof-use-rdb-preamble`,
   auto-rewrite knobs to `LiveConfig`.
3. Wire `CONFIG GET/SET` for those fields.
4. Replace `INFO persistence` hardcoded fields with actual values:
   - `loading`
   - `rdb_changes_since_last_save`
   - `rdb_bgsave_in_progress`
   - `rdb_last_save_time`
   - `rdb_last_bgsave_status`
   - `aof_enabled`
   - `aof_rewrite_in_progress`
   - `aof_rewrite_scheduled`
   - `aof_last_bgrewrite_status`
   - `aof_last_write_status`
   - `aof_current_size`
5. Keep names close to upstream where possible so TCL `s <field>` helpers work.

Gates:

```bash
cargo test --workspace info persistence
bash harness/oracle/smoke.sh --skip-build
```

Do not fake values just to satisfy INFO tests. If a field cannot be known yet,
wire it to the new state and let later packets update it.

### Phase 1 - strict startup RDB and real DEBUG RELOAD

Purpose: make RDB load/reload a real operator path, not only an oracle path.

Files:

- `crates/redis-core/src/rdb/load.rs`
- `crates/redis-server/src/main.rs`
- `crates/redis-commands/src/connection.rs`
- `crates/redis-commands/src/persist.rs`
- maybe `redis_core::CommandContext` methods if all-DB mutation needs a helper

Work:

1. Add RDB load options:

   ```rust
   pub struct RdbLoadOptions {
       pub allow_dup: bool,
       pub skip_expired: bool,
       pub aof_preamble: bool,
   }
   ```

   Upstream uses flags like `RDBFLAGS_ALLOW_DUP` and
   `RDBFLAGS_AOF_PREAMBLE`. We need at least `ALLOW_DUP` for `DEBUG RELOAD
   MERGE/NOFLUSH` semantics and `AOF_PREAMBLE` for base-AOF RDB loads.

2. Make startup RDB load fatal by default.

   If an RDB exists and `load_into_dbs` errors, exit nonzero. Do not continue
   serving an empty DB.

3. Implement real `DEBUG RELOAD [MERGE] [NOFLUSH] [NOSAVE]` in the active
   command path (`redis-commands/src/connection.rs::debug_command` today).

   Expected behavior shape:

   - default: save RDB, flush DBs, load RDB
   - `NOSAVE`: skip the save step
   - `NOFLUSH`: do not clear current DBs before load
   - `MERGE`: allow duplicate keys to replace existing keys during load

   The exact upstream nuance is in `redis-core/src/debug.rs` comments and
   `rdb.c` duplicate-key logic. The important thing for v1: it must actually
   reload the DBs the server is using.

4. Remove or replace the fake `DEBUG RELOAD -> +OK` shim.

Gates:

```bash
python3 harness/oracle/rdb-diff --direction=all
VALKEY_BIN_DIR=$(pwd)/target/debug \
  tclsh reference/valkey/tests/test_helper.tcl --single unit/other \
  --clients 1 --skip-leaks --tags "-needs:repl -cluster"
```

Focused manual smoke:

```bash
tmp=$(mktemp -d)
target/debug/redis-server --port 6380 --dir "$tmp" &
pid=$!
redis-cli -p 6380 set x 1
redis-cli -p 6380 save
redis-cli -p 6380 set x 2
redis-cli -p 6380 debug reload nosave
redis-cli -p 6380 get x   # should be 1
kill $pid
```

### Phase 2 - RDB loader compatibility expansion

Purpose: remove the current "set config to avoid compact encodings" caveat.

Files:

- `crates/redis-core/src/rdb/load.rs`
- `crates/redis-core/src/rdb/hash.rs`
- `crates/redis-core/src/rdb/set.rs`
- `crates/redis-core/src/rdb/zset.rs`
- `crates/redis-core/src/rdb/listpack.rs`
- `crates/redis-ds/src/ziplist.rs` if legacy ziplist decoding is reused

Current `load_value_payload` returns unsupported for:

- `RDB_TYPE_HASH_ZIPLIST`
- `RDB_TYPE_HASH_LISTPACK`
- `RDB_TYPE_HASH_2`
- `RDB_TYPE_LIST_ZIPLIST`
- `RDB_TYPE_LIST_QUICKLIST` legacy v1
- `RDB_TYPE_SET_INTSET`
- `RDB_TYPE_SET_LISTPACK`
- `RDB_TYPE_ZSET`
- `RDB_TYPE_ZSET_ZIPLIST`
- `RDB_TYPE_ZSET_LISTPACK`
- legacy stream v1

For boring persistence v1, prioritize:

1. `SET_INTSET`
2. `SET_LISTPACK`
3. `HASH_LISTPACK`
4. `ZSET_LISTPACK`
5. `HASH_2` enough to read field expiries and either apply or explicitly
   discard them without desynchronizing the stream

Do not block boring v1 on Redis 2-era ziplist formats unless upstream tests
we choose to gate require them. The doc should mark them as compatibility v2.

Gates:

- existing RDB oracle still 378/378
- add a new C-saved corpus with default compact encodings enabled
- run `unit/type/set`, `unit/type/zset`, and `unit/type/hash` reload cases
  that previously relied on `DEBUG RELOAD`

### Phase 3 - strict AOF loader

Purpose: AOF startup must be deterministic and fail-closed.

Files:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-server/src/main.rs`
- `crates/redis-commands/src/connection.rs`

Work:

1. Replace best-effort replay with a strict loader:

   ```rust
   pub struct AofLoadOptions {
       pub load_truncated: bool,
       pub allow_rdb_preamble: bool,
       pub manifest_dir: PathBuf,
   }

   pub enum AofLoadStatus {
       NotExist,
       Empty,
       Loaded { commands: usize },
       TruncatedLoaded { commands: usize },
   }
   ```

2. Unknown command during AOF replay is fatal.
3. Parse error in the middle of a command is fatal unless it is exactly the
   permitted truncated-tail case and `aof-load-truncated yes`.
4. Incomplete `MULTI` is allowed only under the same truncated-tail policy.
5. Replay must run as a fake AOF client:
   - authenticated
   - not blocked
   - not re-propagating commands to AOF
   - not writing slowlog/commandlog if we decide to match upstream loading
6. Comments (`#...`) and timestamp annotations (`#TS:<unix>`) are skipped.
7. If an AOF file begins with `REDIS` or `VALKEY`, load it as an RDB preamble
   then continue with the AOF tail if present.
8. `DEBUG LOADAOF` should flush/reload from AOF and update the live DBs.
9. Startup with `appendonly yes` must prefer AOF over RDB, matching
   `server.c::loadDataFromDisk`.

Gates from upstream `integration/aof.tcl`:

- lines 14-69: truncated AOF starts when configured yes, loads valid prefix
- lines 71-107: bad format/short read with `aof-load-truncated no` fails
- lines 148-185: SPOP AOF loads with correct cardinality
- lines 187-204: PEXPIREAT during load
- lines 255-266: unknown command is fatal
- lines 379-398: timestamp annotations load

Do not implement `valkey-check-aof` in this packet. Tests that execute the
utility can be excluded until a dedicated utility plan exists.

### Phase 4 - propagation sink

Purpose: AOF should receive what Valkey would propagate, not blindly every
write command's original argv.

Files:

- `crates/redis-core/src/client.rs`
- `crates/redis-core/src/command_context.rs` or wherever `CommandContext`
  methods live
- `crates/redis-commands/src/dispatch.rs`
- individual command files for edge cases:
  - strings / expire
  - set
  - list blocking pop
  - zset blocking pop
  - transactions
  - scripting if included

Recommended interface:

```rust
pub enum PropagationTarget {
    Aof,
    Replication,
    Both,
}

pub struct PropagationEvent {
    pub db_id: i32,                 // -1 means no SELECT, like upstream
    pub argv: Vec<RedisString>,
    pub target: PropagationTarget,
}

pub struct PropagationState {
    pub prevent_aof: bool,
    pub prevent_repl: bool,
    pub force_aof: bool,
    pub force_repl: bool,
    pub also: Vec<PropagationEvent>,
    pub replacement: Option<Vec<RedisString>>,
}
```

Command handlers need methods:

- `ctx.prevent_aof()`
- `ctx.force_aof()`
- `ctx.also_propagate(db_id, argv, target)`
- `ctx.rewrite_propagation(argv)`

Dispatch should:

1. record dirty count before handler
2. run handler
3. if success and not blocked:
   - if dirty or forced, propagate replacement or original argv unless
     prevented
   - then propagate queued `also` events
   - if multiple `also` events need transaction wrapping, emit MULTI/EXEC like
     upstream `propagatePendingCommands`

Targeted edge cases:

- `GETEX key` with no expiry option should not append to AOF.
- `EXPIRE key -1` should produce the same persisted restart state Valkey does.
- `SPOP key count` must persist exactly the removed members / resulting set
  semantics.
- blocking list/zset pops must persist the actual pop effects, not the blocked
  command shape.
- transactions must persist as a coherent unit, not as queue-time commands.

Gates:

- `integration/aof.tcl` GETEX and SPOP sections
- focused custom AOF content tests that compare file growth and restart state
- existing wire-smoke
- replication smoke if touched

### Phase 5 - safe AOF rewrite

Purpose: `BGREWRITEAOF` must not lose writes.

Files:

- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/persist.rs`
- `crates/redis-server/src/main.rs`
- `crates/redis-core/src/persistence.rs`
- `crates/redis-core/src/server.rs`
- `crates/redis-core/src/live_config.rs`

There are two viable implementation strategies.

#### Option A: manifest-lite, Valkey-shaped (recommended)

Implement enough modern multi-part AOF to match Valkey's layout:

```text
dir/
  appendonlydir/
    appendonly.aof.manifest
    appendonly.aof.1.base.aof     or appendonly.aof.1.base.rdb
    appendonly.aof.1.incr.aof
```

Manifest line shape:

```text
file appendonly.aof.1.base.aof seq 1 type b
file appendonly.aof.1.incr.aof seq 1 type i
```

Work:

1. Add an `AofManifest` parser/writer.
2. On startup:
   - if manifest exists, validate it
   - load at most one BASE plus ordered INCR files
   - fail on missing required files, duplicate base, non-monotonic seq, invalid
     line shape
   - if legacy single `appendfilename` exists and no manifest exists, load it
     and optionally upgrade into manifest layout
3. On appendonly enable:
   - create `appenddirname`
   - if DB non-empty, create BASE + INCR via rewrite or immediate base write
   - install current INCR writer
4. On BGREWRITEAOF:
   - parent opens temp/current next INCR and switches active writer
   - child writes temp BASE from snapshot, either command-form AOF or RDB
     preamble depending `aof-use-rdb-preamble`
   - parent reaper finalizes manifest after child success
5. Old files can be left as history for v1 or deleted synchronously later.

Pros:

- matches upstream test assumptions
- no in-memory delta buffer
- safer for long rewrites
- directly supports AOF rewrite under write load

Cons:

- more code: manifest parser, file naming, startup validation

#### Option B: single-file delta buffer (only if time-boxed)

Keep one AOF file, but fix data loss:

1. parent forks child to write temp compact base
2. parent keeps appending to old AOF and also records a rewrite delta buffer
3. when child exits OK, parent appends delta buffer to temp file and renames
   temp over final file

Pros:

- less code
- catches the core no-data-loss invariant

Cons:

- delta buffer can grow without bound
- does not match modern Valkey tests
- still requires careful parent finalization/reaper

Recommendation: implement Option A if Spark can handle a large surface. If the
experiment is to see how well Spark fills code from a spec, this is the right
test. If it stalls, fall back to Option B as an explicit intermediate commit,
not as the final product claim.

Required gates:

- custom rewrite-under-load runner:

  ```text
  appendonly yes
  write keys 0..N
  start BGREWRITEAOF
  concurrently write keys N..M
  wait rewrite complete
  capture digest/key count
  restart from AOF
  assert digest/key count equal
  ```

- upstream `unit/aofrw.tcl` first test: rewrite during write load.
- upstream `integration/aof-multi-part.tcl` load validation subset if using
  manifest-lite.

### Architect addendum - multi-part AOF manifest wave

Packet `persistence-aof-manifest-architecture-v1` reviewed the
`20260523T195221Z` persistence frontier after AOF propagation fixes. The old
RDB, strict AOF, propagation, and collection rewrite scenarios were green
(`11/12` total), and the only red row was
`multipart-aof-manifest-basic-load`. That means manifest work is no longer
blocked by legacy single-file AOF failures.

Decision: implement Option A as a bounded manifest-lite wave, not a single
large rewrite. The Rust port should keep the AOF disk lifecycle in
`crates/redis-commands/src/aof.rs` and expose small lifecycle functions to
`redis-server/src/main.rs`; do not add a new crate and do not move pure RDB
code out of `redis-core`.

Required manifest contracts for v1:

- Manifest grammar follows `aof.c::aofLoadManifestFromFile`: six required
  fields per line (`file <name> seq <n> type <b|i|h>`), basename-only file
  names, comments allowed, blank lines fatal, unknown extra key/value pairs
  ignored for forward compatibility, duplicate BASE fatal, and INCR sequence
  numbers strictly increasing.
- Startup with `appendonly yes` loads the manifest from
  `<dir>/<appenddirname>/<appendfilename>.manifest` when present, then replays
  at most one BASE followed by ordered INCR files. A missing required manifest
  file is fatal; an absent manifest and absent legacy AOF is a clean empty
  startup.
- BASE `.aof` and INCR `.aof` files use the strict AOF loader. BASE `.rdb`
  files use the existing RDB loader with AOF-preamble semantics. A truncated
  file is allowed only for the final replay file and only when
  `aof-load-truncated yes` is configured.
- New AOF state writes into the last INCR file under `appenddirname`; the root
  `appendfilename` remains only as a legacy upgrade input.
- `BGREWRITEAOF` must use parent-owned finalization: switch future writes to a
  new INCR before finalizing a BASE, then atomically persist the manifest after
  the BASE is durable. The v1 implementation may keep the current conservative
  synchronous command section, but no child/thread may rename over the active
  writer.
- Old BASE/INCR files may remain as history in v1. Deleting history files,
  multiple failed rewrite INCR accumulation, and background garbage collection
  are correctness v2 unless a source-shaped runner is added first.

Frontier scenario expansion for `persistence-aof-manifest-frontier-scenarios-v1`:

`harness/oracle/persistence-frontier.py` is the packet owner for this proof.
It must remain a telemetry runner: every row emits one measurement, and red
rows are preserved as implementation targets instead of being normalized away.
The scenarios are shaped from `integration/aof-multi-part.tcl` and
`support/aofmanifest.tcl` without requiring the external TCL harness.

- keep `multipart-aof-manifest-basic-load`;
- add startup-failure rows for missing manifest targets, non-monotonic INCR
  sequence numbers, blank manifest lines, empty manifests, duplicate BASE
  entries, and unknown manifest file types;
- add green-path startup rows for an empty AOF directory, discontinuous but
  increasing INCR sequence numbers, and empty INCR files;
- add lifecycle rows for `CONFIG SET appendonly yes` creating the
  `appendonlydir` manifest layout and `BGREWRITEAOF` advancing BASE and INCR
  sequence numbers from the previously-loaded manifest.

Type/API ownership decision: do not add cross-crate `AofManifest` vocabulary
yet. Keep manifest structs private or crate-local in `redis-commands/src/aof.rs`
and expose functions such as `load_append_only_files`, `open_aof_on_start`, and
`rewrite_aof_with_manifest` only as needed by `redis-server` and
`persist.rs`. If a later packet makes manifest structs public across crates,
that packet must add a row to `harness/type-vocabulary.tsv`.

### Phase 6 - shutdown and runtime config behavior

Purpose: persistence decisions must hold at process lifecycle boundaries.

Files:

- `crates/redis-commands/src/connection.rs`
- `crates/redis-server/src/main.rs`
- `crates/redis-commands/src/aof.rs`
- `crates/redis-commands/src/persist.rs`

Work:

1. `SHUTDOWN SAVE`:
   - if RDB enabled or SAVE requested, perform foreground RDB save before exit
   - if AOF enabled, flush and fsync according to policy before exit
2. `SHUTDOWN NOSAVE`:
   - skip RDB save
   - still flush already-acknowledged AOF bytes if appendonly is enabled
3. `CONFIG SET appendonly yes`:
   - do not merely open a writer
   - create initial AOF state for current DB, usually via rewrite/base creation
   - wait or report background rewrite status like upstream support helpers
4. `CONFIG SET appendonly no`:
   - flush/close writer
   - if rewrite child active, kill/cancel it or mark failure
5. auto rewrite knobs can be telemetry-only initially, but the fields must not
   lie if exposed in CONFIG/INFO.

Gates:

- `unit/other.tcl` shutdown/save persistence cases
- `unit/aofrw.tcl` turning off AOF kills/cancels rewrite child, if in scope
- custom `SHUTDOWN SAVE` restart smoke

---

## Harness additions

The current RDB oracle is good. The missing piece is a persistence-cycle oracle
that exercises process death/restart and rewrite under writes.

### New runner: `persistence-rdb-cycle`

Proposed command:

```toml
[[runner]]
id = "persistence-rdb-cycle"
kind = "json_command"
surface = "correctness"
method = "restart-oracle"
workload = "rdb-save-restart"
command = ["python3", "harness/oracle/persistence-cycle.py", "--mode", "rdb"]
timeout_s = 900
resources = ["oracle-results", "cargo-target", "port-range:21111-29111"]
claim_level = "internal-regression-gate"
capabilities = ["persistence-rdb"]
```

Runner behavior:

1. create temp dir
2. start Rust server with `--dir temp`
3. populate strings/lists/hashes/sets/zsets/streams/TTLs
4. `SAVE`
5. stop server
6. restart with same dir
7. verify normalized keyspace

### New runner: `persistence-aof-cycle`

```toml
[[runner]]
id = "persistence-aof-cycle"
kind = "json_command"
surface = "correctness"
method = "restart-oracle"
workload = "aof-restart"
command = ["python3", "harness/oracle/persistence-cycle.py", "--mode", "aof"]
timeout_s = 900
resources = ["oracle-results", "cargo-target", "port-range:21111-29111"]
claim_level = "internal-regression-gate"
capabilities = ["persistence-aof"]
```

Runner behavior:

1. start with `appendonly yes`, `appendfsync always` for deterministic smoke
2. populate dataset
3. stop server
4. restart
5. verify keyspace
6. run again with `appendfsync everysec` and explicit wait/flush

### New runner: `persistence-aof-rewrite-cycle`

```toml
[[runner]]
id = "persistence-aof-rewrite-cycle"
kind = "json_command"
surface = "correctness"
method = "restart-oracle"
workload = "aof-rewrite-under-write-load"
command = ["python3", "harness/oracle/persistence-cycle.py", "--mode", "aof-rewrite"]
timeout_s = 1500
resources = ["oracle-results", "cargo-target", "port-range:21111-29111"]
claim_level = "internal-regression-gate"
capabilities = ["persistence-aof-rewrite"]
```

Runner behavior:

1. start appendonly yes
2. populate baseline
3. start writer thread/process that writes known keys while rewrite runs
4. issue `BGREWRITEAOF`
5. wait until INFO says rewrite done
6. stop writer
7. compute digest by querying the live server
8. stop/restart from AOF
9. compute digest again
10. fail if different

Digest should be a normalized keyspace digest, not the current fake
`DEBUG DIGEST` shim. Use SCAN + type-specific reads and sort unordered
structures.

### New runner: `tcl-persistence-core`

```toml
[[runner]]
id = "tcl-persistence-core"
kind = "json_command"
surface = "correctness"
method = "official-suite"
workload = "upstream-persistence-core"
command = [
  "python3",
  "harness/oracle/tcl-survey.py",
  "--skip-build",
  "--timeout-s",
  "180",
  "--files",
  "unit/other,unit/aofrw,integration/aof,integration/aof-multi-part"
]
timeout_s = 3600
resources = ["oracle-results", "cargo-target", "port-range:21111-29111"]
claim_level = "telemetry"
capabilities = ["official-tcl-coverage", "persistence"]
```

This should be telemetry first. Promote to regression gate only after the
expected skips are explicit.

---

## Packet graph

Suggested work packets:

```json
{"id":"persist-0-state-spine","role":"architect","selector":"manual","resources":["persistence-state"],"targets":["crates/redis-core/src/persistence.rs","crates/redis-core/src/server.rs","crates/redis-core/src/live_config.rs","crates/redis-commands/src/info.rs","crates/redis-commands/src/connection.rs"]}
{"id":"persist-0-info-gate","role":"runner","runner":"wire-smoke","depends_on":["persist-0-state-spine"]}

{"id":"persist-1-rdb-strict-reload","role":"translator","selector":"manual","depends_on":["persist-0-state-spine"],"resources":["persistence-state","rdb-file","cargo-target"],"targets":["crates/redis-core/src/rdb/load.rs","crates/redis-server/src/main.rs","crates/redis-commands/src/connection.rs","crates/redis-commands/src/persist.rs"]}
{"id":"persist-1-rdb-cycle","role":"runner","runner":"persistence-rdb-cycle","depends_on":["persist-1-rdb-strict-reload"]}
{"id":"persist-1-rdb-bidirectional","role":"runner","script":"python3 harness/oracle/rdb-diff --direction=all","depends_on":["persist-1-rdb-strict-reload"]}

{"id":"persist-2-rdb-compact-loaders","role":"translator","selector":"manual","depends_on":["persist-1-rdb-strict-reload"],"resources":["rdb-file"],"targets":["crates/redis-core/src/rdb/load.rs","crates/redis-core/src/rdb/hash.rs","crates/redis-core/src/rdb/set.rs","crates/redis-core/src/rdb/zset.rs","crates/redis-core/src/rdb/listpack.rs"]}
{"id":"persist-2-rdb-compact-gate","role":"runner","runner":"persistence-rdb-cycle","depends_on":["persist-2-rdb-compact-loaders"]}

{"id":"persist-3-aof-strict-loader","role":"translator","selector":"manual","depends_on":["persist-0-state-spine"],"resources":["aof-file","persistence-state"],"targets":["crates/redis-commands/src/aof.rs","crates/redis-server/src/main.rs","crates/redis-commands/src/connection.rs"]}
{"id":"persist-3-aof-cycle","role":"runner","runner":"persistence-aof-cycle","depends_on":["persist-3-aof-strict-loader"]}

{"id":"persist-4-propagation-sink","role":"architect","selector":"manual","depends_on":["persist-3-aof-strict-loader"],"resources":["command-dispatch","aof-file","replication-propagation"],"targets":["crates/redis-commands/src/dispatch.rs","crates/redis-core/src/client.rs","crates/redis-core/src/command_context.rs"]}
{"id":"persist-4-edge-command-propagation","role":"translator","selector":"manual","depends_on":["persist-4-propagation-sink"],"resources":["command-dispatch","aof-file"],"targets":["crates/redis-commands/src/string.rs","crates/redis-commands/src/set.rs","crates/redis-commands/src/list.rs","crates/redis-commands/src/zset.rs","crates/redis-commands/src/transaction.rs"]}
{"id":"persist-4-aof-edge-gate","role":"runner","runner":"persistence-aof-cycle","depends_on":["persist-4-edge-command-propagation"]}

{"id":"persist-5-aof-manifest-rewrite","role":"architect","selector":"manual","depends_on":["persist-3-aof-strict-loader"],"resources":["aof-file","persistence-state","child-process"],"targets":["crates/redis-commands/src/aof.rs","crates/redis-commands/src/persist.rs","crates/redis-server/src/main.rs","crates/redis-core/src/server.rs"]}
{"id":"persist-5-aof-rewrite-cycle","role":"runner","runner":"persistence-aof-rewrite-cycle","depends_on":["persist-5-aof-manifest-rewrite"]}
{"id":"persist-5-tcl-aofrw","role":"runner","runner":"tcl-persistence-core","depends_on":["persist-5-aof-manifest-rewrite"]}

{"id":"persist-6-shutdown-config","role":"translator","selector":"manual","depends_on":["persist-5-aof-manifest-rewrite"],"resources":["persistence-state","aof-file","rdb-file"],"targets":["crates/redis-commands/src/connection.rs","crates/redis-server/src/main.rs","crates/redis-commands/src/aof.rs"]}
{"id":"persist-final-wire-rdb-aof","role":"runner","runner":"wire-smoke","depends_on":["persist-6-shutdown-config"]}
```

Architect packets should spend real compute. The propagation sink and manifest
rewrite packets decide cross-cutting interfaces; do not let a translator agent
invent those mid-edit.

---

## Non-goals for this milestone

Explicitly out of scope unless the human overrides:

- RedisModule / Valkey module RDB/AOF hooks
- cluster slot import persistence
- Sentinel
- `valkey-check-aof` standalone utility
- replication AOF sync tests (`integration/replication-aof-sync.tcl`)
- full function-library persistence if `FUNCTION` is not otherwise in the
  single-node core claim
- byte-exact RDB output versus C; the RDB oracle is semantic bidirectional,
  not raw-byte equality
- Linux-only durability optimizations
- replacing RDB/AOF with a new Rust-native format

Do not use private Rust-only RDB opcodes for objects that the public Valkey
claim says are interoperable. The existing private JSON/Bloom opcodes are a
separate product decision and should not infect standard types.

---

## Specific warnings for Spark

1. **Do not leave fake DEBUG success.** If `DEBUG RELOAD` or `DEBUG LOADAOF`
   returns OK, it must actually reload.

2. **Do not make AOF replay forgiving by default.** Corrupt persistence files
   must stop startup. Data loss is worse than refusing to boot.

3. **Do not rename a child rewrite over the active AOF file.** Parent
   finalization is required.

4. **Do not append AOF during AOF replay.** The fake replay client must not
   re-log commands it is loading.

5. **Do not update only the global DB if the runtime owner uses owner DBs.**
   Persistence load/reload must mutate the DB vector that command execution
   reads from.

6. **Do not hide failures by changing test filters.** If a TCL test is out of
   scope, record the skip reason in docs/runner config. Do not silently remove
   it from accounting.

7. **Do not degrade the existing RDB oracle.** Run it after every RDB-adjacent
   change.

8. **Do not turn persistence work into a performance packet.** Performance is
   not the objective here. Reliability and Valkey-compatible failure behavior
   are.

---

## Suggested first Spark task

If we want to test Spark on this big section without handing it the whole
subsystem at once, start with:

```text
Implement persist-0-state-spine and persist-1-rdb-strict-reload only.

You may edit:
  crates/redis-core/src/persistence.rs
  crates/redis-core/src/server.rs
  crates/redis-core/src/live_config.rs
  crates/redis-core/src/rdb/load.rs
  crates/redis-server/src/main.rs
  crates/redis-commands/src/info.rs
  crates/redis-commands/src/connection.rs
  crates/redis-commands/src/persist.rs

You must not edit:
  harness filters to hide persistence tests
  command handlers unrelated to persistence
  RDB save payload semantics except as needed for load options

Stop when:
  cargo test --workspace passes
  python3 harness/oracle/rdb-diff --direction=all passes
  DEBUG RELOAD actually reloads a saved value in a manual smoke
```

That gives a strong signal without risking AOF rewrite data loss. If Spark does
well, hand it Phase 3 and Phase 5 as separate architecture-first packets.

---

## Tracking commands

Current persistence docs/evidence:

```bash
sed -n '1,220p' docs/SCOPE_AND_GAPS.md
sed -n '1,220p' docs/CONFORMANCE.md
sed -n '1,260p' docs/history/RDB_PLAN.md
```

Existing RDB gate:

```bash
python3 harness/oracle/rdb-diff --direction=all
```

Current full-suite accounting:

```bash
python3 harness/oracle/tcl-suite-inventory.py
```

Where to inspect local implementation:

```bash
rg -n "BGREWRITEAOF|BGSAVE|DEBUG RELOAD|DEBUG LOADAOF|appendonly|replay_aof|AofWriter" crates
sed -n '1,260p' crates/redis-commands/src/aof.rs
sed -n '400,510p' crates/redis-commands/src/persist.rs
sed -n '280,380p' crates/redis-server/src/main.rs
```

Where to inspect upstream:

```bash
rg -n "loadDataFromDisk|feedAppendOnlyFile|loadAppendOnlyFiles|rewriteAppendOnlyFileBackground|backgroundRewriteDoneHandler|bgrewriteaofCommand" reference/valkey/src
sed -n '1438,1482p' reference/valkey/src/aof.c
sed -n '2576,2656p' reference/valkey/src/aof.c
sed -n '2769,2825p' reference/valkey/src/aof.c
sed -n '7248,7285p' reference/valkey/src/server.c
```

Expected result after the full milestone: `docs/SCOPE_AND_GAPS.md` should be
able to change "AOF partial / not gated" to "AOF restart and rewrite behavior
gated for single-node standard types", with explicit exclusions for modules,
replication AOF sync, and the standalone check-aof utility.

# Valdr structure audit — god-files and where they hide

**Date**: 2026-05-28
**Scope**: every `.rs` under `crates/`. 106,701 LOC across 120 files.
**Purpose**: surface god-files where multiple unrelated subsystems share a
file, separated from files that are merely *big*. Big is fine when one C
source maps to one Rust file (the harness's stated invariant); god is when
unrelated upstream subsystems get smeared into the same Rust file.

## LOC inventory

```
redis-commands             51,873 LOC   28 files
redis-core                 40,792 LOC   69 files
redis-ds                    6,651 LOC   14 files
redis-server                5,162 LOC    2 files   ← suspiciously few files
redis-protocol              1,800 LOC    4 files
redis-types                   423 LOC    3 files
```

## Top 12 largest files

```
LOC    File                                            Verdict
7184   crates/redis-commands/src/connection.rs         GOD-FILE — split
6316   crates/redis-commands/src/eval.rs               big-but-cohesive (scripting)
5201   crates/redis-commands/src/generated.rs          auto-generated — fine
3576   crates/redis-commands/src/dispatch.rs           big-but-cohesive (router)
3325   crates/redis-commands/src/stream.rs             big-but-cohesive (Streams)
2879   crates/redis-core/src/object.rs                 big-but-cohesive (canonical type owner)
2727   crates/redis-commands/src/zset.rs               big-but-cohesive (ZSET)
2678   crates/redis-server/src/main.rs                 god-file (lighter) — split
2534   crates/redis-core/src/networking.rs             big-but-cohesive (port of networking.c)
2484   crates/redis-server/src/runtime_owner.rs        big-but-cohesive (the accept loop)
2259   crates/redis-core/src/db.rs                     marginal — hooks could split
2202   crates/redis-commands/src/hash.rs               big-but-cohesive (HASH)
```

## The verdict each ranks against

The harness records the *canonical* upstream-C-file → Rust-file mapping in
[`harness/file-deps.tsv`](../harness/file-deps.tsv). One C file → one Rust
file is the invariant; **a god-file is one that contains code from multiple
upstream C files when those files have their own Rust homes**.

- `util.rs` (1,629 LOC) is large but the *only* port of `util.c`+`util.h`.
  Not a god-file. Splitting it would diverge from the harness map and break
  the "look it up by upstream file" mental model.
- `object.rs` (2,879 LOC) is the canonical home of `RedisObject` and every
  inline-encoding variant. Per `harness/type-vocabulary.tsv` it OWNS those
  types — splitting them across files would either break ownership or
  fragment one concept across multiple files. Not a god-file.

## ── #1 god-file: redis-commands/src/connection.rs (7,184 LOC)

By far the worst. The file *claims* (per its doc-comment) to be "connection-
management and server commands: PING, ECHO, SELECT, CLIENT, COMMAND, DEBUG,
TIME, HELLO, RESET, QUIT". In reality it also contains:

| Subsystem present here | Functions counted | Upstream C home | Rust home it *should* live in |
|---|---|---|---|
| Connection commands (PING, ECHO, SELECT, …) | 12  | `commands/*.json` + `t_string.c` neighbors | here (correct) |
| `CONFIG GET` / `CONFIG SET` machinery       | dozens (apply_config_set is ~1,200 LOC) | `config.c` | **`redis-core/src/config.rs`** (does not exist) |
| TCP listener hooks (port/bind reconfig)     | 4   | `server.c` + `anet.c` | **`redis-server/src/main.rs`** or a new `listeners.rs` |
| Shutdown-signal helpers                     | 7   | `server.c` (signal handlers) | **`redis-core/src/server.rs`** (already exists, only 11 KB) |
| ACL file path glue (`aclfile`, `requirepass`) | 4 | `acl.c` | next to **`redis-core/src/acl.rs`** |
| Client output buffer limits                 | 6   | `server.c` config | **`redis-core/src/live_config.rs`** or `client.rs` |

`harness/file-deps.tsv` confirms the split: `config.c` is assigned to
`redis-core/src/config.rs`, **and that file does not exist** — every line of
config-file parsing, validation, default values, and `CONFIG GET`/`CONFIG SET`
landed in `redis-commands/src/connection.rs` instead. This is the file I
edited today to add the TLS reconfig wiring, and the editing felt awkward
specifically because the file is wearing six hats.

**Recommended split (4 new files, ~4,500 LOC moves):**

```
redis-commands/src/connection.rs           ~1,500 LOC  (just the connection commands)
redis-core/src/config.rs                   ~3,500 LOC  (apply_config_set, default_config_pairs,
                                                        config_pairs_with_dynamic, validate_config_set_pair,
                                                        is_live_config_key, LIVE_KEYS, configset_strerror)
redis-server/src/listeners.rs              ~250 LOC    (install_tcp_port_set_hook,
                                                        install_tcp_bind_set_hook,
                                                        drain_pending_tcp_listeners,
                                                        drain_pending_tcp_listener_replacement)
redis-core/src/shutdown.rs                 ~200 LOC    (note_shutdown_signal, shutdown_signal_*,
                                                        shutdown_pending, set_shutdown_pending,
                                                        abort_shutdown_pending,
                                                        mark_shutdown_save_failed,
                                                        shutdown_on_sigterm_force)
```

The ACL glue and client-obuf helpers are smaller — fold them into the
existing `acl.rs` and `client.rs` rather than new files.

**Why this matters**: a reader looking for "where does CONFIG SET enforce
its allowlist?" will not, today, guess `redis-commands/src/connection.rs`.
The harness's own canonical map says they shouldn't have to.

## ── #2 god-file (lighter): redis-server/src/main.rs (2,678 LOC, 57 fns)

The whole `redis-server` crate is two files: `main.rs` + `runtime_owner.rs`.
`main.rs` does:

- CLI arg parsing (`parse_args`, ~250 LOC)
- Config file parsing (`read_config_file`, `unquote_value`, ~100 LOC)
- BGSAVE reaper thread (`bgsave_reaper`, ~50 LOC)
- TLS startup (`build_tls_startup`, ~80 LOC — the one I just rewrote)
- Renamed-ready-keys deferred-rename machinery (~80 LOC at top)
- Startup-log sentinel emission (~30 LOC)
- The single `main()` body wiring everything

These are all *startup-adjacent* concerns so the file isn't conceptually
broken the way `connection.rs` is — but it's at the size where finding
"how does `--io-threads` parse?" requires a grep, not a glance.

**Recommended split (3 new files, ~1,500 LOC moves):**

```
redis-server/src/main.rs                   ~600 LOC   (just main() + the top-level wiring)
redis-server/src/cli.rs                    ~700 LOC   (parse_args, read_config_file,
                                                        unquote_value, expand_cli_args,
                                                        split_cli_words, cli_error_case,
                                                        CliArgs struct)
redis-server/src/startup.rs                ~400 LOC   (build_tls_startup,
                                                        bgsave_reaper,
                                                        renamed_ready_keys machinery,
                                                        wake_ready_after_command,
                                                        startup log sentinels)
redis-server/src/lifecycle.rs              ~150 LOC   (BGSAVE reaper, signal-driven shutdown
                                                        path — likely folds with shutdown.rs above)
```

Net: `redis-server/` goes from 2 files to 4–5, each one with a one-line
elevator pitch a reader can hold.

## ── #3 god-file (smaller): redis-core/src/command_context.rs (1,555 LOC)

Module doc says it's "the contract every command implementation works
against" — but in practice the file accumulates everything `CommandContext`
*touches*:

| Concern in this file | Belongs in |
|---|---|
| `CommandContext` struct + `DbStorage` enum + `DbListRoute` | here (correct) |
| `ReplyErrorArg`, `ReplyArrayLen`, `ArgIndex` traits        | new `redis-core/src/reply_traits.rs` (~200 LOC) |
| `publish_keyspace_message`, `encode_pubsub_message_resp2/3`, `encode_pubsub_pmessage_resp2/3` | already exists: `pubsub_registry.rs` / `notify.rs` |
| `tracking_read_keys_for_command`, `tracking_mutation_for_command`, `command_mutates_first_key` | already exists: `tracking.rs` |
| `ascii_eq_ignore_case`, `parse_i64_from_bytes`             | `util.rs` |

**Recommended split**: extract the four pub-traits to `reply_traits.rs`; move
the pub-sub encoders into `pubsub_registry.rs` (where the registry already
lives); move the tracking helpers into `tracking.rs`. Net: `command_context.rs`
ends ~700 LOC and is purely about the dispatch context.

## ── Marginal: redis-core/src/db.rs (2,259 LOC)

Contains `RedisDb` + `WatchedKeysIndex` + **five distinct hook installers**:
swapdb-wake, stream-key-deleted, stream-db-flushed, stream-rename,
stream-key-overwritten. The stream-side hooks are about *streams reacting to
DB events* — they could move to `redis-core/src/stream_state.rs` (new) or
into `redis-commands/src/stream.rs` (it already owns Stream commands). Not
urgent; the file is otherwise cohesive.

## Big-but-cohesive — leave alone

| File | LOC | Why it stays |
|---|---|---|
| `eval.rs`          | 6,316 | Scripting subsystem; one concern |
| `generated.rs`     | 5,201 | Auto-generated from `commands/*.json`; never hand-edit |
| `dispatch.rs`      | 3,576 | Command router; one concern |
| `stream.rs`        | 3,325 | Streams; one data type |
| `object.rs`        | 2,879 | Canonical `RedisObject` owner; splitting fragments the type vocabulary |
| `zset.rs`          | 2,727 | ZSET commands; one data type |
| `networking.rs`    | 2,534 | Port of upstream `networking.c`; harness invariant |
| `runtime_owner.rs` | 2,484 | The mio accept loop; one architectural concern |
| `hash.rs`          | 2,202 | HASH commands |
| `util.rs`          | 1,629 | Port of `util.c`+`util.h`; harness invariant |

## Crate-level shape

The crate boundaries are sensible — `types` < `protocol` < `ds` < `core` <
`commands` is a clean layered DAG, and the per-crate `CLAUDE.md` files we
added this week give each crate a one-glance briefing. No crate is a god-
crate: `redis-commands` is the largest by LOC but every file in it is one
data type or one subsystem.

Two crate-level oddities to note (not faults, but worth knowing):

1. **Two files named `connection.rs`** in different crates: `redis-core` has
   the port of upstream `connection.c` (the vtable abstraction, 1,066 LOC,
   cohesive) and `redis-commands` has the god-file analyzed above. Same
   name, completely different jobs. Splitting the god-file would also remove
   the naming clash.
2. **`redis-server` is a 2-file crate** carrying 5,162 LOC. Both files are
   doing real work, but a binary that is the entry point and the user-
   visible runtime probably wants to be a 4–5 file crate to be legible.

## Priority order for splits

If we only do *one* of these, it should be the big one — splitting
`redis-commands/src/connection.rs` per the harness-blessed map:

```
1.  HIGH    Split redis-commands/src/connection.rs (4 new files, ~4,500 LOC moved)
            — the harness already says where the pieces go; today's TLS
              work is direct evidence that the current shape causes pain.

2.  MEDIUM  Split redis-server/src/main.rs (3 new files, ~1,500 LOC moved)
            — startup will keep growing; bound it now.

3.  MEDIUM  De-mash redis-core/src/command_context.rs
            (move 4 trait + 4 pubsub-encode + 3 tracking helpers out)
            — purely additive, files already exist for the homes.

4.  LOW     redis-core/src/db.rs hook extraction
            — only worth doing if streams grows another hook category.
```

All four together: ~5,500 LOC re-homed, ~7 new files, zero behaviour change.
The TCL oracle is the safety net — any rehoming that compiles and passes
the suite is by construction faithful.

## What didn't show up that I was looking for

- **No unsafe sprawl**: `unsafe` blocks are budgeted per crate by
  `unsafe-budget.sh`; nothing in the audit jumps out.
- **No "PORT STATUS" trailer drift**: every Rust file ends with the
  machine-readable trailer that `trailer-required.sh` enforces. A grep for
  `# PORT STATUS` confirms 120/120 files have it.
- **No mixed-feature god-files** of the kind we feared — e.g. no single
  file containing both ACL and RDB code, or both networking and command
  dispatch. The boundaries that exist hold; the smear is concentrated in
  the four files named above.

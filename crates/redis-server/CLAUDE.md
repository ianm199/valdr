# redis-server

The `redis-server` binary — the entry point the oracle and Docker image run.
Wires the four library crates into a running server; owns the process, not the
data model. Depends on all of `redis-core`, `redis-commands`, `redis-protocol`,
`redis-types`.

## Owns no canonical types
This crate declares no `type-vocabulary.tsv` types — it composes the others.

## Module map (4 files)
- main.rs           entry point — module decls, top-level statics
                    (RENAMED_READY_KEYS{,_PENDING}), constants
                    (DEFAULT_PORT/BIND/…), and the `fn main()` body wiring
                    everything together
- cli.rs            CliArgs struct + `parse_args` + config-file parsing
                    (`apply_config_file`, `unquote_config_value`,
                    `expose_config_file_value`, `parse_memsize_config`) +
                    `cli_error_case`. Split from main.rs 2026-05-28.
- startup.rs        renamed_ready_keys deferred-rename machinery,
                    `build_tls_startup`, unix control listener, BGSAVE
                    reapers (mac/linux variants), blocked-timeout thread,
                    `dispatch_full_sync_transfer`, replconf-getack helpers.
                    Everything that runs once at startup or as a background
                    thread. Split from main.rs 2026-05-28.
- runtime_owner.rs  the `RuntimeOwner` accept/event loop — mio readiness-backed;
                    one owner accepts sockets, parses RESP via `redis-protocol`,
                    dispatches via `redis-commands`, flushes replies; owns the
                    live DB vector on the plain-TCP path

## Footguns (real, not stale)
- Single-owner model: one `RuntimeOwner` owns the live DB vector and the accept
  loop. The legacy TLS command path is divergent — once the owner loop owns the
  DB, the binary REFUSES to start the old TLS path rather than let TLS commands
  mutate a separate global DB. Don't reintroduce a second DB owner.
- Plain TCP uses `mio`; there is no Tokio/async runtime. Don't add one casually.
- Out of scope here: cluster, modules, full TLS socket migration.

## Common tasks / where to look
- add/change a CLI flag or config-file directive → cli.rs (`parse_args`,
  `apply_config_file`)
- change startup wiring (TLS init, AOF replay, hook installation) → main.rs
  `fn main()` body, or startup.rs if it's a helper main() calls
- BGSAVE/repl-bgsave background thread behavior → startup.rs (mac/linux
  variants of `spawn_bgsave_reaper`)
- change the accept loop, per-connection state, or dispatch wiring →
  runtime_owner.rs
- "is the server picking up my command?" → trace runtime_owner.rs →
  `redis_commands::dispatch`

`main.rs` re-exports cli + startup with `pub(crate) use cli::*;
pub(crate) use startup::*;` so `runtime_owner` calls like
`super::determine_initial_user()` keep resolving across the split.

## Ports (upstream C — never edit `reference/`)
server.c (`main` / `initServer`, serverCron-style background work), anet.c
(accept), config.c (startup config). See `harness/file-deps.tsv`.

## Invariants (hook-enforced)
- every `.rs` ends with a PORT STATUS trailer  — `trailer-required.sh`
- `unsafe` under crate budget                  — `unsafe-budget.sh`
- banned patterns                              — `forbidden-import.sh`

## Build / run
`cargo build -p redis-server` (or `--bin redis-server`) — this is the binary the
oracle drives. End-to-end behavior:
`bash harness/oracle/run-single-node-tcl-suite.sh` (builds + runs the TCL suite).

## Heads up
- `main.rs` frames itself as "Wave A scaffolding" — the framing is dated, but
  the mio/TLS constraints above are current and real.
- The 2026-05-28 split (main.rs 2,679 → 402 LOC + cli.rs + startup.rs) keeps
  the same public paths working via `pub(crate) use` re-exports in main.rs.
  Don't add new code to main.rs — pick cli.rs (parsing) or startup.rs
  (wiring/threads) and put it there.

Project strategy & roles live in the parent `CLAUDE.md` files — not duplicated.

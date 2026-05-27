# redis-server

The `redis-server` binary — the entry point the oracle and Docker image run.
Wires the four library crates into a running server; owns the process, not the
data model. Depends on all of `redis-core`, `redis-commands`, `redis-protocol`,
`redis-types`.

## Owns no canonical types
This crate declares no `type-vocabulary.tsv` types — it composes the others.

## Module map (2 files)
- main.rs           process startup: arg/config parsing, bind, background
                    threads (LRU clock, …), then hands off to the owner loop
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
- change startup, config flags, or daemon setup → main.rs
- change the accept loop, per-connection state, or dispatch wiring → runtime_owner.rs
- "is the server picking up my command?" → trace runtime_owner.rs → `redis_commands::dispatch`

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
`main.rs` frames itself as "Wave A scaffolding" — the framing is dated, but the
mio/TLS constraints above are current and real.

Project strategy & roles live in the parent `CLAUDE.md` files — not duplicated.

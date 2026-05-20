# Path to a runnable Rust Redis — execution plan

**Goal:** A `redis-server` binary that listens on TCP, accepts a real RESP
connection from `redis-cli`, and responds correctly to:
- `PING` → `+PONG\r\n`
- `ECHO <bytes>` → `$<len>\r\n<bytes>\r\n`
- `SET <key> <val>` → `+OK\r\n`
- `GET <key>` → `$<len>\r\n<val>\r\n` or `$-1\r\n`
- `DEL <key> [...]` → `:<count>\r\n`
- `EXISTS <key> [...]` → `:<count>\r\n`
- `INCR <key>` → `:<n>\r\n` or error

And `harness/oracle/wire-diff --suite smoke` passes against the real C
Valkey server on the same corpus.

This converts "Phase A complete, workspace compiles" into "Phase B
working: handles real commands end-to-end."

## Current gap (per `harness/loop/PHASE_B_CLEANUP.md`)

- `main.rs` is `eprintln!("not implemented") + exit(1)`
- No event loop / TCP listener anywhere in the workspace
- `Client::process_input` is `todo!()`
- No command-table *lookup* fn (only the static `COMMANDS` metadata table)
- SET/GET/INCR bodies in `string.rs` are 33 `todo!()` panics
- `RedisServer` lacks ~15 fields needed by call sites that already exist
  in translated files

## Plan in waves

### Wave A — Architect: server + connection scaffolding

**Single Opus sub-agent.** Sequential (everything else depends on it).
Budget: ~$15.

Deliverables:
1. **Expand `RedisServer`** with the ~15 fields the planner identified
   (commands table reference, databases vec, hz, aof_state,
   cmd_time_snapshot, listener handles, etc.). Most are stub-typed for
   now; real values come from later phases.
2. **`Connection` abstraction** in `redis-core::connection`:
   - `enum Connection { Tcp(TcpStream), /* later: Unix, Tls */ }`
   - `fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>`
   - `fn write_all(&mut self, buf: &[u8]) -> io::Result<()>`
   - `fn close(self)`
3. **`Client` gains a `Connection`** field (Option<Connection> for now —
   pre-handshake clients lack one).
4. **RESP2 incremental parser** in `redis-protocol::parser` (or a new
   module) that takes bytes, returns either "incomplete" or
   `Vec<RedisString>` (parsed argv).
5. **Command dispatch:** `redis-commands::dispatch(ctx) -> RedisResult<()>`
   that looks up `ctx.command_name()` in the static `COMMANDS` table and
   routes to the right handler function.
   - For PING/ECHO: handler is an inline fn in `redis-commands::connection`
   - For SET/GET/etc.: existing functions in `redis-commands::string`
     (but most have `todo!()` bodies; this wave only wires the lookup).
6. **`main.rs`** that:
   - Parses minimal CLI args (`--port`, `--bind`; default 6379, 127.0.0.1)
   - Binds TCP listener
   - Blocking-accept loop: per connection, spawn a thread (or just handle
     serially) with a Client and dispatch.
   - On SIGINT, graceful shutdown.

**Verification:**
- `cargo check --workspace` clean
- `cargo build --bin redis-server` produces a binary
- `cargo test --workspace` no new regressions

**Stop condition:** binary builds, can `./target/debug/redis-server --port 6390` and it accepts a TCP connection (even if it doesn't respond yet).

### Wave B — PING + ECHO end-to-end

**Two parallel sub-agents** after Wave A lands.
Budget: ~$10 total.

Agent B1: **PING + ECHO implementation** in `redis-commands::connection`
- `fn ping_command(ctx: &mut CommandContext) -> RedisResult<()>`
  - 0 args → `+PONG`
  - 1 arg → `$<len>\r\n<arg>\r\n`
  - error otherwise
- `fn echo_command(ctx: &mut CommandContext) -> RedisResult<()>`
  - Exactly 1 arg → bulk reply
  - Wrong arity → error

Agent B2: **Wire it through dispatch** — ensure command-table lookup
finds PING and ECHO (they're in `generated.rs`'s static table; agent
verifies the dispatcher hits them).

**Verification:**
- Start `redis-server --port 6390` in background
- `redis-cli -p 6390 PING` returns `PONG`
- `redis-cli -p 6390 ECHO hello` returns `"hello"`
- `harness/oracle/wire-diff --c-port 6379 --rust-port 6390 --suite smoke` for
  PING/ECHO/HELLO subset shows byte-exact match for those commands.

### Wave C — SET / GET / DEL / EXISTS / INCR end-to-end

**Multiple parallel sub-agents** after Wave B lands.
Budget: ~$20 total.

Agent C1: SET + GET + DEL + EXISTS in `redis-commands::string`. The
translated file has 33 `todo!()`s; this wave fills the ones needed for
these five commands (rest can stay TODO).

Agent C2: INCR + DECR + INCRBY + DECRBY (integer arithmetic on string
values). Standard "lookup, parse i64, modify, store" pattern.

Agent C3: Wire-diff harness adjustment — ensure the existing oracle's
RESP encoder/decoder matches what's been built. Smoke test runner.

**Verification:**
- All `harness/oracle/corpus/*.txt` scripts byte-exact match between
  C and Rust servers
- Specifically: 03-set-get.txt, 04-del-exists.txt, 05-incr.txt

### Wave D — Wire-diff smoke + sweep

Single agent after Wave C: run wire-diff full smoke suite, triage any
remaining diffs, fix small ones. Stop when smoke is green or only
non-determinism remains (CLIENT, INFO etc. — won't surface in smoke).

Budget: ~$10.

## Stop conditions overall

- **Success:** wire-diff smoke (5 corpus files, 24 commands) all byte-exact
  match. We have a working Rust Redis for PING/ECHO/SET/GET/DEL/EXISTS/INCR.
- **Budget cap:** $80 total across all waves. Stop and report.
- **Stuck:** 3 consecutive sub-agent failures. Stop and triage.
- **Workspace breaks for >30 min:** stop, hand off to user.

## Won't do in this plan

- Multi-database routing (always DB 0 for now; flagged TODO).
- Async/tokio (use blocking I/O + thread-per-connection).
- TLS (Connection::Tls variant deferred).
- RDB/AOF persistence (data is in-memory only).
- Replication, cluster, modules, scripting, pub/sub — all deferred.
- Multi-line config parsing (`--port` arg only).
- Most of the 1378 TODO(port) markers — Phase B scope is "make smoke
  pass," not "complete every translated file."

## Cost / wall-time estimate

| Wave | Cost | Wall time | Parallel |
|---|---|---|---|
| A — server scaffolding | $15 | 25-40 min | 1 agent |
| B — PING/ECHO | $10 | 15-25 min | 2 agents |
| C — SET/GET/INCR family | $20 | 30-45 min | 3 agents |
| D — wire-diff sweep | $10 | 20-30 min | 1 agent |
| **Total** | **~$55** | **~2 hrs** | |

Adjust upward by ~50% to account for retries / compile-fix sub-waves.

## Risk register

- **Borrow-checker death** in main.rs / connection handling — common in
  Rust networking code that mixes mutable Client with mutable Connection.
  Mitigation: use `RefCell` or split borrows per pass; flag TODO if
  unfixable.
- **Generated command table doesn't expose a lookup fn** — the registry
  is just a static array. Wave A adds a binary-search or HashMap-backed
  lookup. Small but necessary.
- **Dispatch arity errors** — translated handlers expect specific argv
  shapes; the dispatcher might pass them wrong. Wave B/C testing catches
  this.
- **Wire-diff catches a semantic bug** — e.g. SET returns `+OK\r\n` but
  Rust returns `$2\r\nOK\r\n`. These are easy to spot in the oracle's
  per-command output.

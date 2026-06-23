# Valdr — working in this repo

A Rust port of the Valkey server. **This file is the "how to work here" guide.**
Per-crate detail lives in `crates/<crate>/CLAUDE.md`; strategy and the
harness/oracle model live in the parent `rustExperiments/CLAUDE.md`.

## Crate layout

```
crates/
  redis-types/     RedisString + RedisError                (423 LOC,   3 files)
  redis-protocol/  RESP wire encode/decode                (1,800 LOC,  4 files)
  redis-ds/        listpack/quicklist/intset/dict/rax    (6,651 LOC, 14 files)
  redis-core/      live state: RedisServer/RedisDb/etc. (40,792 LOC, 68 files)
  redis-commands/  every command + the dispatch router  (51,873 LOC, 32 files)
  redis-server/    the binary main loop                  (5,162 LOC,  4 files)

  # EdgeStash — Valdr's edge/wasm lane (NOT part of redis-server; see section below)
  valdr-engine/         wasm-safe embeddable command engine    (~19,000 LOC)
  edgestash-demo/       provider-neutral HTTP + Lua limiter     (~1,830 LOC)
  edgestash-cloudflare/ Cloudflare Worker + Durable Object        (~400 LOC)
  valdr-fixture-runner/ JSONL driver for the differential oracle  (~115 LOC)
```

Dependencies flow downward only: `types → protocol → ds → core → commands → server`.
A type owned by `redis-core` (e.g. `RedisDb`) must not be redeclared in
`redis-commands`. `harness/type-vocabulary.tsv` is the enforced single-owner
registry; `pretooluse-type-vocab.sh` blocks violations at the hook layer.

## Where things live (most common lookups)

| You want to… | Look in |
|---|---|
| Find a command's handler | `redis-commands/src/dispatch.rs` → grep `b"NAME"` |
| Add a command handler | matching type file (`string.rs`, `list.rs`, …) + `DispatchEntry` in `dispatch.rs` |
| Change `CONFIG GET`/`SET` behavior | `redis-commands/src/config_cmd.rs` |
| Add a TLS/auth setting | `redis-core/src/live_config.rs` + `redis-core/src/tls.rs` + apply in `config_cmd.rs` |
| Change key read/write/expiry semantics | `redis-core/src/db.rs`, `redis-core/src/expire.rs` |
| Change object encoding | `redis-core/src/object.rs` (canonical owner) + the matching `redis-ds/*.rs` encoding |
| Add a CLI flag or config-file directive | `redis-server/src/cli.rs` (`parse_args`, `apply_config_file`) |
| Change startup wiring | `redis-server/src/main.rs` `fn main()` body; or `redis-server/src/startup.rs` for helpers |
| Change the accept/event loop | `redis-server/src/runtime_owner.rs` |
| Add a TCP listener hook | `redis-commands/src/listeners.rs` |
| Change shutdown signal behavior | `redis-commands/src/shutdown_signals.rs` |
| Change a client-output-buffer-limit | `redis-commands/src/client_limits.rs` |
| Change reply-adapter trait behavior | `redis-core/src/reply_traits.rs` |
| Change stream-reactive hook plumbing | `redis-core/src/stream_hooks.rs` |
| Find the upstream C source for a Rust file | `harness/file-deps.tsv` (one C file → one Rust file) |
| Find the upstream C source for a command | `harness/command-registry.json` |

## Build commands

```bash
cargo build -p redis-server               # the binary the oracle drives
cargo build -p redis-core                 # the data model
cargo build                               # everything
cargo test  -p redis-core                 # crate-internal unit tests
```

`cargo build` succeeding is *not* the bar. **Behavior is proven by the oracle,
never by the build.** A clean build with a regression is a regression.

## Oracle commands (the bar)

```bash
# Single file (fast, ~5s for unit/type/string)
python3 harness/oracle/tcl-survey.py \
  --runner-id smoke --profile single-node-external \
  --timeout-s 120 --baseport 37000 --portcount 4000 \
  --files unit/type/string --isolated-tests-copy --skip-build

# TLS-specific (needs --tls)
python3 harness/oracle/tcl-survey.py \
  --runner-id tls --profile single-node-external \
  --timeout-s 180 --baseport 36000 --portcount 8000 \
  --files unit/tls --isolated-tests-copy --skip-build --tls

# Full single-node sweep (long; the publication bar)
bash harness/oracle/run-single-node-tcl-suite.sh
```

Numbers: see `docs/TEST_AND_FEATURE_COVERAGE.md` (canonical) and the bars on
the GitHub Pages site (`docs/index.html`).

## Custom subsystem testers — the fast inner loop

When you grind a subsystem that is concurrency-/timing-/state-machine-heavy,
do **not** iterate against the slow end-to-end oracle. Build an in-memory
deterministic tester first; iterate there; let the oracle have the final word.
The reference is `crates/redis-core/tests/conn_transport_kit.rs` (the
`TestPipe` non-blocking duplex that proved the rustls drain-fix) — see the
parent `CLAUDE.md` for the doctrine. Reach for this pattern whenever a real
socket reproduces a bug "sometimes."

## File organization invariants

- **One C file → one Rust file** (per `harness/file-deps.tsv`). Splitting a
  large Rust file across multiple Rust files only when concerns are genuinely
  unrelated AND the harness map agrees. Audit the candidates with
  `docs/history/STRUCTURE_AUDIT.md`.
- **No god files**: a file should be either (a) a one-to-one port of a C
  source, or (b) a single cohesive subsystem. If you find yourself adding a
  third concern to a file, split first.
- **Every `.rs` ends with a PORT STATUS trailer** (enforced by
  `trailer-required.sh`). Format:
  ```
  // ──────────────────────────────────────────────────────────────────────────
  // PORT STATUS
  //   source:        <upstream C file or "extracted from X.rs">
  //   target_crate:  <crate>
  //   confidence:    skeleton | partial | high
  //   todos:         <count>
  //   port_notes:    <count>
  //   unsafe_blocks: <count>
  //   notes:         one-line summary
  // ──────────────────────────────────────────────────────────────────────────
  ```

## Hooks (mechanical guardrails)

Wired via `.claude/settings.json` — **that file is the source of truth; this
table mirrors it.** Defined ≠ wired: scripts exist in `.claude/hooks/` for more
guardrails than are registered. As of 2026-06-21 exactly **three** are live:

| Hook | Event | Wired? | Enforces |
|---|---|---|---|
| `verify-gate.sh`           | PreToolUse (Write\|Edit) | ✅ wired | cannot mark a test PASS without reading the evidence file |
| `pretooluse-type-vocab.sh` | PreToolUse (Write\|Edit) | ✅ wired | type-vocabulary registry: every cross-cutting type has one owner |
| `commit-on-stop.sh`        | Stop                     | ✅ wired | auto-commits agent work so nothing is lost |
| `unsafe-budget.sh`         | —                        | defined, **not wired** | per-crate `unsafe` block ceiling |
| `forbidden-import.sh`      | —                        | defined, **not wired** | banned patterns (e.g. raw `*mut` outside GC) |
| `trailer-required.sh`      | —                        | defined, **not wired** | every `.rs` carries a PORT STATUS trailer |
| `type-vocabulary.sh`, `rebuild-before-measure.sh` | — | defined, **not wired** | chassis guardrails available to wire |

Hooks live in `port-harness/hooks/` (canonical, 12 scripts) with thin wrappers
in `.claude/hooks/`; this project wires 3 of them. To enable more, register them
in `.claude/settings.json` — don't assume a script being present means it runs.

## Recent structural changes (2026-05-28)

A god-file audit (`docs/history/STRUCTURE_AUDIT.md`) led to four splits, all
behavior-preserving via `pub use` re-exports:

| Was | Now |
|---|---|
| `redis-commands/src/connection.rs` (7,184 LOC, 6 concerns) | 5,540 LOC + `config_cmd.rs` + `listeners.rs` + `shutdown_signals.rs` + `client_limits.rs` |
| `redis-server/src/main.rs` (2,679 LOC, 7 concerns) | 402 LOC + `cli.rs` + `startup.rs` |
| `redis-core/src/command_context.rs` (1,556 LOC) | 1,488 LOC + `reply_traits.rs` |
| `redis-core/src/db.rs` (2,260 LOC, 5 hook subsystems) | 2,189 LOC + `stream_hooks.rs` |

External callers (`redis_commands::connection::*`, `super::determine_initial_user`,
etc.) keep working — wildcard re-exports preserve the paths. New code should
import from the canonical module (e.g. `redis_commands::config_cmd::apply_config_set`,
not `redis_commands::connection::apply_config_set`).

## EdgeStash — Valdr's edge / wasm lane

Four of the crates above are **not** part of the native `redis-server`. They are
**EdgeStash**: `valdr-engine` compiled to `wasm32-unknown-unknown` and run inside a
Cloudflare Durable Object — Redis-style atomic state at the edge, with Lua, no
external Redis service. It is **deployed and measured**
(`edgestash-valdr.ianmclaughlin1398.workers.dev`; cold DO ~0.5 s, warm ~66 ms p50)
and has **its own differential oracle** vs real `valkey-server`
(`harness/oracle/valdr-engine-differential.py`, 352 fixtures / 0 divergences) —
independent of the native-server Tcl suite. EdgeStash is the proof that Valdr
yields a reusable embeddable engine, not just a TCP server; the lazy per-key
cold-load (`command_keys` → O(touched), not O(state)) is its scale-to-zero story.

- **Front door / how to run it (press `e` for the Local Explorer):**
  [crates/edgestash-cloudflare/CLAUDE.md](crates/edgestash-cloudflare/CLAUDE.md)
- **Engine internals + the wasm-safety invariant:**
  [crates/valdr-engine/CLAUDE.md](crates/valdr-engine/CLAUDE.md)
- **Design + dated status log:**
  [docs/EDGE_WASM_COMMAND_ENGINE.md](docs/EDGE_WASM_COMMAND_ENGINE.md)

## Per-crate briefings

Each crate has its own `CLAUDE.md` with the module map, footguns, and common
tasks. Claude Code lazy-loads these when you touch files in the crate, so
keep them current.

| Crate | Briefing |
|---|---|
| `redis-types`    | [crates/redis-types/CLAUDE.md](crates/redis-types/CLAUDE.md) |
| `redis-protocol` | [crates/redis-protocol/CLAUDE.md](crates/redis-protocol/CLAUDE.md) |
| `redis-ds`       | [crates/redis-ds/CLAUDE.md](crates/redis-ds/CLAUDE.md) |
| `redis-core`     | [crates/redis-core/CLAUDE.md](crates/redis-core/CLAUDE.md) |
| `redis-commands` | [crates/redis-commands/CLAUDE.md](crates/redis-commands/CLAUDE.md) |
| `redis-server`   | [crates/redis-server/CLAUDE.md](crates/redis-server/CLAUDE.md) |
| `valdr-engine` *(EdgeStash)* | [crates/valdr-engine/CLAUDE.md](crates/valdr-engine/CLAUDE.md) |
| `edgestash-cloudflare` *(EdgeStash)* | [crates/edgestash-cloudflare/CLAUDE.md](crates/edgestash-cloudflare/CLAUDE.md) |

Strategy, harness/oracle model, agent roles, and the security thesis live in
the parent [`../CLAUDE.md`](../CLAUDE.md).

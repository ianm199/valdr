# Plan: Adopt Valkey's TCL test suite as upgraded oracle

## TL;DR

Running the Valkey TCL suite against our Rust server is viable but requires two non-trivial setup steps: (1) setting `VALKEY_BIN_DIR` to point at our binary and emitting the exact startup log line the harness polls for, and (2) implementing enough of `CONFIG SET`/`GET` and `DEBUG` subcommands to avoid crashing test setup. Using `--single unit/type/string` as the first target, roughly 85 of the 116 tests should pass today given our string-command coverage; the 31 failures cluster into four categories: `MEMORY USAGE`, `DEBUG object/set-active-expire`, `needs:repl`, and keyspace-notification tests. The remaining type-test files (hash, set, zset, list) have heavier `CONFIG SET` dependencies that will block more tests until we return a minimal `+OK` stub response.

## How the TCL suite works

`test_helper.tcl` acts as a supervisor process that forks up to 16 client subprocesses, each running a `.tcl` test file via `source`. Files are discovered by globbing `tests/unit/*.tcl`, `tests/unit/type/*.tcl`, `tests/unit/cluster/*.tcl`, and `tests/integration/*.tcl`. The `--single unit/type/string` flag limits execution to one file. Tests are grouped with `start_server` blocks (in `tests/support/server.tcl`) that spawn a fresh server instance per block, wait for it to appear in the log, run the `code` body, then kill the server.

Server spawning is handled by `spawn_server` in `support/server.tcl` (line 343). It calls `exec /usr/bin/env ASAN_OPTIONS=… $executable $config_file >> stdout 2>> stderr &`. The config file is a generated `.conf` written to a temp dir. The function `wait_server_started` (line 373) polls `stdout` for the regex ` PID: $pid.*Server initialized`. After startup, `start_server` also checks for a "Ready to accept" line count increment (line 751). The test client connects over TCP on an auto-selected port in the range 21111–29111. It also unconditionally writes a Unix socket path (`unixsocket /tmp/…/socket`, line 619–620) into the generated config and stores the path for later `valkeycli_exec` calls — but the type-unit tests do not call `valkeycli_exec` directly, so the socket is unused for the Phase B-1 file set.

The infrastructure selects the test-database with `r select 9` and resets state with `r flushall` + `r function flush` before each `start_server` block (lines 426, 442–443 of `server.tcl`). `function flush` will return `-ERR unknown command 'function'` on our server, which will terminate test setup unless we stub it to return `+OK`.

## What we'd need to do to point it at our Rust server

**Step 1 — Binary path.** `support/set_executable_path.tcl` (line 5–8) reads the env var `VALKEY_BIN_DIR` and constructs `$::VALKEY_SERVER_BIN` as `$VALKEY_BIN_DIR/valkey-server`. Our binary is called `redis-server`, not `valkey-server`. Two options: (a) symlink `target/debug/valkey-server → target/debug/redis-server` and set `VALKEY_BIN_DIR=$(pwd)/target/debug`, or (b) add a one-line patch to `set_executable_path.tcl` changing the binary name. Option (a) requires no patch to the reference tree.

**Step 2 — Startup log sentinel.** `wait_server_started` polls for the regex ` PID: <pid>.*Server initialized` in stdout. Our server must emit a log line containing exactly `PID: <own-pid>` and `Server initialized` on startup. Add this log line before the accept loop. Example: `[info] PID: 12345 Server initialized`.

**Step 3 — Stub `FUNCTION FLUSH`.** The harness calls `r function flush` before every test block. Return `+OK\r\n` unconditionally; no real function registry needed.

**Step 4 — Stub `CONFIG GET` / `CONFIG SET`.** Many `start_server` overrides inject config keys like `notify-keyspace-events`, `save`, `loglevel`, `databases`. The server receives these as a config file, not via wire commands. However, tests themselves call `CONFIG GET <key>` and `CONFIG SET <key> <val>` in-flight (e.g. `hash-max-ziplist-value`, `zset-max-ziplist-entries`). Returning `+OK` for any `CONFIG SET` and a plausible default for `CONFIG GET` is sufficient for Phase B-1 type tests, as long as tests that rely on the actual encoding change are tagged and will naturally fail.

**Step 5 — Startup config file parsing.** The server is launched as `./valkey-server /tmp/valkey-test-XXXXXX.conf`. We must parse the config file and at minimum honour `port`, `bind`, `daemonize no`, `loglevel`, `databases`. Unknown directives should be silently ignored rather than causing `FATAL CONFIG FILE ERROR` (which `wait_server_started` detects at line 400 and aborts).

**Invocation to try first:**
```sh
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl \
  --single unit/type/string \
  --clients 1 \
  --skip-leaks \
  --denytags needs:repl
```
`--denytags needs:repl` prevents tests that start a second server for replication.

## Architectural blockers

- **`PID: … Server initialized` log line** — without it, `wait_server_started` spins for 2 minutes then aborts. Must-fix before any test runs.

- **Unix domain socket `unixsocket` config directive** — the config file unconditionally includes `unixsocket /path/socket`. Our server must not crash on unknown config directives. Workaround: parse and silently ignore unknown keys. `valkeycli_exec` (the only caller of the socket) is not used by any Phase B-1 test file.

- **`FUNCTION FLUSH`** — called at the start of every test block in external mode (server.tcl line 443). Stub to return `+OK`.

- **`SELECT 9`** — the harness runs all tests in database 9 (line 426). We must support `SELECT` with at least 10 databases. Likely already implemented.

- **`CONFIG GET <param>` / `CONFIG SET <param> <val>`** — used heavily in hash, zset, and list tests to switch between listpack and ziplist encodings. Stub `CONFIG SET` to return `+OK`; stub `CONFIG GET` to return a two-element array `(<param> <default>)`. Tests that rely on the encoding change will fail, but setup won't abort.

- **`DEBUG SET-ACTIVE-EXPIRE 0/1`** — used in string.tcl (~8 tests) and expire.tcl. All such tests carry `{needs:debug}` tag, so they auto-skip if we deny that tag: `--denytags needs:debug`.

- **`MEMORY USAGE`** — 4 tests in string.tcl, none tagged `needs:debug`. These will fail unless implemented. Workaround: implement `MEMORY USAGE` to return a rough estimate (object allocation size); does not need to be Jemalloc-accurate.

- **`OBJECT ENCODING`** — widely used in hash/set/zset to assert internal encoding transitions. Implement to return a fixed string (`"raw"` or `"listpack"`) for the type; tests checking for ziplist↔hashtable promotion will fail, but basic tests will pass.

- **Keyspace notifications (`SUBSCRIBE`/`PSUBSCRIBE`)** — 1 test block in string.tcl (`MSETEX keyspace notifications`, line 591). Requires `CONFIG SET notify-keyspace-events KEA` and pub/sub. Likely fails silently if `CONFIG SET` is a no-op and pub/sub is unimplemented.

- **Replication (`needs:repl`, `external:skip`)** — tests that call `attach_to_replication_stream`. Deny with `--denytags needs:repl`. 11 tests in string.tcl.

- **`FUNCTION` subsystem** — `tests/unit/functions.tcl` is an entire file that must be skipped entirely (`--skipunit unit/functions`).

- **Module loading (`loadmodule` config directive)** — only relevant in cluster and module tests, not Phase B-1.

## Minimum viable subset (Phase B-1)

Run these files in order, denying `needs:debug needs:repl` tags:

1. `unit/type/string` — 116 tests; ~85 expected to pass. Covers SET/GET/APPEND/STRLEN/MGET/MSET/GETRANGE/SETRANGE/SETNX/GETSET/INCR/GETEX/GETDEL/SUBSTR. Blockers: `MEMORY USAGE` (4 tests, will fail), `DEBUG *` (11 tests, auto-skip with tag deny), replication (11 tests, auto-skip).

2. `unit/expire` — 81 tests; ~65 expected to pass. Heavy use of EXPIRE/EXPIREAT/TTL/PTTL/PERSIST/PEXPIRE. Blockers: `debug set-active-expire` (tagged, auto-skip), AOF-reload tests (`debug loadaof`, tagged), replication (tagged).

3. `unit/scan` — 20 tests; ~17 expected to pass. Covers SCAN/SSCAN/HSCAN/ZSCAN. Blockers: 3 `needs:debug` tests (auto-skip).

4. `unit/type/hash` — 81 tests; ~55 expected to pass. Blockers: `CONFIG SET hash-max-ziplist-value` (11 invocations — blocks encoding-switch tests but not pure-command tests); `DEBUG OBJECT` (1 test).

5. `unit/type/set` — 86 tests; ~70 expected to pass. Lighter `CONFIG SET` usage (7 calls). Mostly pure SET commands.

6. `unit/type/zset` — 170 tests; ~100 expected to pass. Heavy `CONFIG SET zset-max-ziplist-*` (43 calls). Tests that exercise encoding promotion will fail; core command-result tests should pass.

7. `unit/type/list` — 148 tests; ~80 expected to pass. `CONFIG SET list-compress-depth` / `list-max-listpack-size` (21 calls); blocking-pop tests (BLPOP/BRPOP/BLMOVE) will time out or fail without async wait support; `LMPOP`/`BLMPOP` need implementation check.

8. `unit/keyspace` — 65 tests; ~50 expected to pass. Covers DEL/EXISTS/TYPE/RENAME/KEYS/DBSIZE/RANDOMKEY/SORT-lite.

## Predicted failure categories (today)

- **`MEMORY USAGE`** — ~6 test blocks across string.tcl and list.tcl. Will error with unknown command unless implemented.
- **`DEBUG OBJECT` / `DEBUG SET-ACTIVE-EXPIRE`** — ~19 test blocks. Auto-skipped with `--denytags needs:debug`; zero failures if tag deny is used.
- **`CONFIG GET` returning wrong shape** — if `CONFIG GET` is not implemented or returns a non-list, tests that call `lindex [r config get param] 1` will TCL-error and abort the entire file. Must return a 2-element list.
- **`OBJECT ENCODING`** — tests checking for `ziplist`/`listpack`/`embstr` encoding will fail (wrong value returned). Approximately 15 test blocks across hash, zset, list.
- **Blocking commands (BLPOP/BRPOP/BLMOVE/BLMPOP)** — require client-side deferring and timeout handling. The TCL harness uses `rd` (deferring client). These will time out or return errors. ~15 test blocks in list.tcl.
- **Keyspace notifications / SUBSCRIBE** — ~2 test blocks (string.tcl, keyspace.tcl). Will fail if pub/sub is absent.
- **`FUNCTION FLUSH`** — kills test setup entirely if not stubbed. Zero test blocks pass until this is fixed.

## Recommended next action

Add the three stubs (`FUNCTION FLUSH → +OK`, startup log sentinel, config-file unknown-directive ignore) and then run the single-file MVP:

```sh
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl \
  --single unit/type/string \
  --clients 1 \
  --skip-leaks \
  --denytags "needs:repl needs:debug"
```

This converts the string command suite from ~14 hand-written corpus scripts (~100 assertions) to the canonical 116-test file with one shell command. Once string.tcl reaches >90% pass rate, add `unit/expire` as the second gate. The `--denytags` flag is the lever that keeps the suite runnable while the server matures; progressively remove tag denials as the missing features land.

## Risks and caveats

The biggest risk is config-file parsing: the generated `.conf` contains directives our server has never seen (`enable-protected-configs yes`, `enable-debug-command yes`, `propagation-error-behavior panic`, `shutdown-on-sigterm force`, `enable-debug-assert yes`, `hide-user-data-from-log no`, `notify-keyspace-events KEA`, `latency-monitor-threshold 1`, `repl-diskless-sync-delay 0`). If the server exits on the first unknown directive, `wait_server_started` will time out after 2 minutes per test file — a silent, costly failure. Validate this first with a manual `./target/debug/valkey-server /path/to/generated.conf` before wiring the TCL runner. The secondary risk is that `CONFIG GET` must return a proper RESP list (not an error and not a scalar); a malformed reply causes TCL's `lindex` to throw and aborts the file mid-run, making the failure count misleadingly low.

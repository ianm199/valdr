# TCL oracle triage — `unit/type/string` baseline

First run of Valkey's canonical TCL test suite against the Rust server,
captured 2026-05-17 by Round-9 (TCL oracle agent).

## Reproducer

```sh
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port
bash harness/oracle/setup_tcl_runner.sh         # builds + creates valkey-server symlink
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl \
  --single unit/type/string \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug" \
  --skiptest "MSETEX keyspace notifications" \
  --skiptest "/SET with IFEQ" \
  --skiptest "/^LCS" \
  --skiptest "SETRANGE with huge offset" \
  --timeout 60
```

`--tags "-X"` denies tag X; deny-listing `needs:repl` and `needs:debug` skips
the 9 tests in `string.tcl` that require replication or `DEBUG SET-ACTIVE-EXPIRE`
support. `--skiptest` skips by name (a leading `/` makes it a regexp). The
four `--skiptest` filters above suppress tests that *hang* the suite (pubsub
keyspace notifications, IFEQ extension, LCS subcommand, SETRANGE allocator
guard); without them the harness blocks until its 20-minute internal timeout
fires.

## Headline result

| outcome  | count |
|----------|-------|
| passed   | **81** |
| failed   | 6 |
| ignored  | 9 (tag-denied) |
| skipped  | 20 (via `--skiptest`, mostly Valkey-only extensions) |
| total in file | 116 |

Equivalent to **~70 %** of the canonical string suite green on first contact,
including the high-value SET/GET/MSET/MSETEX/GETEX/SETNX/STRLEN/APPEND
families. The wire-diff smoke (18/18 scripts) still passes.

## First 10 failures — categorised

1. **MSETEX with illegal arguments** (cat c) — `r msetex 1 key value EX 0`
   currently *succeeds* and returns `1`; Valkey rejects with
   `ERR invalid expire time in 'msetex' command`. Our `setex_generic` and the
   new `msetex_command` both treat `expire <= 0` as invalid, but the test
   uses `ex 0` which the parser passes through. Fix: tighten the
   non-positive check in `crates/redis-commands/src/string.rs::msetex_command`
   for the `EX 0` path. Genuine bug in the MSETEX implementation just landed.

2. **SETBIT against integer-encoded key** (cat a) — `assert_encoding int mykey`
   expects `OBJECT ENCODING` to report `int` for integer-valued strings; we
   always report `raw`. Genuine implementation gap: the encoding-promotion
   logic in `redis-types::object` knows about `StringEncoding::Int` but
   `set` always builds a `Raw`. Fix: have `SET` parse numeric values and
   store them as `StringEncoding::Int` when they round-trip cleanly.

3. **GETBIT against integer-encoded key** (cat a) — same root cause as 2.

4. **SETRANGE against integer-encoded key** (cat a) — same root cause as 2.

5. **SETRANGE with out of range offset** (cat a) — `SETRANGE` at a 512 MB
   offset should reject with `ERR string exceeds maximum allowed size`; we
   happily allocate 512 MB and return the new length. Fix: add the
   `proto-max-bulk-len`-style guard (default 512 MB) in
   `string::setrange_command` before resizing the buffer.

6. **SETRANGE with huge offset** (cat c — *would deadlock*) — uses a 4 GiB
   offset; we'd OOM-allocate. Currently skipped via `--skiptest`; fix is the
   same guard as #5.

7. **MSETEX keyspace notifications** (cat d — *would hang*) — depends on
   `CONFIG SET notify-keyspace-events KEA` actually wiring the pub/sub
   notification path, plus a working `PSUBSCRIBE` deferring-client. Out of
   scope for the wire-diff oracle; permanently skip via `--skiptest`.

8-13. **`SET … IFEQ …` family** (cat d — *would abort the file*) — 6 tests
   exercise a Valkey-only conditional-set extension. Our SET parser
   correctly rejects the unknown `IFEQ` token with `ERR syntax error`; the
   tests expect `OK`. Either implement IFEQ in `string::set_command` or
   permanently `--skiptest "/SET with IFEQ"`.

14-15. **`LCS` family** (cat b — *would abort the file*) — `LCS` is wired in
   `dispatch.rs` but the handler returns
   `ERR LCS not yet implemented in the Rust port`. Tests assert specific
   prefix/suffix strings, so they explode on first call. The whole file
   aborts because the test body raises an uncaught exception. Implementing
   LCS (O(n·m) DP) would recover 5 tests.

### Bucket totals (post-tag-deny)

- (a) **Genuine bugs** in current implementation: 5 (encoding promotion x3,
  SETRANGE size guard x2). The MSETEX expire-zero bug (item 1) is also a
  genuine bug.
- (b) **Commands not yet implemented**: 1 family (`LCS`, blocks 5 tests).
- (c) **Test-infrastructure issues we can fix in our server**: 2
  (MSETEX EX=0, SETRANGE huge offset; both unblock once we add size /
  arg guards).
- (d) **Valkey-only features we won't support (skip via deny-tag or
  `--skiptest`)**: 11 tests across IFEQ (6), keyspace notifications (1),
  needs:repl (8 already auto-skipped), needs:debug (~5 already auto-skipped),
  hash-randomness DEBUG OBJECT (1).

## Recommended next 3 actions

1. **Implement `StringEncoding::Int` promotion in `SET`**. One change to
   `string::set_command` that, after building the value object, tries
   `parse_strict_i64(value.as_bytes())` and, on success, replaces the
   `Raw` payload with `Int(n)`. Unblocks the 3 `assert_encoding int`
   failures *and* an unknown number of similar checks in `bitops.tcl`,
   `incr.tcl`, `expire.tcl`. Highest bang per buck.

2. **Add the 512 MB SETRANGE / APPEND guard**. Reject `SETRANGE` and
   `APPEND` when the resulting length would exceed
   `512 * 1024 * 1024` with the canonical
   `ERR string exceeds maximum allowed size`. Unblocks two failures plus
   the currently-skipped hang. ~10 LOC.

3. **Land `unit/expire` as the second TCL file** to drive
   TTL / GETEX / SETEX coverage further. Should pick up an additional
   ~50 passes for ~zero new code. Then `unit/type/hash` after that.

## Notes / caveats

- The harness's 20-minute default per-file timeout makes "hangs vs slow" hard
  to distinguish. The reproducer above uses `--timeout 60` to surface them
  fast. The pre-existing hand-written wire-diff smoke (`harness/oracle/smoke.sh`)
  continues to pass at 18/18 scripts.
- The MSETEX implementation just landed in Round 9 is a fresh port; its few
  test failures are likely cheap to fix (off-by-one on the `EX 0` check).
- `CONFIG SET` and `CONFIG GET` were stubbed by a parallel agent (the same
  round that wired INFO/HLL); `CONFIG GET <pattern>` now returns a small
  default table with proper glob matching. Tests that gate on
  `hash-max-listpack-value`-style encoding switches will still fail
  semantically but no longer abort the test file.

## Round 10a update (2026-05-17)

### `unit/type/string` — 81 → 92 / 116

The Round-9 triage was generated *before* the matching code fixes landed.
`set_command` already calls `RedisObject::new_string_try_encoded` (which
promotes canonical decimal ASCII bytes to `StringEncoding::Int`), and
`setrange_command` already returns
`ERR string exceeds maximum allowed size (proto-max-bulk-len)` for
offset+len overflows. The Round 9 baseline therefore under-counted the
real green total. Re-running with only the still-needed `--skiptest`
filters (MSETEX keyspace notifications, IFEQ extension, LCS family — the
latter two are Valkey-only / not-yet-implemented) yields **92 passes,
0 failures**. The previously-skipped `SETRANGE with huge offset` now
passes thanks to the 512 MB guard.

Updated reproducer:

```sh
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/string \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug" \
  --skiptest "MSETEX keyspace notifications" \
  --skiptest "/SET with IFEQ" \
  --skiptest "/^LCS" \
  --timeout 60
```

### `unit/type/list` — first run: 185 / 246 reachable

98 declared tests fan out to 246 instantiations via the `foreach
{type large}` / `foreach resp {3 2}` matrices. With `-needs:repl
-needs:debug -resp3` denied and three test-file aborts skipped, we get
**185 passed, 61 failed** on first contact. The `-resp3` deny is
required because `HELLO 3` raises `NOPROTO RESP3 not yet supported`,
which propagates out of `r hello $resp` and kills the file.

Three skip filters defuse hard aborts (Tcl exceptions that bubble past
`test {}` and stop the whole file):

| skip                                              | reason                                                                |
|--------------------------------------------------|-----------------------------------------------------------------------|
| `BRPOPLPUSH does not affect WATCH while still blocked` | Tcl `fail "$cmd was not called"` reads an undefined `$cmd` var when our non-blocking BRPOPLPUSH unblocks instantly; the failure path itself raises. |
| `/SORT`                                          | `SORT` handler exists in `crates/redis-commands/src/sort.rs` but uses an obsolete `CommandContext` API (compiles in isolation but not via the current crate). Tests using `r sort ... store` abort the file with `ERR unknown command 'sort'`. Re-wiring `sort_command` is a separate task. |
| `/various encodings`                             | Test reads `r dump k` into a Tcl var; DUMP is not implemented and the unset var trips the next test. |

Reproducer:

```sh
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/list \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug -resp3" \
  --skiptest "BRPOPLPUSH does not affect WATCH while still blocked" \
  --skiptest "/SORT" \
  --skiptest "/various encodings" \
  --timeout 30
```

#### Changes landed this round

- `crates/redis-commands/src/list.rs`: added non-blocking stubs for
  `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`. They behave like
  the matching non-blocking command when data is present and reply with
  a null array immediately otherwise. The `parse_blocking_timeout`
  helper validates negative / non-numeric / overflow timeouts using
  Redis' canonical error strings (including hex-string acceptance for
  `0x7FFFFFFFFFFFFF`-style values).
- `crates/redis-commands/src/dispatch.rs`: wired BLPOP/BRPOP/BLMOVE/
  BRPOPLPUSH/BLMPOP.
- `crates/redis-commands/src/connection.rs`: added `CLIENT UNBLOCK`,
  `CLIENT PAUSE`, `CLIENT REPLY`, `CLIENT KILL` subcommand stubs so
  the test scaffolding stops aborting on `r client unblock $id`.

#### Top failure categories (list.tcl, ordered by count)

1. **Real blocking semantics required (~45 tests).** Tests assert
   `wait_for_blocked_client` succeeds or that a `BLPOP` with timeout 0
   blocks until a parallel `LPUSH` lands. Our stubs reply immediately
   so the deferred client's `$rd read` returns `*-1` (null array)
   before the expected payload arrives.
2. **Listpack ↔ quicklist encoding transitions (~6 tests).** Tests use
   `CONFIG SET list-max-listpack-size N` and expect the encoding name
   reported by `OBJECT ENCODING` to flip between `listpack` and
   `quicklist` based on the entry count. Our `Inline` heuristic in
   `redis-core::object::list_inline_is_quicklist` only consults
   element byte-sizes, ignoring the configured entry-count cap.
3. **`CLIENT UNBLOCK` real semantics (2 tests).** Our stub returns `0`
   (no unblock) but the test expects the deferred client to receive
   `UNBLOCKED client unblocked via CLIENT UNBLOCK` on the BLPOP it
   issued. Same root cause as #1 — there is no blocked client to
   unblock.
4. **`SORT … STORE` chain (1 test file abort).** `sort_command` exists
   but is plumbed against the wrong `CommandContext` API surface;
   wiring it requires the API refactor that
   `harness/reconcile_types.sh` covers for the other crates.
5. **`DUMP` not implemented (1 test file abort).** Test stores `r dump
   k` into `$dump` then `RESTORE`s; without DUMP the var is unset and
   the next read crashes.

#### Recommended next 3 highest-leverage fixes

1. **Pump `list-max-listpack-size` through to
   `list_inline_is_quicklist`.** The constant
   `LIST_LISTPACK_NODE_MAX_BYTES = 16 KiB` is hard-coded; read the
   configured value out of the server config (or thread it via
   `ServerContext`) and use it. Unblocks the 6 encoding-transition
   tests in list.tcl plus a handful of similar checks in hash / set /
   zset files.
2. **Wire `crates/redis-commands/src/sort.rs::sort_command` into
   dispatch.** The handler is written but uses a `CommandContext` API
   surface (`arg_bytes`, `arg_parse_i64`, `lookup_key_read_by_bytes`,
   `hash_get_field_as_object`) that no longer exists. Either restore
   those convenience methods on `CommandContext` or rewrite
   `sort_command` against the current API. Adds SORT/SORT_RO coverage
   in `string`, `list`, `set`, `zset` test files.
3. **Land minimal-fidelity blocking infrastructure.** A single
   `BlockedClient` registry in `redis-core` plus a per-key
   `ready_keys` queue would let BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP
   actually wait and wake. Unblocks ~45 list.tcl failures and is also
   needed before stream/zset XREAD/BZ\*POP work. Largest-ticket but
   highest payoff.

### Files touched

- `crates/redis-commands/src/list.rs`
- `crates/redis-commands/src/dispatch.rs`
- `crates/redis-commands/src/connection.rs`
- `docs/TCL_TRIAGE.md` (this file)

Hand-corpus smoke remains **19/19 PASS**.

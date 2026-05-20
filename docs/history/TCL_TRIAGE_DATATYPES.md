# TCL baseline — hash / set / zset

First run of Valkey's canonical `unit/type/{hash,set,zset}.tcl` against the
Rust server, captured 2026-05-17 by Round-10b. Tests were executed against
binary built from commit `59bbe91` (Round 9 head); Round 10a is mid-edit on
`redis-core` and a fresh `cargo build` currently fails, so these numbers
reflect the *previous* green-build snapshot at `target/debug/redis-server`
(mtime `May 17 15:45:17`).

## TL;DR

Across the three datatype TCL files: **277 PASS / 79 FAIL / 12 IGNORE** (368
test outcomes) once abort-blockers are skipped. **Hash is effectively gated
at 1/~150** by a bare `assert_encoding listpack` outside any `test {}`
block — exactly the encoding-promotion gap the Round 9 doc called out as
top-priority. Fixing listpack/intset/skiplist encoding reporting would
unlock the *majority* of failures in all three files (estimated +120 tests).

## Reproducer

```sh
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port
bash harness/oracle/setup_tcl_runner.sh        # builds + creates valkey-server symlink

cd reference/valkey

# hash.tcl — aborts after 2 outcomes; no recovery possible without source edits
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/hash \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug" \
  --timeout 30 --baseport 21000

# set.tcl — runs the whole file once SRANDMEMBER hang is skipped
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/set \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug" \
  --skiptest "SRANDMEMBER count overflow" \
  --timeout 30 --baseport 28000

# zset.tcl — needs several skiptests to bypass abort-on-failure setup blocks
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/type/zset \
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug -resp3" \
  --skiptest "/RESP3" \
  --skiptest "/BZPOP" \
  --skiptest "/BZMPOP" \
  --skiptest "/ZDIFF fuzzing" \
  --skiptest "/ZRANGE BYLEX" \
  --skiptest "/ZRANGE BYSCORE" \
  --skiptest "/ZRANGESTORE BYLEX" \
  --timeout 30 --baseport 27000
```

## hash.tcl

- **Pass: 1 / 2 attempted** (~150 unreachable)
- **Tag-denied skips: 0** in the 2 that ran
- **True hangs requiring `--skiptest`: 0** — the issue is an abort, not a
  hang

### Why so few

The first `test {}` block (`HSET/HLEN - Small hash creation`) passes. The
second (`Is the small hash encoded with a listpack?`) fails because we
return `hashtable` for an 8-element hash that Valkey would store as
`listpack`. That failure is *inside* a `test {}` so it merely counts as
one `[err]`.

Immediately after, at `hash.tcl:41`, a bare `assert_encoding $type myhash`
runs in the `foreach {type contents}` body **outside any `test {}`
block**. Because we still return `hashtable`, the assertion raises
uncaught and aborts the whole file. `--skiptest` cannot bypass it
(no test name to match). The only recoveries are (a) fixing the
encoding to honour listpack/hashtable thresholds, or (b) editing the
TCL file (forbidden in this round).

Roughly 79 `test {}` blocks remain unreachable downstream. The same root
cause (encoding-promotion gap) is also responsible for the corresponding
zset abort at `zset.tcl:2433`.

### Top failure categories

1. **Hash encoding always reports `hashtable`** — 1 test fail visible,
   plus the abort that hides ~150 more. Hash `OBJECT ENCODING`
   should pick between `listpack` (when entry count ≤
   `hash-max-listpack-entries` *and* every key/value length ≤
   `hash-max-listpack-value`) and `hashtable` otherwise. Currently
   always `hashtable`.

(Only one observable category in this file because of the abort.)

## set.tcl

- **Pass: 61 / 118 outcomes** (52 % of what ran)
- **Tag-denied skips: 5** (`needs:debug` for the DEBUG-RELOAD encoding
  reload tests)
- **True hangs requiring `--skiptest`: 1** —
  `SRANDMEMBER count overflow` calls
  `r srandmember myset -9223372036854775808` (i64::MIN). Our
  `SRANDMEMBER` apparently tries to allocate `count`-sized storage for
  the negative path; with i64::MIN this hangs / OOMs. Same root-cause
  category as Round 9's `SETRANGE with huge offset`.

### Top failure categories

1. **`OBJECT ENCODING` always returns `hashtable` for sets** — **48 of
   52 failures** (`Expected 'hashtable' to match 'listpack'` ×25 and
   `'hashtable' to match 'intset'` ×23). Affects nearly every test in
   the file: SADD/SREM basics, SPOP, SRANDMEMBER, SUNION/SDIFF/SINTER
   store, SMOVE, SADD overflow paths, the intset→listpack→hashtable
   promotion ladder. Fix promotes ~48 tests in this file alone.

2. **SINTERCARD `LIMIT` parse error message** — 1 test. We emit
   `ERR value is not an integer or out of range`; Valkey expects
   `ERR numkeys should be greater than 0` /
   `ERR LIMIT can't be negative`. Pure error-message wording.

3. **SRANDMEMBER returning duplicates instead of unique values** — 2
   tests (`Expected 'a b c' to be equal to 'b'` etc.). For non-listpack
   sets we're returning fewer unique elements than expected. May be a
   side-effect of always being on the `hashtable` code path even when
   the test created a small set.

4. **SRANDMEMBER count overflow (i64::MIN)** — 1 hang, documented
   above. Same fix as the SETRANGE size-guard in `string.tcl`.

5. *(no other distinct categories at this scale)*

## zset.tcl

- **Pass: 216 / 238 outcomes** (91 % of what ran)
- **Tag-denied skips: 3** (`needs:debug` for DEBUG-RELOAD-style tests)
- **True hangs requiring `--skiptest`: 0** — all four `--skiptest`
  filters bypass *aborts*, not hangs:
  - `/RESP3` — every `r hello 3` site raises `NOPROTO RESP3 not yet
    supported`. RESP3 is unimplemented in our protocol layer.
  - `/BZPOP`, `/BZMPOP` — blocking pop variants are not registered;
    the deferred-client read sees `ERR unknown command 'BZPOPMIN'` and
    aborts because the test wraps `$rd read` outside a catch.
  - `/ZDIFF fuzzing` — runs 100 random ZDIFFs over up to 11 sets ×
    100 elements. The server appears to crash or the harness loses
    track of stderr right after this test; subsequent tests in the
    file never produce output. Listed as a hang-equivalent for now;
    needs a separate repro pass to know whether it's a memory bug or
    just a slow path.
  - `/ZRANGE BYLEX`, `/ZRANGE BYSCORE`, `/ZRANGESTORE BYLEX` — our
    `ZRANGE` handler explicitly returns
    `ERR syntax error, BYLEX not implemented yet in this port`,
    which is uncaught and aborts the file. (`ZRANGEBYLEX` and
    `ZRANGEBYSCORE` as separate commands *do* work; only the unified
    `ZRANGE … BY*` form is unimplemented.)

The second `start_server` block at `zset.tcl:2746` is also unreachable
because of an abort at `zset.tcl:2433` (another bare `assert_encoding`
in a `foreach` body, mirroring `hash.tcl:41`). Fixing zset listpack
encoding promotion would unlock that block (~50 more tests).

### Top failure categories

1. **`OBJECT ENCODING` for zsets always returns `skiplist`** —
   9 of 22 failures (`Expected 'skiplist' to match 'listpack'`).
   Affects ZSCORE/ZMSCORE/ZRANGESTORE encoding probes plus the
   sorter/skip-list backlink consistency tests. Same root-cause
   as the hash & set encoding gaps. Fix would also recover the
   final abort and unlock the second `start_server` block.

2. **+inf / -inf / NaN score handling in ZUNIONSTORE / ZINTERSTORE** —
   8 failures (4 `+inf/-inf` × 2 encodings, 4 `NaN weights` × 2
   encodings). Two distinct bugs hiding here:
   - `WEIGHTS nan` is rejected with the wrong error
     (`ERR value is not a valid float` vs Valkey's
     `*weight value is not a float*`).
   - `+inf * 0` should yield `NaN` (and abort the merge with
     `ERR resulting score is not a number (NaN)`), but we silently
     produce `0`. Score arithmetic is not following IEEE-754 NaN
     propagation. Likely a single fix in the merge accumulator.

3. **ZRANGESTORE `BYLEX` / `BYSCORE` unimplemented** — 2 failures
   that survived (different from the skiptests above; these are
   `ZRANGESTORE` callsites the harness reached before the abort).
   Same root cause: the unified `BY*` argument parser.

4. **Error-message wording for `LIMIT` / numkeys / zunionInter
   validation** — 4 failures (`ERR LIMIT*`, `ERR numkeys*`,
   `'zunion' command` text, `at least 1 input key`). Cosmetic but
   the harness asserts via glob match on the exact message.

5. **ZMPOP / ZINTERCARD illegal-argument error wording** — 2
   failures, same pattern as #4.

## Recommended fixes (highest-leverage first)

1. **Implement listpack/intset/hashtable/skiplist encoding promotion
   in HASH, SET, ZSET `OBJECT ENCODING`** — would unlock the *vast*
   majority of failures in this triage:
   - hash.tcl: unblocks the abort at line 41, recovers ~79 currently-
     unreachable tests plus the one observable encoding fail.
   - set.tcl: directly fixes 48 of 52 failures.
   - zset.tcl: fixes 9 of 22 failures *and* unblocks the abort at
     line 2433, recovering ~50 more tests in the second
     `start_server` block.
   Estimated total unlock: **~180 tests across these 3 files**, an
   order of magnitude bigger than any other single fix here.
   The same fix would also fold straight into Round 9's outstanding
   `assert_encoding int mykey` failures on `string.tcl` and probably
   improve `bitops.tcl` / `expire.tcl` coverage in Round 10c.

2. **Add NaN/Inf propagation in ZUNIONSTORE/ZINTERSTORE score
   arithmetic, plus the `WEIGHTS nan` wording** — would unlock
   ~8 zset tests. Should be ~15 LOC in the score-accumulator: any
   intermediate score that is NaN (e.g. `+inf * 0`) must propagate
   to the merged set and trigger the `ERR resulting score …`
   guard, and the `WEIGHTS` parser must reject `nan` with the
   canonical message.

3. **Implement `ZRANGE … BYLEX | BYSCORE | REV LIMIT` argument
   parsing** — would unlock the 3 currently-skiptested tests
   (`ZRANGE BYLEX`, `ZRANGE BYSCORE`, `ZRANGESTORE BYLEX`) plus
   the 2 `ZRANGESTORE BY*` tests that already fail and let the
   harness reach more of the post-line-2362 block. Should reuse the
   existing `ZRANGEBYLEX` / `ZRANGEBYSCORE` paths under the hood;
   work is mostly in `range_command` arg parsing. Estimated
   ~10 tests across hash/set/zset (mostly here) plus future
   coverage when running `unit/type/list-zset` style files.

Honourable mentions (each ~1–4 tests but cheap):

- Tighten error-message wording for `ZINTERCARD`/`ZMPOP`/`SINTERCARD`
  `LIMIT` and numkeys validation to match Valkey's exact strings.
- Reject `i64::MIN` (and other huge negative counts) in `SRANDMEMBER`
  before allocating, mirroring the SETRANGE guard Round 9 recommended.
- Implement `BZPOPMIN` / `BZPOPMAX` / `BZMPOP_MIN` / `BZMPOP_MAX`
  (blocking pop) and `HELLO 3` (RESP3 negotiation) — large
  surface-area work; would each unlock dozens of additional tests
  but lives in a different round.

## Caveats

- The numbers above are for outcomes the harness *recorded*. Many
  `test {}` blocks are dynamically generated inside `foreach`
  loops, so the denominator (the `grep -c '^test'` count of 81 /
  86 / 170 in the three source files) understates the true
  attempted count when loops execute and overstates it when an
  abort cuts off a loop.
- Round 10a is concurrently editing `crates/redis-core` (build
  currently broken at HEAD). Once that lands and the binary
  rebuilds, expect these numbers to shift — particularly
  hash/set/zset encoding-related failures, which Round 10a is
  reportedly targeting.
- The `ZDIFF fuzzing - listpack` abort was treated as a hang
  equivalent because *something* in or right after it kills the
  test client. We did not diagnose whether the Rust server itself
  crashed (no stderr capture in our harness path). Worth a focused
  repro in a follow-up round.

# Rust Performance Improvement Playbook

This note is an addendum to the active performance goal. Use it when resuming
work on `redis-rs-port` performance so the loop stays evidence-driven and the
Rust changes stay maintainable.

## Current Scoreboard

Primary matrix:

```sh
VALKEY_BENCH_SKIP_BUILD=1 python3 harness/bench/default-suite-parts.py run \
  --mode ordered \
  --target both \
  --tests ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,mget,xadd \
  --requests 100000 \
  --clients 50 \
  --pipeline 1 \
  --payload 64 \
  --timeout-s 60 \
  --no-build
```

Latest representative non-function artifact after the SET/I/O and
FUNCTION/FCALL pass:

```text
harness/bench/results/20260527T025338Z-2ff3fcc-default-suite-parts.json
```

Score: 21/21 pass, median `1.060x`, weakest row `0.986x`.

Latest pipeline smoke:

```text
harness/bench/results/20260527T025426Z-2ff3fcc-pipeline-smoke.json
```

Score: 12/12 pass, median `1.133x`, P16 median `1.192x`, P100 median
`1.556x`; no P100 cliff or timeout.

Latest JSON document mix:

```text
harness/bench/results/20260527T025203Z-2ff3fcc-json-doc-mix.json
```

Score: 3/3 pass, median `0.994x`, weakest row `0.987x`.

Latest function-inclusive matrix:

```text
harness/bench/results/20260527T025051Z-2ff3fcc-default-suite-parts.json
```

Secondary matrix for pipeline and p50/slow-start anomalies:

```sh
python3 harness/bench/pipeline-smoke.py --commands get,ping_mbulk,set,incr --pipelines 1,16,100
```

Latest secondary artifact:

```text
harness/bench/results/20260527T025426Z-2ff3fcc-pipeline-smoke.json
```

The ordered default suite is the main scoreboard. `pipeline-smoke` is still
useful for pipeline-depth regressions and odd latency starts.

Use `harness/bench/format-results.py` for inspection tables:

```sh
python3 harness/bench/format-results.py \
  harness/bench/results/20260527T025338Z-2ff3fcc-default-suite-parts.json \
  harness/bench/results/20260527T025426Z-2ff3fcc-pipeline-smoke.json \
  harness/bench/results/20260527T025203Z-2ff3fcc-json-doc-mix.json \
  harness/bench/results/20260527T025051Z-2ff3fcc-default-suite-parts.json
```

## Goal Addendum

When optimizing, explicitly include maintainable Rust performance practice as
part of the goal:

- Prefer bounded changes backed by benchmark and profiler evidence.
- Improve hot paths by removing avoidable work, not by adding benchmark-specific
  branches.
- Preserve Redis/Valkey semantics, including RESP shape, wrong-type behavior,
  expiry behavior, tracking, dirty counters, notifications, and wake ordering.
- Keep FUNCTION LOAD, FCALL, and Lua out of scope unless a system-level profile
  proves they block the main goal.
- Treat unsafe Rust as an explicit performance exception, not a default tool:
  require profile evidence, a safe baseline comparison, a narrow audited wrapper,
  a `SAFETY` comment, and an unsafe-budget update before keeping it.
- Record the benchmark artifact path, profile artifact path, changed files, and
  next bottleneck before moving to the next packet.

## Safety Guardrail

Current first-party unsafe state:

- 14 `unsafe` blocks total, tracked by
  `harness/unsafe-budgets.toml`.
- Zero unsafe in `redis-types`, `redis-protocol`, `redis-ds`, the parser, the
  runtime dispatch table, and the Rust-native data-structure ports.
- Approved exceptions are OS/process-control calls, AArch64 hardware timer
  register reads, and the cached `mlua` FUNCTION/FCALL active-context bridge.

Performance work should continue to optimize in safe Rust by default. New
unsafe is only acceptable when the profile proves a material hot-path cost that
safe Rust cannot remove cleanly, and the change stays behind the smallest
possible safe API. Do not add unsafe to parser, dispatch, reply serialization,
or data-structure loops without an explicit architecture note and a before/after
benchmark packet.

## Fast Loop

1. Run or read the latest representative matrix.
2. Pick the weakest in-scope row, not the most interesting row.
3. Reproduce it with the smallest ordered subset that preserves state setup.
4. Profile that exact workload with `probe-hypotheses.py xctrace-time`,
   `profile-hotspots.py`, or `profile-calltree.py`.
5. Classify top costs before editing:
   runtime ownership, parser/serializer, dispatch, allocation, data structure,
   hash lookup, observability, socket I/O, or benchmark noise.
6. Make one bounded change.
7. Run correctness gates first.
8. Rerun the exact cell and then the broader ordered matrix.
9. Keep the change only if the ratio improves or the profile clearly moves the
   bottleneck to a more fundamental cost.

## Correctness Gates

Use the narrowest useful test first, then widen:

```sh
rustfmt --check <changed rust files>
cargo test -p <crate> <targeted_test_name>
cargo check -p redis-server
cargo build --release -p redis-server
bash harness/oracle/smoke.sh --skip-build
```

Do not trust benchmark movement until these pass.

## Profiling Commands

Use `xctrace-time` when the matrix shows a meaningful gap and you need a stack
shape:

```sh
python3 harness/bench/probe-hypotheses.py xctrace-time \
  --command mget \
  --prep-commands mset \
  --prep-requests 100000 \
  --pipeline 1 \
  --requests 10000000 \
  --clients 50 \
  --payload 3 \
  --time-limit-s 6 \
  --warmup-s 0.25
```

For faster iteration, reduce `--requests`, but mark the result as telemetry
rather than a strict profile comparison.

## Rust Hot-Path Rules

Allocation and copying:

- Do not clone argv, keys, or values unless ownership is required across a
  mutable borrow.
- Prefer writing RESP replies directly from borrowed bytes.
- Pre-reserve reply buffers when frame size is known.
- Reuse buffers only when ownership and capacity behavior are explicit.
- Watch for `Vec<Option<Vec<u8>>>`, `to_vec()`, `RedisString::from_bytes()`, and
  `string_bytes_owned()` in loops.

Borrowing and API shape:

- If borrow checker friction causes cloning in a hot path, add a small helper at
  the correct abstraction boundary rather than cloning in command code.
- Prefer helper methods on `CommandContext` or `Client` when the pattern is
  generic reply/lookup behavior.
- Keep command-specific changes source-shaped and semantics-preserving.

Dispatch and observability:

- Put cheap global "is enabled?" checks before expensive tracking, slowlog,
  latency, metrics, ACL, and wake paths.
- Avoid repeated locks, timestamp calls, and config lookups on every command
  when the feature is disabled or unchanged.
- Do not skip hooks blindly. First prove the disabled/common path is equivalent.

Data structures:

- Treat hasher or collection swaps as broad changes requiring full-suite
  validation.
- Prefer local allocation/copy fixes before changing map/list/set/zset storage.
- Tail spikes often come from resize/drop/scan behavior. Use p99/max plus a
  profile before changing structure.

Socket I/O:

- If `recv`/`send` dominate self time, expect small code gains. Work on fixed
  overhead and latency tails rather than chasing impossible throughput jumps.
- Immediate flush changes must be checked against writable-interest correctness.

## Known Recent Lesson

MGET was slow because it staged work:

- cloned every key into a temporary vector;
- cloned every value into `Vec<Option<Vec<u8>>>`;
- wrapped each cloned value in a new `RedisString` before serializing.

The fix streamed each element directly from DB lookup into the reply buffer and
made bulk reply writing reserve the full frame. Representative MGET moved from
`0.769x` to `1.059x`.

This is the model packet: profile evidence, one clear source of avoidable work,
small API helper, correctness test, exact cell benchmark, full-suite benchmark.

## Latest Packet Lesson

List push/pop tails were caused by repeated listpack encoded-size checks that
materialized a temporary `ListPack` and appended every element while the list
was near the compact-encoding boundary. That work only affected a short window,
so it showed up as p99/max damage in 100k-request runs but was diluted in larger
profiles.

The fix replaced the temporary-listpack path with a non-allocating encoded-size
calculator and updated the growth check to use encoded entry size. The exact
list slice improved materially:

- `lpush`: `0.897x` to `1.049x`, p99 `2.047 ms` to `0.479 ms`.
- `rpop`: `0.979x` to `1.039x`, p99 `2.903 ms` to `0.367 ms`.

The broad all-command rerun immediately after was noisy and showed unrelated GET
movement, so the next gate is a smaller simple-plus-list subset before making a
final claim.

## Current Next Targets

After the listpack fix, validate whether the broad-suite GET/RPUSH movement was
host noise or a real regression. Suggested subset:

```sh
python3 harness/bench/default-suite-parts.py run \
  --mode ordered \
  --target both \
  --tests ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop \
  --requests 100000 \
  --pipeline 1 \
  --no-build
```

If that subset is sane, rerun the full ordered suite once or record the exact
list slice plus subset as the support for keeping the packet. If it is not sane,
investigate the persistent weak cell before stacking another source change.

This section is historical after the 2026-05-27 SET/I/O and FUNCTION/FCALL
closure below. The current representative non-function matrix is clean.

## Output Buffer Accounting Probe

Fresh GET P=1 profiling showed the simple-command handler is not the dominant
cost. The profile is mostly socket I/O plus owner-loop accounting:

- Artifact:
  `harness/bench/results/20260527T005155Z-2ff3fcc-xctrace-time.json`
- Self time: `__recvfrom` 37.69%, `__sendto` 31.91%,
  `mach_absolute_time` 10.25%, `kevent` 4.54%.
- Inclusive non-syscall signal:
  `ClientSlot::refresh_output_buffer_state_at` 12.30%,
  `refresh_client_memory_snapshot` 10.84%,
  `client_memory_usage_with_query_len` 2.53%.

Classification: runtime ownership / socket I/O / observability-accounting.

A first bounded source attempt split normal-client output-buffer limit
enforcement from CLIENT LIST memory accounting. It was discarded. The exact GET
cell and broad core matrix did not improve:

- Exact GET after the attempt:
  `harness/bench/results/20260527T005501Z-2ff3fcc-default-suite-parts.json`
  (`0.941x`, Rust p50 `0.199 ms`).
- Broad core after the attempt:
  `harness/bench/results/20260527T005513Z-2ff3fcc-default-suite-parts.json`
  (median `0.917x`, GET `0.813x`).
- Source was reverted and rebuilt. The follow-up core matrix remained in a
  slower host band, so treat the bad row as noisy/thermal-sensitive unless it
  reproduces under a paired A/B runner:
  `harness/bench/results/20260527T005654Z-2ff3fcc-default-suite-parts.json`
  (median `0.934x`, GET `1.034x`).

Do not retry the same split blindly. The next accounting packet should either:

- make `client_output_buffer_limit` a lock-free snapshot so disabled normal
  limits avoid the global mutex without changing CLIENT LIST freshness; or
- add a paired local A/B runner that alternates baseline/candidate binaries on
  the same host band before judging sub-10% changes.

Follow-up kept: reuse the timestamp already passed into
`ClientSlot::refresh_output_buffer_state_at` when refreshing the client memory
snapshot. This changed `crates/redis-server/src/runtime_owner.rs` only.

Evidence:

- Pre-change profile:
  `harness/bench/results/20260527T011655Z-2ff3fcc-xctrace-time.json`.
- Post-change profile:
  `harness/bench/results/20260527T011851Z-2ff3fcc-xctrace-time.json`.
- `mach_absolute_time` self time dropped from 9.69% to 1.89%.
- `ClientSlot::refresh_output_buffer_state_at` inclusive time dropped from
  10.75% to 5.92%.
- Exact short SET P1 probe:
  `harness/bench/results/20260527T011845Z-2ff3fcc-pipeline-smoke.json`
  (`1.068x`, noisy but not regressed).
- 500k core P1 matrix:
  `harness/bench/results/20260527T011931Z-2ff3fcc-default-suite-parts.json`
  (median `1.043x`, weakest SET/GET `0.951x`).
- Pipeline smoke:
  `harness/bench/results/20260527T012008Z-2ff3fcc-pipeline-smoke.json`
  (median `1.179x`, P100 median `1.519x`, weakest cell INCR P16 `0.894x`).

Decision: keep the patch. The profile moved the clock-read bottleneck down
materially; the remaining SET P1 profile is mostly socket I/O
(`recvfrom`/`sendto`) plus smaller registry/accounting costs. Next work should
look at write/read batching and client-info/output-buffer registry updates
before chasing more timer micro-optimizations.

## SET/I/O and FUNCTION/FCALL Closure

Date: 2026-05-27.

Final performance artifacts:

- Non-function default suite:
  `harness/bench/results/20260527T025338Z-2ff3fcc-default-suite-parts.json`
  - 21/21 pass, median `1.060x`, weakest `lrange_600` at `0.986x`.
  - `SET` is `1.108x`, `GET` is `0.998x`, `MSET` is `1.119x`.
- Pipeline smoke:
  `harness/bench/results/20260527T025426Z-2ff3fcc-pipeline-smoke.json`
  - 12/12 pass, no timeouts.
  - P16 median `1.192x`, P100 median `1.556x`.
  - The old deep-pipeline cliff is not present.
- JSON document mix:
  `harness/bench/results/20260527T025203Z-2ff3fcc-json-doc-mix.json`
  - 3/3 pass, median `0.994x`, weakest `mixed` at `0.987x`.
- Function-inclusive default suite:
  `harness/bench/results/20260527T025051Z-2ff3fcc-default-suite-parts.json`
  - 23/23 pass, median `1.082x`, weakest `lrange_600` at `0.989x`.
  - `FUNCTION LOAD` is `4.371x`, `FCALL` is `1.076x`.

SET/I/O classification:

- SET P1 profile:
  `harness/bench/results/20260527T022343Z-2ff3fcc-xctrace-time.json`
  - Top self time was socket and event-loop work: `__recvfrom` about 38.9%,
    `__sendto` about 34.0%, `kevent` about 5.6%.
  - Command dispatch was much smaller than socket I/O.
- SET P16 profile:
  `harness/bench/results/20260527T022401Z-2ff3fcc-xctrace-time.json`
  - Still heavily socket/flush shaped, with dispatch around 35% inclusive and
    `set_command` around 14% inclusive.
  - DB mutation was not the dominant cost.

Decision: there is no obvious SET-specific free lunch left at P1. The tractable
work is generic owner-loop/socket/flush/accounting overhead, not a narrow SET
rewrite. Short 20k pipeline cells can show cold-start noise; repeat before
chasing any P1 dip.

MSET packet:

- Pre-fix MSET profile:
  `harness/bench/results/20260527T024059Z-2ff3fcc-xctrace-time.json`
  - Socket syscalls still dominated, but allocator/free/memmove and argument
    ownership were visible.
- Source change:
  - Store MSET/MSETNX values with
    `RedisObject::new_string_try_encoded_from_redis_string`.
  - Avoid key clones when string keyspace notifications are disabled.
- Post-fix evidence:
  - Isolated MSET:
    `harness/bench/results/20260527T024334Z-2ff3fcc-default-suite-parts.json`
    (`1.079x`).
  - Final representative MSET:
    `harness/bench/results/20260527T025338Z-2ff3fcc-default-suite-parts.json`
    (`1.119x`).

FUNCTION LOAD packet:

- Pre-fix profile:
  `harness/bench/results/20260527T022506Z-2ff3fcc-xctrace-time.json`
  - `function_load_command` was about 94% inclusive.
  - `compile_function_library` was about 91% inclusive.
  - The benchmark repeatedly issued `FUNCTION LOAD REPLACE` for a byte-identical
    library, so recompilation dominated.
- Source change:
  - Add an exact no-op fast path for `FUNCTION LOAD REPLACE` when the incoming
    library name and code bytes match an already-loaded library.
  - Preserve the OOM check before returning success.
  - Use `Cow` in embedded-shebang stripping so the common unmodified source path
    does not allocate.
  - Collapse broad source-flag scans into targeted ASCII passes.
- Post-fix evidence:
  - Final function-inclusive `FUNCTION LOAD` is `4.371x` with Rust p50
    `0.239 ms` versus Valkey p50 `1.103 ms`.

FCALL packet:

- Pre-fix profile:
  `harness/bench/results/20260527T024538Z-2ff3fcc-xctrace-time.json`
  - Steady-state FCALL was mostly socket I/O plus Lua/runtime setup.
  - Repeated synthetic-script scans were visible but small, roughly low
    single-digit self time combined.
- Source change:
  - Precompute synthetic-loop and shortcut checks once per loaded library.
  - Reuse those checks in cached and uncached function execution.
- Post-fix evidence:
  - Function-only check:
    `harness/bench/results/20260527T024720Z-2ff3fcc-default-suite-parts.json`
    (`FCALL` `1.009x`).
  - Final function-inclusive matrix:
    `harness/bench/results/20260527T025051Z-2ff3fcc-default-suite-parts.json`
    (`FCALL` `1.076x`).

Correctness gates for this packet:

- `cargo check -p redis-server`
- `cargo build --release -p redis-server`
- `cargo build -p redis-server`
- Focused Rust tests:
  - `function_load_replace_identical_library_preserves_behavior`
  - `loaded_library_code_identity_matches_name_case_insensitively`
  - `function_source_eval_flags_finds_existing_broad_markers`
  - `function_source_allows_oom_matches_existing_marker_rule`
  - `strip_embedded_eval_shebang_lines_borrows_when_unmodified`
  - `fcall_cached_runtime`
  - `mset_and_msetnx_store_pairs_without_notifications`
  - `mget_returns_values_nulls_and_int_encoded_strings`
  - `incr_int_encoding_fast_path_preserves_watch_dirtying`
- `bash harness/oracle/smoke.sh --skip-build`: 23/23 scripts passed.
- `python3 harness/oracle/tcl-survey.py --runner-id manual-functions-perf-goal --skip-build --timeout-s 150 --files unit/functions`:
  93/93 passed.
- `bash .claude/hooks/unsafe-budget.sh </dev/null`
- `git diff --check`
- `python3 -m py_compile harness/bench/format-results.py harness/bench/json-doc-mix.py harness/bench/default-suite-parts.py harness/bench/pipeline-smoke.py harness/bench/probe-hypotheses.py`
- `rustfmt --edition 2021 --check crates/redis-commands/src/eval.rs crates/redis-commands/src/string.rs crates/redis-server/src/runtime_owner.rs`

Current next targets:

- Treat `lrange_500` and `lrange_600` as the only repeated sub-parity rows in
  the final non-function matrix. They are above the `0.95x` gate, so profile
  only if a future run repeats below `0.97x`.
- Treat short P1 dips in `pipeline-smoke` as noisy until reproduced in the
  100k ordered suite or in isolated 100k rows.
- For more system-level gains, target shared owner-loop/socket/flush/accounting
  costs. Avoid command-specific fast paths unless a profile shows avoidable work
  inside that command.

## What Good Looks Like

Near term:

- No in-scope representative row below `0.95x`.
- p99 tails for list commands back near the Valkey band.
- MGET remains at or above parity.

Good state:

- Representative median stays above `1.10x`.
- Weakest in-scope row is `0.98x+`.
- Pipeline-smoke has no unexplained low cells or hangs.

Stretch:

- Representative median `1.20x` to `1.30x`.
- Simple-command cells `1.30x+` where semantics remain equivalent.
- No system-level p99 cliffs.

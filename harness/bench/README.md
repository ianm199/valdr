# Benchmark Backfill

## Direct Hypothesis Probes

Use `probe-hypotheses.py` when deciding what to optimize next and you do not
want to create a harness work packet yet. These probes are telemetry only; they
write raw artifacts under ignored `harness/bench/results/` and
`harness/bench/profiles/`.

```bash
# Decompose the Redis/Valkey default benchmark suite into bounded cells.
# `ordered` preserves server state between parts, which catches default-suite
# issues like LPOP after LPUSH/RPUSH.
python3 harness/bench/default-suite-parts.py list
python3 harness/bench/default-suite-parts.py run \
  --mode ordered \
  --target both \
  --tests lpush,rpush,lpop \
  --requests 10000 \
  --timeout-s 20

# `isolated` starts a fresh server for each part, which is better for checking
# whether a command is intrinsically slow or only slow after prior state.
python3 harness/bench/default-suite-parts.py run \
  --mode isolated \
  --target both \
  --tests lpop \
  --requests 10000 \
  --timeout-s 20

# Pipeline/payload/command shape. Answers whether the gap is fixed overhead,
# payload copy, or pipeline batching.
python3 harness/bench/probe-hypotheses.py protocol-shape --suite smoke

# Allocation attribution on macOS. Uses MallocStackLogging + malloc_history.
# Throughput is intentionally not trusted here because stack logging is slow.
python3 harness/bench/probe-hypotheses.py alloc-stacks \
  --requests 200000 \
  --commands get,set,incr,ping_mbulk

# CPU-time trace for one workload. This records with xctrace, exports the
# Time Profiler table as XML, and aggregates top frames into JSON. The child
# process uses a minimal environment so .trace metadata does not capture shell
# secrets.
python3 harness/bench/probe-hypotheses.py xctrace-time \
  --command get \
  --requests 1000000 \
  --time-limit-s 6
```

How to read the current probes:

- If `PING_MBULK` is near parity at `pipeline=1` but falls at `pipeline=100`,
  the next bottleneck is not DB storage. Look at pipelined parser/dispatch/write
  batching.
- If `d1024_over_d8` is not meaningfully above `1.0`, larger payloads are not
  amortizing the gap. Payload copy is probably not the first fix.
- `malloc_history` mostly reports live/high-water allocations. Treat it as
  stack attribution, not as a precise per-command allocation counter.
- `xctrace-time` is the highest-fidelity local CPU tool on macOS. The JSON
  includes a command-line `cli_profile` summary; the raw `.trace` bundle is a
  local artifact, not something to commit.

The benchmark runners rebuild `redis-server` by default. For historical data,
use `backfill.py` instead of checking out commits in the main worktree.

`backfill.py` creates one detached git worktree per commit, symlinks the pinned
upstream Valkey build into that worktree, rebuilds the Rust release binary from
that commit, runs the selected benchmark runner, and copies generated artifacts
back into this checkout under `harness/bench/results/` and
`harness/bench/profiles/`.

Examples:

```bash
# Correct the raw profile matrix for a few known commits.
python3 harness/bench/backfill.py --kind matrix 9b82591 ea9b3a8 8857714 752d649

# Backfill a commit range with a shorter matrix.
python3 harness/bench/backfill.py \
  --rev-list 1dd6563..HEAD \
  --kind matrix \
  --env VALKEY_MATRIX_CORE_P1_REQUESTS=10000 \
  --env VALKEY_MATRIX_CORE_P16_REQUESTS=50000 \
  --env VALKEY_MATRIX_CORE_P100_REQUESTS=50000 \
  --env VALKEY_MATRIX_RANGE_REQUESTS=25000

# Add calltree artifacts for selected commits.
python3 harness/bench/backfill.py --kind calltree --suite smoke 9b82591 ea9b3a8

# Rebuild the static chart after copying artifacts back.
python3 harness/bench/history.py
```

The backfill script writes raw artifacts only. It does not append harness ledger
rows. The dashboard renders these under its raw TSV series, while curated
runner history remains sourced from ledgered harness packets.

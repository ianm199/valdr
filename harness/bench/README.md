# Benchmark Backfill

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

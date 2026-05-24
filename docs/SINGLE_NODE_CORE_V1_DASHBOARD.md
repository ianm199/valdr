# single_node_core_v1 Dashboard

This dashboard turns the full upstream TCL-suite inventory into the product
denominator for milestone #1:

```text
single_node_core_v1 = one Redis/Valkey-compatible server process,
without persistence restart/rewrite guarantees, multi-node replication,
cluster, Sentinel, TLS/io-threads platform work, or RedisModule C ABI.
```

The goal is to stop mixing three different things:

- tests we are passing;
- tests we are actually failing;
- tests we have not yet run or cannot summarize.

## Regenerate

```bash
# First refresh the full-suite accounting snapshot.
python3 harness/oracle/tcl-suite-inventory.py

# Then project it onto the single-node core denominator.
python3 harness/oracle/single-node-core-dashboard.py
```

Outputs are generated under the ignored results directory:

```text
harness/oracle/results/single-node-core-v1/latest.json
harness/oracle/results/single-node-core-v1/latest.txt
harness/oracle/results/single-node-core-v1/latest.html
```

The `.txt` file is the ASCII control-plane view. The `.html` file is the
browser dashboard.

## Current Buckets

The dashboard classifies every `single_node_core_v1` file into:

- `proved` - latest file-level survey completed with no failures;
- `known-fail` - latest survey produced concrete failures;
- `abort/no-summary` - the file aborted or the harness could not parse a
  normal upstream `Test Summary`;
- `timeout` - the runner timed out;
- `not-swept` - the file is in scope for milestone #1 but has not been run by
  the generated survey yet.

That last category is deliberate. A not-swept test is not a failure, but it is
also not evidence. The dashboard should drive packet generation toward
`known-fail`, `abort/no-summary`, `timeout`, and the largest `not-swept` files.

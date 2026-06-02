# keyspace-cow-model

Standalone benchmark model for Valdr keyspace snapshot designs.

This is intentionally outside the main `redis-rs-port` Cargo workspace so it can
be reused without touching port crates. It compares steady-state read/write cost
and snapshot cost for four strategies:

- `deep`: `HashMap<Key, Payload>` with full snapshot clone.
- `arc`: `HashMap<Key, Arc<Payload>>` with full index clone and shared values.
- `seg`: segmented copy-on-write `HashMap` roots.
- `im`: persistent HAMT using the `im` crate.

Run:

```bash
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000
```

The output is TSV. `key_clone_mb` and `payload_clone_mb` are instrumented clone
bytes during the measured phase, not total allocator traffic.

Recorded runs live in `results/`, with a short readout in `RESULTS.md`.

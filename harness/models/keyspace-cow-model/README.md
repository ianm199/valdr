# keyspace-cow-model

Standalone benchmark model for Valdr keyspace snapshot designs.

This is intentionally outside the main `redis-rs-port` Cargo workspace so it can
be reused without touching port crates. It compares steady-state read/write
cost, snapshot cost, held-snapshot write cost, INCR-style mutation cost, and
process RSS samples for these strategies:

- `deep`: `HashMap<Key, Payload>` with full snapshot clone.
- `arc`: `HashMap<Key, Arc<Payload>>` with full index clone and shared values.
- `entry`: full index clone with metadata by value and `Arc<Payload>`.
- `seg_deep`: segmented copy-on-write with owned payloads and id-based routing.
- `seg_deep_hash`: segmented copy-on-write with owned payloads and key-byte
  hash routing, matching the current production `KeyspaceMap` shape.
- `seg`: segmented copy-on-write `HashMap` roots.
- `seg_hash`: segmented copy-on-write with key-byte hash routing, matching the
  current production `KeyspaceMap` shape more closely than `id % segments`.
- `seg_entry`: segmented copy-on-write with metadata by value and
  `Arc<Payload>`.
- `seg_entry_hash`: `seg_entry` with production-shaped key-byte hash routing.
- `im`: persistent HAMT using the `im` crate.

Run:

```bash
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024
```

The output is TSV. `key_clone_mb`, `entry_clone_mb`, and `payload_clone_mb` are
instrumented clone bytes during the measured phase, not total allocator traffic.
`rss_kb` is the process RSS sampled after each phase; `rss_delta_kb` is the
sampled movement during that phase and should be treated as directional
allocator telemetry.

The metadata/payload split variants are intentionally a model, not a production
API promise. They exist to answer whether held-snapshot value copying is large
enough to justify a `RedisObject` layout packet, and whether that packet needs
to be broad or limited to large payload classes.

Recorded runs live in `results/`, with a short readout in `RESULTS.md`.

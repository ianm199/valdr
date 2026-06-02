# aof-batch-model

Standalone model for AOF flush-boundary choices.

This is intentionally outside the main `redis-rs-port` workspace. It compares
two shapes that matter for `appendfsync always`:

- `per_command`: encode, write, flush, and `sync_data()` after every command.
- `batched`: encode commands into a staging buffer and write/sync once per
  batch, modeling Valkey's event-loop `server.aof_buf` flush boundary.

The model uses real file I/O and `sync_data()`, so absolute numbers are
host/filesystem specific. The useful signal is the ratio between batch sizes.

Run:

```bash
cargo run --release -- --commands 2000 --frame set --batches 1,4,16,64,256
```

The output is TSV. Recorded runs live in `results/`, with a short readout in
`RESULTS.md`.

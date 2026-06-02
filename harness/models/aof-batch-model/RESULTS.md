# Initial Results

Generated on 2026-06-02 on an Apple M3 Max. These are model results, not Valdr
claims. They are meant to decide whether an AOF staging buffer is worth
prototyping in the port.

## Commands

```bash
cargo run --release -- --commands 2000 --frame set --batches 1,4,16,64,256 > results/set-2k.tsv
cargo run --release -- --commands 2000 --frame incr --batches 1,4,16,64,256 > results/incr-2k.tsv
```

## Current Read

The model supports the upstream-shaped hypothesis: the dominant cost in
`appendfsync always` is sync frequency, not RESP frame size. Batching commands
into an event-loop flush boundary recovers throughput roughly in proportion to
the reduction in `sync_data()` calls.

Selected 2k-command numbers:

| Frame | Batch | Syncs | Throughput | Speedup vs per-command |
|---|---:|---:|---:|---:|
| SET | 1 | 2000 | 237 cmd/s | 1.0x |
| SET | 4 | 500 | 869 cmd/s | 3.7x |
| SET | 16 | 125 | 3,405 cmd/s | 14.4x |
| SET | 64 | 32 | 12,693 cmd/s | 53.6x |
| SET | 256 | 8 | 48,163 cmd/s | 203.3x |
| INCR | 1 | 2000 | 240 cmd/s | 1.0x |
| INCR | 4 | 500 | 958 cmd/s | 4.0x |
| INCR | 16 | 125 | 3,758 cmd/s | 15.7x |
| INCR | 64 | 32 | 14,768 cmd/s | 61.5x |
| INCR | 256 | 8 | 61,650 cmd/s | 256.8x |

## Implication

The production packet should not try to micro-optimize command encoding first.
The bigger lever is an AOF staging buffer with an explicit flush boundary:

- append propagated RESP frames into a per-connection or owner-loop staging
  buffer;
- flush before making successful replies observable to clients;
- for `appendfsync always`, sync once per flush boundary rather than once per
  propagated command;
- preserve `aof_last_write_status`, `fsynced_repl_offset`, and WAITAOF wakeup
  semantics on the flush result.

This does not make `appendfsync always` cheap. It makes pipelined and event-loop
batched workloads behave more like upstream Valkey, where one fsync can cover
multiple accepted commands.

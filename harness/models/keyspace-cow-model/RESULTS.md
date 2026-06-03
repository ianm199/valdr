# Keyspace COW Model Results

Generated on 2026-06-02 on an Apple M3 Max. These are standalone model
results, not Valdr throughput claims. They exist to keep the keyspace snapshot
decision reproducible and to separate first-principles structure from the port's
normal command-path benchmarks.

## Commands

Historical baseline runs:

```bash
cargo run --release -- --keys 10000 --value-bytes 64 --read-ops 100000 --write-ops 5000 --segments 256 > results/keys10k-v64.tsv
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v64.tsv
cargo run --release -- --keys 100000 --value-bytes 1024 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v1k.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 4096 > results/keys1m-v64.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 16384 --variants seg > results/keys1m-v64-seg16384.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 65536 --variants seg > results/keys1m-v64-seg65536.tsv
```

Current packet runs:

```bash
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v64-fnv-incr-rss.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 16384 --variants deep,seg_hash,im > results/keys1m-v64-fnv-incr-rss.tsv
```

Packet M metadata/payload split runs:

```bash
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v64-entry-split.tsv
cargo run --release -- --keys 100000 --value-bytes 1024 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v1k-entry-split.tsv
cargo run --release -- --keys 4096 --value-bytes 65536 --read-ops 100000 --write-ops 1000 --segments 512 > results/keys4k-v64k-entry-split.tsv
```

Model tests:

```bash
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
```

Result: 10/10 pass, including snapshot-isolation checks for deep, `Arc`, `im`,
segmented replace, hashed segmented INCR, entry metadata/payload split, and
production-shaped segmented deep/entry held-snapshot behavior.

## Selected Numbers

100k keys, 64-byte values, 1024 segments:

| Variant | Snapshot | GET ns/op | INCR ns/op | Held Replace ns/op | Held INCR ns/op | Snapshot Clone Bytes |
|---|---:|---:|---:|---:|---:|---:|
| deep | 9.420 ms | 57.8 | 104.1 | 206.2 | 97.5 | 1.53 MiB keys + 6.10 MiB payload |
| arc | 3.344 ms | 59.2 | 130.6 | 154.6 | 194.2 | 1.53 MiB keys |
| seg 1024 | 0.003 ms | 84.9 | 133.2 | 397.0 | 493.1 | none at snapshot |
| seg_hash 1024 | 0.004 ms | 97.5 | 176.2 | 464.1 | 444.8 | none at snapshot |
| im | ~0 ms | 112.4 | 258.5 | 945.8 | 912.7 | none at snapshot |

1M keys, 64-byte values, 16384 segments:

| Variant | Snapshot | GET ns/op | INCR ns/op | Held Replace ns/op | Held INCR ns/op | Held Clone Bytes |
|---|---:|---:|---:|---:|---:|---:|
| deep | 131.180 ms | 160.7 | 416.0 | 397.2 | 261.4 | none after snapshot |
| seg_hash 16384 | 0.112 ms | 300.6 | 674.1 | 4202.8 | 4331.8 | 13.98 MiB keys + 0.61 MiB payload |
| im | ~0 ms | 271.9 | 513.6 | 2104.6 | 2050.5 | 1.78 MiB keys + 0.61 MiB payload |

RSS columns are directional only. The model runs variants in one process, so
absolute `rss_kb` accumulates allocator state across variants. The useful read
is the shape: deep snapshot allocates a large copy immediately; segmented COW
keeps snapshot capture flat and moves allocation into the first writes touching
shared segments; HAMT keeps held-write clone bytes lower but pays in iteration
and live-operation overhead.

## Current Read

- Generic HAMT gives the cleanest persistent-map contract but is not a good
  default live keyspace choice yet. At 100k keys it is slower than hashed
  segmented COW for GET, INCR, held writes, and snapshot iteration; at 1M it
  lowers held-write clone bytes but iteration remains much slower.
- `Arc<Payload>` avoids payload cloning at snapshot time, but a full index clone
  still scales with key count.
- Segmented COW is the best first production step because it preserves
  hash-table-like lookup and makes snapshot capture proportional to segment
  roots instead of key count.
- The cost moves into the snapshot window: the first live write to each shared
  segment clones that segment. Segment count is therefore a real tuning knob,
  not cosmetic configuration.
- Whole-payload COW remains a risk for large mutable values. Splitting metadata
  from payload and adding persistent inner encodings are separate future
  packets; this model deliberately does not pretend segmented index sharing
  solves that class.

## Packet M Metadata/Payload Split Read

The new `seg_deep_hash` variant models the current production shape: segmented
COW index with owned payloads. The new `seg_entry_hash` variant models metadata
by value with shared payload handles.

100k keys, 64-byte values, 1024 segments:

| Variant | Live Replace | Held Metadata | Held Mutate | Held INCR | Held Clone Bytes |
|---|---:|---:|---:|---:|---:|
| seg_deep_hash | 174 ns/op | n/a | 751 ns/op | 701 ns/op | 1.53 MiB keys + 6.10 MiB payload |
| seg_entry_hash | 1131 ns/op | 277 ns/op | 426 ns/op | 467 ns/op | 1.53 MiB keys + 2.29 MiB entries + 0.58 MiB touched payload |

100k keys, 1 KiB values:

| Variant | Live Replace | Held Metadata | Held Mutate | Held INCR | Held Payload Clone |
|---|---:|---:|---:|---:|---:|
| seg_deep_hash | 485 ns/op | n/a | 2363 ns/op | 2390 ns/op | 97.62 MiB |
| seg_entry_hash | 555 ns/op | 369 ns/op | 898 ns/op | 1107 ns/op | 9.31 MiB |

4k keys, 64 KiB values, 512 segments:

| Variant | Live Replace | Held Metadata | Held Mutate | Held INCR | Held Payload Clone |
|---|---:|---:|---:|---:|---:|
| seg_deep_hash | 6028 ns/op | n/a | 28,853 ns/op | 35,438 ns/op | 255.75 MiB |
| seg_entry_hash | 1743 ns/op | 156 ns/op | 4997 ns/op | 4907 ns/op | 55.94 MiB |

Decision: payload handles clearly help medium/large held-snapshot mutation
windows, but the 64-byte run shows enough small-value live-operation risk that
the production packet should not broadly wrap every object payload yet. Keep the
model, keep production counters, and make the next payload packet narrower:
large string payload handles, collection-specific payload handles, or a
feature-flagged production layout probe before migration.

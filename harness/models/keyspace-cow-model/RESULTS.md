# Initial Results

Generated on 2026-06-02 on an Apple M3 Max. These are model results, not Valdr
claims. They are meant to decide what is worth prototyping in the port.

## Commands

```bash
cargo run --release -- --keys 10000 --value-bytes 64 --read-ops 100000 --write-ops 5000 --segments 256 > results/keys10k-v64.tsv
cargo run --release -- --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v64.tsv
cargo run --release -- --keys 100000 --value-bytes 1024 --read-ops 200000 --write-ops 10000 --segments 1024 > results/keys100k-v1k.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 4096 > results/keys1m-v64.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 16384 --variants seg > results/keys1m-v64-seg16384.tsv
cargo run --release -- --keys 1000000 --value-bytes 64 --read-ops 300000 --write-ops 10000 --segments 65536 --variants seg > results/keys1m-v64-seg65536.tsv
```

## Key Observations

- `im::HashMap` HAMT gives O(1)-ish snapshot clone, but GET is substantially
  slower than `std::HashMap` in this model.
- `seg` snapshot clone is also effectively flat, while keeping GET closer to
  `std::HashMap` than HAMT does.
- `seg` pays during a held snapshot by cloning each touched segment once. More
  segments reduce held-write clone bytes but increase read/root overhead.
- `Arc<Payload>` avoids payload cloning at snapshot time, but a full index clone
  still scales with key count.
- Mutating large shared values during a held snapshot copies the full payload:
  the 100k-key, 1 KiB-value run cloned about 9.3 MiB of payloads for 10k random
  byte mutations in `arc`, `seg`, and `im`.

## Selected Numbers

100k keys, 64-byte values:

| Variant | Snapshot | GET ns/op | Held Replace ns/op | Snapshot Clone Bytes |
|---|---:|---:|---:|---:|
| deep | 8.56 ms | 79.5 | 221.7 | 1.53 MiB keys + 6.10 MiB payload |
| arc | 3.30 ms | 156.7 | 149.9 | 1.53 MiB keys |
| seg 1024 | 0.003 ms | 106.5 | 490.1 | none at snapshot |
| im | ~0 ms | 199.8 | 857.1 | none at snapshot |

1M keys, 64-byte values:

| Variant | Snapshot | GET ns/op | Held Replace ns/op | Held Replace Clone Bytes |
|---|---:|---:|---:|---:|
| deep | 109.86 ms | 148.4 | 341.9 | none after snapshot |
| arc | 73.07 ms | 178.5 | 359.0 | none after snapshot |
| seg 4096 | 0.022 ms | 210.3 | 4477.7 | 13.97 MiB keys |
| seg 16384 | 0.061 ms | 239.5 | 3173.6 | 6.96 MiB keys |
| seg 65536 | 0.209 ms | 256.9 | 1448.4 | 2.16 MiB keys |
| im | ~0 ms | 308.1 | 2229.4 | 1.79 MiB keys |

## Current Read

The model argues against making a generic HAMT the default live keyspace. The
read-path hit is too large relative to Valdr's current GET/SET margin.

Segmented COW looks more plausible as a first-principles direction, but the
current toy shape still has a visible GET tax and a tunable write-window cost.
It deserves a more faithful prototype before any Valdr integration:

- use power-of-two segment routing from the existing key hash, not `id % n`;
- tune segment count by target average entries per segment;
- test smaller segment maps or open-addressed segment storage;
- split metadata from payload before introducing `Arc` values in Valdr.

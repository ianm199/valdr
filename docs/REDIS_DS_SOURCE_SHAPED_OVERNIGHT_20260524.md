# Redis-DS Source-Shaped Overnight — 2026-05-24

## Intent

This run exercises the "generate source-shaped drafts, then do the real
integration work" flow on Redis/Valkey. The target is `redis-ds`, because its
current canonical files are mostly skeletons while upstream Valkey has compact,
bounded C implementations for the same concepts.

The run must not overwrite live command behavior for speed. Wave 1 is leaf data
structures plus tests. Object-model wiring comes later.

## Cheap-Bulk Drafts

Generated with:

```bash
../port-harness/templates/c-to-rust/cheap-bulk-translate/translate.sh \
  harness/cheap-bulk/redis-ds-wave-20260524.spec
```

Result:

| Draft | LOC | Read |
|---|---:|---|
| `intset.rs` | 655 | Best candidate. Byte-buffer shape, focused tests, no `todo!`. |
| `ziplist.rs` | 1049 | Useful read-only decoder draft; remove UTF-8 parsing in integration. |
| `adlist.rs` | 692 | Useful API map; likely simplify away `Rc<RefCell>` if possible. |
| `quicklist.rs` | 1142 | Scaffold only until canonical `ListPack` exists. |
| `rax.rs` | 437 | Behavior map only; many `todo!` markers. |

Total cheap-bulk cost: `$0.0926`.

## Packet Order

1. `ds-listpack-canonicalize-v1`
   - Use the older full `source-drafts/redis-ds/listpack_listpack.rs` plus
     upstream `listpack.c`.
   - Goal: real canonical `ListPack` with unit tests.
   - Closeout: canonical `ListPack` now owns a safe Valkey-compatible byte blob
     with header/count/EOF layout, compact integer and string entry encodings,
     backlen navigation, append/prepend/insert/replace/delete, seek, compare,
     find, merge, raw-byte validation, and focused unit tests. Object-model
     wiring remains a later packet.

2. `ds-intset-source-port-v1`
   - Integrate the generated `intset.rs` draft into the canonical owner.
   - Goal: byte-faithful `IntSet` plus tests.
   - Closeout: canonical `IntSet` now owns a safe Valkey-compatible byte blob
     implementation with add/remove/find/random/min/max/get/len/blob_len,
     validation, raw-byte construction, and focused unit tests. Object-model
     wiring remains a later packet.

3. `ds-ziplist-readonly-v1`
   - Integrate the generated `ziplist.rs` draft as a legacy read-only decoder.
   - Goal: iterator / get / compare / validate tests without UTF-8 conversion.
   - Closeout: canonical `Ziplist` now owns a safe read-only Valkey-compatible
     byte blob decoder with header/blob length access, positive and negative
     offset indexing, forward/backward iteration, byte and integer payload
     reads, compare/find, safe-to-add, raw-byte deep validation, and focused
     unit tests. Mutating ziplist writes and object-model/RDB wiring remain
     later packets.

4. `ds-adlist-source-port-v1`
   - Integrate useful adlist behavior into canonical `LinkedList`.
   - Goal: append/prepend/delete/index/search/iteration tests.
   - Closeout: canonical `LinkedList` now owns a safe `VecDeque`-backed
     implementation with head/tail insertion, pop/delete, index, search,
     forward/backward iteration, rotation, clone-based duplication, join, and
     focused unit tests. Stable C node pointers and dup/free/match callbacks
     remain intentionally reshaped into Rust-owned storage plus Clone/Drop/
     PartialEq/search closures.

5. `redis-ds-unit-after-wave1-v1`
   - Runner: `cargo test -p redis-ds`.

6. `ds-quicklist-mvp-v1`
   - Only after listpack. Use the generated draft as API checklist, but keep
     MVP bounded: push/pop/index/iteration/count, no compression/bookmarks.
   - Closeout: canonical `QuickList` now owns a safe `VecDeque` of
     ListPack-backed or plain nodes with Valkey fill-limit accounting,
     head/tail push and pop, positive/negative index lookup, owned forward and
     reverse iteration, count/node-count helpers, large-element plain-node
     handling, and focused unit tests. LZF compression, bookmarks, iterator
     mutation, node split/merge insertion paths, and object-model wiring remain
     later packets.

7. `ds-rax-behavior-map-v1`
   - Use the generated draft and upstream source to land a safe behavior-first
     ordered byte-key map API if feasible. If compressed-node parity is too
     large, land a clean architecture note and tests for the intended surface.

8. `redis-ds-unit-after-wave2-v1`
   - Runner: `cargo test -p redis-ds`.

## Non-Goals

- No object-model wiring in this overnight unless a packet explicitly creates a
  follow-up and proves `redis-ds` first.
- No command handler rewrites.
- No `unsafe`.
- No external dependencies.
- No UTF-8 conversion for Redis payload bytes.

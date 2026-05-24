porting_md=../PORTING_REDIS_DS.md
output_dir=../../source-drafts/generated/redis-ds-wave-20260524
model=deepseek/deepseek-v4-flash

context=../../PORTING.md
context=../type-vocabulary.tsv
context=../source-drafts.tsv
context=../../crates/redis-ds/src/lib.rs
context=../../crates/redis-ds/src/zipmap.rs
context=../../crates/redis-ds/src/pqsort.rs
context=../../docs/CHEAP_BULK_TRANSLATION_REDIS_TRIAGE_20260524.md

source=../../reference/valkey/src/listpack.c
source=../../reference/valkey/src/listpack.h
source=../../reference/valkey/src/intset.c
source=../../reference/valkey/src/intset.h
source=../../reference/valkey/src/ziplist.c
source=../../reference/valkey/src/ziplist.h
source=../../reference/valkey/src/quicklist.c
source=../../reference/valkey/src/quicklist.h
source=../../reference/valkey/src/rax.c
source=../../reference/valkey/src/rax.h
source=../../reference/valkey/src/adlist.c
source=../../reference/valkey/src/adlist.h

target=crates/redis-ds/src/intset.rs|source-shaped IntSet implementation with sorted integer storage, add/remove/find/random/blob length/integrity helpers, and focused tests
target=crates/redis-ds/src/ziplist.rs|read-mostly Ziplist decoder, iterator, integrity validator, and focused tests for legacy RDB compatibility
target=crates/redis-ds/src/quicklist.rs|QuickList MVP built from safe Rust nodes and ListPack/plain payload helpers; no LZF compression unless cheap and isolated
target=crates/redis-ds/src/rax.rs|behavior-faithful RadixTree API using safe owned storage; lexicographic insert/find/delete/prefix/range iteration, not packed C node layout
target=crates/redis-ds/src/adlist.rs|safe LinkedList/VecDeque-backed adlist equivalent with append/prepend/delete/iteration helpers and focused tests

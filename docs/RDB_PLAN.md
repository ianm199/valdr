# RDB Plan — Def 3.3 implementation map

## TL;DR

Def 3.3 requires 8 implementation rounds (18–25) plus 2 oracle rounds (26–27), 10 total. The scope is RDB v80 (VALKEY magic, Valkey 9.0) save+load for the six data types we carry — string, list, hash, set, zset, stream — with a bidirectional wire-diff oracle that catches mismatches in both directions. The fundamental oracle decision is: our Inline encodings will emit the "big" (hashtable/zset_2) RDB type byte in Phase 1 because building real listpack/intset binary payloads from a `HashMap` is Phase 2 work; Phase 1 fidelity is sufficient to pass C-load and our-load under the wire-diff oracle.

---

## RDB v80 binary format

### File layout

The `rdbSaveRio` function (rdb.c:1477) writes the following, in order:

1. **Magic header** — 9 bytes. Either `REDIS0NNN` (legacy, RDB_VERSION <= 11) or `VALKEYNNN` (Valkey 9+, RDB_VERSION 80). Since we target RDB_VERSION 80 (rdb.h:52), the prefix is `VALKEY080`. The magic determines which version range is accepted on load (rdb.c:3154-3167).
2. **AUX fields** — zero or more `RDB_OPCODE_AUX (0xFA)` records emitted by `rdbSaveInfoAuxFields` (rdb.c:1261). Each AUX record is: opcode byte, key string, value string. Mandatory fields Valkey writes: `valkey-ver`, `redis-bits`, `ctime`, `used-mem`, `aof-base`. On load (rdb.c:3269–3374) unknown AUX keys are silently skipped.
3. **Per-DB sections** — for each non-empty database, `rdbSaveDb` (rdb.c:1386) writes: `RDB_OPCODE_SELECTDB (0xFE)` + db-id length, then `RDB_OPCODE_RESIZEDB (0xFB)` + db-size length + expires-size length, then one key-value record per key.
4. **Key-value record** — `rdbSaveKeyValuePair` (rdb.c:1190): optional `RDB_OPCODE_EXPIRETIME_MS (0xFC)` + 8-byte little-endian ms timestamp, optional `RDB_OPCODE_IDLE (0xF8)` + varint, optional `RDB_OPCODE_FREQ (0xF9)` + 1 byte, then the RDB type byte, the key as a raw string, the value payload.
5. **EOF** — `RDB_OPCODE_EOF (0xFF)` byte (rdb.c:1507).
6. **CRC64** — 8 bytes, little-endian, of the entire preceding stream (rdb.c:1511-1513). If the server had `rdb_checksum` disabled, written as zero and skipped on load.

### Type byte table

| RDB_TYPE constant | Decimal | What it serializes | Our ObjectKind + encoding |
|---|---|---|---|
| RDB_TYPE_STRING = 0 | 0 | String (int/embstr/raw/LZF) | `RedisObject::String` (Int/Embstr/Raw) |
| RDB_TYPE_LIST = 1 | 1 | Legacy linked list (obsolete; load-compat only) | never emitted by us |
| RDB_TYPE_SET = 2 | 2 | Set as flat string array | `Set::Inline` (large) or `Set::HashTable` |
| RDB_TYPE_ZSET = 3 | 3 | ZSet with text-encoded doubles (obsolete on save; load-compat) | never emitted by us |
| RDB_TYPE_HASH = 4 | 4 | Hash as flat field/value string pairs | `Hash::Inline` (large) |
| RDB_TYPE_ZSET_2 = 5 | 5 | ZSet with IEEE754 little-endian doubles | `ZSet::Inline` (all sizes in Phase 1) |
| RDB_TYPE_SET_INTSET = 11 | 11 | IntSet binary blob | `Set::IntSet` |
| RDB_TYPE_LIST_QUICKLIST = 14 | 14 | Legacy quicklist; load-compat only | never emitted by us |
| RDB_TYPE_STREAM_LISTPACKS = 15 | 15 | Stream v1; load-compat only | never emitted by us |
| RDB_TYPE_HASH_LISTPACK = 16 | 16 | Hash as listpack binary blob | `Hash::Inline` (small, Phase 2) |
| RDB_TYPE_ZSET_LISTPACK = 17 | 17 | ZSet as listpack binary blob | `ZSet::Inline` (small, Phase 2) |
| RDB_TYPE_LIST_QUICKLIST_2 = 18 | 18 | Quicklist v2 with container tag per node | `List::Inline` (all sizes) |
| RDB_TYPE_STREAM_LISTPACKS_2 = 19 | 19 | Stream v2; load-compat only | never emitted by us |
| RDB_TYPE_SET_LISTPACK = 20 | 20 | Set as listpack blob | `Set::Inline` (small, Phase 2) |
| RDB_TYPE_STREAM_LISTPACKS_3 = 21 | 21 | Stream v3 with consumer active_time | `Stream::Inline` + consumer groups |
| RDB_TYPE_HASH_2 = 22 | 22 | Hash with per-field expiry (RDB 80, Valkey 9.0) | not implemented yet; skip |

Types 6 (`MODULE_PRE_GA`), 7 (`MODULE_2`), 9 (`HASH_ZIPMAP`), 10 (`LIST_ZIPLIST`), 12 (`ZSET_ZIPLIST`), 13 (`HASH_ZIPLIST`) are either module-specific or legacy ziplist formats. On load, the C server converts ziplist formats to listpack automatically (rdb.c:2438-2583). We must handle reading them but will never write them.

### Length encoding (`rdbSaveLen`)

`rdbSaveLen` (rdb.c:232) and `rdbLoadLenByRef` (rdb.c:275) implement a variable-width unsigned integer encoding. The top 2 bits of the first byte determine the format:

- `00xxxxxx` — 6-bit length (0–63). Single byte.
- `01xxxxxx xxxxxxxx` — 14-bit length (64–16383). Two bytes, big-endian within the 14-bit field.
- `10000000` + 4 bytes — 32-bit length in network byte order (big-endian).
- `10000001` + 8 bytes — 64-bit length in network byte order (big-endian).
- `11xxxxxx` — special encoding marker (`RDB_ENCVAL = 3`). The low 6 bits are an `RDB_ENC_*` type, not a length. Used for integer-encoded strings and LZF-compressed strings.

The special `RDB_ENC_*` values that follow an `RDB_ENCVAL` prefix byte:
- `RDB_ENC_INT8 = 0` — the next byte is a signed 8-bit integer.
- `RDB_ENC_INT16 = 1` — the next 2 bytes are a signed 16-bit little-endian integer.
- `RDB_ENC_INT32 = 2` — the next 4 bytes are a signed 32-bit little-endian integer.
- `RDB_ENC_LZF = 3` — the next varint is compressed length, then another varint is original length, then compressed bytes.

A string is saved by `rdbSaveRawString` (rdb.c:500): if the byte string fits as an 8/16/32-bit integer, emit the `RDB_ENCVAL` + int encoding (2–5 bytes). Otherwise if `rdb_compression` is on and length > 20 bytes, attempt LZF; if compressed bytes are fewer than original, emit the LZF form. Otherwise emit `rdbSaveLen(len)` + raw bytes.

### Opcodes

| Opcode | Value | Where it appears | State impact |
|---|---|---|---|
| `RDB_OPCODE_EOF` | 0xFF | After last DB section | Terminates the load loop |
| `RDB_OPCODE_SELECTDB` | 0xFE | Before each non-empty DB | Sets current `dbid` for subsequent keys |
| `RDB_OPCODE_EXPIRETIME` | 0xFD | Before a key's type byte | Loads expire in seconds, multiplied by 1000 (rdb.c:3206) |
| `RDB_OPCODE_EXPIRETIME_MS` | 0xFC | Before a key's type byte | Loads expire in milliseconds directly |
| `RDB_OPCODE_RESIZEDB` | 0xFB | After SELECTDB | Provides dict/expires size hints for pre-allocation |
| `RDB_OPCODE_AUX` | 0xFA | After magic header, before first DB | Two strings (key, value); unknown keys skipped |
| `RDB_OPCODE_FREQ` | 0xF9 | Before a key's type byte | 1-byte LFU frequency; we store but do not use in Phase 1 |
| `RDB_OPCODE_IDLE` | 0xF8 | Before a key's type byte | Varint LRU idle seconds; we store in `lru_clock` |
| `RDB_OPCODE_MODULE_AUX` | 0xF7 | Before/after DB sections | Module data; we skip (no module support) |
| `RDB_OPCODE_FUNCTION2` | 0xF5 | Before DB sections | Lua function library; we skip entirely |
| `RDB_OPCODE_SLOT_IMPORT` | 0xF3 | Before DB sections | Cluster slot import state; we skip (no cluster) |
| `RDB_OPCODE_SLOT_INFO` | 0xF4 | Inside DB, between key-value pairs | Cluster slot size hints; we skip (no cluster) |

EXPIRETIME (old seconds form) must still be handled on load because old RDB files may contain it; we do not emit it. EXPIRETIME_MS is what we emit.

---

## Per-encoding serialize requirements

| ObjectKind / Encoding | RDB type byte | Wire shape (save) | Load notes |
|---|---|---|---|
| `String::Int(i64)` | `RDB_TYPE_STRING = 0` | `rdbSaveLongLongAsStringObject`: if fits int8/16/32 → `RDB_ENCVAL` + int bytes; else len-prefix decimal text | `rdbLoadEncodedStringObject` + `tryObjectEncoding` converts back to Int if it fits |
| `String::Embstr(bytes)` | `RDB_TYPE_STRING = 0` | `rdbSaveRawString`: try int enc first, then LZF if >20 bytes, else len + raw bytes | Same loader; result is Embstr if ≤44 bytes, Raw otherwise |
| `String::Raw(bytes)` | `RDB_TYPE_STRING = 0` | Same as Embstr path — LZF compression applies if `rdb_compression` on and len > 20 | Same loader |
| `List::Inline(VecDeque)` | `RDB_TYPE_LIST_QUICKLIST_2 = 18` | Save as single-node quicklist: `rdbSaveLen(1)` (one node), `rdbSaveLen(QUICKLIST_NODE_CONTAINER_PACKED)`, then the listpack blob as a raw string. **But we don't have a real listpack serializer.** Recommendation: emit as multiple plain nodes (container = QUICKLIST_NODE_CONTAINER_PLAIN) — one raw string per element. C loads PLAIN nodes fine (rdb.c:2326). | On load, read node count, then per-node container tag + raw blob. PLAIN nodes are appended as oversized quicklist entries. |
| `Hash::Inline(HashMap)` | `RDB_TYPE_HASH = 4` | `rdbSaveLen(n_fields)` then N × (`rdbSaveRawString(field)` + `rdbSaveRawString(value)`). HashMap iteration order is unspecified — this is fine, hash is unordered. | Load: read count, then N field+value pairs into our HashMap. |
| `Set::Inline(HashSet)` | `RDB_TYPE_SET = 2` | `rdbSaveLen(n_members)` then N × `rdbSaveRawString(member)`. | Load: read count, N raw strings, insert into HashSet. |
| `Set::IntSet(Vec<i64>)` | `RDB_TYPE_SET_INTSET = 11` | `rdbSaveRawString(intset_blob, blob_len)` — the intset is an on-disk binary struct (little-endian int array with a 4-byte header). We must serialize this struct ourselves. | Load: raw blob → validate → set our IntSet vec by reading the blob. |
| `ZSet::Inline(InlineZSet)` | `RDB_TYPE_ZSET_2 = 5` | `rdbSaveLen(n_members)` then N × (`rdbSaveRawString(member)` + `rdbSaveBinaryDoubleValue(score)`). C saves from tail to head (rdb.c:970) for O(1) skiplist inserts on load; we iterate in any order since our loader uses a HashMap. | Load: read count, N member+score pairs. Binary double is 8 bytes little-endian IEEE754. |
| `Stream::Inline` + consumer groups | `RDB_TYPE_STREAM_LISTPACKS_3 = 21` | Most complex. See Stream section below. | See Stream section below. |

**List encoding decision (concrete recommendation):**

Use `QUICKLIST_NODE_CONTAINER_PLAIN = 2` for all list nodes in Phase 1. The wire shape per element is: `rdbSaveLen(2)` (container = PLAIN), then `rdbSaveRawString(element_bytes, len)`. C correctly loads PLAIN nodes (rdb.c:2326-2328). The downside is the saved RDB looks structurally different from a Valkey-written list (which uses PACKED/listpack nodes), but C loads it without error and the bidirectional oracle will confirm it.

**IntSet binary layout (needed for `Set::IntSet`):**

The intset blob is: `[encoding:u32-le][length:u32-le][contents: N × encoding_size bytes, little-endian, sorted]`. Encoding values: 2 = INT16, 4 = INT32, 8 = INT64. We pick the smallest encoding that fits all values. This is ~40 lines of Rust.

### Stream encoding detail (RDB_TYPE_STREAM_LISTPACKS_3)

The stream serializer in `rdbSaveObject` (rdb.c:1033–1142) and loader (rdb.c:2590–2885) use the following layered structure. Our `InlineStream` stores entries as `Vec<StreamEntry>` and consumer groups as a `HashMap`; we must translate to/from the wire shape.

**Save wire shape (rdb.c:1033–1142):**
1. `rdbSaveLen(rax_node_count)` + per-node `rdbSaveRawString(nodekey=streamID_16_bytes)` + `rdbSaveRawString(listpack_blob)`. Building real listpack blobs (master entry + delta-encoded entries per rax node) is the hard part — requires the format documented in stream.h and t_stream.c.
2. Metadata: `length`, `last_id.ms/seq`, `first_id.ms/seq`, `max_deleted_entry_id.ms/seq`, `entries_added` — all as varints.
3. Consumer groups: `num_cgroups`, then per group: name, last_id, entries_read, global PEL (count + N × rawid[16] + delivery_time + delivery_count), consumer list (count + N × name + seen_time + active_time + local PEL IDs, which resolve against the global PEL).

The two-pass PEL strategy (global PEL loaded first, consumer PEL resolves shared NACK pointers) is the subtle correctness constraint.

---

## Bidirectional oracle design

The oracle tests two independent directions. Both must pass before Def 3.3 is complete.

**Direction 1: we-save → C-loads**

1. Run our Rust server, populate keys of every type via RESP commands.
2. Issue SAVE (synchronous, no fork needed in Phase 1).
3. Start a real Valkey 9.x process pointing at our dump.rdb file.
4. Issue a debug digest or iterate all keys via SCAN + GET/HGETALL/etc from the C server.
5. Wire-diff: compare the keyspace our server reported (via SCAN) against what C loaded.

What can be checked byte-for-byte: integer-encoded string values, exact ZSet scores, sorted set member order via ZRANGE. What must be normalized: HashMap/HashSet iteration order is unspecified; normalize by sorting keys/members before comparison.

**Direction 2: C-saves → we-load**

1. Start a real Valkey 9.x process, populate keys, issue SAVE.
2. Point our Rust server at that dump.rdb on startup.
3. Compare keyspace via SCAN + GET family.

**New harness script: `harness/oracle/rdb-diff.py`**

This script takes two (host, port) endpoints plus a key list. For each key it issues TYPE, then the appropriate dump command (GET/HGETALL/LRANGE/SMEMBERS/ZRANGE WITHSCORES/XRANGE), normalizes the response (sort sets, sort hash fields), and diffs. A non-zero exit code causes the round to fail. It should accept a `--exclude-patterns` flag for keys we know have ordering nondeterminism.

**Corpus files:** store a set of pre-populated RDB files in `harness/corpus/rdb/` — one per type (string_only.rdb, hash_only.rdb, etc.) and one mixed.rdb. Generate these by running Valkey 9.x locally and issuing SAVE. Check them into the repo. The C-saves → we-load direction uses these as fixed inputs.

---

## Round-by-round implementation plan

| Round | Focus | Model | Cost | Files | Stop condition |
|---|---|---|---|---|---|
| 18 | `crates/redis-persist` scaffold: CRC64 port from `reference/valkey/src/crc64.c` (tabular, ~180 LoC), `rio` buffered I/O wrapper (File + Vec<u8> backends), `rdbSaveLen`/`rdbLoadLen` varint codec | Sonnet | $6 | new `crates/redis-persist/src/{lib,crc64,rio,varint}.rs` | Unit test: encode len 0, 63, 64, 16383, 16384, 2^32, 2^63. CRC64 agrees with a reference implementation for `b"hello world"`. |
| 19 | RDB file framing: magic header write/parse, AUX fields (write 5 mandatory, skip unknown on read), SELECTDB, RESIZEDB, EXPIRETIME_MS, IDLE, FREQ, EOF; all ops coded but value payloads are stubs | Sonnet | $7 | `crates/redis-persist/src/{framing,aux}.rs` | Save an empty DB → `redis-check-rdb dump.rdb` passes. |
| 20 | String type save/load: `RDB_TYPE_STRING` with all three paths — `RDB_ENCVAL` int8/16/32, LZF (skip compression; always emit raw for now), raw len+bytes | Sonnet | $6 | `crates/redis-persist/src/string.rs` | Save 200 string keys (mix of int, short, long) → start Valkey 9.x pointing at our dump.rdb → `DEBUG DIGEST` matches ours. |
| 21 | Hash and Set save/load: `RDB_TYPE_HASH` (flat pairs), `RDB_TYPE_SET` (flat members), `RDB_TYPE_SET_INTSET` (binary blob builder) | Sonnet | $8 | `crates/redis-persist/src/{hash,set}.rs` | 50 hashes + 50 sets + 10 intset-sets round-trip through both oracle directions. |
| 22 | List and ZSet save/load: `RDB_TYPE_LIST_QUICKLIST_2` (PLAIN nodes), `RDB_TYPE_ZSET_2` (binary doubles, save from tail-to-head for load efficiency) | Sonnet | $8 | `crates/redis-persist/src/{list,zset}.rs` | 50 lists + 50 zsets round-trip both directions. Check ZSet score exact bit equality (NaN/±Inf sentinel handling). |
| 23 | Expire-time load path wired into `RedisDb::set_key_with_expire`; IDLE/FREQ stored as fields on `RedisObject`; key-expiry skipping on load (expired keys at load time are silently dropped per rdb.c:1185) | Sonnet | $6 | `crates/redis-persist/src/loader.rs`, `crates/redis-core/src/object.rs` | Load an RDB that has 10 keys with TTLs in the past → none appear in keyspace. Load 10 keys with future TTLs → all appear with correct TTL reported via TTL command. |
| 24 | Stream save/load: `RDB_TYPE_STREAM_LISTPACKS_3`. Must build real listpack binary blobs from InlineStream entries. Each listpack encodes a master entry + delta-encoded entries. Consumer groups, global PEL, and consumer-local PELs. | Opus | $22 | `crates/redis-persist/src/stream.rs`, `crates/redis-ds/src/listpack.rs` (new) | 5 streams with 20 entries each, 2 consumer groups each with PEL entries, round-trip both oracle directions. |
| 25 | SAVE command (synchronous) + server startup load path. SAVE writes to `temp-<pid>.rdb`, renames atomically. Startup: if `dbfilename` exists, load before bind. BGSAVE stubbed as sync SAVE in Phase 1. | Sonnet | $7 | `crates/redis-commands/src/server_commands.rs`, `crates/redis-server/src/main.rs` | `redis-cli SAVE` returns `+OK`, process restarts pointing at file, all keys survive. TCL test `unit/dump` passes for the non-module, non-cluster cases. |
| 26 | Bidirectional oracle harness: `harness/oracle/rdb-diff.py`, corpus RDB files in `harness/corpus/rdb/`, CI integration. | Sonnet | $5 | `harness/oracle/rdb-diff.py`, `harness/corpus/rdb/*.rdb` | Script exits 0 on reference corpus. Detects a deliberately injected 1-byte mutation as a diff. |
| 27 | LZF compression for strings: implement `rdbSaveLzfStringObject` (skip compress if len ≤ 20 or compressed bytes >= original; else emit `RDB_ENCVAL + RDB_ENC_LZF` blob) + decompression on load. Use the `lzf` crate (pure Rust port). | Sonnet | $5 | `crates/redis-persist/src/lzf.rs` | A 1 KB repetitive string round-trips through LZF save/load. Files we save match what `redis-check-rdb --fix` would accept. |

**Total: 10 rounds, 18–27. Estimated total cost: ~$74.**

---

## Risks and open decisions

**Risk 1 — Stream listpack binary format (highest).** The stream radix tree stores listpack blobs where entries are delta-encoded relative to a "master entry." This is the most complex serialization in the codebase. The master entry format (count, deleted, num-fields, field-names, then delta entries per entry) is documented only in `stream.h` comments and `t_stream.c`. A single off-by-one in the listpack encoding produces a corrupt stream that C validates against. Mitigation: Round 24 is Opus with the full stream.h and t_stream.c as context. Gate behind a `--feature stream-rdb` flag so a failed Round 24 doesn't block the rest.

**Risk 2 — CRC64 byte-exactness.** Valkey's CRC64 uses a specific polynomial and table (`reference/valkey/src/crc64.c`). If we port the table wrong (e.g., endian-flip one entry), every RDB we save will fail Valkey's checksum validation. The C server sets `cksum = 0` and skips validation if the stored checksum is zero (rdb.c:1483, 1511). Mitigation: Round 18 includes a cross-check: save an RDB, run `redis-check-rdb`, confirm checksum passes. Use the zero-cksum escape hatch during development if needed.

**Risk 3 — EXPIRETIME two-opcode routing.** The load loop handles both `RDB_OPCODE_EXPIRETIME` (seconds, old form) and `RDB_OPCODE_EXPIRETIME_MS` (milliseconds, new form). The seconds form multiplies by 1000 (rdb.c:3207). If our loader routes to the wrong branch, expire times are off by 1000×, which is a silent correctness bug — the key looks live for 1000× longer or is immediately expired. Mitigation: Round 23 has explicit tests for both opcode types by loading corpus RDB files that contain both.

**Risk 4 — IntSet binary layout.** The intset struct layout (`reference/valkey/src/intset.h`) is: `[encoding:u32-le][length:u32-le][contents]`. If we pick the wrong encoding (e.g., always INT64 when INT16 fits), the C server will still load it correctly, but our file size is larger than expected and the bidirectional oracle will flag a structural mismatch if we compare raw bytes. Not a correctness risk, but a fidelity gap. Mitigation: pick encoding based on actual min/max value, include unit test that verifies produced blob matches a reference blob generated by C valkey.

**Risk 5 — RDB_TYPE_HASH_2 (field-level expiry, RDB 80).** Our `Hash::Inline` does not model per-field expiry. If a C Valkey 9.x server saves a hash with `HEXPIRE`-set field expiries, the RDB will contain `RDB_TYPE_HASH_2 = 22`. Our loader must handle this type: read the extra 8-byte expiry timestamp per field and either discard it or apply it. If we silently corrupt by not reading those bytes, the stream is desynchronized and all subsequent keys fail. Mitigation: in the loader, detect `RDB_TYPE_HASH_2` and read (and discard) the expiry bytes. Add a corpus test with a HASH_2 input.

---

## Open decisions for operator input

**Decision 1 — LZF: implement in Round 27 or skip permanently?** Real Valkey LZF-compresses strings longer than 20 bytes when `rdb-compression yes` (the default). If we skip LZF, strings over 20 bytes will be stored larger than Valkey would store them. The bidirectional oracle still passes (C loads uncompressed strings correctly). The file size difference is the only observable gap. Recommendation: implement in Round 27 with the `lzf` crate. Total cost is ~$5 and eliminates the size divergence.

**Decision 2 — RDB version: target v80 (Valkey 9+) only, or also support v11 (Valkey 7.x/8.x)?** The `RDB_VERSION = 80` in rdb.h:52 means our saved files use the VALKEY magic and will not load in any Redis-branded server (which only accepts REDIS magic). The plan as written targets v80 only. If the operator needs interop with Redis OSS or Valkey 7.x/8.x, we must also support emitting REDIS magic with v11, which requires skipping `RDB_TYPE_HASH_2` fields. This adds roughly 1 round. Recommendation: v80 only for Def 3.3.

**Decision 3 — BGSAVE (fork) or sync SAVE only?** Round 25 stubs BGSAVE as a synchronous SAVE. Real BGSAVE forks so the parent keeps serving requests. Without fork, BGSAVE blocks the server during save. For a read-mostly cache in CI/dev, this is acceptable. If the operator needs non-blocking BGSAVE, add ~2 rounds (fork + child I/O + parent `wait`).

---

## Total estimate

- Rounds: 10 (Rounds 18–27)
- Tokens (estimate): ~1.2M input, ~600k output across all rounds
- Cost (if API): ~$74 best case, ~$110 if stream round (24) needs a second pass (expected)
- Highest-risk round: 24 (stream), assigned Opus; all others Sonnet

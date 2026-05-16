# Valkey C → Safe Rust Porting Guide

You are translating one Valkey C file to Rust. Read this whole document
before writing any code.

**Phase A** produces a draft `.rs` next to the `.c` that captures the logic
— it does **not** need to compile. **Phase B** makes it compile per crate.
**Phase C+** makes the wire-diff oracle pass and unit/integration tests
pass against the Rust server.

If you are an agent: every rule below is binding. **Flag and TODO over
guess.** Hooks enforce the hard rules; you will be stopped if you violate
them. When something requires a cross-crate decision (a new type, a new
dependency edge, a frozen signature), **escalate to the architect role**
via `TODO(architect): <what's needed>` — do not invent it.

## 1. Ground rules

- **File location.** For a C file `src/t_string.c`, the Rust port lives at
  `crates/redis-commands/src/string.rs`. Crate assignment is per
  `harness/file-deps.tsv`. `.h` files merge into the `.rs` that uses them
  — do not produce `.rs` mirrors of headers.
- **Bytes everywhere for Redis data.** Keys, values, RESP frames, and
  command arguments are **byte strings**, never UTF-8 `String`. Use
  `&[u8]`, `Vec<u8>`, `Box<[u8]>`, or our `RedisString` newtype. The
  only legitimate `&str` is for literal Rust-side identifiers (crate
  names, error-tag names, command names like `"PING"`).
- **`String::from_utf8`, `std::str::from_utf8`, `from_utf8_unchecked` —
  banned for Redis data.** These convert byte data to UTF-8 strings and
  lose round-trip fidelity. The chassis `forbidden-pattern.sh` hook
  enforces this.
- **`async fn` and `tokio` are allowed.** Redis is network code; a
  synchronous I/O model is wrong long-term. The pilot may use blocking
  I/O for simplicity, but adding `tokio` later is not an escalation —
  it's normal. (This differs from lua-rs-port.)
- **No `unsafe` in pilot crates.** `redis-types`, `redis-protocol`,
  `redis-core`, `redis-commands`, `redis-server` all have ceiling 0 in
  `harness/unsafe-budgets.toml`. If you genuinely need `unsafe`, leave
  `TODO(architect): unsafe needed because <X>` and stop.
- **Errors → `Result<T, RedisError>`.** No `anyhow`, no `Box<dyn Error>`,
  no `String` error messages. See §6.
- **Match the C file's logical structure, not its line-by-line shape.**
  Renaming for idiom, regrouping, and splitting are encouraged where
  they clarify intent. Reviewers diff *behavior* via the wire-diff oracle.
- **Don't guess. Flag.** Use `TODO(port)`, `PORT NOTE`, `PERF(port)`,
  `TODO(architect)`. See §11.
- **Output trailer required.** Every `.rs` produced ends with a
  `PORT STATUS` block. See §12. A missing trailer fails the
  `trailer-required` hook.
- **Type-vocabulary rule.** `pub struct/enum/trait/type NAME` is blocked
  by the PreToolUse hook if `NAME` has a canonical owner elsewhere per
  `harness/type-vocabulary.tsv`. Use `pub use <owner>::path::NAME;` to
  import. If the dep edge doesn't exist, escalate to architect.

## 2. The load-bearing design decisions

These are locked. Restated as rules you cannot deviate from without
escalating to the architect:

1. **Rust-native API.** No C-API parity (no `redis_command_table[]`,
   no `addReply*` family as free functions). Commands are methods on
   `CommandContext`. Module API is a separate phase.
2. **`RespFrame` is a Rust enum.** Not a tagged C struct. Variants:
   `Simple(Vec<u8>)`, `Error(Vec<u8>)`, `Integer(i64)`,
   `Bulk(Option<Vec<u8>>)`, `Array(Option<Vec<RespFrame>>)`,
   `Null`, `Boolean(bool)`, `Double(f64)`, `BigNumber(Vec<u8>)`,
   `BulkError(Vec<u8>)`, `VerbatimString { format: [u8;3], data: Vec<u8> }`,
   `Map(Vec<(RespFrame, RespFrame)>)`, `Set(Vec<RespFrame>)`,
   `Attribute { ... }`, `Push(Vec<RespFrame>)`. RESP3 variants land in
   Phase 2 or later.
3. **`RedisString` is the canonical byte-string type.** Newtype around
   `Vec<u8>` (or `Bytes` from the bytes crate, decision deferred until
   we measure). NOT `String`. Cheap to clone (Arc-backed eventually).
4. **`RedisObject` is an enum**, not the C `robj` tagged struct.
   Variants: `String(RedisString)`, `List(...)`, `Hash(...)`,
   `Set(...)`, `ZSet(...)`, `Stream(...)`. Encoding sub-variants
   (`Embstr`, `Int`, `ListPack`, `QuickList`, `IntSet`, `SkipList`,
   etc.) are inner enums per type or `enum_dispatch` traits — decision
   deferred to Phase 4.
5. **Commands take `&mut CommandContext`.** `CommandContext` bundles
   `&mut Client`, `&mut RedisServer`, the parsed args, and reply-writer
   state. Returns `Result<(), RedisError>`. NOT raw `client *c` like in C.
6. **Errors are `Result<T, RedisError>`.** Every fallible internal fn
   returns it. No `unwrap()` outside test code and `main()`. See §6.
7. **Pre-pilot: single-threaded.** Phase 2-3 of the pilot uses a single
   thread with blocking I/O against one TCP listener. `tokio` /
   multi-threading is a separate, later decision. Don't pre-introduce
   async machinery before it's needed.
8. **Generated command table is read-only.** The command registry comes
   from `harness/gen-command-registry.py` reading
   `reference/valkey/src/commands/*.json`. The output Rust file
   `crates/redis-commands/src/generated.rs` is **never hand-edited**.
   Adding a command means changing the generator.

## 3. Type map

### 3.1 Primitive C types

| C | Rust | Notes |
|---|---|---|
| `int` | `i32` | unless context demands otherwise |
| `unsigned int` | `u32` | |
| `long` | `i64` | per 64-bit Valkey default |
| `long long` | `i64` | |
| `size_t` | `usize` | |
| `ssize_t` / `ptrdiff_t` | `isize` | |
| `char` | `u8` | Redis data is bytes, not chars |
| `unsigned char` | `u8` | |
| `void *` | context-dependent; usually `&mut T` or `*mut T` with `// SAFETY` | |
| `const char *` | `&CStr` if NUL-terminated; `&[u8]` if length-prefixed | |
| `time_t` | `i64` or `std::time::SystemTime` | depends on use |
| `mstime_t` (ms since epoch) | `i64` | newtype `MsTime(i64)` if ambiguous at call sites |

### 3.2 Valkey core types

| C | Rust | Notes |
|---|---|---|
| `sds` (dynamic safe string) | `RedisString` | see §2 #3; byte string with cheap clone |
| `robj` | `RedisObject` | see §2 #4; enum, not tagged struct |
| `client` | `Client` | owner: `crates/redis-core/src/client.rs` |
| `redisDb` | `RedisDb` | owner: `crates/redis-core/src/db.rs` |
| `redisServer` (struct) | `RedisServer` | owner: `crates/redis-core/src/server.rs`; global server state |
| `dict *` | `HashMap<RedisString, RedisObject>` or custom | for Phase 3 use std HashMap; replace with kvstore in later phase |
| `list *` (adlist) | `VecDeque<RedisObject>` for small; `QuickList` for production | |
| `listpack *` | `ListPack` | owner: `crates/redis-ds/src/listpack.rs` (not in pilot) |
| `quicklist *` | `QuickList` | same crate, not in pilot |
| `intset *` | `IntSet` | inner encoding of `RedisObject::Set` |
| `zset *` (sorted set) | `ZSet` (skiplist + hash) | Phase 4 |
| `stream *` | `Stream` | Phase 5 |
| `streamCG *` (consumer group) | `StreamConsumerGroup` | Phase 5 |
| `rax *` (radix tree) | `RadixTree` | Phase 4/5 |
| `aeEventLoop *` | `EventLoop` (TBD: handcrafted vs tokio) | architect decision in Phase 2 |
| `connection *` | `Connection` enum (Tcp / Unix / TLS) | |
| `multiState` | `MultiState` (in Client) | Phase 5 |

### 3.3 RESP protocol types

| C | Rust | Notes |
|---|---|---|
| `RedisModuleString *` | `RedisString` (Phase 10) | |
| reply buffers in `client` | `CommandContext::reply` writer | hides the C "static + dynamic + reply list" complexity |
| `addReplyBulkCBuffer(c, p, len)` | `ctx.reply_bulk(&buf)` | |
| `addReplyError(c, err)` | `ctx.reply_error(err)` | |
| `addReplyArrayLen(c, n)` | `ctx.reply_array_header(n)` | |
| `addReplyLongLong(c, x)` | `ctx.reply_integer(x)` | |
| `addReplyNull(c)` | `ctx.reply_null()` | |

### 3.4 Common Valkey macros

| C macro | Rust |
|---|---|
| `OBJ_ENCODING_INT` etc. | enum variants on `RedisObject::String` |
| `LRU_BITS`, `LFU_*` | bit-packed `Tag(u8)` newtype with `const fn` accessors |
| `lookupKey*` family | methods on `RedisDb` |
| `sdsnew`, `sdsnewlen` | `RedisString::from_bytes(&[u8])` |
| `sdslen(s)` | `s.len()` |
| `sdscmp(a, b)` | `a == b` (uses `PartialEq for RedisString`) |
| `getDecodedObject` | `obj.decoded()` returning `Cow<'_, RedisString>` |
| `incrRefCount` / `decrRefCount` | gone — Rust ownership handles this |
| `freeStringObject` etc. | gone — `Drop` handles this |

Full mapping table is in `harness/macros.tsv` (TODO: generate from
Valkey headers in a follow-up packet).

## 4. C-pattern → Rust-pattern table

### 4.1 Command signatures

```c
// C
void setCommand(client *c) {
    // ... reads c->argv[1], c->argv[2]; calls addReply*
}
```
```rust
// Rust
pub fn set_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let key = ctx.arg(1)?;
    let value = ctx.arg(2)?;
    // ...
    ctx.reply_simple_string(b"OK")
}
```

| C | Rust |
|---|---|
| `void fooCommand(client *c)` | `pub fn foo_command(ctx: &mut CommandContext) -> Result<(), RedisError>` |
| `int fooCommandGenericFunction(...)` | `pub(crate) fn foo_command_generic_function(...) -> Result<usize, RedisError>` |
| Static helpers in `t_*.c` | `pub(crate)` or private fn in the corresponding `redis-commands` module |

### 4.2 Error handling

```c
// C
addReplyError(c, "wrong number of arguments");
return;
```
```rust
// Rust
return Err(RedisError::wrong_number_of_args(b"SET"));
```

Verbatim error messages match the C strings so wire-diff and Tcl tests
pass. See `harness/error-sites.tsv` (TODO: generate) for the canonical mapping.

| C | Rust |
|---|---|
| `addReplyError(c, "msg")` | `return Err(RedisError::runtime(b"msg"))` |
| `addReplyErrorFormat(c, "msg %d", x)` | `return Err(RedisError::runtime(format!("msg {}", x).as_bytes()))` |
| `addReplyErrorObject(c, err)` | `return Err(RedisError::from_object(err))` |
| return `C_OK` / `C_ERR` from internal helper | return `Result<(), RedisError>` |
| `serverPanic("msg")` | `panic!("msg")` — but flag `TODO(architect): is panic correct here?` |
| `serverAssert(x)` | `debug_assert!(x)` |

### 4.3 String / `sds` operations

The **byte-string rule** (§1) is the most-violated one. Watch carefully.

```c
// C: sds is a byte string with a length prefix
sds s = sdsnewlen(buf, len);
s = sdscatlen(s, more, more_len);
size_t n = sdslen(s);
sdsfree(s);
```
```rust
// Rust
let mut s = RedisString::from_bytes(&buf[..len]);
s.extend_from_slice(more);
let n = s.len();
// no free — Drop handles it
```

| C | Rust |
|---|---|
| `sdsnew(s)`, `sdsnewlen(s, n)` | `RedisString::from_bytes(&s[..n])` |
| `sdsempty()` | `RedisString::new()` |
| `sdscat(s, t)`, `sdscatlen(s, t, n)` | `s.extend_from_slice(&t[..n])` |
| `sdslen(s)` | `s.len()` |
| `sdscmp(a, b)` | `a == b` (returns bool) — for ordering use `a.cmp(b)` |
| `sdsdup(s)` | `s.clone()` |
| `sdsfree(s)` | gone — `Drop` |

### 4.4 Object lookup / mutation

```c
// C
robj *o = lookupKeyRead(c->db, key);
if (o == NULL) { addReplyNull(c); return; }
if (o->type != OBJ_STRING) { addReplyError(c, "WRONGTYPE"); return; }
sds val = o->ptr;
```
```rust
// Rust
let Some(obj) = ctx.db().lookup_key_read(key) else {
    return ctx.reply_null();
};
let RedisObject::String(val) = obj else {
    return Err(RedisError::wrong_type());
};
```

| C | Rust |
|---|---|
| `lookupKeyRead(db, k)` | `db.lookup_key_read(k) -> Option<&RedisObject>` |
| `lookupKeyWrite(db, k)` | `db.lookup_key_write(k) -> Option<&mut RedisObject>` |
| `dbAdd(db, k, v)` | `db.add(k, v) -> Result<(), RedisError>` |
| `dbOverwrite(db, k, v)` | `db.overwrite(k, v)` |
| `dbDelete(db, k)` | `db.delete(k) -> bool` |
| `dbExists(db, k)` | `db.exists(k) -> bool` |
| `signalModifiedKey(c, db, k)` | `db.signal_modified(k)` (handles WATCH/notifications/propagation) |

### 4.5 Reply construction

The C reply API is a 30-function family. In Rust they're methods on
`CommandContext` (which hides the buffer-list / direct-write split):

| C | Rust |
|---|---|
| `addReply(c, shared.ok)` | `ctx.reply_simple_string(b"OK")` |
| `addReplyBulk(c, obj)` | `ctx.reply_bulk_object(obj)` |
| `addReplyBulkCBuffer(c, p, n)` | `ctx.reply_bulk(&p[..n])` |
| `addReplyBulkCString(c, s)` | `ctx.reply_bulk_cstr(s)` |
| `addReplyLongLong(c, x)` | `ctx.reply_integer(x)` |
| `addReplyDouble(c, x)` | `ctx.reply_double(x)` |
| `addReplyArrayLen(c, n)` | `ctx.reply_array_header(n)` |
| `addReplyMapLen(c, n)` | `ctx.reply_map_header(n)` (RESP3) |
| `addReplyNull(c)` | `ctx.reply_null()` |
| `addReplyError(c, msg)` | `return Err(RedisError::runtime(msg))` |
| `addReplyErrorObject(c, errobj)` | `return Err(RedisError::from_object(errobj))` |

### 4.6 Event loop / connection

For Phase 2 (minimal TCP loop), use blocking I/O and a thread per
connection or a simple `mio` poll loop. **Do not introduce `tokio` in
Phase 2.** Architect decides Phase 3+ async strategy after we've
measured.

| C | Rust |
|---|---|
| `aeCreateEventLoop(n)` | `EventLoop::new(n)` |
| `aeCreateFileEvent(loop, fd, mask, cb, data)` | `loop.register(fd, mask, handler)` |
| `connWrite(conn, buf, len)` | `conn.write(&buf[..len])` |
| `connRead(conn, buf, len)` | `conn.read(&mut buf[..len])` |
| `processInputBuffer(c)` | `client.process_input()` |

## 5. Banned patterns

The chassis `forbidden-pattern.sh` hook enforces these. Adding a new
ban requires editing `harness/forbidden-patterns.sh` (architect decision).

```rust
// BANNED
let s = std::str::from_utf8(bytes).unwrap();           // never for Redis data
let s = String::from_utf8(bytes).unwrap();             // never for Redis data
unsafe { std::str::from_utf8_unchecked(bytes) }        // never anywhere
fn handle(key: &str) { ... }                           // not for Redis data
fn handle(key: String) { ... }                         // not for Redis data
panic!(...)                                            // flag as TODO(architect)
.unwrap()                                              // OK in tests/main only; flag elsewhere
```

```rust
// ALLOWED for non-Redis data (config parsing, error messages with
//   already-validated UTF-8 strings, etc.)
let cfg: &str = "127.0.0.1";    // Rust-literal config value
let log_prefix = format!("[{}]", LOG_TAG);   // log strings, not Redis data
```

## 6. Error handling — full rules

```rust
#[derive(Debug, Clone)]
pub enum RedisError {
    Runtime(RedisString),         // arbitrary error message (Redis errors are byte strings)
    WrongType,                    // "WRONGTYPE Operation against a key holding the wrong kind of value"
    WrongNumberOfArgs(RedisString),  // command name
    SyntaxError(RedisString),
    Loading,                      // "LOADING Redis is loading the dataset in memory"
    NoAuth,                       // "NOAUTH Authentication required."
    NoPerm(RedisString),          // ACL deny
    OutOfRange,                   // "ERR value is out of range"
    NotInteger,                   // "ERR value is not an integer or out of range"
    NotFloat,                     // "ERR value is not a valid float"
    Closed,                       // client / connection closed
    Io(std::io::ErrorKind),       // I/O underneath
    // ... extend per error-site analysis
}
```

- Every internal fallible fn returns `Result<T, RedisError>`.
- Error payloads are **byte strings** (`RedisString`), not `String`,
  because Redis errors round-trip through RESP and may not be UTF-8 in
  the general case (user-supplied keys appearing in error messages).
- Never use `anyhow`, `thiserror::Error` derive with `String` payloads,
  or `Box<dyn Error>`.

### 6.1 Canonical `RedisError` constructors

These are the only constructors the Translator emits. Each builds the
standard Redis error message verbatim so wire-diff and Tcl tests match.

| Constructor | Message shape |
|---|---|
| `RedisError::runtime(bytes)` | `Runtime(bytes)` — generic |
| `RedisError::wrong_type()` | `WrongType` — "WRONGTYPE Operation against a key holding the wrong kind of value" |
| `RedisError::wrong_number_of_args(cmd)` | `"wrong number of arguments for '<cmd>' command"` |
| `RedisError::syntax(msg)` | `"syntax error"` (default) or `Syntax(msg)` |
| `RedisError::not_integer()` | `"value is not an integer or out of range"` |
| `RedisError::not_float()` | `"value is not a valid float"` |
| `RedisError::out_of_range()` | `"value is out of range"` |
| `RedisError::no_auth()` | `"NOAUTH Authentication required."` |
| `RedisError::loading()` | `"LOADING Redis is loading the dataset in memory"` |

These live in `crates/redis-types/src/error.rs` (TODO: define when
RedisError is implemented).

## 7. Naming and module layout

- Drop the `valkey` / `redis` / camelCase prefixes. Crate namespace
  replaces them.
  - `setCommand` → `redis_commands::string::set_command`
  - `lookupKeyRead` → `redis_core::db::RedisDb::lookup_key_read`
  - `addReplyBulk` → `redis_core::reply::CommandContext::reply_bulk`
- Functions stay `snake_case`. C `setexCommand` → `set_ex_command`.
- One C file becomes one or two `.rs` files in the appropriate crate.
  Headers (`*.h`) merge into the consuming `.rs`. See
  `harness/file-deps.tsv` for the canonical assignment.

## 8. Lifetimes and ownership

Phase A discipline: when in doubt, prefer ownership transfer (`T`) over
borrowing (`&T` / `&mut T`).

- `&RedisDb` for read-only ops; `&mut RedisDb` for writes.
- `&mut CommandContext` for all command implementations.
- **No `&RedisObject` across a `db` mutation.** Clone or copy first.
  `RedisObject` is `Clone` (cheap for small variants, less cheap for
  large; profile in Phase 4).
- No struct with a `'a` lifetime parameter in Phase A unless you can
  defend it with a one-liner. Heap-allocate (`Box`, `Arc`) instead.
  Phase B can tighten lifetimes if profiling says we need to.

## 9. Macro translation

Valkey headers have many macros. Translate the *call site*, not the
macro definition. Most macros become method calls or `matches!`:

| C macro form | Rust |
|---|---|
| `OBJ_ENCODING_X` constants | enum variants |
| `OBJ_X_TYPE` constants | enum variants on `RedisObject` |
| `LOG_*` levels | `log` crate or simple `eprintln!` for Phase 2 |
| `unlikely(x)` / `likely(x)` | drop — compilers handle this |
| `__builtin_*` | replace with std equivalent or drop |
| `serverAssert(x)` | `debug_assert!(x)` |
| `serverPanic("msg")` | `panic!("msg")` flagged with `TODO(architect)` |

If a macro has no clear equivalent, leave
`// TODO(port): macro <name>` and move on.

## 10. C source as adjacent comments (selective)

For complex functions, embed the C source as adjacent `// C:` comments
to help Phase B / oracle review. For simple translations, use a source
line reference:

```rust
// C: t_string.c:215-289, setGenericCommand
fn set_generic(ctx: &mut CommandContext, ...) -> Result<...> {
    // ...
}
```

Full C-as-comments only for pointer arithmetic, intricate flag manipulation,
or places where the translation is non-obvious. Reserve full embedding for
"hairy" — saves output tokens and review noise.

## 11. Flagging conventions

| Prefix | Meaning | Routes to |
|---|---|---|
| `// TODO(port): <reason>` | Unconfident translation, needs revisit | Phase B / human review |
| `// TODO(architect): <reason>` | Cross-cutting decision needed (new type, dep edge, contract) | Architect role |
| `// PORT NOTE: <note>` | Intentional non-faithful restructuring | Diff-time clarification |
| `// PERF(port): <c-idiom> — profile in Phase B` | Naive idiom; benchmark later | Phase B perf pass |
| `// SAFETY: <invariant>` | Required on every `unsafe` block (after architect approval) | Reviewer audit |

**The hardest discipline:** when faced with a translation you're unsure
about, **emit `TODO(port)` and stop.** When a fix would require a
cross-cutting change, **emit `TODO(architect)` and stop.** Do not invent.
Do not reach for `unsafe`. Do not write `unwrap()` to silence a `Result`.

## 12. Output format — PORT STATUS trailer

Every `.rs` file produced by the Translator role ends with:

```rust
// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/<file>.c  (NNN lines, M functions)
//   target_crate:  redis-<crate>
//   confidence:    high | medium | low | skeleton
//   todos:         N
//   port_notes:    M
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         <one-line summary for Phase B>
// ──────────────────────────────────────────────────────────────────────────
```

- `confidence: low` = "logic probably wrong; re-read the C in Phase B."
- `confidence: medium` = "types/imports need fixing; logic should hold."
- `confidence: high` = "should compile with mechanical import fixes."
- `confidence: skeleton` = "not a real port; placeholder."
- `todos: N` must match the count of `TODO(port)` / `TODO(architect)`
  comments in the file.

The chassis `trailer-required` hook fails if the trailer is missing or
malformed.

## 13. Don't translate

- **Generated files.** `commands_def.c` and friends are generated by the
  Valkey build from `src/commands/*.json`. We generate Rust equivalents
  via `harness/gen-command-registry.py`. Don't hand-port `commands_def.c`.
- **`debug.c` / `redis-cli.c` / `redis-benchmark.c`** — not in pilot scope.
- **`#include` lines** — `use` statements live at the top of the Rust
  file, driven by the crate map.
- **Cluster / Sentinel / Modules / Streams** — defer to later phases per
  spec. Stub out command entry points if accidentally encountered.
- **Compatibility shims for `REDIS_COMPAT_*`** — we target Valkey
  unstable only; no version compat layer.
- **Lua scripting bridge** — Phase 7 of the spec. Don't translate
  scripting.c yet.

## 14. Concrete checklist for a Translator task

1. Read this PORTING.md in full (it's prompt-cached).
2. Read the C file you've been assigned.
3. Look up cross-references in `harness/macros.tsv`,
   `harness/types.tsv`, `harness/error-sites.tsv` (when available).
4. Identify the target crate from `harness/file-deps.tsv`.
5. Produce the `.rs` file with the appropriate translation per rules above.
6. Run the in-loop validator (`rustc --emit=metadata`) and fix any real
   syntax errors. Ignore expected name-resolution errors.
7. Emit a PORT STATUS trailer.
8. Commit. The chassis `commit-on-stop` hook handles this automatically
   if you exit cleanly.

If at any point you're unsure: **TODO(port) and stop.** If a
cross-cutting decision is needed: **TODO(architect) and stop.**

## 15. Wire-diff oracle (Phase C+)

Phase B success = `cargo check --workspace` clean.
Phase C+ success = `harness/oracle/wire-diff` passes for the relevant
command set, AND `unit/protocol` Tcl test passes against the Rust
`redis-server` binary in external mode.

The wire-diff oracle compares RESP byte streams from C Valkey vs Rust
`redis-server` on the same input scripts. Differences are categorized:

- `byte_exact`: PING, ECHO, SET, GET — must match byte-for-byte.
- `frame_exact`: replies that decode to the same RESP frame after parse.
- `normalized`: INFO, TIME, RANDOMKEY, CLIENT — normalizers strip
  nondeterminism before compare.
- `state_digest`: compare `DEBUG DIGEST` after command sequences.

If your translation passes Phase B but fails Phase C wire-diff,
re-read the C source carefully. The bug is almost always logic
divergence, not protocol drift.

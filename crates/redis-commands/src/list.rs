//! List type and command implementations.
//!
//! Covers LPUSH, RPUSH, LPUSHX, RPUSHX, LINSERT, LLEN, LINDEX, LSET,
//! LPOP, RPOP, LRANGE, LTRIM, LPOS, LREM, LMOVE, RPOPLPUSH, LMPOP,
//! and their blocking variants BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP.
//!
//! Lists use two internal encodings, transitioning automatically:
//! - **Listpack** — compact sequential encoding for short lists.
//! - **Quicklist** — doubly-linked chain of listpack nodes for larger lists.
//!
//! C source: `reference/valkey/src/t_list.c` (1333 lines, 59 functions)
//! Crate:    `redis-commands` (later phase)
//!
//! # Architect items
//!
//! TODO(architect): `ListTypeIterator` borrow-checker design — in C the
//! iterator stores `robj *subject` enabling in-place mutation through the
//! cursor.  Rust cannot alias `&mut RedisObject` inside the struct while
//! it lives.  Functions that mutate the subject (delete, insert, replace)
//! receive `subject: &mut RedisObject` as an explicit extra parameter.
//! Phase B must decide between RefCell, a cursor redesign, or split-borrow.
//!
//! TODO(architect): `QuickList` and `ListPack` canonical types live in
//! `redis-ds` (Phase 4).  All ql/lp operations are stubbed with
//! placeholder types until that crate is available.
//!
//! TODO(architect): blocking infrastructure (`blockForKeys`, `deny_blocking`
//! flag) lives in `redis-core/src/blocked.rs`.  Blocking command variants
//! are stubs pending Phase 3+.
//!
//! TODO(architect): `CommandContext` needs `notify_keyspace_event`,
//! `signal_modified_key`, `server_dirty_add`, and `db_mut().lookup_key_write`
//! accessors wired up in Phase 3 when `RedisServer` is part of the context.
//!
//! TODO(architect): `initDeferredReplyBuffer` / `commitDeferredReplyBuffer` /
//! `reply_deferred_len` / `set_deferred_array_len` — deferred reply protocol
//! used by LMPOP / BLMPOP / LPOS COUNT.  Needs `CommandContext` extension.
//!
//! TODO(architect): `prepareClientForFutureWrites` / `writePreparedClient` —
//! optimised write-preparation path for range replies (Phase 3 networking).
//!
//! TODO(architect): `rewriteClientCommandVector` — command rewriting for
//! AOF/replication (Phase 3+ replication layer).

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

/// Internal storage encoding of a list object.
/// C: `OBJ_ENCODING_LISTPACK` / `OBJ_ENCODING_QUICKLIST`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListEncoding {
    Listpack,
    Quicklist,
}

/// Which end of the list to operate on.
/// C: `LIST_HEAD` / `LIST_TAIL`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPosition {
    Head,
    Tail,
}

/// Type of encoding conversion to perform.
/// C: `list_conv_type` — `LIST_CONV_AUTO` / `LIST_CONV_GROWING` / `LIST_CONV_SHRINKING`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConvType {
    Auto,
    Growing,
    Shrinking,
}

/// Callback invoked just before a list encoding conversion.
/// C: `typedef void (*beforeConvertCB)(void *data)` — the `data` argument is
/// absorbed into the closure environment.
pub type BeforeConvertCallback = Box<dyn FnOnce()>;

/// Value at a list entry position.
///
/// Unifies the C `(unsigned char *vstr, size_t vlen, long long lval)` triple:
/// `vstr == NULL` in C signals the integer case.
#[derive(Debug, Clone)]
pub enum ListEntryValue {
    Bytes(Vec<u8>),
    Integer(i64),
}

/// Placeholder for the quicklist iterator from `redis-ds` (Phase 4).
/// TODO(architect): replace with `redis_ds::quicklist::QuickListIter`.
pub struct QuickListIterPlaceholder;

/// Iterator over list elements.
/// C: `listTypeIterator`
///
/// PORT NOTE: In C this struct stores `robj *subject` for in-place mutation.
/// Rust cannot alias `&mut RedisObject` inside the struct while it is alive;
/// functions that need to mutate the subject receive it as an explicit param.
/// See TODO(architect) in module doc.
pub struct ListTypeIterator {
    pub encoding: ListEncoding,
    pub direction: ListPosition,
    /// TODO(port): replace with `redis_ds::quicklist::QuickListIter`
    pub quicklist_iter: Option<Box<QuickListIterPlaceholder>>,
    /// Serialised current-element cursor for the listpack path.
    /// TODO(port): replace with a proper `redis_ds::listpack::ListPackCursor`
    pub listpack_ptr: Option<Vec<u8>>,
}

/// An entry at the current iterator position.
/// C: `listTypeEntry`
///
/// PORT NOTE: The C struct back-references `listTypeIterator *li`.  In Rust,
/// `encoding` and `direction` are copied from the iterator when the entry is
/// populated by `list_type_next`, avoiding the aliased back-reference.
pub struct ListTypeEntry {
    pub encoding: ListEncoding,
    pub direction: ListPosition,
    /// String value for quicklist entries; `None` → entry is integer-encoded.
    /// TODO(port): replace with `redis_ds::quicklist::QuickListEntry`
    pub quicklist_value: Option<Vec<u8>>,
    pub quicklist_longval: i64,
    pub quicklist_sz: usize,
    /// String value for listpack entries; `None` → entry is integer-encoded.
    /// TODO(port): replace with `redis_ds::listpack::ListPackElement`
    pub listpack_value: Option<Vec<u8>>,
    pub listpack_integer: Option<i64>,
}

impl ListTypeEntry {
    fn new(encoding: ListEncoding, direction: ListPosition) -> Self {
        Self {
            encoding,
            direction,
            quicklist_value: None,
            quicklist_longval: 0,
            quicklist_sz: 0,
            listpack_value: None,
            listpack_integer: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Encoding-conversion helpers (internal)
// ─────────────────────────────────────────────────────────────────────────────

/// Check whether a listpack list needs to grow into a quicklist.
///
/// Checks the combined byte count and element count of the listpack plus the
/// elements in `args[start..=end]` against the server-configured limits.
///
/// C: `listTypeTryConvertListpack` (static) — t_list.c:42-71
fn list_type_try_convert_listpack(
    subject: &mut RedisObject,
    args: Option<&[RedisObject]>,
    start: usize,
    end: usize,
    before_convert: Option<BeforeConvertCallback>,
) -> Result<(), RedisError> {
    debug_assert!(matches!(subject, RedisObject::List(_)));

    let mut add_bytes: usize = 0;
    let add_length: usize = if args.is_some() {
        end.saturating_sub(start).saturating_add(1)
    } else {
        0
    };

    if let Some(argv) = args {
        for i in start..=end {
            if let Some(RedisObject::String(s)) = argv.get(i) {
                add_bytes += s.len();
            }
        }
    }

    // TODO(port): quicklistNodeExceedsLimit(server.list_max_listpack_size,
    //   lpBytes(lp) + add_bytes, lpLength(lp) + add_length)
    // TODO(architect): server config must be accessible (server.list_max_listpack_size,
    //   server.list_compress_depth) — either via CommandContext or a passed Config ref
    let exceeds_limit = false; // TODO(port): real limit check
    if exceeds_limit {
        if let Some(cb) = before_convert {
            cb();
        }
        // TODO(port): ql = quicklistNew(max_listpack_size, compress_depth)
        // if lpLength(lp) > 0: quicklistAppendListpack(ql, lp) else lpFree(lp)
        // update subject's inner value to Quicklist encoding
    }
    let _ = (add_bytes, add_length);
    Ok(())
}

/// Check whether a quicklist can shrink back to a listpack.
///
/// Only converts when the quicklist has exactly one packed node and both its
/// byte size and element count are below the configured limit.  When
/// `shrinking` is `true` the limit is halved to avoid oscillation.
///
/// C: `listTypeTryConvertQuicklist` (static) — t_list.c:82-109
fn list_type_try_convert_quicklist(
    subject: &mut RedisObject,
    shrinking: bool,
    before_convert: Option<BeforeConvertCallback>,
) -> Result<(), RedisError> {
    debug_assert!(matches!(subject, RedisObject::List(_)));

    // TODO(port): return early unless ql->len == 1 &&
    //   ql->head->container == QUICKLIST_NODE_CONTAINER_PACKED
    // TODO(port): quicklistNodeLimit(server.list_max_listpack_size, &sz_limit, &count_limit)
    //   if shrinking: sz_limit /= 2; count_limit /= 2
    //   if ql->head->sz > sz_limit || ql->count > count_limit: return Ok(())
    let can_convert = false; // TODO(port): real check
    if can_convert {
        if let Some(cb) = before_convert {
            cb();
        }
        // TODO(port): extract listpack from unique quicklist node, update subject encoding
        // ql->head->entry → subject value; ql->head->entry = NULL; quicklistRelease(ql)
    }
    let _ = shrinking;
    Ok(())
}

/// Route to the appropriate conversion helper based on current encoding and intent.
///
/// C: `listTypeTryConversionRaw` (static) — t_list.c:126-137
fn list_type_try_conversion_raw(
    subject: &mut RedisObject,
    lct: ListConvType,
    args: Option<&[RedisObject]>,
    start: usize,
    end: usize,
    before_convert: Option<BeforeConvertCallback>,
) -> Result<(), RedisError> {
    // TODO(port): derive encoding from subject's List variant inner data
    let encoding = ListEncoding::Listpack; // placeholder — needs real encoding read
    match encoding {
        ListEncoding::Quicklist => {
            if lct == ListConvType::Growing {
                return Ok(()); // growing has nothing to do with quicklist
            }
            list_type_try_convert_quicklist(subject, lct == ListConvType::Shrinking, before_convert)
        }
        ListEncoding::Listpack => {
            if lct == ListConvType::Shrinking {
                return Ok(()); // shrinking has nothing to do with listpack
            }
            list_type_try_convert_listpack(subject, args, start, end, before_convert)
        }
    }
}

/// Try an encoding conversion without specifying elements about to be added.
/// C: `listTypeTryConversion` — t_list.c:141-143
pub fn list_type_try_conversion(
    subject: &mut RedisObject,
    lct: ListConvType,
    before_convert: Option<BeforeConvertCallback>,
) -> Result<(), RedisError> {
    list_type_try_conversion_raw(subject, lct, None, 0, 0, before_convert)
}

/// Try an encoding conversion accounting for `args[start..=end]` about to be appended.
/// C: `listTypeTryConversionAppend` — t_list.c:147-149
pub fn list_type_try_conversion_append(
    subject: &mut RedisObject,
    args: &[RedisObject],
    start: usize,
    end: usize,
    before_convert: Option<BeforeConvertCallback>,
) -> Result<(), RedisError> {
    list_type_try_conversion_raw(subject, ListConvType::Growing, Some(args), start, end, before_convert)
}

// ─────────────────────────────────────────────────────────────────────────────
// Push / Pop / Length
// ─────────────────────────────────────────────────────────────────────────────

/// Push `value` onto the head or tail of `subject`.
///
/// Reference-count management (C's `incrRefCount`/`decrRefCount`) is
/// eliminated by Rust ownership.
///
/// C: `listTypePush` — t_list.c:156-179
pub fn list_type_push(
    subject: &mut RedisObject,
    value: &RedisObject,
    position: ListPosition,
) -> Result<(), RedisError> {
    // TODO(port): derive encoding from subject's List variant
    let encoding = ListEncoding::Listpack; // placeholder
    match encoding {
        ListEncoding::Quicklist => {
            // TODO(port): if value is OBJ_ENCODING_INT:
            //   ll2string(buf, 32, vlong); quicklistPush(ql, buf, strlen(buf), pos)
            // else: quicklistPush(ql, sds_bytes, sds_len, pos)
        }
        ListEncoding::Listpack => {
            // TODO(port): if value is OBJ_ENCODING_INT:
            //   lp = lpPrependInteger/lpAppendInteger(lp, vlong)
            // else:
            //   lp = lpPrepend/lpAppend(lp, bytes, len)
            // update subject's inner listpack pointer
        }
    }
    let _ = (value, position);
    Ok(())
}

/// Pop and return the head or tail element of `subject`.
/// Returns `None` if the list is empty.
///
/// C: `listTypePop` — t_list.c:185-210
pub fn list_type_pop(
    subject: &mut RedisObject,
    position: ListPosition,
) -> Result<Option<RedisString>, RedisError> {
    // TODO(port): derive encoding from subject's List variant
    let encoding = ListEncoding::Listpack; // placeholder
    match encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistPopCustom(ql, ql_where, &value_ptr, NULL, &vlong, listPopSaver)
            // C: listPopSaver creates a string object from raw bytes
            // if value_ptr is NULL: createStringObjectFromLongLong(vlong)
        }
        ListEncoding::Listpack => {
            // TODO(port): p = if Head lpFirst(lp) else lpLast(lp)
            // if p: vstr = lpGet(p, &vlen, intbuf); value = RedisString::from_bytes(vstr[..vlen])
            //        update subject's lp = lpDelete(lp, p, NULL)
        }
    }
    let _ = position;
    Ok(None) // TODO(port): return actual popped value
}

/// Return the number of elements in the list.
///
/// C: `listTypeLength` — `unsigned long` → `u64` (64-bit platform)
pub fn list_type_length(subject: &RedisObject) -> u64 {
    // TODO(port): derive encoding from subject
    // match encoding { Quicklist => quicklistCount(ql), Listpack => lpLength(lp) }
    let _ = subject;
    0
}

// ─────────────────────────────────────────────────────────────────────────────
// Iterator lifecycle
// ─────────────────────────────────────────────────────────────────────────────

/// Create an iterator starting at absolute index `index` moving in `direction`.
///
/// PORT NOTE: C allocates via `zmalloc` and returns a pointer; Rust returns
/// by value (no heap allocation at this level).
///
/// C: `listTypeInitIterator` — t_list.c:223-240
pub fn list_type_init_iterator(
    subject: &RedisObject,
    index: i64,
    direction: ListPosition,
) -> Result<ListTypeIterator, RedisError> {
    // TODO(port): derive encoding from subject's List variant
    let encoding = ListEncoding::Listpack; // placeholder
    let mut iter = ListTypeIterator {
        encoding,
        direction,
        quicklist_iter: None,
        listpack_ptr: None,
    };
    match encoding {
        ListEncoding::Quicklist => {
            // TODO(port): iter_dir = if direction==Head AL_START_TAIL else AL_START_HEAD
            // iter.quicklist_iter = quicklistGetIteratorAtIdx(ql, iter_dir, index)
        }
        ListEncoding::Listpack => {
            // TODO(port): iter.listpack_ptr = lpSeek(lp, index) (serialised as bytes)
        }
    }
    let _ = (subject, index);
    Ok(iter)
}

/// Change the traversal direction of an existing iterator.
///
/// PORT NOTE: `entry` is needed for the listpack path to re-anchor the cursor.
/// `subject` is explicit because the iterator does not store it (see module doc).
///
/// C: `listTypeSetIteratorDirection` — t_list.c:243-258
pub fn list_type_set_iterator_direction(
    iter: &mut ListTypeIterator,
    subject: &RedisObject,
    entry: &ListTypeEntry,
    direction: ListPosition,
) {
    if iter.direction == direction {
        return;
    }
    iter.direction = direction;
    match iter.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): dir = if direction==Head AL_START_TAIL else AL_START_HEAD
            // quicklistSetDirection(iter.quicklist_iter, dir)
        }
        ListEncoding::Listpack => {
            // PORT NOTE: the listpack iterator always points to the *next* element
            // past the current one, so we must re-anchor when the direction changes.
            // TODO(port): iter.listpack_ptr =
            //   if direction==Tail lpNext(lp, entry.lpe) else lpPrev(lp, entry.lpe)
        }
    }
    let _ = (subject, entry);
}

/// Release an iterator and free associated resources.
///
/// PORT NOTE: In Rust, `drop(iter)` is sufficient; this function exists for
/// API symmetry with the C code.  The `Drop` impl on `ListTypeIterator`
/// should call `quicklistReleaseIterator` once that type is from redis-ds.
///
/// C: `listTypeReleaseIterator` — t_list.c:261-264
pub fn list_type_release_iterator(iter: ListTypeIterator) {
    drop(iter); // TODO(port): Drop impl should free quicklist_iter via quicklistReleaseIterator
}

/// Advance the iterator and fill `entry` with the current element.
/// Returns `true` if an entry was found; `false` at end-of-list.
///
/// C: `listTypeNext` — returns int (0/1) — t_list.c:269-287
pub fn list_type_next(
    iter: &mut ListTypeIterator,
    subject: &RedisObject,
    entry: &mut ListTypeEntry,
) -> bool {
    // C: serverAssert(li->subject->encoding == li->encoding) — protect from mid-iter convert
    // TODO(port): verify subject's current encoding matches iter.encoding
    entry.encoding = iter.encoding;
    entry.direction = iter.direction;
    match iter.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistNext(iter.quicklist_iter, &qe)
            // populate entry.quicklist_value / quicklist_longval / quicklist_sz from qe
            false // TODO(port): return actual result
        }
        ListEncoding::Listpack => {
            // TODO(port): entry.listpack_ptr = iter.listpack_ptr (current cursor position)
            // if non-NULL: advance iter.listpack_ptr via lpNext/lpPrev; return true
            // else: return false
            false // TODO(port): return actual result
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry value extraction
// ─────────────────────────────────────────────────────────────────────────────

/// Extract the value at the current entry position.
///
/// Unifies C's `(vstr, vlen, lval)` output-parameter triple into a Rust enum.
///
/// C: `listTypeGetValue` — t_list.c:293-310
pub fn list_type_get_value(entry: &ListTypeEntry) -> ListEntryValue {
    match entry.encoding {
        ListEncoding::Quicklist => {
            if let Some(ref v) = entry.quicklist_value {
                ListEntryValue::Bytes(v.clone())
            } else {
                ListEntryValue::Integer(entry.quicklist_longval)
            }
        }
        ListEncoding::Listpack => {
            if let Some(ref v) = entry.listpack_value {
                ListEntryValue::Bytes(v.clone())
            } else {
                ListEntryValue::Integer(entry.listpack_integer.unwrap_or(0))
            }
        }
    }
}

/// Return the current entry as a `RedisString`.
///
/// C: `listTypeGet` — t_list.c:313-323
pub fn list_type_get(entry: &ListTypeEntry) -> RedisString {
    match list_type_get_value(entry) {
        ListEntryValue::Bytes(b) => RedisString::from_bytes(&b),
        ListEntryValue::Integer(n) => {
            // PORT NOTE: matches C's `createStringObjectFromLongLong` — decimal encoding.
            RedisString::from_bytes(n.to_string().as_bytes())
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutation via iterator
// ─────────────────────────────────────────────────────────────────────────────

/// Insert `value` before or after the current entry.
///
/// C: `listTypeInsert` — t_list.c:325-344
pub fn list_type_insert(
    iter: &mut ListTypeIterator,
    subject: &mut RedisObject,
    entry: &mut ListTypeEntry,
    value: &RedisObject,
    position: ListPosition,
) -> Result<(), RedisError> {
    // TODO(port): bytes = getDecodedObject(value) → &[u8]
    let bytes: Vec<u8> = Vec::new(); // TODO(port): decoded byte representation of value
    match iter.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): match position {
            //   Tail => quicklistInsertAfter(iter.quicklist_iter, &entry.qe, bytes, len),
            //   Head => quicklistInsertBefore(iter.quicklist_iter, &entry.qe, bytes, len),
            // }
        }
        ListEncoding::Listpack => {
            // TODO(port): lpw = if position==Tail LP_AFTER else LP_BEFORE
            // subject lp = lpInsertString(lp, bytes, len, entry.lpe, lpw, &entry.lpe)
        }
    }
    let _ = (subject, entry, value, bytes, position);
    Ok(())
}

/// Replace the element at the current iterator position.
///
/// C: `listTypeReplace` — t_list.c:347-362
pub fn list_type_replace(
    iter: &mut ListTypeIterator,
    subject: &mut RedisObject,
    entry: &mut ListTypeEntry,
    value: &RedisObject,
) -> Result<(), RedisError> {
    // TODO(port): bytes = getDecodedObject(value) → &[u8]
    let bytes: Vec<u8> = Vec::new(); // TODO(port): decoded bytes
    match iter.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistReplaceEntry(iter.quicklist_iter, &entry.qe, bytes, len)
        }
        ListEncoding::Listpack => {
            // TODO(port): subject lp = lpReplace(lp, &entry.lpe, bytes, len)
        }
    }
    let _ = (subject, entry, value, bytes);
    Ok(())
}

/// Replace the element at absolute index `index`.
/// Returns `true` if the replacement happened; `false` if index is out of range.
///
/// C: `listTypeReplaceAtIndex` — t_list.c:368-389
pub fn list_type_replace_at_index(
    subject: &mut RedisObject,
    index: i64,
    value: &RedisObject,
) -> Result<bool, RedisError> {
    // TODO(port): bytes = getDecodedObject(value) → &[u8]
    let bytes: Vec<u8> = Vec::new(); // TODO(port): decoded bytes
    // TODO(port): derive encoding from subject
    let encoding = ListEncoding::Listpack; // placeholder
    let replaced = match encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistReplaceAtIndex(ql, index, bytes, len) → bool
            false
        }
        ListEncoding::Listpack => {
            // TODO(port): p = lpSeek(lp, index); if p { lpReplace(lp, &p, bytes, len); true } else { false }
            false
        }
    };
    let _ = (subject, index, value, bytes);
    Ok(replaced)
}

/// Compare the given object with the element at the current entry position.
/// `obj` must be string-encoded (sds-backed in C).
///
/// C: `listTypeEqual` — t_list.c:392-401
pub fn list_type_equal(entry: &ListTypeEntry, obj: &RedisObject) -> bool {
    // C: serverAssertWithInfo(NULL, o, sdsEncodedObject(o))
    match entry.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistCompare(&entry.qe, obj_bytes, obj_len)
            false
        }
        ListEncoding::Listpack => {
            // TODO(port): lpCompare(entry.lpe, obj_bytes, obj_len)
            false
        }
    }
}

/// Delete the element at the current iterator position and advance.
///
/// C: `listTypeDelete` — t_list.c:404-426
pub fn list_type_delete(
    iter: &mut ListTypeIterator,
    subject: &mut RedisObject,
    entry: &mut ListTypeEntry,
) -> Result<(), RedisError> {
    match iter.encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistDelEntry(iter.quicklist_iter, &entry.qe)
        }
        ListEncoding::Listpack => {
            // TODO(port): p = entry.lpe; subject lp = lpDelete(lp, p, &p)
            // update iter.listpack_ptr based on direction:
            //   Tail → iter.lpi = p
            //   Head → if p { lpPrev(lp, p) } else { lpLast(lp) }
        }
    }
    let _ = (subject, entry);
    Ok(())
}

/// Duplicate a list object, preserving its encoding.
///
/// C: `listTypeDup` — t_list.c:433-445.  The resulting object has a
/// logical refcount of 1 (Rust: single owner).
pub fn list_type_dup(subject: &RedisObject) -> Result<RedisObject, RedisError> {
    debug_assert!(matches!(subject, RedisObject::List(_)));
    // TODO(port): match encoding {
    //   Listpack  => createObject(OBJ_LIST, lpDup(lp)) with OBJ_ENCODING_LISTPACK
    //   Quicklist => createObject(OBJ_LIST, quicklistDup(ql)) with OBJ_ENCODING_QUICKLIST
    // }
    Err(RedisError::runtime(b"list_type_dup: not yet implemented"))
}

/// Delete a range of `count` elements starting at `start` (negative = from tail).
///
/// C: `listTypeDelRange` — t_list.c:448-456
pub fn list_type_del_range(
    subject: &mut RedisObject,
    start: i64,
    count: i64,
) -> Result<(), RedisError> {
    // TODO(port): derive encoding from subject
    let encoding = ListEncoding::Listpack; // placeholder
    match encoding {
        ListEncoding::Quicklist => {
            // TODO(port): quicklistDelRange(ql, start, count)
        }
        ListEncoding::Listpack => {
            // TODO(port): subject lp = lpDeleteRange(lp, start, count)
        }
    }
    let _ = (subject, start, count);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Range-reply helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Reply with a range of elements from a quicklist-encoded list.
///
/// C: `addListQuicklistRangeReply` — t_list.c:656-674
///
/// TODO(architect): `prepareClientForFutureWrites`/`writePreparedClient`
/// optimisation deferred to Phase 3 networking work.
pub(crate) fn add_list_quicklist_range_reply(
    ctx: &mut CommandContext,
    subject: &RedisObject,
    from: i64,
    rangelen: i64,
    reverse: bool,
) -> Result<(), RedisError> {
    ctx.reply_array_header(rangelen as usize)?;
    // TODO(port): direction = if reverse AL_START_TAIL else AL_START_HEAD
    // iter = quicklistGetIteratorAtIdx(ql, direction, from)
    // while rangelen-- { quicklistNext(iter, &qe); reply qe.value (bytes) or qe.longval }
    // quicklistReleaseIterator(iter)
    let _ = (subject, from, reverse);
    Ok(())
}

/// Reply with a range of elements from a listpack-encoded list.
///
/// C: `addListListpackRangeReply` — t_list.c:679-699
///
/// TODO(architect): same write-prep optimisation as quicklist variant.
pub(crate) fn add_list_listpack_range_reply(
    ctx: &mut CommandContext,
    subject: &RedisObject,
    from: i64,
    rangelen: i64,
    reverse: bool,
) -> Result<(), RedisError> {
    ctx.reply_array_header(rangelen as usize)?;
    // TODO(port): p = lpSeek(lp, from)
    // while rangelen-- {
    //   vstr = lpGetValue(p, &vlen, &lval)
    //   if vstr: reply_bulk(vstr[..vlen]) else reply_integer(lval)
    //   p = if reverse lpPrev(lp, p) else lpNext(lp, p)
    // }
    let _ = (subject, from, reverse);
    Ok(())
}

/// Reply with a sub-range of a list, supporting negative indexes and reverse order.
///
/// Clamps `start` and `end` to valid range; returns empty array if range is empty.
///
/// C: `addListRangeReply` — t_list.c:706-730
pub(crate) fn add_list_range_reply(
    ctx: &mut CommandContext,
    subject: &RedisObject,
    start: i64,
    end: i64,
    reverse: bool,
) -> Result<(), RedisError> {
    let llen = list_type_length(subject) as i64;
    let mut start = start;
    let mut end = end;

    if start < 0 {
        start += llen;
    }
    if end < 0 {
        end += llen;
    }
    if start < 0 {
        start = 0;
    }

    if start > end || start >= llen {
        return ctx.reply_empty_array();
    }
    if end >= llen {
        end = llen - 1;
    }
    let rangelen = end - start + 1;
    let from = if reverse { end } else { start };

    // TODO(port): derive encoding from subject
    let encoding = ListEncoding::Listpack; // placeholder
    match encoding {
        ListEncoding::Quicklist => add_list_quicklist_range_reply(ctx, subject, from, rangelen, reverse),
        ListEncoding::Listpack => add_list_listpack_range_reply(ctx, subject, from, rangelen, reverse),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Housekeeping after element removal
// ─────────────────────────────────────────────────────────────────────────────

/// Fire keyspace notifications, delete the key if now empty, attempt encoding
/// downgrade, signal modified key, and increment dirty counter.
///
/// PORT NOTE: C's `int *deleted` output parameter is a `bool` return value here.
///
/// C: `listElementsRemoved` — t_list.c:736-751
pub(crate) fn list_elements_removed(
    ctx: &mut CommandContext,
    key: &RedisString,
    position: ListPosition,
    subject: &mut RedisObject,
    count: i64,
) -> Result<bool, RedisError> {
    let event: &[u8] = match position {
        ListPosition::Head => b"lpop",
        ListPosition::Tail => b"rpop",
    };
    // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, event, key, db_id)

    let deleted = if list_type_length(subject) == 0 {
        // TODO(architect): ctx.db_mut().delete(key)
        // TODO(architect): ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", key, db_id)
        true
    } else {
        list_type_try_conversion(subject, ListConvType::Shrinking, None)?;
        false
    };

    // TODO(architect): ctx.signal_modified_key(key)
    // TODO(architect): ctx.server_dirty_add(count)
    let _ = (event, count);
    Ok(deleted)
}

// ─────────────────────────────────────────────────────────────────────────────
// Pop range + key reply (LMPOP / BLMPOP)
// ─────────────────────────────────────────────────────────────────────────────

/// Pop up to `count` elements from `subject` and reply as `[key, [elem, ...]]`.
///
/// C: `listPopRangeAndReplyWithKey` — t_list.c:633-651
pub(crate) fn list_pop_range_and_reply_with_key(
    ctx: &mut CommandContext,
    subject: &mut RedisObject,
    key: &RedisString,
    position: ListPosition,
    count: i64,
) -> Result<(), RedisError> {
    let llen = list_type_length(subject) as i64;
    let rangelen = count.min(llen);
    let (rangestart, rangeend, reverse) = match position {
        ListPosition::Head => (0i64, rangelen - 1, false),
        ListPosition::Tail => (-rangelen, -1i64, true),
    };

    // TODO(architect): initDeferredReplyBuffer(ctx)
    ctx.reply_array_header(2)?;
    ctx.reply_bulk(key.as_bytes())?;
    add_list_range_reply(ctx, subject, rangestart, rangeend, reverse)?;
    list_type_del_range(subject, rangestart, rangelen)?;
    list_elements_removed(ctx, key, position, subject, rangelen)?;
    // TODO(architect): commitDeferredReplyBuffer(ctx, 1)
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Generic push / pop
// ─────────────────────────────────────────────────────────────────────────────

/// Generic LPUSH / RPUSH / LPUSHX / RPUSHX.
/// `xx` = true means push only when the key already exists (X variants).
///
/// C: `pushGenericCommand` — t_list.c:464-490
fn push_generic_command(
    ctx: &mut CommandContext,
    position: ListPosition,
    xx: bool,
) -> Result<(), RedisError> {
    // TODO(architect): lobj = ctx.db_mut().lookup_key_write(ctx.arg(1)?)
    // TODO(port): if lobj exists and type != OBJ_LIST: return WrongType error
    let lobj_exists = false; // TODO(port): from real DB lookup

    if !lobj_exists {
        if xx {
            return ctx.reply_integer(0);
        }
        // TODO(architect): lobj = createListListpackObject(); ctx.db_mut().add(argv[1], lobj)
    }

    let argc = ctx.argc();
    // TODO(port): list_type_try_conversion_append(lobj, argv, 2, argc-1, None)?
    // for j in 2..argc: list_type_push(lobj, argv[j], position)?; server.dirty++

    let event: &[u8] = match position {
        ListPosition::Head => b"lpush",
        ListPosition::Tail => b"rpush",
    };
    // TODO(architect): ctx.signal_modified_key(argv[1])
    // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, event, argv[1], db_id)
    // TODO(port): ctx.reply_integer(list_type_length(lobj) as i64)
    let _ = (argc, event, lobj_exists);
    ctx.reply_integer(0) // TODO(port): replace with actual list length after push
}

/// Generic LPOP / RPOP.
///
/// An optional third argument provides a count; when absent, a single bulk
/// reply is returned.  With count, an array of up to count elements is returned.
///
/// C: `popGenericCommand` — t_list.c:757-802
fn pop_generic_command(ctx: &mut CommandContext, position: ListPosition) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc > 3 {
        return Err(RedisError::wrong_number_of_args(b"pop"));
    }
    let has_count = argc == 3;
    let count: Option<i64> = if has_count {
        // TODO(port): getPositiveLongFromObjectOrReply(argv[2], NULL) → count
        Some(0) // TODO(port): parse actual count from argv[2]
    } else {
        None
    };

    // TODO(architect): o = ctx.db_mut().lookup_key_write_or_reply_null(argv[1])
    // TODO(port): if o == NULL or type != OBJ_LIST: return
    let o_present = false; // TODO(port): from real DB lookup
    if !o_present {
        return Ok(());
    }

    match count {
        Some(0) => ctx.reply_empty_array(),
        None => {
            // Single pop: reply with bulk string
            // TODO(port): value = list_type_pop(o, position)?
            // list_elements_removed(ctx, argv[1], position, o, 1)?
            // ctx.reply_bulk_object(value)
            ctx.reply_null()
        }
        Some(n) => {
            // Range pop: reply with array
            // TODO(port): llen = list_type_length(o) as i64
            // rangelen = n.min(llen)
            // rangestart/rangeend/reverse as in C
            // initDeferredReplyBuffer; addListRangeReply; listTypeDelRange; listElementsRemoved; commit
            let _ = n;
            ctx.reply_empty_array()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LMOVE / LMPOP helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Parse "LEFT" or "RIGHT" from a command argument byte slice.
/// Returns an error and replies if the argument is invalid.
///
/// C: `getListPositionFromObjectOrReply` — t_list.c:1086-1096
pub(crate) fn get_list_position_from_object_or_reply(
    _ctx: &mut CommandContext,
    arg: &[u8],
) -> Result<ListPosition, RedisError> {
    if arg.eq_ignore_ascii_case(b"right") {
        Ok(ListPosition::Tail)
    } else if arg.eq_ignore_ascii_case(b"left") {
        Ok(ListPosition::Head)
    } else {
        Err(RedisError::syntax(b"syntax error"))
    }
}

/// Map a list position to its canonical replication string.
///
/// PORT NOTE: C returns `robj *shared.left/right`; Rust returns `&'static [u8]`.
///
/// C: `getStringObjectFromListPosition` — t_list.c:1098-1105
pub(crate) fn get_string_object_from_list_position(position: ListPosition) -> &'static [u8] {
    match position {
        ListPosition::Head => b"LEFT",
        ListPosition::Tail => b"RIGHT",
    }
}

/// Push `value` onto `dstobj` (creating it if absent) and reply with the value.
///
/// C: `lmoveHandlePush` — t_list.c:1072-1084
pub(crate) fn lmove_handle_push(
    ctx: &mut CommandContext,
    dst_key: &RedisString,
    dst_obj_exists: bool,
    value: &RedisString,
    position: ListPosition,
) -> Result<(), RedisError> {
    if !dst_obj_exists {
        // TODO(architect): createListListpackObject(); ctx.db_mut().add(dst_key, dstobj)
    }
    // TODO(port): list_type_try_conversion_append(dstobj, &[value_as_obj], 0, 0, None)?
    // TODO(port): list_type_push(dstobj, value_as_obj, position)?
    // TODO(architect): ctx.signal_modified_key(dst_key)
    // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, "lpush"/"rpush", dst_key, db_id)
    ctx.reply_bulk(value.as_bytes())?;
    let _ = (dst_key, dst_obj_exists, position);
    Ok(())
}

/// Generic LMOVE / RPOPLPUSH.
///
/// C: `lmoveGenericCommand` — t_list.c:1107-1129
fn lmove_generic_command(
    ctx: &mut CommandContext,
    wherefrom: ListPosition,
    whereto: ListPosition,
) -> Result<(), RedisError> {
    // TODO(architect): sobj = ctx.db_mut().lookup_key_write_or_reply_null(argv[1])
    // TODO(port): if sobj == NULL || checkType(sobj, OBJ_LIST): return
    // TODO(architect): dobj = ctx.db_mut().lookup_key_write(argv[2])
    // TODO(port): if checkType(dobj, OBJ_LIST): return
    // value = list_type_pop(sobj, wherefrom)?
    // lmove_handle_push(ctx, argv[2], dobj.is_some(), &value, whereto)?
    // list_elements_removed(ctx, argv[1], wherefrom, sobj, 1)?
    // TODO(port): rewriteClientCommandVector for BLMOVE / BRPOPLPUSH replication
    let _ = (wherefrom, whereto);
    ctx.reply_null()
}

/// Generic blocking pop: scan keys for first non-empty, or block.
///
/// `count == None` → single-element BLPOP/BRPOP protocol (bulk reply per element).
/// `count == Some(n)` → BLMPOP protocol (array reply with key + elements).
///
/// C: `blockingPopGenericCommand` — t_list.c:1167-1224
fn blocking_pop_generic_command(
    ctx: &mut CommandContext,
    keys: &[RedisString],
    position: ListPosition,
    timeout_arg_idx: usize,
    count: Option<i64>,
) -> Result<(), RedisError> {
    // TODO(port): timeout = getTimeoutFromObjectOrReply(argv[timeout_arg_idx], UNIT_SECONDS)?
    let _timeout: i64 = 0; // TODO(port): parse timeout from argv

    for key in keys {
        // TODO(architect): obj = ctx.db_mut().lookup_key_write(key)
        let obj: Option<&mut RedisObject> = None; // TODO(port): real DB lookup
        let obj = match obj {
            None => continue,
            Some(o) => o,
        };
        // TODO(port): if checkType(obj, OBJ_LIST): return Err(WrongType)
        let llen = list_type_length(obj) as i64;
        if llen == 0 {
            continue;
        }

        match count {
            Some(n) => {
                let key_str = key.clone();
                list_pop_range_and_reply_with_key(ctx, obj, &key_str, position, n)?;
                // TODO(port): rewriteClientCommandVector([LR]POP COUNT actual_count)
            }
            None => {
                // TODO(port): value = list_type_pop(obj, position)?
                // list_elements_removed(ctx, key, position, obj, 1)?
                ctx.reply_array_header(2)?;
                ctx.reply_bulk(key.as_bytes())?;
                // TODO(port): ctx.reply_bulk(value.as_bytes())
                // TODO(port): rewriteClientCommandVector([LR]POP key)
            }
        }
        return Ok(());
    }

    // No non-empty key found.
    // TODO(architect): if ctx.deny_blocking() { ctx.reply_null_array() }
    // else { blockForKeys(ctx, BLOCKED_LIST, keys, _timeout, 0) }
    let _ = timeout_arg_idx;
    ctx.reply_null_array()
}

/// Generic BLMOVE.
///
/// C: `blmoveGenericCommand` — t_list.c:1236-1255
fn blmove_generic_command(
    ctx: &mut CommandContext,
    wherefrom: ListPosition,
    whereto: ListPosition,
    timeout: i64,
) -> Result<(), RedisError> {
    // TODO(architect): key = ctx.db_mut().lookup_key_write(argv[1])
    // TODO(port): if checkType(key, OBJ_LIST): return
    // if key.is_none():
    //   if ctx.deny_blocking() → reply_null
    //   else → blockForKeys(ctx, BLOCKED_LIST, &[argv[1]], timeout, 0)
    // else:
    //   debug_assert!(list_type_length(key.unwrap()) > 0)
    //   lmove_generic_command(ctx, wherefrom, whereto)
    let _ = (wherefrom, whereto, timeout);
    ctx.reply_null()
}

/// Core of LMPOP and BLMPOP: parse numkeys, position, optional COUNT.
///
/// C: `lmpopGenericCommand` — t_list.c:1278-1322
fn lmpop_generic_command(
    ctx: &mut CommandContext,
    numkeys_idx: usize,
    is_block: bool,
) -> Result<(), RedisError> {
    // TODO(port): getRangeLongFromObjectOrReply(argv[numkeys_idx], 1, LONG_MAX, &numkeys,
    //   "numkeys should be greater than 0")
    let numkeys: usize = 0; // TODO(port): parse

    let where_idx = numkeys_idx + numkeys + 1;
    if where_idx >= ctx.argc() {
        return Err(RedisError::syntax(b"syntax error"));
    }
    // TODO(port): get_list_position_from_object_or_reply(ctx, argv[where_idx])
    let position = ListPosition::Head; // TODO(port): parse from argv[where_idx]

    let mut count: i64 = 1;
    let mut j = where_idx + 1;
    while j < ctx.argc() {
        let opt = ctx.arg(j)?.to_vec();
        let moreargs = ctx.argc().saturating_sub(1 + j);
        if opt.eq_ignore_ascii_case(b"COUNT") && moreargs > 0 {
            j += 1;
            // TODO(port): getRangeLongFromObjectOrReply(argv[j], 1, LONG_MAX, &count,
            //   "count should be greater than 0")
            count = 1; // TODO(port): parse actual count
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
        j += 1;
    }

    let keys: Vec<RedisString> = (numkeys_idx + 1..numkeys_idx + 1 + numkeys)
        .filter_map(|i| ctx.arg(i).ok().map(|a| RedisString::from_bytes(a)))
        .collect();

    if is_block {
        blocking_pop_generic_command(ctx, &keys, position, 1, Some(count))
    } else {
        mpop_generic_command(ctx, &keys, position, count)
    }
}

/// Multi-key non-blocking pop: find the first non-empty key and pop from it.
///
/// C: `mpopGenericCommand` — t_list.c:811-841
fn mpop_generic_command(
    ctx: &mut CommandContext,
    keys: &[RedisString],
    position: ListPosition,
    count: i64,
) -> Result<(), RedisError> {
    for key in keys {
        // TODO(architect): obj = ctx.db_mut().lookup_key_write(key)
        let obj: Option<&mut RedisObject> = None; // TODO(port): real DB lookup
        let obj = match obj {
            None => continue,
            Some(o) => o,
        };
        // TODO(port): if checkType(obj, OBJ_LIST): return Err(WrongType)
        if list_type_length(obj) == 0 {
            continue;
        }

        let key_str = key.clone();
        list_pop_range_and_reply_with_key(ctx, obj, &key_str, position, count)?;
        // TODO(port): rewriteClientCommandVector([LR]POP, key, actual_count_obj)
        return Ok(());
    }
    ctx.reply_null_array()
}

// ─────────────────────────────────────────────────────────────────────────────
// Command entry points
// ─────────────────────────────────────────────────────────────────────────────

/// LPUSH <key> <element> [<element> ...]
pub fn lpush_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    push_generic_command(ctx, ListPosition::Head, false)
}

/// RPUSH <key> <element> [<element> ...]
pub fn rpush_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    push_generic_command(ctx, ListPosition::Tail, false)
}

/// LPUSHX <key> <element> [<element> ...]
pub fn lpushx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    push_generic_command(ctx, ListPosition::Head, true)
}

/// RPUSHX <key> <element> [<element> ...]
pub fn rpushx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    push_generic_command(ctx, ListPosition::Tail, true)
}

/// LINSERT <key> (BEFORE|AFTER) <pivot> <element>
///
/// C: `linsertCommand` — t_list.c:513-561
pub fn linsert_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let direction_arg = ctx.arg(2)?.to_vec();
    let position = if direction_arg.eq_ignore_ascii_case(b"after") {
        ListPosition::Tail
    } else if direction_arg.eq_ignore_ascii_case(b"before") {
        ListPosition::Head
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };

    // TODO(architect): subject = ctx.db_mut().lookup_key_write_or_reply_zero(argv[1])
    // TODO(port): if subject == NULL || checkType(subject, OBJ_LIST): return
    let subject: Option<&mut RedisObject> = None; // TODO(port): real DB lookup
    let subject = match subject {
        None => return ctx.reply_integer(0),
        Some(s) => s,
    };

    // Pre-convert to avoid encoding change inside the iterator loop.
    // TODO(port): list_type_try_conversion_append(subject, argv, 4, 4, None)?

    let mut iter = list_type_init_iterator(subject, 0, ListPosition::Tail)?;
    let mut entry = ListTypeEntry::new(iter.encoding, iter.direction);
    let mut inserted = false;

    while list_type_next(&mut iter, subject, &mut entry) {
        // TODO(port): if list_type_equal(&entry, argv[3]):
        //   list_type_insert(&mut iter, subject, &mut entry, argv[4], position)?
        //   inserted = true; break
        let _ = &position;
        break; // TODO(port): remove this placeholder break once iteration body is implemented
    }
    list_type_release_iterator(iter);

    if inserted {
        // TODO(architect): ctx.signal_modified_key(argv[1])
        // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, b"linsert", argv[1], db_id)
        // TODO(architect): ctx.server_dirty_incr()
        ctx.reply_integer(list_type_length(subject) as i64)
    } else {
        ctx.reply_integer(-1)
    }
}

/// LLEN <key>
///
/// C: `llenCommand` — t_list.c:564-568
pub fn llen_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(architect): o = ctx.db().lookup_key_read_or_reply_zero(argv[1])
    // TODO(port): if o == NULL || checkType(o, OBJ_LIST): return
    let len: u64 = 0; // TODO(port): list_type_length(o)
    ctx.reply_integer(len as i64)
}

/// LINDEX <key> <index>
///
/// C: `lindexCommand` — t_list.c:571-596
pub fn lindex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(architect): o = ctx.db().lookup_key_read_or_reply_null(argv[1])
    // TODO(port): if o == NULL || checkType(o, OBJ_LIST): return
    // TODO(port): index = getLongFromObjectOrReply(argv[2])?
    let index: i64 = 0; // TODO(port): parse from argv[2]
    // let mut iter = list_type_init_iterator(o, index, ListPosition::Tail)?;
    // let mut entry = ListTypeEntry::new(iter.encoding, iter.direction);
    // if list_type_next(&mut iter, o, &mut entry) {
    //   match list_type_get_value(&entry) {
    //     Bytes(b)  => ctx.reply_bulk(&b)?,
    //     Integer(n) => ctx.reply_integer(n)?,
    //   }
    // } else { ctx.reply_null()? }
    // list_type_release_iterator(iter);
    let _ = index;
    ctx.reply_null()
}

/// LSET <key> <index> <element>
///
/// C: `lsetCommand` — t_list.c:599-620
pub fn lset_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(architect): o = ctx.db_mut().lookup_key_write_or_reply_nokeyerr(argv[1])
    // TODO(port): if o == NULL || checkType(o, OBJ_LIST): return
    // TODO(port): index = getLongFromObjectOrReply(argv[2])?
    let index: i64 = 0; // TODO(port): parse from argv[2]
    // TODO(port): list_type_try_conversion_append(o, argv, 3, 3, None)?
    // if list_type_replace_at_index(o, index, argv[3])? {
    //   list_type_try_conversion(o, ListConvType::Shrinking, None)?
    //   signal_modified; notify lset; dirty++; reply OK
    // } else {
    //   return Err(RedisError::out_of_range())
    // }
    let _ = index;
    ctx.reply_simple_string(b"OK") // TODO(port): replace with full implementation
}

/// LPOP <key> [count]
pub fn lpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    pop_generic_command(ctx, ListPosition::Head)
}

/// RPOP <key> [count]
pub fn rpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    pop_generic_command(ctx, ListPosition::Tail)
}

/// LRANGE <key> <start> <stop>
///
/// C: `lrangeCommand` — t_list.c:854-865
pub fn lrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): start = getLongFromObjectOrReply(argv[2])?
    // TODO(port): end   = getLongFromObjectOrReply(argv[3])?
    let start: i64 = 0; // TODO(port): parse
    let end: i64 = -1; // TODO(port): parse
    // TODO(architect): o = ctx.db().lookup_key_read_or_reply_emptyarray(argv[1])
    // TODO(port): if checkType(o, OBJ_LIST): return
    let o: Option<&RedisObject> = None; // TODO(port): real DB lookup
    match o {
        None => ctx.reply_empty_array(),
        Some(o) => add_list_range_reply(ctx, o, start, end, false),
    }
}

/// LTRIM <key> <start> <stop>
///
/// C: `ltrimCommand` — t_list.c:868-918
pub fn ltrim_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): start = getLongFromObjectOrReply(argv[2])?
    // TODO(port): end   = getLongFromObjectOrReply(argv[3])?
    let mut start: i64 = 0; // TODO(port): parse
    let mut end: i64 = -1; // TODO(port): parse

    // TODO(architect): o = ctx.db_mut().lookup_key_write_or_reply_ok(argv[1])
    // TODO(port): if checkType(o, OBJ_LIST): return
    let o: Option<&mut RedisObject> = None; // TODO(port): real DB lookup
    let o = match o {
        None => return ctx.reply_simple_string(b"OK"),
        Some(o) => o,
    };

    let llen = list_type_length(o) as i64;
    if start < 0 {
        start += llen;
    }
    if end < 0 {
        end += llen;
    }
    if start < 0 {
        start = 0;
    }

    let (ltrim, rtrim) = if start > end || start >= llen {
        (llen, 0i64)
    } else {
        if end >= llen {
            end = llen - 1;
        }
        (start, llen - end - 1)
    };

    // TODO(port): match encoding {
    //   Quicklist => { quicklistDelRange(ql, 0, ltrim); quicklistDelRange(ql, -rtrim, rtrim) }
    //   Listpack  => { lp = lpDeleteRange(lp, 0, ltrim); lp = lpDeleteRange(lp, -rtrim, rtrim) }
    // }
    // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, b"ltrim", argv[1], db_id)

    if list_type_length(o) == 0 {
        // TODO(architect): ctx.db_mut().delete(argv[1])
        // TODO(architect): ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", argv[1], db_id)
    } else {
        list_type_try_conversion(o, ListConvType::Shrinking, None)?;
    }

    let total_trim = ltrim + rtrim;
    if total_trim > 0 {
        // TODO(architect): ctx.signal_modified_key(argv[1])
    }
    // TODO(architect): ctx.server_dirty_add(total_trim)
    ctx.reply_simple_string(b"OK")
}

/// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
///
/// Returns the index of the first/Nth matching element, or an array of
/// matching indexes when COUNT is specified.
///
/// C: `lposCommand` — t_list.c:937-1025
pub fn lpos_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None; // None = COUNT option not given
    let mut maxlen: i64 = 0;

    let mut j = 3usize;
    while j < ctx.argc() {
        let opt = ctx.arg(j)?.to_vec();
        let moreargs = ctx.argc().saturating_sub(1 + j);
        if opt.eq_ignore_ascii_case(b"RANK") && moreargs > 0 {
            j += 1;
            // TODO(port): getRangeLongFromObjectOrReply(argv[j], -LONG_MAX, LONG_MAX, &mut rank)?
            rank = 1; // TODO(port): parse actual value; placeholder keeps rank non-zero
            if rank == 0 {
                return Err(RedisError::runtime(
                    b"RANK can't be zero: use 1 to start from \
                      the first match, 2 from the second ... \
                      or use negative to start from the end of the list",
                ));
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") && moreargs > 0 {
            j += 1;
            // TODO(port): getPositiveLongFromObjectOrReply(argv[j], "COUNT can't be negative")?
            count = Some(0); // TODO(port): parse actual value
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") && moreargs > 0 {
            j += 1;
            // TODO(port): getPositiveLongFromObjectOrReply(argv[j], "MAXLEN can't be negative")?
            maxlen = 0; // TODO(port): parse actual value
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
        j += 1;
    }

    let direction = if rank < 0 {
        rank = -rank;
        ListPosition::Head
    } else {
        ListPosition::Tail
    };

    // TODO(architect): o = ctx.db().lookup_key_read(argv[1])
    let o: Option<&RedisObject> = None; // TODO(port): real DB lookup
    if o.is_none() {
        return if count.is_some() {
            ctx.reply_empty_array()
        } else {
            ctx.reply_null()
        };
    }
    // TODO(port): if checkType(o, OBJ_LIST): return

    // TODO(architect): if count.is_some(): arraylenptr = ctx.reply_deferred_len()
    // TODO(port): full iteration / match tracking:
    //   start_idx = if direction == Head { -1i64 } else { 0i64 }
    //   let mut li = list_type_init_iterator(o, start_idx, direction)?
    //   let llen = list_type_length(o) as i64
    //   let (mut index, mut matches, mut matchindex, mut arraylen) = (0i64, 0i64, -1i64, 0i64)
    //   while list_type_next(&mut li, o, &mut entry) && (maxlen == 0 || index < maxlen) {
    //     if list_type_equal(&entry, argv[2]) {
    //       matches += 1
    //       matchindex = if direction==Tail { index } else { llen - index - 1 }
    //       if matches >= rank {
    //         if count.is_some() {
    //           arraylen += 1; ctx.reply_integer(matchindex)?
    //           if count.unwrap() > 0 && matches - rank + 1 >= count.unwrap() { break }
    //         } else { break }
    //       }
    //     }
    //     index += 1; matchindex = -1
    //   }
    //   list_type_release_iterator(li)
    //   if count.is_some() { ctx.set_deferred_array_len(arraylenptr, arraylen) }
    //   else { if matchindex != -1 { reply_integer(matchindex) } else { reply_null() } }
    let _ = (rank, maxlen, direction, o);
    ctx.reply_null()
}

/// LREM <key> <count> <element>
///
/// Removes up to abs(count) occurrences of `element`.  Positive count
/// removes from head; negative count removes from tail; zero removes all.
///
/// C: `lremCommand` — t_list.c:1028-1070
pub fn lrem_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): toremove = getRangeLongFromObjectOrReply(argv[2], -LONG_MAX, LONG_MAX)?
    let mut toremove: i64 = 0; // TODO(port): parse from argv[2]

    // TODO(architect): subject = ctx.db_mut().lookup_key_write_or_reply_zero(argv[1])
    // TODO(port): if subject == NULL || checkType(subject, OBJ_LIST): return
    let subject: Option<&mut RedisObject> = None; // TODO(port): real DB lookup
    let subject = match subject {
        None => return ctx.reply_integer(0),
        Some(s) => s,
    };

    let (iter_direction, start_idx) = if toremove < 0 {
        toremove = -toremove;
        (ListPosition::Head, -1i64)
    } else {
        (ListPosition::Tail, 0i64)
    };

    let mut iter = list_type_init_iterator(subject, start_idx, iter_direction)?;
    let mut entry = ListTypeEntry::new(iter.encoding, iter.direction);
    let mut removed: i64 = 0;

    while list_type_next(&mut iter, subject, &mut entry) {
        // TODO(port): if list_type_equal(&entry, argv[3]):
        //   list_type_delete(&mut iter, subject, &mut entry)?
        //   ctx.server_dirty_incr(); removed += 1
        //   if toremove != 0 && removed == toremove: break
        break; // TODO(port): remove placeholder break once iteration body is implemented
    }
    list_type_release_iterator(iter);

    if removed > 0 {
        // TODO(architect): ctx.notify_keyspace_event(NOTIFY_LIST, b"lrem", argv[1], db_id)
        if list_type_length(subject) == 0 {
            // TODO(architect): ctx.db_mut().delete(argv[1])
            // TODO(architect): ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", argv[1], db_id)
        } else {
            list_type_try_conversion(subject, ListConvType::Shrinking, None)?;
        }
        // TODO(architect): ctx.signal_modified_key(argv[1])
    }
    ctx.reply_integer(removed)
}

/// LMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT)
///
/// C: `lmoveCommand` — t_list.c:1132-1137
pub fn lmove_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let wherefrom_arg = ctx.arg(3)?.to_vec();
    let whereto_arg = ctx.arg(4)?.to_vec();
    let wherefrom = get_list_position_from_object_or_reply(ctx, &wherefrom_arg)?;
    let whereto = get_list_position_from_object_or_reply(ctx, &whereto_arg)?;
    lmove_generic_command(ctx, wherefrom, whereto)
}

/// RPOPLPUSH <source> <destination>  (deprecated alias for LMOVE src dst TAIL LEFT)
///
/// C: `rpoplpushCommand` — t_list.c:1154-1156
pub fn rpoplpush_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lmove_generic_command(ctx, ListPosition::Tail, ListPosition::Head)
}

/// BLPOP <key> [<key> ...] <timeout>
///
/// C: `blpopCommand` — t_list.c:1227-1229
pub fn blpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    let keys: Vec<RedisString> = (1..argc - 1)
        .filter_map(|i| ctx.arg(i).ok().map(|a| RedisString::from_bytes(a)))
        .collect();
    blocking_pop_generic_command(ctx, &keys, ListPosition::Head, argc - 1, None)
}

/// BRPOP <key> [<key> ...] <timeout>
///
/// C: `brpopCommand` — t_list.c:1232-1234
pub fn brpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    let keys: Vec<RedisString> = (1..argc - 1)
        .filter_map(|i| ctx.arg(i).ok().map(|a| RedisString::from_bytes(a)))
        .collect();
    blocking_pop_generic_command(ctx, &keys, ListPosition::Tail, argc - 1, None)
}

/// BLMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) <timeout>
///
/// C: `blmoveCommand` — t_list.c:1258-1265
pub fn blmove_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let wherefrom_arg = ctx.arg(3)?.to_vec();
    let whereto_arg = ctx.arg(4)?.to_vec();
    let wherefrom = get_list_position_from_object_or_reply(ctx, &wherefrom_arg)?;
    let whereto = get_list_position_from_object_or_reply(ctx, &whereto_arg)?;
    // TODO(port): timeout = getTimeoutFromObjectOrReply(argv[5], UNIT_SECONDS)?
    let timeout: i64 = 0; // TODO(port): parse actual timeout
    blmove_generic_command(ctx, wherefrom, whereto, timeout)
}

/// BRPOPLPUSH <source> <destination> <timeout>  (deprecated)
///
/// C: `brpoplpushCommand` — t_list.c:1268-1272
pub fn brpoplpush_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): timeout = getTimeoutFromObjectOrReply(argv[3], UNIT_SECONDS)?
    let timeout: i64 = 0; // TODO(port): parse actual timeout
    blmove_generic_command(ctx, ListPosition::Tail, ListPosition::Head, timeout)
}

/// LMPOP numkeys <key> [<key> ...] (LEFT|RIGHT) [COUNT count]
///
/// C: `lmpopCommand` — t_list.c:1325-1327
pub fn lmpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lmpop_generic_command(ctx, 1, false)
}

/// BLMPOP timeout numkeys <key> [<key> ...] (LEFT|RIGHT) [COUNT count]
///
/// C: `blmpopCommand` — t_list.c:1330-1332
pub fn blmpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lmpop_generic_command(ctx, 2, true)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_list.c  (1333 lines, 59 functions)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         62
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Syntax-checked clean (only expected E0432/E0433/E0282
//                  name-resolution errors).  Logic structure is faithful to
//                  the C source throughout.  All cross-crate dependencies
//                  (QuickList/ListPack from redis-ds Phase 4, server config,
//                  DB lookup, blocking infra, deferred-reply protocol, command
//                  rewriting for replication) are stubbed with
//                  TODO(port)/TODO(architect).  The C iterator's back-reference
//                  to robj* is resolved by passing subject explicitly — a PORT
//                  NOTE divergence that Phase B must confirm or redesign.
//                  Zero unsafe blocks.
// ──────────────────────────────────────────────────────────────────────────

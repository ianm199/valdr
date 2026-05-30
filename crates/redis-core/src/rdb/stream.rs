//! RDB stream type serialization — Round 23.
//!
//! Implements `save_stream_object` and `load_stream_object_{2,3}` for the
//! `RDB_TYPE_STREAM_LISTPACKS_{2,3}` wire forms. We always emit type 21
//! (`_3`, with consumer `active_time`). On load we accept type 21 and
//! type 19, mapping the missing `active_time` to `seen_time` for `_2`.
//!
//! Wire layout after the type byte (mirrors `rdbSaveObject` in rdb.c:1033–1142):
//!
//! 1. `save_len(listpacks_count)` — number of listpack nodes (= stream length)
//! 2. For each listpack node:
//!    - 16-byte raw stream ID written as an RDB string (`save_string`)
//!    - the listpack blob written as an RDB string (`save_string`)
//! 3. `save_len(length)`             — number of entries in the stream
//! 4. `save_len(last_id.ms)`         — last ID, ms component
//! 5. `save_len(last_id.seq)`        — last ID, seq component
//! 6. `save_len(first_id.ms)`        — `_2`+ only
//! 7. `save_len(first_id.seq)`       — `_2`+ only
//! 8. `save_len(max_deleted_id.ms)`  — `_2`+ only
//! 9. `save_len(max_deleted_id.seq)` — `_2`+ only
//! 10. `save_len(entries_added)`     — `_2`+ only
//! 11. `save_len(num_groups)`
//! 12. For each group:
//!     - `save_string(name)`
//!     - `save_len(last_delivered_id.ms)`
//!     - `save_len(last_delivered_id.seq)`
//!     - `save_len(entries_read)`    — `_2`+ only
//!     - Group PEL:
//!         - `save_len(pel_size)`
//!         - per-entry: 16 raw bytes id + 8 LE bytes delivery_time + `save_len(delivery_count)`
//!     - Consumers:
//!         - `save_len(consumer_count)`
//!         - per-consumer: `save_string(name)` + 8 LE bytes seen_time
//!                       + 8 LE bytes active_time (`_3` only)
//!                       + consumer PEL: `save_len(pel_size)` + N * 16 raw bytes id
//!
//! Stream IDs in raw form are 16 bytes big-endian: `BE(ms) || BE(seq)`. This
//! matches `streamEncodeID` in t_stream.c and is the rax key format used by
//! both per-listpack-node keys and PEL entry keys.
//!
//! Per-entry listpack layout (one entry per node — operator-confirmed MVP):
//!   Primary master header entries (all listpack INTEGERS except field names):
//!     count          int = 1
//!     deleted        int = 0
//!     num-fields N   int
//!     field-name-1   string
//!     ...
//!     field-name-N   string
//!     0              int (terminator)
//!   Single entry record (`SAMEFIELDS` form):
//!     flags          int = 0x02
//!     ms-delta       int = 0
//!     seq-delta      int = 0
//!     value-1        string
//!     ...
//!     value-N        string
//!     lp-count       int = N + 3
//!
//! On load we walk the listpack body, distinguishing the master header from
//! delta records via the count/deleted/num-fields prefix and reconstructing
//! `StreamEntry` instances by applying ms-delta/seq-delta to the node's
//! primary ID. Our save format always produces single-entry nodes, but the
//! decoder handles N-entry nodes for compatibility with files produced by
//! real Valkey servers running with the default `stream-node-max-entries`.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use redis_types::RedisString;

use redis_ds::stream::{Consumer, ConsumerGroup, InlineStream, PelEntry, StreamEntry, StreamId};

use crate::object::RedisObject;

use super::header::{read_rdb_string, write_rdb_string};
use super::listpack::{decode_listpack, ListpackBuilder};
use super::varint::{load_len, write_len};

#[allow(dead_code)] // stream RDB format flag; used when full stream serialization is wired
const STREAM_ITEM_FLAG_NONE: i64 = 0;
const STREAM_ITEM_FLAG_DELETED: i64 = 1 << 0;
const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 1 << 1;

/// Serialize a stream `RedisObject` as `RDB_TYPE_STREAM_LISTPACKS_3`.
///
/// The caller writes the type byte and key beforehand; this function writes
/// the value payload only. Empty streams (with or without groups) are
/// preserved by emitting `listpacks_count = 0` and `length = 0`, then the
/// remaining metadata and groups exactly as for non-empty streams.
pub fn save_stream_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let s = obj.stream().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "save_stream_object called on non-stream object",
        )
    })?;

    write_len(w, s.entries.len() as u64)?;

    for entry in &s.entries {
        let raw_id = encode_raw_id(&entry.id);
        write_rdb_string(w, &raw_id)?;
        let lp = encode_entry_as_listpack(entry);
        write_rdb_string(w, &lp)?;
    }

    write_len(w, s.entries.len() as u64)?;
    write_len(w, s.last_id.ms)?;
    write_len(w, s.last_id.seq)?;

    let first_id = derive_first_id(s);
    write_len(w, first_id.ms)?;
    write_len(w, first_id.seq)?;

    write_len(w, s.max_deleted_id.ms)?;
    write_len(w, s.max_deleted_id.seq)?;

    write_len(w, s.entries_added)?;

    write_len(w, s.groups.len() as u64)?;
    for (name, group) in &s.groups {
        write_rdb_string(w, name.as_bytes())?;
        write_len(w, group.last_delivered_id.ms)?;
        write_len(w, group.last_delivered_id.seq)?;
        write_len(w, group.entries_read as u64)?;

        write_len(w, group.pel.len() as u64)?;
        for entry in &group.pel {
            w.write_all(&encode_raw_id(&entry.entry_id))?;
            w.write_all(&entry.delivery_time_ms.to_le_bytes())?;
            write_len(w, entry.delivery_count)?;
        }

        write_len(w, group.consumers.len() as u64)?;
        for consumer in group.consumers.values() {
            write_rdb_string(w, consumer.name.as_bytes())?;
            w.write_all(&consumer.seen_time_ms.to_le_bytes())?;
            w.write_all(&consumer.active_time_ms.to_le_bytes())?;
            write_len(w, consumer.pel.len() as u64)?;
            for entry in &consumer.pel {
                w.write_all(&encode_raw_id(&entry.entry_id))?;
            }
        }
    }

    Ok(())
}

/// Load `RDB_TYPE_STREAM_LISTPACKS_3` (consumer `active_time` present).
pub fn load_stream_object_3(r: &mut impl Read) -> io::Result<RedisObject> {
    load_stream_inner(r, true, true)
}

/// Load `RDB_TYPE_STREAM_LISTPACKS` (legacy Redis <= 6.2, rdb_ver < 10): no
/// first_id / max_deleted_id / entries_added, no per-group entries_read, and
/// no per-consumer active_time. Reuses the same listpack node decoder; the
/// absent metadata is defaulted (see `load_stream_inner`).
pub fn load_stream_object_legacy(r: &mut impl Read) -> io::Result<RedisObject> {
    load_stream_inner(r, false, false)
}

/// Load `RDB_TYPE_STREAM_LISTPACKS_2` (no consumer `active_time`).
///
/// `active_time` is initialised from `seen_time` on each consumer to match
/// the fallback `rdbLoadObject` performs at rdb.c:2828.
pub fn load_stream_object_2(r: &mut impl Read) -> io::Result<RedisObject> {
    load_stream_inner(r, true, false)
}

fn load_stream_inner(
    r: &mut impl Read,
    has_v2_fields: bool,
    has_active_time: bool,
) -> io::Result<RedisObject> {
    let mut stream = InlineStream::new();

    let (listpacks_count, _) = load_len(r)?;
    for _ in 0..listpacks_count {
        let node_key = read_rdb_string(r)?;
        if node_key.len() != 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stream node key must be 16 bytes (BE ms || BE seq), got {}",
                    node_key.len()
                ),
            ));
        }
        let node_id = decode_raw_id(&node_key)?;
        let blob = read_rdb_string(r)?;
        let mut decoded = decode_entries_from_listpack(&blob, &node_id)?;
        stream.entries.append(&mut decoded);
    }

    let (length, _) = load_len(r)?;
    if length as usize != stream.entries.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stream length {} disagrees with decoded entries count {}",
                length,
                stream.entries.len()
            ),
        ));
    }

    let (last_ms, _) = load_len(r)?;
    let (last_seq, _) = load_len(r)?;
    stream.last_id = StreamId::new(last_ms, last_seq);

    if has_v2_fields {
        let (_first_ms, _) = load_len(r)?;
        let (_first_seq, _) = load_len(r)?;

        let (max_del_ms, _) = load_len(r)?;
        let (max_del_seq, _) = load_len(r)?;
        stream.max_deleted_id = StreamId::new(max_del_ms, max_del_seq);

        let (entries_added, _) = load_len(r)?;
        stream.entries_added = entries_added;
    } else {
        // Legacy RDB_TYPE_STREAM_LISTPACKS (Redis <= 6.2, rdb_ver < 10) has no
        // first_id / max_deleted_id / entries_added. Default the tombstone to
        // 0-0 and seed entries_added from the current length, matching the
        // legacy path in C `rdbLoadObject`.
        stream.max_deleted_id = StreamId::new(0, 0);
        stream.entries_added = stream.entries.len() as u64;
    }

    let (num_groups, _) = load_len(r)?;
    for _ in 0..num_groups {
        let group_name_bytes = read_rdb_string(r)?;
        let group_name = RedisString::from_vec(group_name_bytes);
        let (cg_ms, _) = load_len(r)?;
        let (cg_seq, _) = load_len(r)?;
        let entries_read = if has_v2_fields {
            load_len(r)?.0 as i64
        } else {
            stream
                .lag_view()
                .estimate_entries_read(StreamId::new(cg_ms, cg_seq))
        };

        let mut group = ConsumerGroup::new(group_name.clone(), StreamId::new(cg_ms, cg_seq));
        group.entries_read = entries_read;

        let (pel_size, _) = load_len(r)?;
        let mut group_pel_meta: HashMap<StreamId, (i64, u64)> =
            HashMap::with_capacity(super::prealloc_capacity(pel_size));
        for _ in 0..pel_size {
            let raw = read_raw_id(r)?;
            let id = decode_raw_id(&raw)?;
            let delivery_time_ms = read_i64_le(r)?;
            let (delivery_count, _) = load_len(r)?;
            group_pel_meta.insert(id, (delivery_time_ms, delivery_count));
            group.pel.push(PelEntry {
                entry_id: id,
                delivery_time_ms,
                delivery_count,
            });
        }
        group.pel.sort_by_key(|p| p.entry_id);

        let (consumer_count, _) = load_len(r)?;
        for _ in 0..consumer_count {
            let consumer_name_bytes = read_rdb_string(r)?;
            let consumer_name = RedisString::from_vec(consumer_name_bytes);
            let seen_time_ms = read_i64_le(r)?;
            let active_time_ms = if has_active_time {
                read_i64_le(r)?
            } else {
                seen_time_ms
            };

            let mut consumer = Consumer::new(consumer_name.clone(), seen_time_ms);
            consumer.seen_time_ms = seen_time_ms;
            consumer.active_time_ms = active_time_ms;

            let (c_pel_size, _) = load_len(r)?;
            for _ in 0..c_pel_size {
                let raw = read_raw_id(r)?;
                let id = decode_raw_id(&raw)?;
                let (delivery_time_ms, delivery_count) =
                    *group_pel_meta.get(&id).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "consumer PEL entry {} not present in group PEL — corrupt stream",
                                id.to_display_string()
                            ),
                        )
                    })?;
                consumer.pel.push(PelEntry {
                    entry_id: id,
                    delivery_time_ms,
                    delivery_count,
                });
            }
            consumer.pel.sort_by_key(|p| p.entry_id);
            group.consumers.insert(consumer_name, consumer);
        }

        stream.groups.insert(group_name, group);
    }

    Ok(make_stream_object(stream))
}

/// Encode a single `StreamEntry` as a 1-entry listpack node using the
/// `SAMEFIELDS` form. The resulting blob is suitable for use as a rax-node
/// payload that C Valkey can load via `rdbLoadObject`.
pub fn encode_entry_as_listpack(entry: &StreamEntry) -> Vec<u8> {
    let n = entry.fields.len() as i64;
    let mut builder = ListpackBuilder::new();
    builder.append_int(1);
    builder.append_int(0);
    builder.append_int(n);
    for (field, _) in &entry.fields {
        builder.append_string(field.as_bytes());
    }
    builder.append_int(0);

    builder.append_int(STREAM_ITEM_FLAG_SAMEFIELDS);
    builder.append_int(0);
    builder.append_int(0);
    for (_, value) in &entry.fields {
        builder.append_string(value.as_bytes());
    }
    builder.append_int(n + 3);

    builder.finalize()
}

/// Decode the entries contained in a single listpack node, applying
/// ms-delta / seq-delta against the node's primary ID. Skips entries
/// marked with `STREAM_ITEM_FLAG_DELETED` (tombstones in delta-packed
/// nodes produced by real Valkey).
fn decode_entries_from_listpack(blob: &[u8], node_id: &StreamId) -> io::Result<Vec<StreamEntry>> {
    let raw = decode_listpack(blob)?;
    if raw.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty listpack inside stream node",
        ));
    }

    let mut cursor = 0usize;
    let _count = parse_int(&raw, &mut cursor, "master count")?;
    let _deleted = parse_int(&raw, &mut cursor, "master deleted")?;
    let master_num_fields = parse_int(&raw, &mut cursor, "master num-fields")?;
    if master_num_fields < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negative master num-fields in stream listpack",
        ));
    }

    let mut master_fields: Vec<RedisString> =
        Vec::with_capacity(super::prealloc_capacity(master_num_fields as u64));
    for _ in 0..master_num_fields {
        let field_bytes = take_bytes(&raw, &mut cursor, "master field name")?;
        master_fields.push(RedisString::from_vec(field_bytes));
    }
    let terminator = parse_int(&raw, &mut cursor, "master terminator")?;
    if terminator != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected master terminator 0, got {} (stream listpack corrupt)",
                terminator
            ),
        ));
    }

    let mut out: Vec<StreamEntry> = Vec::new();
    while cursor < raw.len() {
        let flags = parse_int(&raw, &mut cursor, "entry flags")?;
        let ms_delta = parse_int(&raw, &mut cursor, "entry ms-delta")?;
        let seq_delta = parse_int(&raw, &mut cursor, "entry seq-delta")?;

        let is_samefields = (flags & STREAM_ITEM_FLAG_SAMEFIELDS) != 0;
        let is_deleted = (flags & STREAM_ITEM_FLAG_DELETED) != 0;

        let mut paired: Vec<(RedisString, RedisString)> = Vec::new();
        if is_samefields {
            for field in &master_fields {
                let v = take_bytes(&raw, &mut cursor, "entry value")?;
                paired.push((field.clone(), RedisString::from_vec(v)));
            }
        } else {
            let nf = parse_int(&raw, &mut cursor, "entry num-fields")?;
            if nf < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "negative entry num-fields",
                ));
            }
            for _ in 0..nf {
                let field = take_bytes(&raw, &mut cursor, "entry field name")?;
                let value = take_bytes(&raw, &mut cursor, "entry value")?;
                paired.push((RedisString::from_vec(field), RedisString::from_vec(value)));
            }
        }

        let _lp_count = parse_int(&raw, &mut cursor, "entry lp-count")?;

        if !is_deleted {
            let id = StreamId::new(
                node_id.ms.wrapping_add(ms_delta as u64),
                node_id.seq.wrapping_add(seq_delta as u64),
            );
            out.push(StreamEntry { id, fields: paired });
        }
    }

    Ok(out)
}

fn parse_int(raw: &[Vec<u8>], cursor: &mut usize, what: &str) -> io::Result<i64> {
    let bytes = take_bytes(raw, cursor, what)?;
    let s = core::str::from_utf8(&bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("non-UTF-8 integer in listpack ({what})"),
        )
    })?;
    s.parse::<i64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("non-numeric integer in listpack ({what}): {s:?}"),
        )
    })
}

fn take_bytes(raw: &[Vec<u8>], cursor: &mut usize, what: &str) -> io::Result<Vec<u8>> {
    if *cursor >= raw.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("listpack underrun while reading {what}"),
        ));
    }
    let v = raw[*cursor].clone();
    *cursor += 1;
    Ok(v)
}

fn encode_raw_id(id: &StreamId) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&id.ms.to_be_bytes());
    buf[8..16].copy_from_slice(&id.seq.to_be_bytes());
    buf
}

fn decode_raw_id(bytes: &[u8]) -> io::Result<StreamId> {
    if bytes.len() != 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raw stream id must be 16 bytes, got {}", bytes.len()),
        ));
    }
    let ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let seq = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok(StreamId::new(ms, seq))
}

fn read_raw_id(r: &mut impl Read) -> io::Result<[u8; 16]> {
    let mut buf = [0u8; 16];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_i64_le(r: &mut impl Read) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

fn derive_first_id(s: &InlineStream) -> StreamId {
    if let Some(first) = s.entries.first() {
        first.id
    } else {
        StreamId::ZERO
    }
}

fn make_stream_object(stream: InlineStream) -> RedisObject {
    let mut obj = RedisObject::new_stream();
    if let Some(slot) = obj.stream_mut() {
        *slot = stream;
    }
    obj
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn rs(s: &str) -> RedisString {
        RedisString::from_bytes(s.as_bytes())
    }

    fn make_entry(ms: u64, seq: u64, fields: &[(&str, &str)]) -> StreamEntry {
        StreamEntry {
            id: StreamId::new(ms, seq),
            fields: fields.iter().map(|(f, v)| (rs(f), rs(v))).collect(),
        }
    }

    fn roundtrip(stream: InlineStream) -> InlineStream {
        let obj = make_stream_object(stream);
        let mut buf: Vec<u8> = Vec::new();
        save_stream_object(&mut buf, &obj).unwrap();
        let mut cur = Cursor::new(&buf);
        let loaded = load_stream_object_3(&mut cur).unwrap();
        loaded.stream().unwrap().clone()
    }

    #[test]
    fn entry_listpack_roundtrip() {
        let entry = make_entry(123, 4, &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")]);
        let blob = encode_entry_as_listpack(&entry);
        let decoded = decode_entries_from_listpack(&blob, &entry.id).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].id, entry.id);
        assert_eq!(decoded[0].fields.len(), 3);
        assert_eq!(decoded[0].fields[0].0.as_bytes(), b"f1");
        assert_eq!(decoded[0].fields[0].1.as_bytes(), b"v1");
        assert_eq!(decoded[0].fields[2].0.as_bytes(), b"f3");
        assert_eq!(decoded[0].fields[2].1.as_bytes(), b"v3");
    }

    #[test]
    fn empty_stream_roundtrip() {
        let stream = InlineStream::new();
        let result = roundtrip(stream);
        assert!(result.entries.is_empty());
        assert_eq!(result.last_id, StreamId::ZERO);
        assert_eq!(result.entries_added, 0);
        assert!(result.groups.is_empty());
    }

    #[test]
    fn three_entry_roundtrip() {
        let mut stream = InlineStream::new();
        stream.append(make_entry(1, 0, &[("a", "1")]));
        stream.append(make_entry(2, 0, &[("b", "2"), ("c", "3")]));
        stream.append(make_entry(3, 5, &[("x", "hello world")]));
        let result = roundtrip(stream);
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.last_id, StreamId::new(3, 5));
        assert_eq!(result.entries_added, 3);
        assert_eq!(result.entries[0].id, StreamId::new(1, 0));
        assert_eq!(result.entries[1].fields.len(), 2);
        assert_eq!(result.entries[2].fields[0].1.as_bytes(), b"hello world");
    }

    #[test]
    fn empty_stream_with_group_roundtrip() {
        let mut stream = InlineStream::new();
        let group_name = rs("g1");
        stream.groups.insert(
            group_name.clone(),
            ConsumerGroup::new(group_name.clone(), StreamId::new(0, 0)),
        );
        let result = roundtrip(stream);
        assert!(result.entries.is_empty());
        assert_eq!(result.groups.len(), 1);
        assert!(result.groups.contains_key(&group_name));
    }

    #[test]
    fn group_with_pel_and_consumers_roundtrip() {
        let mut stream = InlineStream::new();
        stream.append(make_entry(10, 0, &[("k", "v")]));
        stream.append(make_entry(20, 0, &[("k", "v")]));

        let group_name = rs("workers");
        let mut group = ConsumerGroup::new(group_name.clone(), StreamId::new(20, 0));
        group.entries_read = 2;

        let id1 = StreamId::new(10, 0);
        let id2 = StreamId::new(20, 0);
        group.pel.push(PelEntry {
            entry_id: id1,
            delivery_time_ms: 1_700_000_000_000,
            delivery_count: 1,
        });
        group.pel.push(PelEntry {
            entry_id: id2,
            delivery_time_ms: 1_700_000_001_000,
            delivery_count: 3,
        });

        let consumer_a_name = rs("a");
        let mut consumer_a = Consumer::new(consumer_a_name.clone(), 1_700_000_000_000);
        consumer_a.active_time_ms = 1_700_000_000_500;
        consumer_a.pel.push(PelEntry {
            entry_id: id1,
            delivery_time_ms: 1_700_000_000_000,
            delivery_count: 1,
        });

        let consumer_b_name = rs("b");
        let mut consumer_b = Consumer::new(consumer_b_name.clone(), 1_700_000_001_000);
        consumer_b.active_time_ms = 1_700_000_001_500;
        consumer_b.pel.push(PelEntry {
            entry_id: id2,
            delivery_time_ms: 1_700_000_001_000,
            delivery_count: 3,
        });

        group.consumers.insert(consumer_a_name.clone(), consumer_a);
        group.consumers.insert(consumer_b_name.clone(), consumer_b);
        stream.groups.insert(group_name.clone(), group);

        let result = roundtrip(stream);
        let g = result.groups.get(&group_name).unwrap();
        assert_eq!(g.entries_read, 2);
        assert_eq!(g.pel.len(), 2);
        assert_eq!(g.pel[0].entry_id, id1);
        assert_eq!(g.pel[0].delivery_count, 1);
        assert_eq!(g.pel[1].entry_id, id2);
        assert_eq!(g.pel[1].delivery_count, 3);

        let c_a = g.consumers.get(&consumer_a_name).unwrap();
        assert_eq!(c_a.seen_time_ms, 1_700_000_000_000);
        assert_eq!(c_a.active_time_ms, 1_700_000_000_500);
        assert_eq!(c_a.pel.len(), 1);
        assert_eq!(c_a.pel[0].entry_id, id1);
        assert_eq!(c_a.pel[0].delivery_time_ms, 1_700_000_000_000);
        assert_eq!(c_a.pel[0].delivery_count, 1);

        let c_b = g.consumers.get(&consumer_b_name).unwrap();
        assert_eq!(c_b.pel[0].entry_id, id2);
        assert_eq!(c_b.pel[0].delivery_count, 3);
    }

    #[test]
    fn raw_id_roundtrip_be_format() {
        let id = StreamId::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let bytes = encode_raw_id(&id);
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[7], 0x08);
        assert_eq!(bytes[8], 0x09);
        assert_eq!(bytes[15], 0x10);
        let back = decode_raw_id(&bytes).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn load_v2_sets_active_time_from_seen_time() {
        let mut stream = InlineStream::new();
        let group_name = rs("g1");
        let mut group = ConsumerGroup::new(group_name.clone(), StreamId::ZERO);
        let consumer_name = rs("c1");
        let consumer = Consumer::new(consumer_name.clone(), 1_700_000_000_000);
        group.consumers.insert(consumer_name.clone(), consumer);
        stream.groups.insert(group_name.clone(), group);

        let obj = make_stream_object(stream);
        let mut v3_buf: Vec<u8> = Vec::new();
        save_stream_object(&mut v3_buf, &obj).unwrap();
        let v2_buf = strip_active_time_from_v3(&v3_buf);

        let mut cur = Cursor::new(&v2_buf);
        let loaded = load_stream_object_2(&mut cur).unwrap();
        let g = loaded.stream().unwrap().groups.get(&group_name).unwrap();
        let c = g.consumers.get(&consumer_name).unwrap();
        assert_eq!(c.active_time_ms, c.seen_time_ms);
    }

    fn strip_active_time_from_v3(v3: &[u8]) -> Vec<u8> {
        let mut cur = Cursor::new(v3);
        let mut out: Vec<u8> = Vec::new();

        fn read_len(cur: &mut Cursor<&[u8]>, out: &mut Vec<u8>) -> u64 {
            let start = cur.position() as usize;
            let (n, _) = load_len(cur).unwrap();
            let end = cur.position() as usize;
            out.extend_from_slice(&cur.get_ref()[start..end]);
            n
        }

        fn read_string(cur: &mut Cursor<&[u8]>, out: &mut Vec<u8>) {
            let start = cur.position() as usize;
            let _ = read_rdb_string(cur).unwrap();
            let end = cur.position() as usize;
            out.extend_from_slice(&cur.get_ref()[start..end]);
        }

        fn copy_bytes(cur: &mut Cursor<&[u8]>, out: &mut Vec<u8>, n: usize) {
            let start = cur.position() as usize;
            cur.set_position((start + n) as u64);
            out.extend_from_slice(&cur.get_ref()[start..start + n]);
        }

        fn skip_bytes(cur: &mut Cursor<&[u8]>, n: usize) {
            let start = cur.position() as usize;
            cur.set_position((start + n) as u64);
        }

        let listpacks = read_len(&mut cur, &mut out);
        for _ in 0..listpacks {
            read_string(&mut cur, &mut out);
            read_string(&mut cur, &mut out);
        }
        let _length = read_len(&mut cur, &mut out);
        let _last_ms = read_len(&mut cur, &mut out);
        let _last_seq = read_len(&mut cur, &mut out);
        let _first_ms = read_len(&mut cur, &mut out);
        let _first_seq = read_len(&mut cur, &mut out);
        let _max_del_ms = read_len(&mut cur, &mut out);
        let _max_del_seq = read_len(&mut cur, &mut out);
        let _entries_added = read_len(&mut cur, &mut out);

        let ngroups = read_len(&mut cur, &mut out);
        for _ in 0..ngroups {
            read_string(&mut cur, &mut out);
            read_len(&mut cur, &mut out);
            read_len(&mut cur, &mut out);
            read_len(&mut cur, &mut out);

            let pel_size = read_len(&mut cur, &mut out);
            for _ in 0..pel_size {
                copy_bytes(&mut cur, &mut out, 16);
                copy_bytes(&mut cur, &mut out, 8);
                read_len(&mut cur, &mut out);
            }

            let nconsumers = read_len(&mut cur, &mut out);
            for _ in 0..nconsumers {
                read_string(&mut cur, &mut out);
                copy_bytes(&mut cur, &mut out, 8);
                skip_bytes(&mut cur, 8);
                let c_pel_size = read_len(&mut cur, &mut out);
                for _ in 0..c_pel_size {
                    copy_bytes(&mut cur, &mut out, 16);
                }
            }
        }

        out
    }
}

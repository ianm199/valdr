//! `Ziplist` - legacy compact list/hash encoding superseded by listpack.
//!
//! Ziplists remain relevant for backward-compatible RDB loading: older RDB
//! files can store small lists and hashes as contiguous ziplist blobs. This
//! module owns a safe, read-only byte-buffer decoder for those legacy blobs.
//! Mutating operations (`push`, `insert`, `delete`, `replace`, `merge`) are
//! intentionally left out until a packet needs live ziplist writes.

pub const ZIPLIST_HEAD: usize = 0;
pub const ZIPLIST_TAIL: usize = 1;

const ZIP_END: u8 = 255;
const ZIP_BIG_PREVLEN: u8 = 254;

const ZIP_STR_MASK: u8 = 0xc0;
const ZIP_STR_06B: u8 = 0 << 6;
const ZIP_STR_14B: u8 = 1 << 6;
const ZIP_STR_32B: u8 = 2 << 6;

const ZIP_INT_16B: u8 = 0xc0;
const ZIP_INT_32B: u8 = 0xc0 | (1 << 4);
const ZIP_INT_64B: u8 = 0xc0 | (2 << 4);
const ZIP_INT_24B: u8 = 0xc0 | (3 << 4);
const ZIP_INT_8B: u8 = 0xfe;

const ZIP_INT_IMM_MASK: u8 = 0x0f;
const ZIP_INT_IMM_MIN: u8 = 0xf1;
const ZIP_INT_IMM_MAX: u8 = 0xfd;

const ZIPLIST_HEADER_SIZE: usize = 4 + 4 + 2;
const ZIPLIST_END_SIZE: usize = 1;
const ZIPLIST_MAX_SAFETY_SIZE: usize = 1 << 30;
const LONG_STR_SIZE: usize = 21;

const INT24_MIN: i64 = -8_388_608;
const INT24_MAX: i64 = 8_388_607;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZiplistEntry {
    pub offset: usize,
    pub prev_raw_len_size: usize,
    pub prev_raw_len: usize,
    pub len_size: usize,
    pub len: usize,
    pub header_size: usize,
    pub encoding: u8,
}

impl ZiplistEntry {
    pub fn raw_len(self) -> usize {
        self.header_size + self.len
    }

    pub fn payload_offset(self) -> usize {
        self.offset + self.header_size
    }

    pub fn is_bytes(self) -> bool {
        zip_is_str(self.encoding)
    }

    pub fn is_integer(self) -> bool {
        !self.is_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZiplistValue<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedZiplistValue {
    Bytes(Vec<u8>),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ziplist {
    data: Vec<u8>,
}

impl Ziplist {
    /// Create a new empty ziplist.
    pub fn new() -> Self {
        let size = ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE;
        let mut data = vec![0; size];
        write_u32_le(&mut data, 0, size as u32);
        write_u32_le(&mut data, 4, ZIPLIST_HEADER_SIZE as u32);
        write_u16_le(&mut data, 8, 0);
        data[ZIPLIST_HEADER_SIZE] = ZIP_END;
        Self { data }
    }

    /// Build a `Ziplist` from a raw Valkey ziplist blob.
    ///
    /// The blob must pass deep integrity validation. This keeps all public
    /// cursor APIs non-panicking even when the caller is loading old RDB bytes.
    pub fn from_raw_bytes(data: Vec<u8>) -> Option<Self> {
        if Self::validate_integrity(&data, true) {
            Some(Self { data })
        } else {
            None
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn blob_len(&self) -> usize {
        read_u32_le(&self.data, 0).map_or(0, |value| value as usize)
    }

    pub fn safe_to_add(&self, add: usize) -> bool {
        self.blob_len()
            .checked_add(add)
            .is_some_and(|len| len <= ZIPLIST_MAX_SAFETY_SIZE)
    }

    pub fn is_empty(&self) -> bool {
        self.first().is_none()
    }

    /// This read-only variant scans when the cached length is `u16::MAX`
    /// instead of mutating through an immutable receiver.
    pub fn len(&self) -> usize {
        match read_u16_le(&self.data, 8) {
            Some(count) if count != u16::MAX => count as usize,
            _ => count_entries(&self.data).unwrap_or(0),
        }
    }

    /// Source-shaped `ziplistLen` variant: scans and refreshes the cached
    /// header count when it fits in the 16-bit header field.
    pub fn length(&mut self) -> usize {
        if let Some(count) = read_u16_le(&self.data, 8) {
            if count != u16::MAX {
                return count as usize;
            }
        }

        let count = count_entries(&self.data).unwrap_or(0);
        if count < u16::MAX as usize {
            write_u16_le(&mut self.data, 8, count as u16);
        }
        count
    }

    /// Return the offset of the first entry, or `None` for an empty list.
    pub fn first(&self) -> Option<usize> {
        let end = self.end_offset()?;
        if self.data.get(ZIPLIST_HEADER_SIZE).copied()? == ZIP_END {
            return None;
        }
        if ZIPLIST_HEADER_SIZE >= end {
            return None;
        }
        decode_entry(&self.data, self.blob_len(), ZIPLIST_HEADER_SIZE, true)
            .map(|entry| entry.offset)
    }

    /// Return the offset of the last entry, or `None` for an empty list.
    pub fn last(&self) -> Option<usize> {
        let end = self.end_offset()?;
        let tail = read_u32_le(&self.data, 4)? as usize;
        if tail == end || self.data.get(tail).copied()? == ZIP_END {
            return None;
        }
        if tail < ZIPLIST_HEADER_SIZE || tail > end {
            return None;
        }
        decode_entry(&self.data, self.blob_len(), tail, true).map(|entry| entry.offset)
    }

    /// Return the entry offset at a positive or negative zero-based index.
    pub fn index(&self, index: isize) -> Option<usize> {
        if index >= 0 {
            let mut cursor = self.first()?;
            let remaining = usize::try_from(index).ok()?;
            for _ in 0..remaining {
                cursor = self.next(cursor)?;
            }
            Some(cursor)
        } else {
            let mut cursor = self.last()?;
            let mut remaining = usize::try_from(index.checked_neg()?.checked_sub(1)?).ok()?;
            while remaining > 0 {
                cursor = self.prev(cursor)?;
                remaining -= 1;
            }
            Some(cursor)
        }
    }

    pub fn entry_at_offset(&self, offset: usize) -> Option<ZiplistEntry> {
        if self.data.get(offset).copied()? == ZIP_END {
            return None;
        }
        decode_entry(&self.data, self.blob_len(), offset, true)
    }

    pub fn entry_at_index(&self, index: isize) -> Option<ZiplistEntry> {
        self.index(index)
            .and_then(|offset| self.entry_at_offset(offset))
    }

    pub fn next(&self, offset: usize) -> Option<usize> {
        if self.data.get(offset).copied()? == ZIP_END {
            return None;
        }
        let entry = self.entry_at_offset(offset)?;
        let next = entry.offset.checked_add(entry.raw_len())?;
        let end = self.end_offset()?;
        if next >= end || self.data.get(next).copied()? == ZIP_END {
            return None;
        }
        decode_entry(&self.data, self.blob_len(), next, true).map(|entry| entry.offset)
    }

    /// Passing the EOF offset returns the tail entry.
    pub fn prev(&self, offset: usize) -> Option<usize> {
        let end = self.end_offset()?;
        if offset == end || self.data.get(offset).copied()? == ZIP_END {
            return self.last();
        }
        if offset == ZIPLIST_HEADER_SIZE {
            return None;
        }
        let entry = self.entry_at_offset(offset)?;
        if entry.prev_raw_len == 0 {
            return None;
        }
        let prev = offset.checked_sub(entry.prev_raw_len)?;
        decode_entry(&self.data, self.blob_len(), prev, true).map(|entry| entry.offset)
    }

    pub fn get(&self, offset: usize) -> Option<ZiplistValue<'_>> {
        let entry = self.entry_at_offset(offset)?;
        self.get_entry(&entry)
    }

    pub fn get_entry(&self, entry: &ZiplistEntry) -> Option<ZiplistValue<'_>> {
        let payload_start = entry.payload_offset();
        let payload_end = payload_start.checked_add(entry.len)?;
        let end = self.end_offset()?;
        if payload_end > end {
            return None;
        }

        if entry.is_bytes() {
            Some(ZiplistValue::Bytes(
                self.data.get(payload_start..payload_end)?,
            ))
        } else {
            decode_integer(&self.data, payload_start, entry.encoding).map(ZiplistValue::Integer)
        }
    }

    pub fn get_owned(&self, offset: usize) -> Option<OwnedZiplistValue> {
        match self.get(offset)? {
            ZiplistValue::Bytes(bytes) => Some(OwnedZiplistValue::Bytes(bytes.to_vec())),
            ZiplistValue::Integer(value) => Some(OwnedZiplistValue::Integer(value)),
        }
    }

    pub fn compare(&self, offset: usize, value: &[u8]) -> bool {
        match self.get(offset) {
            Some(ZiplistValue::Bytes(bytes)) => bytes == value,
            Some(ZiplistValue::Integer(stored)) => {
                Self::try_encode_integer(value).is_some_and(|(_, parsed)| parsed == stored)
            }
            None => false,
        }
    }

    pub fn find(&self, start: usize, value: &[u8], skip: usize) -> Option<usize> {
        let mut cursor = start;
        let mut skip_count = 0usize;
        let mut encoded_value: Option<Option<i64>> = None;

        loop {
            let entry = self.entry_at_offset(cursor)?;
            if skip_count == 0 {
                match self.get_entry(&entry)? {
                    ZiplistValue::Bytes(bytes) if bytes == value => return Some(cursor),
                    ZiplistValue::Bytes(_) => {}
                    ZiplistValue::Integer(stored) => {
                        let parsed = match encoded_value {
                            Some(parsed) => parsed,
                            None => {
                                let parsed = Self::try_encode_integer(value).map(|(_, v)| v);
                                encoded_value = Some(parsed);
                                parsed
                            }
                        };
                        if parsed == Some(stored) {
                            return Some(cursor);
                        }
                    }
                }
                skip_count = skip;
            } else {
                skip_count -= 1;
            }

            let next = entry.offset.checked_add(entry.raw_len())?;
            let end = self.end_offset()?;
            if next >= end || self.data.get(next).copied()? == ZIP_END {
                return None;
            }
            cursor = next;
        }
    }

    pub fn iter(&self) -> ZiplistIter<'_> {
        ZiplistIter {
            ziplist: self,
            next_offset: self.first(),
            reverse: false,
        }
    }

    pub fn iter_rev(&self) -> ZiplistIter<'_> {
        ZiplistIter {
            ziplist: self,
            next_offset: self.last(),
            reverse: true,
        }
    }

    pub fn validate_integrity(data: &[u8], deep: bool) -> bool {
        Self::validate_integrity_sized(data, data.len(), deep)
    }

    /// Sized variant matching the C API's explicit allocation size argument.
    pub fn validate_integrity_sized(data: &[u8], size: usize, deep: bool) -> bool {
        validate_integrity_sized_with(data, size, deep, |_, _| true)
    }

    /// Deep validation with the C entry-callback shape adapted to safe offsets.
    pub fn validate_integrity_with<F>(data: &[u8], deep: bool, entry_cb: F) -> bool
    where
        F: FnMut(usize, u16) -> bool,
    {
        validate_integrity_sized_with(data, data.len(), deep, entry_cb)
    }

    pub fn try_encode_integer(bytes: &[u8]) -> Option<(u8, i64)> {
        if bytes.is_empty() || bytes.len() >= 32 {
            return None;
        }

        let value = parse_i64_bytes(bytes)?;
        let encoding = if (0..=12).contains(&value) {
            ZIP_INT_IMM_MIN + value as u8
        } else if (i8::MIN as i64..=i8::MAX as i64).contains(&value) {
            ZIP_INT_8B
        } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
            ZIP_INT_16B
        } else if (INT24_MIN..=INT24_MAX).contains(&value) {
            ZIP_INT_24B
        } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
            ZIP_INT_32B
        } else {
            ZIP_INT_64B
        };
        Some((encoding, value))
    }

    fn end_offset(&self) -> Option<usize> {
        self.blob_len().checked_sub(ZIPLIST_END_SIZE)
    }
}

impl Default for Ziplist {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ZiplistIter<'a> {
    ziplist: &'a Ziplist,
    next_offset: Option<usize>,
    reverse: bool,
}

impl<'a> Iterator for ZiplistIter<'a> {
    type Item = (ZiplistEntry, ZiplistValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.next_offset?;
        let entry = self.ziplist.entry_at_offset(offset)?;
        let value = self.ziplist.get_entry(&entry)?;
        self.next_offset = if self.reverse {
            self.ziplist.prev(offset)
        } else {
            self.ziplist.next(offset)
        };
        Some((entry, value))
    }
}

fn validate_integrity_sized_with<F>(data: &[u8], size: usize, deep: bool, mut entry_cb: F) -> bool
where
    F: FnMut(usize, u16) -> bool,
{
    if !validate_header(data, size) {
        return false;
    }
    if !deep {
        return true;
    }

    let Some(header_count) = read_u16_le(data, 8) else {
        return false;
    };
    let Some(tail_offset) = read_u32_le(data, 4).map(|offset| offset as usize) else {
        return false;
    };
    let end = size - ZIPLIST_END_SIZE;
    let mut count = 0usize;
    let mut cursor = ZIPLIST_HEADER_SIZE;
    let mut previous_offset = None;
    let mut previous_raw_size = 0usize;

    while cursor < size {
        if data.get(cursor).copied() == Some(ZIP_END) {
            break;
        }

        let Some(entry) = decode_entry(data, size, cursor, true) else {
            return false;
        };
        if entry.prev_raw_len != previous_raw_size {
            return false;
        }
        if !entry_cb(cursor, header_count) {
            return false;
        }

        previous_raw_size = entry.raw_len();
        previous_offset = Some(cursor);
        let Some(next) = cursor.checked_add(previous_raw_size) else {
            return false;
        };
        cursor = next;
        let Some(next_count) = count.checked_add(1) else {
            return false;
        };
        count = next_count;
    }

    if cursor != end {
        return false;
    }
    if let Some(offset) = previous_offset {
        if offset != tail_offset {
            return false;
        }
    }
    if header_count != u16::MAX && count != header_count as usize {
        return false;
    }

    true
}

fn validate_header(data: &[u8], size: usize) -> bool {
    if size > data.len() || size < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE {
        return false;
    }
    let Some(bytes) = read_u32_le(data, 0).map(|bytes| bytes as usize) else {
        return false;
    };
    if bytes != size {
        return false;
    }
    if data.get(size - ZIPLIST_END_SIZE).copied() != Some(ZIP_END) {
        return false;
    }
    let Some(tail) = read_u32_le(data, 4).map(|tail| tail as usize) else {
        return false;
    };
    tail <= size - ZIPLIST_END_SIZE
}

fn count_entries(data: &[u8]) -> Option<usize> {
    let total = read_u32_le(data, 0)? as usize;
    if !validate_header(data, total) {
        return None;
    }
    let end = total.checked_sub(ZIPLIST_END_SIZE)?;
    let mut cursor = ZIPLIST_HEADER_SIZE;
    let mut count = 0usize;
    while cursor < end {
        if data.get(cursor).copied()? == ZIP_END {
            break;
        }
        let entry = decode_entry(data, total, cursor, true)?;
        cursor = cursor.checked_add(entry.raw_len())?;
        count = count.checked_add(1)?;
    }
    if cursor == end {
        Some(count)
    } else {
        None
    }
}

fn decode_entry(
    data: &[u8],
    size: usize,
    offset: usize,
    validate_prevlen: bool,
) -> Option<ZiplistEntry> {
    let end = size.checked_sub(ZIPLIST_END_SIZE)?;
    if offset < ZIPLIST_HEADER_SIZE || offset >= end {
        return None;
    }
    if data.get(offset).copied()? == ZIP_END {
        return None;
    }

    let (prev_raw_len_size, prev_raw_len) = decode_prevlen(data, size, offset)?;
    let enc_offset = offset.checked_add(prev_raw_len_size)?;
    if enc_offset >= end {
        return None;
    }

    let raw_encoding = data.get(enc_offset).copied()?;
    let encoding = entry_encoding(raw_encoding);
    let (len_size, len) = decode_length(data, size, enc_offset, encoding)?;
    let header_size = prev_raw_len_size.checked_add(len_size)?;
    let raw_len = header_size.checked_add(len)?;
    let entry_end = offset.checked_add(raw_len)?;
    if entry_end > end {
        return None;
    }

    if validate_prevlen && prev_raw_len > 0 {
        let prev_offset = offset.checked_sub(prev_raw_len)?;
        if prev_offset < ZIPLIST_HEADER_SIZE || prev_offset >= end {
            return None;
        }
    }

    Some(ZiplistEntry {
        offset,
        prev_raw_len_size,
        prev_raw_len,
        len_size,
        len,
        header_size,
        encoding,
    })
}

fn decode_prevlen(data: &[u8], size: usize, offset: usize) -> Option<(usize, usize)> {
    let first = data.get(offset).copied()?;
    if first < ZIP_BIG_PREVLEN {
        return Some((1, first as usize));
    }

    let end = size.checked_sub(ZIPLIST_END_SIZE)?;
    let last_prevlen_byte = offset.checked_add(4)?;
    if last_prevlen_byte >= end {
        return None;
    }
    let value = read_u32_le(data, offset.checked_add(1)?)? as usize;
    Some((5, value))
}

fn decode_length(
    data: &[u8],
    size: usize,
    enc_offset: usize,
    encoding: u8,
) -> Option<(usize, usize)> {
    let end = size.checked_sub(ZIPLIST_END_SIZE)?;
    if enc_offset >= end {
        return None;
    }

    if zip_is_str(encoding) {
        match encoding {
            ZIP_STR_06B => {
                let byte = data.get(enc_offset).copied()?;
                Some((1, (byte & 0x3f) as usize))
            }
            ZIP_STR_14B => {
                if enc_offset.checked_add(1)? >= end {
                    return None;
                }
                let high = data.get(enc_offset).copied()? & 0x3f;
                let low = data.get(enc_offset + 1).copied()?;
                Some((2, (((high as usize) << 8) | low as usize)))
            }
            ZIP_STR_32B => {
                if enc_offset.checked_add(4)? >= end {
                    return None;
                }
                let len = ((data.get(enc_offset + 1).copied()? as usize) << 24)
                    | ((data.get(enc_offset + 2).copied()? as usize) << 16)
                    | ((data.get(enc_offset + 3).copied()? as usize) << 8)
                    | data.get(enc_offset + 4).copied()? as usize;
                Some((5, len))
            }
            _ => None,
        }
    } else {
        let len = match encoding {
            ZIP_INT_8B => 1,
            ZIP_INT_16B => 2,
            ZIP_INT_24B => 3,
            ZIP_INT_32B => 4,
            ZIP_INT_64B => 8,
            ZIP_INT_IMM_MIN..=ZIP_INT_IMM_MAX => 0,
            _ => return None,
        };
        Some((1, len))
    }
}

fn entry_encoding(raw: u8) -> u8 {
    if raw < ZIP_STR_MASK {
        raw & ZIP_STR_MASK
    } else {
        raw
    }
}

fn zip_is_str(encoding: u8) -> bool {
    (encoding & ZIP_STR_MASK) < ZIP_STR_MASK
}

fn decode_integer(data: &[u8], payload_offset: usize, encoding: u8) -> Option<i64> {
    match encoding {
        ZIP_INT_8B => Some(data.get(payload_offset).copied()? as i8 as i64),
        ZIP_INT_16B => read_array::<2>(data, payload_offset)
            .map(i16::from_le_bytes)
            .map(i64::from),
        ZIP_INT_24B => {
            let bytes = data.get(payload_offset..payload_offset.checked_add(3)?)?;
            let raw = (bytes[0] as i32) | ((bytes[1] as i32) << 8) | ((bytes[2] as i32) << 16);
            let signed = if raw & 0x80_0000 != 0 {
                raw | !0x00ff_ffff
            } else {
                raw
            };
            Some(signed as i64)
        }
        ZIP_INT_32B => read_array::<4>(data, payload_offset)
            .map(i32::from_le_bytes)
            .map(i64::from),
        ZIP_INT_64B => read_array::<8>(data, payload_offset).map(i64::from_le_bytes),
        ZIP_INT_IMM_MIN..=ZIP_INT_IMM_MAX => Some((encoding & ZIP_INT_IMM_MASK) as i64 - 1),
        _ => None,
    }
}

fn parse_i64_bytes(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() >= LONG_STR_SIZE {
        return None;
    }

    if bytes.len() == 1 && bytes[0].is_ascii_digit() {
        return Some((bytes[0] - b'0') as i64);
    }

    let negative = bytes[0] == b'-';
    let mut index = if negative { 1 } else { 0 };
    if index == bytes.len() {
        return None;
    }

    let first = bytes[index];
    if !(b'1'..=b'9').contains(&first) {
        return None;
    }
    let mut value = (first - b'0') as u128;
    index += 1;

    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add((byte - b'0') as u128)?;
        index += 1;
    }

    if negative {
        let min_abs = i64::MAX as u128 + 1;
        if value > min_abs {
            return None;
        }
        if value == min_abs {
            Some(i64::MIN)
        } else {
            Some(-(value as i64))
        }
    } else if value > i64::MAX as u128 {
        None
    } else {
        Some(value as i64)
    }
}

fn read_u16_le(data: &[u8], offset: usize) -> Option<u16> {
    read_array::<2>(data, offset).map(u16::from_le_bytes)
}

fn read_u32_le(data: &[u8], offset: usize) -> Option<u32> {
    read_array::<4>(data, offset).map(u32::from_le_bytes)
}

fn read_array<const N: usize>(data: &[u8], offset: usize) -> Option<[u8; N]> {
    let end = offset.checked_add(N)?;
    let slice = data.get(offset..end)?;
    let mut bytes = [0; N];
    bytes.copy_from_slice(slice);
    Some(bytes)
}

fn write_u16_le(data: &mut [u8], offset: usize, value: u16) -> bool {
    write_bytes(data, offset, &value.to_le_bytes())
}

fn write_u32_le(data: &mut [u8], offset: usize, value: u32) -> bool {
    write_bytes(data, offset, &value.to_le_bytes())
}

fn write_bytes(data: &mut [u8], offset: usize, bytes: &[u8]) -> bool {
    let Some(end) = offset.checked_add(bytes.len()) else {
        return false;
    };
    let Some(target) = data.get_mut(offset..end) else {
        return false;
    };
    target.copy_from_slice(bytes);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(value: &[u8]) -> OwnedZiplistValue {
        OwnedZiplistValue::Bytes(value.to_vec())
    }

    fn integer(value: i64) -> OwnedZiplistValue {
        OwnedZiplistValue::Integer(value)
    }

    fn make_ziplist(values: &[OwnedZiplistValue]) -> Vec<u8> {
        let mut data = vec![0; ZIPLIST_HEADER_SIZE];
        let mut previous_len = 0usize;
        let mut tail_offset = ZIPLIST_HEADER_SIZE;

        for value in values {
            tail_offset = data.len();
            let start = data.len();
            encode_prevlen(&mut data, previous_len);
            encode_value(&mut data, value);
            previous_len = data.len() - start;
        }

        data.push(ZIP_END);
        let total = data.len() as u32;
        assert!(write_u32_le(&mut data, 0, total));
        assert!(write_u32_le(&mut data, 4, tail_offset as u32));
        assert!(write_u16_le(
            &mut data,
            8,
            values.len().min(u16::MAX as usize) as u16,
        ));
        data
    }

    fn encode_prevlen(out: &mut Vec<u8>, len: usize) {
        if len < ZIP_BIG_PREVLEN as usize {
            out.push(len as u8);
        } else {
            out.push(ZIP_BIG_PREVLEN);
            out.extend_from_slice(&(len as u32).to_le_bytes());
        }
    }

    fn encode_value(out: &mut Vec<u8>, value: &OwnedZiplistValue) {
        match value {
            OwnedZiplistValue::Bytes(bytes) => {
                encode_string_header(out, bytes.len());
                out.extend_from_slice(bytes);
            }
            OwnedZiplistValue::Integer(value) => {
                let encoding = encoding_for_integer(*value);
                out.push(encoding);
                encode_integer_payload(out, *value, encoding);
            }
        }
    }

    fn encode_string_header(out: &mut Vec<u8>, len: usize) {
        if len <= 0x3f {
            out.push(ZIP_STR_06B | len as u8);
        } else if len <= 0x3fff {
            out.push(ZIP_STR_14B | ((len >> 8) as u8 & 0x3f));
            out.push((len & 0xff) as u8);
        } else {
            out.push(ZIP_STR_32B);
            out.push(((len >> 24) & 0xff) as u8);
            out.push(((len >> 16) & 0xff) as u8);
            out.push(((len >> 8) & 0xff) as u8);
            out.push((len & 0xff) as u8);
        }
    }

    fn encoding_for_integer(value: i64) -> u8 {
        if (0..=12).contains(&value) {
            ZIP_INT_IMM_MIN + value as u8
        } else if (i8::MIN as i64..=i8::MAX as i64).contains(&value) {
            ZIP_INT_8B
        } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
            ZIP_INT_16B
        } else if (INT24_MIN..=INT24_MAX).contains(&value) {
            ZIP_INT_24B
        } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
            ZIP_INT_32B
        } else {
            ZIP_INT_64B
        }
    }

    fn encode_integer_payload(out: &mut Vec<u8>, value: i64, encoding: u8) {
        match encoding {
            ZIP_INT_8B => out.push(value as i8 as u8),
            ZIP_INT_16B => out.extend_from_slice(&(value as i16).to_le_bytes()),
            ZIP_INT_24B => {
                let raw = value as i32;
                out.push((raw & 0xff) as u8);
                out.push(((raw >> 8) & 0xff) as u8);
                out.push(((raw >> 16) & 0xff) as u8);
            }
            ZIP_INT_32B => out.extend_from_slice(&(value as i32).to_le_bytes()),
            ZIP_INT_64B => out.extend_from_slice(&value.to_le_bytes()),
            ZIP_INT_IMM_MIN..=ZIP_INT_IMM_MAX => {}
            _ => unreachable!("test encoding helper received invalid encoding"),
        }
    }

    fn owned_values(ziplist: &Ziplist) -> Vec<OwnedZiplistValue> {
        ziplist
            .iter()
            .map(|(_, value)| match value {
                ZiplistValue::Bytes(bytes) => OwnedZiplistValue::Bytes(bytes.to_vec()),
                ZiplistValue::Integer(value) => OwnedZiplistValue::Integer(value),
            })
            .collect()
    }

    #[test]
    fn ziplist_new_empty_matches_valkey_header() {
        let ziplist = Ziplist::new();

        assert_eq!(ziplist.blob_len(), ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE);
        assert_eq!(
            read_u32_le(ziplist.as_bytes(), 4),
            Some(ZIPLIST_HEADER_SIZE as u32)
        );
        assert_eq!(read_u16_le(ziplist.as_bytes(), 8), Some(0));
        assert!(ziplist.is_empty());
        assert_eq!(ziplist.len(), 0);
        assert!(Ziplist::validate_integrity(ziplist.as_bytes(), false));
        assert!(Ziplist::validate_integrity(ziplist.as_bytes(), true));
    }

    #[test]
    fn ziplist_iterates_c_documented_immediate_integer_example() {
        let raw = vec![
            0x0f, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0xf3, 0x02, 0xf6,
            0xff,
        ];
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();

        assert_eq!(ziplist.blob_len(), 15);
        assert_eq!(ziplist.len(), 2);
        assert_eq!(owned_values(&ziplist), vec![integer(2), integer(5)]);
        assert_eq!(
            ziplist
                .iter_rev()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            vec![ZiplistValue::Integer(5), ZiplistValue::Integer(2)]
        );
    }

    #[test]
    fn ziplist_indexes_forward_and_backward() {
        let raw = make_ziplist(&[bytes(b"alpha"), integer(7), bytes(b"omega")]);
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();

        assert_eq!(
            ziplist.get_owned(ziplist.index(0).unwrap()),
            Some(bytes(b"alpha"))
        );
        assert_eq!(
            ziplist.get_owned(ziplist.index(1).unwrap()),
            Some(integer(7))
        );
        assert_eq!(
            ziplist.get_owned(ziplist.index(2).unwrap()),
            Some(bytes(b"omega"))
        );
        assert_eq!(
            ziplist.get_owned(ziplist.index(-1).unwrap()),
            Some(bytes(b"omega"))
        );
        assert_eq!(
            ziplist.get_owned(ziplist.index(-2).unwrap()),
            Some(integer(7))
        );
        assert_eq!(ziplist.index(3), None);
        assert_eq!(ziplist.index(-4), None);
    }

    #[test]
    fn ziplist_decodes_large_prevlen_and_multibyte_string_lengths() {
        let large = vec![b'x'; 300];
        let raw = make_ziplist(&[OwnedZiplistValue::Bytes(large.clone()), bytes(b"tail")]);
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();
        let second = ziplist.entry_at_index(1).unwrap();

        assert_eq!(
            ziplist.get_owned(ziplist.index(0).unwrap()),
            Some(OwnedZiplistValue::Bytes(large))
        );
        assert_eq!(ziplist.get_owned(second.offset), Some(bytes(b"tail")));
        assert_eq!(second.prev_raw_len_size, 5);
        assert!(second.prev_raw_len >= ZIP_BIG_PREVLEN as usize);
    }

    #[test]
    fn ziplist_decodes_all_integer_widths() {
        let values = [
            0,
            12,
            -128,
            i16::MAX as i64,
            INT24_MIN,
            INT24_MAX,
            i32::MIN as i64,
            i64::MIN,
        ];
        let raw = make_ziplist(&values.iter().copied().map(integer).collect::<Vec<_>>());
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();

        assert_eq!(
            owned_values(&ziplist),
            values.iter().copied().map(integer).collect::<Vec<_>>()
        );
    }

    #[test]
    fn ziplist_compare_keeps_payloads_as_bytes() {
        let raw = make_ziplist(&[bytes(&[0xff, 0x00, b'a']), integer(42), bytes(b"42")]);
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();

        assert!(ziplist.compare(ziplist.index(0).unwrap(), &[0xff, 0x00, b'a']));
        assert!(!ziplist.compare(ziplist.index(0).unwrap(), b"a"));
        assert!(ziplist.compare(ziplist.index(1).unwrap(), b"42"));
        assert!(!ziplist.compare(ziplist.index(1).unwrap(), b"0042"));
        assert!(ziplist.compare(ziplist.index(2).unwrap(), b"42"));
    }

    #[test]
    fn ziplist_find_honors_skip_count() {
        let raw = make_ziplist(&[bytes(b"k1"), bytes(b"v1"), bytes(b"k2"), integer(99)]);
        let ziplist = Ziplist::from_raw_bytes(raw).unwrap();
        let start = ziplist.first().unwrap();

        let found = ziplist.find(start, b"k2", 1).unwrap();
        assert_eq!(ziplist.get_owned(found), Some(bytes(b"k2")));
        assert_eq!(ziplist.find(start, b"v1", 1), None);

        let found_integer = ziplist.find(start, b"99", 0).unwrap();
        assert_eq!(ziplist.get_owned(found_integer), Some(integer(99)));
    }

    #[test]
    fn ziplist_validate_integrity_rejects_corruption() {
        let raw = make_ziplist(&[bytes(b"a"), bytes(b"b")]);
        assert!(Ziplist::validate_integrity(&raw, false));
        assert!(Ziplist::validate_integrity(&raw, true));

        let mut bad_size = raw.clone();
        assert!(write_u32_le(&mut bad_size, 0, 999));
        assert!(!Ziplist::validate_integrity(&bad_size, false));

        let mut bad_end = raw.clone();
        let last = bad_end.len() - 1;
        bad_end[last] = 0;
        assert!(!Ziplist::validate_integrity(&bad_end, false));

        let mut bad_tail = raw.clone();
        assert!(write_u32_le(&mut bad_tail, 4, 999));
        assert!(!Ziplist::validate_integrity(&bad_tail, false));

        let mut bad_prevlen = raw.clone();
        let second = Ziplist::from_raw_bytes(raw.clone())
            .unwrap()
            .index(1)
            .unwrap();
        bad_prevlen[second] = 99;
        assert!(!Ziplist::validate_integrity(&bad_prevlen, true));

        let mut bad_count = raw;
        assert!(write_u16_le(&mut bad_count, 8, 3));
        assert!(Ziplist::validate_integrity(&bad_count, false));
        assert!(!Ziplist::validate_integrity(&bad_count, true));
    }

    #[test]
    fn ziplist_validate_integrity_callback_can_reject_entry() {
        let raw = make_ziplist(&[bytes(b"a"), bytes(b"b")]);
        let mut seen = Vec::new();

        assert!(Ziplist::validate_integrity_with(
            &raw,
            true,
            |offset, count| {
                seen.push((offset, count));
                true
            }
        ));
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().all(|(_, count)| *count == 2));

        assert!(!Ziplist::validate_integrity_with(
            &raw,
            true,
            |offset, _| { offset != ZIPLIST_HEADER_SIZE }
        ));
    }

    #[test]
    fn ziplist_try_encode_integer_matches_string2ll_shape_without_utf8() {
        assert_eq!(
            Ziplist::try_encode_integer(b"0"),
            Some((ZIP_INT_IMM_MIN, 0))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"12"),
            Some((ZIP_INT_IMM_MIN + 12, 12))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"-128"),
            Some((ZIP_INT_8B, -128))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"32767"),
            Some((ZIP_INT_16B, 32767))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"-8388608"),
            Some((ZIP_INT_24B, INT24_MIN))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"9223372036854775807"),
            Some((ZIP_INT_64B, i64::MAX))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"-9223372036854775808"),
            Some((ZIP_INT_64B, i64::MIN))
        );

        assert_eq!(Ziplist::try_encode_integer(b""), None);
        assert_eq!(Ziplist::try_encode_integer(b"+1"), None);
        assert_eq!(Ziplist::try_encode_integer(b"01"), None);
        assert_eq!(Ziplist::try_encode_integer(b"-0"), None);
        assert_eq!(Ziplist::try_encode_integer(&[0xff]), None);
    }

    #[test]
    fn ziplist_safe_to_add_checks_global_limit() {
        let ziplist = Ziplist::new();
        let max_add = ZIPLIST_MAX_SAFETY_SIZE - ziplist.blob_len();

        assert!(ziplist.safe_to_add(max_add));
        assert!(!ziplist.safe_to_add(max_add + 1));
    }
}

// --------------------------------------------------------------------------
// PORT STATUS
//   source:        reference/valkey/src/ziplist.c, reference/valkey/src/ziplist.h
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Source-shaped read-only legacy ziplist decoder. Valkey
//                  header/entry byte layout, integer encodings, compare/find,
//                  iteration, safe-to-add, and deep validation are covered.
//                  Mutating ziplist writes remain out of scope for this packet.
// --------------------------------------------------------------------------

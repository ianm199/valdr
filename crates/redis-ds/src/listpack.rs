//! `ListPack` - compact contiguous-buffer encoding for collections.
//! The blob layout: `[total-bytes:u32-le][element-count:u16-le][entry...][0xff]`.
//! Each entry stores either compact integer bytes or string header+payload bytes
//! followed by a reverse-encoded backlen. Public cursors are byte offsets into
//! the encoded blob.

const LP_HDR_SIZE: usize = 6;
const LP_HDR_NUMELE_UNKNOWN: u16 = u16::MAX;
const LP_MAX_INT_ENCODING_LEN: usize = 9;
const LP_MAX_BACKLEN_SIZE: usize = 5;
pub const LP_INTBUF_SIZE: usize = 21;
const LISTPACK_MAX_SAFETY_SIZE: usize = 1 << 30;

const LP_ENCODING_7BIT_UINT: u8 = 0x00;
const LP_ENCODING_7BIT_UINT_MASK: u8 = 0x80;
const LP_ENCODING_6BIT_STR: u8 = 0x80;
const LP_ENCODING_6BIT_STR_MASK: u8 = 0xc0;
const LP_ENCODING_13BIT_INT: u8 = 0xc0;
const LP_ENCODING_13BIT_INT_MASK: u8 = 0xe0;
const LP_ENCODING_12BIT_STR: u8 = 0xe0;
const LP_ENCODING_12BIT_STR_MASK: u8 = 0xf0;
const LP_ENCODING_32BIT_STR: u8 = 0xf0;
const LP_ENCODING_16BIT_INT: u8 = 0xf1;
const LP_ENCODING_24BIT_INT: u8 = 0xf2;
const LP_ENCODING_32BIT_INT: u8 = 0xf3;
const LP_ENCODING_64BIT_INT: u8 = 0xf4;
const LP_EOF: u8 = 0xff;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertWhere {
    Before,
    After,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPackValue<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedListPackValue {
    Bytes(Vec<u8>),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPack {
    pub(crate) data: Vec<u8>,
}

impl ListPack {
 /// Create a new empty listpack with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(LP_HDR_SIZE + 1)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity.max(LP_HDR_SIZE + 1));
        data.extend_from_slice(&((LP_HDR_SIZE + 1) as u32).to_le_bytes());
        data.extend_from_slice(&0u16.to_le_bytes());
        data.push(LP_EOF);
        Self { data }
    }

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

 /// Deep copy.
    pub fn dup(&self) -> Self {
        self.clone()
    }

 /// Shrink the buffer to fit current size.
    pub fn shrink_to_fit(&mut self) {
        self.data.truncate(self.bytes_len());
        self.data.shrink_to_fit();
    }

 /// Check if adding bytes would exceed maximum size.
    pub fn safe_to_add(&self, add: usize) -> bool {
        self.bytes_len().saturating_add(add) <= LISTPACK_MAX_SAFETY_SIZE
    }

 /// Total byte length of the listpack.
    pub fn bytes_len(&self) -> usize {
        read_total_bytes(&self.data).unwrap_or(self.data.len() as u32) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.first().is_none()
    }

 /// Count of elements (scans when cached count is unknown).
    pub fn len(&self) -> usize {
        match read_num_elements(&self.data) {
            Some(n) if n != LP_HDR_NUMELE_UNKNOWN => n as usize,
            _ => count_entries(&self.data).unwrap_or(0),
        }
    }

 /// Count of elements, refreshing the cached header count.
    pub fn length(&mut self) -> usize {
        let cached = read_num_elements(&self.data);
        if let Some(n) = cached {
            if n != LP_HDR_NUMELE_UNKNOWN {
                return n as usize;
            }
        }

        let count = count_entries(&self.data).unwrap_or(0);
        if count < LP_HDR_NUMELE_UNKNOWN as usize {
            set_num_elements(&mut self.data, count as u16);
        }
        count
    }

 /// Cursor to the first entry, or None if empty.
    pub fn first(&self) -> Option<usize> {
        if !self.has_valid_header() {
            return None;
        }
        if self.data.get(LP_HDR_SIZE).copied() == Some(LP_EOF) {
            None
        } else if validate_entry_at(&self.data, LP_HDR_SIZE, self.bytes_len()) {
            Some(LP_HDR_SIZE)
        } else {
            None
        }
    }

 /// Cursor to the last entry, or None if empty.
    pub fn last(&self) -> Option<usize> {
        let eof_pos = self.bytes_len().checked_sub(1)?;
        if self.data.get(eof_pos).copied() != Some(LP_EOF) {
            return None;
        }
        self.prev(eof_pos)
    }

 /// Cursor to the next entry after pos, or None if at end.
    pub fn next(&self, pos: usize) -> Option<usize> {
        let total = self.bytes_len();
        if self.data.get(pos).copied() == Some(LP_EOF) {
            return None;
        }
        let next = pos.checked_add(entry_total_size(&self.data, pos)?)?;
        if next >= total {
            return None;
        }
        if self.data.get(next).copied() == Some(LP_EOF) {
            None
        } else if validate_entry_at(&self.data, next, total) {
            Some(next)
        } else {
            None
        }
    }

 /// Cursor to the previous entry before pos. Passing EOF returns the last entry.
    pub fn prev(&self, pos: usize) -> Option<usize> {
        let total = self.bytes_len();
        if pos <= LP_HDR_SIZE || pos >= total {
            return None;
        }
        let prev_encoded_len = decode_backlen(&self.data, pos.checked_sub(1)?)?;
        let backlen_size = encode_backlen_size(prev_encoded_len)?;
        let span = prev_encoded_len.checked_add(backlen_size)?;
        let prev = pos.checked_sub(span)?;
        if prev < LP_HDR_SIZE {
            return None;
        }
        let next = prev.checked_add(entry_total_size(&self.data, prev)?)?;
        if next == pos && validate_entry_at(&self.data, prev, total) {
            Some(prev)
        } else {
            None
        }
    }

 /// Cursor to the entry at index (supports negative indices).
    pub fn seek(&self, mut index: i64) -> Option<usize> {
        let mut forward = true;
        match read_num_elements(&self.data) {
            Some(n) if n != LP_HDR_NUMELE_UNKNOWN => {
                let len = n as i64;
                if index < 0 {
                    index = len.checked_add(index)?;
                }
                if index < 0 || index >= len {
                    return None;
                }
                if index > len / 2 {
                    forward = false;
                    index -= len;
                }
            }
            _ if index < 0 => {
                forward = false;
            }
            _ => {}
        }

        if forward {
            let mut cursor = self.first();
            while index > 0 {
                cursor = cursor.and_then(|pos| self.next(pos));
                index -= 1;
            }
            cursor
        } else {
            let mut cursor = self.last();
            while index < -1 {
                cursor = cursor.and_then(|pos| self.prev(pos));
                index += 1;
            }
            cursor
        }
    }

 /// Get the value at pos.
    pub fn get(&self, pos: usize) -> Option<ListPackValue<'_>> {
        decode_value(&self.data, pos).map(|decoded| decoded.value)
    }

    pub fn get_owned(&self, pos: usize) -> Option<OwnedListPackValue> {
        match self.get(pos)? {
            ListPackValue::Bytes(bytes) => Some(OwnedListPackValue::Bytes(bytes.to_vec())),
            ListPackValue::Integer(value) => Some(OwnedListPackValue::Integer(value)),
        }
    }

 /// Get the value at pos as a byte slice, using intbuf for integer conversion.
    pub fn get_as_bytes<'a>(
        &'a self,
        pos: usize,
        intbuf: &'a mut [u8; LP_INTBUF_SIZE],
    ) -> Option<&'a [u8]> {
        match self.get(pos)? {
            ListPackValue::Bytes(bytes) => Some(bytes),
            ListPackValue::Integer(value) => {
                let len = i64_to_bytes(value, intbuf);
                Some(&intbuf[..len])
            }
        }
    }

 /// Check if the value at pos equals the given bytes.
    pub fn compare(&self, pos: usize, value: &[u8]) -> bool {
        match self.get(pos) {
            Some(ListPackValue::Bytes(bytes)) => bytes == value,
            Some(ListPackValue::Integer(stored)) => bytes_to_i64(value) == Some(stored),
            None => false,
        }
    }

 /// Find the cursor to the next entry equal to value, skipping skip entries.
    pub fn find(&self, start_pos: usize, value: &[u8], skip: usize) -> Option<usize> {
        let mut cursor = Some(start_pos);
        let mut skip_count = 0usize;
        let mut parsed_target = None;
        let mut parsed_target_known = false;

        while let Some(pos) = cursor {
            if self.data.get(pos).copied() == Some(LP_EOF) {
                break;
            }

            if skip_count == 0 {
                match self.get(pos)? {
                    ListPackValue::Bytes(bytes) if bytes == value => return Some(pos),
                    ListPackValue::Bytes(_) => {}
                    ListPackValue::Integer(stored) => {
                        if !parsed_target_known {
                            parsed_target = if value.is_empty() || value.len() >= 32 {
                                None
                            } else {
                                bytes_to_i64(value)
                            };
                            parsed_target_known = true;
                        }
                        if parsed_target == Some(stored) {
                            return Some(pos);
                        }
                    }
                }
                skip_count = skip;
            } else {
                skip_count -= 1;
            }

            cursor = self.next(pos);
        }

        None
    }

 /// Append a byte string.
    pub fn append(&mut self, value: &[u8]) -> bool {
        let Some(eof_pos) = self.bytes_len().checked_sub(1) else {
            return false;
        };
        self.insert_string(value, eof_pos, InsertWhere::Before)
            .is_some()
    }

 /// Append an integer.
    pub fn append_integer(&mut self, value: i64) -> bool {
        let Some(eof_pos) = self.bytes_len().checked_sub(1) else {
            return false;
        };
        self.insert_integer(value, eof_pos, InsertWhere::Before)
            .is_some()
    }

 /// Prepend a byte string.
    pub fn prepend(&mut self, value: &[u8]) -> bool {
        match self.first() {
            Some(pos) => self
                .insert_string(value, pos, InsertWhere::Before)
                .is_some(),
            None => self.append(value),
        }
    }

 /// Prepend an integer.
    pub fn prepend_integer(&mut self, value: i64) -> bool {
        match self.first() {
            Some(pos) => self
                .insert_integer(value, pos, InsertWhere::Before)
                .is_some(),
            None => self.append_integer(value),
        }
    }

 /// Insert a byte string at pos.
    pub fn insert_string(
        &mut self,
        value: &[u8],
        pos: usize,
        where_: InsertWhere,
    ) -> Option<usize> {
        let content = encode_entry_content(value);
        self.insert_content(&content, pos, where_).flatten()
    }

 /// Insert an integer at pos.
    pub fn insert_integer(&mut self, value: i64, pos: usize, where_: InsertWhere) -> Option<usize> {
        let content = encode_integer_content(value);
        self.insert_content(&content, pos, where_).flatten()
    }

 /// Replace the entry at pos with a byte string.
    pub fn replace(&mut self, pos: &mut usize, value: &[u8]) -> bool {
        let Some(new_pos) = self.insert_string(value, *pos, InsertWhere::Replace) else {
            return false;
        };
        *pos = new_pos;
        true
    }

 /// Replace the entry at pos with an integer.
    pub fn replace_integer(&mut self, pos: &mut usize, value: i64) -> bool {
        let Some(new_pos) = self.insert_integer(value, *pos, InsertWhere::Replace) else {
            return false;
        };
        *pos = new_pos;
        true
    }

 /// Delete the entry at pos, returning the cursor to the next entry or None.
    pub fn delete(&mut self, pos: usize) -> Option<usize> {
        self.delete_internal(pos)?
    }

 /// Delete num entries starting from pos.
    pub fn delete_range_with_entry(&mut self, pos: &mut Option<usize>, num: usize) -> bool {
        if num == 0 {
            return true;
        }

        let mut cursor = match *pos {
            Some(pos) => pos,
            None => return true,
        };

        for _ in 0..num {
            match self.delete_internal(cursor) {
                Some(Some(next)) => cursor = next,
                Some(None) => {
                    *pos = None;
                    return true;
                }
                None => return false,
            }
        }

        *pos = Some(cursor);
        true
    }

 /// Delete num entries starting at index.
    pub fn delete_range(&mut self, index: i64, num: usize) -> bool {
        if num == 0 {
            return true;
        }
        let mut cursor = self.seek(index);
        self.delete_range_with_entry(&mut cursor, num)
    }

 /// Merge two listpacks, consuming both and returning the merged result.
    pub fn merge(first: Self, second: Self) -> Option<Self> {
        if !first.is_valid(true) || !second.is_valid(true) {
            return None;
        }

        let first_bytes = first.bytes_len();
        let second_bytes = second.bytes_len();
        let merged_len = first_bytes
            .checked_add(second_bytes)?
            .checked_sub(LP_HDR_SIZE + 1)?;
        if merged_len > u32::MAX as usize || merged_len > LISTPACK_MAX_SAFETY_SIZE {
            return None;
        }

        let mut data = Vec::with_capacity(merged_len);
        data.extend_from_slice(&first.data[..first_bytes - 1]);
        data.extend_from_slice(&second.data[LP_HDR_SIZE..second_bytes]);

        let len = first.len().saturating_add(second.len());
        set_total_bytes(&mut data, merged_len as u32);
        set_num_elements(
            &mut data,
            if len < LP_HDR_NUMELE_UNKNOWN as usize {
                len as u16
            } else {
                LP_HDR_NUMELE_UNKNOWN
            },
        );

        Self::from_raw_bytes(data)
    }

 /// Estimate bytes needed to encode an integer repeated num times.
    pub fn estimate_bytes_repeated_integer(value: i64, repeat: usize) -> Option<usize> {
        let content = encode_integer_content(value);
        let backlen = encode_backlen_size(content.len())?;
        LP_HDR_SIZE
            .checked_add(content.len().checked_add(backlen)?.checked_mul(repeat)?)?
            .checked_add(1)
    }

 /// Same as `lpValidateFirst`, intentionally without entry validation.
    pub fn validate_first(&self) -> Option<usize> {
        if self.data.get(LP_HDR_SIZE).copied() == Some(LP_EOF) {
            None
        } else {
            Some(LP_HDR_SIZE)
        }
    }

 /// Validate and advance cursor to the next entry.
    pub fn validate_next(&self, cursor: &mut Option<usize>, lpbytes: usize) -> bool {
        validate_next_raw(&self.data, cursor, lpbytes)
    }

 /// Validate a raw listpack blob.
    pub fn validate_integrity(data: &[u8], deep: bool) -> bool {
        Self::validate_integrity_with(data, deep, None)
    }

    pub fn validate_integrity_with(
        data: &[u8],
        deep: bool,
        entry_cb: Option<fn(pos: usize, header_count: u16) -> bool>,
    ) -> bool {
        if data.len() < LP_HDR_SIZE + 1 {
            return false;
        }

        let Some(bytes) = read_total_bytes(data).map(|n| n as usize) else {
            return false;
        };
        if bytes != data.len() {
            return false;
        }
        if data.get(bytes - 1).copied() != Some(LP_EOF) {
            return false;
        }
        if !deep {
            return true;
        }

        let Some(header_count) = read_num_elements(data) else {
            return false;
        };

        let mut count = 0usize;
        let mut cursor = if data.get(LP_HDR_SIZE).copied() == Some(LP_EOF) {
            None
        } else {
            Some(LP_HDR_SIZE)
        };

        while let Some(pos) = cursor {
            if data.get(pos).copied() == Some(LP_EOF) {
                break;
            }
            let prev = pos;
            if !validate_next_raw(data, &mut cursor, bytes) {
                return false;
            }
            if let Some(cb) = entry_cb {
                if !cb(prev, header_count) {
                    return false;
                }
            }
            count += 1;
        }

        let eof_at_declared_end = match cursor {
            Some(pos) => pos == bytes - 1 && data.get(pos).copied() == Some(LP_EOF),
            None => bytes == LP_HDR_SIZE + 1 && data.get(LP_HDR_SIZE).copied() == Some(LP_EOF),
        };
        if !eof_at_declared_end {
            return false;
        }

        if header_count != LP_HDR_NUMELE_UNKNOWN && header_count as usize != count {
            return false;
        }

        true
    }

    pub fn is_valid(&self, deep: bool) -> bool {
        Self::validate_integrity(&self.data, deep)
    }

    fn has_valid_header(&self) -> bool {
        self.bytes_len() == self.data.len()
            && self.data.len() > LP_HDR_SIZE
            && self.data.last().copied() == Some(LP_EOF)
    }

    fn insert_content(
        &mut self,
        content: &[u8],
        mut pos: usize,
        mut where_: InsertWhere,
    ) -> Option<Option<usize>> {
        if content.is_empty() {
            return None;
        }
        let old_total = self.bytes_len();
        if old_total != self.data.len() || old_total < LP_HDR_SIZE + 1 {
            return None;
        }

        if where_ == InsertWhere::After {
            if self.data.get(pos).copied() == Some(LP_EOF) {
                return None;
            }
            if !validate_entry_at(&self.data, pos, old_total) {
                return None;
            }
            pos = pos.checked_add(entry_total_size(&self.data, pos)?)?;
            where_ = InsertWhere::Before;
        }

        let replaced_len = if where_ == InsertWhere::Replace {
            if self.data.get(pos).copied() == Some(LP_EOF) {
                return None;
            }
            if !validate_entry_at(&self.data, pos, old_total) {
                return None;
            }
            entry_total_size(&self.data, pos)?
        } else {
            if pos > old_total - 1 {
                return None;
            }
            if self.data.get(pos).copied() != Some(LP_EOF)
                && !validate_entry_at(&self.data, pos, old_total)
            {
                return None;
            }
            0
        };

        let backlen_len = encode_backlen_size(content.len())?;
        let new_entry_len = content.len().checked_add(backlen_len)?;
        let new_total = old_total
            .checked_add(new_entry_len)?
            .checked_sub(replaced_len)?;
        if new_total > u32::MAX as usize || new_total > LISTPACK_MAX_SAFETY_SIZE {
            return None;
        }

        let mut replacement = Vec::with_capacity(new_entry_len);
        replacement.extend_from_slice(content);
        let (actual_backlen_len, backlen) = encode_backlen_bytes(content.len())?;
        replacement.extend_from_slice(&backlen[..actual_backlen_len]);

        self.data
            .splice(pos..pos.checked_add(replaced_len)?, replacement);
        set_total_bytes(&mut self.data, new_total as u32);

        if where_ != InsertWhere::Replace {
            increment_cached_len(&mut self.data);
        }

        Some(Some(pos))
    }

    fn delete_internal(&mut self, pos: usize) -> Option<Option<usize>> {
        let old_total = self.bytes_len();
        if old_total != self.data.len()
            || old_total < LP_HDR_SIZE + 1
            || self.data.get(pos).copied() == Some(LP_EOF)
        {
            return None;
        }

        let replaced_len = entry_total_size(&self.data, pos)?;
        let new_total = old_total.checked_sub(replaced_len)?;
        self.data
            .splice(pos..pos.checked_add(replaced_len)?, core::iter::empty());
        set_total_bytes(&mut self.data, new_total as u32);
        decrement_cached_len(&mut self.data);

        if self.data.get(pos).copied() == Some(LP_EOF) {
            Some(None)
        } else {
            Some(Some(pos))
        }
    }
}

impl Default for ListPack {
    fn default() -> Self {
        Self::new()
    }
}

fn read_total_bytes(data: &[u8]) -> Option<u32> {
    let bytes = data.get(0..4)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn set_total_bytes(data: &mut [u8], value: u32) {
    if data.len() >= 4 {
        data[..4].copy_from_slice(&value.to_le_bytes());
    }
}

fn read_num_elements(data: &[u8]) -> Option<u16> {
    let bytes = data.get(4..6)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn set_num_elements(data: &mut [u8], value: u16) {
    if data.len() >= LP_HDR_SIZE {
        data[4..6].copy_from_slice(&value.to_le_bytes());
    }
}

fn increment_cached_len(data: &mut [u8]) {
    let Some(count) = read_num_elements(data) else {
        return;
    };
    if count != LP_HDR_NUMELE_UNKNOWN {
        set_num_elements(data, count.saturating_add(1));
    }
}

fn decrement_cached_len(data: &mut [u8]) {
    let Some(count) = read_num_elements(data) else {
        return;
    };
    if count != LP_HDR_NUMELE_UNKNOWN {
        set_num_elements(data, count.saturating_sub(1));
    }
}

fn count_entries(data: &[u8]) -> Option<usize> {
    if !ListPack::validate_integrity(data, false) {
        return None;
    }

    let lpbytes = data.len();
    let mut count = 0usize;
    let mut cursor = if data.get(LP_HDR_SIZE).copied() == Some(LP_EOF) {
        None
    } else {
        Some(LP_HDR_SIZE)
    };

    while let Some(pos) = cursor {
        if data.get(pos).copied() == Some(LP_EOF) {
            return (pos == lpbytes - 1).then_some(count);
        }
        if !validate_next_raw(data, &mut cursor, lpbytes) {
            return None;
        }
        count += 1;
    }

    Some(count)
}

fn bytes_to_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= LP_INTBUF_SIZE {
        return None;
    }
    if s.len() == 1 && s[0].is_ascii_digit() {
        return Some((s[0] - b'0') as i64);
    }

    let mut index = 0usize;
    let negative = s[0] == b'-';
    if negative {
        index += 1;
        if index == s.len() {
            return None;
        }
    }

    if !(b'1'..=b'9').contains(&s[index]) {
        return None;
    }

    let mut value = (s[index] - b'0') as u64;
    index += 1;
    while index < s.len() {
        let digit = s[index];
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add((digit - b'0') as u64)?;
        index += 1;
    }

    if negative {
        if value > (1u64 << 63) {
            return None;
        }
        Some((value as i64).wrapping_neg())
    } else if value > i64::MAX as u64 {
        None
    } else {
        Some(value as i64)
    }
}

fn i64_to_bytes(value: i64, buf: &mut [u8; LP_INTBUF_SIZE]) -> usize {
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    let negative = value < 0;
    let mut n = value.unsigned_abs();
    let mut tmp = [0u8; LP_INTBUF_SIZE];
    let mut len = 0usize;

    while n > 0 {
        tmp[len] = b'0' + (n % 10) as u8;
        n /= 10;
        len += 1;
    }

    let mut out = 0usize;
    if negative {
        buf[out] = b'-';
        out += 1;
    }
    for idx in (0..len).rev() {
        buf[out] = tmp[idx];
        out += 1;
    }
    out
}

fn encode_integer_content(value: i64) -> Vec<u8> {
    let mut buf = [0u8; LP_MAX_INT_ENCODING_LEN];
    let len = encode_integer_get_type(value, &mut buf);
    buf[..len].to_vec()
}

fn encode_entry_content(value: &[u8]) -> Vec<u8> {
    if let Some(integer) = bytes_to_i64(value) {
        return encode_integer_content(integer);
    }

    let len = value.len();
    let mut out = if len < 64 {
        let mut out = Vec::with_capacity(1 + len);
        out.push(LP_ENCODING_6BIT_STR | len as u8);
        out
    } else if len < 4096 {
        let mut out = Vec::with_capacity(2 + len);
        out.push(LP_ENCODING_12BIT_STR | ((len >> 8) as u8 & 0x0f));
        out.push((len & 0xff) as u8);
        out
    } else {
        let mut out = Vec::with_capacity(5 + len);
        out.push(LP_ENCODING_32BIT_STR);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out
    };
    out.extend_from_slice(value);
    out
}

fn encode_integer_get_type(value: i64, out: &mut [u8; LP_MAX_INT_ENCODING_LEN]) -> usize {
    if (0..=127).contains(&value) {
        out[0] = value as u8;
        1
    } else if (-4096..=4095).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 13) + value) as u16
        } else {
            value as u16
        };
        out[0] = ((unsigned >> 8) as u8) | LP_ENCODING_13BIT_INT;
        out[1] = (unsigned & 0xff) as u8;
        2
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 16) + value) as u32
        } else {
            value as u32
        };
        out[0] = LP_ENCODING_16BIT_INT;
        out[1] = (unsigned & 0xff) as u8;
        out[2] = (unsigned >> 8) as u8;
        3
    } else if (-8_388_608..=8_388_607).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 24) + value) as u32
        } else {
            value as u32
        };
        out[0] = LP_ENCODING_24BIT_INT;
        out[1] = (unsigned & 0xff) as u8;
        out[2] = ((unsigned >> 8) & 0xff) as u8;
        out[3] = (unsigned >> 16) as u8;
        4
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 32) + value) as u64
        } else {
            value as u64
        };
        out[0] = LP_ENCODING_32BIT_INT;
        out[1] = (unsigned & 0xff) as u8;
        out[2] = ((unsigned >> 8) & 0xff) as u8;
        out[3] = ((unsigned >> 16) & 0xff) as u8;
        out[4] = (unsigned >> 24) as u8;
        5
    } else {
        let unsigned = value as u64;
        out[0] = LP_ENCODING_64BIT_INT;
        out[1] = (unsigned & 0xff) as u8;
        out[2] = ((unsigned >> 8) & 0xff) as u8;
        out[3] = ((unsigned >> 16) & 0xff) as u8;
        out[4] = ((unsigned >> 24) & 0xff) as u8;
        out[5] = ((unsigned >> 32) & 0xff) as u8;
        out[6] = ((unsigned >> 40) & 0xff) as u8;
        out[7] = ((unsigned >> 48) & 0xff) as u8;
        out[8] = (unsigned >> 56) as u8;
        9
    }
}

fn encode_backlen_size(len: usize) -> Option<usize> {
    if len <= 127 {
        Some(1)
    } else if len <= 16_383 {
        Some(2)
    } else if len <= 2_097_151 {
        Some(3)
    } else if len <= 268_435_455 {
        Some(4)
    } else if len <= 34_359_738_367 {
        Some(5)
    } else {
        None
    }
}

fn encode_backlen_bytes(len: usize) -> Option<(usize, [u8; LP_MAX_BACKLEN_SIZE])> {
    let mut out = [0u8; LP_MAX_BACKLEN_SIZE];
    let bytes = encode_backlen_size(len)?;
    match bytes {
        1 => out[0] = len as u8,
        2 => {
            out[0] = (len >> 7) as u8;
            out[1] = (len & 127) as u8 | 128;
        }
        3 => {
            out[0] = (len >> 14) as u8;
            out[1] = ((len >> 7) & 127) as u8 | 128;
            out[2] = (len & 127) as u8 | 128;
        }
        4 => {
            out[0] = (len >> 21) as u8;
            out[1] = ((len >> 14) & 127) as u8 | 128;
            out[2] = ((len >> 7) & 127) as u8 | 128;
            out[3] = (len & 127) as u8 | 128;
        }
        5 => {
            out[0] = (len >> 28) as u8;
            out[1] = ((len >> 21) & 127) as u8 | 128;
            out[2] = ((len >> 14) & 127) as u8 | 128;
            out[3] = ((len >> 7) & 127) as u8 | 128;
            out[4] = (len & 127) as u8 | 128;
        }
        _ => return None,
    }
    Some((bytes, out))
}

fn decode_backlen(data: &[u8], pos: usize) -> Option<usize> {
    let mut value = 0usize;
    let mut shift = 0usize;
    let mut index = pos;

    loop {
        let byte = *data.get(index)?;
        value |= ((byte & 127) as usize) << shift;
        if byte & 128 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 28 || index == 0 {
            return None;
        }
        index -= 1;
    }
}

#[derive(Debug, Clone, Copy)]
struct DecodedValue<'a> {
    value: ListPackValue<'a>,
    encoded_len: usize,
}

fn decode_value(data: &[u8], pos: usize) -> Option<DecodedValue<'_>> {
    let byte = *data.get(pos)?;
    let slice = data.get(pos..)?;

    if byte & LP_ENCODING_7BIT_UINT_MASK == LP_ENCODING_7BIT_UINT {
        return Some(DecodedValue {
            value: ListPackValue::Integer((byte & 0x7f) as i64),
            encoded_len: 1,
        });
    }

    if byte & LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR {
        let len = (byte & 0x3f) as usize;
        let start = pos.checked_add(1)?;
        let end = start.checked_add(len)?;
        return Some(DecodedValue {
            value: ListPackValue::Bytes(data.get(start..end)?),
            encoded_len: 1 + len,
        });
    }

    if byte & LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT {
        let uval = (((byte & 0x1f) as u64) << 8) | *slice.get(1)? as u64;
        return Some(DecodedValue {
            value: ListPackValue::Integer(decode_signed(uval, 1u64 << 12, 8191)),
            encoded_len: 2,
        });
    }

    match byte {
        LP_ENCODING_16BIT_INT => {
            let uval = (*slice.get(1)? as u64) | ((*slice.get(2)? as u64) << 8);
            Some(DecodedValue {
                value: ListPackValue::Integer(decode_signed(uval, 1u64 << 15, u16::MAX as u64)),
                encoded_len: 3,
            })
        }
        LP_ENCODING_24BIT_INT => {
            let uval = (*slice.get(1)? as u64)
                | ((*slice.get(2)? as u64) << 8)
                | ((*slice.get(3)? as u64) << 16);
            Some(DecodedValue {
                value: ListPackValue::Integer(decode_signed(
                    uval,
                    1u64 << 23,
                    (u32::MAX >> 8) as u64,
                )),
                encoded_len: 4,
            })
        }
        LP_ENCODING_32BIT_INT => {
            let uval = (*slice.get(1)? as u64)
                | ((*slice.get(2)? as u64) << 8)
                | ((*slice.get(3)? as u64) << 16)
                | ((*slice.get(4)? as u64) << 24);
            Some(DecodedValue {
                value: ListPackValue::Integer(decode_signed(uval, 1u64 << 31, u32::MAX as u64)),
                encoded_len: 5,
            })
        }
        LP_ENCODING_64BIT_INT => {
            let uval = (*slice.get(1)? as u64)
                | ((*slice.get(2)? as u64) << 8)
                | ((*slice.get(3)? as u64) << 16)
                | ((*slice.get(4)? as u64) << 24)
                | ((*slice.get(5)? as u64) << 32)
                | ((*slice.get(6)? as u64) << 40)
                | ((*slice.get(7)? as u64) << 48)
                | ((*slice.get(8)? as u64) << 56);
            Some(DecodedValue {
                value: ListPackValue::Integer(decode_signed(uval, 1u64 << 63, u64::MAX)),
                encoded_len: 9,
            })
        }
        LP_ENCODING_32BIT_STR => {
            let len = u32::from_le_bytes([
                *slice.get(1)?,
                *slice.get(2)?,
                *slice.get(3)?,
                *slice.get(4)?,
            ]) as usize;
            let start = pos.checked_add(5)?;
            let end = start.checked_add(len)?;
            Some(DecodedValue {
                value: ListPackValue::Bytes(data.get(start..end)?),
                encoded_len: 5 + len,
            })
        }
        _ if byte & LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR => {
            let len = (((byte & 0x0f) as usize) << 8) | *slice.get(1)? as usize;
            let start = pos.checked_add(2)?;
            let end = start.checked_add(len)?;
            Some(DecodedValue {
                value: ListPackValue::Bytes(data.get(start..end)?),
                encoded_len: 2 + len,
            })
        }
        _ => None,
    }
}

fn decode_signed(uval: u64, negstart: u64, negmax: u64) -> i64 {
    if uval >= negstart {
        -((negmax - uval) as i64) - 1
    } else {
        uval as i64
    }
}

fn encoded_size(data: &[u8], pos: usize) -> Option<usize> {
    decode_value(data, pos).map(|decoded| decoded.encoded_len)
}

fn entry_total_size(data: &[u8], pos: usize) -> Option<usize> {
    let encoded = encoded_size(data, pos)?;
    encoded.checked_add(encode_backlen_size(encoded)?)
}

fn validate_entry_at(data: &[u8], pos: usize, lpbytes: usize) -> bool {
    let mut cursor = Some(pos);
    validate_next_raw(data, &mut cursor, lpbytes)
}

fn validate_next_raw(data: &[u8], cursor: &mut Option<usize>, lpbytes: usize) -> bool {
    let Some(pos) = *cursor else {
        return false;
    };

    if pos < LP_HDR_SIZE || pos > lpbytes.saturating_sub(1) || lpbytes > data.len() {
        return false;
    }

    if data.get(pos).copied() == Some(LP_EOF) {
        if pos + 1 != lpbytes {
            return false;
        }
        *cursor = None;
        return true;
    }

    let Some(encoded) = encoded_size(data, pos) else {
        return false;
    };
    let Some(backlen_size) = encode_backlen_size(encoded) else {
        return false;
    };
    let Some(total) = encoded.checked_add(backlen_size) else {
        return false;
    };
    let Some(next) = pos.checked_add(total) else {
        return false;
    };
    if next > lpbytes.saturating_sub(1) {
        return false;
    }

    let Some(prevlen) = decode_backlen(data, next - 1) else {
        return false;
    };
    if prevlen != encoded {
        return false;
    }

    *cursor = Some(next);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(lp: &ListPack) -> Vec<OwnedListPackValue> {
        let mut out = Vec::new();
        let mut cursor = lp.first();
        while let Some(pos) = cursor {
            if let Some(value) = lp.get_owned(pos) {
                out.push(value);
            }
            cursor = lp.next(pos);
        }
        out
    }

    #[test]
    fn listpack_new_has_valkey_header_and_eof() {
        let lp = ListPack::new();

        assert_eq!(lp.as_bytes(), &[7, 0, 0, 0, 0, 0, LP_EOF]);
        assert_eq!(lp.bytes_len(), 7);
        assert_eq!(lp.len(), 0);
        assert!(lp.is_empty());
        assert!(lp.first().is_none());
        assert!(lp.last().is_none());
        assert!(lp.is_valid(true));
    }

    #[test]
    fn listpack_append_get_and_integer_encoding_round_trip() {
        let mut lp = ListPack::new();

        assert!(lp.append(b"alpha"));
        assert!(lp.append(b"42"));
        assert!(lp.append_integer(-4096));

        let first = lp.first().unwrap();
        let second = lp.next(first).unwrap();
        let third = lp.next(second).unwrap();

        assert_eq!(lp.get(first), Some(ListPackValue::Bytes(b"alpha")));
        assert_eq!(lp.get(second), Some(ListPackValue::Integer(42)));
        assert_eq!(lp.get(third), Some(ListPackValue::Integer(-4096)));
        assert!(lp.next(third).is_none());
        assert_eq!(lp.len(), 3);

        let mut intbuf = [0u8; LP_INTBUF_SIZE];
        assert_eq!(lp.get_as_bytes(second, &mut intbuf), Some(&b"42"[..]));
        assert!(lp.is_valid(true));
    }

    #[test]
    fn listpack_prepend_insert_replace_and_seek_preserve_order() {
        let mut lp = ListPack::new();
        assert!(lp.append(b"middle"));
        assert!(lp.prepend_integer(1));
        assert!(lp.append(b"tail"));

        let middle = lp.seek(1).unwrap();
        assert!(lp
            .insert_string(b"after", middle, InsertWhere::After)
            .is_some());
        let mut tail = lp.seek(-1).unwrap();
        assert!(lp.replace(&mut tail, b"last"));

        assert_eq!(
            values(&lp),
            vec![
                OwnedListPackValue::Integer(1),
                OwnedListPackValue::Bytes(b"middle".to_vec()),
                OwnedListPackValue::Bytes(b"after".to_vec()),
                OwnedListPackValue::Bytes(b"last".to_vec()),
            ]
        );
        assert_eq!(lp.seek(0), lp.first());
        assert_eq!(lp.seek(-1), lp.last());
        assert_eq!(lp.seek(99), None);
        assert_eq!(lp.seek(-99), None);
        assert!(lp.is_valid(true));
    }

    #[test]
    fn listpack_delete_and_delete_range_update_cursors_and_header() {
        let mut lp = ListPack::new();
        assert!(lp.append(b"a"));
        assert!(lp.append(b"b"));
        assert!(lp.append(b"c"));
        assert!(lp.append(b"d"));

        let second = lp.seek(1).unwrap();
        let next = lp.delete(second).unwrap();
        assert_eq!(lp.get(next), Some(ListPackValue::Bytes(b"c")));

        let mut cursor = lp.seek(1);
        assert!(lp.delete_range_with_entry(&mut cursor, 2));
        assert!(cursor.is_none());

        assert_eq!(values(&lp), vec![OwnedListPackValue::Bytes(b"a".to_vec())]);
        assert_eq!(lp.len(), 1);
        assert!(lp.is_valid(true));

        let only = lp.first().unwrap();
        assert_eq!(lp.delete(only), None);
        assert!(lp.is_empty());
        assert!(lp.is_valid(true));
    }

    #[test]
    fn listpack_find_compare_and_strict_integer_strings_match_valkey() {
        let mut lp = ListPack::new();
        assert!(lp.append(b"001"));
        assert!(lp.append(b"7"));
        assert!(lp.append(b"target"));
        assert!(lp.append(b"skip-me"));
        assert!(lp.append(b"target"));

        let first = lp.first().unwrap();
        let second = lp.next(first).unwrap();
        assert_eq!(lp.get(first), Some(ListPackValue::Bytes(b"001")));
        assert_eq!(lp.get(second), Some(ListPackValue::Integer(7)));
        assert!(lp.compare(first, b"001"));
        assert!(lp.compare(second, b"7"));
        assert!(!lp.compare(second, b"07"));

        assert_eq!(lp.find(first, b"target", 0), lp.seek(2));
        assert_eq!(lp.find(first, b"target", 1), lp.seek(2));
        assert_eq!(lp.find(first, b"absent", 0), None);
    }

    #[test]
    fn listpack_validation_rejects_header_backlen_and_count_corruption() {
        let mut lp = ListPack::new();
        assert!(lp.append(b"alpha"));
        assert!(lp.append_integer(i64::MIN));
        assert!(ListPack::validate_integrity(lp.as_bytes(), true));

        let mut bad_total = lp.as_bytes().to_vec();
        bad_total[0] = bad_total[0].wrapping_add(1);
        assert!(!ListPack::validate_integrity(&bad_total, false));

        let mut bad_eof = lp.as_bytes().to_vec();
        let last = bad_eof.len() - 1;
        bad_eof[last] = 0;
        assert!(!ListPack::validate_integrity(&bad_eof, false));

        let mut bad_backlen = lp.as_bytes().to_vec();
        let first = lp.first().unwrap();
        let backlen_pos = first + encoded_size(&bad_backlen, first).unwrap();
        bad_backlen[backlen_pos] ^= 0x01;
        assert!(!ListPack::validate_integrity(&bad_backlen, true));

        let mut bad_count = lp.as_bytes().to_vec();
        set_num_elements(&mut bad_count, 99);
        assert!(ListPack::validate_integrity(&bad_count, false));
        assert!(!ListPack::validate_integrity(&bad_count, true));

        let mut early_eof = lp.as_bytes().to_vec();
        let first = lp.first().unwrap();
        early_eof[first] = LP_EOF;
        assert!(!ListPack::validate_integrity(&early_eof, true));
    }

    #[test]
    fn listpack_from_raw_bytes_and_merge_preserve_blob_layout() {
        let mut left = ListPack::new();
        assert!(left.append(b"left"));
        let mut right = ListPack::new();
        assert!(right.append_integer(2));
        assert!(right.append(b"right"));

        let raw = left.as_bytes().to_vec();
        let restored = ListPack::from_raw_bytes(raw.clone()).unwrap();
        assert_eq!(restored.as_bytes(), raw.as_slice());

        let merged = ListPack::merge(left, right).unwrap();
        assert_eq!(
            values(&merged),
            vec![
                OwnedListPackValue::Bytes(b"left".to_vec()),
                OwnedListPackValue::Integer(2),
                OwnedListPackValue::Bytes(b"right".to_vec()),
            ]
        );
        assert!(merged.is_valid(true));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (compact buffer encoding, Redis stdlib)
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Safe byte-buffer owner. Blob layout, integer/string entry
//                  encodings, backlen navigation, seek, compare/find, mutation,
//                  merge, and validation covered without wiring encodings yet.
// ──────────────────────────────────────────────────────────────────────────

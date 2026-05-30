//! `QuickList` - listpack-backed node list used as Redis' list encoding.
//! This is a bounded MVP of the upstream structure: node fill accounting,
//! plain nodes for large elements, push/pop at both ends, index lookup,
//! count, duplication, and owned iteration. LZF compression, bookmarks,
//! iterator mutation, node splitting/merging, and object-model wiring remain
//! outside this packet.

use std::collections::VecDeque;
use std::iter::Rev;

use crate::listpack::{ListPack, ListPackValue, OwnedListPackValue};

const OPTIMIZATION_LEVEL: [usize; 5] = [4096, 8192, 16384, 32768, 65536];
const SIZE_SAFETY_LIMIT: usize = 8192;
const SIZE_ESTIMATE_OVERHEAD: usize = 8;
pub const QUICKLIST_INTBUF_SIZE: usize = 21;

#[cfg(target_pointer_width = "64")]
const QL_FILL_BITS: u32 = 16;
#[cfg(target_pointer_width = "64")]
const QL_COMP_BITS: u32 = 16;

#[cfg(target_pointer_width = "32")]
const QL_FILL_BITS: u32 = 14;
#[cfg(target_pointer_width = "32")]
const QL_COMP_BITS: u32 = 14;

const FILL_MAX: i32 = (1i32 << (QL_FILL_BITS - 1)) - 1;
const FILL_MIN: i32 = -5;
const COMPRESS_MAX: u32 = (1u32 << QL_COMP_BITS) - 1;

pub const QUICKLIST_HEAD: i32 = 0;
pub const QUICKLIST_TAIL: i32 = -1;

pub const AL_START_HEAD: i32 = 0;
pub const AL_START_TAIL: i32 = 1;

pub const QUICKLIST_NODE_CONTAINER_PLAIN: u8 = 1;
pub const QUICKLIST_NODE_CONTAINER_PACKED: u8 = 2;

pub const QUICKLIST_NODE_ENCODING_RAW: u8 = 1;
pub const QUICKLIST_NODE_ENCODING_LZF: u8 = 2;
pub const QUICKLIST_NOCOMPRESS: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuickListSide {
    Head,
    Tail,
}

impl QuickListSide {
    fn from_where(where_: i32) -> Option<Self> {
        match where_ {
            QUICKLIST_HEAD => Some(Self::Head),
            QUICKLIST_TAIL => Some(Self::Tail),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuickListValue<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
}

impl<'a> QuickListValue<'a> {
    pub fn to_owned_value(self) -> OwnedQuickListValue {
        match self {
            Self::Bytes(bytes) => OwnedQuickListValue::Bytes(bytes.to_vec()),
            Self::Integer(value) => OwnedQuickListValue::Integer(value),
        }
    }

    pub fn as_bytes(self, intbuf: &'a mut [u8; QUICKLIST_INTBUF_SIZE]) -> &'a [u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Integer(value) => {
                let len = i64_to_bytes(value, intbuf);
                &intbuf[..len]
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedQuickListValue {
    Bytes(Vec<u8>),
    Integer(i64),
}

impl OwnedQuickListValue {
    pub fn as_bytes<'a>(&'a self, intbuf: &'a mut [u8; QUICKLIST_INTBUF_SIZE]) -> &'a [u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Integer(value) => {
                let len = i64_to_bytes(*value, intbuf);
                &intbuf[..len]
            }
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Integer(value) => i64_to_vec(value),
        }
    }
}

impl From<OwnedListPackValue> for OwnedQuickListValue {
    fn from(value: OwnedListPackValue) -> Self {
        match value {
            OwnedListPackValue::Bytes(bytes) => Self::Bytes(bytes),
            OwnedListPackValue::Integer(value) => Self::Integer(value),
        }
    }
}

impl<'a> From<ListPackValue<'a>> for QuickListValue<'a> {
    fn from(value: ListPackValue<'a>) -> Self {
        match value {
            ListPackValue::Bytes(bytes) => Self::Bytes(bytes),
            ListPackValue::Integer(value) => Self::Integer(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QuickListNodeEntry {
    Plain(Vec<u8>),
    Packed(ListPack),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuickListNode {
    entry: QuickListNodeEntry,
    sz: usize,
    count: usize,
    encoding: u8,
    container: u8,
    recompress: bool,
    attempted_compress: bool,
    dont_compress: bool,
}

impl QuickListNode {
    fn plain(value: &[u8]) -> Self {
        Self {
            entry: QuickListNodeEntry::Plain(value.to_vec()),
            sz: value.len(),
            count: 1,
            encoding: QUICKLIST_NODE_ENCODING_RAW,
            container: QUICKLIST_NODE_CONTAINER_PLAIN,
            recompress: false,
            attempted_compress: false,
            dont_compress: false,
        }
    }

    fn packed(value: &[u8]) -> Option<Self> {
        let mut lp = ListPack::new();
        if !lp.append(value) {
            return None;
        }
        let mut node = Self {
            entry: QuickListNodeEntry::Packed(lp),
            sz: 0,
            count: 0,
            encoding: QUICKLIST_NODE_ENCODING_RAW,
            container: QUICKLIST_NODE_CONTAINER_PACKED,
            recompress: false,
            attempted_compress: false,
            dont_compress: false,
        };
        node.refresh_metadata();
        Some(node)
    }

    fn from_listpack(lp: ListPack) -> Option<Self> {
        let count = lp.len();
        if count == 0 {
            return None;
        }
        Some(Self {
            sz: lp.bytes_len(),
            count,
            entry: QuickListNodeEntry::Packed(lp),
            encoding: QUICKLIST_NODE_ENCODING_RAW,
            container: QUICKLIST_NODE_CONTAINER_PACKED,
            recompress: false,
            attempted_compress: false,
            dont_compress: false,
        })
    }

    fn is_plain(&self) -> bool {
        self.container == QUICKLIST_NODE_CONTAINER_PLAIN
    }

    fn refresh_metadata(&mut self) {
        match &self.entry {
            QuickListNodeEntry::Plain(value) => {
                self.sz = value.len();
                self.count = 1;
                self.container = QUICKLIST_NODE_CONTAINER_PLAIN;
            }
            QuickListNodeEntry::Packed(lp) => {
                self.sz = lp.bytes_len();
                self.count = lp.len();
                self.container = QUICKLIST_NODE_CONTAINER_PACKED;
            }
        }
        self.encoding = QUICKLIST_NODE_ENCODING_RAW;
    }

    fn prepend(&mut self, value: &[u8]) -> bool {
        match &mut self.entry {
            QuickListNodeEntry::Packed(lp) => {
                if !lp.prepend(value) {
                    return false;
                }
                self.refresh_metadata();
                true
            }
            QuickListNodeEntry::Plain(_) => false,
        }
    }

    fn append(&mut self, value: &[u8]) -> bool {
        match &mut self.entry {
            QuickListNodeEntry::Packed(lp) => {
                if !lp.append(value) {
                    return false;
                }
                self.refresh_metadata();
                true
            }
            QuickListNodeEntry::Plain(_) => false,
        }
    }

    fn pop(&mut self, side: QuickListSide) -> Option<OwnedQuickListValue> {
        match &mut self.entry {
            QuickListNodeEntry::Plain(value) => {
                let out = OwnedQuickListValue::Bytes(value.clone());
                value.clear();
                self.count = 0;
                self.sz = 0;
                Some(out)
            }
            QuickListNodeEntry::Packed(lp) => {
                let pos = match side {
                    QuickListSide::Head => lp.first()?,
                    QuickListSide::Tail => lp.last()?,
                };
                let before = lp.len();
                let value = lp.get_owned(pos)?;
                let _ = lp.delete(pos);
                let after = lp.len();
                if after >= before {
                    return None;
                }
                self.refresh_metadata();
                Some(value.into())
            }
        }
    }

    fn value_at(&self, offset: usize) -> Option<OwnedQuickListValue> {
        match &self.entry {
            QuickListNodeEntry::Plain(value) => {
                (offset == 0).then(|| OwnedQuickListValue::Bytes(value.clone()))
            }
            QuickListNodeEntry::Packed(lp) => {
                let pos = lp.seek(i64::try_from(offset).ok()?)?;
                lp.get_owned(pos).map(OwnedQuickListValue::from)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuickList {
    nodes: VecDeque<QuickListNode>,
    count: usize,
    fill: i32,
    compress: u32,
}

impl QuickList {
    pub fn create() -> Self {
        Self {
            nodes: VecDeque::new(),
            count: 0,
            fill: -2,
            compress: QUICKLIST_NOCOMPRESS,
        }
    }

    pub fn new() -> Self {
        Self::create()
    }

    pub fn with_options(fill: i32, compress: i32) -> Self {
        let mut quicklist = Self::create();
        quicklist.set_options(fill, compress);
        quicklist
    }

 /// Compression is recorded but not applied by this MVP.
    pub fn set_compress_depth(&mut self, compress: i32) {
        self.compress = compress.clamp(0, COMPRESS_MAX as i32) as u32;
    }

    pub fn set_fill(&mut self, fill: i32) {
        self.fill = fill.clamp(FILL_MIN, FILL_MAX);
    }

    pub fn set_options(&mut self, fill: i32, compress: i32) {
        self.set_fill(fill);
        self.set_compress_depth(compress);
    }

    pub fn fill(&self) -> i32 {
        self.fill
    }

    pub fn compress_depth(&self) -> u32 {
        self.compress
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

 /// Adapted to reusable Rust-owned storage.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.count = 0;
    }

    pub fn release(&mut self) {
        self.clear();
    }

 /// Returns true when a new head node was created, false when the existing
 /// head node absorbed the value or insertion failed.
    pub fn push_head(&mut self, value: &[u8]) -> bool {
        if is_large_element(value.len(), self.fill) {
            self.nodes.push_front(QuickListNode::plain(value));
            self.count += 1;
            return true;
        }

        if let Some(head) = self.nodes.front_mut() {
            if quicklist_node_allow_insert(head, self.fill, value.len()) && head.prepend(value) {
                self.count += 1;
                return false;
            }
        }

        let Some(node) = QuickListNode::packed(value) else {
            return false;
        };
        self.nodes.push_front(node);
        self.count += 1;
        true
    }

 /// Returns true when a new tail node was created, false when the existing
 /// tail node absorbed the value or insertion failed.
    pub fn push_tail(&mut self, value: &[u8]) -> bool {
        if is_large_element(value.len(), self.fill) {
            self.nodes.push_back(QuickListNode::plain(value));
            self.count += 1;
            return true;
        }

        if let Some(tail) = self.nodes.back_mut() {
            if quicklist_node_allow_insert(tail, self.fill, value.len()) && tail.append(value) {
                self.count += 1;
                return false;
            }
        }

        let Some(node) = QuickListNode::packed(value) else {
            return false;
        };
        self.nodes.push_back(node);
        self.count += 1;
        true
    }

    pub fn push(&mut self, value: &[u8], where_: i32) -> bool {
        match QuickListSide::from_where(where_) {
            Some(QuickListSide::Head) => self.push_head(value),
            Some(QuickListSide::Tail) => self.push_tail(value),
            None => false,
        }
    }

    pub fn push_head_sized(&mut self, value: &[u8], sz: usize) -> Option<bool> {
        Some(self.push_head(value.get(..sz)?))
    }

    pub fn push_tail_sized(&mut self, value: &[u8], sz: usize) -> Option<bool> {
        Some(self.push_tail(value.get(..sz)?))
    }

 /// Empty listpacks are ignored so the safe owner never stores zero-count nodes.
    pub fn append_listpack(&mut self, lp: ListPack) -> bool {
        let Some(node) = QuickListNode::from_listpack(lp) else {
            return false;
        };
        self.count += node.count;
        self.nodes.push_back(node);
        true
    }

    pub fn append_plain_node(&mut self, data: Vec<u8>) {
        self.nodes.push_back(QuickListNode::plain(&data));
        self.count += 1;
    }

    pub fn pop_head(&mut self) -> Option<OwnedQuickListValue> {
        self.pop_side(QuickListSide::Head)
    }

    pub fn pop_tail(&mut self) -> Option<OwnedQuickListValue> {
        self.pop_side(QuickListSide::Tail)
    }

    pub fn pop(&mut self, where_: i32) -> Option<OwnedQuickListValue> {
        self.pop_side(QuickListSide::from_where(where_)?)
    }

    fn pop_side(&mut self, side: QuickListSide) -> Option<OwnedQuickListValue> {
        if self.count == 0 {
            return None;
        }

        let value = match side {
            QuickListSide::Head => self.nodes.front_mut()?.pop(side)?,
            QuickListSide::Tail => self.nodes.back_mut()?.pop(side)?,
        };

        self.count = self.count.saturating_sub(1);
        match side {
            QuickListSide::Head if self.nodes.front().is_some_and(|node| node.count == 0) => {
                let _ = self.nodes.pop_front();
            }
            QuickListSide::Tail if self.nodes.back().is_some_and(|node| node.count == 0) => {
                let _ = self.nodes.pop_back();
            }
            _ => {}
        }
        Some(value)
    }

 /// Collapsed to direct owned lookup.
    pub fn index(&self, index: i64) -> Option<OwnedQuickListValue> {
        let count = i64::try_from(self.count).ok()?;
        let normalized = if index < 0 {
            count.checked_add(index)?
        } else {
            index
        };
        if normalized < 0 || normalized >= count {
            return None;
        }
        self.index_usize(usize::try_from(normalized).ok()?)
    }

    pub fn first(&self) -> Option<OwnedQuickListValue> {
        self.index_usize(0)
    }

    pub fn last(&self) -> Option<OwnedQuickListValue> {
        self.count
            .checked_sub(1)
            .and_then(|index| self.index_usize(index))
    }

    fn index_usize(&self, mut index: usize) -> Option<OwnedQuickListValue> {
        for node in &self.nodes {
            if index < node.count {
                return node.value_at(index);
            }
            index = index.checked_sub(node.count)?;
        }
        None
    }

    pub fn iter(&self) -> QuickListIter<'_> {
        QuickListIter {
            quicklist: self,
            front: 0,
            back: self.count,
        }
    }

    pub fn iter_rev(&self) -> Rev<QuickListIter<'_>> {
        self.iter().rev()
    }

    pub fn dup(&self) -> Self {
        self.clone()
    }

    pub fn compare_at(&self, index: i64, value: &[u8]) -> bool {
        let Some(stored) = self.index(index) else {
            return false;
        };
        let mut intbuf = [0u8; QUICKLIST_INTBUF_SIZE];
        stored.as_bytes(&mut intbuf) == value
    }

    pub fn node_limit(fill: i32) -> (usize, u32) {
        quicklist_node_limit(fill)
    }

    pub fn node_exceeds_limit(fill: i32, new_sz: usize, new_count: u32) -> bool {
        quicklist_node_exceeds_limit(fill, new_sz, new_count)
    }
}

impl Default for QuickList {
    fn default() -> Self {
        Self::create()
    }
}

#[derive(Debug, Clone)]
pub struct QuickListIter<'a> {
    quicklist: &'a QuickList,
    front: usize,
    back: usize,
}

impl Iterator for QuickListIter<'_> {
    type Item = OwnedQuickListValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front >= self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        self.quicklist.index_usize(index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back.saturating_sub(self.front);
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for QuickListIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front >= self.back {
            return None;
        }
        self.back -= 1;
        self.quicklist.index_usize(self.back)
    }
}

impl ExactSizeIterator for QuickListIter<'_> {}

fn quicklist_node_neg_fill_limit(fill: i32) -> usize {
    let offset = fill.saturating_neg().saturating_sub(1) as usize;
    let idx = offset.min(OPTIMIZATION_LEVEL.len() - 1);
    OPTIMIZATION_LEVEL[idx]
}

fn quicklist_node_limit(fill: i32) -> (usize, u32) {
    if fill >= 0 {
        let count = if fill == 0 { 1 } else { fill as u32 };
        (usize::MAX, count)
    } else {
        (quicklist_node_neg_fill_limit(fill), u32::MAX)
    }
}

fn quicklist_node_exceeds_limit(fill: i32, new_sz: usize, new_count: u32) -> bool {
    let (sz_limit, count_limit) = quicklist_node_limit(fill);
    if sz_limit != usize::MAX {
        new_sz > sz_limit
    } else if count_limit != u32::MAX {
        new_sz > SIZE_SAFETY_LIMIT || new_count > count_limit
    } else {
        true
    }
}

fn is_large_element(sz: usize, fill: i32) -> bool {
    if fill >= 0 {
        sz > SIZE_SAFETY_LIMIT
    } else {
        sz > quicklist_node_neg_fill_limit(fill)
    }
}

fn quicklist_node_allow_insert(node: &QuickListNode, fill: i32, sz: usize) -> bool {
    if node.is_plain() || is_large_element(sz, fill) {
        return false;
    }
    let Some(new_sz) = node
        .sz
        .checked_add(sz)
        .and_then(|sz| sz.checked_add(SIZE_ESTIMATE_OVERHEAD))
    else {
        return false;
    };
    let Some(new_count) = node
        .count
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
    else {
        return false;
    };
    !quicklist_node_exceeds_limit(fill, new_sz, new_count)
}

fn i64_to_vec(value: i64) -> Vec<u8> {
    let mut buf = [0u8; QUICKLIST_INTBUF_SIZE];
    let len = i64_to_bytes(value, &mut buf);
    buf[..len].to_vec()
}

fn i64_to_bytes(value: i64, buf: &mut [u8; QUICKLIST_INTBUF_SIZE]) -> usize {
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    let negative = value < 0;
    let mut n = value.unsigned_abs();
    let mut tmp = [0u8; QUICKLIST_INTBUF_SIZE];
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(values: Vec<OwnedQuickListValue>) -> Vec<Vec<u8>> {
        values
            .into_iter()
            .map(OwnedQuickListValue::into_bytes)
            .collect()
    }

    #[test]
    fn quicklist_new_has_valkey_defaults() {
        let ql = QuickList::new();

        assert_eq!(ql.count(), 0);
        assert_eq!(ql.len(), 0);
        assert_eq!(ql.node_count(), 0);
        assert_eq!(ql.fill(), -2);
        assert_eq!(ql.compress_depth(), 0);
        assert!(ql.is_empty());
    }

    #[test]
    fn quicklist_set_options_clamps_fill_and_compress() {
        let mut ql = QuickList::new();

        ql.set_options(-99, -1);
        assert_eq!(ql.fill(), FILL_MIN);
        assert_eq!(ql.compress_depth(), 0);

        ql.set_options(FILL_MAX + 100, COMPRESS_MAX as i32 + 100);
        assert_eq!(ql.fill(), FILL_MAX);
        assert_eq!(ql.compress_depth(), COMPRESS_MAX);
    }

    #[test]
    fn quicklist_push_head_and_tail_preserve_order_and_count() {
        let mut ql = QuickList::new();

        assert!(ql.push_tail(b"bravo"));
        assert!(!ql.push_tail(b"charlie"));
        assert!(!ql.push_head(b"alpha"));

        assert_eq!(ql.count(), 3);
        assert_eq!(ql.node_count(), 1);
        assert_eq!(
            bytes(ql.iter().collect()),
            vec![b"alpha".to_vec(), b"bravo".to_vec(), b"charlie".to_vec()]
        );
        assert!(ql.compare_at(1, b"bravo"));
        assert!(!ql.compare_at(1, b"alpha"));
    }

    #[test]
    fn quicklist_fill_count_limit_creates_new_nodes() {
        let mut ql = QuickList::with_options(1, 0);

        assert!(ql.push_tail(b"one"));
        assert!(ql.push_tail(b"two"));
        assert!(ql.push_head(b"zero"));

        assert_eq!(ql.count(), 3);
        assert_eq!(ql.node_count(), 3);
        assert_eq!(ql.index(0).unwrap().into_bytes(), b"zero".to_vec());
        assert_eq!(ql.index(1).unwrap().into_bytes(), b"one".to_vec());
        assert_eq!(ql.index(2).unwrap().into_bytes(), b"two".to_vec());
    }

    #[test]
    fn quicklist_index_supports_negative_offsets_across_nodes() {
        let mut ql = QuickList::with_options(2, 0);
        for value in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            ql.push_tail(value);
        }

        assert_eq!(ql.node_count(), 3);
        assert_eq!(ql.index(0).unwrap().into_bytes(), b"a".to_vec());
        assert_eq!(ql.index(3).unwrap().into_bytes(), b"d".to_vec());
        assert_eq!(ql.index(-1).unwrap().into_bytes(), b"e".to_vec());
        assert_eq!(ql.index(-5).unwrap().into_bytes(), b"a".to_vec());
        assert_eq!(ql.index(5), None);
        assert_eq!(ql.index(-6), None);
    }

    #[test]
    fn quicklist_pop_head_and_tail_update_nodes_and_count() {
        let mut ql = QuickList::with_options(1, 0);
        ql.push_tail(b"left");
        ql.push_tail(b"middle");
        ql.push_tail(b"right");

        assert_eq!(ql.pop_head().unwrap().into_bytes(), b"left".to_vec());
        assert_eq!(ql.pop_tail().unwrap().into_bytes(), b"right".to_vec());
        assert_eq!(ql.count(), 1);
        assert_eq!(ql.node_count(), 1);
        assert_eq!(
            ql.pop(QUICKLIST_TAIL).unwrap().into_bytes(),
            b"middle".to_vec()
        );
        assert_eq!(ql.pop_head(), None);
        assert!(ql.is_empty());
        assert_eq!(ql.node_count(), 0);
    }

    #[test]
    fn quicklist_large_elements_use_plain_nodes() {
        let mut ql = QuickList::new();
        let large = vec![b'x'; OPTIMIZATION_LEVEL[1] + 1];

        assert!(ql.push_tail(&large));

        assert_eq!(ql.count(), 1);
        assert_eq!(ql.node_count(), 1);
        assert!(matches!(
            ql.nodes.front().map(|node| &node.entry),
            Some(QuickListNodeEntry::Plain(value)) if value == &large
        ));
        assert_eq!(ql.pop_head().unwrap().into_bytes(), large);
    }

    #[test]
    fn quicklist_append_listpack_and_plain_node_extend_tail() {
        let mut lp = ListPack::new();
        assert!(lp.append(b"packed-a"));
        assert!(lp.append(b"packed-b"));

        let mut ql = QuickList::new();
        assert!(ql.append_listpack(lp));
        ql.append_plain_node(b"plain".to_vec());

        assert_eq!(ql.count(), 3);
        assert_eq!(ql.node_count(), 2);
        assert_eq!(
            bytes(ql.iter().collect()),
            vec![
                b"packed-a".to_vec(),
                b"packed-b".to_vec(),
                b"plain".to_vec()
            ]
        );
    }

    #[test]
    fn quicklist_iterator_is_double_ended_and_exact_sized() {
        let mut ql = QuickList::new();
        for value in [b"a".as_slice(), b"b", b"c", b"d"] {
            ql.push_tail(value);
        }

        let mut iter = ql.iter();
        assert_eq!(iter.len(), 4);
        assert_eq!(iter.next().unwrap().into_bytes(), b"a".to_vec());
        assert_eq!(iter.next_back().unwrap().into_bytes(), b"d".to_vec());
        assert_eq!(iter.len(), 2);
        assert_eq!(bytes(iter.collect()), vec![b"b".to_vec(), b"c".to_vec()]);

        assert_eq!(
            bytes(ql.iter_rev().collect()),
            vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn quicklist_integer_listpack_values_can_be_returned_as_bytes() {
        let mut ql = QuickList::new();
        ql.push_tail(b"42");

        let value = ql.index(0).unwrap();
        assert_eq!(value, OwnedQuickListValue::Integer(42));
        assert_eq!(value.into_bytes(), b"42".to_vec());
        assert_eq!(ql.pop_tail().unwrap(), OwnedQuickListValue::Integer(42));
    }

    #[test]
    fn quicklist_node_limits_match_valkey_fill_rules() {
        assert_eq!(QuickList::node_limit(0), (usize::MAX, 1));
        assert_eq!(QuickList::node_limit(3), (usize::MAX, 3));
        assert_eq!(QuickList::node_limit(-1), (4096, u32::MAX));
        assert_eq!(QuickList::node_limit(-2), (8192, u32::MAX));
        assert_eq!(QuickList::node_limit(-99), (65536, u32::MAX));

        assert!(QuickList::node_exceeds_limit(2, SIZE_SAFETY_LIMIT + 1, 1));
        assert!(QuickList::node_exceeds_limit(2, 100, 3));
        assert!(!QuickList::node_exceeds_limit(2, 100, 2));
        assert!(QuickList::node_exceeds_limit(-1, 4097, 1));
    }

    #[test]
    fn quicklist_dup_and_clear_are_independent() {
        let mut original = QuickList::new();
        original.push_tail(b"a");
        original.push_tail(b"b");

        let copy = original.dup();
        original.pop_head();
        original.clear();

        assert!(original.is_empty());
        assert_eq!(copy.count(), 2);
        assert_eq!(
            bytes(copy.iter().collect()),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }
}

// --------------------------------------------------------------------------
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Bounded safe MVP over ListPack nodes: push/pop head-tail,
//                  count, direct indexing, owned iteration, fill-limit and
//                  plain-node behavior. LZF compression, bookmarks, iterator
//                  mutation, split/merge insert paths, and object wiring are
//                  intentionally deferred.
// --------------------------------------------------------------------------

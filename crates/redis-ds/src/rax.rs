//! `RadixTree` - Redis's radix tree (rax) behavior surface.
//!
//! Upstream rax is a packed compressed radix tree whose node layout stores edge
//! bytes, child pointers, and optional value pointers in one allocation. This
//! Rust owner intentionally preserves the byte-key map semantics first: owned
//! byte keys, insert/try-insert/find/remove, size, prefix scans, and
//! lexicographic seek/iteration. The packed-node representation is deferred
//! until a later packet needs allocator parity or node-level introspection.

use std::collections::btree_map::{Entry, Iter, Range};
use std::collections::BTreeMap;
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekOp {
    Equal,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RadixTree<V = ()> {
    entries: BTreeMap<Vec<u8>, V>,
}

impl<V> RadixTree<V> {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn size(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn insert(&mut self, key: &[u8], value: V) -> Option<V> {
        self.entries.insert(key.to_vec(), value)
    }

    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<(), V> {
        match self.entries.entry(key.to_vec()) {
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
            Entry::Occupied(_) => Err(value),
        }
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    pub fn find(&self, key: &[u8]) -> Option<&V> {
        self.entries.get(key)
    }

    pub fn find_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.entries.get_mut(key)
    }

    pub fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], &V)> {
        self.entries
            .get_key_value(key)
            .map(|(key, value)| (key.as_slice(), value))
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        self.entries.remove(key)
    }

    pub fn remove_entry(&mut self, key: &[u8]) -> Option<(Vec<u8>, V)> {
        self.entries.remove_entry(key)
    }

    pub fn iter(&self) -> RadixIter<'_, V> {
        RadixIter {
            inner: self.entries.iter(),
        }
    }

    pub fn range_from(&self, key: &[u8], inclusive: bool) -> RadixRange<'_, V> {
        let start = if inclusive {
            Included(key.to_vec())
        } else {
            Excluded(key.to_vec())
        };
        RadixRange {
            inner: self.entries.range((start, Unbounded)),
        }
    }

    pub fn prefix_iter(&self, prefix: &[u8]) -> RadixRange<'_, V> {
        let start = prefix.to_vec();
        let inner = if let Some(end) = prefix_upper_bound(prefix) {
            self.entries.range(start..end)
        } else {
            self.entries.range(start..)
        };
        RadixRange { inner }
    }

    pub fn seek(&self, op: SeekOp, key: &[u8]) -> Option<(&[u8], &V)> {
        match op {
            SeekOp::Equal => self.get_key_value(key),
            SeekOp::GreaterThan => self
                .entries
                .range((Excluded(key.to_vec()), Unbounded))
                .next()
                .map(as_entry),
            SeekOp::GreaterOrEqual => self
                .entries
                .range((Included(key.to_vec()), Unbounded))
                .next()
                .map(as_entry),
            SeekOp::LessThan => self
                .entries
                .range((Unbounded, Excluded(key.to_vec())))
                .next_back()
                .map(as_entry),
            SeekOp::LessOrEqual => self
                .entries
                .range((Unbounded, Included(key.to_vec())))
                .next_back()
                .map(as_entry),
            SeekOp::First => self.entries.iter().next().map(as_entry),
            SeekOp::Last => self.entries.iter().next_back().map(as_entry),
        }
    }

    pub fn first(&self) -> Option<(&[u8], &V)> {
        self.seek(SeekOp::First, &[])
    }

    pub fn last(&self) -> Option<(&[u8], &V)> {
        self.seek(SeekOp::Last, &[])
    }

    pub fn alloc_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .entries
                .keys()
                .map(|key| key.capacity() + std::mem::size_of::<V>())
                .sum::<usize>()
    }
}

impl<V> Default for RadixTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RadixIter<'a, V> {
    inner: Iter<'a, Vec<u8>, V>,
}

impl<'a, V> Iterator for RadixIter<'a, V> {
    type Item = (&'a [u8], &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(as_entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<V> DoubleEndedIterator for RadixIter<'_, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(as_entry)
    }
}

impl<V> ExactSizeIterator for RadixIter<'_, V> {}
impl<V> FusedIterator for RadixIter<'_, V> {}

pub struct RadixRange<'a, V> {
    inner: Range<'a, Vec<u8>, V>,
}

impl<'a, V> Iterator for RadixRange<'a, V> {
    type Item = (&'a [u8], &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(as_entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<V> DoubleEndedIterator for RadixRange<'_, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(as_entry)
    }
}

impl<V> FusedIterator for RadixRange<'_, V> {}

fn as_entry<'a, V>((key, value): (&'a Vec<u8>, &'a V)) -> (&'a [u8], &'a V) {
    (key.as_slice(), value)
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != u8::MAX {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{RadixTree, SeekOp};

    #[test]
    fn rax_insert_find_overwrite_and_len() {
        let mut tree = RadixTree::new();

        assert!(tree.is_empty());
        assert_eq!(tree.insert(b"foo", 1), None);
        assert_eq!(tree.insert(b"bar", 2), None);
        assert_eq!(tree.insert(b"foo", 3), Some(1));

        assert_eq!(tree.len(), 2);
        assert_eq!(tree.size(), 2);
        assert_eq!(tree.find(b"foo"), Some(&3));
        assert_eq!(tree.find(b"bar"), Some(&2));
        assert_eq!(tree.find(b"baz"), None);
    }

    #[test]
    fn rax_try_insert_preserves_existing_value() {
        let mut tree = RadixTree::new();

        assert_eq!(tree.try_insert(b"key", b"first".to_vec()), Ok(()));
        assert_eq!(
            tree.try_insert(b"key", b"second".to_vec()),
            Err(b"second".to_vec())
        );

        assert_eq!(
            tree.find(b"key").map(Vec::as_slice),
            Some(b"first".as_slice())
        );
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn rax_remove_returns_value_and_keeps_other_prefixes() {
        let mut tree = RadixTree::new();
        tree.insert(b"foo", 1);
        tree.insert(b"foobar", 2);
        tree.insert(b"footer", 3);

        assert_eq!(tree.remove(b"foo"), Some(1));
        assert_eq!(tree.remove(b"missing"), None);

        assert_eq!(tree.find(b"foo"), None);
        assert_eq!(tree.find(b"foobar"), Some(&2));
        assert_eq!(tree.find(b"footer"), Some(&3));
        assert_eq!(tree.len(), 2);
    }

    #[test]
    fn rax_preserves_binary_keys_and_empty_key() {
        let mut tree = RadixTree::new();

        tree.insert(b"", 7);
        tree.insert(&[0, 159, 255, b'a'], 8);

        assert_eq!(tree.find(b""), Some(&7));
        assert_eq!(tree.find(&[0, 159, 255, b'a']), Some(&8));
        assert_eq!(tree.first(), Some((b"".as_slice(), &7)));
    }

    #[test]
    fn rax_iterates_in_byte_lexicographic_order() {
        let mut tree = RadixTree::new();
        for key in [b"foo".as_slice(), b"foobar", b"bar", b"", &[0xff]] {
            tree.insert(key, key.len());
        }

        let keys: Vec<Vec<u8>> = tree.iter().map(|(key, _)| key.to_vec()).collect();

        assert_eq!(
            keys,
            vec![
                b"".to_vec(),
                b"bar".to_vec(),
                b"foo".to_vec(),
                b"foobar".to_vec(),
                vec![0xff],
            ]
        );
    }

    #[test]
    fn rax_prefix_iter_limits_to_prefix_range() {
        let mut tree = RadixTree::new();
        for key in [
            b"foo".as_slice(),
            b"foobar",
            b"food",
            b"fop",
            b"bar",
            b"footer",
        ] {
            tree.insert(key, key.len());
        }

        let keys: Vec<Vec<u8>> = tree
            .prefix_iter(b"foo")
            .map(|(key, _)| key.to_vec())
            .collect();

        assert_eq!(
            keys,
            vec![
                b"foo".to_vec(),
                b"foobar".to_vec(),
                b"food".to_vec(),
                b"footer".to_vec()
            ]
        );
    }

    #[test]
    fn rax_prefix_iter_handles_empty_and_max_byte_prefixes() {
        let mut tree = RadixTree::new();
        tree.insert(b"a", 1);
        tree.insert(&[0xff], 2);
        tree.insert(&[0xff, 0x00], 3);
        tree.insert(&[0xff, 0xff], 4);

        let all: Vec<Vec<u8>> = tree.prefix_iter(b"").map(|(key, _)| key.to_vec()).collect();
        let max_prefix: Vec<Vec<u8>> = tree
            .prefix_iter(&[0xff])
            .map(|(key, _)| key.to_vec())
            .collect();

        assert_eq!(all.len(), 4);
        assert_eq!(
            max_prefix,
            vec![vec![0xff], vec![0xff, 0x00], vec![0xff, 0xff]]
        );
    }

    #[test]
    fn rax_seek_matches_valkey_iterator_operators() {
        let mut tree = RadixTree::new();
        for (idx, key) in [b"bar".as_slice(), b"foo", b"foobar", b"footer"]
            .into_iter()
            .enumerate()
        {
            tree.insert(key, idx);
        }

        assert_eq!(
            tree.seek(SeekOp::Equal, b"foo").map(|(key, _)| key),
            Some(b"foo".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::GreaterThan, b"foo").map(|(key, _)| key),
            Some(b"foobar".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::GreaterOrEqual, b"foob")
                .map(|(key, _)| key),
            Some(b"foobar".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::LessThan, b"foo").map(|(key, _)| key),
            Some(b"bar".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::LessOrEqual, b"foo").map(|(key, _)| key),
            Some(b"foo".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::First, b"ignored").map(|(key, _)| key),
            Some(b"bar".as_slice())
        );
        assert_eq!(
            tree.seek(SeekOp::Last, b"ignored").map(|(key, _)| key),
            Some(b"footer".as_slice())
        );
        assert_eq!(tree.seek(SeekOp::Equal, b"fo").map(|(key, _)| key), None);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/rax.c, reference/valkey/src/rax.h
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Safe behavior-first ordered byte-key map over BTreeMap; packed compressed node layout, callbacks, random walk, and debug node introspection remain later packets.
// ──────────────────────────────────────────────────────────────────────────

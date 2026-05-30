//! `LinkedList` - Redis's generic doubly-linked list.
//!
//! A safe owner type backed by `VecDeque`: head/tail insertion, deletion, indexing,
//! search, forward/backward iteration, rotation, duplication, and join.

use std::collections::{vec_deque, VecDeque};
use std::iter::Rev;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListDirection {
    Head,
    Tail,
}

enum ListIterInner<'a, T> {
    Head(vec_deque::Iter<'a, T>),
    Tail(Rev<vec_deque::Iter<'a, T>>),
}

pub struct ListIter<'a, T> {
    inner: ListIterInner<'a, T>,
}

impl<'a, T> ListIter<'a, T> {
    fn from_list(list: &'a LinkedList<T>, direction: ListDirection) -> Self {
        let inner = match direction {
            ListDirection::Head => ListIterInner::Head(list.entries.iter()),
            ListDirection::Tail => ListIterInner::Tail(list.entries.iter().rev()),
        };
        Self { inner }
    }
}

impl<'a, T> Iterator for ListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ListIterInner::Head(iter) => iter.next(),
            ListIterInner::Tail(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            ListIterInner::Head(iter) => iter.size_hint(),
            ListIterInner::Tail(iter) => iter.size_hint(),
        }
    }
}

impl<T> ExactSizeIterator for ListIter<'_, T> {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinkedList<T> {
    entries: VecDeque<T>,
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn front(&self) -> Option<&T> {
        self.entries.front()
    }

    pub fn back(&self) -> Option<&T> {
        self.entries.back()
    }

    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.entries.front_mut()
    }

    pub fn back_mut(&mut self) -> Option<&mut T> {
        self.entries.back_mut()
    }

    pub fn add_head(&mut self, value: T) {
        self.entries.push_front(value);
    }

    pub fn add_tail(&mut self, value: T) {
        self.entries.push_back(value);
    }

    pub fn prepend(&mut self, value: T) {
        self.add_head(value);
    }

    pub fn append(&mut self, value: T) {
        self.add_tail(value);
    }

    pub fn pop_head(&mut self) -> Option<T> {
        self.entries.pop_front()
    }

    pub fn pop_tail(&mut self) -> Option<T> {
        self.entries.pop_back()
    }

    pub fn insert_node(&mut self, old_index: usize, value: T, after: bool) -> bool {
        if old_index >= self.len() {
            return false;
        }

        let insert_index = if after {
            old_index.saturating_add(1)
        } else {
            old_index
        };
        self.entries.insert(insert_index, value);
        true
    }

    pub fn insert_before(&mut self, old_index: usize, value: T) -> bool {
        self.insert_node(old_index, value, false)
    }

    pub fn insert_after(&mut self, old_index: usize, value: T) -> bool {
        self.insert_node(old_index, value, true)
    }

    pub fn delete_at(&mut self, index: isize) -> Option<T> {
        let index = self.resolve_index(index)?;
        self.entries.remove(index)
    }

    pub fn unlink_at(&mut self, index: isize) -> Option<T> {
        self.delete_at(index)
    }

    pub fn delete(&mut self, key: &T) -> Option<T>
    where
        T: PartialEq,
    {
        let index = self.search_key(key)?;
        self.entries.remove(index)
    }

    pub fn index(&self, index: isize) -> Option<&T> {
        let index = self.resolve_index(index)?;
        self.entries.get(index)
    }

    pub fn index_mut(&mut self, index: isize) -> Option<&mut T> {
        let index = self.resolve_index(index)?;
        self.entries.get_mut(index)
    }

    pub fn search_key(&self, key: &T) -> Option<usize>
    where
        T: PartialEq,
    {
        self.search_by(|value| value == key)
    }

    pub fn search_by<F>(&self, matcher: F) -> Option<usize>
    where
        F: FnMut(&T) -> bool,
    {
        self.entries.iter().position(matcher)
    }

    pub fn contains_key(&self, key: &T) -> bool
    where
        T: PartialEq,
    {
        self.search_key(key).is_some()
    }

    pub fn iter(&self) -> vec_deque::Iter<'_, T> {
        self.entries.iter()
    }

    pub fn iter_mut(&mut self) -> vec_deque::IterMut<'_, T> {
        self.entries.iter_mut()
    }

    pub fn iter_from_head(&self) -> ListIter<'_, T> {
        self.get_iterator(ListDirection::Head)
    }

    pub fn iter_from_tail(&self) -> ListIter<'_, T> {
        self.get_iterator(ListDirection::Tail)
    }

    pub fn get_iterator(&self, direction: ListDirection) -> ListIter<'_, T> {
        ListIter::from_list(self, direction)
    }

    pub fn rotate_tail_to_head(&mut self) {
        if let Some(value) = self.entries.pop_back() {
            self.entries.push_front(value);
        }
    }

    pub fn rotate_head_to_tail(&mut self) {
        if let Some(value) = self.entries.pop_front() {
            self.entries.push_back(value);
        }
    }

    pub fn join(&mut self, other: &mut LinkedList<T>) {
        self.entries.append(&mut other.entries);
    }

    pub fn dup(&self) -> Self
    where
        T: Clone,
    {
        self.clone()
    }

    pub fn as_deque(&self) -> &VecDeque<T> {
        &self.entries
    }

    pub fn into_deque(self) -> VecDeque<T> {
        self.entries
    }

    fn resolve_index(&self, index: isize) -> Option<usize> {
        if index >= 0 {
            let index = index as usize;
            return (index < self.len()).then_some(index);
        }

        let from_tail = index.checked_neg()?.checked_sub(1)? as usize;
        if from_tail < self.len() {
            Some(self.len() - 1 - from_tail)
        } else {
            None
        }
    }
}

impl<T> Extend<T> for LinkedList<T> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.entries.extend(iter);
    }
}

impl<T> FromIterator<T> for LinkedList<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

impl<T> IntoIterator for LinkedList<T> {
    type Item = T;
    type IntoIter = vec_deque::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a LinkedList<T> {
    type Item = &'a T;
    type IntoIter = vec_deque::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut LinkedList<T> {
    type Item = &'a mut T;
    type IntoIter = vec_deque::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_values(list: &LinkedList<i32>) -> Vec<i32> {
        list.iter_from_head().copied().collect()
    }

    #[test]
    fn new_list_is_empty() {
        let list: LinkedList<i32> = LinkedList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
        assert_eq!(list.front(), None);
        assert_eq!(list.back(), None);
    }

    #[test]
    fn append_and_prepend_preserve_head_tail_order() {
        let mut list = LinkedList::new();
        list.add_tail(2);
        list.add_tail(3);
        list.add_head(1);
        list.prepend(0);
        list.append(4);

        assert_eq!(list.len(), 5);
        assert_eq!(list.front(), Some(&0));
        assert_eq!(list.back(), Some(&4));
        assert_eq!(list_values(&list), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn pop_head_and_tail_remove_from_the_requested_end() {
        let mut list = LinkedList::from_iter([1, 2, 3]);

        assert_eq!(list.pop_head(), Some(1));
        assert_eq!(list.pop_tail(), Some(3));
        assert_eq!(list.pop_tail(), Some(2));
        assert_eq!(list.pop_head(), None);
        assert!(list.is_empty());
    }

    #[test]
    fn insert_before_and_after_existing_indexes() {
        let mut list = LinkedList::from_iter([1, 3, 5]);

        assert!(list.insert_after(0, 2));
        assert!(list.insert_before(3, 4));
        assert!(list.insert_after(4, 6));
        assert!(!list.insert_before(99, 0));
        assert!(!list.insert_after(99, 0));

        assert_eq!(list_values(&list), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn delete_accepts_head_based_and_tail_based_indexes() {
        let mut list = LinkedList::from_iter([1, 2, 3, 4, 5]);

        assert_eq!(list.delete_at(0), Some(1));
        assert_eq!(list.delete_at(-1), Some(5));
        assert_eq!(list.unlink_at(-2), Some(3));
        assert_eq!(list.delete_at(10), None);
        assert_eq!(list.delete_at(-10), None);

        assert_eq!(list_values(&list), vec![2, 4]);
    }

    #[test]
    fn delete_by_key_removes_the_first_matching_value_from_head() {
        let mut list = LinkedList::from_iter([1, 2, 3, 2]);

        assert_eq!(list.delete(&2), Some(2));
        assert_eq!(list_values(&list), vec![1, 3, 2]);
        assert_eq!(list.delete(&9), None);
    }

    #[test]
    fn index_accepts_positive_and_negative_offsets() {
        let list = LinkedList::from_iter([10, 20, 30]);

        assert_eq!(list.index(0), Some(&10));
        assert_eq!(list.index(1), Some(&20));
        assert_eq!(list.index(2), Some(&30));
        assert_eq!(list.index(-1), Some(&30));
        assert_eq!(list.index(-2), Some(&20));
        assert_eq!(list.index(-3), Some(&10));
        assert_eq!(list.index(3), None);
        assert_eq!(list.index(-4), None);
    }

    #[test]
    fn index_mut_updates_the_selected_value() {
        let mut list = LinkedList::from_iter([1, 2, 3]);

        if let Some(value) = list.index_mut(-1) {
            *value = 30;
        }

        assert_eq!(list_values(&list), vec![1, 2, 30]);
    }

    #[test]
    fn search_key_and_search_by_scan_from_head() {
        let list = LinkedList::from_iter([3, 4, 5, 4]);

        assert_eq!(list.search_key(&4), Some(1));
        assert_eq!(list.search_key(&9), None);
        assert_eq!(list.search_by(|value| value % 5 == 0), Some(2));
        assert!(list.contains_key(&3));
        assert!(!list.contains_key(&8));
    }

    #[test]
    fn iterators_walk_from_head_or_tail() {
        let list = LinkedList::from_iter([1, 2, 3]);

        let forward: Vec<_> = list.iter_from_head().copied().collect();
        let backward: Vec<_> = list.iter_from_tail().copied().collect();
        let via_direction: Vec<_> = list.get_iterator(ListDirection::Tail).copied().collect();

        assert_eq!(forward, vec![1, 2, 3]);
        assert_eq!(backward, vec![3, 2, 1]);
        assert_eq!(via_direction, vec![3, 2, 1]);
    }

    #[test]
    fn iter_mut_can_edit_values_without_changing_order() {
        let mut list = LinkedList::from_iter([1, 2, 3]);

        for value in &mut list {
            *value *= 10;
        }

        assert_eq!(list_values(&list), vec![10, 20, 30]);
    }

    #[test]
    fn rotations_match_valkey_adlist_end_moves() {
        let mut list = LinkedList::from_iter([1, 2, 3]);
        list.rotate_tail_to_head();
        assert_eq!(list_values(&list), vec![3, 1, 2]);

        list.rotate_head_to_tail();
        assert_eq!(list_values(&list), vec![1, 2, 3]);

        let mut empty: LinkedList<i32> = LinkedList::new();
        empty.rotate_head_to_tail();
        empty.rotate_tail_to_head();
        assert!(empty.is_empty());
    }

    #[test]
    fn join_appends_other_and_leaves_other_empty() {
        let mut left = LinkedList::from_iter([1, 2]);
        let mut right = LinkedList::from_iter([3, 4]);

        left.join(&mut right);

        assert_eq!(list_values(&left), vec![1, 2, 3, 4]);
        assert!(right.is_empty());
    }

    #[test]
    fn dup_clones_values_into_an_independent_list() {
        let mut original = LinkedList::from_iter([1, 2, 3]);
        let copy = original.dup();

        assert_eq!(original, copy);
        original.add_tail(4);
        assert_eq!(list_values(&copy), vec![1, 2, 3]);
        assert_eq!(list_values(&original), vec![1, 2, 3, 4]);
    }

    #[test]
    fn clear_removes_all_values_but_keeps_list_reusable() {
        let mut list = LinkedList::from_iter([1, 2, 3]);

        list.clear();
        assert!(list.is_empty());
        list.add_tail(4);

        assert_eq!(list_values(&list), vec![4]);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (doubly-linked list, Redis stdlib)
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Safe owner implementation. Node handles are collapsed into
//                  index/value APIs; callbacks map to Clone, Drop, PartialEq,
//                  or search_by. Allocation hooks are not exposed.
// ──────────────────────────────────────────────────────────────────────────

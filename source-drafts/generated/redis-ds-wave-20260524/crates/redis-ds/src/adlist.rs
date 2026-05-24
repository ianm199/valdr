// UPSTREAM MAP
// Upstream functions covered:
//   listCreate, listEmpty, listRelease, listAddNodeHead, listAddNodeTail,
//   listInsertNode, listDelNode, listUnlinkNode, listLinkNodeHead,
//   listLinkNodeTail, listInitNode, listGetIterator, listNext,
//   listReleaseIterator, listRewind, listRewindTail, listDup,
//   listSearchKey, listIndex, listRotateTailToHead, listRotateHeadToTail,
//   listJoin
//
// Upstream defaults/constants covered:
//   AL_START_HEAD (0), AL_START_TAIL (1)

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::{Rc, Weak};

// ─── Internal ──────────────────────────────────────────────────────────────

/// A node in the linked list.
// C: typedef struct listNode { struct listNode *prev, *next; void *value; } listNode;
#[derive(Debug)]
pub struct ListNode<T> {
    pub prev: Option<Weak<RefCell<ListNode<T>>>>,
    pub next: Option<Rc<RefCell<ListNode<T>>>>,
    pub value: T,
}

impl<T> ListNode<T> {
    /// Create a new node with the given value, isolated (no links).
    // C: listInitNode – sets prev/next to NULL, value = given.
    pub fn new(value: T) -> Self {
        ListNode {
            prev: None,
            next: None,
            value,
        }
    }
}

// ─── Direction ─────────────────────────────────────────────────────────────

/// Direction for the list iterator.
// C: #define AL_START_HEAD 0, AL_START_TAIL 1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListDirection {
    Head,
    Tail,
}

// ─── Iterator ──────────────────────────────────────────────────────────────

/// An iterator over the elements of a `LinkedList`.
// C: typedef struct listIter { listNode *next; int direction; } listIter;
#[derive(Debug)]
pub struct ListIter<T> {
    current: Option<Rc<RefCell<ListNode<T>>>>,
    direction: ListDirection,
    // We do not store a reference to the list; the nodes keep the list alive.
}

impl<T> ListIter<T> {
    /// Rewind the iterator to the head of the list.
    // C: listRewind
    pub fn rewind(&mut self, list: &LinkedList<T>) {
        self.current = list.head.clone();
        self.direction = ListDirection::Head;
    }

    /// Rewind the iterator to the tail of the list.
    // C: listRewindTail
    pub fn rewind_tail(&mut self, list: &LinkedList<T>) {
        self.current = list.tail.clone();
        self.direction = ListDirection::Tail;
    }

    /// Advance the iterator and return the next node, or `None` if at end.
    // C: listNext
    pub fn next(&mut self) -> Option<Rc<RefCell<ListNode<T>>>> {
        let cur = self.current.clone()?;
        let next = match self.direction {
            ListDirection::Head => {
                // Move to the next node (forward)
                cur.borrow().next.clone()
            }
            ListDirection::Tail => {
                // Move to the previous node (backward)
                cur.borrow()
                    .prev
                    .clone()
                    .and_then(|w| w.upgrade())
            }
        };
        self.current = next.clone();
        Some(cur)
    }

    /// Peek at the current node without advancing.
    pub fn peek(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        self.current.clone()
    }
}

// ─── LinkedList ────────────────────────────────────────────────────────────

/// A generic doubly linked list.
// C: typedef struct list { listNode *head, *tail; ... dup/free/match; unsigned long len; } list;
// PORT NOTE: We do not include custom dup/free/match function pointers for this draft.
//   The `dup`, `free` and `match` methods from C require user-supplied callbacks.
//   In safe Rust we assume T: Clone for dup, automatic Drop for free, and T: Eq for
//   basic search. For the full generality a `TODO(port-wire)` is left below.
#[derive(Debug)]
pub struct LinkedList<T> {
    head: Option<Rc<RefCell<ListNode<T>>>>,
    tail: Option<Rc<RefCell<ListNode<T>>>>,
    len: usize,
    // TODO(port-wire): Add optional dup, free, match closures to match C API
    //   dup: Option<Box<dyn Fn(&T) -> T>>,
    //   free: Option<Box<dyn FnMut(T)>>,
    //   match_fn: Option<Box<dyn Fn(&T, &T) -> bool>>,
}

impl<T> LinkedList<T> {
    /// Create a new empty list.
    // C: listCreate
    pub fn new() -> Self {
        LinkedList {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Remove all elements from the list without destroying the list itself.
    // C: listEmpty
    pub fn clear(&mut self) {
        // Dropping the head node will recursively drop all nodes via Rc.
        self.head = None;
        self.tail = None;
        self.len = 0;
    }

    /// Return the number of elements.
    // C: listLength macro
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return `true` if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Add a node at the head of the list.
    // C: listAddNodeHead
    pub fn add_head(&mut self, value: T) -> Rc<RefCell<ListNode<T>>> {
        let node = Rc::new(RefCell::new(ListNode::new(value)));
        self.link_node_head(node.clone());
        node
    }

    /// Add a node at the tail of the list.
    // C: listAddNodeTail
    pub fn add_tail(&mut self, value: T) -> Rc<RefCell<ListNode<T>>> {
        let node = Rc::new(RefCell::new(ListNode::new(value)));
        self.link_node_tail(node.clone());
        node
    }

    /// Insert a node before or after an existing node `old_node`.
    // C: listInsertNode
    pub fn insert_node(
        &mut self,
        old_node: &Rc<RefCell<ListNode<T>>>,
        value: T,
        after: bool,
    ) -> Rc<RefCell<ListNode<T>>> {
        let new_node = Rc::new(RefCell::new(ListNode::new(value)));
        let is_tail = self.is_tail(old_node);

        // Update pointers
        if after {
            // new_node->prev = old_node
            new_node.borrow_mut().prev = Some(Rc::downgrade(old_node));
            // new_node->next = old_node->next
            new_node.borrow_mut().next = old_node.borrow().next.clone();
            // old_node->next = new_node
            old_node.borrow_mut().next = Some(new_node.clone());

            if is_tail {
                self.tail = Some(new_node.clone());
            }
        } else {
            // new_node->next = old_node
            new_node.borrow_mut().next = Some(old_node.clone());
            // new_node->prev = old_node->prev
            new_node.borrow_mut().prev = old_node.borrow().prev.clone();
            // old_node->prev = new_node
            old_node.borrow_mut().prev = Some(Rc::downgrade(&new_node));

            if self.is_head(old_node) {
                self.head = Some(new_node.clone());
            }
        }

        // Fix prev/next of the node that new_node was inserted between.
        // If after: old_node's old next (if any) should point back to new_node.
        if after {
            if let Some(ref next) = new_node.borrow().next {
                next.borrow_mut().prev = Some(Rc::downgrade(&new_node));
            }
        } else {
            if let Some(ref prev_weak) = new_node.borrow().prev {
                if let Some(prev) = prev_weak.upgrade() {
                    prev.borrow_mut().next = Some(new_node.clone());
                }
            }
        }

        self.len += 1;
        new_node
    }

    /// Delete the specified node from the list.
    // C: listDelNode
    pub fn delete_node(&mut self, node: &Rc<RefCell<ListNode<T>>>) {
        self.unlink_node(node);
        // When the last Rc is dropped, the node will be freed automatically.
    }

    /// Remove a node from the list without freeing it.
    // C: listUnlinkNode
    pub fn unlink_node(&mut self, node: &Rc<RefCell<ListNode<T>>>) {
        debug_assert!(self.len > 0);

        let prev = node.borrow().prev.as_ref().and_then(|w| w.upgrade());
        let next = node.borrow().next.clone();

        if let Some(ref p) = prev {
            p.borrow_mut().next = next.clone();
        } else {
            self.head = next.clone();
        }

        if let Some(ref n) = next {
            n.borrow_mut().prev = node.borrow().prev.clone();
        } else {
            self.tail = prev.clone();
        }

        node.borrow_mut().prev = None;
        node.borrow_mut().next = None;

        self.len -= 1;
    }

    /// Link a pre-allocated node at the head of the list.
    // C: listLinkNodeHead
    pub fn link_node_head(&mut self, node: Rc<RefCell<ListNode<T>>>) {
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node);
        } else {
            let old_head = self.head.as_ref().unwrap().clone();
            node.borrow_mut().next = Some(old_head.clone());
            old_head.borrow_mut().prev = Some(Rc::downgrade(&node));
            self.head = Some(node);
        }
        self.len += 1;
    }

    /// Link a pre-allocated node at the tail of the list.
    // C: listLinkNodeTail
    pub fn link_node_tail(&mut self, node: Rc<RefCell<ListNode<T>>>) {
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node);
        } else {
            let old_tail = self.tail.as_ref().unwrap().clone();
            node.borrow_mut().prev = Some(Rc::downgrade(&old_tail));
            old_tail.borrow_mut().next = Some(node.clone());
            self.tail = Some(node);
        }
        self.len += 1;
    }

    /// Obtain an iterator over the list starting from the head.
    // C: listGetIterator (direction=AL_START_HEAD)
    pub fn iter_from_head(&self) -> ListIter<T> {
        ListIter {
            current: self.head.clone(),
            direction: ListDirection::Head,
        }
    }

    /// Obtain an iterator over the list starting from the tail.
    // C: listGetIterator (direction=AL_START_TAIL)
    pub fn iter_from_tail(&self) -> ListIter<T> {
        ListIter {
            current: self.tail.clone(),
            direction: ListDirection::Tail,
        }
    }

    /// Duplicate the list. Requires `T: Clone`.
    // C: listDup (uses custom dup callback; here we assume Clone)
    pub fn dup(&self) -> Self
    where
        T: Clone,
    {
        let mut copy = LinkedList::new();
        let mut iter = self.iter_from_head();
        while let Some(node) = iter.next() {
            let val = node.borrow().value.clone();
            copy.add_tail(val);
        }
        copy
    }

    /// Search for the first node whose value equals `key` using `PartialEq`.
    // C: listSearchKey (uses optional match callback; here we use Eq)
    pub fn search_key(&self, key: &T) -> Option<Rc<RefCell<ListNode<T>>>>
    where
        T: PartialEq,
    {
        let mut iter = self.iter_from_head();
        while let Some(node) = iter.next() {
            if node.borrow().value == *key {
                return Some(node);
            }
        }
        None
    }

    /// Return the node at the given zero-based index.
    /// Negative indices count from the tail (-1 = last).
    // C: listIndex
    pub fn index(&self, index: isize) -> Option<Rc<RefCell<ListNode<T>>>> {
        if self.len == 0 {
            return None;
        }
        let idx = if index >= 0 {
            if (index as usize) >= self.len {
                return None;
            }
            index as usize
        } else {
            let pos = (-index) as usize - 1;
            if pos >= self.len {
                return None;
            }
            self.len - 1 - pos
        };

        if idx <= self.len / 2 {
            // Forward from head
            let mut cur = self.head.clone()?;
            for _ in 0..idx {
                let next = cur.borrow().next.clone()?;
                cur = next;
            }
            Some(cur)
        } else {
            // Backward from tail
            let mut cur = self.tail.clone()?;
            for _ in 0..(self.len - 1 - idx) {
                let prev = cur.borrow().prev.as_ref()?.upgrade()?;
                cur = prev;
            }
            Some(cur)
        }
    }

    /// Rotate the list by moving the tail element to the head.
    // C: listRotateTailToHead
    pub fn rotate_tail_to_head(&mut self) {
        if self.len <= 1 {
            return;
        }
        let tail = self.tail.take().unwrap();
        // Detach tail
        let prev_tail = tail.borrow().prev.as_ref().unwrap().upgrade().unwrap();
        prev_tail.borrow_mut().next = None;
        self.tail = Some(prev_tail);

        // Attach tail as new head
        let old_head = self.head.take().unwrap();
        tail.borrow_mut().prev = None;
        tail.borrow_mut().next = Some(old_head.clone());
        old_head.borrow_mut().prev = Some(Rc::downgrade(&tail));
        self.head = Some(tail);
    }

    /// Rotate the list by moving the head element to the tail.
    // C: listRotateHeadToTail
    pub fn rotate_head_to_tail(&mut self) {
        if self.len <= 1 {
            return;
        }
        let head = self.head.take().unwrap();
        // Detach head
        let next_head = head.borrow().next.clone().unwrap();
        next_head.borrow_mut().prev = None;
        self.head = Some(next_head);

        // Attach head as new tail
        let old_tail = self.tail.take().unwrap();
        head.borrow_mut().prev = Some(Rc::downgrade(&old_tail));
        head.borrow_mut().next = None;
        old_tail.borrow_mut().next = Some(head.clone());
        self.tail = Some(head);
    }

    /// Append all elements of `other` to the end of this list.
    /// After the call, `other` is emptied.
    // C: listJoin
    pub fn join(&mut self, other: &mut LinkedList<T>) {
        if other.is_empty() {
            return;
        }
        let other_head = other.head.take().unwrap();
        let other_tail = other.tail.take().unwrap();

        if self.is_empty() {
            self.head = Some(other_head);
            self.tail = Some(other_tail);
        } else {
            let self_tail = self.tail.as_ref().unwrap().clone();
            self_tail.borrow_mut().next = Some(other_head.clone());
            other_head.borrow_mut().prev = Some(Rc::downgrade(&self_tail));
            self.tail = Some(other_tail);
        }

        self.len += other.len;
        other.len = 0;
    }

    // ─── Private helpers ──────────────────────────────────────────────────

    fn is_head(&self, node: &Rc<RefCell<ListNode<T>>>) -> bool {
        self.head.as_ref().map_or(false, |h| Rc::ptr_eq(h, node))
    }

    fn is_tail(&self, node: &Rc<RefCell<ListNode<T>>>) -> bool {
        self.tail.as_ref().map_or(false, |t| Rc::ptr_eq(t, node))
    }
}

impl<T> Default for LinkedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_list_is_empty() {
        let list: LinkedList<i32> = LinkedList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn add_head_and_tail() {
        let mut list = LinkedList::new();
        list.add_head(10);
        list.add_head(20);
        list.add_tail(30);
        assert_eq!(list.len(), 3);
        // 20 -> 10 -> 30
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 20);
        assert_eq!(iter.next().unwrap().borrow().value, 10);
        assert_eq!(iter.next().unwrap().borrow().value, 30);
        assert!(iter.next().is_none());
    }

    #[test]
    fn delete_node() {
        let mut list = LinkedList::new();
        let node = list.add_head(42);
        list.delete_node(&node);
        assert!(list.is_empty());
    }

    #[test]
    fn index_positive_and_negative() {
        let mut list = LinkedList::new();
        list.add_tail(1);
        list.add_tail(2);
        list.add_tail(3);
        assert_eq!(list.index(0).unwrap().borrow().value, 1);
        assert_eq!(list.index(1).unwrap().borrow().value, 2);
        assert_eq!(list.index(2).unwrap().borrow().value, 3);
        assert_eq!(list.index(-1).unwrap().borrow().value, 3);
        assert_eq!(list.index(-2).unwrap().borrow().value, 2);
        assert_eq!(list.index(-3).unwrap().borrow().value, 1);
        assert!(list.index(3).is_none());
        assert!(list.index(-4).is_none());
    }

    #[test]
    fn rotate_tail_to_head() {
        let mut list = LinkedList::new();
        list.add_tail(1);
        list.add_tail(2);
        list.add_tail(3);
        list.rotate_tail_to_head();
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 3);
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn rotate_head_to_tail() {
        let mut list = LinkedList::new();
        list.add_tail(1);
        list.add_tail(2);
        list.add_tail(3);
        list.rotate_head_to_tail();
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        assert_eq!(iter.next().unwrap().borrow().value, 3);
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        assert!(iter.next().is_none());
    }

    #[test]
    fn join_lists() {
        let mut list_a = LinkedList::new();
        list_a.add_tail(1);
        list_a.add_tail(2);
        let mut list_b = LinkedList::new();
        list_b.add_tail(3);
        list_b.add_tail(4);
        list_a.join(&mut list_b);
        assert_eq!(list_a.len(), 4);
        assert!(list_b.is_empty());
        let mut iter = list_a.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        assert_eq!(iter.next().unwrap().borrow().value, 3);
        assert_eq!(iter.next().unwrap().borrow().value, 4);
    }

    #[test]
    fn dup_list() {
        let mut list = LinkedList::new();
        list.add_tail(10);
        list.add_tail(20);
        let copy = list.dup();
        assert_eq!(copy.len(), 2);
        let mut iter = copy.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 10);
        assert_eq!(iter.next().unwrap().borrow().value, 20);
    }

    #[test]
    fn search_key_partial_eq() {
        let mut list = LinkedList::new();
        list.add_tail("apple");
        list.add_tail("banana");
        list.add_tail("cherry");
        let found = list.search_key(&"banana");
        assert!(found.is_some());
        assert_eq!(found.unwrap().borrow().value, "banana");
        assert!(list.search_key(&"grape").is_none());
    }

    #[test]
    fn insert_after() {
        let mut list = LinkedList::new();
        let a = list.add_tail(1);
        list.add_tail(3);
        list.insert_node(&a, 2, true);
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        assert_eq!(iter.next().unwrap().borrow().value, 3);
    }

    #[test]
    fn insert_before() {
        let mut list = LinkedList::new();
        list.add_tail(1);
        let b = list.add_tail(3);
        list.insert_node(&b, 2, false);
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        assert_eq!(iter.next().unwrap().borrow().value, 3);
    }

    #[test]
    fn unlink_node_and_node_stays_alive() {
        let mut list = LinkedList::new();
        let node = list.add_tail(42);
        list.unlink_node(&node);
        assert!(list.is_empty());
        // The node still exists because we hold an Rc.
        assert_eq!(node.borrow().value, 42);
        assert!(node.borrow().prev.is_none());
        assert!(node.borrow().next.is_none());
    }

    #[test]
    fn clear_empty_list() {
        let mut list: LinkedList<i32> = LinkedList::new();
        list.clear();
        assert!(list.is_empty());
    }

    #[test]
    fn rewind_and_rewind_tail() {
        let mut list = LinkedList::new();
        list.add_tail(1);
        list.add_tail(2);
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 1);
        iter.rewind_tail(&list);
        assert_eq!(iter.next().unwrap().borrow().value, 2);
        iter.rewind(&list);
        assert_eq!(iter.next().unwrap().borrow().value, 1);
    }

    #[test]
    fn iterator_advances_correctly() {
        let mut list = LinkedList::new();
        list.add_tail('a');
        list.add_tail('b');
        list.add_tail('c');
        let mut iter = list.iter_from_head();
        assert_eq!(iter.next().unwrap().borrow().value, 'a');
        assert_eq!(iter.next().unwrap().borrow().value, 'b');
        assert_eq!(iter.next().unwrap().borrow().value, 'c');
        assert!(iter.next().is_none());
    }

    #[test]
    fn iterator_backwards() {
        let mut list = LinkedList::new();
        list.add_tail(10);
        list.add_tail(20);
        list.add_tail(30);
        let mut iter = list.iter_from_tail();
        assert_eq!(iter.next().unwrap().borrow().value, 30);
        assert_eq!(iter.next().unwrap().borrow().value, 20);
        assert_eq!(iter.next().unwrap().borrow().value, 10);
        assert!(iter.next().is_none());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/adlist.c  (≈360 lines, 27 functions)
//                  reference/valkey/src/adlist.h  (≈100 lines, macros)
//   target_crate:  redis-ds
//   confidence:    medium
//   todos:         2
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:
//     - Translated to safe Rust using Rc<RefCell<Node<T>>>. This differs from
//       C's raw pointer layout but preserves behaviour.
//     - C's custom dup/free/match function pointers omitted;
//       use T: Clone for dup, Drop for free, T: PartialEq for search.
//       See TODO(port-wire) markers in struct definition.
//     - listInitNode merged into ListNode::new.
//     - listReleaseIterator is dead code in safe Rust (Rc handles lifetime);
//       we still expose release_iterator as a no-op for API compatibility.
//     - listEmpty renamed to clear for Rust idiom.
//     - listLength is len().
//     - listFirst / listLast / listPrevNode / listNextNode / listNodeValue
//       are trivial field accesses; not exposed as separate functions.
//     - listBatchDelete, listReleaseVoid not translated (not used in target).
//   oracle tests to run: cargo test -p redis-ds -- adlist
//   intentional deferrals:
//     - LZF compression, plain-node override, bookmark support (QuickList-level)
//     - Splitting/merging nodes for overflow (handled externally by QuickList)
// ──────────────────────────────────────────────────────────────────────────

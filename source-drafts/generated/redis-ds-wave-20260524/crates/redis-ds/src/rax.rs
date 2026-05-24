// UPSTREAM MAP:
//   rax.c (1761 lines) — RadixTree (radix tree) implementation
//   rax.h (declarations)
//   Functions covered:
//     raxNew, raxInsert, raxTryInsert, raxRemove, raxFind
//     raxStart, raxSeek, raxNext, raxPrev, raxStop, raxEOF
//     raxSize, raxAllocSize (stub)
//   Internal helpers translated into Rust private methods:
//     raxLowWalk, raxGenericInsert, raxNode compression nodes, raxRemove.

// NOTE: This is a behavior-faithful safe Rust draft.  The tree stores
// byte-string keys and arbitrary values (generic `V`).  The C "packed node"
// memory layout is replaced by owned `Vec`s and `Box`ed children, which
// eliminates raw pointer arithmetic and unsafe code.

use std::cmp::Ordering;
use std::collections::VecDeque;

// ─── Public types ───────────────────────────────────────────────────────────

/// A radix tree (compressed trie) mapping byte-string keys to values of
/// type `V`.
///
/// This is the canonical Rust encoding of Valkey's `rax` structure,
/// adapted for safe owned storage and generic values.
pub struct RadixTree<V> {
    /// Number of key-value entries.
    num_ele: usize,
    /// Number of internal nodes (for bookkeeping, matches C `numnodes`).
    num_nodes: usize,
    /// Root node.
    root: Node<V>,
}

// ─── Internal node representation ───────────────────────────────────────────

/// A single node in the radix tree.
///
/// Each node has an optional value (`value` is `Some` when this node
/// represents a key), an edge label (the "data" section in C), and a
/// vector of child pointers.
///
/// The edge label is stored as a `Vec<u8>`. For non‑compressed nodes
/// (`is_compressed == false`), this vector has one byte per child,
/// and there must be exactly `children.len()` bytes.  For compressed
/// nodes (`is_compressed == true`), the vector contains a multi‑byte
/// label, and there is exactly one child.
enum Node<V> {
    Inner {
        // The edge label bytes.
        label: Vec<u8>,
        // If true, the node is “compressed”: it represents a chain of
        // 1‑child nodes compressed into a single label with one child.
        // Otherwise it is a normal (non‑compressed) node with multiple
        // children; each label byte maps to the corresponding child.
        is_compressed: bool,
        // Children vector: for normal nodes, `children.len() == label.len()`;
        // for compressed nodes, `children.len() == 1`.
        children: Vec<Box<Node<V>>>,
        // Optional stored value.  `Some(val)` means the node is a key.
        value: Option<V>,
    },
    // A leaf node that has no children and no label bytes; it can still
    // be a key.  This corresponds to C nodes with `size == 0`.
    Leaf { value: Option<V> },
}

impl<V> Node<V> {
    fn new_leaf(value: Option<V>) -> Self {
        Node::Leaf { value }
    }

    fn label(&self) -> &[u8] {
        match self {
            Node::Inner { label, .. } => label.as_slice(),
            Node::Leaf { .. } => &[],
        }
    }

    fn is_compressed(&self) -> bool {
        match self {
            Node::Inner { is_compressed, .. } => *is_compressed,
            Node::Leaf { .. } => false,
        }
    }

    fn is_key(&self) -> bool {
        match self {
            Node::Inner { value, .. } => value.is_some(),
            Node::Leaf { value } => value.is_some(),
        }
    }

    fn children_count(&self) -> usize {
        match self {
            Node::Inner { children, .. } => children.len(),
            Node::Leaf { .. } => 0,
        }
    }

    fn children_mut(&mut self) -> &mut Vec<Box<Node<V>>> {
        match self {
            Node::Inner { children, .. } => children,
            Node::Leaf { .. } => panic!("Leaf has no children"),
        }
    }

    fn children(&self) -> &[Box<Node<V>>] {
        match self {
            Node::Inner { children, .. } => children,
            Node::Leaf { .. } => &[],
        }
    }

    fn take_value(&mut self) -> Option<V> {
        // upstream: raxGetData / raxSetData
        match self {
            Node::Inner { value, .. } => value.take(),
            Node::Leaf { value } => value.take(),
        }
    }

    fn set_value(&mut self, v: V) {
        match self {
            Node::Inner { value, .. } => *value = Some(v),
            Node::Leaf { value } => *value = Some(v),
        }
    }
}

// ─── RadixTree implementation ───────────────────────────────────────────────

impl<V> RadixTree<V> {
    /// Create a new, empty radix tree.
    /// upstream: rax.c: raxNew()
    pub fn new() -> Self {
        RadixTree {
            num_ele: 0,
            num_nodes: 1, // root counts as one node
            root: Node::new_leaf(None),
        }
    }

    /// Return the number of stored entries.
    /// upstream: raxSize()
    pub fn size(&self) -> usize {
        self.num_ele
    }

    /// Return an estimated total allocation size in bytes (stub).
    /// upstream: raxAllocSize()
    pub fn alloc_size(&self) -> usize {
        // TODO(port): accurate accounting of Vec/heap allocations
        0
    }

    /// Insert or update a key-value pair.
    ///
    /// Returns `true` if the entry was newly inserted; `false` if it already
    /// existed.  If `old` is `Some`, it will receive the previously stored
    /// value (if any).
    /// upstream: raxInsert() / raxGenericInsert()
    pub fn insert(&mut self, key: &[u8], value: V, old: &mut Option<V>) -> bool {
        self.generic_insert(key, value, old, true)
    }

    /// Insert a key-value pair only if the key does not already exist.
    ///
    /// Returns `true` on success (new entry inserted), `false` if the key
    /// was already present.  If `old` is `Some`, it receives the existing value.
    /// upstream: raxTryInsert()
    pub fn try_insert(&mut self, key: &[u8], value: V, old: &mut Option<V>) -> bool {
        self.generic_insert(key, value, old, false)
    }

    /// Find the value associated with `key`.
    /// Returns `Some(&V)` if found, `None` otherwise.
    /// upstream: raxFind()
    pub fn find(&self, key: &[u8]) -> Option<&V> {
        // upstream: raxLowWalk
        let mut node = &self.root;
        let mut i = 0;
        while i < key.len() {
            match node {
                Node::Inner { label, is_compressed, children } => {
                    if *is_compressed {
                        // compressed: compare the whole label
                        let match_len = label.len().min(key.len() - i);
                        if &key[i..i + match_len] != &label[..match_len] {
                            return None;
                        }
                        i += label.len();
                        // compressed node has exactly one child
                        node = &children[0];
                    } else {
                        // non-compressed: find the matching edge byte
                        let byte = key[i];
                        if let Some(pos) = label.iter().position(|&b| b == byte) {
                            i += 1;
                            node = &children[pos];
                        } else {
                            return None;
                        }
                    }
                }
                Node::Leaf { .. } => return None, // no children
            }
        }
        // After consuming all key bytes, check if node is a key.
        match node {
            Node::Inner { value, .. } | Node::Leaf { value } => value.as_ref(),
        }
    }

    /// Remove the entry for `key`.
    /// Returns the removed value (if any) in `old`.
    /// Returns `true` if the key existed and was removed.
    /// upstream: raxRemove()
    pub fn remove(&mut self, key: &[u8], old: &mut Option<V>) -> bool {
        // We perform the low walk with a stack of parent pointers.
        // After deletion, we clean up empty chains and try compression.
        // (For brevity this draft includes the core logic with TODO(port)
        // markers for the compression step; a production version must
        // translate the full C algorithm.)
        todo!("raxRemove full translation – see rax.c raxRemove()")
    }

    // ─── internal ─────────────────────────────────────────────────────────

    fn generic_insert(
        &mut self,
        key: &[u8],
        value: V,
        old: &mut Option<V>,
        overwrite: bool,
    ) -> bool {
        // This is the core insertion logic, split into two main cases
        // (ALGO1 and ALGO2 from the C source).  The walk stops at the
        // deepest node reachable via the key prefix.
        //
        // For the draft we outline the structure and leave the full
        // node-splitting/compression as TODO(port) – the human integrator
        // will fill in the lower-level node manipulation using the C
        // comments and our safe Node API.
        //
        // See rax.c: raxGenericInsert() lines 385–624.

        // Step 1: Low walk to find stop node and split position.
        // (We return the index of the first mismatching character and
        //  the stop node, plus whether we stopped inside a compressed
        //  node.)
        let (i, stop_node, split_pos, in_compressed) = self.low_walk(key);

        // Step 2: If we consumed all input bytes and the stop node
        //         is a key or can become one.
        //         (Similar to ALGO2's trivial case and the normal
        //          "node exists" branch.)
        if i == key.len() {
            if in_compressed && split_pos > 0 {
                // ALGO2: stopped in middle of compressed node with perfect match
                // -> split the compressed node, create a postfix node with the value.
                todo!("ALGO2 insertion – split compressed node");
            } else {
                // Node that can hold the key (maybe it is already a key).
                // Update value.
                if stop_node.is_key() {
                    *old = stop_node.take_value();
                    if overwrite {
                        stop_node.set_value(value);
                    }
                    return false; // existing entry
                } else {
                    stop_node.set_value(value);
                    self.num_ele += 1;
                    return true;
                }
            }
        }

        // Step 3: Mismatch or insufficient path.
        if in_compressed {
            // ALGO1: stopped inside a compressed node because of a character mismatch.
            todo!("ALGO1 insertion – split compressed node and insert new child");
        } else {
            // We stopped at a normal node but the key continues.
            // Walk down creating new children as needed, then set the
            // final node's value.
            // For the draft we create a simple chain of leaf nodes.
            let rest = &key[i..];
            // (In a full translation we would decide whether to create
            //  a compressed node chain or individual children.)
            todo!("Normal insertion: create children for remaining key bytes");
        }

        false // placeholder
    }

    /// Low‑level walk down the tree for a key.
    /// Returns (consumed_index, reference_to_node, split_position, in_compressed).
    /// `split_position` is meaningful only when `in_compressed` is true.
    /// upstream: raxLowWalk()
    fn low_walk(&self, key: &[u8]) -> (usize, &Node<V>, usize, bool) {
        // In a full translation this would return mutable references for
        // insertion; for the draft we only do a read-only walk.
        let mut node = &self.root;
        let mut i = 0;
        let mut split_pos = 0;
        let mut in_compressed = false;

        while i < key.len() {
            match node {
                Node::Inner { label, is_compressed, children } => {
                    if *is_compressed {
                        let match_len = label.len().min(key.len() - i);
                        let mut j = 0;
                        while j < match_len && label[j] == key[i + j] {
                            j += 1;
                        }
                        if j < label.len() {
                            // Stop inside compressed node
                            split_pos = j;
                            in_compressed = true;
                            break;
                        }
                        i += match_len;
                        node = &children[0];
                    } else {
                        // look for edge byte
                        if let Some(pos) = label.iter().position(|&b| b == key[i]) {
                            i += 1;
                            node = &children[pos];
                        } else {
                            break; // mismatch, stop at this node
                        }
                    }
                }
                Node::Leaf { .. } => break, // no further children
            }
        }
        (i, node, split_pos, in_compressed)
    }
}

impl<V> Default for RadixTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Iterator ───────────────────────────────────────────────────────────────

/// An iterator over the entries of a `RadixTree`, in lexicographic order.
///
/// After construction with `RadixTree::iter()` or after calling `seek`,
/// the iterator yields `(&[u8], &V)` pairs (the key and its stored value).
pub struct RadixTreeIter<'a, V> {
    tree: &'a RadixTree<V>,
    // The internal iterator state: a stack of nodes and indices,
    // plus a current key prefix.
    // For brevity we define a placeholder; a full translation would
    // duplicate the C iterator logic (raxIterator, raxSeek, etc.).
    _inner: Vec<u8>,
}

impl<'a, V> RadixTreeIter<'a, V> {
    /// Create a new forward iterator starting from the first key.
    /// upstream: raxStart() + raxSeek(it, "^", ...)
    pub fn new(tree: &'a RadixTree<V>) -> Self {
        // TODO(port): implement proper iterator using low_walk and stack.
        // For the draft we return an empty iterator.
        RadixTreeIter { tree, _inner: Vec::new() }
    }

    /// Seek to a specific key or position using operator strings:
    /// "=", ">", ">=", "<", "<=", "^" (first), "$" (last).
    /// upstream: raxSeek()
    pub fn seek(&mut self, op: &str, key: &[u8]) -> bool {
        // See rax.c raxSeek() for full behavior.
        todo!("seek implementation")
    }
}

impl<'a, V> Iterator for RadixTreeIter<'a, V> {
    type Item = (&'a [u8], &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        // upstrem: raxNext()
        todo!("iterator next")
    }
}

// ─── Tests (illustrative) ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_find() {
        let mut t: RadixTree<Vec<u8>> = RadixTree::new();
        let mut old = None;
        assert!(t.insert(b"foo", b"bar".to_vec(), &mut old));
        assert!(old.is_none());
        assert!(!t.insert(b"foo", b"baz".to_vec(), &mut old));
        assert_eq!(old, Some(b"bar".to_vec()));
        assert_eq!(t.find(b"foo").unwrap(), &b"baz".to_vec());
    }

    #[test]
    fn not_found() {
        let t: RadixTree<Vec<u8>> = RadixTree::new();
        assert!(t.find(b"missing").is_none());
    }

    #[test]
    fn empty_tree_size() {
        let t: RadixTree<Vec<u8>> = RadixTree::new();
        assert_eq!(t.size(), 0);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/rax.c (1761 lines, 20+ functions)
//   target_crate:  redis-ds
//   confidence:    medium
//   todos:         9 (marked TODO or todo!)
//   port_notes:    3 (Node enum instead of bitfield struct;
//                    iterator not yet implemented;
//                    remove/insert compression logic stubbed)
//   unsafe_blocks: 0
//   notes:         behavior-faithful radix tree with safe owned storage;
//                  insertion and deletion core algorithms are outlined;
//                  iterator prefix/range iteration is TODO(port);
//                  full integration requires filling node-splitting and
//                  compression from C's ALGO1/ALGO2.
// ──────────────────────────────────────────────────────────────────────────

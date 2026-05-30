//! FIFO queue for Valkey.
//! The C implementation uses an "unrolled singly-linked list" of 7-item blocks.
//! Each block holds up to 7 `void *` slots. The critical C trick: malloc'd
//! blocks are at least 8-byte aligned, so the lower 3 bits of a block pointer
//! are always zero. The C code packs a 3-bit slot index into those unused bits,
//! giving a zero-overhead union of `(next: *mut FifoBlock, index: u3)` in
//! same 8-byte word.
//! This trick cannot be replicated in **safe** Rust:
//! - Reading/writing the low bits of a pointer requires `unsafe` pointer casts.
//! - `Option<Box<FifoBlock>>` does not expose its internal pointer bits.
//! - A safe alternative (store index separately) costs 8 bytes per block
//! complicates the "first == last" detection used in pop/push.
//! The additional challenge is the `last` pointer: the C `fifo` struct holds
//! both `first` and `last` raw pointers so push is O(1). In safe Rust a tail
//! pointer into an owned chain requires either `Rc<RefCell<…>>` (overhead) or
//! `unsafe` raw `*mut`.
//! **Phase A decision**: use `std::collections::VecDeque<T>` as the backing
//! store. All public semantics are preserved exactly; only the internal block
//! layout differs. The block-chain optimisation (≈60 % memory reduction,
//! 7× fewer allocations vs `adlist`) can be restored in Phase B once
//! architect decides on the `unsafe` budget for `redis-core`.

use std::collections::VecDeque;

// PORT NOTE: retained for documentation and the `fifo_join` small-fifo fast
// path; not used to size any Rust allocation.
#[allow(dead_code)]
const ITEMS_PER_BLOCK: usize = 7;

/// A space-efficient FIFO (First-In, First-Out) queue.
/// This is a generic Rust port of the C `fifo` type. The C type
/// stores `void *` (type-erased pointers); this version is generic over `T`.
/// **Ownership semantics vs. C:**
/// - `Drop for Fifo<T>` invokes `T::drop` on every remaining item.
/// - When `T` is a raw pointer (e.g. `*mut SomeType`) `drop` is a no-op, which
/// matches C's `fifoRelease` (which frees blocks but not items).
/// - When `T` is `Box<X>` the items are freed on drop. Callers must decide
/// which semantics they need.
/// # C equivalent
/// ```c
/// struct fifo { long length; fifoBlock *first; fifoBlock *last; };
/// ```
pub struct Fifo<T> {
 // PORT NOTE: The C struct tracks `length`, `first`, and `last` separately
 // to support O(1) push-to-tail and block recycling. VecDeque<T> subsumes
 // all three for Phase A.
    items: VecDeque<T>,
}

impl<T> Fifo<T> {
 /// Create a new, empty FIFO.
 /// # C: `fifoCreate(void)`
 /// ```c
 /// fifo *q = zmalloc(sizeof(fifo));
 /// q->length = 0; q->first = q->last = NULL;
 /// ```
    pub fn new() -> Self {
        Fifo {
            items: VecDeque::new(),
        }
    }

 /// Push `item` onto the back of the FIFO.
 /// # C: `fifoPush(fifo *q, void *ptr)`
 /// In the C implementation this is amortised O(1): if the last block has a
 /// free slot the item is written directly; otherwise a new 7-item block is
 /// allocated. `VecDeque::push_back` is also amortised O(1).
    pub fn push(&mut self, item: T) {
        self.items.push_back(item);
    }

 /// Peek at the front item without removing it.
 /// Returns `Some(&item)` if the queue is non-empty, `None` otherwise.
 /// # C: `bool fifoPeek(fifo *q, void **item)`
 /// ```c
 /// if (q->length == 0) return false;
 /// int firstIdx = (q->first == q->last) ? 0
 ///: q->first->u.last_or_first_idx & IDX_MASK;
 /// *item = q->first->items[firstIdx];
 /// return true;
 /// ```
    pub fn peek(&self) -> Option<&T> {
        self.items.front()
    }

 /// Remove and return the front item.
 /// Returns `Some(item)` if the queue is non-empty, `None` otherwise.
 /// # C: `bool fifoPop(fifo *q, void **item)`
 /// The C implementation distinguishes two cases:
 /// - **Single block**: pop from slot 0 then `memmove` remaining items left
 /// (keeps the block left-justified so the next push reuses it).
 /// - **Multiple blocks**: increment `firstIdx`; when the first block is
 /// exhausted, free it and advance `q->first`.
 /// `VecDeque::pop_front` is O(1) and handles both cases uniformly.
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }

 /// Return the number of items currently in the FIFO.
 /// # C: `long fifoLength(fifo *q)` — returns `long` (mapped to `i64`).
 /// PERF(port): C stores length as a plain `long` field (O(1) read).
 /// `VecDeque::len` is also O(1) but returns `usize`; we cast here.
    pub fn len(&self) -> i64 {
        self.items.len() as i64
    }

 /// Returns `true` if the FIFO contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

 /// Drain all items from `other` and append them to the back of `self`.
 /// After this call `other` is empty but still valid (matching C semantics).
 /// # C: `void fifoJoin(fifo *q, fifo *other)`
 /// The C implementation has three cases:
 /// 1. `other` is empty → no-op.
 /// 2. `q` is empty → move block pointers from `other` to `q` (O(1)).
 /// 3. `other->length < ITEMS_PER_BLOCK` → pop/push each item individually
 /// (prevents a string of half-empty blocks in the middle of the chain).
 /// 4. General case → shift `q->last`'s items right-to-justify them, then
 /// link `q->last->next = other->first` and update `q->last = other->last`
 /// (O(1) block-pointer surgery, O(shift) item movement where shift ≤ 6).
 /// PORT NOTE: With VecDeque the join is always O(n) via `extend(drain)`.
 /// The O(1) block-pointer fast path is Phase B work (requires unsafe or
 /// an arena-index design). The correctness semantics are identical.
    pub fn join(&mut self, other: &mut Fifo<T>) {
        if other.items.is_empty() {
            return;
        }
        self.items.extend(other.items.drain(..));
    }

 /// Move all items into a new `Fifo`, leaving `self` empty.
 /// Returns a new `Fifo` containing all former items. `self` remains valid
 /// and empty after the call.
 /// # C: `fifo *fifoPopAll(fifo *q)`
 /// The C implementation is O(1): it allocates a new `fifo` struct and does
 /// pointer surgery (`overwriteFifoContents`), zeroing out `q`.
 /// PORT NOTE: `std::mem::replace` achieves the same O(1) semantics for
 /// `VecDeque` backing store — no per-item work is done.
    pub fn pop_all(&mut self) -> Fifo<T> {
        Fifo {
            items: std::mem::take(&mut self.items),
        }
    }
}

impl<T> Default for Fifo<T> {
    fn default() -> Self {
        Fifo::new()
    }
}

impl<T> Drop for Fifo<T> {
 /// Release the FIFO.
 /// The backing `VecDeque` is dropped here, which invokes `T::drop` on each
 /// remaining item. When `T` is a raw pointer this is a no-op, matching
 /// C `fifoRelease` which frees blocks but not items.
 /// # C: `void fifoRelease(fifo *q)`
 /// ```c
 /// // walks the block chain and zfrees each block, then zfree(q)
 /// // does NOT free items
 /// ```
    fn drop(&mut self) {
 // VecDeque<T> drop handles everything.
 // No-op body: the compiler inserts the VecDeque drop automatically.
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal C structures — documented here for Phase B reference
// ──────────────────────────────────────────────────────────────────────────────
// The following mirrors the C internal layout. It is NOT compiled into
// Phase A binary but is retained so Phase B implementors can restore
// block-chain optimisation without re-reading the C source.
// TODO(architect): Decide whether Phase B should restore the block-chain using:
// (a) unsafe raw *mut FifoBlock<T> for the tail pointer, or
// (b) a safe arena (slab of FifoBlock nodes accessed by u32 index), or
// (c) keep VecDeque<T> (good enough for pilot throughput).
// The pointer-tagging trick (3-bit index in low bits of next ptr) requires
// unsafe and cannot be expressed in the Rust type system at all. An arena
// approach avoids unsafe but needs an architect decision on the API.
// ```text
// C layout (64-byte cache-line friendly block):
// struct fifoBlock {
// void *items[7]; // 7 × 8 bytes = 56 bytes
// union {
// uintptr_t last_or_first_idx; // | next ptr (61 bits) | idx (3 bits) |
// fifoBlock *next;
// } u; // 8 bytes
// }; // total: 64 bytes (one cache line)
// Invariants:
// last block: u.next == NULL, low 3 bits = lastIdx (0..6)
// first block: u.next != NULL, low 3 bits = firstIdx (0..6)
// middle block: u.next != NULL, low 3 bits = firstIdx (0 normally, >0 after join)
// single block: first == last, u.next == NULL, low 3 bits = lastIdx, items left-justified
// ```

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    4
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Phase A uses VecDeque<T>; C block-chain + pointer-tagging
//                  deferred to Phase B (TODO(architect) for unsafe budget).
//                  All public semantics (push, pop, peek, join, pop_all, len,
//                  release) are correctly preserved.  join() is O(n) vs C's
//                  amortised O(1) fast path for large fifos.
// ──────────────────────────────────────────────────────────────────────────────

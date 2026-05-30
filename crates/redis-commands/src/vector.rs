//! Generic dynamic array — port of /.
//! The C implementation is a type-erased growable array parameterised
//! runtime by a `size_t item_size` field (backed by a `void *data` pointer).
//! The Rust translation makes the element type a compile-time generic
//! parameter `T`, which eliminates all pointer arithmetic and all manual
//! memory management.
//! # API mapping
//! | C | Rust |
//! |---|---|
//! | `vectorInit(&a, alloc, item_size)` | `Vector::new(alloc)` — `item_size` is `size_of::<T>` |
//! | `vectorLen(&a)` | `a.len` |
//! | `vectorGet(&a, idx)` → `void *` | `a.get(idx)` → `Option<&T>` |
//! | `vectorPush(&a)` → `void *` to uninit | `a.push(item)` → `&mut T` (see PORT NOTE) |
//! | `vectorCleanup(&a)` | `drop(a)` — `Vec<T>` handles deallocation |
//! # PORT NOTE: `vectorPush` API change
//! The C `vectorPush` returned a raw pointer to **uninitialised** storage
//! expected the caller to write before reading. Safe Rust has no direct
//! equivalent of "pointer to uninitialised heap slot". The translation
//! requires callers to supply the initial value at push time; this is
//! semantically equivalent because every `vectorPush` call-site
//! immediately writes through the returned pointer before any read.

// assert → debug_assert!
// zmalloc/zrealloc/zfree → Vec<T>

/// Generic growable array.
/// Mirrors the C `vector` struct. The C `void *data` raw pointer and
/// `size_t item_size` runtime parameter are replaced by a monomorphised
/// `Vec<T>`; the `alloc` / `len` fields are therefore implicit
/// `Vec::capacity` / `Vec::len`.
pub struct Vector<T> {
    data: Vec<T>,
}

impl<T> Vector<T> {
 /// Initialise a new `Vector`, optionally pre-allocating `alloc` slots.
 /// ```text
 /// void vectorInit(vector *a, uint32_t alloc, size_t item_size) {
 /// assert(item_size);
 /// a->data = alloc ? zmalloc(alloc * item_size): NULL;
 /// a->alloc = alloc;
 /// a->len = 0;
 /// a->item_size = item_size;
 /// }
 /// ```
 /// The `item_size > 0` assertion maps to the Rust type system: a
 /// zero-sized `T` (`size_of::<T> == 0`) triggers a `debug_assert!`
 /// at runtime for parity with the C assert.
    pub fn new(alloc: u32) -> Self {
        debug_assert!(
            std::mem::size_of::<T>() > 0,
            "Vector<T>: zero-sized types are not supported (matches C assert(item_size))"
        );
        Vector {
            data: Vec::with_capacity(alloc as usize),
        }
    }

 /// Return the number of live elements.
    pub fn len(&self) -> u32 {
 // PERF(port): C returns u32; usize→u32 cast is safe for any realistic
 // vector size in this codebase. Flag if sizes exceed 2^32.
        self.data.len() as u32
    }

 /// Returns `true` if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

 /// Return an immutable reference to the element at `idx`, or `None` if
 /// `idx >= len`.
 /// ```text
 /// void *vectorGet(vector *a, uint32_t idx) {
 /// assert(idx < a->len);
 /// return (uint8_t *)a->data + idx * a->item_size;
 /// }
 /// ```
 /// # PORT NOTE
 /// The C function asserts on out-of-bounds and returns a raw pointer.
 /// The Rust translation returns `Option<&T>` (no panic on out-of-bounds).
 /// Call sites that relied on the C assert to catch bugs should use
 /// `get(idx).expect(...)` in test contexts only, or propagate the `None`.
    pub fn get(&self, idx: u32) -> Option<&T> {
        self.data.get(idx as usize)
    }

 /// Return a mutable reference to the element at `idx`, or `None` if
 /// `idx >= len`.
 /// Not present in the C API (callers mutated through the `void *` returned
 /// by `vectorGet`), but required for ergonomic safe-Rust usage.
    pub fn get_mut(&mut self, idx: u32) -> Option<&mut T> {
        self.data.get_mut(idx as usize)
    }

 /// Append `item` to the vector and return a mutable reference to
 /// newly-inserted slot.
 /// ```text
 /// void *vectorPush(vector *a) {
 /// if (a->len == a->alloc) {
 /// uint32_t alloc = a->alloc ? 2 * a->alloc: 8;
 /// a->data = zrealloc(a->data, alloc * a->item_size);
 /// a->alloc = alloc;
 /// }
 /// void *item = (uint8_t *)a->data + a->len * a->item_size;
 /// a->len++;
 /// return item;
 /// }
 /// ```
 /// # PORT NOTE
 /// The C function returned a `void *` to uninitialised storage; the Rust
 /// translation requires the initial value at push time. The C growth
 /// strategy (double from 8) is approximated by `Vec::push`, which uses
 /// its own amortised doubling strategy — close enough for Phase A.
 /// # PERF(port)
 /// `Vec::push` may reallocate; capacity growth is amortised O(1) as in C.
 /// The C starting capacity of 8 can be matched by calling `Vector::new(8)`
 /// rather than `Vector::default`.
    pub fn push(&mut self, item: T) -> &mut T {
 // C growth: start at 8 if alloc == 0, else double.
 // Vec::push uses its own doubling strategy; no manual realloc needed.
 // PORT NOTE: initial capacity of 8 (when empty) matches C only if
 // callers use `Vector::new(0)` and the first push triggers growth
 // Vec's default capacity (currently 0→4→8 in std). For Phase A this
 // is acceptable; Phase B can override reserve logic if needed.
        self.data.push(item);
        let last = self.data.len() - 1;
        &mut self.data[last]
    }

 /// Release the vector's internal buffer.
 /// ```text
 /// void vectorCleanup(vector *a) {
 /// if (a->data) { zfree(a->data); }
 /// }
 /// ```
 /// In C the `vector` struct itself is *not* freed by `vectorCleanup`
 /// (the caller owns it, often as a stack variable). In Rust, `Vec::drop`
 /// frees the heap buffer automatically when the `Vector` goes out
 /// scope, so this method is a no-op provided for API parity. Callers
 /// can simply drop the `Vector` instead.
    pub fn cleanup(self) {
 // Intentional no-op body: Vec<T>'s Drop impl frees the buffer.
 // handled by Vec::drop.
        drop(self);
    }

 /// Return the current allocated capacity (mirrors the C `alloc` field).
    pub fn capacity(&self) -> u32 {
        self.data.capacity() as u32
    }

 /// Consume the `Vector` and return the underlying `Vec<T>`.
    pub fn into_vec(self) -> Vec<T> {
        self.data
    }
}

impl<T> Default for Vector<T> {
    fn default() -> Self {
 // C equivalent: vectorInit(&a, 0, sizeof(T)) — zero capacity, no alloc.
        Vector { data: Vec::new() }
    }
}

// Drop is handled by Vec<T>'s own Drop impl; no custom Drop needed.

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_len() {
        let v: Vector<i32> = Vector::new(10);
        assert_eq!(v.len(), 0);
        assert!(v.capacity() >= 10);
    }

    #[test]
    fn test_push_and_get() {
        let mut v: Vector<i32> = Vector::new(4);
        let slot = v.push(42);
        assert_eq!(*slot, 42);
        assert_eq!(v.len(), 1);
        assert_eq!(v.get(0).copied(), Some(42));
        assert_eq!(v.get(1), None);
    }

    #[test]
    fn test_growth_beyond_initial_alloc() {
        let mut v: Vector<i32> = Vector::new(2);
        for i in 0..10_i32 {
            v.push(i);
        }
        assert_eq!(v.len(), 10);
        for i in 0..10_u32 {
            assert_eq!(v.get(i).copied(), Some(i as i32));
        }
    }

    #[test]
    fn test_default_is_empty() {
        let v: Vector<u64> = Vector::default();
        assert!(v.is_empty());
    }

    #[test]
    fn test_cleanup_consumes() {
        let v: Vector<f32> = Vector::new(4);
        v.cleanup(); // should compile and not panic
    }

    #[test]
    fn test_get_mut() {
        let mut v: Vector<i32> = Vector::new(4);
        v.push(10);
        if let Some(x) = v.get_mut(0) {
            *x = 99;
        }
        assert_eq!(v.get(0).copied(), Some(99));
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    3
//     1. vectorPush returned void* to uninit memory; Rust push() takes a value.
//     2. C void*/item_size type erasure → Rust generic <T> (compile-time monomorphisation).
//     3. vectorCleanup is a no-op (Vec<T> Drop handles deallocation).
//   unsafe_blocks: 0
//   notes:         Straightforward 1:1 mapping; no Redis data paths; no commands.
// ──────────────────────────────────────────────────────────────────────────────

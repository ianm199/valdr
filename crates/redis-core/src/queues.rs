//! Lock-free queue implementations: MPSC, SPMC, and SPSC.
//!
//! Three ring-buffer queue variants are provided:
//!
//! - [`MpscQueue`] – Multi-Producer Single-Consumer.
//!   Producers atomically reserve ring-buffer slots; a single consumer drains
//!   in batch. Producers that hit a full queue save their reservation in an
//!   [`MpscTicket`] and retry later.
//!
//! - [`SpmcQueue`] – Single-Producer Multi-Consumer.
//!   Sequence-number cells; consumers CAS-compete to claim populated slots.
//!   Each cell is cache-line padded to reduce false sharing.
//!
//! - [`SpscQueue`] – Single-Producer Single-Consumer.
//!   Supports batched enqueue with a deferred tail commit for amortised
//!   visibility flushes.
//!
//! # Ownership model
//!
//! All three queue types store type-erased raw pointers (`*mut T`). The queues
//! do **not** dereference stored pointers; callers are responsible for pointer
//! lifetime and aliasing discipline.
//!
//! TODO(architect): Decide whether these queues should hold `Arc<T>` (safe,
//! cheaper caller API) or remain raw-pointer queues with documented ownership
//! discipline. Raw-pointer queues are the direct C translation, but they push
//! `unsafe` to every call site. A typed `Arc`-based wrapper could live in the
//! same module once the policy is decided.
//!
//! # Cache-line alignment
//!
//! PORT NOTE: The C structs use `_Alignas(CACHE_LINE_SIZE)` on individual
//! *fields* to partition hot data across separate cache lines and prevent
//! false sharing. In Rust, `#[repr(align(N))]` applies to the *type*, so
//! each logical "cache-line group" must be wrapped in a dedicated newtype.
//! The per-queue structs below document the intended groupings in field-level
//! comments.

use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

/// Cache-line size assumed for alignment commentary and assertions.
///
/// Corresponds to `CACHE_LINE_SIZE` in Valkey's `config.h` (typically 64 on
/// x86-64 and AArch64).
pub const CACHE_LINE_SIZE: usize = 64;

// ============================================================================
// MPSC QUEUE (Multi-Producer Single-Consumer)
// ============================================================================

/// Retry ticket saved by [`MpscQueue::enqueue`] when the queue is full.
///
/// If `enqueue` returns `false`, it stores the reserved slot index here.
/// The caller must pass the same ticket on the next retry so the producer
/// fills its reserved slot rather than reserving a second one.
pub struct MpscTicket {
    pub index: usize,
    pub has_reservation: bool,
}

impl MpscTicket {
    pub fn new() -> Self {
        Self {
            index: 0,
            has_reservation: false,
        }
    }
}

impl Default for MpscTicket {
    fn default() -> Self {
        Self::new()
    }
}

/// Multi-Producer Single-Consumer lock-free ring-buffer queue.
///
/// PORT NOTE: We use `Vec<AtomicPtr<T>>` to express ownership and avoid manual allocation.
/// `AtomicPtr<T>::load` returns `*mut T`; callers must not dereference without
/// satisfying their own aliasing invariants (see module-level ownership note).
///
/// PORT NOTE: Field comments mark the intended cache-line groupings:
/// - "consumer cache line": `head` + `tail_cache`
/// - "producer cache line": `tail` + `head_cache`
/// - "buffer cache line":   `buffer` + `queue_size`
pub struct MpscQueue<T> {
    // --- consumer cache line ---
    /// Monotonically-increasing dequeue cursor (atomic; shared with producers
    /// for fullness checks via `head_cache`).
    head: AtomicUsize,
    /// Consumer-local cached copy of `tail`; avoids an atomic load on the
    /// fast dequeue path.
    tail_cache: usize,

    // --- producer cache line ---
    /// Monotonically-increasing enqueue slot counter (atomic; incremented by
    /// each producer to claim a slot).
    tail: AtomicUsize,
    /// Producer-local cached copy of `head` (atomic so multiple producers can
    /// update it safely); avoids repeated `head` loads on the enqueue path.
    head_cache: AtomicUsize,

    // --- buffer ---
    /// Ring buffer; a `null` entry means the slot is empty (not yet written
    /// or already consumed).
    buffer: Vec<AtomicPtr<T>>,
    queue_size: usize,
}

impl<T> MpscQueue<T> {
    /// Creates a new MPSC queue. `queue_size` must be a positive power of two.
    pub fn new(queue_size: usize) -> Self {
        debug_assert!(
            queue_size > 0 && (queue_size & (queue_size - 1)) == 0,
            "MpscQueue: queue_size must be a positive power of two"
        );
        let mut buffer: Vec<AtomicPtr<T>> = Vec::with_capacity(queue_size);
        for _ in 0..queue_size {
            buffer.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        Self {
            head: AtomicUsize::new(0),
            tail_cache: 0,
            tail: AtomicUsize::new(0),
            head_cache: AtomicUsize::new(0),
            buffer,
            queue_size,
        }
    }

    /// Resets all queue indices to zero (buffer deallocation handled by `Drop`).
    ///
    /// This method only resets the bookkeeping state; the buffer itself is
    /// re-zeroed so the queue can be reused.
    pub fn reset(&mut self) {
        self.head.store(0, Ordering::Relaxed);
        self.tail.store(0, Ordering::Relaxed);
        self.head_cache.store(0, Ordering::Relaxed);
        self.tail_cache = 0;
        for slot in &self.buffer {
            slot.store(std::ptr::null_mut(), Ordering::Relaxed);
        }
    }

    /// Pushes `data` into the queue. Returns `true` on success.
    ///
    /// If the queue is full, the slot reservation is recorded in `ticket` and
    /// `false` is returned. The caller must retry with the same `ticket` (and
    /// the same `data`) once space is available.
    ///
    /// Takes `&self` so that multiple producer threads can call this
    /// concurrently; all mutations are performed through atomic operations.
    pub fn enqueue(&self, data: *mut T, ticket: &mut MpscTicket) -> bool {
        debug_assert!(!data.is_null(), "MpscQueue::enqueue: data must not be null");

        // Reserve a slot, or reuse the existing reservation.
        let tail = if !ticket.has_reservation {
            self.tail.fetch_add(1, Ordering::Relaxed)
        } else {
            ticket.index
        };

        // Fullness check using producer's cached head copy.
        let head = self.head_cache.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.queue_size {
            // Cached limit reached — refresh from the true consumer head.
            let head = self.head.load(Ordering::Acquire);
            self.head_cache.store(head, Ordering::Release);

            if tail.wrapping_sub(head) >= self.queue_size {
                // Queue is full; save reservation so the caller can retry.
                ticket.index = tail;
                ticket.has_reservation = true;
                return false;
            }
        }

        // Commit data to the reserved slot.
        let slot = tail & (self.queue_size - 1);
        self.buffer[slot].store(data, Ordering::Release);

        ticket.has_reservation = false;
        true
    }

    /// Drains up to `max_jobs` items into `jobs_out`. Returns the number of
    /// items actually popped.
    ///
    /// Stops early if a slot was reserved by a producer but not yet written
    /// (data pointer is still null). This prevents reordering a later-written
    /// item ahead of an earlier reservation.
    pub fn dequeue_batch(&mut self, jobs_out: &mut Vec<*mut T>, max_jobs: usize) -> usize {
        let mut popped_count = 0usize;
        let mut head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail_cache;

        // Refresh tail cache if the queue looks empty.
        if head == tail {
            tail = self.tail.load(Ordering::Acquire);
            self.tail_cache = tail;
            if head == tail {
                return 0;
            }
        }

        let available = tail.wrapping_sub(head);
        let limit = available.min(max_jobs);

        for _ in 0..limit {
            let slot = head & (self.queue_size - 1);
            let data = self.buffer[slot].load(Ordering::Relaxed);

            // Stop if the producer has reserved this slot but not yet written.
            if data.is_null() {
                break;
            }

            jobs_out.push(data);
            popped_count += 1;
            self.buffer[slot].store(std::ptr::null_mut(), Ordering::Relaxed);
            head = head.wrapping_add(1);
        }

        if popped_count > 0 {
            self.head.store(head, Ordering::Release);
            // Ensure data visibility for the caller.
            fence(Ordering::Acquire);
        }

        popped_count
    }
}

// ============================================================================
// SPMC QUEUE (Single-Producer Multi-Consumer)
// ============================================================================

/// One ring-buffer cell for [`SpmcQueue`].
///
/// PORT NOTE: In C, `data` is a plain `void *` field. Correctness relies on the
/// release store to `sequence` (which the producer does after writing `data`)
/// creating a happens-before edge that the consumer's acquire load on `sequence`
/// observes. We promote `data` to `AtomicPtr<T>` so that `SpmcCell<T>` is
/// `Sync` without an `unsafe impl`. This costs one extra atomic operation per
/// enqueue/dequeue but avoids `UnsafeCell`.
///
/// The `#[repr(align(64))]` provides cache-line alignment; padding each cell to
/// a cache line prevents consumer false sharing.
#[repr(align(64))]
pub struct SpmcCell<T> {
    sequence: AtomicUsize,
    data: AtomicPtr<T>,
}

/// Single-Producer Multi-Consumer lock-free ring-buffer queue.
///
/// PORT NOTE: Field comments mark the intended cache-line groupings:
/// - "shared consumer cache line": `head`
/// - "producer cache line":        `tail` + `head_cache`
/// - "buffer cache line":          `buffer` + `queue_size`
pub struct SpmcQueue<T> {
    // --- shared consumer cache line (high contention) ---
    /// Atomic dequeue cursor; consumers CAS this to claim a slot.
    head: AtomicUsize,

    // --- producer cache line ---
    /// Producer-local monotonically-increasing enqueue index (non-atomic;
    /// only the single producer writes this).
    tail: usize,
    /// Producer-local cached consumer position; avoids loading `head` atomically
    /// on every enqueue.
    head_cache: usize,

    // --- buffer ---
    /// Ring buffer; cells are individually cache-line padded via `SpmcCell`'s
    /// `#[repr(align(64))]`.
    buffer: Vec<SpmcCell<T>>,
    queue_size: usize,
}

impl<T> SpmcQueue<T> {
    /// Creates a new SPMC queue. `queue_size` must be a positive power of two.
    pub fn new(queue_size: usize) -> Self {
        debug_assert!(
            queue_size > 0 && (queue_size & (queue_size - 1)) == 0,
            "SpmcQueue: queue_size must be a positive power of two"
        );
        let mut buffer: Vec<SpmcCell<T>> = Vec::with_capacity(queue_size);
        for i in 0..queue_size {
            buffer.push(SpmcCell {
                sequence: AtomicUsize::new(i),
                data: AtomicPtr::new(std::ptr::null_mut()),
            });
        }
        Self {
            head: AtomicUsize::new(0),
            tail: 0,
            head_cache: 0,
            buffer,
            queue_size,
        }
    }

    /// Resets queue bookkeeping state (buffer deallocation handled by `Drop`).
    ///
    /// Note: A reset queue is not safe to use without re-initialisation of the
    /// cells, consistent with re-initialization after a reset.
    pub fn reset(&mut self) {
        self.head.store(0, Ordering::Relaxed);
        self.tail = 0;
        self.head_cache = 0;
    }

    /// Returns `true` if the queue appears empty from the producer's view.
    ///
    /// May update `head_cache` on the slow path.
    pub fn is_empty(&mut self) -> bool {
        // Fast path: cached consumer position.
        if self.tail == self.head_cache {
            return true;
        }
        // Slow path: refresh atomic head.
        let curr_head = self.head.load(Ordering::Acquire);
        self.head_cache = curr_head;
        self.tail == curr_head
    }

    /// Returns an approximate item count (may race with consumers).
    pub fn size(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        self.tail.saturating_sub(head)
    }

    /// Pushes `data` into the next slot. Returns `false` if the slot is
    /// occupied (queue full or consumer hasn't cleared the wrapping slot yet).
    pub fn enqueue(&mut self, data: *mut T) -> bool {
        let slot = self.tail & (self.queue_size - 1);
        let cell = &self.buffer[slot];
        let seq = cell.sequence.load(Ordering::Acquire);

        // seq == tail: slot is empty and ready for this generation.
        // seq <  tail: slot still occupied by a consumer, or stale.
        if seq != self.tail {
            return false;
        }

        cell.data.store(data, Ordering::Relaxed);

        // Publish availability: advance sequence to (tail + 1) with release
        // so consumers' acquire load on sequence sees the data write.
        cell.sequence.store(self.tail + 1, Ordering::Release);
        self.tail += 1;

        true
    }

    /// Pops and returns the next item, or a null pointer if the queue is empty.
    ///
    /// Multiple consumers may call this concurrently; they compete via a
    /// weak CAS on `head`.
    ///
    /// PORT NOTE: Returns `*mut T` (null = empty).
    /// Callers need `unsafe` to dereference the returned pointer.
    ///
    /// Takes `&self` because all mutations are through atomics.
    pub fn dequeue(&self) -> *mut T {
        let mut head = self.head.load(Ordering::Relaxed);

        loop {
            let slot = head & (self.queue_size - 1);
            let cell = &self.buffer[slot];
            let seq = cell.sequence.load(Ordering::Acquire);

            let diff = (seq as isize).wrapping_sub(head.wrapping_add(1) as isize);

            if diff == 0 {
                // Slot has data; attempt to claim it via CAS on head.
                match self.head.compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let data = cell.data.load(Ordering::Relaxed);
                        // Mark slot empty for the next generation (head + queue_size).
                        cell.sequence
                            .store(head.wrapping_add(self.queue_size), Ordering::Release);
                        return data;
                    }
                    Err(actual) => {
                        // CAS failed; another consumer advanced head. Retry.
                        head = actual;
                    }
                }
            } else if diff < 0 {
                // Producer hasn't filled this slot yet — queue is empty.
                return std::ptr::null_mut();
            } else {
                // diff > 0: our local `head` is stale; reload from the atomic.
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }
}

// ============================================================================
// SPSC QUEUE (Single-Producer Single-Consumer)
// ============================================================================

/// Single-Producer Single-Consumer lock-free ring-buffer queue with batching.
///
/// The producer may enqueue multiple items with `commit = false` to batch them,
/// then call [`SpscQueue::commit`] once to make them all visible to the consumer
/// in a single atomic store.
///
/// PORT NOTE: We use `Vec<AtomicPtr<T>>` so the buffer is safe to share between
/// producer and consumer without `UnsafeCell`.
///
/// PORT NOTE: Field comments mark the intended cache-line groupings:
/// - "consumer cache line": `head` + `tail_cache`
/// - "producer cache line": `tail` + `tail_local` + `head_cache`
/// - "buffer cache line":   `buffer` + `queue_size`
pub struct SpscQueue<T> {
    // --- consumer cache line ---
    /// Shared dequeue cursor (atomic; advanced by consumer after each batch).
    head: AtomicUsize,
    /// Consumer-local cached tail; avoids an atomic load on the hot dequeue path.
    tail_cache: usize,

    // --- producer cache line ---
    /// Shared enqueue cursor (atomic; committed view visible to the consumer).
    tail: AtomicUsize,
    /// Producer-local write index; may be ahead of `tail` during batching.
    tail_local: usize,
    /// Producer-local cached head; avoids an atomic load on the fullness check.
    head_cache: usize,

    // --- buffer ---
    buffer: Vec<AtomicPtr<T>>,
    queue_size: usize,
}

impl<T> SpscQueue<T> {
    /// Creates a new SPSC queue. `queue_size` must be a positive power of two.
    pub fn new(queue_size: usize) -> Self {
        debug_assert!(
            queue_size > 0 && (queue_size & (queue_size - 1)) == 0,
            "SpscQueue: queue_size must be a positive power of two"
        );
        let mut buffer: Vec<AtomicPtr<T>> = Vec::with_capacity(queue_size);
        for _ in 0..queue_size {
            buffer.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        Self {
            head: AtomicUsize::new(0),
            tail_cache: 0,
            tail: AtomicUsize::new(0),
            tail_local: 0,
            head_cache: 0,
            buffer,
            queue_size,
        }
    }

    /// Resets all queue state (buffer deallocation handled by `Drop`).
    pub fn reset(&mut self) {
        self.head.store(0, Ordering::Relaxed);
        self.tail.store(0, Ordering::Relaxed);
        self.head_cache = 0;
        self.tail_cache = 0;
        self.tail_local = 0;
    }

    /// Returns `true` if the queue is full from the producer's perspective.
    ///
    /// As a side-effect, may flush any pending batched enqueues to the consumer
    /// so that the consumer can advance its head before we declare "full".
    pub fn is_full(&mut self) -> bool {
        let curr_tail = self.tail_local;

        if curr_tail.wrapping_sub(self.head_cache) >= self.queue_size {
            self.head_cache = self.head.load(Ordering::Acquire);

            if curr_tail.wrapping_sub(self.head_cache) >= self.queue_size {
                // Flush any pending batch before declaring full.
                let committed = self.tail.load(Ordering::Relaxed);
                if self.tail_local != committed {
                    self.tail.store(self.tail_local, Ordering::Release);
                }
                return true;
            }
        }
        false
    }

    /// Enqueues `data`. The caller must ensure the queue is not full by calling
    /// [`SpscQueue::is_full`] first.
    ///
    /// If `commit` is `true`, the shared tail is updated immediately and the
    /// item becomes visible to the consumer at once. If `false`, only
    /// `tail_local` advances (batching mode); call [`SpscQueue::commit`] to
    /// publish the batch.
    pub fn enqueue(&mut self, data: *mut T, commit: bool) {
        let slot = self.tail_local & (self.queue_size - 1);
        self.buffer[slot].store(data, Ordering::Relaxed);
        self.tail_local = self.tail_local.wrapping_add(1);

        if commit {
            self.tail.store(self.tail_local, Ordering::Release);
        }
    }

    /// Publishes any pending batched enqueues by advancing the shared tail to
    /// `tail_local`. A no-op if `tail_local` equals the already-committed tail.
    pub fn commit(&mut self) {
        let committed = self.tail.load(Ordering::Relaxed);
        if self.tail_local == committed {
            return;
        }
        self.tail.store(self.tail_local, Ordering::Release);
    }

    /// Pops up to `num_jobs` items into `jobs_out`. Returns the actual count.
    pub fn dequeue_batch(&mut self, jobs_out: &mut Vec<*mut T>, num_jobs: usize) -> usize {
        let curr_head = self.head.load(Ordering::Relaxed);
        let mut curr_tail_cache = self.tail_cache;

        if curr_head == curr_tail_cache {
            curr_tail_cache = self.tail.load(Ordering::Acquire);
            self.tail_cache = curr_tail_cache;
            if curr_head == curr_tail_cache {
                return 0;
            }
        }

        let available = curr_tail_cache.wrapping_sub(curr_head);
        let count = num_jobs.min(available);

        for i in 0..count {
            let slot = curr_head.wrapping_add(i) & (self.queue_size - 1);
            jobs_out.push(self.buffer[slot].load(Ordering::Relaxed));
        }

        self.head
            .store(curr_head.wrapping_add(count), Ordering::Release);
        count
    }

    /// Returns `true` if the queue is empty from the producer's perspective
    /// (compares `tail_local` against `head`, refreshing `head_cache` on the
    /// slow path).
    pub fn is_empty(&mut self) -> bool {
        // Fast path: producer-local tail vs cached head.
        if self.tail_local == self.head_cache {
            return true;
        }
        // Slow path: refresh head.
        let curr_head = self.head.load(Ordering::Acquire);
        self.head_cache = curr_head;
        self.tail_local == curr_head
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/queues.c  (295 lines, 16 functions)
//                  src/queues.h  (141 lines, 3 structs + mpscTicket)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    6
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         All three queue variants translated faithfully. The only
//                  material deviation from C is promoting spmcCell::data from a
//                  plain `void *` (guarded by sequence ordering) to AtomicPtr<T>
//                  to avoid `unsafe impl Sync`; and using Vec<AtomicPtr<T>> for
//                  all buffers instead of zmalloc'd raw arrays. Cache-line field
//                  groupings are documented in comments but not enforced by the
//                  type layout — Phase B should add explicit padding newtypes.
//                  Callers that dereference returned `*mut T` values still need
//                  `unsafe` at their own call sites (per the ownership TODO(architect)).
// ──────────────────────────────────────────────────────────────────────────

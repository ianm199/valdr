//! Thread-safe, two-priority-level FIFO queue — Phase A port of `mutexqueue.c` / `mutexqueue.h`.
//!
//! Wraps two [`Fifo<T>`] queues (priority and normal) behind a [`Mutex`] +
//! [`Condvar`] pair.  Priority items are popped before normal items; within
//! each level, ordering is strict FIFO.  Callers may block-wait for items.
//!
//! # Design mapping
//! The C `struct mutexQueue` embeds a `pthread_mutex_t` and a `pthread_cond_t`
//! alongside two `fifo *` pointers.  In Rust the mutex and condvar live in the
//! outer [`MutexQueue<T>`] struct; the two FIFOs are wrapped in a
//! [`MutexQueueInner<T>`] that sits behind the mutex — exactly the pattern
//! required for `Condvar::wait`.
//!
//! # C source reference
//! `src/mutexqueue.c` (159 lines, 7 functions) + `src/mutexqueue.h` (66 lines).
//! Phase assignment: **defer** (harness/file-deps.tsv lines 130–131).

use std::sync::{Condvar, Mutex, MutexGuard};

use crate::fifo::Fifo;

// ──────────────────────────────────────────────────────────────────────────
// Inner (mutex-protected) state
// ──────────────────────────────────────────────────────────────────────────

/// The data guarded by [`MutexQueue`]'s internal mutex.
///
/// Mirrors the `priority_fifo` / `normal_fifo` fields of the C `mutexQueue`
/// struct.  The synchronisation primitives themselves live in the outer wrapper.
struct MutexQueueInner<T> {
    /// Items inserted with [`MutexQueue::push_priority`].  Drained before `normal_fifo`.
    priority_fifo: Fifo<T>,
    /// Items inserted with [`MutexQueue::add`] or [`MutexQueue::add_multiple`].
    normal_fifo: Fifo<T>,
}

impl<T> MutexQueueInner<T> {
    fn new() -> Self {
        MutexQueueInner {
            priority_fifo: Fifo::new(),
            normal_fifo: Fifo::new(),
        }
    }

    /// Total item count across both FIFOs.
    ///
    /// # C: `mutexQueueLengthInternal` (static inline)
    /// Only callable while the caller holds the [`MutexGuard`] — enforced by
    /// Rust's ownership model (this method is private to the module).
    fn len(&self) -> i64 {
        self.priority_fifo.len() + self.normal_fifo.len()
    }

    /// `true` iff both FIFOs are empty.
    fn is_empty(&self) -> bool {
        self.priority_fifo.is_empty() && self.normal_fifo.is_empty()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Public interface
// ──────────────────────────────────────────────────────────────────────────

/// A thread-safe, two-priority-level FIFO queue.
///
/// Items pushed with [`push_priority`][Self::push_priority] are stored in a
/// dedicated priority FIFO and are returned by [`pop`][Self::pop] /
/// [`pop_all`][Self::pop_all] before any normal-priority items.  Within each
/// priority level, ordering is FIFO.
///
/// All methods take `&self`; interior mutability is provided by an inner
/// [`Mutex`].  The type is `Send + Sync` whenever `T: Send`.
///
/// # C equivalent
/// ```c
/// struct mutexQueue {
///     fifo           *priority_fifo;
///     fifo           *normal_fifo;
///     pthread_mutex_t mutex;
///     pthread_cond_t  notify_cv;
/// };
/// ```
pub struct MutexQueue<T> {
    inner: Mutex<MutexQueueInner<T>>,
    notify_cv: Condvar,
}

impl<T> MutexQueue<T> {
    /// Create a new, empty `MutexQueue`.
    ///
    /// # C: `mutexQueueCreate` (mutexqueue.c:23–32)
    pub fn new() -> Self {
        MutexQueue {
            inner: Mutex::new(MutexQueueInner::new()),
            notify_cv: Condvar::new(),
        }
    }

    /// Acquire the inner mutex, recovering from a poisoned state.
    ///
    /// PORT NOTE: `pthread_mutex_lock` never exposes a "poisoned" concept; Rust's
    /// `Mutex` becomes poisoned when a thread panics while holding it.  We recover
    /// via `into_inner()` because `Fifo<T>` has no invariants that a panic could
    /// permanently corrupt — the worst outcome (a partially modified FIFO) is no
    /// worse than the undefined behaviour Valkey already accepts in that scenario.
    fn lock_inner(&self) -> MutexGuard<'_, MutexQueueInner<T>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Return the total number of items in the queue (priority + normal).
    ///
    /// # C: `mutexQueueLength` (mutexqueue.c:60–69)
    pub fn len(&self) -> i64 {
        self.lock_inner().len()
    }

    /// Return `true` if the queue contains no items.
    pub fn is_empty(&self) -> bool {
        self.lock_inner().is_empty()
    }

    /// Insert `value` into the **priority** FIFO.
    ///
    /// Priority items are popped before normal items.  Within the priority FIFO,
    /// order is FIFO.  Wakes all threads waiting in a blocking [`pop`][Self::pop]
    /// or [`pop_all`][Self::pop_all] if the queue was empty before this call.
    ///
    /// # C: `mutexQueuePushPriority` (mutexqueue.c:72–82)
    pub fn push_priority(&self, value: T) {
        let mut guard = self.lock_inner();
        let was_empty = guard.is_empty();
        guard.priority_fifo.push(value);
        if was_empty {
            // PORT NOTE: C calls pthread_cond_broadcast before pthread_mutex_unlock.
            // notify_all does not require holding the guard in Rust, but we call it
            // while the guard is still in scope to preserve the same ordering.
            self.notify_cv.notify_all();
        }
        // `guard` drops (mutex unlocks) here.
    }

    /// Insert `value` at the end of the **normal** FIFO.
    ///
    /// Wakes all threads waiting in a blocking [`pop`][Self::pop] or
    /// [`pop_all`][Self::pop_all] if the queue was empty before this call.
    ///
    /// # C: `mutexQueueAdd` (mutexqueue.c:85–95)
    pub fn add(&self, value: T) {
        let mut guard = self.lock_inner();
        let was_empty = guard.is_empty();
        guard.normal_fifo.push(value);
        if was_empty {
            self.notify_cv.notify_all();
        }
    }

    /// Drain all items from `value_fifo` and append them to the **normal** FIFO.
    ///
    /// If `value_fifo` is empty this is a no-op.  After the call `value_fifo` is
    /// empty but still valid — matching C's `fifoJoin` semantics.  Wakes waiting
    /// threads if the queue was previously empty.
    ///
    /// # C: `mutexQueueAddMultiple` (mutexqueue.c:98–111)
    pub fn add_multiple(&self, value_fifo: &mut Fifo<T>) {
        // C: if (fifoLength(valueFifo) == 0) return;
        if value_fifo.is_empty() {
            return;
        }
        let mut guard = self.lock_inner();
        let was_empty = guard.is_empty();
        guard.normal_fifo.join(value_fifo);
        if was_empty {
            self.notify_cv.notify_all();
        }
    }

    /// Remove and return the first item from the queue, respecting priority order.
    ///
    /// If `blocking` is `true` the call blocks until an item is available.
    /// If `blocking` is `false` and the queue is empty, returns `None`.
    ///
    /// Items are returned from the priority FIFO first; when it is exhausted,
    /// items are returned from the normal FIFO.
    ///
    /// # C: `mutexQueuePop` (mutexqueue.c:114–135)
    /// ```c
    /// if (blocking) {
    ///     while (mutexQueueLengthInternal(mq) == 0)
    ///         pthread_cond_wait(&mq->notify_cv, &mq->mutex);
    /// }
    /// if (fifoLength(mq->priority_fifo) > 0)      fifoPop(mq->priority_fifo, &value);
    /// else if (fifoLength(mq->normal_fifo) > 0)   fifoPop(mq->normal_fifo, &value);
    /// ```
    pub fn pop(&self, blocking: bool) -> Option<T> {
        let mut guard = self.lock_inner();

        if blocking {
            // C: while (mutexQueueLengthInternal(mq) == 0) { pthread_cond_wait(...) }
            while guard.is_empty() {
                guard = self
                    .notify_cv
                    .wait(guard)
                    .unwrap_or_else(|e| e.into_inner());
            }
        }

        if !guard.priority_fifo.is_empty() {
            guard.priority_fifo.pop()
        } else {
            guard.normal_fifo.pop()
        }
    }

    /// Remove and return **all** items from the queue as a new [`Fifo<T>`].
    ///
    /// If `blocking` is `true` the call blocks until at least one item is
    /// available.  If `blocking` is `false` and the queue is empty, returns
    /// `None`.
    ///
    /// The returned `Fifo<T>` contains all priority items first (FIFO order),
    /// followed by all normal items (FIFO order).  After the call the internal
    /// FIFOs are empty but remain valid.
    ///
    /// # C: `mutexQueuePopAll` (mutexqueue.c:137–158)
    /// ```c
    /// if (mutexQueueLengthInternal(mq) > 0) {
    ///     result = fifoCreate();
    ///     fifoJoin(result, mq->priority_fifo);
    ///     fifoJoin(result, mq->normal_fifo);
    /// }
    /// ```
    pub fn pop_all(&self, blocking: bool) -> Option<Fifo<T>> {
        let mut guard = self.lock_inner();

        if blocking {
            // C: while (mutexQueueLengthInternal(mq) == 0) { pthread_cond_wait(...) }
            while guard.is_empty() {
                guard = self
                    .notify_cv
                    .wait(guard)
                    .unwrap_or_else(|e| e.into_inner());
            }
        }

        if guard.is_empty() {
            return None;
        }

        let mut result = Fifo::new();
        // PORT NOTE: C calls fifoJoin which drains the source in-place.
        // Fifo::join matches that semantics: priority items land first.
        result.join(&mut guard.priority_fifo);
        result.join(&mut guard.normal_fifo);
        Some(result)
    }

    /// Assert that the queue is empty, then consume and release it.
    ///
    /// Corresponds to `mutexQueueRelease` in C.  In Rust, memory cleanup for the
    /// inner FIFOs and synchronisation primitives is handled automatically by
    /// [`Drop`]; this method exists solely to enforce the C precondition that the
    /// queue must be empty before release, and to broadcast to any waiting threads
    /// before teardown.
    ///
    /// # Panics (debug builds only)
    /// Panics if the queue is not empty, matching the C `assert`.
    ///
    /// # C: `mutexQueueRelease` (mutexqueue.c:39–51)
    /// ```c
    /// assert(mutexQueueLength(theQueue) == 0);
    /// pthread_mutex_destroy(&mq->mutex);
    /// pthread_cond_broadcast(&mq->notify_cv);
    /// pthread_cond_destroy(&mq->notify_cv);
    /// fifoRelease(mq->priority_fifo);
    /// fifoRelease(mq->normal_fifo);
    /// zfree(mq);
    /// ```
    pub fn release(self) {
        // C: assert(mutexQueueLength(theQueue) == 0);
        debug_assert!(
            self.is_empty(),
            "mutexQueueRelease: queue must be empty before release"
        );
        // C: pthread_cond_broadcast — wake any threads still blocking on wait.
        // In Rust this is a best-effort courtesy signal; if no threads are waiting
        // it is a harmless no-op.  The Condvar is cleaned up by Drop.
        self.notify_cv.notify_all();
        // `self` is consumed here; Drop runs on `inner` and `notify_cv` automatically.
    }
}

impl<T> Default for MutexQueue<T> {
    fn default() -> Self {
        MutexQueue::new()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/mutexqueue.c  (159 lines, 7 functions)
//                  + src/mutexqueue.h (66 lines)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    4
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Straight mapping of pthread mutex+condvar to std::sync::Mutex
//                  + Condvar with generic T replacing void*.  MutexGuard poison
//                  recovery via into_inner() matches C's no-error-check semantics.
//                  notify_all() called while guard is still in scope to preserve
//                  C's broadcast-before-unlock ordering.  release() takes self
//                  (consumes the queue) rather than *mut; Drop handles cleanup.
//                  lib.rs module declaration not modified — architect to add
//                  `pub mod mutexqueue;`.
// ──────────────────────────────────────────────────────────────────────────

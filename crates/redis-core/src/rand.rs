//! Pseudo-random number generation — drand48 variant.
//!
//! Port of `src/rand.c` (101 lines, 3 functions) and `src/rand.h` from Valkey.
//!
//! Implements a 48-bit linear congruential PRNG derived from `drand48()`.
//! The original purpose is to give Lua's `math.random()` a deterministic,
//! cross-platform sequence regardless of the platform libc `rand()`.
//!
//! # Design note (global state)
//!
//! The C source keeps mutable state in module-level C statics (`x[3]`, `a[3]`,
//! `c`). Mutable statics in Rust require `unsafe`. To stay within the pilot
//! crate `unsafe` budget of zero, the global state is kept in a `thread_local!`
//! `RefCell<RandState>`. This is faithful to the Phase-2 single-threaded
//! constraint (PORTING.md §2 #7). If the server ever calls these functions
//! from multiple threads, migrate to `std::sync::Mutex<RandState>`.
//!
//! Callers that need isolated PRNG state (e.g. tests, multi-tenant scripting)
//! can create their own `RandState` and call its methods directly.

// PORT NOTE: C module-level statics translated to thread_local! + RefCell to
// avoid unsafe. Behaviour is identical under single-threaded use (Phase 2).

use std::cell::RefCell;

// ── Public constant ───────────────────────────────────────────────────────────

/// Maximum value returned by [`server_lrand48`].
///
/// C: `rand.h` — `#define SERVER_LRAND48_MAX INT32_MAX`
pub const SERVER_LRAND48_MAX: i32 = i32::MAX;

// ── PRNG constants ────────────────────────────────────────────────────────────

/// Bit width of each state word; also the shift used to split 32-bit products.
const N: u32 = 16;

/// 16-bit mask: `(1 << 15) + (1 << 15) - 1 == 0xFFFF`.
///
/// C: `#define MASK ((1 << (N - 1)) + (1 << (N - 1)) - 1)`
const MASK: u32 = 0xFFFF;

/// Default initial state word 0.  C: `#define X0 0x330E`
const X0: u32 = 0x330E;
/// Default initial state word 1.  C: `#define X1 0xABCD`
const X1: u32 = 0xABCD;
/// Default initial state word 2.  C: `#define X2 0x1234`
const X2: u32 = 0x1234;

/// Multiplier word 0.  C: `#define A0 0xE66D`
const A0: u32 = 0xE66D;
/// Multiplier word 1.  C: `#define A1 0xDEEC`
const A1: u32 = 0xDEEC;
/// Multiplier word 2.  C: `#define A2 0x5`
const A2: u32 = 0x5;

/// Addend.  C: `#define C 0xB`
const C_VAL: u32 = 0xB;

// ── RandState ─────────────────────────────────────────────────────────────────

/// Internal 48-bit linear-congruential PRNG state, split into three 16-bit words.
///
/// Corresponds to the C statics `x[3]`, `a[3]`, and `c` in `rand.c`.
///
/// Normally accessed through the module-level functions [`server_lrand48`] and
/// [`server_srand48`], which use the thread-local global instance. Create a
/// `RandState` directly when isolated, reproducible sequences are needed.
#[derive(Debug, Clone)]
pub struct RandState {
    /// Current state (`x` in C). Three 16-bit words stored in u32.
    x: [u32; 3],
    /// Multiplier (`a` in C). Three 16-bit words stored in u32.
    a: [u32; 3],
    /// Addend (`c` in C). 16-bit value stored in u32.
    c: u32,
}

impl RandState {
    /// Creates a new `RandState` with the Valkey default initial values.
    pub fn new() -> Self {
        RandState {
            x: [X0, X1, X2],
            a: [A0, A1, A2],
            c: C_VAL,
        }
    }

    /// Seeds this state from a 32-bit signed integer.
    ///
    /// C: `rand.c:84-86, serverSrand48`
    /// ```c
    /// void serverSrand48(int32_t seedval) {
    ///     SEED(X0, LOW(seedval), HIGH(seedval));
    /// }
    /// ```
    pub fn seed(&mut self, seedval: i32) {
        let s = seedval as u32;
        self.x = [X0, low(s), low(s >> N)];
        self.a = [A0, A1, A2];
        self.c = C_VAL;
    }

    /// Advances the PRNG and returns the next pseudo-random value in
    /// `[0, `[`SERVER_LRAND48_MAX`]`]`.
    ///
    /// C: `rand.c:79-82, serverLrand48`
    /// ```c
    /// int32_t serverLrand48(void) {
    ///     next();
    ///     return (((int32_t)x[2] << (N - 1)) + (x[1] >> 1));
    /// }
    /// ```
    pub fn lrand48(&mut self) -> i32 {
        self.advance();
        ((self.x[2] as i32) << (N as i32 - 1)) + (self.x[1] >> 1) as i32
    }

    /// Advances the 48-bit LCG state by one step.
    ///
    /// C: `rand.c:88-100, next` (static)
    ///
    /// The function implements one step of the recurrence:
    /// `X_{n+1} = (a * X_n + c) mod 2^48`
    /// where all arithmetic is done on 16-bit limbs to avoid 64-bit
    /// division and to remain portable across the original target systems.
    ///
    /// Each C macro is expanded inline with a `// C:` comment.
    fn advance(&mut self) {
        // C: MUL(a[0], x[0], p)
        //    { int32_t l = (long)a[0] * (long)x[0]; p[0]=LOW(l); p[1]=HIGH(l); }
        let prod_ax = self.a[0].wrapping_mul(self.x[0]);
        let mut p = [low(prod_ax), high(prod_ax)];

        // C: ADDEQU(p[0], c, carry0)
        //    carry0 = CARRY(p[0], c);  p[0] = LOW(p[0] + c);
        let (p0_new, carry0) = addequ(p[0], self.c);
        p[0] = p0_new;

        // C: ADDEQU(p[1], carry0, carry1)
        //    carry1 = CARRY(p[1], carry0);  p[1] = LOW(p[1] + carry0);
        let (p1_new0, carry1) = addequ(p[1], carry0);
        p[1] = p1_new0;

        // C: MUL(a[0], x[1], q)
        let prod_ax1 = self.a[0].wrapping_mul(self.x[1]);
        let q = [low(prod_ax1), high(prod_ax1)];

        // C: ADDEQU(p[1], q[0], carry0)
        //    carry0 = CARRY(p[1], q[0]);  p[1] = LOW(p[1] + q[0]);
        let (p1_new1, carry0) = addequ(p[1], q[0]);
        p[1] = p1_new1;

        // C: MUL(a[1], x[0], r)
        let prod_a1x = self.a[1].wrapping_mul(self.x[0]);
        let r = [low(prod_a1x), high(prod_a1x)];

        // C: x[2] = LOW(carry0 + carry1 + CARRY(p[1], r[0])
        //              + q[1] + r[1]
        //              + a[0]*x[2] + a[1]*x[1] + a[2]*x[0]);
        //
        // All old values of x[] are read here, before any x[] assignment below.
        let carry_p1_r0 = carry(p[1], r[0]);
        let new_x2 = low(carry0
            .wrapping_add(carry1)
            .wrapping_add(carry_p1_r0)
            .wrapping_add(q[1])
            .wrapping_add(r[1])
            .wrapping_add(self.a[0].wrapping_mul(self.x[2]))
            .wrapping_add(self.a[1].wrapping_mul(self.x[1]))
            .wrapping_add(self.a[2].wrapping_mul(self.x[0])));

        // C: x[1] = LOW(p[1] + r[0]);
        let new_x1 = low(p[1].wrapping_add(r[0]));

        // C: x[0] = LOW(p[0]);
        let new_x0 = low(p[0]);

        self.x[2] = new_x2;
        self.x[1] = new_x1;
        self.x[0] = new_x0;
    }
}

impl Default for RandState {
    fn default() -> Self {
        Self::new()
    }
}

// ── C macro helpers (module-private) ─────────────────────────────────────────

/// `LOW(x)` — keep the bottom 16 bits of `x`.
///
/// C: `#define LOW(x) ((unsigned)(x) & MASK)`
#[inline]
fn low(x: u32) -> u32 {
    x & MASK
}

/// `HIGH(x)` — shift right by 16 and keep the bottom 16 bits.
///
/// C: `#define HIGH(x) LOW((x) >> N)`
#[inline]
fn high(x: u32) -> u32 {
    low(x >> N)
}

/// `CARRY(x, y)` — returns `1` if `x + y > MASK`, else `0`.
///
/// C: `#define CARRY(x, y) ((int32_t)(x) + (long)(y) > MASK)`
///
/// Widened to u64 to avoid any overflow before the comparison.
#[inline]
fn carry(x: u32, y: u32) -> u32 {
    if (x as u64) + (y as u64) > MASK as u64 {
        1
    } else {
        0
    }
}

/// `ADDEQU(x, y, z)` — returns `(LOW(x + y), CARRY(x, y))`.
///
/// C: `#define ADDEQU(x, y, z) (z = CARRY(x, (y)), x = LOW(x + (y)))`
///
/// The C macro mutates both `x` and `z` in-place via statement expression;
/// here we return a tuple `(new_x, carry_out)` instead.
#[inline]
fn addequ(x: u32, y: u32) -> (u32, u32) {
    let c = carry(x, y);
    let new_x = low(x.wrapping_add(y));
    (new_x, c)
}

// ── Module-global state (thread-local) ────────────────────────────────────────

thread_local! {
    /// Thread-local mirror of the C file-scope statics `x[3]`, `a[3]`, `c`.
    ///
    /// Initialised on first access with the default Valkey constants,
    /// matching the C `static uint32_t x[3] = {X0, X1, X2}, a[3] = {A0, A1, A2}, c = C;`.
    static GLOBAL_STATE: RefCell<RandState> = RefCell::new(RandState::new());
}

// ── Public free functions (matching the C ABI names) ─────────────────────────

/// Advances the global PRNG and returns the next pseudo-random value in
/// `[0, `[`SERVER_LRAND48_MAX`]`]`.
///
/// C: `rand.c:79-82, serverLrand48`
pub fn server_lrand48() -> i32 {
    GLOBAL_STATE.with(|s| s.borrow_mut().lrand48())
}

/// Return a pseudo-random floating point value in `[0, 1)`.
///
/// Valkey's LFU counter logic uses this helper as a probability draw. The
/// implementation is intentionally derived from `server_lrand48()` so it keeps
/// the same deterministic PRNG source as the translated C code.
pub fn rand_float() -> f64 {
    (server_lrand48() as f64) / ((SERVER_LRAND48_MAX as f64) + 1.0)
}

/// Seeds the global PRNG with `seedval`.
///
/// C: `rand.c:84-86, serverSrand48`
pub fn server_srand48(seedval: i32) {
    GLOBAL_STATE.with(|s| s.borrow_mut().seed(seedval));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_sequence_is_deterministic() {
        let mut a = RandState::new();
        let mut b = RandState::new();
        a.seed(42);
        b.seed(42);
        for _ in 0..100 {
            assert_eq!(a.lrand48(), b.lrand48());
        }
    }

    #[test]
    fn output_within_range() {
        let mut s = RandState::new();
        s.seed(0);
        for _ in 0..1000 {
            let v = s.lrand48();
            assert!(v >= 0, "lrand48 must be non-negative");
            assert!(v <= SERVER_LRAND48_MAX);
        }
    }

    #[test]
    fn rand_float_is_unit_interval() {
        server_srand48(123);
        for _ in 0..1000 {
            let v = rand_float();
            assert!(v >= 0.0);
            assert!(v < 1.0);
        }
    }

    #[test]
    fn different_seeds_differ() {
        let mut a = RandState::new();
        let mut b = RandState::new();
        a.seed(1);
        b.seed(2);
        let seq_a: Vec<i32> = (0..10).map(|_| a.lrand48()).collect();
        let seq_b: Vec<i32> = (0..10).map(|_| b.lrand48()).collect();
        assert_ne!(seq_a, seq_b);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/rand.c  (101 lines, 3 functions: serverLrand48,
//                               serverSrand48, next [static])
//                  src/rand.h  (39 lines, merged)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    1  (global mutable state → thread_local! + RefCell)
//   unsafe_blocks: 0
//   notes:         Arithmetic is uint32 wrapping throughout; CARRY uses u64
//                  widening to avoid overflow before comparison. The C
//                  file-scope statics are mirrored as thread_local to stay
//                  within the pilot unsafe budget. Self-test covers
//                  determinism, range, and seed independence.
// ──────────────────────────────────────────────────────────────────────────

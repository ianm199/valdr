//! MT19937-64: 64-bit Mersenne Twister pseudorandom number generator.
//!
//! Ported from Valkey `src/mt19937-64.c` (2004/9/29 version).
//! Original authors: Takuji Nishimura and Makoto Matsumoto.
//!
//! Copyright (C) 2004, Makoto Matsumoto and Takuji Nishimura. All rights reserved.
//! Licensed under the 3-clause BSD-style license included in the original source.
//!
//! References:
//! - T. Nishimura, "Tables of 64-bit Mersenne Twisters", ACM TOMACS 10 (2000) 348–357.
//! - M. Matsumoto and T. Nishimura, "Mersenne Twister: a 623-dimensionally
//!   equidistributed uniform pseudorandom number generator", ACM TOMACS 8 (1998) 3–30.
//!
//! PORT NOTE: The C source uses two file-scope `static` variables (`mt[]` and `mti`)
//! making the generator an implicit process-global singleton. This Rust port
//! encapsulates all mutable state in [`Mt19937_64`], eliminating global mutable
//! state and the need for `unsafe`. Callers that require the C-compatible single-
//! instance behaviour should wrap `Mt19937_64` in `std::sync::Mutex`.
//!
//! PORT NOTE: `key_length` is a separate parameter in `init_by_array64` in C
//! (`unsigned long long`). Here it is derived from the slice length, removing
//! the redundant argument and the possibility of it mismatching the slice.

// ──────────────────────────────────────────────────────────────────────────
// MT19937-64 algorithmic constants
// C: #define NN 312  #define MM 156  #define MATRIX_A ...  #define UM ...  #define LM ...
// ──────────────────────────────────────────────────────────────────────────

/// Degree of recurrence.
const NN: usize = 312;

/// Middle word index.
const MM: usize = 156;

/// Constant vector A used in the twist transformation.
const MATRIX_A: u64 = 0xB5026F5AA96619E9;

/// Mask for the most significant 33 bits.
const UM: u64 = 0xFFFFFFFF80000000;

/// Mask for the least significant 31 bits.
const LM: u64 = 0x7FFFFFFF;

/// Lookup table for the twist matrix multiplication.
///
/// `MAG01[x & 1]` is XORed with the twisted word.
/// C: static unsigned long long mag01[2] = {0ULL, MATRIX_A}
const MAG01: [u64; 2] = [0, MATRIX_A];

// ──────────────────────────────────────────────────────────────────────────
// Generator state
// ──────────────────────────────────────────────────────────────────────────

/// 64-bit Mersenne Twister PRNG.
///
/// All generator state is self-contained. Construct with [`Mt19937_64::new`],
/// then seed with [`Mt19937_64::init_genrand64`] or [`Mt19937_64::init_by_array64`]
/// before drawing numbers. If no explicit seed is given, the first call to
/// [`Mt19937_64::genrand64_int64`] auto-seeds with the default value `5489`.
pub struct Mt19937_64 {
    /// The state vector.
    mt: [u64; NN],
    /// Current index into `mt`. `NN + 1` means the state is uninitialised.
    mti: usize,
}

impl Mt19937_64 {
    /// Creates an uninitialised generator.
    ///
    /// `mti` is set to `NN + 1`, which signals that the state array has not
    /// been seeded. [`Mt19937_64::genrand64_int64`] will auto-seed on the first
    /// call if the caller has not done so explicitly.
    pub fn new() -> Self {
        Self {
            mt: [0u64; NN],
            mti: NN + 1,
        }
    }

    /// Seeds the generator from a single 64-bit value.
    ///
    /// C: mt19937-64.c:73–78, init_genrand64
    pub fn init_genrand64(&mut self, seed: u64) {
        self.mt[0] = seed;
        self.mti = 1;
        while self.mti < NN {
            let prev = self.mt[self.mti - 1];
            self.mt[self.mti] = 6364136223846793005_u64
                .wrapping_mul(prev ^ (prev >> 62))
                .wrapping_add(self.mti as u64);
            self.mti += 1;
        }
        // After the loop mti == NN, signalling a fully-seeded but not-yet-
        // twisted state. The next genrand64_int64 call will run the twist.
    }

    /// Seeds the generator from an array of 64-bit words.
    ///
    /// C: mt19937-64.c:83–105, init_by_array64
    /// PORT NOTE: `key_length` is derived from `init_key.len()` rather than
    /// passed separately, removing the redundant C argument.
    pub fn init_by_array64(&mut self, init_key: &[u64]) {
        let key_length = init_key.len() as u64;
        self.init_genrand64(19650218);

        let mut i: usize = 1;
        let mut j: usize = 0;

        // C: k = (NN > key_length ? NN : key_length);
        let mut k: u64 = if NN as u64 > key_length {
            NN as u64
        } else {
            key_length
        };

        while k > 0 {
            let prev = self.mt[i - 1];
            // C: mt[i] = (mt[i] ^ ((mt[i-1] ^ (mt[i-1] >> 62)) * 3935559000370003845ULL))
            //              + init_key[j] + j;   /* non linear */
            self.mt[i] = (self.mt[i] ^ (prev ^ (prev >> 62)).wrapping_mul(3935559000370003845_u64))
                .wrapping_add(init_key[j])
                .wrapping_add(j as u64);

            i += 1;
            j += 1;

            if i >= NN {
                self.mt[0] = self.mt[NN - 1];
                i = 1;
            }
            if j >= key_length as usize {
                j = 0;
            }
            k -= 1;
        }

        let mut k: u64 = NN as u64 - 1;
        while k > 0 {
            let prev = self.mt[i - 1];
            // C: mt[i] = (mt[i] ^ ((mt[i-1] ^ (mt[i-1] >> 62)) * 2862933555777941757ULL))
            //              - i;   /* non linear */
            self.mt[i] = (self.mt[i] ^ (prev ^ (prev >> 62)).wrapping_mul(2862933555777941757_u64))
                .wrapping_sub(i as u64);

            i += 1;
            if i >= NN {
                self.mt[0] = self.mt[NN - 1];
                i = 1;
            }
            k -= 1;
        }

        // C: mt[0] = 1ULL << 63;   /* MSB is 1; assuring non-zero initial array */
        self.mt[0] = 1_u64 << 63;
    }

    /// Generates a pseudorandom `u64` uniformly distributed on [0, 2^64 − 1].
    ///
    /// If the generator has not been seeded, it is auto-seeded with `5489` on
    /// the first call, matching the C behaviour.
    ///
    /// C: mt19937-64.c:108–143, genrand64_int64
    pub fn genrand64_int64(&mut self) -> u64 {
        if self.mti >= NN {
            // If init_genrand64() has not been called, use the default seed.
            if self.mti == NN + 1 {
                self.init_genrand64(5489);
            }

            // Generate NN words at one time (the "twist").

            // First leg: indices 0 .. NN-MM-1 (mt[i+MM] is in-bounds).
            // C: for (i=0; i<NN-MM; i++)
            for i in 0..(NN - MM) {
                let x = (self.mt[i] & UM) | (self.mt[i + 1] & LM);
                self.mt[i] = self.mt[i + MM] ^ (x >> 1) ^ MAG01[(x & 1) as usize];
            }

            // Second leg: indices NN-MM .. NN-2 (mt[i+(MM-NN)] wraps to low indices).
            // C: for (; i<NN-1; i++) { mt[i] = mt[i+(MM-NN)] ^ ... }
            // MM-NN = 156-312 = -156 in C (signed).  In Rust with usize arithmetic:
            // i + MM - NN is always >= 0 for i in [NN-MM, NN-1).
            for i in (NN - MM)..(NN - 1) {
                let x = (self.mt[i] & UM) | (self.mt[i + 1] & LM);
                self.mt[i] = self.mt[i + MM - NN] ^ (x >> 1) ^ MAG01[(x & 1) as usize];
            }

            // Final element.
            let x = (self.mt[NN - 1] & UM) | (self.mt[0] & LM);
            self.mt[NN - 1] = self.mt[MM - 1] ^ (x >> 1) ^ MAG01[(x & 1) as usize];

            self.mti = 0;
        }

        let mut x = self.mt[self.mti];
        self.mti += 1;

        // Tempering.
        x ^= (x >> 29) & 0x5555555555555555_u64;
        x ^= (x << 17) & 0x71D67FFFEDA60000_u64;
        x ^= (x << 37) & 0xFFF7EEE000000000_u64;
        x ^= x >> 43;

        x
    }

    /// Generates a pseudorandom `i64` uniformly distributed on [0, 2^63 − 1].
    ///
    /// C: mt19937-64.c:146–149, genrand64_int63
    pub fn genrand64_int63(&mut self) -> i64 {
        (self.genrand64_int64() >> 1) as i64
    }

    /// Generates a pseudorandom `f64` uniformly distributed on the **closed**
    /// interval [0, 1].
    ///
    /// C: mt19937-64.c:152–155, genrand64_real1
    pub fn genrand64_real1(&mut self) -> f64 {
        (self.genrand64_int64() >> 11) as f64 * (1.0 / 9007199254740991.0)
    }

    /// Generates a pseudorandom `f64` uniformly distributed on the **half-open**
    /// interval [0, 1).
    ///
    /// C: mt19937-64.c:158–161, genrand64_real2
    pub fn genrand64_real2(&mut self) -> f64 {
        (self.genrand64_int64() >> 11) as f64 * (1.0 / 9007199254740992.0)
    }

    /// Generates a pseudorandom `f64` uniformly distributed on the **open**
    /// interval (0, 1).
    ///
    /// C: mt19937-64.c:164–167, genrand64_real3
    pub fn genrand64_real3(&mut self) -> f64 {
        ((self.genrand64_int64() >> 12) as f64 + 0.5) * (1.0 / 4503599627370496.0)
    }

    // TODO(port): genrand64_real4 is declared in mt19937-64.h but has no
    // implementation in the C source file and no formula comment. Omitted.
}

impl Default for Mt19937_64 {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/mt19937-64.c  (187 lines, 6 functions) + mt19937-64.h
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Pure algorithmic port; global C static state replaced by
//                  Mt19937_64 struct. Wrapping arithmetic preserves u64 twos-
//                  complement overflow semantics matching C unsigned behaviour.
//                  genrand64_real4 (header-only declaration) omitted with TODO.
// ──────────────────────────────────────────────────────────────────────────

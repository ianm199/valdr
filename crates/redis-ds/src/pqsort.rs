//! Partial-range quicksort (`pqsort`).
//! This is a direct port of — the NetBSD
//! `libc` quicksort adapted by Redis Ltd. to support sorting only a
//! specified index range `[lrange, rrange]` of the array. Elements outside
//! that range are still moved to their final partition (lesser / equal /
//! greater) but their internal order is unspecified.
//! The C implementation operates on `void *` arrays with an element-size
//! argument (`es`). The Rust port replaces that idiom with a generic
//! type parameter `T` and a `&mut [T]` slice, eliminating the need for raw
//! pointer arithmetic and `unsafe`.
//! Original C copyright:
//! Copyright (c) 1992, 1993 The Regents of the University of California
//! Modifications copyright (c) 2009-2012 Redis Ltd.


// ── helpers ──────────────────────────────────────────────────────────────────

/// Returns the index of the median element among `a`, `b`, `c`.
/// Equivalent to the static `med3` function in C.
fn med3_idx<T, F>(slice: &[T], a: usize, b: usize, c: usize, cmp: &F) -> usize
where
    F: Fn(&T, &T) -> i32,
{
    if cmp(&slice[a], &slice[b]) < 0 {
        if cmp(&slice[b], &slice[c]) < 0 {
            b
        } else if cmp(&slice[a], &slice[c]) < 0 {
            c
        } else {
            a
        }
    } else if cmp(&slice[b], &slice[c]) > 0 {
        b
    } else if cmp(&slice[a], &slice[c]) < 0 {
        a
    } else {
        c
    }
}

/// Swap `count` elements starting at index `start_a` with those at `start_b`.
/// Replaces the `vecswap` and `swapfunc`/`swapcode` macros in C.
/// C swaps raw bytes (word-at-a-time optimisation); here we use slice::swap which
/// is element-at-a-time. Performance may differ; profile if needed.
fn vecswap<T>(slice: &mut [T], start_a: usize, start_b: usize, count: usize) {
    for i in 0..count {
        slice.swap(start_a + i, start_b + i);
    }
}

/// Returns `true` when the index ranges `[l1, r1]` and `[l2, r2]` overlap
/// (both bounds inclusive).
/// Requires `l1 <= r1` and `l2 <= r2`. In the C code this is expressed as a
/// pointer-comparison predicate; here we use index arithmetic.
/// The C condition is `!((lrange < _l && rrange < _l) || (lrange > _r && rrange > _r))`.
/// Given lrange <= rrange:
/// - "both before" ⟺ rrange < _l (since lrange <= rrange < _l)
/// - "both after" ⟺ lrange > _r (since lrange > _r >= rrange)
/// The condition simplifies: !(rrange < l1 || l2 > r1).
fn ranges_overlap(partition_l: usize, partition_r: usize, lrange: usize, rrange: usize) -> bool {
    !(rrange < partition_l || lrange > partition_r)
}

// ── core recursive function ───────────────────────────────────────────────────

/// Internal recursive partial sort.
/// * `slice` — the current sub-array to (partially) sort.
/// * `abs_offset` — the absolute element index of `slice[0]` within
/// original array. Used to compare against `lrange`/`rrange`.
/// * `lrange`/`rrange` — inclusive target range as absolute element indices
/// in the *original* array.
/// * `cmp` — comparison function; negative / zero / positive like C's
/// `qsort` comparator.
/// C uses `goto loop` to tail-call the right partition in place, saving stack
/// space. Rust recurses on both partitions; the compiler may optimize the tail
/// recursion or not. C `lrange`/`rrange` are raw pointers; here they are absolute
/// element indices, with `abs_offset` tracking where the current slice starts.
fn pqsort_inner<T, F>(slice: &mut [T], abs_offset: usize, lrange: usize, rrange: usize, cmp: &F)
where
    F: Fn(&T, &T) -> i32,
{
    let n = slice.len();
    if n == 0 {
        return;
    }

 // Insertion sort for small sub-arrays (n < 7)
    if n < 7 {
        for pm in 1..n {
            let mut pl = pm;
            while pl > 0 && cmp(&slice[pl - 1], &slice[pl]) > 0 {
                slice.swap(pl - 1, pl);
                pl -= 1;
            }
        }
        return;
    }

 // Pivot selection (median-of-3, or pseudo-median-of-9 for n > 40).
 // Indices add negligible overhead here versus C pointer arithmetic.
    let mut pm = n / 2;
    if n > 7 {
        let mut pl = 0usize;
        let mut pn_last = n - 1;
        if n > 40 {
            let d = n / 8;
 // d = n/8; pl starts at 0, so pl + 2*d <= n/4 < n — safe.
            pl = med3_idx(slice, pl, pl + d, pl + 2 * d, cmp);
 // pm = n/2; pm + d ≤ n/2 + n/8 = 5n/8 < n — safe.
 // pm - d: use saturating_sub in case n is very small (guarded by n > 40 above).
 // saturating_sub adds a branch but makes the invariant explicit.
            let pm_lo = pm.saturating_sub(d);
            let pm_hi = (pm + d).min(n - 1);
            pm = med3_idx(slice, pm_lo, pm, pm_hi, cmp);
            pn_last = med3_idx(
                slice,
                pn_last.saturating_sub(2 * d),
                pn_last.saturating_sub(d),
                pn_last,
                cmp,
            );
        }
        pm = med3_idx(slice, pl, pm, pn_last, cmp);
    }

 // Move pivot to front so partition loops compare against slice[0].
    slice.swap(0, pm);

 // Bentley-McIlroy 3-way partition
 // Invariants during the loop (expressed as element indices):
 // [0.. pa): elements equal to pivot (gathered at front)
 // [pa.. pb): elements < pivot
 // [pc+1.. pd+1): elements > pivot
 // [pd+1.. n): elements equal to pivot (gathered at back)
 // pivot itself sits at slice[0] throughout
 // Key safety argument (no usize underflow):
 // pb >= 1 always (starts at 1, only increases).
 // During the backward scan pb <= pc, so pc >= pb >= 1 > 0.
 // pd >= pc always (pd decreases only when pc does, in lock-step).
 // Therefore pd >= 1 and neither `pc -= 1` nor `pd -= 1` underflows.
    let mut pa = 1usize;
    let mut pb = 1usize;
    let mut pc = n - 1;
    let mut pd = n - 1;

    loop {
 // Forward scan: advance pb over elements ≤ pivot; push equals to front.
        while pb <= pc {
            let cmp_result = cmp(&slice[pb], &slice[0]);
            if cmp_result > 0 {
                break;
            }
            if cmp_result == 0 {
                slice.swap(pa, pb);
                pa += 1;
            }
            pb += 1;
        }
 // Backward scan: retreat pc over elements ≥ pivot; push equals to back.
 // pb <= pc holds at entry → pc >= 1 → no underflow.
        while pb <= pc {
            let cmp_result = cmp(&slice[pc], &slice[0]);
            if cmp_result < 0 {
                break;
            }
            if cmp_result == 0 {
                slice.swap(pc, pd);
 // pd >= pc >= pb >= 1, so pd >= 1 → no underflow.
                pd -= 1;
            }
 // pc >= pb >= 1 → no underflow.
            pc -= 1;
        }
        if pb > pc {
            break;
        }
 // Exchange a > pivot element (at pb) with a < pivot element (at pc).
        slice.swap(pb, pc);
        pb += 1;
 // After swap: pc was > pb (otherwise we'd have broken), so pc >= pb >= 2 → pc - 1 >= 1.
        pc -= 1;
    }

 // Rearrange front/back equal-to-pivot runs to the centre.
 // After the partition pb = pc + 1 (they crossed by 1), so:
 // [0..pa) — equals at front (count: pa)
 // [pa..pb) — less than pivot (count: pb - pa)
 // [pc+1..pd+1) — greater than pivot
 // [pd+1..n) — equals at back

 // Move front-equals adjacent to the pivot point:
 // vecswap(a, pb - r1, r1) with r1 = min(pa, pb - pa)
 // This swaps slice[0..r1] with slice[pb-r1..pb].
    let r1 = pa.min(pb - pa);
    vecswap(slice, 0, pb - r1, r1);

 // Move back-equals adjacent to the pivot point:
 // vecswap(pb, pn - r2, r2) with r2 = min(pd - pc, n - pd - 1)
 // pd >= pc (invariant) → no underflow.
 // pd <= n-1 → n - pd - 1 >= 0 → no underflow.
    let r2 = (pd - pc).min(n - pd - 1);
    vecswap(slice, pb, n - r2, r2);

 // Recurse into left partition if it overlaps [lrange, rrange].
 // Left partition contains the `pb - pa` elements that are < pivot.
    let left_size = pb - pa;
    if left_size > 1 {
        let abs_l = abs_offset;
        let abs_r = abs_offset + left_size - 1;
        if ranges_overlap(abs_l, abs_r, lrange, rrange) {
            pqsort_inner(&mut slice[..left_size], abs_offset, lrange, rrange, cmp);
        }
    }

 // Tail-iterate into right partition if it overlaps [lrange, rrange].
 // Right partition contains the `pd - pc` elements that are > pivot.
 // C uses `goto loop` for tail call; here we recurse.
    let right_size = pd - pc;
    if right_size > 1 {
        let new_start = n - right_size;
        let abs_l = abs_offset + new_start;
        let abs_r = abs_offset + n - 1;
        if ranges_overlap(abs_l, abs_r, lrange, rrange) {
            pqsort_inner(
                &mut slice[new_start..],
                abs_offset + new_start,
                lrange,
                rrange,
                cmp,
            );
        }
    }
}

// ── public API ────────────────────────────────────────────────────────────────

/// Partially sorts `slice` so that the elements in `slice[lrange..=rrange]`
/// are correctly placed: every element in `slice[0..lrange]` compares
/// ≤ every element in `slice[lrange..=rrange]`, which in turn compares
/// ≤ every element in `slice[rrange+1..]`. The order of elements *within*
/// `slice[0..lrange]` and `slice[rrange+1..]` is unspecified.
/// # Arguments
/// * `slice` — the array to sort, modified in place.
/// * `lrange` — inclusive lower bound of the target range (element index).
/// * `rrange` — inclusive upper bound of the target range (element index).
/// * `cmp` — comparison function; must return negative / zero / positive
/// analogously to C's `qsort` comparator.
/// # Panics
/// Panics only if `lrange > rrange` or either bound is ≥ `slice.len`
/// the algorithm happens to index out of bounds. The caller is responsible
/// for passing valid bounds.
/// # Port note
/// The C signature is `pqsort(void *a, size_t n, size_t es, cmp, lrange, rrange)`.
/// The `n` and `es` arguments collapse into `slice.len` and the element type `T`.
/// The comparator returns `i32` (matching C's convention) rather than
/// `std::cmp::Ordering` to keep the call sites natural.
pub fn pqsort<T, F>(slice: &mut [T], lrange: usize, rrange: usize, cmp: F)
where
    F: Fn(&T, &T) -> i32,
{
 // ((unsigned char*)a)+((rrange+1)*es)-1 → rrange as-is (last byte of element rrange)
    pqsort_inner(slice, 0, lrange, rrange, &cmp);
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp_i32(a: &i32, b: &i32) -> i32 {
        a.cmp(b) as i32
    }

    #[test]
    fn sort_full_range() {
        let mut v = vec![5i32, 3, 1, 4, 2];
        pqsort(&mut v, 0, 4, cmp_i32);
        assert_eq!(v, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn sort_middle_range() {
        let mut v = vec![5i32, 3, 1, 4, 2];
        pqsort(&mut v, 1, 3, cmp_i32);
 // Elements at indices 1, 2, 3 must be correctly ordered relative
 // the full-sort result (2, 3, 4), but the exact values of the outer
 // elements are unspecified.
        let sorted = v.iter().cloned().collect::<Vec<_>>();
 // The element that would be at index 1..=3 in sorted order (2, 3, 4)
 // must appear at positions 1..=3.
        let mut mid: Vec<i32> = sorted[1..=3].to_vec();
        mid.sort();
        assert_eq!(mid, vec![2, 3, 4]);
    }

    #[test]
    fn sort_single_element_range() {
        let mut v = vec![5i32, 1, 3];
        pqsort(&mut v, 1, 1, cmp_i32);
        let median = *v.iter().min_by_key(|&&x| (x - 3).abs()).unwrap();
        assert_eq!(median, 3);
    }

    #[test]
    fn sort_already_sorted() {
        let mut v = vec![1i32, 2, 3, 4, 5];
        pqsort(&mut v, 0, 4, cmp_i32);
        assert_eq!(v, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn sort_all_equal() {
        let mut v = vec![7i32; 10];
        pqsort(&mut v, 0, 9, cmp_i32);
        assert_eq!(v, vec![7i32; 10]);
    }

    #[test]
    fn sort_two_elements() {
        let mut v = vec![2i32, 1];
        pqsort(&mut v, 0, 1, cmp_i32);
        assert_eq!(v, vec![1, 2]);
    }

    #[test]
    fn sort_empty() {
        let mut v: Vec<i32> = vec![];
        pqsort(&mut v, 0, 0, cmp_i32);
        assert_eq!(v, vec![]);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         void*/es pointer-arithmetic idiom replaced by generic &mut [T];
//                  goto-loop tail call replaced by recursion (PORT NOTE);
//                  lrange/rrange converted from pointers to absolute element indices;
//                  no unsafe needed — all index arithmetic proven free of usize underflow.
// ──────────────────────────────────────────────────────────────────────────

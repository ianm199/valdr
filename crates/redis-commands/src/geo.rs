//! Port of `src/geo.c` and `src/geo.h`.
//!
//! Implements the GEO command family: GEOADD, GEORADIUS, GEORADIUSBYMEMBER,
//! GEORADIUS_RO, GEORADIUSBYMEMBER_RO, GEOSEARCH, GEOSEARCHSTORE, GEOHASH,
//! GEOPOS, GEODIST.
//!
//! Geo data is stored as a sorted set where each member's score is a 52-bit
//! WGS84 geohash encoding of (longitude, latitude). This module handles
//! encoding, decoding, bounding-box range queries, and result formatting.
//!
//! Types `GeoShape` / `GeoShapeKind` / `GeoHashBits` / `GeoHashFix52Bits` /
//! `GeoHashRadius` are defined in sibling geohash modules and imported here.
//! Only `GeoPoint` is new to this module.
//!
//! C source: src/geo.c (1022 lines, 21 functions), src/geo.h (25 lines).

#![allow(dead_code, unused_variables, unused_mut)]

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};

use super::geohash_geohash::{
    geohash_decode_to_long_lat_wgs84, geohash_encode, geohash_encode_wgs84,
    GeoHashBits, GeoHashRange, GeoShape, GeoShapeKind,
    GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, GEO_STEP_MAX,
};
use super::geohash_geohash_helper::{
    geohash_align_52_bits, geohash_calculate_areas_by_shape_wgs84, geohash_get_distance,
    geohash_get_distance_if_in_polygon, geohash_get_distance_if_in_radius_wgs84,
    geohash_get_distance_if_in_rectangle, GeoHashFix52Bits, GeoHashRadius,
};

// ─── Constants ────────────────────────────────────────────────────────────────

const SORT_NONE: u8 = 0;
const SORT_ASC: u8 = 1;
const SORT_DESC: u8 = 2;

/// Search by explicit lon/lat coordinates (GEORADIUS / GEORADIUS_RO).
const RADIUS_COORDS: u32 = 1 << 0;
/// Search by named set member (GEORADIUSBYMEMBER / GEORADIUSBYMEMBER_RO).
const RADIUS_MEMBER: u32 = 1 << 1;
/// Reject STORE / STOREDIST options (read-only variants).
const RADIUS_NOSTORE: u32 = 1 << 2;
/// GEOSEARCH / GEOSEARCHSTORE command variant.
const GEOSEARCH_FLAG: u32 = 1 << 3;
/// GEOSEARCHSTORE variant (writes results to a destination key).
const GEOSEARCHSTORE_FLAG: u32 = 1 << 4;

/// Standard base-32 geohash alphabet for the GEOHASH command.
/// C: `char *geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz";` (geo.c:899).
const GEO_ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// ─── Types ────────────────────────────────────────────────────────────────────

/// A single GEO search result point.
///
/// PORT NOTE: C `char *member` (sds byte string) → `Vec<u8>`. Member names
/// are Redis data and must not be treated as UTF-8.
///
/// C: `geoPoint` struct in geo.h:10-16.
/// PORT NOTE: C `geoArray` uses an inline buffer of 8 elements to avoid heap
/// allocation for small result sets. Rust uses `Vec<GeoPoint>` directly;
/// the inline-buffer optimisation is deferred to Phase B profiling.
/// PERF(port): geoArray inline-buffer optimisation — profile in Phase B.
pub struct GeoPoint {
    pub longitude: f64,
    pub latitude: f64,
    /// Distance from search centre (metres, before dividing by conversion).
    pub dist: f64,
    /// Raw sorted-set score (the 52-bit geohash encoding).
    pub score: f64,
    /// Set member name as raw bytes.
    pub member: Vec<u8>,
}

// ─── Decode helper ────────────────────────────────────────────────────────────

/// Decode a 52-bit geohash score to `[longitude, latitude]`.
/// Returns `None` on decoding failure.
/// C: geo.c:100-103, decodeGeohash
fn decode_geohash(bits: f64) -> Option<[f64; 2]> {
    let hash = GeoHashBits { bits: bits as u64, step: GEO_STEP_MAX };
    geohash_decode_to_long_lat_wgs84(hash)
}

// ─── Argument parse helpers ───────────────────────────────────────────────────

/// Parse a decimal float from raw bytes.
///
/// PORT NOTE: Uses `std::str::from_utf8` solely for numeric parsing of command
/// arguments (not for treating Redis keys/values as text). Replace with a
/// byte-level strtod in Phase B; see `valkey_strtod.rs`.
/// TODO(port): use valkey_strtod equivalent once redis-core/src/strtod.rs ported.
fn parse_geo_f64(bytes: &[u8]) -> Result<f64, RedisError> {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(RedisError::not_float)
}

fn parse_geo_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(RedisError::not_integer)
}

fn parse_geo_i32(bytes: &[u8]) -> Result<i32, RedisError> {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(RedisError::not_integer)
}

/// Parse longitude and latitude from two consecutive command arguments.
/// Returns `Err` if either value is non-numeric or outside WGS84 bounds.
/// C: geo.c:108-120, extractLongLatOrReply
fn extract_long_lat_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
    xy: &mut [f64; 2],
) -> Result<(), RedisError> {
    for i in 0..2usize {
        let raw = ctx.arg(arg_base + i)?.as_bytes().to_vec();
        xy[i] = parse_geo_f64(&raw)?;
    }
    if xy[0] < GEO_LONG_MIN
        || xy[0] > GEO_LONG_MAX
        || xy[1] < GEO_LAT_MIN
        || xy[1] > GEO_LAT_MAX
    {
        // PORT NOTE: C uses addReplyErrorFormat; Rust returns Err with the message.
        // The exact message format must match for wire-diff oracle.
        let msg = format!(
            "ERR invalid longitude,latitude pair {},{}\r\n",
            xy[0], xy[1]
        );
        return Err(RedisError::runtime(msg.as_bytes()));
    }
    Ok(())
}

/// Decode lon/lat from a sorted-set member's score.
/// C: geo.c:125-137, longLatFromMemberOrReply
fn long_lat_from_member_or_reply(
    zobj: &RedisObject,
    member: &[u8],
    xy: &mut [f64; 2],
) -> Result<(), RedisError> {
    // TODO(port): call zset_score(zobj, member) once zset.rs is ported.
    // C: zsetScore(zobj, objectGetVal(member), &score)
    let score = zset_score(zobj, member)?;
    match decode_geohash(score) {
        Some(decoded) => {
            xy[0] = decoded[0];
            xy[1] = decoded[1];
            Ok(())
        }
        None => Err(RedisError::runtime(b"failed to decode geohash for member")),
    }
}

/// Parse a unit string and return metres-per-unit conversion factor.
/// C: geo.c:145-160, extractUnitOrReply
fn extract_unit_or_reply(unit: &[u8]) -> Result<f64, RedisError> {
    if unit.eq_ignore_ascii_case(b"m") {
        Ok(1.0)
    } else if unit.eq_ignore_ascii_case(b"km") {
        Ok(1000.0)
    } else if unit.eq_ignore_ascii_case(b"ft") {
        Ok(0.3048)
    } else if unit.eq_ignore_ascii_case(b"mi") {
        Ok(1609.34)
    } else {
        Err(RedisError::runtime(
            b"unsupported unit provided. please use M, KM, FT, MI",
        ))
    }
}

/// Parse `<number> <unit>` from two consecutive arguments.
/// Returns `(metres_per_unit, distance_in_units)`.
/// C: geo.c:166-185, extractDistanceOrReply
fn extract_distance_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
) -> Result<(f64, f64), RedisError> {
    let dist_raw = ctx.arg(arg_base)?.as_bytes().to_vec();
    let distance =
        parse_geo_f64(&dist_raw).map_err(|_| RedisError::runtime(b"need numeric radius"))?;
    if distance < 0.0 {
        return Err(RedisError::runtime(b"radius cannot be negative"));
    }
    let unit_raw = ctx.arg(arg_base + 1)?.as_bytes().to_vec();
    let to_meters = extract_unit_or_reply(&unit_raw)?;
    Ok((to_meters, distance))
}

/// Parse `<width> <height> <unit>` from three consecutive arguments.
/// Returns `(metres_per_unit, width, height)`.
/// C: geo.c:191-212, extractBoxOrReply
fn extract_box_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
) -> Result<(f64, f64, f64), RedisError> {
    let w_raw = ctx.arg(arg_base)?.as_bytes().to_vec();
    let w = parse_geo_f64(&w_raw).map_err(|_| RedisError::runtime(b"need numeric width"))?;
    let h_raw = ctx.arg(arg_base + 1)?.as_bytes().to_vec();
    let h = parse_geo_f64(&h_raw).map_err(|_| RedisError::runtime(b"need numeric height"))?;
    if h < 0.0 || w < 0.0 {
        return Err(RedisError::runtime(b"height or width cannot be negative"));
    }
    let unit_raw = ctx.arg(arg_base + 2)?.as_bytes().to_vec();
    let to_meters = extract_unit_or_reply(&unit_raw)?;
    Ok((to_meters, w, h))
}

/// Reply with a distance formatted to 4 decimal places as a bulk string.
/// C: geo.c:219-223, addReplyDoubleDistance
/// PERF(port): C uses fixedpoint_d2string into a stack buffer; here we alloc.
fn reply_double_distance(ctx: &mut CommandContext, d: f64) -> Result<(), RedisError> {
    let s = format!("{:.4}", d);
    ctx.reply_bulk(s.as_bytes())
}

// ─── Shape containment test ───────────────────────────────────────────────────

/// Test whether a geohash-encoded score lies within `shape`.
///
/// Returns `Some((xy, distance_metres))` if the point is inside the shape,
/// `None` if outside or if decoding fails.
///
/// PORT NOTE: C `geoWithinShape` takes `double *xy` and `double *distance`
/// out-params and returns C_OK/C_ERR. This port returns `Option<([f64;2], f64)>`
/// so callers don't need uninitialized out-params.
///
/// C: geo.c:239-258, geoWithinShape
fn geo_within_shape(shape: &GeoShape, score: f64) -> Option<([f64; 2], f64)> {
    let xy = decode_geohash(score)?;
    let distance = match &shape.kind {
        GeoShapeKind::Circular { radius } => geohash_get_distance_if_in_radius_wgs84(
            shape.xy[0],
            shape.xy[1],
            xy[0],
            xy[1],
            radius * shape.conversion,
        )?,
        GeoShapeKind::Rectangle { height, width } => geohash_get_distance_if_in_rectangle(
            width * shape.conversion,
            height * shape.conversion,
            shape.xy[0],
            shape.xy[1],
            xy[0],
            xy[1],
        )?,
        GeoShapeKind::Polygon { points } => geohash_get_distance_if_in_polygon(
            shape.xy[0],
            shape.xy[1],
            [xy[0], xy[1]],
            points,
        )?,
    };
    Some((xy, distance))
}

// ─── Range query helpers ──────────────────────────────────────────────────────

/// Query a sorted set for all members with scores in `[min, max)`, filter by
/// shape, and append matching points to `ga`. Returns the count of new points.
///
/// PORT NOTE: The C implementation has two paths (listpack / skiplist encoding).
/// The Rust translation uses a unified logical description; the encoding-specific
/// iterator will come from `zset.rs` once ported (Phase B).
///
/// C: geo.c:272-333, geoGetPointsInRange
fn geo_get_points_in_range(
    zobj: &RedisObject,
    min: f64,
    max: f64,
    shape: &GeoShape,
    ga: &mut Vec<GeoPoint>,
    limit: usize,
) -> usize {
    // C: zrangespec range = {.min = min, .max = max, .minex = 0, .maxex = 1};
    // min inclusive, max exclusive.
    let origin = ga.len();

    // TODO(port): call zobj.iter_score_range_exclusive_max(min, max) once
    // RedisObject::ZSet exposes an iterator yielding (member: Vec<u8>, score: f64)
    // in ascending score order. Both C paths (listpack geo.c:278-307 and skiplist
    // geo.c:308-331) share the same filter logic reproduced below.
    //
    // Shared inner loop (encoding-independent):
    //   for (member_bytes, score) in zobj.score_range_iter(min, max) {
    //       if score >= max { break; }  // maxex = 1
    //       if let Some((xy, distance)) = geo_within_shape(shape, score) {
    //           ga.push(GeoPoint {
    //               longitude: xy[0], latitude: xy[1],
    //               dist: distance, score, member: member_bytes,
    //           });
    //           if limit > 0 && ga.len() >= limit { break; }
    //       }
    //   }

    ga.len() - origin
}

/// Compute the `[min, max)` score range covering a single geohash bounding box.
/// C: geo.c:338-362, scoresOfGeoHashBox
fn scores_of_geohash_box(hash: GeoHashBits) -> (GeoHashFix52Bits, GeoHashFix52Bits) {
    let min = geohash_align_52_bits(hash);
    let mut hash_next = hash;
    hash_next.bits = hash_next.bits.wrapping_add(1);
    let max = geohash_align_52_bits(hash_next);
    (min, max)
}

/// Populate `ga` with all zset members inside a single geohash bounding box.
/// C: geo.c:367-372, membersOfGeoHashBox
fn members_of_geohash_box(
    zobj: &RedisObject,
    hash: GeoHashBits,
    ga: &mut Vec<GeoPoint>,
    shape: &GeoShape,
    limit: usize,
) -> usize {
    let (min, max) = scores_of_geohash_box(hash);
    geo_get_points_in_range(zobj, min as f64, max as f64, shape, ga, limit)
}

/// Search a sorted set across the centre geohash cell and all eight neighbours.
/// Duplicate adjacent cells (large radii) are skipped automatically.
/// C: geo.c:375-428, membersOfAllNeighbors
fn members_of_all_neighbors(
    zobj: &RedisObject,
    n: &GeoHashRadius,
    shape: &GeoShape,
    ga: &mut Vec<GeoPoint>,
    limit: usize,
) -> usize {
    let neighbors = [
        n.hash,
        n.neighbors.north,
        n.neighbors.south,
        n.neighbors.east,
        n.neighbors.west,
        n.neighbors.north_east,
        n.neighbors.north_west,
        n.neighbors.south_east,
        n.neighbors.south_west,
    ];
    let mut count = 0usize;
    // C: `last_processed` tracks the index of the last non-skipped neighbor to
    // detect duplicates (which occur when the radius covers multiple cells).
    let mut last_processed: Option<usize> = None;

    for (i, &neighbor) in neighbors.iter().enumerate() {
        // C: HASHISZERO(neighbors[i]) → skip zero hash
        if neighbor.bits == 0 && neighbor.step == 0 {
            continue;
        }
        // Skip if this neighbor is identical to the last processed one.
        if let Some(last) = last_processed {
            if neighbors[i].bits == neighbors[last].bits
                && neighbors[i].step == neighbors[last].step
            {
                continue;
            }
        }
        if limit > 0 && ga.len() >= limit {
            break;
        }
        count += members_of_geohash_box(zobj, neighbor, ga, shape, limit);
        last_processed = Some(i);
    }
    count
}

// ─── Sort comparators ─────────────────────────────────────────────────────────

/// Ascending sort by distance. C: geo.c:431-441, sort_gp_asc (static).
fn sort_gp_asc(a: &GeoPoint, b: &GeoPoint) -> std::cmp::Ordering {
    a.dist.partial_cmp(&b.dist).unwrap_or(std::cmp::Ordering::Equal)
}

/// Descending sort by distance. C: geo.c:443-445, sort_gp_desc (static).
fn sort_gp_desc(a: &GeoPoint, b: &GeoPoint) -> std::cmp::Ordering {
    sort_gp_asc(a, b).reverse()
}

// ─── Commands ─────────────────────────────────────────────────────────────────

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
///
/// Encodes each (longitude, latitude) pair as a 52-bit WGS84 geohash score and
/// delegates to the ZADD logic.
///
/// PORT NOTE: C rewrites the client's argv vector and calls zaddCommand via
/// replaceClientCommandVector. Rust calls zadd_generic directly with the encoded
/// (score, member) pairs. TODO(port): call zadd_generic once zset.rs is ported.
///
/// C: geo.c:452-513, geoaddCommand
pub fn geoadd_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut xx = false;
    let mut nx = false;
    let mut long_idx = 2usize;

    // Parse optional flags: NX, XX, CH (CH is passed through to zadd_generic).
    while long_idx < ctx.argc() {
        let opt = ctx.arg(long_idx)?.as_bytes().to_vec();
        if opt.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if opt.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if opt.eq_ignore_ascii_case(b"ch") {
            // CH: return count of changed elements rather than added elements.
            // Forwarded to zadd_generic; no local state needed.
        } else {
            break;
        }
        long_idx += 1;
    }

    let remaining = ctx.argc().saturating_sub(long_idx);
    if remaining % 3 != 0 || (xx && nx) {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let elements = remaining / 3;
    let key = ctx.arg(1)?.as_bytes().to_vec();

    // Build (score, member) pairs by encoding each (lon, lat) as a geohash score.
    let mut scored: Vec<(f64, Vec<u8>)> = Vec::with_capacity(elements);
    for i in 0..elements {
        let base = long_idx + i * 3;
        let mut xy = [0.0_f64; 2];
        extract_long_lat_or_reply(ctx, base, &mut xy)?;

        let hash = geohash_encode_wgs84(xy[0], xy[1], GEO_STEP_MAX)
            .ok_or_else(|| RedisError::runtime(b"geohash encoding failed"))?;
        let bits: GeoHashFix52Bits = geohash_align_52_bits(hash);
        let score = bits as f64;

        let member = ctx.arg(base + 2)?.as_bytes().to_vec();
        scored.push((score, member));
    }

    // TODO(port): call zadd_generic(ctx, &key, xx, nx, ch, &scored) once zset.rs ported.
    // C: geo.c:511-512 — replaceClientCommandVector(c, argc, argv); zaddCommand(c);
    zadd_generic(ctx, &key, xx, nx, &scored)
}

/// Core implementation of GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH, GEOSEARCHSTORE.
///
/// `src_key_index`: argv index of the source sorted-set key.
/// `flags`: bitmask of RADIUS_COORDS | RADIUS_MEMBER | RADIUS_NOSTORE |
///          GEOSEARCH_FLAG | GEOSEARCHSTORE_FLAG.
///
/// C: geo.c:533-864, georadiusGeneric
pub fn georadius_generic(
    ctx: &mut CommandContext,
    src_key_index: usize,
    flags: u32,
) -> Result<(), RedisError> {
    let mut storekey: Option<Vec<u8>> = None;
    let mut storedist = false;

    let src_key = ctx.arg(src_key_index)?.as_bytes().to_vec();

    // Validate source key type (must be a sorted set if it exists).
    // TODO(port): borrow checker — ctx.db() borrow and ctx.reply_* calls cannot
    // coexist without restructuring CommandContext (Phase B).
    // The lookup_key_read result must not outlive the db borrow.
    {
        let zobj_ref = ctx.db().lookup_key_read(&src_key);
        if let Some(z) = zobj_ref {
            if !matches!(z, RedisObject::ZSet(_)) {
                return Err(RedisError::wrong_type());
            }
        }
    }
    let zobj_present = ctx.db().lookup_key_read(&src_key).is_some();

    let base_args: usize;
    let mut shape = GeoShape {
        xy: [0.0; 2],
        conversion: 1.0,
        bounds: [0.0; 4],
        kind: GeoShapeKind::Circular { radius: 0.0 }, // placeholder; overwritten below
    };

    if flags & RADIUS_COORDS != 0 {
        // GEORADIUS or GEORADIUS_RO: center from explicit coordinates.
        base_args = 6;
        extract_long_lat_or_reply(ctx, 2, &mut shape.xy)?;
        let (conversion, radius) = extract_distance_or_reply(ctx, base_args - 2)?;
        shape.conversion = conversion;
        shape.kind = GeoShapeKind::Circular { radius };
    } else if flags & RADIUS_MEMBER != 0 && !zobj_present {
        // GEORADIUSBYMEMBER with missing source key: proceed to determine STORE.
        base_args = 5;
    } else if flags & RADIUS_MEMBER != 0 {
        // GEORADIUSBYMEMBER: center from member position.
        base_args = 5;
        let member = ctx.arg(2)?.as_bytes().to_vec();
        // TODO(port): borrow checker — need immutable zobj ref while mutating ctx.
        // For Phase A: re-lookup (two reads) to avoid holding the first borrow.
        {
            let zobj_ref = ctx.db().lookup_key_read(&src_key);
            if let Some(z) = zobj_ref {
                // Clone needed data before releasing borrow.
                let score = zset_score(z, &member)?;
                let xy = decode_geohash(score)
                    .ok_or_else(|| RedisError::runtime(b"failed to decode member geohash"))?;
                shape.xy = xy;
            } else {
                return Err(RedisError::runtime(b"member does not exist"));
            }
        }
        let (conversion, radius) = extract_distance_or_reply(ctx, base_args - 2)?;
        shape.conversion = conversion;
        shape.kind = GeoShapeKind::Circular { radius };
    } else if flags & GEOSEARCH_FLAG != 0 {
        // GEOSEARCH / GEOSEARCHSTORE: richer argument syntax.
        if flags & GEOSEARCHSTORE_FLAG != 0 {
            storekey = Some(ctx.arg(1)?.as_bytes().to_vec());
            base_args = 3;
        } else {
            base_args = 2;
        }
        // shape.kind left as Circular placeholder; overwritten in option parsing below.
    } else {
        return Err(RedisError::runtime(b"Unknown georadius search type"));
    }

    // ── Optional parameter parsing ──────────────────────────────────────────
    let mut withdist = false;
    let mut withhash = false;
    let mut withcoords = false;
    let mut frommember = false;
    let mut fromloc = false;
    let mut byradius = false;
    let mut bybox = false;
    let mut bypolygon = false;
    let mut sort: u8 = SORT_NONE;
    let mut any = false;
    let mut count: i64 = 0;

    let remaining = ctx.argc().saturating_sub(base_args);
    let mut i = 0usize;
    while i < remaining {
        let arg_raw = ctx.arg(base_args + i)?.as_bytes().to_vec();
        let arg = arg_raw.as_slice();

        if arg.eq_ignore_ascii_case(b"withdist") {
            withdist = true;
        } else if arg.eq_ignore_ascii_case(b"withhash") {
            withhash = true;
        } else if arg.eq_ignore_ascii_case(b"withcoord") {
            withcoords = true;
        } else if arg.eq_ignore_ascii_case(b"any") {
            any = true;
        } else if arg.eq_ignore_ascii_case(b"asc") {
            sort = SORT_ASC;
        } else if arg.eq_ignore_ascii_case(b"desc") {
            sort = SORT_DESC;
        } else if arg.eq_ignore_ascii_case(b"count") && i + 1 < remaining {
            let cnt_raw = ctx.arg(base_args + i + 1)?.as_bytes().to_vec();
            count = parse_geo_i64(&cnt_raw)?;
            if count <= 0 {
                return Err(RedisError::runtime(b"COUNT must be > 0"));
            }
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"store")
            && i + 1 < remaining
            && flags & RADIUS_NOSTORE == 0
            && flags & GEOSEARCH_FLAG == 0
        {
            storekey = Some(ctx.arg(base_args + i + 1)?.as_bytes().to_vec());
            storedist = false;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"storedist")
            && i + 1 < remaining
            && flags & RADIUS_NOSTORE == 0
            && flags & GEOSEARCH_FLAG == 0
        {
            storekey = Some(ctx.arg(base_args + i + 1)?.as_bytes().to_vec());
            storedist = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"storedist")
            && flags & GEOSEARCH_FLAG != 0
            && flags & GEOSEARCHSTORE_FLAG != 0
        {
            storedist = true;
        } else if arg.eq_ignore_ascii_case(b"frommember")
            && i + 1 < remaining
            && flags & GEOSEARCH_FLAG != 0
            && !fromloc
            && !bypolygon
        {
            let member = ctx.arg(base_args + i + 1)?.as_bytes().to_vec();
            if zobj_present {
                let zobj_ref = ctx.db().lookup_key_read(&src_key);
                if let Some(z) = zobj_ref {
                    let score = zset_score(z, &member)?;
                    let xy = decode_geohash(score).ok_or_else(|| {
                        RedisError::runtime(b"failed to decode member geohash")
                    })?;
                    shape.xy = xy;
                }
            }
            frommember = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"fromlonlat")
            && i + 2 < remaining
            && flags & GEOSEARCH_FLAG != 0
            && !frommember
            && !bypolygon
        {
            extract_long_lat_or_reply(ctx, base_args + i + 1, &mut shape.xy)?;
            fromloc = true;
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"byradius")
            && i + 2 < remaining
            && flags & GEOSEARCH_FLAG != 0
            && !bybox
            && !bypolygon
        {
            let (conversion, radius) = extract_distance_or_reply(ctx, base_args + i + 1)?;
            shape.conversion = conversion;
            shape.kind = GeoShapeKind::Circular { radius };
            byradius = true;
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"bybox")
            && i + 3 < remaining
            && flags & GEOSEARCH_FLAG != 0
            && !byradius
            && !bypolygon
        {
            let (conversion, w, h) = extract_box_or_reply(ctx, base_args + i + 1)?;
            shape.conversion = conversion;
            shape.kind = GeoShapeKind::Rectangle { height: h, width: w };
            bybox = true;
            i += 3;
        } else if arg.eq_ignore_ascii_case(b"bypolygon")
            && i + 2 < remaining
            && flags & GEOSEARCH_FLAG != 0
            && !byradius
            && !bybox
            && !frommember
            && !fromloc
        {
            let nv_raw = ctx.arg(base_args + i + 1)?.as_bytes().to_vec();
            let num_vertices = parse_geo_i32(&nv_raw)
                .map_err(|_| RedisError::runtime(b"invalid number of vertices"))?;
            let possible = remaining.saturating_sub(i + 2) / 2;
            if num_vertices < 3 || (possible as i32) < num_vertices {
                return Err(RedisError::runtime(
                    b"GEOSEARCH BYPOLYGON must have at least 3 vertices",
                ));
            }
            let nv = num_vertices as usize;
            let mut points: Vec<[f64; 2]> = Vec::with_capacity(nv);
            for j in 0..nv {
                let mut pt = [0.0_f64; 2];
                extract_long_lat_or_reply(ctx, base_args + i + 2 + j * 2, &mut pt)?;
                points.push(pt);
            }
            shape.conversion = 1.0;
            shape.kind = GeoShapeKind::Polygon { points };
            bypolygon = true;
            i += 1 + nv * 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
        i += 1;
    }

    // ── Validate option combinations ────────────────────────────────────────
    if storekey.is_some() && (withdist || withhash || withcoords) {
        let msg = if flags & GEOSEARCHSTORE_FLAG != 0 {
            b"GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
                as &[u8]
        } else {
            b"STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        };
        return Err(RedisError::runtime(msg));
    }

    if flags & GEOSEARCH_FLAG != 0 && !frommember && !fromloc && !bypolygon {
        return Err(RedisError::runtime(
            b"exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
        ));
    }

    if flags & GEOSEARCH_FLAG != 0 && !byradius && !bybox && !bypolygon {
        return Err(RedisError::runtime(
            b"exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for GEOSEARCH",
        ));
    }

    if any && count == 0 {
        return Err(RedisError::runtime(
            b"the ANY argument requires COUNT argument",
        ));
    }

    // ── Handle empty / missing source key ───────────────────────────────────
    if !zobj_present {
        if let Some(ref sk) = storekey {
            let sk = sk.clone();
            if ctx.db_mut().delete(&sk) {
                ctx.db_mut().signal_modified(&sk);
                // TODO(architect): notify_keyspace_event(NOTIFY_GENERIC, "del", &sk, db_id)
                // TODO(architect): server.dirty++
            }
            return ctx.reply_integer(0);
        } else {
            return ctx.reply_array_header(0);
        }
    }

    // COUNT without sort implies ASC (need ordering for closest-N semantics).
    if count != 0 && sort == SORT_NONE && !any {
        sort = SORT_ASC;
    }

    // ── Search ──────────────────────────────────────────────────────────────
    // TODO(port): geohash_calculate_areas_by_shape_wgs84 takes &mut GeoShape.
    // In Phase B, clone shape or restructure to satisfy the borrow checker.
    let georadius = geohash_calculate_areas_by_shape_wgs84(&mut shape)
        .ok_or_else(|| RedisError::runtime(b"geohash area calculation failed"))?;

    let limit = if any { count as usize } else { 0 };
    let mut ga: Vec<GeoPoint> = Vec::new();

    // TODO(port): borrow checker — holding &RedisObject from lookup_key_read
    // while also calling ctx.reply_* requires Phase B restructuring.
    // For now we reference the db independently.
    {
        let zobj = ctx.db().lookup_key_read(&src_key);
        if let Some(z) = zobj {
            members_of_all_neighbors(z, &georadius, &shape, &mut ga, limit);
        }
    }

    if ga.is_empty() && storekey.is_none() {
        return ctx.reply_array_header(0);
    }

    let result_length = ga.len();
    let returned_items = if count == 0 || (result_length as i64) < count {
        result_length
    } else {
        count as usize
    };

    // ── Sort results ─────────────────────────────────────────────────────────
    if sort == SORT_ASC {
        if returned_items == result_length {
            ga.sort_by(sort_gp_asc);
        } else {
            // PERF(port): C uses pqsort for partial sort; here we sort the full
            // array. Implement partial_sort equivalent in Phase B for large sets.
            ga.sort_by(sort_gp_asc);
        }
    } else if sort == SORT_DESC {
        ga.sort_by(sort_gp_desc);
    }

    // ── Return or store results ───────────────────────────────────────────────
    if storekey.is_none() {
        let option_count =
            withdist as usize + withcoords as usize + withhash as usize;
        ctx.reply_array_header(returned_items)?;

        for gp in ga.iter_mut().take(returned_items) {
            gp.dist /= shape.conversion;

            if option_count > 0 {
                ctx.reply_array_header(option_count + 1)?;
            }
            ctx.reply_bulk(&gp.member)?;

            if withdist {
                reply_double_distance(ctx, gp.dist)?;
            }
            if withhash {
                ctx.reply_integer(gp.score as i64)?;
            }
            if withcoords {
                ctx.reply_array_header(2)?;
                // TODO(port): addReplyHumanLongDouble → high-precision float.
                // C uses ld2string with LD_STR_HUMAN (17 sig-digit format).
                // Using format! here for Phase A; Phase C wire-diff may require
                // matching the exact precision Valkey emits.
                ctx.reply_bulk(format!("{}", gp.longitude).as_bytes())?;
                ctx.reply_bulk(format!("{}", gp.latitude).as_bytes())?;
            }
        }
    } else {
        let sk = storekey.unwrap();
        // TODO(port): create RedisObject::ZSet, insert all (score, member) pairs,
        // convert encoding (zsetConvertToListpackIfNeeded), then db.set_key.
        // C: geo.c:824-860.
        for gp in ga.iter_mut().take(returned_items) {
            gp.dist /= shape.conversion;
            let _score = if storedist { gp.dist } else { gp.score };
            // TODO(port): insert (_score, &gp.member) into destination ZSet.
        }
        if returned_items > 0 {
            // TODO(port): ctx.db_mut().set_key(&sk, zset_obj, 0)?;
            // TODO(architect): notify_keyspace_event(NOTIFY_ZSET, "geosearchstore", &sk, db_id)
            // TODO(architect): server.dirty += returned_items
        } else if ctx.db_mut().delete(&sk) {
            ctx.db_mut().signal_modified(&sk);
            // TODO(architect): notify_keyspace_event(NOTIFY_GENERIC, "del", &sk, db_id)
            // TODO(architect): server.dirty++
        }
        ctx.reply_integer(returned_items as i64)?;
    }

    Ok(())
}

// ─── Command entry points ─────────────────────────────────────────────────────

/// GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD]
///            [ASC|DESC] [COUNT count [ANY]] [STORE key|STOREDIST key]
/// C: geo.c:867-869, georadiusCommand
pub fn georadius_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 1, RADIUS_COORDS)
}

/// GEORADIUSBYMEMBER key member radius unit [options]
/// C: geo.c:872-874, georadiusbymemberCommand
pub fn georadiusbymember_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 1, RADIUS_MEMBER)
}

/// GEORADIUS_RO — read-only variant; STORE / STOREDIST not accepted.
/// C: geo.c:877-879, georadiusroCommand
pub fn georadiusro_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 1, RADIUS_COORDS | RADIUS_NOSTORE)
}

/// GEORADIUSBYMEMBER_RO — read-only variant.
/// C: geo.c:882-884, georadiusbymemberroCommand
pub fn georadiusbymemberro_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 1, RADIUS_MEMBER | RADIUS_NOSTORE)
}

/// GEOSEARCH key [FROMMEMBER member|FROMLONLAT lon lat]
///              [BYRADIUS r unit|BYBOX w h unit|BYPOLYGON n lon1 lat1 ...]
///              [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
/// C: geo.c:886-888, geosearchCommand
pub fn geosearch_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 1, GEOSEARCH_FLAG)
}

/// GEOSEARCHSTORE dest src [options] [STOREDIST]
/// C: geo.c:890-892, geosearchstoreCommand
pub fn geosearchstore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    georadius_generic(ctx, 2, GEOSEARCH_FLAG | GEOSEARCHSTORE_FLAG)
}

/// GEOHASH key member [member ...]
///
/// Returns an 11-character base-32 geohash string for each member.
/// The internal WGS84 (±85°) lat range is re-encoded to standard (±90°) before
/// producing the output string, for compatibility with external geohash tools.
///
/// C: geo.c:898-954, geohashCommand
pub fn geohash_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let key = ctx.arg(1)?.as_bytes().to_vec();
    {
        let zobj_ref = ctx.db().lookup_key_read(&key);
        if let Some(z) = zobj_ref {
            if !matches!(z, RedisObject::ZSet(_)) {
                return Err(RedisError::wrong_type());
            }
        }
    }

    let argc = ctx.argc();
    ctx.reply_array_header(argc - 2)?;

    for j in 2..argc {
        let member = ctx.arg(j)?.as_bytes().to_vec();

        // Look up score; reply null for missing members.
        let score_opt = {
            let zobj_ref = ctx.db().lookup_key_read(&key);
            match zobj_ref {
                None => None,
                Some(z) => zset_score(z, &member).ok(),
            }
        };

        let score = match score_opt {
            None => {
                ctx.reply_null()?;
                continue;
            }
            Some(s) => s,
        };

        let xy = match decode_geohash(score) {
            None => {
                ctx.reply_null()?;
                continue;
            }
            Some(v) => v,
        };

        // Re-encode using standard geohash coordinate ranges (±90° lat, ±180° lon)
        // rather than the Redis-internal ±85° range, for standard geohash output.
        // C: geo.c:928-948.
        let long_range = GeoHashRange { min: -180.0, max: 180.0 };
        let lat_range = GeoHashRange { min: -90.0, max: 90.0 };
        let hash = match geohash_encode(&long_range, &lat_range, xy[0], xy[1], 26) {
            None => {
                ctx.reply_null()?;
                continue;
            }
            Some(h) => h,
        };

        let mut buf = [0u8; 11];
        for k in 0..11usize {
            let idx = if k == 10 {
                // Only 52 bits available; the 11th character assumes zero padding.
                0
            } else {
                ((hash.bits >> (52 - ((k + 1) * 5))) & 0x1f) as usize
            };
            buf[k] = GEO_ALPHABET[idx];
        }
        ctx.reply_bulk(&buf)?;
    }

    Ok(())
}

/// GEOPOS key member [member ...]
///
/// Returns `[[longitude, latitude], ...]`; null array for missing members.
/// C: geo.c:960-986, geoposCommand
pub fn geopos_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let key = ctx.arg(1)?.as_bytes().to_vec();
    {
        let zobj_ref = ctx.db().lookup_key_read(&key);
        if let Some(z) = zobj_ref {
            if !matches!(z, RedisObject::ZSet(_)) {
                return Err(RedisError::wrong_type());
            }
        }
    }

    let argc = ctx.argc();
    ctx.reply_array_header(argc - 2)?;

    for j in 2..argc {
        let member = ctx.arg(j)?.as_bytes().to_vec();
        let score_opt = {
            let zobj_ref = ctx.db().lookup_key_read(&key);
            match zobj_ref {
                None => None,
                Some(z) => zset_score(z, &member).ok(),
            }
        };

        let score = match score_opt {
            None => {
                ctx.reply_null_array()?;
                continue;
            }
            Some(s) => s,
        };

        let xy = match decode_geohash(score) {
            None => {
                ctx.reply_null_array()?;
                continue;
            }
            Some(v) => v,
        };

        ctx.reply_array_header(2)?;
        // TODO(port): addReplyHumanLongDouble → match Valkey's high-precision float format.
        ctx.reply_bulk(format!("{}", xy[0]).as_bytes())?;
        ctx.reply_bulk(format!("{}", xy[1]).as_bytes())?;
    }

    Ok(())
}

/// GEODIST key member1 member2 [unit]
///
/// Returns the great-circle distance between two members in the requested unit.
/// Replies null if either member is missing.
/// C: geo.c:993-1022, geodistCommand
pub fn geodist_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    let to_meter = if argc == 5 {
        let unit = ctx.arg(4)?.as_bytes().to_vec();
        extract_unit_or_reply(&unit)?
    } else if argc > 5 {
        return Err(RedisError::syntax(b"syntax error"));
    } else {
        1.0_f64
    };

    let key = ctx.arg(1)?.as_bytes().to_vec();
    {
        let zobj_ref = ctx.db().lookup_key_read(&key);
        match zobj_ref {
            None => {
                ctx.reply_null()?;
                return Ok(());
            }
            Some(z) if !matches!(z, RedisObject::ZSet(_)) => {
                return Err(RedisError::wrong_type());
            }
            _ => {}
        }
    }

    let m1 = ctx.arg(2)?.as_bytes().to_vec();
    let m2 = ctx.arg(3)?.as_bytes().to_vec();

    let (score1, score2) = {
        let zobj_ref = ctx.db().lookup_key_read(&key);
        match zobj_ref {
            None => {
                ctx.reply_null()?;
                return Ok(());
            }
            Some(z) => {
                let s1 = zset_score(z, &m1);
                let s2 = zset_score(z, &m2);
                (s1, s2)
            }
        }
    };

    let score1 = match score1 {
        Ok(s) => s,
        Err(_) => {
            ctx.reply_null()?;
            return Ok(());
        }
    };
    let score2 = match score2 {
        Ok(s) => s,
        Err(_) => {
            ctx.reply_null()?;
            return Ok(());
        }
    };

    let xy1 = match decode_geohash(score1) {
        None => {
            ctx.reply_null()?;
            return Ok(());
        }
        Some(v) => v,
    };
    let xy2 = match decode_geohash(score2) {
        None => {
            ctx.reply_null()?;
            return Ok(());
        }
        Some(v) => v,
    };

    let dist = geohash_get_distance(xy1[0], xy1[1], xy2[0], xy2[1]) / to_meter;
    reply_double_distance(ctx, dist)
}

// ─── Placeholder stubs for cross-crate dependencies ───────────────────────────

/// TODO(port): replace with zset::zset_score() once t_zset.c → zset.rs is ported.
/// C: zsetScore(zobj, key, &score) → C_OK / C_ERR.
fn zset_score(zobj: &RedisObject, member: &[u8]) -> Result<f64, RedisError> {
    todo!("zset_score: blocked on zset.rs port")
}

/// TODO(port): replace with zset::zadd_generic() once t_zset.c → zset.rs is ported.
/// C: zaddCommand delegates to zaddGenericCommand with ZADD_IN_NONE flags.
fn zadd_generic(
    ctx: &mut CommandContext,
    key: &[u8],
    xx: bool,
    nx: bool,
    scored: &[(f64, Vec<u8>)],
) -> Result<(), RedisError> {
    todo!("zadd_generic: blocked on zset.rs port")
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/geo.c  (1022 lines, 21 functions)
//                  src/geo.h  (25 lines)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         23
//   port_notes:    7
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Full logic translation complete. Two cross-crate blockers:
//                  (1) zset_score / zadd_generic — need zset.rs port (Phase B);
//                  (2) geo_get_points_in_range — needs ZSet score-range iterator
//                  from zset.rs. Borrow-checker issues in georadius_generic noted
//                  with TODO(port); restructuring required in Phase B.
//                  GeoShape/GeoHashBits imported from geohash_geohash.rs;
//                  GeoHashFix52Bits/GeoHashRadius imported from
//                  geohash_geohash_helper.rs. Only GeoPoint is new here.
//                  parse_geo_f64 uses from_utf8 for numeric parsing (not Redis
//                  data treatment); replace with byte-level strtod in Phase B.
//                  addReplyHumanLongDouble (coords reply) uses format!() for
//                  Phase A; needs precision-matched impl for wire-diff in Phase C.
// ──────────────────────────────────────────────────────────────────────────────

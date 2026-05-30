//! GEO command family: GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH,
//! GEOSEARCHSTORE, GEORADIUS, GEORADIUSBYMEMBER, GEORADIUS_RO,
//! GEORADIUSBYMEMBER_RO.
//! Geo data is stored as a sorted set whose scores are 52-bit WGS84 geohash
//! encodings of (longitude, latitude). GEOADD encodes coordinates
//! delegates to ZADD logic; all other commands decode scores, perform
//! geometric filtering, and format results.
//! Geohash math lives in the sibling modules `geohash_geohash`
//! `geohash_geohash_helper`; only `GeoPoint` is new to this module.

use redis_core::command_context::CommandContext;
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_ZSET};
use redis_core::object::{InlineZSet, RedisObject};
use redis_types::{RedisError, RedisResult, RedisString};

use crate::geohash_geohash::{
    geohash_decode_to_long_lat_wgs84, geohash_encode, geohash_encode_wgs84, GeoHashBits,
    GeoHashRange, GeoShape, GeoShapeKind, GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN,
    GEO_STEP_MAX,
};
use crate::geohash_geohash_helper::{
    geohash_align_52_bits, geohash_calculate_areas_by_shape_wgs84, geohash_get_distance,
    geohash_get_distance_if_in_polygon, geohash_get_distance_if_in_radius_wgs84,
    geohash_get_distance_if_in_rectangle, GeoHashFix52Bits, GeoHashRadius,
};

// ── Constants ─────────────────────────────────────────────────────────────────

const SORT_NONE: u8 = 0;
const SORT_ASC: u8 = 1;
const SORT_DESC: u8 = 2;

const RADIUS_COORDS: u32 = 1 << 0;
const RADIUS_MEMBER: u32 = 1 << 1;
const RADIUS_NOSTORE: u32 = 1 << 2;
const GEOSEARCH_FLAG: u32 = 1 << 3;
const GEOSEARCHSTORE_FLAG: u32 = 1 << 4;

/// Base-32 geohash alphabet.
const GEO_ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// ── Types ─────────────────────────────────────────────────────────────────────

/// A single GEO search result.
struct GeoPoint {
    longitude: f64,
    latitude: f64,
    dist: f64,
    score: f64,
    member: Vec<u8>,
}

// ── Decode helper ─────────────────────────────────────────────────────────────

/// Decode a 52-bit geohash score to `[longitude, latitude]`.
fn decode_geohash(bits: f64) -> Option<[f64; 2]> {
    let hash = GeoHashBits {
        bits: bits as u64,
        step: GEO_STEP_MAX,
    };
    geohash_decode_to_long_lat_wgs84(hash)
}

// ── Argument parse helpers ────────────────────────────────────────────────────

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

fn geo_err(msg: impl AsRef<[u8]>) -> RedisError {
    let msg = msg.as_ref();
    let mut out = Vec::with_capacity(b"ERR ".len() + msg.len());
    out.extend_from_slice(b"ERR ");
    out.extend_from_slice(msg);
    RedisError::runtime(out)
}

fn geo_member_missing_error(member: &[u8]) -> RedisError {
    let mut out =
        Vec::with_capacity(b"ERR member ".len() + member.len() + b" does not exist".len());
    out.extend_from_slice(b"ERR member ");
    out.extend_from_slice(member);
    out.extend_from_slice(b" does not exist");
    RedisError::runtime(out)
}

fn geo_member_decode_error(member: &[u8]) -> RedisError {
    let mut out = Vec::with_capacity(
        b"ERR failed to decode, member ".len() + member.len() + b" is not a valid geohash".len(),
    );
    out.extend_from_slice(b"ERR failed to decode, member ");
    out.extend_from_slice(member);
    out.extend_from_slice(b" is not a valid geohash");
    RedisError::runtime(out)
}

/// Parse longitude and latitude from two consecutive command arguments.
fn extract_long_lat_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
    xy: &mut [f64; 2],
) -> Result<(), RedisError> {
    for i in 0..2usize {
        let raw = ctx.arg(arg_base + i)?.as_bytes().to_vec();
        xy[i] = parse_geo_f64(&raw)?;
    }
    if xy[0] < GEO_LONG_MIN || xy[0] > GEO_LONG_MAX || xy[1] < GEO_LAT_MIN || xy[1] > GEO_LAT_MAX {
        let msg = format!(
            "ERR invalid longitude,latitude pair {},{}\r\n",
            xy[0], xy[1]
        );
        return Err(RedisError::runtime(msg.as_bytes()));
    }
    Ok(())
}

/// Parse a unit string and return metres-per-unit conversion factor.
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
        Err(geo_err(
            b"unsupported unit provided. please use M, KM, FT, MI",
        ))
    }
}

/// Parse `<number> <unit>` from two consecutive arguments.
/// Returns `(metres_per_unit, distance_in_units)`.
fn extract_distance_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
) -> Result<(f64, f64), RedisError> {
    let dist_raw = ctx.arg(arg_base)?.as_bytes().to_vec();
    let distance = parse_geo_f64(&dist_raw).map_err(|_| geo_err(b"need numeric radius"))?;
    if distance < 0.0 {
        return Err(geo_err(b"radius cannot be negative"));
    }
    let unit_raw = ctx.arg(arg_base + 1)?.as_bytes().to_vec();
    let to_meters = extract_unit_or_reply(&unit_raw)?;
    Ok((to_meters, distance))
}

/// Parse `<width> <height> <unit>` from three consecutive arguments.
/// Returns `(metres_per_unit, width, height)`.
fn extract_box_or_reply(
    ctx: &mut CommandContext,
    arg_base: usize,
) -> Result<(f64, f64, f64), RedisError> {
    let w_raw = ctx.arg(arg_base)?.as_bytes().to_vec();
    let w = parse_geo_f64(&w_raw).map_err(|_| geo_err(b"need numeric width"))?;
    let h_raw = ctx.arg(arg_base + 1)?.as_bytes().to_vec();
    let h = parse_geo_f64(&h_raw).map_err(|_| geo_err(b"need numeric height"))?;
    if h < 0.0 || w < 0.0 {
        return Err(geo_err(b"height or width cannot be negative"));
    }
    let unit_raw = ctx.arg(arg_base + 2)?.as_bytes().to_vec();
    let to_meters = extract_unit_or_reply(&unit_raw)?;
    Ok((to_meters, w, h))
}

/// Format a distance to 4 decimal places as a bulk-string reply.
fn reply_double_distance(ctx: &mut CommandContext, d: f64) -> RedisResult<()> {
    let s = format!("{:.4}", d);
    ctx.reply_bulk(s.as_bytes())
}

/// Format a coordinate using the same precision Redis uses (LD_STR_HUMAN):
/// 17 significant decimal digits after the point, trailing zeros stripped.
fn format_coord(v: f64) -> Vec<u8> {
    let raw = format!("{:.17}", v);
    let bytes = raw.as_bytes();
    if !bytes.contains(&b'.') {
        return bytes.to_vec();
    }
    let trim_end = bytes
        .iter()
        .rposition(|&b| b != b'0')
        .map(|p| if bytes[p] == b'.' { p } else { p + 1 })
        .unwrap_or(bytes.len());
    bytes[..trim_end].to_vec()
}

// ── Shape containment test ────────────────────────────────────────────────────

/// Test whether a geohash score lies within `shape`.
/// Returns `Some((xy, dist_m))` if inside, `None` otherwise.
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
        GeoShapeKind::Polygon { points } => {
            geohash_get_distance_if_in_polygon(shape.xy[0], shape.xy[1], [xy[0], xy[1]], points)?
        }
    };
    Some((xy, distance))
}

// ── Range query helpers ───────────────────────────────────────────────────────

/// Query a sorted set for all members with scores in `[min, max)`,
/// filter by `shape`, and append matching points to `ga`.
/// Returns the count of new points added.
fn geo_get_points_in_range(
    zset: &InlineZSet,
    min: f64,
    max: f64,
    shape: &GeoShape,
    ga: &mut Vec<GeoPoint>,
    limit: usize,
) -> usize {
    let origin = ga.len();
    for (score, member) in zset.iter_ascending() {
        if score < min {
            continue;
        }
        if score >= max {
            break;
        }
        if limit > 0 && ga.len() >= limit {
            break;
        }
        if let Some((xy, distance)) = geo_within_shape(shape, score) {
            ga.push(GeoPoint {
                longitude: xy[0],
                latitude: xy[1],
                dist: distance,
                score,
                member: member.as_bytes().to_vec(),
            });
        }
    }
    ga.len() - origin
}

/// Compute the `[min, max)` score range covering a single geohash cell.
fn scores_of_geohash_box(hash: GeoHashBits) -> (GeoHashFix52Bits, GeoHashFix52Bits) {
    let min = geohash_align_52_bits(hash);
    let mut hash_next = hash;
    hash_next.bits = hash_next.bits.wrapping_add(1);
    let max = geohash_align_52_bits(hash_next);
    (min, max)
}

/// Populate `ga` with all zset members inside a single geohash cell.
fn members_of_geohash_box(
    zset: &InlineZSet,
    hash: GeoHashBits,
    ga: &mut Vec<GeoPoint>,
    shape: &GeoShape,
    limit: usize,
) -> usize {
    let (min, max) = scores_of_geohash_box(hash);
    geo_get_points_in_range(zset, min as f64, max as f64, shape, ga, limit)
}

/// Search across the centre geohash cell and all eight neighbours.
/// Duplicate adjacent cells (large radii) are skipped.
fn members_of_all_neighbors(
    zset: &InlineZSet,
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
    let mut last_processed: Option<usize> = None;

    for (i, &neighbor) in neighbors.iter().enumerate() {
        if neighbor.bits == 0 && neighbor.step == 0 {
            continue;
        }
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
        count += members_of_geohash_box(zset, neighbor, ga, shape, limit);
        last_processed = Some(i);
    }
    count
}

// ── Sort comparators ──────────────────────────────────────────────────────────

fn sort_gp_asc(a: &GeoPoint, b: &GeoPoint) -> std::cmp::Ordering {
    a.dist
        .partial_cmp(&b.dist)
        .unwrap_or(std::cmp::Ordering::Equal)
}

fn sort_gp_desc(a: &GeoPoint, b: &GeoPoint) -> std::cmp::Ordering {
    sort_gp_asc(a, b).reverse()
}

// ── ZSet accessors ────────────────────────────────────────────────────────────

/// Borrow the `InlineZSet` from a `RedisObject`, returning `WRONGTYPE` if
/// object exists but is not a sorted set, or `None` if the key is absent.
fn as_zset(obj: Option<&RedisObject>) -> RedisResult<Option<&InlineZSet>> {
    match obj {
        None => Ok(None),
        Some(o) => o.zset().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Retrieve the score for `member` in the sorted set object `obj`.
/// Returns `Err` if the member is absent.
fn zset_score_from_obj(obj: &RedisObject, member: &[u8]) -> RedisResult<f64> {
    let zset = obj.zset().ok_or_else(RedisError::wrong_type)?;
    let key = RedisString::from_bytes(member);
    zset.score(&key)
        .ok_or_else(|| geo_member_missing_error(member))
}

// ── ZADD helper ───────────────────────────────────────────────────────────────

/// Insert or update `(score, member)` pairs into the sorted set at `key`.
/// Applies NX / XX / CH semantics. Returns the integer reply value.
/// Used internally by GEOADD.
fn zadd_geo(
    ctx: &mut CommandContext,
    key: &RedisString,
    nx: bool,
    xx: bool,
    ch: bool,
    pairs: &[(f64, RedisString)],
) -> RedisResult<i64> {
    if let Some(existing) = ctx.db().lookup_key_read(key) {
        if !existing.is_zset() {
            return Err(RedisError::wrong_type());
        }
    }

    if ctx.db().lookup_key_read(key).is_none() {
        if xx {
            return Ok(0);
        }
        let obj = RedisObject::new_zset();
        ctx.db_mut().set_key(key.clone(), obj, 0);
    }

    let mut added: i64 = 0;
    let mut changed: i64 = 0;

    let zset = ctx
        .db_mut()
        .lookup_key_write(key)
        .and_then(|o| o.zset_mut())
        .ok_or_else(|| RedisError::runtime(b"internal: zset not found after create"))?;

    for (score, member) in pairs {
        let prev = zset.score(member);
        match prev {
            None => {
                if xx {
                    continue;
                }
                zset.upsert(member.clone(), *score);
                added += 1;
                changed += 1;
            }
            Some(_) => {
                if nx {
                    continue;
                }
                zset.upsert(member.clone(), *score);
                changed += 1;
            }
        }
    }

    if added > 0 || changed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zadd", key);
    }

    Ok(if ch { changed } else { added })
}

// ── Commands ──────────────────────────────────────────────────────────────────

/// GEOADD key [NX|XX] [CH] longitude latitude member [...]
pub fn geoadd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mut xx = false;
    let mut nx = false;
    let mut ch = false;
    let mut long_idx = 2usize;

    while long_idx < ctx.argc() {
        let opt = ctx.arg(long_idx)?.as_bytes().to_vec();
        if opt.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if opt.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if opt.eq_ignore_ascii_case(b"ch") {
            ch = true;
        } else {
            break;
        }
        long_idx += 1;
    }

    let remaining = ctx.argc().saturating_sub(long_idx);
    if !remaining.is_multiple_of(3) || remaining == 0 || (xx && nx) {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let elements = remaining / 3;
    let key = RedisString::from_bytes(ctx.arg(1)?.as_bytes());

    let mut pairs: Vec<(f64, RedisString)> = Vec::with_capacity(elements);
    for i in 0..elements {
        let base = long_idx + i * 3;
        let mut xy = [0.0_f64; 2];
        extract_long_lat_or_reply(ctx, base, &mut xy)?;

        let hash = geohash_encode_wgs84(xy[0], xy[1], GEO_STEP_MAX)
            .ok_or_else(|| RedisError::runtime(b"geohash encoding failed"))?;
        let bits: GeoHashFix52Bits = geohash_align_52_bits(hash);
        let score = bits as f64;

        let member = RedisString::from_bytes(ctx.arg(base + 2)?.as_bytes());
        pairs.push((score, member));
    }

    let count = zadd_geo(ctx, &key, nx, xx, ch, &pairs)?;
    ctx.reply_integer(count)
}

/// Core implementation of GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH, GEOSEARCHSTORE.
pub fn georadius_generic(
    ctx: &mut CommandContext,
    src_key_index: usize,
    flags: u32,
) -> RedisResult<()> {
    let mut storekey: Option<RedisString> = None;
    let mut storedist = false;

    let src_key = RedisString::from_bytes(ctx.arg(src_key_index)?.as_bytes());

    let zobj_present = match ctx.db().lookup_key_read(&src_key) {
        Some(z) if !z.is_zset() => return Err(RedisError::wrong_type()),
        Some(_) => true,
        None => false,
    };

    let base_args: usize;
    let mut shape = GeoShape {
        xy: [0.0; 2],
        conversion: 1.0,
        bounds: [0.0; 4],
        kind: GeoShapeKind::Circular { radius: 0.0 },
    };

    if flags & RADIUS_COORDS != 0 {
        base_args = 6;
        extract_long_lat_or_reply(ctx, 2, &mut shape.xy)?;
        let (conversion, radius) = extract_distance_or_reply(ctx, base_args - 2)?;
        shape.conversion = conversion;
        shape.kind = GeoShapeKind::Circular { radius };
    } else if flags & RADIUS_MEMBER != 0 && !zobj_present {
        base_args = 5;
    } else if flags & RADIUS_MEMBER != 0 {
        base_args = 5;
        let member = ctx.arg(2)?.as_bytes().to_vec();
        let score = {
            let zobj = ctx.db().lookup_key_read(&src_key);
            match zobj {
                Some(z) => zset_score_from_obj(z, &member)?,
                None => return Err(geo_member_missing_error(&member)),
            }
        };
        let xy = decode_geohash(score).ok_or_else(|| geo_member_decode_error(&member))?;
        shape.xy = xy;
        let (conversion, radius) = extract_distance_or_reply(ctx, base_args - 2)?;
        shape.conversion = conversion;
        shape.kind = GeoShapeKind::Circular { radius };
    } else if flags & GEOSEARCH_FLAG != 0 {
        if flags & GEOSEARCHSTORE_FLAG != 0 {
            storekey = Some(RedisString::from_bytes(ctx.arg(1)?.as_bytes()));
            base_args = 3;
        } else {
            base_args = 2;
        }
    } else {
        return Err(geo_err(b"Unknown georadius search type"));
    }

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
                return Err(geo_err(b"COUNT must be > 0"));
            }
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"store")
            && i + 1 < remaining
            && flags & RADIUS_NOSTORE == 0
            && flags & GEOSEARCH_FLAG == 0
        {
            storekey = Some(RedisString::from_bytes(
                ctx.arg(base_args + i + 1)?.as_bytes(),
            ));
            storedist = false;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"storedist")
            && i + 1 < remaining
            && flags & RADIUS_NOSTORE == 0
            && flags & GEOSEARCH_FLAG == 0
        {
            storekey = Some(RedisString::from_bytes(
                ctx.arg(base_args + i + 1)?.as_bytes(),
            ));
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
                let score = {
                    let zobj = ctx.db().lookup_key_read(&src_key);
                    match zobj {
                        Some(z) => zset_score_from_obj(z, &member)?,
                        None => return Err(geo_member_missing_error(&member)),
                    }
                };
                let xy = decode_geohash(score).ok_or_else(|| geo_member_decode_error(&member))?;
                shape.xy = xy;
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
            shape.kind = GeoShapeKind::Rectangle {
                height: h,
                width: w,
            };
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
            let num_vertices =
                parse_geo_i32(&nv_raw).map_err(|_| geo_err(b"invalid number of vertices"))?;
            let possible = remaining.saturating_sub(i + 2) / 2;
            if num_vertices < 3 || (possible as i32) < num_vertices {
                return Err(geo_err(
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

    if storekey.is_some() && (withdist || withhash || withcoords) {
        let msg = if flags & GEOSEARCHSTORE_FLAG != 0 {
            b"GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
                as &[u8]
        } else {
            b"STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        };
        return Err(geo_err(msg));
    }

    if flags & GEOSEARCH_FLAG != 0 && !frommember && !fromloc && !bypolygon {
        let cmd = ctx.command_name();
        let mut msg = Vec::with_capacity(
            b"exactly one of FROMMEMBER or FROMLONLAT can be specified for ".len() + cmd.len(),
        );
        msg.extend_from_slice(b"exactly one of FROMMEMBER or FROMLONLAT can be specified for ");
        msg.extend_from_slice(cmd);
        return Err(geo_err(msg));
    }

    if flags & GEOSEARCH_FLAG != 0 && !byradius && !bybox && !bypolygon {
        let cmd = ctx.command_name();
        let mut msg = Vec::with_capacity(
            b"exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for ".len() + cmd.len(),
        );
        msg.extend_from_slice(
            b"exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for ",
        );
        msg.extend_from_slice(cmd);
        return Err(geo_err(msg));
    }

    if any && count == 0 {
        return Err(geo_err(b"the ANY argument requires COUNT argument"));
    }

    if !zobj_present {
        if let Some(ref sk) = storekey {
            let sk = sk.clone();
            if ctx.db_mut().delete(&sk) {
                ctx.db_mut().signal_modified(&sk);
                ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &sk);
            }
            return ctx.reply_integer(0);
        } else {
            return ctx.reply_array_header(0usize);
        }
    }

    if count != 0 && sort == SORT_NONE && !any {
        sort = SORT_ASC;
    }

    let georadius = geohash_calculate_areas_by_shape_wgs84(&mut shape)
        .ok_or_else(|| RedisError::runtime(b"geohash area calculation failed"))?;

    let limit = if any { count as usize } else { 0 };
    let mut ga: Vec<GeoPoint> = Vec::new();

    {
        let zset_opt = ctx.db().lookup_key_read(&src_key).and_then(|o| o.zset());
        if let Some(zset) = zset_opt {
            members_of_all_neighbors(zset, &georadius, &shape, &mut ga, limit);
        }
    }

    if ga.is_empty() && storekey.is_none() {
        return ctx.reply_array_header(0usize);
    }

    let result_length = ga.len();
    let returned_items = if count == 0 || (result_length as i64) < count {
        result_length
    } else {
        count as usize
    };

    if sort == SORT_ASC {
        ga.sort_by(sort_gp_asc);
    } else if sort == SORT_DESC {
        ga.sort_by(sort_gp_desc);
    }

    if storekey.is_none() {
        let option_count = withdist as usize + withcoords as usize + withhash as usize;
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
                ctx.reply_array_header(2usize)?;
                ctx.reply_bulk(&format_coord(gp.longitude))?;
                ctx.reply_bulk(&format_coord(gp.latitude))?;
            }
        }
    } else {
        let sk = storekey.unwrap();
        if returned_items > 0 {
            let mut new_zset = InlineZSet::new();
            for gp in ga.iter_mut().take(returned_items) {
                gp.dist /= shape.conversion;
                let score = if storedist { gp.dist } else { gp.score };
                let member = RedisString::from_bytes(&gp.member);
                new_zset.upsert(member, score);
            }
            let obj = RedisObject::new_zset_from_inline(new_zset);
            ctx.db_mut().set_key(sk.clone(), obj, 0);
            ctx.notify_keyspace_event(NOTIFY_ZSET, b"geosearchstore", &sk);
        } else if ctx.db_mut().delete(&sk) {
            ctx.db_mut().signal_modified(&sk);
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &sk);
        }
        ctx.reply_integer(returned_items as i64)?;
    }

    Ok(())
}

/// GEORADIUS key x y radius unit [options]
pub fn georadius_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 1, RADIUS_COORDS)
}

/// GEORADIUSBYMEMBER key member radius unit [options]
pub fn georadiusbymember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 1, RADIUS_MEMBER)
}

/// GEORADIUS_RO — read-only variant.
pub fn georadiusro_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 1, RADIUS_COORDS | RADIUS_NOSTORE)
}

/// GEORADIUSBYMEMBER_RO — read-only variant.
pub fn georadiusbymemberro_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 1, RADIUS_MEMBER | RADIUS_NOSTORE)
}

/// GEOSEARCH key [FROMMEMBER member|FROMLONLAT lon lat]
/// [BYRADIUS r unit|BYBOX w h unit|BYPOLYGON n...]
/// [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
pub fn geosearch_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 1, GEOSEARCH_FLAG)
}

/// GEOSEARCHSTORE dest src [options] [STOREDIST]
pub fn geosearchstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    georadius_generic(ctx, 2, GEOSEARCH_FLAG | GEOSEARCHSTORE_FLAG)
}

/// GEOHASH key member [member...]
/// Returns an 11-character base-32 geohash string for each member.
pub fn geohash_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let key = RedisString::from_bytes(ctx.arg(1)?.as_bytes());

    if as_zset(ctx.db().lookup_key_read(&key))?.is_none() {
        let n = ctx.argc() - 2;
        ctx.reply_array_header(n)?;
        for _ in 0..n {
            ctx.reply_null()?;
        }
        return Ok(());
    }

    let argc = ctx.argc();
    ctx.reply_array_header(argc - 2)?;

    for j in 2..argc {
        let member = ctx.arg(j)?.as_bytes().to_vec();

        let score_opt = {
            let zobj = ctx.db().lookup_key_read(&key);
            match zobj.and_then(|o| o.zset()) {
                None => None,
                Some(z) => {
                    let k = RedisString::from_bytes(&member);
                    z.score(&k)
                }
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

        let long_range = GeoHashRange {
            min: -180.0,
            max: 180.0,
        };
        let lat_range = GeoHashRange {
            min: -90.0,
            max: 90.0,
        };
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

/// GEOPOS key member [member...]
/// Returns `[[longitude, latitude],...]`; null array for missing members.
pub fn geopos_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let key = RedisString::from_bytes(ctx.arg(1)?.as_bytes());

    if let Some(obj) = ctx.db().lookup_key_read(&key) {
        if !obj.is_zset() {
            return Err(RedisError::wrong_type());
        }
    }

    let argc = ctx.argc();
    ctx.reply_array_header(argc - 2)?;

    for j in 2..argc {
        let member = ctx.arg(j)?.as_bytes().to_vec();
        let score_opt = {
            let zobj = ctx.db().lookup_key_read(&key);
            match zobj.and_then(|o| o.zset()) {
                None => None,
                Some(z) => {
                    let k = RedisString::from_bytes(&member);
                    z.score(&k)
                }
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

        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk(&format_coord(xy[0]))?;
        ctx.reply_bulk(&format_coord(xy[1]))?;
    }

    Ok(())
}

/// GEODIST key member1 member2 [unit]
/// Returns the great-circle distance in the requested unit, or null if either
/// member is missing.
pub fn geodist_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.argc();
    let to_meter = if argc == 5 {
        let unit = ctx.arg(4)?.as_bytes().to_vec();
        extract_unit_or_reply(&unit)?
    } else if argc > 5 {
        return Err(RedisError::syntax(b"syntax error"));
    } else {
        1.0_f64
    };

    let key = RedisString::from_bytes(ctx.arg(1)?.as_bytes());

    match ctx.db().lookup_key_read(&key) {
        None => {
            ctx.reply_null()?;
            return Ok(());
        }
        Some(z) if !z.is_zset() => {
            return Err(RedisError::wrong_type());
        }
        _ => {}
    }

    let m1 = ctx.arg(2)?.as_bytes().to_vec();
    let m2 = ctx.arg(3)?.as_bytes().to_vec();

    let (score1, score2) = {
        let zobj = ctx.db().lookup_key_read(&key);
        match zobj.and_then(|o| o.zset()) {
            None => {
                ctx.reply_null()?;
                return Ok(());
            }
            Some(z) => {
                let k1 = RedisString::from_bytes(&m1);
                let k2 = RedisString::from_bytes(&m2);
                (z.score(&k1), z.score(&k2))
            }
        }
    };

    let score1 = match score1 {
        None => {
            ctx.reply_null()?;
            return Ok(());
        }
        Some(s) => s,
    };
    let score2 = match score2 {
        None => {
            ctx.reply_null()?;
            return Ok(());
        }
        Some(s) => s,
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

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  src/geo.h  (25 lines)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         3
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Full implementation. Cross-crate blockers resolved:
//                  (1) zset_score → inline zset accessor (InlineZSet::score).
//                  (2) zadd_generic → zadd_geo helper using InlineZSet::upsert.
//                  (3) geo_get_points_in_range → iter_ascending range scan.
//                  GEOPOS coordinates use format_coord (LD_STR_HUMAN equivalent).
//                  GEOSEARCHSTORE stores results via InlineZSet + set_key.
//                  TODO(architect): BYPOLYGON is wired but not smoke-tested.
//                  TODO(architect): partial-sort (pqsort) for COUNT without ANY.
//                  TODO(architect): addReplyHumanLongDouble precision parity.
// ──────────────────────────────────────────────────────────────────────────────

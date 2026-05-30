//! Geohash encoding and decoding for Redis GEO commands.
//! The geohash algorithm divides the world into a grid by interleaving
//! bit representations of normalised longitude (x) and latitude (y) into a
//! single 64-bit integer. Encoding quantises a (lon, lat) pair into a
//! `GeoHashBits` value; decoding recovers the bounding box of the cell;
//! neighbour computation shifts the cell one step in any of the eight
//! cardinal/diagonal directions.
//! All public functions are pure (no Redis I/O, no `CommandContext`).
//! The original C functions return `int` (0 = failure, 1 = success) through
//! out-parameters; the Rust translation uses `Option<T>` instead.

// ── Constants ────────────────────────────────────────────

/// Maximum step value: 26 steps × 2 bits = 52-bit precision.
pub const GEO_STEP_MAX: u8 = 26;

/// EPSG:900913 / EPSG:3785 / OSGEO:41001 latitude bounds.
pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;

/// EPSG:900913 longitude bounds.
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

/// Shape-type discriminants (mirrors the C `#define` constants).
pub const CIRCULAR_TYPE: i32 = 1;
pub const RECTANGLE_TYPE: i32 = 2;
pub const POLYGON_TYPE: i32 = 3;

// ── Types ────────────────────────────────────────────────

/// Cardinal / diagonal directions for geohash neighbour lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoDirection {
    North = 0,
    East,
    West,
    South,
    SouthWest,
    SouthEast,
    NorthWest,
    NorthEast,
}

/// A raw geohash value: `bits` holds the interleaved lat/lon, `step` is
/// precision (number of coordinate-pair bits on each axis, 1–32).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

/// A continuous range [min, max] for a single geographic axis.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashRange {
    pub min: f64,
    pub max: f64,
}

/// The decoded bounding box of a geohash cell.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashArea {
    pub hash: GeoHashBits,
    pub longitude: GeoHashRange,
    pub latitude: GeoHashRange,
}

/// The eight neighbouring cells of a geohash cell.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashNeighbors {
    pub north: GeoHashBits,
    pub east: GeoHashBits,
    pub west: GeoHashBits,
    pub south: GeoHashBits,
    pub north_east: GeoHashBits,
    pub south_east: GeoHashBits,
    pub north_west: GeoHashBits,
    pub south_west: GeoHashBits,
}

/// The shape variant for a geo search region.
/// PORT NOTE: The C `GeoShape` struct uses an anonymous `union` discriminated
/// by an `int type` field (`CIRCULAR_TYPE`, `RECTANGLE_TYPE`, `POLYGON_TYPE`).
/// Rust's enum collapses both the discriminant and the payload; the `type`
/// integer is no longer needed as a separate field.
#[derive(Debug, Clone, PartialEq)]
pub enum GeoShapeKind {
 /// `CIRCULAR_TYPE` — a circle with the given radius (in the unit set by
 /// `conversion`; typically metres).
    Circular { radius: f64 },
 /// `RECTANGLE_TYPE` — an axis-aligned rectangle.
    Rectangle { height: f64, width: f64 },
 /// `POLYGON_TYPE` — an arbitrary polygon. Each element is `[lon, lat]`.
 /// PORT NOTE: The C `num_vertices` field is redundant with `points.len`.
    Polygon { points: Vec<[f64; 2]> },
}

/// A geo-search shape with its centre point, unit conversion factor,
/// pre-computed axis-aligned bounding box.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoShape {
 /// Search centre: `xy[0]` = longitude, `xy[1]` = latitude.
    pub xy: [f64; 2],
 /// Unit conversion factor (e.g. 1000 for km→m).
    pub conversion: f64,
 /// Pre-computed AABB: `[min_lon, min_lat, max_lon, max_lat]`.
    pub bounds: [f64; 4],
 /// The actual shape kind and its parameters.
    pub kind: GeoShapeKind,
}

// ── Private helpers ───────────────────────────────────────────────────────

/// Interleave the lower bits of `xlo` (even positions) and `ylo` (odd
/// positions) into a single 64-bit integer.
/// Both inputs must be < 2³² (i.e. fit in 32 bits).
/// Algorithm: <https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN>
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    x | (y << 1)
}

/// Reverse the interleave: recover the even-bit component (low 32 bits
/// result) and the odd-bit component (high 32 bits of result) from a 64-bit
/// interleaved value.
/// Derived: <http://stackoverflow.com/questions/4909263>
fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
        0x0000_0000_FFFF_FFFF,
    ];
 // S[0] = 0 intentionally: the first mask step needs no shift (x | x = x).
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    x | (y << 32)
}

/// Shift the longitude (x) component of `hash` by one step in direction `d`
/// (+1 = east, −1 = west, 0 = no-op).
fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA_u64;
    let y = hash.bits & 0x5555_5555_5555_5555_u64;

    let shift = 64u32 - (hash.step as u32) * 2;
    let zz = 0x5555_5555_5555_5555_u64 >> shift;

    let mut x = if d > 0 {
        x.wrapping_add(zz.wrapping_add(1))
    } else {
        let x = x | zz;
        x.wrapping_sub(zz.wrapping_add(1))
    };

    x &= 0xAAAA_AAAA_AAAA_AAAA_u64 >> shift;
    hash.bits = x | y;
}

/// Shift the latitude (y) component of `hash` by one step in direction `d`
/// (+1 = north, −1 = south, 0 = no-op).
fn geohash_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA_u64;
    let y = hash.bits & 0x5555_5555_5555_5555_u64;

    let shift = 64u32 - (hash.step as u32) * 2;
    let zz = 0xAAAA_AAAA_AAAA_AAAA_u64 >> shift;

    let mut y = if d > 0 {
        y.wrapping_add(zz.wrapping_add(1))
    } else {
        let y = y | zz;
        y.wrapping_sub(zz.wrapping_add(1))
    };

    y &= 0x5555_5555_5555_5555_u64 >> shift;
    hash.bits = x | y;
}

// ── Public API ────────────────────────────────────────────────────────────

/// Return the standard WGS84-compatible coordinate ranges used by all Redis
/// GEO commands (EPSG:900913 / EPSG:3785 / OSGEO:41001).
/// Returns `(long_range, lat_range)`.
pub fn geohash_get_coord_range() -> (GeoHashRange, GeoHashRange) {
    let long_range = GeoHashRange {
        max: GEO_LONG_MAX,
        min: GEO_LONG_MIN,
    };
    let lat_range = GeoHashRange {
        max: GEO_LAT_MAX,
        min: GEO_LAT_MIN,
    };
    (long_range, lat_range)
}

/// Encode `(longitude, latitude)` into a `GeoHashBits` with the given
/// `step` precision using the supplied coordinate ranges.
/// Returns `None` if any argument is out of range or `step` is 0 or > 32.
pub fn geohash_encode(
    long_range: &GeoHashRange,
    lat_range: &GeoHashRange,
    longitude: f64,
    latitude: f64,
    step: u8,
) -> Option<GeoHashBits> {
    if step > 32
        || step == 0
        || (lat_range.max == 0.0 && lat_range.min == 0.0)
        || (long_range.max == 0.0 && long_range.min == 0.0)
    {
        return None;
    }

    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        return None;
    }

    if latitude < lat_range.min
        || latitude > lat_range.max
        || longitude < long_range.min
        || longitude > long_range.max
    {
        return None;
    }

    let lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    let long_offset = (longitude - long_range.min) / (long_range.max - long_range.min);

 // Convert to fixed-point based on step size.
    let lat_fp = lat_offset * ((1u64 << step) as f64);
    let long_fp = long_offset * ((1u64 << step) as f64);

    Some(GeoHashBits {
        bits: interleave64(lat_fp as u32, long_fp as u32),
        step,
    })
}

/// Encode using the default WGS84 coordinate ranges.
pub fn geohash_encode_type(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    let (long_range, lat_range) = geohash_get_coord_range();
    geohash_encode(&long_range, &lat_range, longitude, latitude, step)
}

/// Alias for `geohash_encode_type` (WGS84 is the only supported datum).
pub fn geohash_encode_wgs84(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    geohash_encode_type(longitude, latitude, step)
}

/// Decode a `GeoHashBits` value into a `GeoHashArea` (bounding box) using
/// the supplied coordinate ranges.
/// Returns `None` if `hash` is zero-valued or any range is zero.
pub fn geohash_decode(
    long_range: GeoHashRange,
    lat_range: GeoHashRange,
    hash: GeoHashBits,
) -> Option<GeoHashArea> {
    if (hash.bits == 0 && hash.step == 0)
        || (lat_range.max == 0.0 && lat_range.min == 0.0)
        || (long_range.max == 0.0 && long_range.min == 0.0)
    {
        return None;
    }

    let step = hash.step;
 // hash = [LAT][LONG] after deinterleave
    let hash_sep = deinterleave64(hash.bits);

    let lat_scale = lat_range.max - lat_range.min;
    let long_scale = long_range.max - long_range.min;

 // Low 32 bits = lat part; high 32 bits = long part.
    let ilato = hash_sep as u32;
    let ilono = (hash_sep >> 32) as u32;

    let step_divisor = (1u64 << step) as f64;

    let area = GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: lat_range.min + (ilato as f64 / step_divisor) * lat_scale,
            max: lat_range.min + ((ilato as f64 + 1.0) / step_divisor) * lat_scale,
        },
        longitude: GeoHashRange {
            min: long_range.min + (ilono as f64 / step_divisor) * long_scale,
            max: long_range.min + ((ilono as f64 + 1.0) / step_divisor) * long_scale,
        },
    };

    Some(area)
}

/// Decode using the default WGS84 coordinate ranges.
pub fn geohash_decode_type(hash: GeoHashBits) -> Option<GeoHashArea> {
    let (long_range, lat_range) = geohash_get_coord_range();
    geohash_decode(long_range, lat_range, hash)
}

/// Alias for `geohash_decode_type`.
pub fn geohash_decode_wgs84(hash: GeoHashBits) -> Option<GeoHashArea> {
    geohash_decode_type(hash)
}

/// Compute the centre point of a `GeoHashArea` and clamp it to valid bounds.
/// Returns `Some([longitude, latitude])`, or `None` if `area` is degenerate.
pub fn geohash_decode_area_to_long_lat(area: &GeoHashArea) -> Option<[f64; 2]> {
    let mut lon = (area.longitude.min + area.longitude.max) / 2.0;
    if lon > GEO_LONG_MAX {
        lon = GEO_LONG_MAX;
    }
    if lon < GEO_LONG_MIN {
        lon = GEO_LONG_MIN;
    }

    let mut lat = (area.latitude.min + area.latitude.max) / 2.0;
    if lat > GEO_LAT_MAX {
        lat = GEO_LAT_MAX;
    }
    if lat < GEO_LAT_MIN {
        lat = GEO_LAT_MIN;
    }

    Some([lon, lat])
}

/// Decode a hash to its centre `[longitude, latitude]` using WGS84 ranges.
/// Returns `None` if the hash is invalid.
pub fn geohash_decode_to_long_lat_type(hash: GeoHashBits) -> Option<[f64; 2]> {
    let area = geohash_decode_type(hash)?;
    geohash_decode_area_to_long_lat(&area)
}

/// Alias for `geohash_decode_to_long_lat_type`.
pub fn geohash_decode_to_long_lat_wgs84(hash: GeoHashBits) -> Option<[f64; 2]> {
    geohash_decode_to_long_lat_type(hash)
}

/// Compute all eight neighbouring geohash cells of `hash`.
pub fn geohash_neighbors(hash: &GeoHashBits) -> GeoHashNeighbors {
    let mut neighbors = GeoHashNeighbors {
        east: *hash,
        west: *hash,
        north: *hash,
        south: *hash,
        south_east: *hash,
        south_west: *hash,
        north_east: *hash,
        north_west: *hash,
    };

    geohash_move_x(&mut neighbors.east, 1);
    geohash_move_y(&mut neighbors.east, 0);

    geohash_move_x(&mut neighbors.west, -1);
    geohash_move_y(&mut neighbors.west, 0);

    geohash_move_x(&mut neighbors.south, 0);
    geohash_move_y(&mut neighbors.south, -1);

    geohash_move_x(&mut neighbors.north, 0);
    geohash_move_y(&mut neighbors.north, 1);

    geohash_move_x(&mut neighbors.north_west, -1);
    geohash_move_y(&mut neighbors.north_west, 1);

    geohash_move_x(&mut neighbors.north_east, 1);
    geohash_move_y(&mut neighbors.north_east, 1);

    geohash_move_x(&mut neighbors.south_east, 1);
    geohash_move_y(&mut neighbors.south_east, -1);

    geohash_move_x(&mut neighbors.south_west, -1);
    geohash_move_y(&mut neighbors.south_west, -1);

    neighbors
}

// ── Unit tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleave_roundtrip() {
        let x: u32 = 0xDEAD_BEEF;
        let y: u32 = 0xCAFE_BABE;
        let interleaved = interleave64(x, y);
        let deinterleaved = deinterleave64(interleaved);
        assert_eq!(deinterleaved as u32, x);
        assert_eq!((deinterleaved >> 32) as u32, y);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let lon = 13.361389;
        let lat = 38.115556;
        let step = 26u8;

        let hash = geohash_encode_wgs84(lon, lat, step).expect("encode should succeed");
        let decoded = geohash_decode_to_long_lat_wgs84(hash).expect("decode should succeed");

 // Precision at step=26 is sub-millimetre; allow 1e-5 degree tolerance.
        assert!(
            (decoded[0] - lon).abs() < 1e-5,
            "lon mismatch: {} vs {}",
            decoded[0],
            lon
        );
        assert!(
            (decoded[1] - lat).abs() < 1e-5,
            "lat mismatch: {} vs {}",
            decoded[1],
            lat
        );
    }

    #[test]
    fn encode_rejects_invalid_step() {
        assert!(geohash_encode_wgs84(0.0, 0.0, 0).is_none());
        assert!(geohash_encode_wgs84(0.0, 0.0, 33).is_none());
    }

    #[test]
    fn encode_rejects_out_of_range_coords() {
        assert!(geohash_encode_wgs84(181.0, 0.0, 10).is_none());
        assert!(geohash_encode_wgs84(0.0, 86.0, 10).is_none());
    }

    #[test]
    fn neighbors_are_distinct() {
        let hash = geohash_encode_wgs84(0.0, 0.0, 10).expect("encode");
        let n = geohash_neighbors(&hash);
        let cells = [
            n.north,
            n.south,
            n.east,
            n.west,
            n.north_east,
            n.north_west,
            n.south_east,
            n.south_west,
        ];
        for i in 0..cells.len() {
            for j in (i + 1)..cells.len() {
                assert_ne!(
                    cells[i].bits, cells[j].bits,
                    "neighbours[{}] == neighbours[{}]",
                    i, j
                );
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  src/geohash.h  (144 lines, type + constant definitions)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Pure math module; no Redis I/O.  C int-return / out-param
//                  pattern replaced with Option<T>.  C union in GeoShape
//                  replaced with GeoShapeKind enum.  C static helpers
//                  interleave64/deinterleave64/geohash_move_{x,y} kept
//                  private (fn, not pub).  geohashNeighbors returns by value
//                  instead of mutating an out-pointer.
// ──────────────────────────────────────────────────────────────────────────

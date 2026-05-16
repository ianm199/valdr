//! Geohash helper utilities for GEO radius / shape queries.
//!
//! Ported from `geohash_helper.c` / `geohash_helper.h`
//! (369 lines C + 66 lines header; 10 public functions).
//!
//! This module provides:
//!   * Step-size estimation for radius searches (`geohash_estimate_steps_by_radius`).
//!   * Bounding-box computation for circle, rectangle, and polygon shapes.
//!   * Area enumeration for range queries (`geohash_calculate_areas_by_shape_wgs84`).
//!   * Great-circle distance functions (haversine formula).
//!   * Point-in-{radius, rectangle, polygon} membership tests.
//!
//! All functions are pure computational helpers; they do not touch Redis I/O or
//! `CommandContext`.  The original C functions used out-parameters and `int`
//! return codes; this port converts them to `Option<T>` returns.
//!
//! C source ref: geohash_helper.c:1-369, geohash_helper.h:1-66.

use super::geohash_geohash::{
    geohash_decode, geohash_encode, geohash_get_coord_range, geohash_neighbors, GeoHashArea,
    GeoHashBits, GeoHashNeighbors, GeoHashRange, GeoShape, GeoShapeKind, GEO_LAT_MAX, GEO_LAT_MIN,
    GEO_LONG_MAX, GEO_LONG_MIN,
};

// ── Type aliases (from geohash_helper.h) ────────────────────────────────────

/// A 52-bit aligned geohash value stored in a 64-bit integer.
///
/// C: `typedef uint64_t GeoHashFix52Bits;`
pub type GeoHashFix52Bits = u64;

/// A variable-precision geohash value stored in a 64-bit integer.
///
/// C: `typedef uint64_t GeoHashVarBits;`
pub type GeoHashVarBits = u64;

// ── GeoHashRadius (from geohash_helper.h) ────────────────────────────────────

/// The result of a geohash area search: the centre hash, its bounding area,
/// and the eight neighbouring hashes (some may be zeroed out when they fall
/// entirely outside the search region).
///
/// C: `GeoHashRadius` struct in geohash_helper.h:44-48.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashRadius {
    pub hash: GeoHashBits,
    pub area: GeoHashArea,
    pub neighbors: GeoHashNeighbors,
}

// ── Constants ────────────────────────────────────────────────────────────────

/// Earth's quadratic mean radius for WGS-84 (metres).
///
/// C: `const double EARTH_RADIUS_IN_METERS = 6372797.560856;`
pub const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;

/// Maximum Mercator projected coordinate (metres).
///
/// C: `const double MERCATOR_MAX = 20037726.37;`
pub const MERCATOR_MAX: f64 = 20037726.37;

/// Minimum Mercator projected coordinate (metres).
///
/// C: `const double MERCATOR_MIN = -20037726.37;`
pub const MERCATOR_MIN: f64 = -20037726.37;

// ── Private angle-conversion helpers ─────────────────────────────────────────

/// Convert degrees to radians.
///
/// C: `static inline double deg_rad(double ang)` — geohash_helper.c:55-57.
#[inline]
fn deg_rad(ang: f64) -> f64 {
    ang * (std::f64::consts::PI / 180.0)
}

/// Convert radians to degrees.
///
/// C: `static inline double rad_deg(double ang)` — geohash_helper.c:58-60.
#[inline]
fn rad_deg(ang: f64) -> f64 {
    ang / (std::f64::consts::PI / 180.0)
}

// ── Public functions ──────────────────────────────────────────────────────────

/// Estimate the geohash step precision needed to cover `range_meters` at the
/// given latitude.
///
/// Returns a step value in 1..=26 (26 being maximum precision).  A higher step
/// value means smaller cells; a lower step means larger cells.  The latitude
/// correction widens the search towards the poles.
///
/// C: `geohashEstimateStepsByRadius` — geohash_helper.c:64-85.
pub fn geohash_estimate_steps_by_radius(mut range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }
    step -= 2;

    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }

    if step < 1 {
        step = 1;
    }
    if step > 26 {
        step = 26;
    }
    step as u8
}

/// Compute the axis-aligned bounding box for `shape` and store it in
/// `shape.bounds`.
///
/// For `GeoShapeKind::Polygon`, the function also derives the centroid and
/// writes it into `shape.xy[0]` (longitude) and `shape.xy[1]` (latitude).
///
/// Returns `true` on success; `false` is never returned by the current
/// implementation (mirrors C convention where passing `NULL` bounds returns 0).
///
/// PORT NOTE: The C function takes a separate `double *bounds` out-parameter.
/// Because `GeoShape` already owns a `bounds` field and the only call site
/// passes `shape->bounds`, this port writes directly to `shape.bounds`.
///
/// C: `geohashBoundingBox` — geohash_helper.c:102-164.
pub fn geohash_bounding_box(shape: &mut GeoShape) -> bool {
    let conversion = shape.conversion;

    // PORT NOTE: The polygon case must compute the centroid and write it back
    // to shape.xy, so it is handled before the shared lon/lat path to avoid
    // a simultaneous &shape.kind borrow while mutating shape.xy.
    if let GeoShapeKind::Polygon { .. } = &shape.kind {
        // Clone points to release the immutable borrow on shape.kind before
        // mutating shape.xy and shape.bounds below.
        // PERF(port): clone of polygon points — profile in Phase B.
        let points = match &shape.kind {
            GeoShapeKind::Polygon { points } => points.clone(),
            _ => unreachable!(),
        };

        let mut x = 0.0_f64;
        let mut y = 0.0_f64;
        let mut z = 0.0_f64;
        let mut min_lon = GEO_LONG_MAX;
        let mut max_lon = GEO_LONG_MIN;
        let mut min_lat = GEO_LAT_MAX;
        let mut max_lat = GEO_LAT_MIN;

        for point in &points {
            let longitude = point[0];
            let latitude = point[1];
            if longitude < min_lon {
                min_lon = longitude;
            }
            if longitude > max_lon {
                max_lon = longitude;
            }
            if latitude < min_lat {
                min_lat = latitude;
            }
            if latitude > max_lat {
                max_lat = latitude;
            }
            // Accumulate Cartesian coordinates for centroid (no div by N needed;
            // only the angle matters).
            // C: geohash_helper.c:129-136.
            let lon_rad = deg_rad(longitude);
            let lat_rad = deg_rad(latitude);
            x += lat_rad.cos() * lon_rad.cos();
            y += lat_rad.cos() * lon_rad.sin();
            z += lat_rad.sin();
        }

        shape.bounds = [min_lon, min_lat, max_lon, max_lat];

        // Recover centroid lon/lat from accumulated Cartesian sum.
        let central_lon = y.atan2(x);
        let central_hyp = (x * x + y * y).sqrt();
        let central_lat = z.atan2(central_hyp);
        shape.xy[0] = rad_deg(central_lon);
        shape.xy[1] = rad_deg(central_lat);
        return true;
    }

    // For circular and rectangular shapes, compute height/width half-extents
    // in metres then convert to lat/lon deltas.
    let (height, width) = match &shape.kind {
        GeoShapeKind::Circular { radius } => (conversion * radius, conversion * radius),
        GeoShapeKind::Rectangle { height, width } => {
            (conversion * height / 2.0, conversion * width / 2.0)
        }
        GeoShapeKind::Polygon { .. } => unreachable!(),
    };
    // Immutable borrow on shape.kind released here (NLL).

    let longitude = shape.xy[0];
    let latitude = shape.xy[1];
    let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
    let long_delta_top =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude - lat_delta).cos());

    // Southern hemisphere: the wider edge is at the bottom (southern boundary).
    // C: geohash_helper.c:158-163.
    let southern_hemisphere = latitude < 0.0;
    shape.bounds[0] = if southern_hemisphere {
        longitude - long_delta_bottom
    } else {
        longitude - long_delta_top
    };
    shape.bounds[2] = if southern_hemisphere {
        longitude + long_delta_bottom
    } else {
        longitude + long_delta_top
    };
    shape.bounds[1] = latitude - lat_delta;
    shape.bounds[3] = latitude + lat_delta;
    true
}

/// Compute the nine geohash areas (centre + 8 neighbours) that cover the given
/// search shape, expressed in WGS-84 coordinates.
///
/// The returned `GeoHashRadius` contains the centre hash, its decoded bounding
/// area, and the eight neighbours.  Neighbours whose cells fall entirely outside
/// the search region are zeroed out (bits = 0, step = 0) as a pruning optimisation.
///
/// Returns `None` if the coordinate encoding fails (e.g. inputs out of range).
///
/// PORT NOTE: The C function returns a `GeoHashRadius` by value; out-of-range
/// failures are not signalled (the callee leaves `hash` uninitialised).  This
/// port uses `Option<GeoHashRadius>` to surface failures without panicking.
///
/// C: `geohashCalculateAreasByShapeWGS84` — geohash_helper.c:169-274.
pub fn geohash_calculate_areas_by_shape_wgs84(shape: &mut GeoShape) -> Option<GeoHashRadius> {
    geohash_bounding_box(shape);
    let min_lon = shape.bounds[0];
    let min_lat = shape.bounds[1];
    let max_lon = shape.bounds[2];
    let max_lat = shape.bounds[3];

    let longitude = shape.xy[0];
    let latitude = shape.xy[1];

    // Determine the effective search radius in metres.
    // C: geohash_helper.c:193-210.
    let radius_meters_raw = match &shape.kind {
        GeoShapeKind::Circular { radius } => *radius,
        GeoShapeKind::Rectangle { height, width } => {
            let hw = width / 2.0;
            let hh = height / 2.0;
            (hw * hw + hh * hh).sqrt()
        }
        GeoShapeKind::Polygon { .. } => {
            // Use the maximum distance from the centroid to any bounding-box corner.
            let dist_top_left = geohash_get_distance(longitude, latitude, min_lon, max_lat);
            let dist_top_right = geohash_get_distance(longitude, latitude, max_lon, max_lat);
            let dist_bottom_left = geohash_get_distance(longitude, latitude, min_lon, min_lat);
            let dist_bottom_right = geohash_get_distance(longitude, latitude, max_lon, min_lat);
            dist_top_left
                .max(dist_top_right)
                .max(dist_bottom_left)
                .max(dist_bottom_right)
        }
    };
    // Immutable borrow on shape.kind released here (NLL).
    let radius_meters = radius_meters_raw * shape.conversion;

    let mut steps = geohash_estimate_steps_by_radius(radius_meters, latitude) as i32;

    let (long_range, lat_range) = geohash_get_coord_range();
    let mut hash = geohash_encode(&long_range, &lat_range, longitude, latitude, steps as u8)?;
    let mut neighbors = geohash_neighbors(&hash);
    let mut area = geohash_decode(long_range, lat_range, hash)?;

    // Determine whether the current step is fine-grained enough to cover the
    // search region in all cardinal directions.
    // C: geohash_helper.c:225-245.
    //
    // TODO(port): geohash_decode returns None for zeroed hashes; unwrap_or_default
    // gives all-zero GeoHashArea which may spuriously trigger decrease_step when
    // max_lat > 0.  In practice, neighbours from a valid encode should not be
    // zero-valued, so this is unlikely to matter.
    let decrease_step = {
        let north = geohash_decode(long_range, lat_range, neighbors.north).unwrap_or_default();
        let south = geohash_decode(long_range, lat_range, neighbors.south).unwrap_or_default();
        let east = geohash_decode(long_range, lat_range, neighbors.east).unwrap_or_default();
        let west = geohash_decode(long_range, lat_range, neighbors.west).unwrap_or_default();

        north.latitude.max < max_lat
            || south.latitude.min > min_lat
            || east.longitude.max < max_lon
            || west.longitude.min > min_lon
    };

    if steps > 1 && decrease_step {
        steps -= 1;
        hash = geohash_encode(&long_range, &lat_range, longitude, latitude, steps as u8)?;
        neighbors = geohash_neighbors(&hash);
        area = geohash_decode(long_range, lat_range, hash)?;
    }

    // Prune neighbours that fall entirely outside the bounding box.
    // C: `GZERO(x)` expands to `x.bits = 0; x.step = 0;` — translated as
    // `x = GeoHashBits::default()` (GeoHashBits derives Default with all-zeros).
    // C: geohash_helper.c:248-269.
    if steps >= 2 {
        if area.latitude.min < min_lat {
            neighbors.south = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
        }
        if area.latitude.max > max_lat {
            neighbors.north = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.longitude.min < min_lon {
            neighbors.west = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.longitude.max > max_lon {
            neighbors.east = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
        }
    }

    Some(GeoHashRadius { hash, neighbors, area })
}

/// Left-align a geohash into a canonical 52-bit fixed-precision representation.
///
/// The geohash `bits` are shifted left so that the most significant bit of the
/// encoded value sits at bit position 51, regardless of the original step size.
///
/// C: `geohashAlign52Bits` — geohash_helper.c:276-280.
pub fn geohash_align_52_bits(hash: GeoHashBits) -> GeoHashFix52Bits {
    hash.bits << (52 - hash.step as u32 * 2)
}

/// Compute the great-circle distance between two latitudes along the same
/// meridian, using a simplified haversine (arcsin-of-sin cancellation).
///
/// When the longitude difference is 0, `asin(sqrt(a))` reduces to
/// `asin(sin(|Δlat|/2)) * 2 = |Δlat|` (for latitudes in [-π/2, π/2]), so the
/// full formula simplifies to `R * |Δlat_rad|`.
///
/// C: `geohashGetLatDistance` — geohash_helper.c:287-289.
pub fn geohash_get_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2d) - deg_rad(lat1d)).abs()
}

/// Compute the great-circle distance between two (lon, lat) points in metres
/// using the haversine formula.
///
/// When the longitude difference is negligibly small (≤ `GEO_EPSILON = 1e-15`),
/// the cheaper `geohash_get_lat_distance` is used instead.
///
/// C: `geohashGetDistance` — geohash_helper.c:292-306.
pub fn geohash_get_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = deg_rad(lon1d);
    let lon2r = deg_rad(lon2d);
    let v = ((lon2r - lon1r) / 2.0).sin();

    const GEO_EPSILON: f64 = 1e-15;
    if v.abs() <= GEO_EPSILON {
        return geohash_get_lat_distance(lat1d, lat2d);
    }

    let lat1r = deg_rad(lat1d);
    let lat2r = deg_rad(lat2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// Return the great-circle distance between `(x1, y1)` and `(x2, y2)` if it is
/// within `radius` metres, or `None` if the point is outside the radius.
///
/// PORT NOTE: The C function writes through a `double *distance` out-parameter
/// and returns 0/1.  This port returns `Option<f64>`.
///
/// C: `geohashGetDistanceIfInRadius` — geohash_helper.c:308-312.
pub fn geohash_get_distance_if_in_radius(
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
    radius: f64,
) -> Option<f64> {
    let distance = geohash_get_distance(x1, y1, x2, y2);
    if distance > radius {
        None
    } else {
        Some(distance)
    }
}

/// WGS-84 alias for `geohash_get_distance_if_in_radius`.
///
/// C: `geohashGetDistanceIfInRadiusWGS84` — geohash_helper.c:314-316.
pub fn geohash_get_distance_if_in_radius_wgs84(
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
    radius: f64,
) -> Option<f64> {
    geohash_get_distance_if_in_radius(x1, y1, x2, y2, radius)
}

/// Return the great-circle distance from centre `(x1, y1)` to point `(x2, y2)`
/// if the point lies inside the axis-aligned rectangle of dimensions
/// `width_m × height_m` centred at `(x1, y1)`, or `None` if it is outside.
///
/// The latitude check is performed first because `geohash_get_lat_distance` is
/// cheaper than the full haversine.
///
/// PORT NOTE: The C function writes through a `double *distance` out-parameter
/// and returns 0/1.  This port returns `Option<f64>`.
///
/// C: `geohashGetDistanceIfInRectangle` — geohash_helper.c:326-345.
pub fn geohash_get_distance_if_in_rectangle(
    width_m: f64,
    height_m: f64,
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
) -> Option<f64> {
    let lat_distance = geohash_get_lat_distance(y2, y1);
    if lat_distance > height_m / 2.0 {
        return None;
    }
    let lon_distance = geohash_get_distance(x2, y2, x1, y2);
    if lon_distance > width_m / 2.0 {
        return None;
    }
    Some(geohash_get_distance(x1, y1, x2, y2))
}

/// Test whether `point` lies inside the polygon defined by `vertices` using the
/// ray-casting algorithm (PNPOLY by W. Randolph Franklin).
///
/// If the point is inside, returns `Some(distance)` where distance is the
/// great-circle distance from `(centroid_lon, centroid_lat)` to `point`.
/// Returns `None` if the point is outside the polygon.
///
/// `vertices` is a slice of `[lon, lat]` pairs (index 0 = longitude, 1 = latitude).
/// `point` is `[lon, lat]`.
///
/// PORT NOTE: The C function takes `double *point` (pointer to 2-element array)
/// and `double (*vertices)[2]` (pointer-to-array-of-2).  This port uses
/// `[f64; 2]` value for `point` and `&[[f64; 2]]` for vertices, matching
/// `GeoShapeKind::Polygon { points: Vec<[f64; 2]> }`.
///
/// PORT NOTE: The C function writes through `double *distance` and returns 0/1.
/// This port returns `Option<f64>`.
///
/// C: `geohashGetDistanceIfInPolygon` — geohash_helper.c:353-368.
pub fn geohash_get_distance_if_in_polygon(
    centroid_lon: f64,
    centroid_lat: f64,
    point: [f64; 2],
    vertices: &[[f64; 2]],
) -> Option<f64> {
    let num_vertices = vertices.len();
    let mut inside = false;
    // C: for (i = 0, j = num_vertices - 1; i < num_vertices; j = i++)
    let mut j = num_vertices.wrapping_sub(1);
    for i in 0..num_vertices {
        let vertex_a = vertices[i];
        let vertex_b = vertices[j];
        // Ray-casting edge test: PNPOLY algorithm.
        if ((vertex_a[1] > point[1]) != (vertex_b[1] > point[1]))
            && (point[0]
                < (vertex_b[0] - vertex_a[0]) * (point[1] - vertex_a[1])
                    / (vertex_b[1] - vertex_a[1])
                    + vertex_a[0])
        {
            inside = !inside;
        }
        j = i;
    }
    if inside {
        Some(geohash_get_distance(centroid_lon, centroid_lat, point[0], point[1]))
    } else {
        None
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/geohash_helper.c  (369 lines, 10 functions)
//                  src/geohash_helper.h  (66 lines, type/function declarations)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         1
//   port_notes:    4
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Pure math module; all C out-params converted to Option<T>.
//                  GeoHashRadius is defined here (not in type-vocabulary).
//                  Clone of polygon points in geohash_bounding_box is the only
//                  allocation; profile in Phase B if geo poly queries are hot.
// ──────────────────────────────────────────────────────────────────────────

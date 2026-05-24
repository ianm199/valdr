//! LOLWUT command implementation.
//!
//! Implements the `LOLWUT` command dispatcher and the shared canvas drawing
//! primitives (`LwCanvas`) used by LOLWUT versions 5, 6, and 9.
//!
//! The `LOLWUT` command displays version-specific computer art. It dispatches
//! to a per-version implementation based on the `VERSION` argument or the
//! compiled-in server version string. The canvas primitives provide a generic
//! pixel grid with Bresenham line drawing and rotated-square drawing.
//!
//! # C source reference
//! `src/lolwut.c` (187 lines, 7 functions) and `src/lolwut.h`

use redis_core::command_context::CommandContext;
use redis_types::error::RedisError;

// ── Canvas ────────────────────────────────────────────────────────────────────

/// A simple pixel canvas backed by a flat byte buffer.
///
/// Each pixel is stored as a `u8`. A value of `0` means "no dot"; any
/// non-zero value means "dot displayed". Coordinates originate at the
/// top-left corner: `(0, 0)` is the top-left pixel.
///
/// The canvas is used by LOLWUT version implementations (5, 6, 9) for
/// drawing; it is then converted to a printable string (usually Braille
/// block characters) by the caller.
///
/// # C source reference
/// `lolwut.h` — `struct lwCanvas`
pub struct LwCanvas {
    /// Canvas width in pixels.
    pub width: i32,
    /// Canvas height in pixels.
    pub height: i32,
    /// Row-major flat pixel buffer. Length is always `width * height`.
    pixels: Vec<u8>,
}

impl LwCanvas {
    /// Allocate a new canvas with every pixel set to `bgcolor`.
    ///
    /// # C source reference
    /// `lolwut.c:94–101`, `lwCreateCanvas`
    pub fn new(width: i32, height: i32, bgcolor: i32) -> Self {
        let capacity = (width as usize).saturating_mul(height as usize);
        let pixels = vec![bgcolor as u8; capacity];
        Self { width, height, pixels }
    }

    /// Set the pixel at `(x, y)` to `color`.
    ///
    /// Out-of-bounds writes are silently ignored, matching the C behaviour.
    ///
    /// # C source reference
    /// `lolwut.c:113–116`, `lwDrawPixel`
    pub fn draw_pixel(&mut self, x: i32, y: i32, color: i32) {
        if x < 0 || x >= self.width || y < 0 || y >= self.height {
            return;
        }
        let idx = (x + y * self.width) as usize;
        self.pixels[idx] = color as u8;
    }

    /// Return the stored value of the pixel at `(x, y)`, or `0` for
    /// out-of-bounds coordinates.
    ///
    /// # C source reference
    /// `lolwut.c:119–122`, `lwGetPixel`
    pub fn get_pixel(&self, x: i32, y: i32) -> i32 {
        if x < 0 || x >= self.width || y < 0 || y >= self.height {
            return 0;
        }
        self.pixels[(x + y * self.width) as usize] as i32
    }

    /// Draw a line from `(x1, y1)` to `(x2, y2)` using Bresenham's
    /// line algorithm.
    ///
    /// # C source reference
    /// `lolwut.c:125–145`, `lwDrawLine`
    pub fn draw_line(&mut self, mut x1: i32, mut y1: i32, x2: i32, y2: i32, color: i32) {
        let dx = (x2 - x1).abs();
        let dy = (y2 - y1).abs();
        let sx: i32 = if x1 < x2 { 1 } else { -1 };
        let sy: i32 = if y1 < y2 { 1 } else { -1 };
        let mut err = dx - dy;

        loop {
            self.draw_pixel(x1, y1, color);
            if x1 == x2 && y1 == y2 {
                break;
            }
            let e2 = err * 2;
            if e2 > -dy {
                err -= dy;
                x1 += sx;
            }
            if e2 < dx {
                err += dx;
                y1 += sy;
            }
        }
    }

    /// Draw a rotated square centered at `(x, y)` with the given `size`
    /// and `angle` (in radians).
    ///
    /// The four corners are derived using parametric circle equations
    /// starting at `PI/4 + angle` and stepping by `PI/2`. The `size`
    /// argument is the desired edge length; internally it is divided by
    /// `sqrt(2)` to convert from a circle-radius basis to an edge-length
    /// basis. See the block comment in the C source for the full
    /// derivation.
    ///
    /// # C source reference
    /// `lolwut.c:166–186`, `lwDrawSquare`
    pub fn draw_square(&mut self, x: i32, y: i32, mut size: f32, angle: f32, color: i32) {
        use std::f32::consts::PI;

        // C: size /= 1.4142135623; size = round(size);
        size /= 1.414_213_562_3_f32;
        size = size.round();

        let mut k = PI / 4.0_f32 + angle;
        let mut px = [0_i32; 4];
        let mut py = [0_i32; 4];

        for j in 0..4 {
            px[j] = (k.sin() * size + x as f32).round() as i32;
            py[j] = (k.cos() * size + y as f32).round() as i32;
            k += PI / 2.0_f32;
        }

        for j in 0..4 {
            let next = (j + 1) % 4;
            self.draw_line(px[j], py[j], px[next], py[next], color);
        }
    }
}

// ── LOLWUT commands ───────────────────────────────────────────────────────────

/// LOLWUT — default handler for unstable / unrecognised server versions.
///
/// Emits a plain version banner as a verbatim text reply so clients can
/// display it in a human-readable way.
///
/// # C source reference
/// `lolwut.c:46–52`, `lolwutUnstableCommand`
pub fn lolwut_unstable_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut rendered: Vec<u8> = Vec::new();

    // C: server.extended_redis_compat ? "Redis" : "Valkey"
    // TODO(port): access ctx.server().extended_redis_compat once Server API is stable.
    // For now, default to the Valkey branch.
    rendered.extend_from_slice(b"Valkey ver.");

    // C: server.extended_redis_compat ? REDIS_VERSION : VALKEY_VERSION
    // TODO(port): append the real VALKEY_VERSION constant once defined in redis-core.
    // Placeholder empty slice; the version string is a compile-time constant in C.
    rendered.extend_from_slice(b"");

    rendered.push(b'\n');

    // C: addReplyVerbatim(c, rendered, sdslen(rendered), "txt")
    // TODO(port): CommandContext::reply_verbatim_string — confirm API name in redis-core.
    ctx.reply_verbatim_string(b"txt", &rendered)
}

/// `LOLWUT [VERSION <ver>] [... version-specific arguments ...]`
///
/// Parses the optional `VERSION <ver>` prefix, then dispatches to the
/// matching version-specific implementation:
///
/// | Compiled or requested version | Handler           |
/// |-------------------------------|-------------------|
/// | 4.9, 5.x (not 5.9)           | lolwut5           |
/// | 5.9, 6.x (not 6.9)           | lolwut6           |
/// | 9.x                           | lolwut9           |
/// | everything else               | lolwut_unstable   |
///
/// # Arg-offset note
/// The C implementation temporarily shifts `c->argv` and decrements
/// `c->argc` by 2 before calling the sub-handler, hiding the
/// `VERSION <ver>` tokens. In Rust, `CommandContext` does not expose
/// pointer arithmetic, so this crate passes an `arg_offset` value
/// instead. The sub-handlers must accept and honour it.
///
/// # TODO(architect)
/// `CommandContext` needs a `with_arg_offset(n)` or equivalent API so
/// that `VERSION <ver>` tokens are hidden from the version-specific
/// handlers, matching the C semantics.
///
/// # C source reference
/// `lolwut.c:55–86`, `lolwutCommand`
pub fn lolwut_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): VALKEY_VERSION should come from a constant in redis-core / redis-types.
    // Using a placeholder; the real version determines the default dispatch branch.
    let compiled_version: &[u8] = b"0.0.0";

    // Parse optional VERSION argument.
    // C: if (c->argc >= 3 && !strcasecmp(objectGetVal(c->argv[1]), "version"))
    let (version_bytes, arg_offset): (Vec<u8>, usize) = if ctx.argc() >= 3 {
        let arg1 = ctx.arg(1)?;
        if arg1.eq_ignore_ascii_case(b"version") {
            // C: getLongFromObjectOrReply(c, c->argv[2], &ver, NULL) != C_OK
            let ver: i64 = ctx.arg_long(2)?;

            // C: snprintf(verstr, sizeof(verstr), "%u.0.0", (unsigned int)ver)
            // Build "N.0.0" as bytes, avoiding String/&str for Redis data.
            // PORT NOTE: version string is control-flow only, never sent as Redis data.
            let ver_u32 = ver as u32;
            let mut ver_buf: Vec<u8> = Vec::with_capacity(10);
            write_decimal_bytes(ver_u32, &mut ver_buf);
            ver_buf.extend_from_slice(b".0.0");

            (ver_buf, 2)
        } else {
            (compiled_version.to_vec(), 0)
        }
    } else {
        (compiled_version.to_vec(), 0)
    };

    let v = version_bytes.as_slice();

    // Dispatch on version string prefix bytes.
    // C: lolwut.c:72–79, version-based if-else chain.
    //
    // The arg_offset is passed to sub-handlers to replicate the C argv shift.
    // TODO(architect): sub-handlers in lolwut_lolwut5/6/9 need to accept arg_offset.
    if matches!(
        (v.first(), v.get(1), v.get(2)),
        (Some(b'5'), Some(b'.'), Some(c)) if *c != b'9'
    ) || matches!(
        (v.first(), v.get(1), v.get(2)),
        (Some(b'4'), Some(b'.'), Some(b'9'))
    ) {
        // C: lolwut5Command(c)  [after argv shift]
        // TODO(port): lolwut5_command lives in crate::lolwut_lolwut5; cross-module call.
        crate::lolwut_lolwut5::lolwut5_command_with_offset(ctx, arg_offset)
    } else if matches!(
        (v.first(), v.get(1), v.get(2)),
        (Some(b'6'), Some(b'.'), Some(c)) if *c != b'9'
    ) || matches!(
        (v.first(), v.get(1), v.get(2)),
        (Some(b'5'), Some(b'.'), Some(b'9'))
    ) {
        // C: lolwut6Command(c)  [after argv shift]
        // TODO(port): lolwut6_command lives in crate::lolwut_lolwut6; cross-module call.
        crate::lolwut_lolwut6::lolwut6_command_with_offset(ctx, arg_offset)
    } else if v.first() == Some(&b'9') {
        // C: lolwut9Command(c)  [after argv shift]
        // TODO(port): lolwut9_command lives in crate::lolwut_lolwut9; cross-module call.
        crate::lolwut_lolwut9::lolwut9_command_with_offset(ctx, arg_offset)
    } else {
        lolwut_unstable_command(ctx)
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Write the decimal digits of `n` into `buf` (no leading zeros for n > 0;
/// writes a single `b'0'` for n == 0).
///
/// This avoids going through `format!` / `String` for a value that only
/// feeds into a byte-slice dispatch comparison.
fn write_decimal_bytes(n: u32, buf: &mut Vec<u8>) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let start = buf.len();
    let mut remaining = n;
    while remaining > 0 {
        buf.push(b'0' + (remaining % 10) as u8);
        remaining /= 10;
    }
    buf[start..].reverse();
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/lolwut.c  (187 lines, 7 functions) + src/lolwut.h
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         7
//   port_notes:    2
//   unsafe_blocks: 0
//   notes: >
//     Canvas primitives (LwCanvas, draw_pixel, draw_line, draw_square) are
//     straightforward numeric translations.  The main complexity is in
//     lolwut_command: the C code mutates c->argv/argc to shift the argument
//     window before delegating to version-specific handlers; Rust requires an
//     architectural API change (TODO(architect)) to replicate this.
//     server.extended_redis_compat and VALKEY_VERSION/REDIS_VERSION constants
//     need injection via CommandContext once the server API is finalised.
//     addReplyVerbatim mapping (ctx.reply_verbatim_string) needs confirmation.
// ──────────────────────────────────────────────────────────────────────────────

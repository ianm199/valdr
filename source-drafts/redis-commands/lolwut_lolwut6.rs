//! LOLWUT version 6 — 8-bit city skyline.
//!
//! Implements the `LOLWUT 6` sub-command, which renders a parallax-style
//! city skyline using the four standard terminal gray levels (ANSI color
//! escapes). The skyline is drawn on an [`LwCanvas`] and returned as a
//! verbatim text RESP reply.
//!
//! Dedicated to the 8-bit game developers of past and present.
//! Original 8-bit image from Plaguemon by hikikomori.
//! Thanks to the Shhh computer art collective for tuning the artistic effect.
//!
//! # C source reference
//! `src/lolwut6.c` (192 lines, 4 functions)

// TODO(port): add `rand` crate dependency to crates/redis-commands/Cargo.toml.
// The C code uses the standard libc rand() which is seeded globally by the
// server. For LOLWUT the output is purely aesthetic, so exact seed parity
// is not required for wire-diff. rand::thread_rng() is a correct substitute.
use rand::Rng;

use crate::lolwut_lolwut::LwCanvas;
use redis_core::command_context::CommandContext;
use redis_types::error::RedisError;

// ── Skyscraper descriptor ─────────────────────────────────────────────────────

/// Parameters describing a single skyscraper to be drawn on the canvas.
///
/// # C source reference
/// `lolwut6.c:74–80`, `struct skyscraper`
struct Skyscraper {
    /// X offset of the building's left edge.
    xoff: i32,
    /// Building width in pixels.
    width: i32,
    /// Building height in pixels.
    height: i32,
    /// When `true`, draw illuminated windows in the building interior.
    windows: bool,
    /// Base fill color (0 = black, 1 = dark gray, 2 = light gray, 3 = white).
    color: i32,
}

// ── Canvas rendering ──────────────────────────────────────────────────────────

/// Render a canvas to an ANSI-colored byte vector using the four standard
/// terminal gray levels.
///
/// Both the foreground and background ANSI attributes are set for every pixel
/// so that the output looks consistent across different terminal emulators.
/// Pixels map to colors as follows:
/// - 0 → black (`0;30;40m`)
/// - 1 → dark gray (`0;90;100m`)
/// - 2 → light gray (`0;37;47m`)
/// - 3 → white (`0;97;107m`)
///
/// Rows are separated by `\n` except for the final row.
///
/// # C source reference
/// `lolwut6.c:47–69`, `renderCanvas` (static)
fn render_canvas(canvas: &LwCanvas) -> Vec<u8> {
    let mut text: Vec<u8> = Vec::new();
    for y in 0..canvas.height {
        for x in 0..canvas.width {
            let color = canvas.get_pixel(x, y);
            // C: switch (color) { case 0: ce = "0;30;40m"; ... }
            let ce: &[u8] = match color {
                0 => b"0;30;40m",   // Black
                1 => b"0;90;100m",  // Dark gray
                2 => b"0;37;47m",   // Light gray
                3 => b"0;97;107m",  // White
                _ => b"0;30;40m",   // Safety fallback — matches C default case
            };
            // C: sdscatprintf(text, "\033[%s \033[0m", ce)
            // ce already contains the trailing 'm', so \x1b[ + ce = full SGR sequence.
            text.extend_from_slice(b"\x1b[");
            text.extend_from_slice(ce);
            text.push(b' ');
            text.extend_from_slice(b"\x1b[0m");
        }
        if y != canvas.height - 1 {
            // C: if (y != canvas->height - 1) text = sdscatlen(text, "\n", 1)
            text.push(b'\n');
        }
    }
    text
}

// ── Skyscraper generation ─────────────────────────────────────────────────────

/// Draw a single skyscraper onto the canvas according to the parameters in
/// `si`, using `rng` as the random source for window colors.
///
/// The roof row is narrower: the leftmost two and rightmost two pixels of
/// the top row are skipped. Windows are rendered as 2-wide × 1-tall cells
/// in a grid across the building interior. Each window cell gets a random
/// gray (1 or 2) that differs from the building's base color; the right
/// pixel of each 2-wide window cell copies the already-written left pixel
/// to ensure the pair is uniformly colored.
///
/// # C source reference
/// `lolwut6.c:82–114`, `generateSkyscraper`
fn generate_skyscraper<R: Rng>(canvas: &mut LwCanvas, si: &Skyscraper, rng: &mut R) {
    let starty = canvas.height - 1;
    let endy = starty - si.height + 1;
    // C: for (int y = starty; y >= endy; y--)
    let mut y = starty;
    while y >= endy {
        let mut x = si.xoff;
        while x < si.xoff + si.width {
            // C: if (y == endy && (x <= si->xoff + 1 || x >= si->xoff + si->width - 2)) continue;
            if y == endy && (x <= si.xoff + 1 || x >= si.xoff + si.width - 2) {
                x += 1;
                continue;
            }
            let mut color = si.color;
            // C: if (si->windows && x > ... && y > ...) { ... window logic ... }
            if si.windows
                && x > si.xoff + 1
                && x < si.xoff + si.width - 2
                && y > endy + 1
                && y < starty - 1
            {
                // C: int relx = x - (si->xoff + 1); int rely = y - (endy + 1);
                let relx = x - (si.xoff + 1);
                let rely = y - (endy + 1);
                // C: if (relx / 2 % 2 && rely % 2) — window-grid cell check
                if (relx / 2 % 2 != 0) && (rely % 2 != 0) {
                    // C: do { color = 1 + rand() % 2; } while (color == si->color)
                    // PERF(port): unbounded loop — matches C but terminates in ≤ 2 iterations
                    // since there are only two gray values and one is excluded each time.
                    loop {
                        color = 1 + (rng.gen::<u32>() % 2) as i32;
                        if color != si.color {
                            break;
                        }
                    }
                    // C: if (relx % 2) color = lwGetPixel(canvas, x-1, y)
                    // Right pixel of a 2-wide window cell inherits the left pixel's color.
                    if relx % 2 != 0 {
                        color = canvas.get_pixel(x - 1, y);
                    }
                }
            }
            canvas.draw_pixel(x, y, color);
            x += 1;
        }
        y -= 1;
    }
}

// ── Skyline generation ────────────────────────────────────────────────────────

/// Generate a parallax-style city skyline on the canvas.
///
/// Three passes, back-to-front:
/// 1. Color 2 (light gray) — sparse, tall background buildings, no windows.
/// 2. Color 1 (dark gray) — denser mid-ground buildings, no windows.
/// 3. Color 0 (black) — closely-spaced foreground buildings with windows.
///
/// All building positions, widths, and heights are randomized.
///
/// # C source reference
/// `lolwut6.c:117–154`, `generateSkyline`
fn generate_skyline(canvas: &mut LwCanvas) {
    let mut rng = rand::thread_rng();

    // C: for (int color = 2; color >= 1; color--) — background + mid-ground passes
    for color in (1..=2_i32).rev() {
        let mut offset: i32 = -10;
        while offset < canvas.width {
            // C: offset += rand() % 8
            offset += (rng.gen::<u32>() % 8) as i32;
            let xoff = offset;
            let width = 10 + (rng.gen::<u32>() % 9) as i32;
            // C: height = canvas->height / 2 + rand() % canvas->height / 2 (or / 3)
            // C operator precedence: (canvas->height / 2) + ((rand() % canvas->height) / 2)
            let height = if color == 2 {
                canvas.height / 2
                    + (rng.gen::<u32>() % canvas.height as u32) as i32 / 2
            } else {
                canvas.height / 2
                    + (rng.gen::<u32>() % canvas.height as u32) as i32 / 3
            };
            let si = Skyscraper { xoff, width, height, windows: false, color };
            generate_skyscraper(canvas, &si, &mut rng);
            // C: offset advancement differs by color layer
            if color == 2 {
                offset += width / 2;
            } else {
                offset += width + 1;
            }
        }
    }

    // C: foreground pass — black buildings with windows
    let mut offset: i32 = -10;
    while offset < canvas.width {
        offset += (rng.gen::<u32>() % 8) as i32;
        let xoff = offset;
        let mut width = 5 + (rng.gen::<u32>() % 14) as i32;
        // C: if (si.width % 4) si.width += (si.width % 3)
        // PORT NOTE: rounds width up to a multiple-of-4-friendly size for even window grid.
        if width % 4 != 0 {
            width += width % 3;
        }
        // C: si.height = canvas->height / 3 + rand() % canvas->height / 2
        let height =
            canvas.height / 3 + (rng.gen::<u32>() % canvas.height as u32) as i32 / 2;
        let si = Skyscraper { xoff, width, height, windows: true, color: 0 };
        generate_skyscraper(canvas, &si, &mut rng);
        offset += width + 5;
    }
}

// ── Command entry points ──────────────────────────────────────────────────────

/// `LOLWUT [columns] [rows]`
///
/// Renders an 8-bit city skyline using ANSI escape-code shading. Defaults
/// to 80 columns × 20 rows. Both dimensions are clamped to \[1, 1000\].
/// The reply is a verbatim text RESP frame.
///
/// # C source reference
/// `lolwut6.c:163–191`, `lolwut6Command`
pub fn lolwut6_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lolwut6_command_with_offset(ctx, 0)
}

/// Version of [`lolwut6_command`] that skips `arg_offset` leading arguments.
///
/// Called from `crate::lolwut_lolwut::lolwut_command` when dispatched via
/// `LOLWUT VERSION 6`, where the `VERSION <ver>` tokens have been logically
/// removed by setting `arg_offset = 2`.
///
/// # TODO(architect)
/// `CommandContext` should expose a `with_arg_offset(n)` or slice-view API
/// so argument skipping is transparent to sub-handlers. The threaded
/// `arg_offset` parameter is a Phase A workaround for the missing API.
///
/// # C source reference
/// `lolwut.c:68–69`, `c->argv += 2; c->argc -= 2;` (the argv shift that
/// hides the VERSION tokens before delegating to the version handler).
pub fn lolwut6_command_with_offset(
    ctx: &mut CommandContext,
    arg_offset: usize,
) -> Result<(), RedisError> {
    let mut cols: i64 = 80;
    let mut rows: i64 = 20;

    // C: if (c->argc > 1 && getLongFromObjectOrReply(c, c->argv[1], &cols, NULL) != C_OK) return;
    if ctx.argc() > 1 + arg_offset {
        cols = ctx.arg_long(1 + arg_offset)?;
    }
    // C: if (c->argc > 2 && getLongFromObjectOrReply(c, c->argv[2], &rows, NULL) != C_OK) return;
    if ctx.argc() > 2 + arg_offset {
        rows = ctx.arg_long(2 + arg_offset)?;
    }

    // C: clamp cols and rows to [1, 1000]
    if cols < 1 { cols = 1; }
    if cols > 1000 { cols = 1000; }
    if rows < 1 { rows = 1; }
    if rows > 1000 { rows = 1000; }

    // C: lwCanvas *canvas = lwCreateCanvas(cols, rows, 3)
    let mut canvas = LwCanvas::new(cols as i32, rows as i32, 3);
    generate_skyline(&mut canvas);
    let mut rendered = render_canvas(&canvas);

    // C: sdscatprintf(rendered, "\nDedicated to ...\n... %s ver. ", compat ? "Redis" : "Valkey")
    rendered.extend_from_slice(
        b"\nDedicated to the 8 bit game developers of past and present.\n\
          Original 8 bit image from Plaguemon by hikikomori. ",
    );
    // TODO(port): read ctx.server().extended_redis_compat to select b"Redis" vs b"Valkey".
    // Defaulting to the Valkey branch until CommandContext server-access API is stable.
    rendered.extend_from_slice(b"Valkey");
    rendered.extend_from_slice(b" ver. ");

    // C: rendered = sdscat(rendered, server.extended_redis_compat ? REDIS_VERSION : VALKEY_VERSION)
    // TODO(port): append real VALKEY_VERSION / REDIS_VERSION constant once available
    // in redis-core::version. The C constant is a compile-time string literal.
    rendered.push(b'\n');

    // C: addReplyVerbatim(c, rendered, sdslen(rendered), "txt")
    ctx.reply_verbatim_string(b"txt", &rendered)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/lolwut6.c  (192 lines, 4 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         4
//   port_notes:    1
//   unsafe_blocks: 0
//   notes: >
//     Straightforward numeric translation. The main unresolved items are:
//     (1) `rand` crate dependency must be added to Cargo.toml — all rand()
//     calls are faithfully translated using rand::thread_rng() which shares
//     state within generate_skyline (matching C's global rand state within
//     one skyline generation call);
//     (2) server.extended_redis_compat and VALKEY_VERSION/REDIS_VERSION
//     constants need injection via CommandContext once the server API is
//     finalised (same pattern as lolwut_lolwut.rs);
//     (3) lolwut6_command_with_offset arg_offset workaround needs an
//     architectural fix per TODO(architect).
// ──────────────────────────────────────────────────────────────────────────────

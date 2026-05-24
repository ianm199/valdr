//! LOLWUT version 5 — Schotter (Georg Nees, 1968).
//!
//! Renders a Schotter-style square grid with progressive random rotation and
//! translation as you move down the rows. The output uses Unicode Braille
//! characters (U+2800–U+28FF) to pack a 2×4 pixel block into a single terminal
//! cell.
//!
//! Called by the main `LOLWUT` dispatcher in `lolwut_lolwut` when the server
//! or requested version matches the 4.9 / 5.x family.
//!
//! # Command syntax
//! ```text
//! LOLWUT [terminal_cols] [squares_per_row] [squares_per_col]
//! ```
//! Defaults: 66 cols, 8 squares/row, 12 squares/col.
//!
//! # C source reference
//! `src/lolwut5.c` (171 lines, 4 functions)

use crate::lolwut_lolwut::LwCanvas;
use redis_core::command_context::CommandContext;
use redis_types::error::RedisError;

// ── Braille pixel-group encoding ─────────────────────────────────────────────

/// Translate a group of 8 pixels (a 2×4 block) to the 3-byte UTF-8 encoding
/// of the corresponding Braille character.
///
/// The `byte` argument encodes which dots are lit, with the following bit
/// positions (bit 0 = least significant):
///
/// ```text
///   col 0  col 1
///   bit 0  bit 3   ← row 0
///   bit 1  bit 4   ← row 1
///   bit 2  bit 5   ← row 2
///   bit 6  bit 7   ← row 3
/// ```
///
/// All Braille codepoints fall in U+2800–U+28FF (the U+0800–U+FFFF range),
/// so the encoding uses three bytes: `1110xxxx 10xxxxxx 10xxxxxx`.
///
/// # C source reference
/// `lolwut5.c:54–62`, `lwTranslatePixelsGroup`
pub fn lw_translate_pixels_group(byte: i32, output: &mut [u8; 3]) {
    let code: i32 = 0x2800 + byte;
    // C: 1110xxxx 10xxxxxx 10xxxxxx  (U0800-UFFFF range)
    output[0] = (0xE0 | (code >> 12)) as u8;
    output[1] = (0x80 | ((code >> 6) & 0x3F)) as u8;
    output[2] = (0x80 | (code & 0x3F)) as u8;
}

// ── Canvas rendering ─────────────────────────────────────────────────────────

/// Render a canvas to a flat byte buffer of UTF-8 Braille characters.
///
/// Each 2×4 pixel block maps to one Braille terminal cell. Rows of Braille
/// characters are separated by `\n`. The C source checks `y != canvas->height - 1`
/// where `y` strides by 4; this logic is preserved verbatim for wire-diff parity.
///
/// # PORT NOTE
/// The C newline guard (`y != canvas->height - 1`) compares the stride-4 pixel
/// row index against the last pixel index directly. For the typical case where
/// `height` is a multiple of 4, this means the final Braille row does receive a
/// trailing newline (because `height - 1` is never exactly equal to the
/// stride-4 `y`). The condition is reproduced faithfully rather than simplified.
///
/// # C source reference
/// `lolwut5.c:109–131`, `renderCanvas`
fn render_canvas(canvas: &LwCanvas) -> Vec<u8> {
    let mut text: Vec<u8> = Vec::new();
    let mut y: i32 = 0;
    while y < canvas.height {
        let mut x: i32 = 0;
        while x < canvas.width {
            // Pack the 8 pixels of the 2×4 block into a single byte,
            // following the bit-layout defined in lw_translate_pixels_group.
            let mut byte: i32 = 0;
            if canvas.get_pixel(x,     y    ) != 0 { byte |= 1 << 0; }
            if canvas.get_pixel(x,     y + 1) != 0 { byte |= 1 << 1; }
            if canvas.get_pixel(x,     y + 2) != 0 { byte |= 1 << 2; }
            if canvas.get_pixel(x + 1, y    ) != 0 { byte |= 1 << 3; }
            if canvas.get_pixel(x + 1, y + 1) != 0 { byte |= 1 << 4; }
            if canvas.get_pixel(x + 1, y + 2) != 0 { byte |= 1 << 5; }
            if canvas.get_pixel(x,     y + 3) != 0 { byte |= 1 << 6; }
            if canvas.get_pixel(x + 1, y + 3) != 0 { byte |= 1 << 7; }

            let mut unicode = [0u8; 3];
            lw_translate_pixels_group(byte, &mut unicode);
            text.extend_from_slice(&unicode);

            x += 2;
        }
        // C: if (y != canvas->height - 1) text = sdscatlen(text, "\n", 1);
        if y != canvas.height - 1 {
            text.push(b'\n');
        }
        y += 4;
    }
    text
}

// ── Schotter art generation ───────────────────────────────────────────────────

/// Generate a Schotter-style canvas.
///
/// Creates a `squares_per_row × squares_per_col` grid. Squares in higher rows
/// (larger `y`) are progressively rotated and displaced by amounts proportional
/// to `y / squares_per_col`, recreating Georg Nees's 1968 plotter work.
///
/// `console_cols` is the terminal column count; the canvas width is
/// `console_cols * 2` to account for the 2-pixel-per-Braille-cell ratio.
///
/// # PERF(port)
/// C uses global `rand()` / `RAND_MAX`. The stubs `lw_rand_f32` and
/// `lw_rand_bool` return constant values as placeholders.
/// TODO(port): replace stubs with real PRNG once `rand` crate is wired in.
/// TODO(architect): add `rand = "0.8"` (or later) to `redis-commands/Cargo.toml`.
///
/// # C source reference
/// `lolwut5.c:71–102`, `lwDrawSchotter`
pub fn lw_draw_schotter(
    console_cols: i64,
    squares_per_row: i64,
    squares_per_col: i64,
) -> LwCanvas {
    let canvas_width: i32 = (console_cols * 2) as i32;
    let padding: i32 = if canvas_width > 4 { 2 } else { 0 };
    let square_side: f32 = (canvas_width - padding * 2) as f32 / squares_per_row as f32;
    let canvas_height: i32 =
        (square_side * squares_per_col as f32 + (padding * 2) as f32) as i32;
    let mut canvas = LwCanvas::new(canvas_width, canvas_height, 0);

    for y in 0..squares_per_col {
        for x in 0..squares_per_row {
            // C: sx = x * square_side + square_side/2 + padding
            let mut sx: i32 =
                (x as f32 * square_side + square_side / 2.0_f32 + padding as f32) as i32;
            let mut sy: i32 =
                (y as f32 * square_side + square_side / 2.0_f32 + padding as f32) as i32;
            let mut angle: f32 = 0.0_f32;

            if y > 1 {
                // C: r1 = (float)rand() / (float)RAND_MAX / squares_per_col * y;
                // TODO(port): replace lw_rand_f32/lw_rand_bool with real rand crate calls.
                let mut r1 = lw_rand_f32() / squares_per_col as f32 * y as f32;
                let mut r2 = lw_rand_f32() / squares_per_col as f32 * y as f32;
                let mut r3 = lw_rand_f32() / squares_per_col as f32 * y as f32;

                // C: if (rand() % 2) r1 = -r1;
                if lw_rand_bool() { r1 = -r1; }
                if lw_rand_bool() { r2 = -r2; }
                if lw_rand_bool() { r3 = -r3; }

                angle = r1;
                sx += (r2 * square_side / 3.0_f32) as i32;
                sy += (r3 * square_side / 3.0_f32) as i32;
            }

            canvas.draw_square(sx, sy, square_side, angle, 1);
        }
    }

    canvas
}

// ── PRNG stubs ────────────────────────────────────────────────────────────────

/// Return a pseudo-random f32 in [0.0, 1.0].
///
/// Stub placeholder for `(float)rand() / (float)RAND_MAX` in the C source.
/// TODO(port): replace with `rand::random::<f32>()` once `rand` is in Cargo.toml.
fn lw_rand_f32() -> f32 {
    0.5_f32
}

/// Return a pseudo-random bool.
///
/// Stub placeholder for `rand() % 2` in the C source.
/// TODO(port): replace with `rand::random::<bool>()` once `rand` is in Cargo.toml.
fn lw_rand_bool() -> bool {
    false
}

// ── Command entry points ──────────────────────────────────────────────────────

/// `LOLWUT [cols] [squares_per_row] [squares_per_col]`
///
/// Entry point when no `VERSION` override is in use. Delegates immediately to
/// `lolwut5_command_with_offset` with `arg_offset = 0`.
///
/// # C source reference
/// `lolwut5.c:140–170`, `lolwut5Command`
pub fn lolwut5_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lolwut5_command_with_offset(ctx, 0)
}

/// `LOLWUT [VERSION 5] [cols] [squares_per_row] [squares_per_col]`
///
/// Parses optional numeric arguments then draws Schotter art and replies with a
/// verbatim text RESP frame.
///
/// The `arg_offset` parameter replicates the C pattern where `lolwutCommand`
/// temporarily shifts `c->argv` by 2 when a `VERSION <ver>` prefix is present,
/// hiding those tokens from the version-specific handler. In Rust, the caller
/// passes `arg_offset = 2` instead of mutating a pointer.
///
/// # Argument layout after `arg_offset`
/// | Slot | Rust access                        | Meaning         | Default |
/// |------|------------------------------------|-----------------|---------|
/// | 1    | `ctx.arg_long(1 + arg_offset)?`    | cols            | 66      |
/// | 2    | `ctx.arg_long(2 + arg_offset)?`    | squares_per_row | 8       |
/// | 3    | `ctx.arg_long(3 + arg_offset)?`    | squares_per_col | 12      |
///
/// # C source reference
/// `lolwut5.c:140–170`, `lolwut5Command`
pub fn lolwut5_command_with_offset(
    ctx: &mut CommandContext,
    arg_offset: usize,
) -> Result<(), RedisError> {
    let mut cols: i64 = 66;
    let mut squares_per_row: i64 = 8;
    let mut squares_per_col: i64 = 12;

    // C: if (c->argc > 1 && getLongFromObjectOrReply(c, c->argv[1], &cols, NULL) != C_OK) return;
    if ctx.argc() > 1 + arg_offset {
        cols = ctx.arg_long(1 + arg_offset)?;
    }
    if ctx.argc() > 2 + arg_offset {
        squares_per_row = ctx.arg_long(2 + arg_offset)?;
    }
    if ctx.argc() > 3 + arg_offset {
        squares_per_col = ctx.arg_long(3 + arg_offset)?;
    }

    // Clamp all three parameters to safe bounds.
    cols = cols.max(1).min(1000);
    squares_per_row = squares_per_row.max(1).min(200);
    squares_per_col = squares_per_col.max(1).min(200);

    let canvas = lw_draw_schotter(cols, squares_per_row, squares_per_col);
    let mut rendered = render_canvas(&canvas);

    // C: sdscatprintf(rendered,
    //        "\nGeorg Nees - schotter, plotter on paper, 1968. %s ver. ",
    //        server.extended_redis_compat ? "Redis" : "Valkey");
    // TODO(port): access ctx.server().extended_redis_compat once Server API is stable.
    // Defaulting to the Valkey branch for now.
    rendered.extend_from_slice(b"\nGeorg Nees - schotter, plotter on paper, 1968. Valkey ver. ");

    // C: sdscat(rendered, server.extended_redis_compat ? REDIS_VERSION : VALKEY_VERSION);
    // TODO(port): append the real VALKEY_VERSION constant once defined in redis-core.
    // Placeholder empty slice; version is a compile-time constant in C.
    rendered.extend_from_slice(b"");

    rendered.push(b'\n');

    // C: addReplyVerbatim(c, rendered, sdslen(rendered), "txt")
    // TODO(port): confirm that CommandContext::reply_verbatim_string matches
    // addReplyVerbatim semantics (format tag "txt", bulk verbatim reply in RESP3).
    ctx.reply_verbatim_string(b"txt", &rendered)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/lolwut5.c  (171 lines, 4 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         7
//   port_notes:    1
//   unsafe_blocks: 0
//   notes: >
//     lw_translate_pixels_group and render_canvas are straightforward numeric
//     translations with no ambiguity. lw_draw_schotter uses C rand()/RAND_MAX
//     which is stubbed (lw_rand_f32/lw_rand_bool) pending the `rand` crate
//     being added to Cargo.toml (TODO(architect)). The arg_offset mechanism
//     faithfully replicates the C argv-pointer-shift used by lolwutCommand.
//     server.extended_redis_compat / VALKEY_VERSION access is deferred the
//     same way as in lolwut_lolwut.rs. Canvas types are imported from
//     crate::lolwut_lolwut to avoid redefining vocabulary-registered types.
// ──────────────────────────────────────────────────────────────────────────────

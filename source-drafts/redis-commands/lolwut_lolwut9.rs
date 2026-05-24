//! LOLWUT version 9 — Julia set ASCII art.
//!
//! Renders an ASCII representation of a Julia set onto a character grid.
//! Each cell is mapped to a character from a density-ordered palette based
//! on how many iterations the complex point takes to escape.
//!
//! Usage: `LOLWUT [columns rows] [real imaginary]`
//!
//! Defaults: 80 columns, 40 rows; random Julia constant if not supplied.
//!
//! # C source reference
//! `src/lolwut9.c` (87 lines, 2 functions)

use redis_core::command_context::CommandContext;
use redis_types::error::RedisError;

/// Density-ordered ASCII characters used to visualise Julia set escape depth.
///
/// Index 0 is the "inside the set" character; higher indices represent
/// faster escape (brighter / denser appearance).
///
/// # C source reference
/// `lolwut9.c:11`, `ascii_array`
const ASCII_ARRAY: &[u8] = b" .:-=+*%#&@";

// ── Core computation ──────────────────────────────────────────────────────────

/// Compute the number of iterations before the point `(x, y)` escapes the
/// Julia set defined by the constant `(julia_r, julia_i)`.
///
/// Iterates `f(z) = z² + c` up to `max_iter` times. Returns the zero-based
/// iteration index when `|z|² > 4`, or `max_iter - 1` if the point never
/// escapes within the budget.
///
/// # C source reference
/// `lolwut9.c:18–27`, `juliaSetIteration`
fn julia_set_iteration(
    mut x: f32,
    mut y: f32,
    julia_r: f32,
    julia_i: f32,
    max_iter: usize,
) -> usize {
    for i in 0..max_iter {
        let x_new = x * x - y * y + julia_r;
        let y_new = 2.0_f32 * x * y + julia_i;
        x = x_new;
        y = y_new;
        if x * x + y * y > 4.0_f32 {
            return i;
        }
    }
    max_iter.saturating_sub(1)
}

// ── Command entry points ──────────────────────────────────────────────────────

/// `LOLWUT [columns rows] [real imaginary]`
///
/// Direct entry point (argc and argv start at index 0 = command name).
/// Delegates immediately to [`lolwut9_command_with_offset`] with no offset.
///
/// # C source reference
/// `lolwut9.c:36–86`, `lolwut9Command`
pub fn lolwut9_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    lolwut9_command_with_offset(ctx, 0)
}

/// Inner implementation called both directly and via the VERSION dispatcher in
/// `lolwut_command` (see `lolwut_lolwut.rs`).
///
/// `arg_offset` is the number of leading argument positions already consumed
/// by the dispatcher (0 for a direct call, 2 when `VERSION <n>` was stripped).
/// All positional reads are shifted by `arg_offset` to replicate the C argv
/// pointer adjustment.
///
/// # Argument layout after offset adjustment
/// | position | content          |
/// |----------|------------------|
/// | 1        | columns          |
/// | 2        | rows             |
/// | 3        | julia real part  |
/// | 4        | julia imag part  |
///
/// # C source reference
/// `lolwut9.c:36–86`, `lolwut9Command`
pub fn lolwut9_command_with_offset(
    ctx: &mut CommandContext,
    arg_offset: usize,
) -> Result<(), RedisError> {
    // C: long cols = 80; long rows = 40;
    let mut cols: i64 = 80;
    let mut rows: i64 = 40;

    // C: if (c->argc == 2 || c->argc == 4 || c->argc > 5) { addReplyError(...) }
    // Effective argc accounts for the offset already consumed by the dispatcher.
    let effective_argc = ctx.argc().saturating_sub(arg_offset);
    if effective_argc == 2 || effective_argc == 4 || effective_argc > 5 {
        return Err(RedisError::runtime(
            b"Syntax error. Use: LOLWUT [columns rows] [real imaginary]",
        ));
    }

    // C: if (c->argc > 1 && getLongFromObjectOrReply(c, c->argv[1], &cols, NULL) != C_OK) return;
    if effective_argc > 1 {
        cols = ctx.arg_long(1 + arg_offset)?;
    }
    // C: if (c->argc > 2 && getLongFromObjectOrReply(c, c->argv[2], &rows, NULL) != C_OK) return;
    if effective_argc > 2 {
        rows = ctx.arg_long(2 + arg_offset)?;
    }

    // Clamp to safe rendering bounds.
    // C: if (cols < 1) cols = 1; if (cols > 160) cols = 160;
    cols = cols.max(1).min(160);
    // C: if (rows < 1) rows = 1; if (rows > 80) rows = 80;
    rows = rows.max(1).min(80);

    let cols = cols as usize;
    let rows = rows as usize;

    // Obtain the Julia set constants.
    let (julia_r, julia_i): (f32, f32) = if effective_argc == 5 {
        // C: getLongDoubleFromObjectOrReply(c, c->argv[3], &input_r, NULL)
        // TODO(port): ctx.arg_f64 / arg_long_double — confirm the exact method name in
        //             CommandContext once the redis-core API is stabilised.
        let r: f64 = ctx.arg_f64(3 + arg_offset)?;
        let i: f64 = ctx.arg_f64(4 + arg_offset)?;
        (r as f32, i as f32)
    } else {
        // C: julia_r = rand() / (float)RAND_MAX * 2 - 1;
        //    julia_i = rand() / (float)RAND_MAX * 2 - 1;
        // TODO(port): Rust stdlib has no built-in rand(); the `rand` crate provides
        //             thread_rng().gen_range(-1.0_f32..1.0_f32). Until the dependency
        //             is approved, produce a fixed but visually interesting constant.
        //             TODO(architect): add `rand` crate dependency to redis-commands
        //             for random Julia constants.
        (
            ctx.server().pseudo_random_f32_minus1_to_1(),
            ctx.server().pseudo_random_f32_minus1_to_1(),
        )
    };

    // Build the output grid.
    // C: sds output_array = sdsnewlen(NULL, sizeof(char) * (cols + 1) * rows);
    // Each row is `cols` pixels plus one newline.
    let row_len = cols + 1;
    let mut output: Vec<u8> = vec![0u8; row_len * rows];

    let max_iter = ASCII_ARRAY.len();

    for i in 0..rows {
        for j in 0..cols {
            // C: float x = -2.0f + 4.0f * (j + 0.5f) / cols;
            //    float y =  2.0f - 4.0f * (i + 0.5f) / rows;
            let x = -2.0_f32 + 4.0_f32 * (j as f32 + 0.5_f32) / cols as f32;
            let y = 2.0_f32 - 4.0_f32 * (i as f32 + 0.5_f32) / rows as f32;

            // C: int iterations = juliaSetIteration(x, y, julia_r, julia_i, sizeof(ascii_array) - 1);
            let iterations = julia_set_iteration(x, y, julia_r, julia_i, max_iter - 1);

            // C: output_array[i * (cols + 1) + j] = ascii_array[iterations % sizeof(ascii_array)];
            output[i * row_len + j] = ASCII_ARRAY[iterations % max_iter];
        }
        // C: output_array[i * (cols + 1) + cols] = '\n';
        output[i * row_len + cols] = b'\n';
    }

    // Append the textual footer.
    // C: sdscatprintf(output_array, "Ascii representation of Julia set with constant %.2f + %.2fi\n", julia_r, julia_i)
    // PORT NOTE: formatting floats to 2 decimal places via format! produces a &str, but
    //            it is only used as the body of the byte reply — not stored as Redis data.
    let footer_line1 = format!(
        "Ascii representation of Julia set with constant {:.2} + {:.2}i\n",
        julia_r, julia_i
    );
    output.extend_from_slice(footer_line1.as_bytes());

    // C: server.extended_redis_compat ? "Redis" : "Valkey"
    // TODO(port): access ctx.server().extended_redis_compat once the ServerState API
    //             is defined; for now default to the Valkey brand.
    // C: server.extended_redis_compat ? REDIS_VERSION : VALKEY_VERSION
    // TODO(port): replace the version placeholder with the real constant from redis-core
    //             once VALKEY_VERSION / REDIS_VERSION are defined there.
    output.extend_from_slice(b"Don't forget to have fun! Valkey ver. ");
    // Placeholder — real version injected in Phase B once constants are available.
    output.extend_from_slice(b"0.0.0");
    output.push(b'\n');

    // C: addReplyVerbatim(c, output_array, sdslen(output_array), "txt");
    // TODO(port): confirm method name ctx.reply_verbatim_string vs reply_verbatim on
    //             CommandContext once redis-core API is finalised.
    ctx.reply_verbatim_string(b"txt", &output)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/lolwut9.c  (87 lines, 2 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         6
//   port_notes:    1
//   unsafe_blocks: 0
//   notes: >
//     Logic translation is straightforward; the Julia set iteration and grid
//     rendering match the C exactly. Two architectural gaps remain: (1) random
//     Julia constants require a rand() equivalent — flagged for TODO(architect)
//     to add the `rand` crate; a pseudo_random_f32_minus1_to_1() placeholder
//     is emitted on CommandContext::server() instead. (2) server.extended_redis_compat
//     and VALKEY_VERSION/REDIS_VERSION constants need injection from redis-core
//     once those are defined. The arg_offset parameter replicates the C argv-
//     pointer shift performed by lolwutCommand before dispatching here.
// ──────────────────────────────────────────────────────────────────────────────

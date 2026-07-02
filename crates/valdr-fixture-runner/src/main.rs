//! JSONL driver around one persistent `valdr_engine::Engine` instance.
//!
//! Reads one JSON object per stdin line:
//! `{"id": "<string>", "now_millis": <optional u64>, "cmd": ["SET", "k", "v"]}`.
//! For each line the host clock is set to `now_millis` when present, otherwise
//! to the current wall-clock milliseconds (the engine itself has no clock).
//! The command is executed against the single engine instance and one JSON
//! line is emitted: `{"id": "...", "resp_hex": "<lowercase hex of the RESP2
//! encoding of the reply frame>"}`.
//!
//! Malformed input is a harness bug, not engine behavior, so it aborts the
//! process with a message on stderr instead of being papered over.

use std::io::{self, BufRead, Write};
use std::process::exit;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_protocol::encode_resp2;
use serde_json::{json, Value as JsonValue};
use valdr_engine::{Engine, NoopHost};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let seed = parse_seed();
    let mut engine = Engine::new(NoopHost::with_seed(0, seed));

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => die(&format!("stdin read failed: {error}")),
        };
        if line.trim().is_empty() {
            continue;
        }
        let fixture: JsonValue = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => die(&format!("invalid fixture JSON: {error}: {line}")),
        };
        let id = match fixture.get("id").and_then(JsonValue::as_str) {
            Some(id) => id.to_owned(),
            None => die(&format!("fixture line missing string 'id': {line}")),
        };
        let now_millis = match fixture.get("now_millis") {
            Some(value) => match value.as_u64() {
                Some(now_millis) => now_millis,
                None => die(&format!("fixture 'now_millis' must be a u64: {line}")),
            },
            None => wall_clock_millis(),
        };
        let argv = match fixture.get("cmd").and_then(JsonValue::as_array) {
            Some(items) => collect_argv(items, &line),
            None => die(&format!("fixture line missing array 'cmd': {line}")),
        };

        engine.host_mut().set_now_millis(now_millis);
        let frame = engine.execute(&argv);
        let mut encoded = Vec::new();
        encode_resp2(&frame, &mut encoded);

        let reply = json!({"id": id, "resp_hex": hex_lower(&encoded)});
        if let Err(error) = writeln!(out, "{reply}") {
            die(&format!("stdout write failed: {error}"));
        }
        if let Err(error) = out.flush() {
            die(&format!("stdout flush failed: {error}"));
        }
    }
}

/// Parses `--seed <u64>` from argv; defaults to 0 for deterministic replay.
fn parse_seed() -> u64 {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(pos) = args.iter().position(|a| a == "--seed") {
        match args.get(pos + 1).and_then(|v| v.parse().ok()) {
            Some(s) => s,
            None => die("--seed requires a u64 argument"),
        }
    } else {
        0
    }
}

fn collect_argv(items: &[JsonValue], line: &str) -> Vec<Vec<u8>> {
    let mut argv = Vec::with_capacity(items.len());
    for item in items {
        match item.as_str() {
            Some(text) => argv.push(text.as_bytes().to_vec()),
            None => die(&format!("fixture 'cmd' entries must be strings: {line}")),
        }
    }
    argv
}

fn wall_clock_millis() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the unix epoch");
    u64::try_from(since_epoch.as_millis()).expect("wall clock millis exceed u64")
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn die(message: &str) -> ! {
    let stderr = io::stderr();
    let mut err = stderr.lock();
    let _ = writeln!(err, "valdr-fixture-runner: {message}");
    exit(2);
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        new differential harness
//   target_crate:  valdr-fixture-runner
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         JSONL stdin/stdout driver exposing valdr-engine to the differential oracle
// ──────────────────────────────────────────────────────────────────────────

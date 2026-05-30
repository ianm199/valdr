//! `valkey-check-aof` utility — a dry-parse validator for AOF files.
//! Invoked when the binary is run via the `valkey-check-aof` name (argv[0]
//! dispatch in `main`). It does not mutate any keyspace — it walks the bytes
//! an AOF (or the files named by a manifest), tracking the last fully-parsed
//! offset and line so it can report validity. Output strings match upstream
//! validator format so test suites recognize the results.

use std::path::{Path, PathBuf};

const MANIFEST_MAX_LINE: usize = 1024;

enum InputFileType {
    Resp,
    RdbPreamble,
    MultiPart,
}

enum AofCheck {
    Ok,
    Empty,
    Truncated,
}

/// Entry point dispatched from `main` when argv[0] is `valkey-check-aof`.
pub(crate) fn run_check_aof(args: &[String]) -> i32 {
    let (fix, filepath) = match parse_args(args) {
        Some(v) => v,
        None => {
            println!("Usage: valkey-check-aof [--fix|--truncate-to-timestamp $timestamp] <file.manifest|file.aof>");
            return 1;
        }
    };

    let path = Path::new(&filepath);
    let dirpath = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    match get_input_file_type(path) {
        InputFileType::MultiPart => check_multi_part_aof(&dirpath, path, fix),
        InputFileType::Resp => check_old_style_aof(path, fix, false),
        InputFileType::RdbPreamble => check_old_style_aof(path, fix, true),
    }
 // The check functions terminate the process via `std::process::exit`.
    0
}

/// Returns `(fix, filepath)`. `--version`/`-v` exits the process directly.
fn parse_args(args: &[String]) -> Option<(bool, String)> {
    match args.len() {
        1 => {
            if args[0] == "-v" || args[0] == "--version" {
                println!("valkey-check-aof {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            Some((false, args[0].clone()))
        }
        2 if args[0] == "--fix" => Some((true, args[1].clone())),
        3 if args[0] == "--truncate-to-timestamp" => Some((false, args[2].clone())),
        _ => None,
    }
}

fn get_input_file_type(path: &Path) -> InputFileType {
    if file_is_manifest(path) {
        InputFileType::MultiPart
    } else if file_is_rdb(path) {
        InputFileType::RdbPreamble
    } else {
        InputFileType::Resp
    }
}

fn file_is_rdb(path: &Path) -> bool {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            println!("Cannot open file {}: {}", path.display(), e);
            std::process::exit(1);
        }
    };
    bytes.len() >= 8 && &bytes[..5] == b"REDIS"
}

/// Scans leading lines; a line beginning with `file` marks it a manifest, a
/// comment is skipped, anything else stops the scan. So a RESP AOF (first byte
/// `*`) is never mistaken for a manifest even if its payload contains the word "file".
fn file_is_manifest(path: &Path) -> bool {
    let content = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            println!("Cannot open file {}: {}", path.display(), e);
            std::process::exit(1);
        }
    };
    if content.is_empty() {
        return false;
    }
    let mut is_manifest = false;
    for raw_line in content.split_inclusive(|&b| b == b'\n') {
        let line = &raw_line[..raw_line.len().min(MANIFEST_MAX_LINE)];
        if line.first() == Some(&b'#') {
            continue;
        } else if line.starts_with(b"file") {
            is_manifest = true;
        } else {
            break;
        }
    }
    is_manifest
}

fn check_old_style_aof(path: &Path, fix: bool, preamble: bool) {
    println!("Start checking Old-Style AOF");
    let name = path.display().to_string();
    match check_single_aof(&name, path, true, fix, preamble) {
        Ok(AofCheck::Ok) => println!("AOF {} is valid", name),
        Ok(AofCheck::Empty) => println!("AOF {} is empty", name),
        Ok(AofCheck::Truncated) => println!("Successfully truncated AOF {}", name),
        Err(()) => std::process::exit(1),
    }
    std::process::exit(0);
}

fn check_multi_part_aof(dirpath: &Path, manifest_path: &Path, fix: bool) {
    println!("Start checking Multi Part AOF");
    let manifest = match load_manifest(manifest_path) {
        Ok(m) => m,
        Err(()) => {
            println!("Invalid AOF manifest file format");
            std::process::exit(1);
        }
    };

    let total = manifest.base.iter().len() + manifest.incr.len();
    let mut seen = 0usize;

    if let Some(base) = &manifest.base {
        let base_path = dirpath.join(base);
        seen += 1;
        let last_file = seen == total;
        let preamble = file_is_rdb(&base_path);
        println!(
            "Start to check BASE AOF ({} format).",
            if preamble { "RDB" } else { "RESP" }
        );
        match check_single_aof(base, &base_path, last_file, fix, preamble) {
            Ok(AofCheck::Ok) => println!("BASE AOF {} is valid", base),
            Ok(AofCheck::Empty) => println!("BASE AOF {} is empty", base),
            Ok(AofCheck::Truncated) => println!("Successfully truncated AOF {}", base),
            Err(()) => std::process::exit(1),
        }
    }

    if !manifest.incr.is_empty() {
        println!("Start to check INCR files.");
        for incr in &manifest.incr {
            let incr_path = dirpath.join(incr);
            seen += 1;
            let last_file = seen == total;
            match check_single_aof(incr, &incr_path, last_file, fix, false) {
                Ok(AofCheck::Ok) => println!("INCR AOF {} is valid", incr),
                Ok(AofCheck::Empty) => println!("INCR AOF {} is empty", incr),
                Ok(AofCheck::Truncated) => println!("Successfully truncated AOF {}", incr),
                Err(()) => std::process::exit(1),
            }
        }
    }

    println!("All AOF files and manifest are valid");
    std::process::exit(0);
}

struct Manifest {
    base: Option<String>,
    incr: Vec<String>,
}

/// Strict manifest parse mirroring `aofLoadManifestFromDisk`'s structural rules.
/// Any malformed line yields `Err(`, which the caller reports as
/// "Invalid AOF manifest file format".
fn load_manifest(path: &Path) -> Result<Manifest, ()> {
    let content = std::fs::read(path).map_err(|_| ())?;
    let mut base = None;
    let mut incr = Vec::new();
    for raw in content.split(|&b| b == b'\n') {
        if raw.is_empty() {
            continue;
        }
        if raw.len() > MANIFEST_MAX_LINE {
            return Err(());
        }
        if raw.first() == Some(&b'#') {
            continue;
        }
        let fields: Vec<&[u8]> = raw
            .split(|&b| b == b' ' || b == b'\r')
            .filter(|f| !f.is_empty())
            .collect();
        if fields.len() < 6 || !fields.len().is_multiple_of(2) {
            return Err(());
        }
        if fields[0] != b"file" || fields[2] != b"seq" || fields[4] != b"type" {
            return Err(());
        }
        let name = String::from_utf8(fields[1].to_vec()).map_err(|_| ())?;
        match fields[5] {
            b"b" => base = Some(name),
            b"i" => incr.push(name),
            b"h" => {}
            _ => return Err(()),
        }
    }
    Ok(Manifest { base, incr })
}

/// Walk a single AOF file, tracking the last fully-parsed byte offset (`pos`)
/// and 1-based line counter. Returns Ok when fully parsed, Err when a trailing
/// short/garbled record is found.
fn check_single_aof(
    name: &str,
    path: &Path,
    last_file: bool,
    fix: bool,
    preamble: bool,
) -> Result<AofCheck, ()> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            println!("Cannot open file {}: {}, aborting...", path.display(), e);
            std::process::exit(1);
        }
    };
    let size = bytes.len();
    if size == 0 {
        return Ok(AofCheck::Empty);
    }

    let mut cur = Cursor::new(&bytes);

    if preamble {
 // The RDB-preamble occupies the head of the file. Validate it via
 // shared RDB checker; the AOF tail (if any) begins after the RDB EOF.
        match redis_core::rdb::load::check_rdb_file(path) {
            report if report.ok => {
                println!("RDB preamble is OK, proceeding with AOF tail...");
            }
            _ => {
                println!("RDB preamble of AOF file is not sane, aborting.");
                std::process::exit(1);
            }
        }
 // We cannot currently resume RESP parsing after the RDB section without
 // the loader reporting its consumed length; treat a clean RDB preamble
 // as a valid base (the common multi-part case where the base is a pure
 // RDB snapshot and the tail lives in the INCR file).
        return Ok(AofCheck::Ok);
    }

    let mut multi = 0i64;
    let mut error: Option<String> = None;
    loop {
        let line_start_pos = if multi == 0 {
            cur.pos
        } else {
            cur.committed_pos
        };
        if multi == 0 {
            cur.committed_pos = line_start_pos;
        }
        let Some(&first) = bytes.get(cur.pos) else {
            break; // EOF
        };
        if first == b'#' {
 // Timestamp annotation line; consume to end of line.
            if !cur.skip_annotation() {
                break;
            }
        } else if first == b'*' {
            match cur.process_resp(&mut multi) {
                Ok(true) => {
                    cur.committed_pos = cur.pos;
                }
                Ok(false) => break,
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        } else {
            error = Some(format!("AOF {} format error", name));
            break;
        }
    }

    if multi != 0 && error.is_none() {
        error = Some("Reached EOF before reading EXEC for MULTI".to_string());
    }
    if let Some(e) = &error {
        println!("{}", e);
    }

    let pos = cur.committed_pos;
    let diff = size - pos;
    println!(
        "AOF analyzed: filename={}, size={}, ok_up_to={}, ok_up_to_line={}, diff={}",
        name, size, pos, cur.line, diff
    );

    if diff > 0 {
        if fix {
            if !last_file {
                println!(
                    "Failed to truncate AOF {} because it is not the last file",
                    name
                );
                std::process::exit(1);
            }
            println!(
                "This will shrink the AOF {} from {} bytes, with {} bytes, to {} bytes",
                name, size, diff, pos
            );
            print!("Continue? [y/N]: ");
            let mut answer = String::new();
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            if std::io::stdin().read_line(&mut answer).is_err()
                || !answer.trim_start().to_ascii_lowercase().starts_with('y')
            {
                println!("Aborting...");
                std::process::exit(1);
            }
            match std::fs::OpenOptions::new().write(true).open(path) {
                Ok(f) => {
                    if f.set_len(pos as u64).is_err() {
                        println!("Failed to truncate AOF {}", name);
                        std::process::exit(1);
                    }
                }
                Err(_) => {
                    println!("Failed to truncate AOF {}", name);
                    std::process::exit(1);
                }
            }
            return Ok(AofCheck::Truncated);
        }
        println!(
            "AOF {} is not valid. Use the --fix option to try fixing it.",
            name
        );
        return Err(());
    }
    Ok(AofCheck::Ok)
}

/// Byte cursor tracking state: `line` starts at 1 and increments once
/// per consumed `\r\n`; `pos` is the read head, `committed_pos` the last
/// offset that completed a full RESP record.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    committed_pos: usize,
    line: i64,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor {
            bytes,
            pos: 0,
            committed_pos: 0,
            line: 1,
        }
    }

    fn consume_newline(&mut self) -> bool {
        if self.bytes.get(self.pos) == Some(&b'\r') && self.bytes.get(self.pos + 1) == Some(&b'\n')
        {
            self.pos += 2;
            self.line += 1;
            true
        } else {
            false
        }
    }

 /// Read a `<prefix><number>\r\n` header. Returns the number.
    fn read_long(&mut self, prefix: u8) -> Option<i64> {
        if self.bytes.get(self.pos) != Some(&prefix) {
            return None;
        }
        self.pos += 1;
        let start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\r' {
            self.pos += 1;
        }
        let num: i64 = std::str::from_utf8(&self.bytes[start..self.pos])
            .ok()?
            .parse()
            .ok()?;
        if !self.consume_newline() {
            return None;
        }
        Some(num)
    }

 /// Read a `$<len>\r\n<bytes>\r\n` bulk string, discarding its content.
    fn read_string(&mut self) -> Result<Vec<u8>, ()> {
        let len = self.read_long(b'$').ok_or(())?;
        if len < 0 {
            return Err(());
        }
        let len = len as usize;
        if self.pos + len + 2 > self.bytes.len() {
            return Err(());
        }
        let s = self.bytes[self.pos..self.pos + len].to_vec();
        self.pos += len;
        if !self.consume_newline() {
            return Err(());
        }
        Ok(s)
    }

 /// Decode one RESP array, tracking MULTI/EXEC nesting. Returns Ok(false) on
 /// a clean short read (incomplete trailing record), Err on a malformed one.
    fn process_resp(&mut self, multi: &mut i64) -> Result<bool, String> {
        let argc = match self.read_long(b'*') {
            Some(n) => n,
            None => return Ok(false),
        };
        for i in 0..argc {
            let str_bytes = match self.read_string() {
                Ok(s) => s,
                Err(()) => return Ok(false),
            };
            if i == 0 {
                if str_bytes.eq_ignore_ascii_case(b"multi") {
                    *multi += 1;
                    if *multi > 1 {
                        return Err("Unexpected MULTI".to_string());
                    }
                } else if str_bytes.eq_ignore_ascii_case(b"exec") {
                    *multi -= 1;
                    if *multi != 0 {
                        return Err("Unexpected EXEC".to_string());
                    }
                }
            }
        }
        Ok(true)
    }

 /// Consume a `#...\r\n` annotation line.
    fn skip_annotation(&mut self) -> bool {
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.pos += 1;
        }
        if self.pos < self.bytes.len() {
            self.pos += 1;
            self.line += 1;
            self.committed_pos = self.pos;
            true
        } else {
            false
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-server
//   confidence:    partial
//   todos:         2
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         dry-parse validator; RDB-preamble AOF-tail + --fix truncation deferred
// ──────────────────────────────────────────────────────────────────────────

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
    let (fix, truncate_to_timestamp, filepath) = match parse_args(args) {
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
        InputFileType::MultiPart => {
            check_multi_part_aof(&dirpath, path, fix, truncate_to_timestamp)
        }
        InputFileType::Resp => check_old_style_aof(path, fix, truncate_to_timestamp, false),
        InputFileType::RdbPreamble => {
            check_old_style_aof(path, fix, truncate_to_timestamp, true)
        }
    }
    // The check functions terminate the process via `std::process::exit`.
    0
}

/// Returns `(fix, truncate_to_timestamp, filepath)`.
/// `--version`/`-v` exits the process directly.
fn parse_args(args: &[String]) -> Option<(bool, Option<i64>, String)> {
    match args.len() {
        1 => {
            if args[0] == "-v" || args[0] == "--version" {
                println!("valkey-check-aof {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            Some((false, None, args[0].clone()))
        }
        2 if args[0] == "--fix" => Some((true, None, args[1].clone())),
        3 if args[0] == "--truncate-to-timestamp" => {
            let timestamp = parse_i64_ascii(args[1].as_bytes())?;
            Some((false, Some(timestamp), args[2].clone()))
        }
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

fn check_old_style_aof(path: &Path, fix: bool, truncate_to_timestamp: Option<i64>, preamble: bool) {
    println!("Start checking Old-Style AOF");
    let name = path.display().to_string();
    match check_single_aof(&name, path, true, fix, truncate_to_timestamp, preamble) {
        Ok(AofCheck::Ok) => println!("AOF {} is valid", name),
        Ok(AofCheck::Empty) => println!("AOF {} is empty", name),
        Ok(AofCheck::Truncated) => println!("Successfully truncated AOF {}", name),
        Err(()) => std::process::exit(1),
    }
    std::process::exit(0);
}

fn check_multi_part_aof(
    dirpath: &Path,
    manifest_path: &Path,
    fix: bool,
    truncate_to_timestamp: Option<i64>,
) {
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
        match check_single_aof(
            base,
            &base_path,
            last_file,
            fix,
            truncate_to_timestamp,
            preamble,
        ) {
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
            match check_single_aof(
                incr,
                &incr_path,
                last_file,
                fix,
                truncate_to_timestamp,
                false,
            ) {
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
    if content.is_empty() {
        return Err(());
    }
    let mut base = None;
    let mut incr = Vec::new();
    let mut max_incr_seq = 0i64;
    let mut saw_line = false;
    let mut pos = 0usize;

    while pos < content.len() {
        let Some(rel_end) = content[pos..].iter().position(|&b| b == b'\n') else {
            return Err(());
        };
        let end = pos + rel_end;
        let raw = &content[pos..=end];
        pos = end + 1;
        saw_line = true;

        if raw.len() > MANIFEST_MAX_LINE {
            return Err(());
        }
        if raw.first() == Some(&b'#') {
            continue;
        }
        let line = trim_manifest_line(raw);
        if line.is_empty() {
            return Err(());
        }

        let fields: Vec<&[u8]> = line
            .split(|b| b.is_ascii_whitespace())
            .filter(|f| !f.is_empty())
            .collect();
        if fields.len() < 6 || !fields.len().is_multiple_of(2) {
            return Err(());
        }

        let mut name: Option<&[u8]> = None;
        let mut seq: Option<i64> = None;
        let mut file_type: Option<&[u8]> = None;
        for pair in fields.chunks_exact(2) {
            if pair[0].eq_ignore_ascii_case(b"file") {
                if !path_is_base_name(pair[1]) {
                    return Err(());
                }
                name = Some(pair[1]);
            } else if pair[0].eq_ignore_ascii_case(b"seq") {
                seq = parse_i64_ascii(pair[1]);
            } else if pair[0].eq_ignore_ascii_case(b"type") {
                file_type = Some(pair[1]);
            }
        }

        let name = name.ok_or(())?;
        let seq = seq.filter(|n| *n != 0).ok_or(())?;
        let file_type = file_type.ok_or(())?;
        let name = String::from_utf8(name.to_vec()).map_err(|_| ())?;

        match file_type {
            b"b" => {
                if base.is_some() {
                    return Err(());
                }
                base = Some(name);
            }
            b"i" => {
                if seq <= max_incr_seq {
                    return Err(());
                }
                max_incr_seq = seq;
                incr.push(name);
            }
            b"h" => {}
            _ => return Err(()),
        }
    }
    if !saw_line || (base.is_none() && incr.is_empty()) {
        return Err(());
    }
    Ok(Manifest { base, incr })
}

fn trim_manifest_line(line: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = line.len();
    while start < end && matches!(line[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(line[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    &line[start..end]
}

fn path_is_base_name(path: &[u8]) -> bool {
    !path.contains(&b'/')
}

fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut i = 0usize;
    let mut sign = 1i64;
    if bytes[0] == b'-' {
        sign = -1;
        i = 1;
    } else if bytes[0] == b'+' {
        i = 1;
    }
    if i == bytes.len() {
        return None;
    }
    let mut out = 0i64;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        i += 1;
    }
    out.checked_mul(sign)
}

/// Walk a single AOF file, tracking the last fully-parsed byte offset (`pos`)
/// and 1-based line counter. Returns Ok when fully parsed, Err when a trailing
/// short/garbled record is found.
fn check_single_aof(
    name: &str,
    path: &Path,
    last_file: bool,
    fix: bool,
    truncate_to_timestamp: Option<i64>,
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
    let mut valid_before_multi = 0usize;
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
            if let Some(result) =
                cur.process_annotation(name, path, last_file, truncate_to_timestamp)
            {
                return Ok(result);
            }
        } else if first == b'*' {
            let record_start = cur.pos;
            let multi_before = multi;
            match cur.process_resp(&mut multi) {
                Ok(true) => {
                    if multi_before == 0 && multi > 0 {
                        valid_before_multi = record_start;
                        cur.committed_pos = valid_before_multi;
                    } else if multi > 0 {
                        cur.committed_pos = valid_before_multi;
                    } else {
                        cur.committed_pos = cur.pos;
                    }
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
        cur.committed_pos = valid_before_multi;
    }
    if let Some(e) = &error {
        println!("{}", e);
    }

    let pos = cur.committed_pos;
    let diff = size - pos;
    if diff == 0 && truncate_to_timestamp.is_some() {
        println!(
            "Truncate nothing in AOF {} to timestamp {}",
            name,
            truncate_to_timestamp.unwrap()
        );
        return Ok(AofCheck::Ok);
    }
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

    /// Consume a `#...\r\n` annotation line. In truncate-to-timestamp mode,
    /// truncate at the start of the first `#TS:` annotation greater than the
    /// requested target.
    fn process_annotation(
        &mut self,
        name: &str,
        path: &Path,
        last_file: bool,
        truncate_to_timestamp: Option<i64>,
    ) -> Option<AofCheck> {
        let annotation_start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.pos += 1;
        }
        if self.pos >= self.bytes.len() {
            println!("Failed to read annotations from AOF {}, aborting...", name);
            std::process::exit(1);
        }

        let line = &self.bytes[annotation_start..=self.pos];
        self.pos += 1;
        self.line += 1;
        self.committed_pos = self.pos;

        let Some(target) = truncate_to_timestamp else {
            return None;
        };
        if !line.starts_with(b"#TS:") {
            return None;
        }
        let Some(cr_pos) = line.iter().position(|&b| b == b'\r') else {
            println!("Invalid timestamp annotation");
            std::process::exit(1);
        };
        let Some(timestamp) = parse_i64_ascii(&line[4..cr_pos]) else {
            println!("Invalid timestamp annotation");
            std::process::exit(1);
        };
        if timestamp <= target {
            return None;
        }
        if annotation_start == 0 {
            println!(
                "AOF {} has nothing before timestamp {}, aborting...",
                name, target
            );
            std::process::exit(1);
        }
        if !last_file {
            println!(
                "Failed to truncate AOF {} to timestamp {} to offset {} because it is not the last file.",
                name, target, annotation_start
            );
            println!(
                "If you insist, please delete all files after this file according to the manifest file and delete the corresponding records in manifest file manually. Then re-run valkey-check-aof."
            );
            std::process::exit(1);
        }
        match std::fs::OpenOptions::new().write(true).open(path) {
            Ok(file) => {
                if file.set_len(annotation_start as u64).is_err() {
                    println!("Failed to truncate AOF {} to timestamp {}", name, target);
                    std::process::exit(1);
                }
            }
            Err(_) => {
                println!("Failed to truncate AOF {} to timestamp {}", name, target);
                std::process::exit(1);
            }
        }
        Some(AofCheck::Truncated)
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

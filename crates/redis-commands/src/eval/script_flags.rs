//! Lua script shebang and function-source flag parsing.
//!
//! This module intentionally preserves the existing byte-scanning behavior used
//! by queued write preflight, EVAL/EVALSHA, and FUNCTION LOAD compatibility
//! checks.

use std::borrow::Cow;

use redis_types::{RedisError, RedisResult};

use super::{ascii_eq_ci, ascii_lower, ascii_starts_with_ci};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct EvalScriptFlags {
    pub(super) has_shebang: bool,
    pub(super) no_writes: bool,
    pub(super) allow_oom: bool,
    pub(super) allow_stale: bool,
}

pub(super) fn parse_eval_shebang(script_bytes: &[u8]) -> RedisResult<(EvalScriptFlags, &[u8])> {
    if script_bytes.starts_with(b"#!") && !script_bytes.starts_with(b"#!lua") {
        return Err(RedisError::runtime(b"ERR Could not find scripting engine"));
    }
    if !script_bytes.starts_with(b"#!lua") {
        return Ok((EvalScriptFlags::default(), script_bytes));
    }
    let line_end = script_bytes
        .iter()
        .position(|b| *b == b'\n')
        .unwrap_or(script_bytes.len());
    let first_line = &script_bytes[..line_end];
    let body = if line_end < script_bytes.len() {
        &script_bytes[line_end + 1..]
    } else {
        b""
    };

    let mut flags = EvalScriptFlags {
        has_shebang: true,
        ..EvalScriptFlags::default()
    };
    let rest = first_line
        .strip_prefix(b"#!lua")
        .unwrap_or(first_line)
        .trim_ascii();
    if rest.is_empty() {
        return Ok((flags, body));
    }
    for token in rest.split(|b| b.is_ascii_whitespace()) {
        if token.is_empty() {
            continue;
        }
        let Some(value) = token.strip_prefix(b"flags=") else {
            return Err(RedisError::runtime(b"ERR Unknown lua shebang option"));
        };
        if value.is_empty() {
            continue;
        }
        for flag in value.split(|b| *b == b',') {
            if flag.is_empty() {
                continue;
            }
            if ascii_eq_ci(flag, b"no-writes") {
                flags.no_writes = true;
            } else if ascii_eq_ci(flag, b"allow-oom") {
                flags.allow_oom = true;
            } else if ascii_eq_ci(flag, b"allow-stale") {
                flags.allow_stale = true;
            } else {
                return Err(RedisError::runtime(
                    b"ERR Unexpected flag in script shebang",
                ));
            }
        }
    }
    Ok((flags, body))
}

pub(super) fn function_source_eval_flags(code: &[u8]) -> EvalScriptFlags {
    let mut flags = EvalScriptFlags::default();
    let mut offset = 0usize;
    while offset < code.len()
        && !(flags.has_shebang && flags.no_writes && flags.allow_oom && flags.allow_stale)
    {
        match ascii_lower(code[offset]) {
            b'#' => {
                if !flags.has_shebang && ascii_starts_with_ci(&code[offset..], b"#!lua") {
                    flags.has_shebang = true;
                }
            }
            b'f' => {
                let rest = &code[offset..];
                if !flags.no_writes && ascii_starts_with_ci(rest, b"flags=no-writes") {
                    flags.no_writes = true;
                }
                if !flags.allow_oom && ascii_starts_with_ci(rest, b"flags=allow-oom") {
                    flags.allow_oom = true;
                }
                if !flags.allow_stale && ascii_starts_with_ci(rest, b"flags=allow-stale") {
                    flags.allow_stale = true;
                }
            }
            _ => {}
        }
        offset += 1;
    }

    flags
}

pub(super) fn function_source_allows_oom(code: &[u8]) -> bool {
    let mut offset = 0usize;
    while offset < code.len() {
        if ascii_lower(code[offset]) == b'f'
            && ascii_starts_with_ci(&code[offset..], b"flags=allow-oom")
        {
            return true;
        }
        offset += 1;
    }
    false
}

pub(super) fn strip_embedded_eval_shebang_lines(code: &[u8]) -> Cow<'_, [u8]> {
    let mut out: Option<Vec<u8>> = None;
    let mut start = 0usize;
    while start < code.len() {
        let rel_end = code[start..]
            .iter()
            .position(|b| *b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(code.len() - start);
        let line = &code[start..start + rel_end];
        let trimmed = line.trim_ascii_start();
        if !trimmed.starts_with(b"#!lua flags=") {
            if let Some(out) = out.as_mut() {
                out.extend_from_slice(line);
            }
        } else if out.is_none() {
            let mut stripped = Vec::with_capacity(code.len());
            stripped.extend_from_slice(&code[..start]);
            out = Some(stripped);
        }
        start += rel_end;
    }
    match out {
        Some(out) => Cow::Owned(out),
        None => Cow::Borrowed(code),
    }
}

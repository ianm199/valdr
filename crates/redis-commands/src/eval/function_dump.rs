//! Function library list/dump/RDB serialization helpers.

use std::collections::HashMap;

use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

use super::bytes::{ascii_casecmp_bytes, hex_decode, hex_encode};
use super::{
    compile_function_library, function_libraries, function_script_checks, install_function_library,
    parse_function_library_header, snapshot_function_libraries, FunctionDefinition,
    LoadedFunctionLibrary, PreparedFunctionLibraries,
};

pub(crate) fn function_library_codes_for_aof_rewrite() -> Vec<Vec<u8>> {
    let mut libraries = snapshot_function_libraries();
    libraries.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    libraries.into_iter().map(|library| library.code).collect()
}

pub(crate) fn function_vm_memory_used_estimate() -> usize {
    snapshot_function_libraries()
        .iter()
        .map(|library| {
            library.name.len()
                + library.code.len()
                + library
                    .functions
                    .iter()
                    .map(|function| {
                        function.name.len()
                            + function.description.as_ref().map_or(0, Vec::len)
                            + 256
                    })
                    .sum::<usize>()
        })
        .sum()
}

pub(super) fn function_library_frame(
    library: &LoadedFunctionLibrary,
    with_code: bool,
) -> RespFrame {
    let mut functions = library.functions.clone();
    functions.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    let function_items = functions.iter().map(function_definition_frame).collect();
    let mut fields = vec![
        (
            RespFrame::bulk(RedisString::from_static(b"library_name")),
            RespFrame::bulk(RedisString::from_vec(library.name.clone())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"engine")),
            RespFrame::bulk(RedisString::from_static(b"LUA")),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"functions")),
            RespFrame::array(function_items),
        ),
    ];
    if with_code {
        fields.push((
            RespFrame::bulk(RedisString::from_static(b"library_code")),
            RespFrame::bulk(RedisString::from_vec(library.code.clone())),
        ));
    }
    RespFrame::Map(fields)
}

fn function_definition_frame(function: &FunctionDefinition) -> RespFrame {
    let mut flags = Vec::new();
    if function.no_writes {
        flags.push(RespFrame::bulk(RedisString::from_static(b"no-writes")));
    }
    if function.allow_oom {
        flags.push(RespFrame::bulk(RedisString::from_static(b"allow-oom")));
    }
    if function.allow_stale {
        flags.push(RespFrame::bulk(RedisString::from_static(b"allow-stale")));
    }
    let flags = RespFrame::array(flags);
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"name")),
            RespFrame::bulk(RedisString::from_vec(function.name.clone())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"description")),
            function
                .description
                .as_ref()
                .map(|description| RespFrame::bulk(RedisString::from_vec(description.clone())))
                .unwrap_or_else(RespFrame::null_bulk),
        ),
        (RespFrame::bulk(RedisString::from_static(b"flags")), flags),
    ])
}

const FUNCTION_DUMP_MAGIC: &[u8] = b"VALKEYRSFUNC1\n";

pub(super) fn encode_function_dump(libraries: &[LoadedFunctionLibrary]) -> Vec<u8> {
    let mut libraries = libraries.to_vec();
    libraries.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    let mut out = FUNCTION_DUMP_MAGIC.to_vec();
    for library in libraries {
        out.extend_from_slice(&hex_encode(&library.name));
        out.push(b' ');
        out.extend_from_slice(&hex_encode(&library.code));
        out.push(b'\n');
    }
    out
}

pub(super) fn decode_function_dump(payload: &[u8]) -> RedisResult<Vec<LoadedFunctionLibrary>> {
    decode_function_dump_inner(payload).ok_or_else(function_dump_payload_error)
}

fn decode_function_dump_inner(payload: &[u8]) -> Option<Vec<LoadedFunctionLibrary>> {
    let rest = payload.strip_prefix(FUNCTION_DUMP_MAGIC)?;
    let mut libraries = Vec::new();
    for line in rest.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let split = line.iter().position(|b| *b == b' ')?;
        let name = hex_decode(&line[..split])?;
        let code = hex_decode(&line[split + 1..])?;
        let (parsed_name, library_body) = parse_function_library_header(&code).ok()?;
        if parsed_name != name {
            return None;
        }
        let functions = compile_function_library(library_body).ok()?;
        libraries.push(LoadedFunctionLibrary {
            name: parsed_name,
            script_checks: function_script_checks(&code),
            code,
            functions,
        });
    }
    Some(libraries)
}

fn function_dump_payload_error() -> RedisError {
    RedisError::runtime(b"ERR DUMP payload version or checksum are wrong")
}

pub fn function_rdb_payloads() -> Vec<Vec<u8>> {
    let libraries = snapshot_function_libraries();
    if libraries.is_empty() {
        Vec::new()
    } else {
        vec![encode_function_dump(&libraries)]
    }
}

pub fn prepare_rdb_function_replacement(
    payloads: &[Vec<u8>],
) -> RedisResult<PreparedFunctionLibraries> {
    let mut prepared = HashMap::new();
    for payload in payloads {
        for library in decode_function_dump(payload)? {
            install_function_library(&mut prepared, library, false, false)?;
        }
    }
    Ok(PreparedFunctionLibraries {
        libraries: prepared.into_values().collect(),
    })
}

pub fn install_rdb_function_replacement(prepared: PreparedFunctionLibraries) {
    let mut guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clear();
    for library in prepared.libraries {
        guard.insert(library.name.clone(), library);
    }
}

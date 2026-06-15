//! Process-wide loaded function library store.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use redis_types::{RedisError, RedisResult};

use super::bytes::ascii_eq_ci;
use super::script_checks::FunctionScriptChecks;

#[derive(Debug, Clone)]
pub(super) struct FunctionDefinition {
    pub(super) name: Vec<u8>,
    pub(super) description: Option<Vec<u8>>,
    pub(super) no_writes: bool,
    pub(super) allow_oom: bool,
    pub(super) allow_stale: bool,
}

#[derive(Debug, Clone)]
pub(super) struct LoadedFunctionLibrary {
    pub(super) name: Vec<u8>,
    pub(super) code: Vec<u8>,
    pub(super) functions: Vec<FunctionDefinition>,
    pub(super) script_checks: FunctionScriptChecks,
}

pub struct PreparedFunctionLibraries {
    pub(super) libraries: Vec<LoadedFunctionLibrary>,
}

pub(super) fn function_libraries() -> &'static Mutex<HashMap<Vec<u8>, LoadedFunctionLibrary>> {
    static LIBRARIES: OnceLock<Mutex<HashMap<Vec<u8>, LoadedFunctionLibrary>>> = OnceLock::new();
    LIBRARIES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn snapshot_function_libraries() -> Vec<LoadedFunctionLibrary> {
    let guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.values().cloned().collect()
}

pub(super) fn install_function_library(
    libraries: &mut HashMap<Vec<u8>, LoadedFunctionLibrary>,
    loaded: LoadedFunctionLibrary,
    replace: bool,
    quote_library_collision: bool,
) -> RedisResult<()> {
    let old_key = function_library_key(libraries, &loaded.name);
    if old_key.is_some() && !replace {
        let mut msg = if quote_library_collision {
            b"ERR Library '".to_vec()
        } else {
            b"ERR Library ".to_vec()
        };
        msg.extend_from_slice(&loaded.name);
        if quote_library_collision {
            msg.extend_from_slice(b"' already exists");
        } else {
            msg.extend_from_slice(b" already exists");
        }
        return Err(RedisError::runtime(msg));
    }
    for (key, library) in libraries.iter() {
        if old_key.as_ref().is_some_and(|old| old == key) {
            continue;
        }
        for existing in &library.functions {
            if let Some(new_fn) = loaded
                .functions
                .iter()
                .find(|new_fn| ascii_eq_ci(&new_fn.name, &existing.name))
            {
                let mut msg = b"ERR Function ".to_vec();
                msg.extend_from_slice(&new_fn.name);
                msg.extend_from_slice(b" already exists");
                return Err(RedisError::runtime(msg));
            }
        }
    }
    if let Some(key) = old_key {
        libraries.remove(&key);
    }
    libraries.insert(loaded.name.clone(), loaded);
    Ok(())
}

pub(super) fn loaded_library_code_is_identical(
    libraries: &HashMap<Vec<u8>, LoadedFunctionLibrary>,
    name: &[u8],
    code: &[u8],
) -> bool {
    libraries
        .values()
        .any(|library| ascii_eq_ci(&library.name, name) && library.code == code)
}

pub(super) fn function_library_key(
    libraries: &HashMap<Vec<u8>, LoadedFunctionLibrary>,
    name: &[u8],
) -> Option<Vec<u8>> {
    libraries
        .keys()
        .find(|existing| ascii_eq_ci(existing, name))
        .cloned()
}

pub(super) fn find_loaded_function(
    name: &[u8],
) -> Option<(LoadedFunctionLibrary, FunctionDefinition)> {
    let guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for library in guard.values() {
        for function in &library.functions {
            if ascii_eq_ci(&function.name, name) {
                return Some((library.clone(), function.clone()));
            }
        }
    }
    None
}

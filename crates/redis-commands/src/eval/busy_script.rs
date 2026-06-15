//! Process-wide busy-script state used by SCRIPT KILL/FUNCTION KILL gates.

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};

use redis_types::RedisError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BusyScriptKind {
    Eval,
    Function,
}

#[derive(Clone, Debug)]
pub(super) struct BusyScriptState {
    pub(super) kind: BusyScriptKind,
    pub(super) owner_id: u64,
    pub(super) name: Vec<u8>,
    pub(super) command: Vec<Vec<u8>>,
    pub(super) dirty: bool,
}

fn busy_script_state() -> &'static Mutex<Option<BusyScriptState>> {
    static STATE: OnceLock<Mutex<Option<BusyScriptState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

static BUSY_SCRIPT_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(crate) fn is_script_busy() -> bool {
    if !BUSY_SCRIPT_ACTIVE.load(AtomicOrdering::Acquire) {
        return false;
    }
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.is_some()
}

pub(crate) fn busy_script_owner_is(client_id: u64) -> bool {
    if !BUSY_SCRIPT_ACTIVE.load(AtomicOrdering::Acquire) {
        return false;
    }
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .as_ref()
        .is_some_and(|state| state.owner_id == client_id)
}

pub(crate) fn busy_script_error_reply() -> Vec<u8> {
    b"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"
        .to_vec()
}

pub(super) fn busy_script_error() -> RedisError {
    RedisError::runtime(
        b"BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
    )
}

pub(super) fn busy_script_snapshot() -> Option<BusyScriptState> {
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clone()
}

pub(super) fn set_busy_script(state: BusyScriptState) {
    let mut guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = Some(state);
    BUSY_SCRIPT_ACTIVE.store(true, AtomicOrdering::Release);
}

pub(super) fn clear_busy_script() {
    let mut guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = None;
    BUSY_SCRIPT_ACTIVE.store(false, AtomicOrdering::Release);
}

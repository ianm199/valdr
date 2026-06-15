//! Active FCALL context bridge for cached Lua function callbacks.
//!
//! Cached function runtimes keep Lua callbacks alive across invocations, but
//! each invocation must re-enter the current command context. This module owns
//! the narrow raw-pointer bridge used for that re-entry:
//!
//! - [`enter_active_function_call`] installs pointers that are valid only for
//!   the current guarded call frame on the current thread.
//! - [`ActiveFunctionCallGuard`] clears the thread-local slot before that frame
//!   exits.
//! - all unsafe dereferences live here so future Lua backends can replace this
//!   bridge without hunting through the command implementation.

use std::cell::{Cell, RefCell};

use mlua::Error as LuaError;
use redis_core::CommandContext;

#[derive(Clone, Copy)]
pub(super) struct ActiveFunctionCall {
    pub(super) ctx: *mut CommandContext<'static>,
    pub(super) read_only: bool,
    pub(super) stale_replica_blocked: bool,
    pub(super) function_allow_stale: bool,
    pub(super) script_dirty: *const Cell<bool>,
    pub(super) script_error_already_recorded: *const Cell<bool>,
}

pub(super) struct ActiveFunctionCallGuard;

impl Drop for ActiveFunctionCallGuard {
    fn drop(&mut self) {
        ACTIVE_FUNCTION_CALL.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }
}

thread_local! {
    static ACTIVE_FUNCTION_CALL: RefCell<Option<ActiveFunctionCall>> = const { RefCell::new(None) };
}

pub(super) fn function_call_active() -> bool {
    ACTIVE_FUNCTION_CALL.with(|slot| slot.borrow().is_some())
}

pub(super) fn enter_active_function_call(
    ctx: &mut CommandContext<'_>,
    read_only: bool,
    stale_replica_blocked: bool,
    function_allow_stale: bool,
    script_dirty: &Cell<bool>,
    script_error_already_recorded: &Cell<bool>,
) -> ActiveFunctionCallGuard {
    ACTIVE_FUNCTION_CALL.with(|slot| {
        *slot.borrow_mut() = Some(ActiveFunctionCall {
            ctx: ctx as *mut CommandContext<'_> as *mut CommandContext<'static>,
            read_only,
            stale_replica_blocked,
            function_allow_stale,
            script_dirty: script_dirty as *const Cell<bool>,
            script_error_already_recorded: script_error_already_recorded as *const Cell<bool>,
        });
    });
    ActiveFunctionCallGuard
}

pub(super) fn active_function_call() -> mlua::Result<ActiveFunctionCall> {
    ACTIVE_FUNCTION_CALL.with(|slot| {
        slot.borrow().ok_or_else(|| {
            LuaError::RuntimeError("FUNCTION runtime called outside active FCALL".to_string())
        })
    })
}

pub(super) fn active_function_dirty(active: ActiveFunctionCall) -> &'static Cell<bool> {
    // The pointer is installed only for the duration of the guarded cached
    // function call and cleared by `ActiveFunctionCallGuard` before the stack
    // frame exits.
    unsafe { &*active.script_dirty }
}

pub(super) fn active_function_error_recorded(active: ActiveFunctionCall) -> &'static Cell<bool> {
    // See `active_function_dirty`; both cells live in the guarded call frame.
    unsafe { &*active.script_error_already_recorded }
}

pub(super) fn with_active_function_context<R>(
    f: impl FnOnce(&mut CommandContext<'static>, ActiveFunctionCall) -> mlua::Result<R>,
) -> mlua::Result<R> {
    let active = active_function_call()?;
    // The cached Lua callbacks are process-static, but the command context is
    // per-call. The guard above ensures this raw pointer is valid only while
    // the callback is executing on the same thread.
    let ctx = unsafe { &mut *active.ctx };
    f(ctx, active)
}

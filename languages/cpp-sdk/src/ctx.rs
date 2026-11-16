//! The context a node function gets, and the `dd_*` calls it makes on it: progress, log and detail
//! events, a fault, the node's state, and arena memory. The events go out as the node makes them.

use std::ffi::{c_char, c_void};
use std::ptr;

use daedalus_wire::{
    FaultCode, LogLevel, Progress, REASON_CAP, Schema, WorkerMsg, truncate_reason,
};

use crate::arena::RunArena;
use crate::convert::c_text;
use crate::ffi::DdValue;
use crate::send::Packets;

/// The context a node gets, as the C header's `dd_ctx`. A node holds a pointer to it and never
/// looks inside.
#[repr(C)]
pub struct DdCtx {
    _opaque: [u8; 0],
}

/// What the loop keeps for one node call. It is passed to the node as a `DdCtx` pointer, and the
/// `dd_*` calls read and write it through that pointer while the node runs.
pub struct Ctx {
    /// The run's arena, which `dd_alloc` allocates in.
    pub arena: *mut RunArena,
    /// The packet stream the run's events go to.
    pub packets: *mut Packets,
    pub run: u64,
    /// The fault the node set with `dd_fault`, if any.
    pub fault: Option<(FaultCode, String)>,
    /// The node's state for this call, or null when it has none.
    pub state: *mut DdValue,
    /// The value the node set with `dd_set_state`, which `state` points to after the set.
    pub state_store: DdValue,
    pub state_set: bool,
}

impl Ctx {
    /// The context the loop made for the running node.
    ///
    /// # Safety
    /// `cx` is the pointer the loop passed to the node that is running now.
    pub unsafe fn from_raw<'a>(cx: *mut DdCtx) -> &'a mut Ctx {
        // SAFETY: the pointer is the live `Ctx` the loop made, and the node calls back only while
        // it runs, so the borrow is not used after the loop moves on.
        unsafe { &mut *cx.cast::<Ctx>() }
    }

    /// The run's arena.
    pub fn arena(&mut self) -> &mut RunArena {
        // SAFETY: the loop keeps the arena alive for the whole run, and nothing else borrows it while
        // the node runs.
        unsafe { &mut *self.arena }
    }

    /// Sends an event. A write that fails is dropped, as the C loop always has.
    fn send(&mut self, msg: &WorkerMsg) {
        // SAFETY: the loop keeps the packet stream alive for the run, and the node does not reach it
        // except through these calls.
        let packets = unsafe { &mut *self.packets };
        let _ = packets.send(msg, &Schema::new());
    }
}

/// Allocates `size` bytes in the run's arena. Valid until the run ends.
///
/// # Safety
/// `cx` is the context the loop passed to the running node.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_alloc(cx: *mut DdCtx, size: usize) -> *mut c_void {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    cx.arena().alloc(size).cast()
}

/// Reports progress, as a fraction from 0.0 to 1.0, and a message. Sent before the run's outputs.
///
/// # Safety
/// `cx` is the running node's context, and `message` is a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_progress(cx: *mut DdCtx, fraction: f32, message: *const c_char) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    // SAFETY: the caller's message is a NUL-terminated string.
    let message = unsafe { c_text(message) };
    let run = cx.run;
    cx.send(&WorkerMsg::Progress(Progress {
        run,
        node_index: 0,
        message,
        fraction: Some(fraction),
    }));
}

/// Logs a line at a level: "error", "warn", "info", "debug" or "trace". Any other level is "info".
///
/// # Safety
/// `cx` is the running node's context, and `level` and `text` are NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_log(cx: *mut DdCtx, level: *const c_char, text: *const c_char) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    // SAFETY: the caller's level is a NUL-terminated string.
    let level = log_level(&unsafe { c_text(level) });
    // SAFETY: the caller's text is a NUL-terminated string.
    let text = unsafe { c_text(text) };
    cx.send(&WorkerMsg::Log { level, text });
}

/// Gives the host more about a failure: text a node writes before it fails. Any number of pieces,
/// kept in order with the step's fault. The host drops them when the run does not fail.
///
/// # Safety
/// `cx` is the running node's context, and `text` is a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_detail(cx: *mut DdCtx, text: *const c_char) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    // SAFETY: the caller's text is a NUL-terminated string.
    let text = unsafe { c_text(text) };
    let run = cx.run;
    cx.send(&WorkerMsg::Detail { run, text });
}

/// Gives this failure a number of the node's own, sent before the fault.
///
/// # Safety
/// `cx` is the running node's context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_fault_code(cx: *mut DdCtx, code: u32) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    let run = cx.run;
    cx.send(&WorkerMsg::NodeCode { run, code });
}

/// Sets the fault a node reports when it returns nonzero. `code` names one of the closed fault
/// codes; any other name is an internal fault. The reason is cut to the wire's cap.
///
/// # Safety
/// `cx` is the running node's context, and `code` and `reason` are NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_fault(cx: *mut DdCtx, code: *const c_char, reason: *const c_char) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    // SAFETY: the caller's code is a NUL-terminated string.
    let code = unsafe { c_text(code) };
    // SAFETY: the caller's reason is a NUL-terminated string.
    let reason = unsafe { c_text(reason) };
    let code = FaultCode::ALL
        .into_iter()
        .find(|known| known.name() == code)
        .unwrap_or(FaultCode::Internal);
    cx.fault = Some((code, truncate_reason(&reason, REASON_CAP).to_owned()));
}

/// The node's state for this call: a pointer, or null when the node has none.
///
/// # Safety
/// `cx` is the running node's context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_state(cx: *mut DdCtx) -> *mut DdValue {
    // SAFETY: the caller passes the running node's context.
    unsafe { Ctx::from_raw(cx) }.state
}

/// Sets the node's state, kept for the next run.
///
/// # Safety
/// `cx` is the running node's context, and `value` is a value of the node's state type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_set_state(cx: *mut DdCtx, value: DdValue) {
    // SAFETY: the caller passes the running node's context.
    let cx = unsafe { Ctx::from_raw(cx) };
    cx.state_store = value;
    cx.state = ptr::addr_of_mut!(cx.state_store);
    cx.state_set = true;
}

/// The level a log names. Any name that is not one of the five is "info".
fn log_level(name: &str) -> LogLevel {
    match name {
        "error" => LogLevel::Error,
        "warn" => LogLevel::Warn,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        _ => LogLevel::Info,
    }
}

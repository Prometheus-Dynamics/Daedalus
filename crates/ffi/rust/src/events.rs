//! Diagnostic events a node emits while it runs. They travel back in the response's `events`, in
//! the order they were emitted, like the events of the other worker SDKs.

use std::cell::RefCell;
use std::collections::BTreeMap;

use daedalus_ffi_core::{InvokeEvent, InvokeEventLevel};

thread_local! {
    static EVENTS: RefCell<Vec<InvokeEvent>> = const { RefCell::new(Vec::new()) };
}

/// Record an event for the node that is running. Outside a node call the event is dropped at the
/// next invoke.
pub fn emit_event(
    level: InvokeEventLevel,
    message: impl Into<String>,
    metadata: BTreeMap<String, serde_json::Value>,
) {
    EVENTS.with(|events| {
        events.borrow_mut().push(InvokeEvent {
            level,
            message: message.into(),
            metadata,
        })
    });
}

pub fn event_trace(message: impl Into<String>) {
    emit_event(InvokeEventLevel::Trace, message, BTreeMap::new());
}

pub fn event_debug(message: impl Into<String>) {
    emit_event(InvokeEventLevel::Debug, message, BTreeMap::new());
}

pub fn event_info(message: impl Into<String>) {
    emit_event(InvokeEventLevel::Info, message, BTreeMap::new());
}

pub fn event_warn(message: impl Into<String>) {
    emit_event(InvokeEventLevel::Warn, message, BTreeMap::new());
}

/// Discard events left over from an earlier call, then start recording for the next one.
pub(crate) fn begin_call() {
    EVENTS.with(|events| events.borrow_mut().clear());
}

/// The events recorded since [`begin_call`], in emission order.
pub(crate) fn take_call_events() -> Vec<InvokeEvent> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

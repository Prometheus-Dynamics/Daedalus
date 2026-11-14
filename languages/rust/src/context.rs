//! What a running node can see and report: whether the host asked it to stop, its state between
//! runs, and progress and log events, which go out in order before the run's outputs.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use daedalus_wire::{LogLevel, WireValue};

/// An event a node emitted during its run.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Progress { fraction: f32, message: String },
    Log { level: LogLevel, text: String },
    Detail { text: String },
    FaultCode { code: u32 },
}

/// The context of one node run.
pub struct Context {
    cancel: Arc<AtomicBool>,
    state: Option<WireValue>,
    /// Events the node has reported. Kept behind a cell, so a body reports through `&Ctx`.
    events: RefCell<Vec<Event>>,
}

impl Context {
    pub(crate) fn new(cancel: Arc<AtomicBool>, state: Option<WireValue>) -> Context {
        Context {
            cancel,
            state,
            events: RefCell::new(Vec::new()),
        }
    }

    /// Whether the host asked this run to stop. A node that loops should check it.
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    /// The node's state from its last run, if it keeps one.
    pub fn state(&self) -> Option<&WireValue> {
        self.state.as_ref()
    }

    /// Replaces the node's state. It is kept in the worker for the next run.
    pub fn set_state(&mut self, value: WireValue) {
        self.state = Some(value);
    }

    /// Reports progress, a fraction from 0 to 1 with a message. It goes out before the run's outputs.
    pub fn progress(&self, fraction: f32, message: &str) {
        self.events.borrow_mut().push(Event::Progress {
            fraction,
            message: message.to_owned(),
        });
    }

    /// Logs a line at a level. It goes out before the run's outputs.
    pub fn log(&self, level: LogLevel, text: &str) {
        self.events.borrow_mut().push(Event::Log {
            level,
            text: text.to_owned(),
        });
    }

    /// Gives the host more about a failure than the fault's reason holds: the lines a check
    /// found, a tool's output. Call it any number of times before returning the error, and the
    /// pieces are kept in order with the step's fault. What a node writes and then doesn't fail
    /// is dropped by the host.
    pub fn detail(&self, text: &str) {
        self.events.borrow_mut().push(Event::Detail {
            text: text.to_owned(),
        });
    }

    /// Gives this failure a number of the node's own, for a host that acts on which failure it
    /// was without reading the words. Call it before returning the error. The number means
    /// what this node's author says: the host passes it through untouched, with the node's id.
    pub fn fault_code(&self, code: u32) {
        self.events.borrow_mut().push(Event::FaultCode { code });
    }

    pub(crate) fn take(self) -> (Option<WireValue>, Vec<Event>) {
        (self.state, self.events.into_inner())
    }
}

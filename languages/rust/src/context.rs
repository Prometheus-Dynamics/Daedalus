//! What a running node can see and report: whether the host asked it to stop, its state between
//! runs, and progress and log events, which go out in order before the run's outputs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use daedalus_wire::{LogLevel, WireValue};

/// An event a node emitted during its run.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Progress { fraction: f32, message: String },
    Log { level: LogLevel, text: String },
}

/// The context of one node run.
pub struct Context {
    cancel: Arc<AtomicBool>,
    state: Option<WireValue>,
    events: Vec<Event>,
}

impl Context {
    pub(crate) fn new(cancel: Arc<AtomicBool>, state: Option<WireValue>) -> Context {
        Context {
            cancel,
            state,
            events: Vec::new(),
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

    pub fn progress(&mut self, fraction: f32, message: &str) {
        self.events.push(Event::Progress {
            fraction,
            message: message.to_owned(),
        });
    }

    pub fn log(&mut self, level: LogLevel, text: &str) {
        self.events.push(Event::Log {
            level,
            text: text.to_owned(),
        });
    }

    pub(crate) fn take(self) -> (Option<WireValue>, Vec<Event>) {
        (self.state, self.events)
    }
}

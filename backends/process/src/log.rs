//! What a worker says with a log message: counted, kept with its stderr for a fault report, and
//! handed to the host's handler when it set one.

use std::path::Path;
use std::sync::{Arc, RwLock};

use daedalus_wire::LogLevel;

/// One log line from a worker.
#[derive(Clone, Copy, Debug)]
pub struct WorkerLog<'a> {
    /// The worker's program, as its spec names it.
    pub program: &'a Path,
    /// The worker's process, which changes when it is started again.
    pub pid: u32,
    pub level: LogLevel,
    pub text: &'a str,
}

type Handler = Arc<dyn Fn(&WorkerLog<'_>) + Send + Sync>;

/// The host's handler for worker log lines, shared by the backend and every worker of it.
#[derive(Default)]
pub(crate) struct LogSink {
    handler: RwLock<Option<Handler>>,
}

impl LogSink {
    pub(crate) fn set(&self, handler: Option<Handler>) {
        match self.handler.write() {
            Ok(mut held) => *held = handler,
            Err(poisoned) => *poisoned.into_inner() = handler,
        }
    }

    pub(crate) fn emit(&self, line: &WorkerLog<'_>) {
        let handler = match self.handler.read() {
            Ok(held) => held.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        if let Some(handler) = handler {
            handler(line);
        }
    }
}

/// A level's name, as it is written in a fault report.
pub(crate) fn name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    }
}

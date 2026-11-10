//! The daemon's log: one line per event on stderr, which a service manager collects. The level
//! comes from the config.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};

/// How much is logged. Each level includes the ones before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    pub const NAMES: [&str; 4] = ["error", "warn", "info", "debug"];

    pub fn parse(name: &str) -> Option<Level> {
        match name {
            "error" => Some(Level::Error),
            "warn" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        Level::NAMES[self as usize]
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

pub fn set_level(level: Level) {
    LEVEL.store(level as u8, Ordering::Relaxed);
}

/// Writes one line, if `level` is within the configured one. A log line that cannot be written
/// is dropped: the daemon does not stop for it.
pub fn log(level: Level, message: fmt::Arguments<'_>) {
    if level as u8 > LEVEL.load(Ordering::Relaxed) {
        return;
    }
    let line = format!("daedalusd: {}: {message}\n", level.name());
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

macro_rules! error {
    ($($arg:tt)*) => { $crate::logging::log($crate::logging::Level::Error, format_args!($($arg)*)) };
}
macro_rules! warning {
    ($($arg:tt)*) => { $crate::logging::log($crate::logging::Level::Warn, format_args!($($arg)*)) };
}
macro_rules! info {
    ($($arg:tt)*) => { $crate::logging::log($crate::logging::Level::Info, format_args!($($arg)*)) };
}
macro_rules! debug {
    ($($arg:tt)*) => { $crate::logging::log($crate::logging::Level::Debug, format_args!($($arg)*)) };
}
pub(crate) use {debug, error, info, warning};

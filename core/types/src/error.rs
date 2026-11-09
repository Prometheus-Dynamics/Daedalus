//! Errors with no heap. Formatted messages are the planner's job.

use core::fmt;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    InvalidId,
    InvalidKey,
    InvalidLiteral,
    OutOfRange,
    Mismatch,
    Capacity,
    Unsupported,
    Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: Code,
    pub detail: &'static str,
}

impl Error {
    pub const fn new(code: Code, detail: &'static str) -> Error {
        Error { code, detail }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.detail)
    }
}

impl core::error::Error for Error {}

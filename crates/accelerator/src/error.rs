use std::time::Duration;

/// Why an accelerator call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AcceleratorError {
    /// The accelerator cannot do this (a program format, dmabuf import, a sync-fd wait).
    Unsupported { what: String },
    /// A handle made by another accelerator instance.
    ForeignHandle,
    /// The program image was rejected.
    Program { reason: String },
    /// A binding does not fit the program's slots.
    Binding { slot: u32, reason: String },
    /// A read past the end of a buffer.
    OutOfRange {
        offset: usize,
        len: usize,
        buffer: usize,
    },
    /// A fence did not signal in time.
    Timeout { after: Duration },
    /// The device or its driver failed.
    Device { reason: String },
}

impl AcceleratorError {
    pub fn unsupported(what: impl Into<String>) -> Self {
        Self::Unsupported { what: what.into() }
    }

    pub fn device(reason: impl Into<String>) -> Self {
        Self::Device {
            reason: reason.into(),
        }
    }
}

impl core::fmt::Display for AcceleratorError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unsupported { what } => write!(f, "accelerator does not support {what}"),
            Self::ForeignHandle => f.write_str("handle belongs to another accelerator"),
            Self::Program { reason } => write!(f, "program rejected: {reason}"),
            Self::Binding { slot, reason } => write!(f, "binding for slot {slot}: {reason}"),
            Self::OutOfRange {
                offset,
                len,
                buffer,
            } => write!(
                f,
                "{len} bytes at {offset} are outside a {buffer}-byte buffer"
            ),
            Self::Timeout { after } => write!(f, "fence not signaled after {after:?}"),
            Self::Device { reason } => write!(f, "accelerator failed: {reason}"),
        }
    }
}

impl std::error::Error for AcceleratorError {}

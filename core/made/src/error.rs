//! Why a made plan's bytes were refused.

use std::fmt;

use daedalus_wire::WireError;

/// A made plan that doesn't read. Nothing is loaded from it.
#[derive(Debug)]
#[non_exhaustive]
pub enum MadeError {
    /// The bytes don't start with a made plan's magic.
    NotAPlan,
    /// A format number this build doesn't read.
    Format(u16),
    /// The body doesn't match its hash: the bytes were changed or cut short.
    Corrupt,
    /// A field is cut short, out of range or not one this format defines.
    Wire(WireError),
}

impl fmt::Display for MadeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MadeError::NotAPlan => write!(f, "these bytes are not a made plan"),
            MadeError::Format(format) => {
                write!(f, "made plan format {format} is not one this build reads")
            }
            MadeError::Corrupt => write!(f, "the made plan's bytes do not match their hash"),
            MadeError::Wire(error) => write!(f, "the made plan is malformed: {error:?}"),
        }
    }
}

impl std::error::Error for MadeError {}

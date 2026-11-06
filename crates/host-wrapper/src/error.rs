//! Typed errors of the wrapper.

use std::fmt;
use std::path::PathBuf;

use lemnos_abi::{AbiVersion, Status};

/// Why a call into the surface failed.
#[derive(Debug)]
pub enum HostError {
    /// The library could not be opened (not found, not a shared object, or its initializers
    /// failed).
    Load {
        path: PathBuf,
        source: libloading::Error,
    },
    /// The library lacks a symbol this crate calls: a surface of another version.
    MissingSymbol(&'static str),
    /// The surface's handshake does not accept this crate's version (major, minor or layout).
    VersionMismatch {
        found: AbiVersion,
        wanted: AbiVersion,
    },
    /// The surface returned a non-success status. `message` is what the surface wrote to its
    /// message sink, when it wrote one.
    Call {
        call: &'static str,
        status: Status,
        message: String,
    },
    /// A port name or a document was not UTF-8 text (checked before the surface is called).
    NotUtf8(&'static str),
    /// A frame was built with a layout the surface cannot carry (checked before the call).
    InvalidFrame(&'static str),
}

impl HostError {
    /// The status of a [`HostError::Call`].
    pub fn status(&self) -> Option<Status> {
        match self {
            Self::Call { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load { path, source } => {
                write!(
                    f,
                    "cannot load the surface library {}: {source}",
                    path.display()
                )
            }
            Self::MissingSymbol(name) => {
                write!(f, "the surface library has no `{name}` symbol")
            }
            Self::VersionMismatch { found, wanted } => write!(
                f,
                "surface ABI mismatch: the library is {found:?}, this crate needs {wanted:?}"
            ),
            Self::Call {
                call,
                status,
                message,
            } => {
                write!(f, "{call} failed with status {}", status.0)?;
                if !message.is_empty() {
                    write!(f, ": {message}")?;
                }
                Ok(())
            }
            Self::NotUtf8(what) => write!(f, "{what} is not valid UTF-8"),
            Self::InvalidFrame(why) => write!(f, "invalid frame: {why}"),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Load { source, .. } => Some(source),
            _ => None,
        }
    }
}

//! Why a worker could not be registered.

use std::fmt;
use std::path::PathBuf;

use daedalus_wire::WireError;

#[derive(Debug)]
pub enum ProcessError {
    /// The program would not start.
    Spawn { program: PathBuf, message: String },
    /// The worker did not send its hello in time, or sent something else.
    Startup { program: PathBuf, reason: String },
    /// The hello names another protocol version or layout hash.
    Hello { program: PathBuf, error: WireError },
    /// A node the worker declares does not match the registry, or is already served.
    Declaration {
        program: PathBuf,
        node: String,
        reason: String,
    },
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessError::Spawn { program, message } => {
                write!(f, "{}: could not start: {message}", program.display())
            }
            ProcessError::Startup { program, reason } => {
                write!(f, "{}: {reason}", program.display())
            }
            ProcessError::Hello { program, error } => {
                write!(f, "{}: {error}", program.display())
            }
            ProcessError::Declaration {
                program,
                node,
                reason,
            } => write!(f, "{} serving node {node}: {reason}", program.display()),
        }
    }
}

impl std::error::Error for ProcessError {}

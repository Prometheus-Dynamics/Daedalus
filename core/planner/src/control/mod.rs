//! `if`, `for_each` and `until`: nodes that hold body graphs.

mod kinds;
mod lower;

use std::fmt;

pub use kinds::{BODY, ELSE, MAX_LIMIT, THEN, declare};
pub use lower::{CONTROL_BACKEND, ControlBackend, NestedBody};

use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControlError {
    BodyMissing {
        node: String,
        body: String,
    },
    /// An edge in a body names something outside the body.
    BodyEdge {
        node: String,
        body: String,
        port: String,
    },
    BodyInvalid {
        node: String,
        body: String,
        errors: Vec<SolveError>,
    },
    /// A body lacks a port its operator needs, or has no way to take its ports.
    BodyShape {
        node: String,
        body: String,
        detail: String,
    },
    TypeMismatch {
        node: String,
        port: String,
        detail: String,
    },
    MaxInvalid {
        node: String,
        value: Option<u32>,
    },
    /// A unit in a body can wait on a fence. Nothing in a control step can wait yet.
    BodyWaits {
        node: String,
        body: String,
    },
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlError::BodyMissing { node, body } => {
                write!(f, "control node `{node}` has no body `{body}`")
            }
            ControlError::BodyEdge { node, body, port } => write!(
                f,
                "body `{body}` of control node `{node}` reaches outside the body at port `{port}`"
            ),
            ControlError::BodyInvalid { node, body, errors } => {
                write!(f, "body `{body}` of control node `{node}` is invalid:")?;
                errors.iter().try_for_each(|error| write!(f, " {error};"))
            }
            ControlError::BodyShape { node, body, detail } => {
                write!(f, "body `{body}` of control node `{node}`: {detail}")
            }
            ControlError::TypeMismatch { node, port, detail } => {
                write!(f, "control node `{node}` port `{port}`: {detail}")
            }
            ControlError::MaxInvalid { node, value } => write!(
                f,
                "control node `{node}` needs a max between 1 and {MAX_LIMIT}, and has {value:?}"
            ),
            ControlError::BodyWaits { node, body } => {
                write!(
                    f,
                    "body `{body}` of control node `{node}` has a unit that can wait"
                )
            }
        }
    }
}

impl std::error::Error for ControlError {}

//! What can go wrong joining records. Every error names the id and both places.

use std::fmt;

use daedalus_types::TypeKey;

use crate::SourceAt;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegError {
    /// Two records for one id disagree. `port` is the first that differs, if a port does.
    DeclConflict {
        id: String,
        first: SourceAt,
        second: SourceAt,
        port: Option<&'static str>,
    },
    DuplicateBody {
        id: String,
        backend: &'static str,
        first: SourceAt,
        second: SourceAt,
    },
    /// A body names no backend.
    UnknownBackend {
        id: String,
        at: SourceAt,
    },
    TypeConflict {
        key: TypeKey,
        first: SourceAt,
        second: SourceAt,
    },
    InvalidId {
        id: String,
        at: SourceAt,
    },
}

impl fmt::Display for RegError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegError::DeclConflict {
                id,
                first,
                second,
                port,
            } => {
                write!(
                    f,
                    "node `{id}` is declared differently at {} and {}",
                    first.0, second.0
                )?;
                match port {
                    Some(port) => write!(f, ": port `{port}` differs"),
                    None => Ok(()),
                }
            }
            RegError::DuplicateBody {
                id,
                backend,
                first,
                second,
            } => write!(
                f,
                "node `{id}` has two `{backend}` bodies, at {} and {}",
                first.0, second.0
            ),
            RegError::UnknownBackend { id, at } => {
                write!(f, "node `{id}` at {} names no backend", at.0)
            }
            RegError::TypeConflict { key, first, second } => write!(
                f,
                "type `{}` is defined differently at {} and {}",
                key.as_str(),
                first.0,
                second.0
            ),
            RegError::InvalidId { id, at } => write!(f, "invalid node id `{id}` at {}", at.0),
        }
    }
}

impl std::error::Error for RegError {}

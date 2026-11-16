//! Joins compiled regions into one list of steps over numbered slots.

mod fences;
mod layout;
mod slots;
pub mod transfers;

use std::fmt;

use daedalus_made::Stitched;

use crate::regions::Regions;
use crate::solve::type_table::TypeTable;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StitchError {
    NoTransfer {
        /// The input that can't be reached.
        port: String,
        /// The output the value comes from.
        from_port: String,
        from_space: String,
        to_space: String,
    },
    /// A backend with nodes on it declares no memory.
    NoMemory { backend: String },
    /// A node's body puts a port in a memory space no backend in the plan declares.
    UnknownSpace { port: String, space: String },
}

impl fmt::Display for StitchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StitchError::NoTransfer {
                port,
                from_port,
                from_space,
                to_space,
            } => {
                write!(
                    f,
                    "`{from_port}` to `{port}`: nothing moves a value from `{from_space}` to `{to_space}`"
                )
            }
            StitchError::NoMemory { backend } => {
                write!(f, "backend `{backend}` runs nodes but declares no memory")
            }
            StitchError::UnknownSpace { port, space } => {
                write!(
                    f,
                    "`{port}` is declared in memory `{space}`, which no backend in the plan supplies"
                )
            }
        }
    }
}

impl std::error::Error for StitchError {}

/// Writes the final list for compiled regions.
pub fn stitch(regions: &Regions, types: &TypeTable) -> Stitched {
    layout::lay_out(regions, types)
}

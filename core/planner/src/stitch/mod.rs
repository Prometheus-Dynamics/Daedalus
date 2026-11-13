//! Joins compiled regions into one list of steps over numbered slots.

mod fences;
mod layout;
mod slots;
pub mod transfers;

use std::fmt;

use daedalus_plan::PlanData;

use crate::regions::Regions;
use crate::solve::type_table::TypeTable;

/// A compiled unit: its region, and its place in that region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnitId {
    pub region: u32,
    pub unit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Run {
        unit: UnitId,
        covers: Vec<u32>,
        reads: Vec<u32>,
        writes: Vec<u32>,
    },
    Move {
        from_slot: u32,
        to_slot: u32,
        transfer: u32,
    },
    Wait {
        unit: UnitId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotInfo {
    pub space: u32,
    /// `None` when the port's type isn't in the table.
    pub ty: Option<u32>,
    /// How many items read it. One means ownership moves with no reference count.
    pub consumers: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferUse {
    /// Index into `Routing::table`.
    pub table: usize,
    pub id: String,
}

/// A host bridge's port and the slot behind it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPort {
    pub node: u32,
    pub port: String,
    pub slot: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stitched {
    pub layout: Vec<Item>,
    pub slots: Vec<SlotInfo>,
    pub spaces: Vec<String>,
    pub transfers: Vec<TransferUse>,
    pub host_inputs: Vec<HostPort>,
    pub host_outputs: Vec<HostPort>,
    /// The same list in the stored format.
    pub plan: PlanData,
}

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

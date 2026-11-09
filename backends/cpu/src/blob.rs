//! The blob form: a region stored as data, naming each node by its place in the firmware's library,
//! and the small runner that loads and runs one with no heap.

mod library;
mod load;
#[cfg(feature = "receive")]
pub mod receive;
mod runner;
#[cfg(feature = "std")]
mod write;

pub use library::{Library, ScalarKind, name_hash};
pub use load::LoadError;
pub use runner::{Interpreter, RunError};
#[cfg(feature = "std")]
pub use write::{
    BlobBackend, ConstDesc, EdgeDesc, HostDesc, ParamDesc, PlanDesc, Source, StepDesc, encode,
};

/// Most inputs or outputs a node may have.
pub const MAX_PORTS: usize = 8;
/// Most nodes in a library, or a plan.
pub const MAX_NODES: usize = 256;

/// A read that names constant `index` rather than an edge.
const CONST_BIT: u32 = 1 << 31;
/// How many bits of a write hold the edge. The output port is above them.
const PORT_SHIFT: u32 = 24;
const EDGE_MASK: u32 = (1 << PORT_SHIFT) - 1;

/// What a full queue does with a new value.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    DropOldest = 0,
    DropNewest = 1,
    Error = 2,
}

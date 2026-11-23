//! The made plan: what a host reads from a plan (its nodes, edges, types and stitched layout), and
//! the request each region was compiled from. A host can load one without the planner.

#![forbid(unsafe_code)]

pub mod bind;
pub mod decode;
pub mod encode;
pub mod error;
pub mod facts;
pub mod plan;
pub mod recipe;
pub mod stitched;
#[cfg(test)]
mod stored_outputs;
#[cfg(test)]
mod stored_policies;

pub use bind::{BindError, Bound, bind};
pub use decode::decode;
pub use encode::encode;
pub use error::MadeError;
pub use facts::{MadeEdge, MadeNode, PlanFacts};
pub use plan::{MadeMove, MadePlan, MadeRegion, declaration};
pub use stitched::{HostPort, Item, SlotInfo, Stitched, TransferUse, UnitId};

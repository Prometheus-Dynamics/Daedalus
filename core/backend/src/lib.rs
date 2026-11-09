//! The contract between Daedalus and a backend. A backend answers four questions: what memory it
//! has, how data gets in and out, which offered nodes it could run, and what it makes of the
//! nodes it was given. This runs while planning, so it may allocate.

#![forbid(unsafe_code)]

mod compiled;
mod memory;
mod offer;
mod region;

pub use compiled::{Compiled, GeneratedCode, Refusal, StepUnit, UnitInfo};
pub use memory::{MemorySpace, SpaceKind, Transfer, TransferCost, find_transfer};
pub use offer::{BodyIndex, NodeRef, Offer, OfferNode, PortInfo};
pub use region::{
    CompileCx, ConstOf, HostBinding, Incoming, InnerEdge, NodeBody, PortRef, RateId, Region,
};

/// Anything that can hold values or run nodes. A backend with no nodes is valid: `can_take`
/// returns nothing, and it supplies only memory and transfers.
pub trait Backend: Send + Sync {
    fn id(&self) -> &str;

    /// Changes whenever what this backend takes or compiles may change, on this machine. A cached
    /// plan is only reused under the same version.
    fn version(&self) -> u64 {
        0
    }

    /// The memory this backend has. The first entry is where its outputs live.
    fn memory(&self) -> Vec<MemorySpace>;

    /// How data can get in and out.
    fn transfers(&self) -> Vec<Transfer>;

    /// Which of the offered nodes this backend could run on this machine.
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef>;

    /// Turns the nodes it was given into something runnable. It may merge them.
    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal>;
}

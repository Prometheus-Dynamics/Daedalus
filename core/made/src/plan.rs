//! A made plan: the facts, and for each region the request it was compiled from and the units the
//! backend answered with. Binding one on a host asks the same requests again.

use daedalus_backend::{Backend, UnitInfo};
use daedalus_types::{TypeKey, fnv1a64};

use crate::facts::PlanFacts;
use crate::recipe::RegionRecipe;

/// One region of a made plan.
#[derive(Clone, Debug, PartialEq)]
pub struct MadeRegion {
    /// The backend's place in `MadePlan::backends`.
    pub backend: u8,
    pub recipe: RegionRecipe,
    /// The units the backend returned when the planner compiled the region.
    pub units: Vec<UnitInfo>,
    /// What the backend declared then: its memory, offers and placements.
    pub decl_hash: u64,
}

/// A move between backends, as the plan makes it.
#[derive(Clone, Debug, PartialEq)]
pub enum MadeMove {
    /// A memory handler the backend itself declares: the `index`th of its transfers.
    Backend {
        id: String,
        backend: String,
        index: u32,
    },
    /// A `#[transfer]` a node body wrote, found in the registry by its key and spaces.
    Record {
        id: String,
        /// The backend that wrote the handler's body.
        backend: String,
        key: TypeKey,
        from: String,
        to: String,
    },
}

/// A plan as a host can load it.
#[derive(Clone, Debug, PartialEq)]
pub struct MadePlan {
    pub facts: PlanFacts,
    /// The backends the plan's regions name, in the planner's priority order.
    pub backends: Vec<String>,
    pub regions: Vec<MadeRegion>,
    /// One per `facts.stitched.transfers`, in the same order.
    pub moves: Vec<MadeMove>,
}

/// What a backend declares that a compiled region depends on: its id, its memory spaces, what it
/// offers and where it places values. A backend that declares something else is another backend.
pub fn declaration(backend: &dyn Backend) -> u64 {
    let mut text = std::format!(
        "{}|{:?}|{:?}|{:?}",
        backend.id(),
        backend.memory(),
        backend.offers(),
        backend.placements()
    );
    // A move is named by its id, and the bind checks only that, so what the move does is part of
    // the declaration too: its spaces, its type and its cost. Its code is not, since it can't be
    // compared across builds.
    for transfer in backend.transfers() {
        text.push_str(&std::format!(
            "|{}>{}>{}>{:?}>{:?}",
            transfer.id,
            transfer.from,
            transfer.to,
            transfer.key.as_ref().map(TypeKey::as_str),
            transfer.cost
        ));
    }
    fnv1a64(text.as_bytes())
}

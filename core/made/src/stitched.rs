//! The final list of steps for a plan, over numbered slots: what each unit reads and writes, where
//! a value moves, and which host ports it binds. The planner writes it; a host reads it.

use daedalus_plan::PlanData;

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
    /// How many items read it. One means ownership moves with no reference count: the reader may
    /// take the slot's own reference. With more, the slot keeps its value for all of them, and a
    /// reader that takes gets a reference of its own.
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

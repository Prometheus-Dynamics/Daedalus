//! A backend that runs nothing and supplies only memory and transfers.

use std::borrow::Cow;

use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost,
};

use crate::steps::make_transfer;
use crate::{DEVICE, HOST, IMPORT, POOL, POOL_TO_HOST};

#[derive(Clone, Copy, Debug, Default)]
pub struct SimMemory;

impl Backend for SimMemory {
    fn id(&self) -> &str {
        "sim-memory"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(POOL),
            kind: SpaceKind::Shared,
            readable: false,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        vec![
            Transfer {
                id: IMPORT,
                key: None,
                from: Cow::Borrowed(POOL),
                to: Cow::Borrowed(DEVICE),
                cost: TransferCost::Import,
                make: make_transfer::<2>,
            },
            Transfer {
                id: POOL_TO_HOST,
                key: None,
                from: Cow::Borrowed(POOL),
                to: Cow::Borrowed(HOST),
                cost: TransferCost::Handoff,
                make: make_transfer::<3>,
            },
        ]
    }

    fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
        Vec::new()
    }

    fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Err(Refusal::whole_region("sim-memory runs no nodes"))
    }
}

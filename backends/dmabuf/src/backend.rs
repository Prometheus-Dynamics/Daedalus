//! The backend: one memory space, two moves, no nodes.

use std::borrow::Cow;
use std::sync::{Arc, Mutex, PoisonError};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost,
};
use daedalus_dma_sys::Access;
use daedalus_runner::{UNSTAMPED, put_typed, take_typed};

use crate::pool::{Block, Pool, Source};

/// The memory space of blocks.
pub const SPACE: &str = "dmabuf";
/// The CPU backend's memory space, which the two moves connect to.
pub const HOST_SPACE: &str = "cpu.host";
/// The move from a block to the CPU.
pub const MAP: &str = "dmabuf.map";
/// The move from the CPU into a block.
pub const COPY: &str = "dmabuf.copy";

/// Where the blocks of `dmabuf.copy` steps come from. A transfer's step is made by a plain
/// function, which can't see the backend it belongs to, so the choice is kept here.
static SOURCE: Mutex<Option<Source>> = Mutex::new(None);

/// The dma-buf memory backend.
#[derive(Clone, Debug)]
pub struct DmabufBackend {
    source: Source,
}

impl DmabufBackend {
    /// A backend whose copies into `dmabuf` allocate from `source`. The latest backend made
    /// decides for the process.
    pub fn new(source: Source) -> DmabufBackend {
        *SOURCE.lock().unwrap_or_else(PoisonError::into_inner) = Some(source.clone());
        DmabufBackend { source }
    }

    pub fn source(&self) -> &Source {
        &self.source
    }
}

impl Default for DmabufBackend {
    /// Blocks from anonymous memory, which needs no device.
    fn default() -> Self {
        DmabufBackend::new(Source::Memfd)
    }
}

impl Backend for DmabufBackend {
    fn id(&self) -> &str {
        "dmabuf"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(SPACE),
            kind: SpaceKind::Shared,
            readable: false,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        vec![
            Transfer {
                id: MAP,
                key: None,
                from: Cow::Borrowed(SPACE),
                to: Cow::Borrowed(HOST_SPACE),
                cost: TransferCost::Import,
                make: make_map,
            },
            Transfer {
                id: COPY,
                key: None,
                from: Cow::Borrowed(HOST_SPACE),
                to: Cow::Borrowed(SPACE),
                cost: TransferCost::Copy,
                make: make_copy,
            },
        ]
    }

    fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
        Vec::new()
    }

    fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Err(Refusal::whole_region("the dmabuf backend runs no nodes"))
    }
}

fn fault(cx: &mut StepCx, reason: &str) -> StepEnd {
    cx.fault(None, reason);
    StepEnd::Fault
}

/// Passes a block on to the CPU side. The block is mapped already, and the reader takes a guard.
struct MapStep;

impl OwnedStep for MapStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(block) = slots.cell(0).and_then(take_typed::<Block>) else {
            return fault(cx, "dmabuf.map was given something that isn't a block");
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "dmabuf.map has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, block);
        StepEnd::Done
    }
}

/// Copies a CPU value's bytes into a block from this step's own pool.
struct CopyStep {
    pool: Pool,
}

impl OwnedStep for CopyStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(input) = slots.cell(0) else {
            return fault(cx, "dmabuf.copy has no input");
        };
        let block: Arc<Block> = match input.bytes() {
            Some(bytes) if !bytes.is_empty() => {
                let Ok(block) = self.pool.take(bytes.len() as u64) else {
                    return fault(cx, "dmabuf.copy couldn't allocate a block");
                };
                let wrote = block
                    .access(Access::Write)
                    .and_then(|guard| guard.write(0, bytes));
                if wrote.is_err() {
                    return fault(cx, "dmabuf.copy couldn't write its block");
                }
                block
            }
            Some(_) => return fault(cx, "dmabuf.copy was given no bytes"),
            // Already a block: nothing to copy.
            None => match take_typed::<Block>(input) {
                Some(block) => block,
                None => return fault(cx, "dmabuf.copy was given a value with no bytes"),
            },
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "dmabuf.copy has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, block);
        StepEnd::Done
    }
}

fn make_map(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(MapStep))
}

fn make_copy(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    let source = SOURCE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .unwrap_or(Source::Memfd);
    Ok(Step::boxed(CopyStep {
        pool: Pool::new(source),
    }))
}

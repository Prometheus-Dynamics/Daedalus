//! The space, and the moves between it and the host.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::os::fd::BorrowedFd;
use std::sync::{Mutex, PoisonError};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost,
};
use daedalus_runner::{UNSTAMPED, put_typed, share_typed, typed_view};
use daedalus_types::Typed;

use crate::{HOST_SPACE, SPACE};

/// A type whose values are a dma-buf. The dma-buf holds the value's bytes.
pub trait DmaBufBacked: Typed + Send + Sync + 'static {
    /// The dma-buf that holds the value, and the value's length in it. A dma-buf can be longer
    /// than its value: a dma-heap allocation is at least a page. `None` when this value has none,
    /// as on a machine with no dma-heap: such a value stays on the host.
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)>;
}

/// A string kept for the life of the process, once for each distinct text.
fn intern(text: String) -> &'static str {
    static KEPT: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    let mut kept = KEPT.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(found) = kept.iter().find(|have| **have == text) {
        return found;
    }
    let leaked: &'static str = Box::leak(text.into_boxed_str());
    kept.push(leaked);
    leaked
}

/// The id of the move that takes a `T` from the dma-buf space to the host.
pub fn to_host_id<T: Typed>() -> &'static str {
    intern(format!("dmabuf.to_host:{}", T::KEY.as_str()))
}

/// The id of the move that takes a `T` from the host to the dma-buf space.
pub fn from_host_id<T: Typed>() -> &'static str {
    intern(format!("dmabuf.from_host:{}", T::KEY.as_str()))
}

/// Hands a `T` on from one space to the other. The value is the same in both: nothing is copied,
/// and the output is another reference to it.
struct Hand<T> {
    /// Whether the value must have a dma-buf to be handed on: true on the way into the space.
    needs_dmabuf: bool,
    of: PhantomData<fn() -> T>,
}

fn fault(cx: &mut StepCx, reason: &str) -> StepEnd {
    cx.fault(None, reason);
    StepEnd::Fault
}

impl<T: DmaBufBacked> OwnedStep for Hand<T> {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(input) = slots.cell(0) else {
            return fault(cx, "a dma-buf move has no input");
        };
        let Some(value) = typed_view::<T>(input) else {
            return fault(cx, "a dma-buf move was given a value of another type");
        };
        if self.needs_dmabuf && value.dmabuf().is_none() {
            return fault(
                cx,
                "a value with no dma-buf can't be moved into dma-buf memory",
            );
        }
        // Another reference to the same value. The cell keeps its own, which the runner lets go
        // of: a value with other readers is still theirs to read.
        let Some(value) = share_typed(input).and_then(|shared| shared.downcast::<T>().ok()) else {
            return fault(cx, "a dma-buf move couldn't share its value");
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "a dma-buf move has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, value);
        StepEnd::Done
    }
}

fn to_host<T: DmaBufBacked>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Hand::<T> {
        needs_dmabuf: false,
        of: PhantomData,
    }))
}

fn from_host<T: DmaBufBacked>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Hand::<T> {
        needs_dmabuf: true,
        of: PhantomData,
    }))
}

/// The backend that supplies the dma-buf space. It runs no nodes: it lists the space, and for
/// each type registered with [`DmaBufMemory::with`] the two moves between the space and the host.
#[derive(Clone, Debug, Default)]
pub struct DmaBufMemory {
    transfers: Vec<Transfer>,
}

impl DmaBufMemory {
    pub fn new() -> DmaBufMemory {
        DmaBufMemory::default()
    }

    /// Lists `T` as a type whose values can be in the dma-buf space. A `T` is handed between the
    /// space and the host as it is, with no copy. A `T` with no dma-buf faults on the way in.
    #[must_use]
    pub fn with<T: DmaBufBacked>(mut self) -> DmaBufMemory {
        let moves = [
            Transfer {
                id: to_host_id::<T>(),
                key: Some(T::KEY),
                from: Cow::Borrowed(SPACE),
                to: Cow::Borrowed(HOST_SPACE),
                cost: TransferCost::Handoff,
                make: to_host::<T>,
            },
            Transfer {
                id: from_host_id::<T>(),
                key: Some(T::KEY),
                from: Cow::Borrowed(HOST_SPACE),
                to: Cow::Borrowed(SPACE),
                cost: TransferCost::Handoff,
                make: from_host::<T>,
            },
        ];
        for transfer in moves {
            if self.transfers.iter().all(|have| have.id != transfer.id) {
                self.transfers.push(transfer);
            }
        }
        self
    }
}

impl Backend for DmaBufMemory {
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
        self.transfers.clone()
    }

    fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
        Vec::new()
    }

    fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Err(Refusal::whole_region("the dma-buf space runs no nodes"))
    }
}

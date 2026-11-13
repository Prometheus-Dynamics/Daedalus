//! The dma-buf space lists its moves per type, and hands a value on with no copy.
#![cfg(target_os = "linux")]

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, TransferCost};
use daedalus_dmabuf::{DmaBufBacked, DmaBufMemory, HOST_SPACE, SPACE, from_host_id, to_host_id};
use daedalus_runner::{put_typed, typed_view};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};

/// A value that may have a file behind it. Any fd will do: the space never reads it.
struct Block {
    fd: Option<OwnedFd>,
    len: u64,
}

impl Typed for Block {
    const KEY: TypeKey = TypeKey::new("t:block");
    const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("t:block"));
}

impl DmaBufBacked for Block {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        self.fd.as_ref().map(|fd| (fd.as_fd(), self.len))
    }
}

fn block(with_fd: bool) -> Arc<Block> {
    let fd = with_fd.then(|| OwnedFd::from(std::fs::File::open("/dev/null").expect("opens")));
    Arc::new(Block { fd, len: 64 })
}

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
    }
}

/// Runs the move `id` on `value`, and gives how it ended with the two cells.
fn moved(id: &str, value: Arc<Block>) -> (StepEnd, [Slot; 2], String) {
    let memory = DmaBufMemory::new().with::<Block>();
    let transfers = memory.transfers();
    let transfer = transfers.iter().find(|one| one.id == id).expect("listed");
    let mut step = (transfer.make)(&cx()).expect("a step");
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), value);
    let mut context = StepCx::new();
    let end = {
        let mut slots = Slots::of(&mut cells);
        run_once(&mut step, &mut slots, &mut context)
    };
    let reason = context.reason().to_string();
    (end, cells, reason)
}

#[test]
fn the_space_and_a_types_two_moves_are_listed_once() {
    let memory = DmaBufMemory::new().with::<Block>().with::<Block>();
    let spaces = memory.memory();
    assert_eq!(spaces.len(), 1);
    assert_eq!(spaces[0].id, SPACE);
    assert!(!spaces[0].readable, "the space runs no nodes");
    let transfers = memory.transfers();
    assert_eq!(transfers.len(), 2, "listing a type twice adds nothing");
    let out = transfers
        .iter()
        .find(|one| one.id == to_host_id::<Block>())
        .unwrap();
    assert_eq!((&*out.from, &*out.to), (SPACE, HOST_SPACE));
    let back = transfers
        .iter()
        .find(|one| one.id == from_host_id::<Block>())
        .unwrap();
    assert_eq!((&*back.from, &*back.to), (HOST_SPACE, SPACE));
    for transfer in &transfers {
        assert_eq!(transfer.key, Some(Block::KEY));
        assert_eq!(transfer.cost, TransferCost::Handoff);
    }
    assert!(
        DmaBufMemory::new().transfers().is_empty(),
        "no type, no moves"
    );
}

#[test]
fn a_value_is_handed_on_as_it_is() {
    for id in [to_host_id::<Block>(), from_host_id::<Block>()] {
        let value = block(true);
        let same = Arc::as_ptr(&value);
        let (end, cells, reason) = moved(id, value);
        assert!(matches!(end, StepEnd::Done), "{id}: {reason}");
        assert_eq!(
            cells[0].state,
            SlotState::Moved as u32,
            "{id}: the input's reference moved to the output"
        );
        let out = typed_view::<Block>(&cells[1]).expect("a block");
        assert!(std::ptr::eq(out, same), "{id}: the same value, not a copy");
        daedalus_runner::drop_owned(cells[1]);
    }
}

#[test]
fn a_value_with_no_dmabuf_leaves_the_space_and_is_refused_on_the_way_in() {
    let (end, cells, reason) = moved(to_host_id::<Block>(), block(false));
    assert!(matches!(end, StepEnd::Done), "{reason}");
    daedalus_runner::drop_owned(cells[1]);
    daedalus_runner::drop_owned(cells[0]);

    let (end, cells, reason) = moved(from_host_id::<Block>(), block(false));
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("no dma-buf"), "{reason}");
    assert_eq!(
        cells[1].state,
        SlotState::Empty as u32,
        "nothing was written"
    );
    daedalus_runner::drop_owned(cells[0]);
}

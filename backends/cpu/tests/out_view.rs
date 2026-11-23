//! A CPU node writes an output in place through an `OutView`: in memory a reader lends when it has
//! some, with no copy and no allocation, and in memory the step keeps otherwise. A write past the
//! room reserved, or a record that writes another length than its size pass said, fails the step
//! with words, and a failed tick's lease is given back.

mod common;

use std::sync::Arc;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{
    HeldRegions, Lease, Lend, OWNER_HANDED, Regions, Slot, SlotState, StepCx, StepEnd,
    WritableRegions,
};
use daedalus_backend::{LentRoom, PortRef};
use daedalus_cpu::steps::{CpuBody, Keep, Setup};
use daedalus_cpu_author::{OutView, Refused};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{PortIndex, WireForm};

use common::{NODE, call, inline};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

#[node(id = "view.fill", inputs(port(name = "n")))]
fn fill(n: u32, mut out: OutView<'_, f32>) -> Result<(), Refused> {
    for (at, item) in out.items_mut(n as usize)?.iter_mut().enumerate() {
        *item = at as f32;
    }
    Ok(())
}

/// Reserves room for `n` items, then writes one more.
#[node(id = "view.over", inputs(port(name = "n")))]
fn over(n: u32, mut out: OutView<'_, u32>) -> Result<(), Refused> {
    out.reserve(n as usize)?;
    for at in 0..=n {
        out.push(&at)?;
    }
    Ok(())
}

/// Reserves room for `n` items, then writes one more in a single `extend`.
#[node(id = "view.spill", inputs(port(name = "n")))]
fn spill(n: u32, mut out: OutView<'_, u32>) -> Result<(), Refused> {
    out.reserve(n as usize)?;
    out.extend(0..=n)
}

/// Writes the items, ignoring a refusal: the step fails with it anyway.
#[node(id = "view.careless", inputs(port(name = "n")))]
fn careless(n: u32, mut out: OutView<'_, u32>) {
    let _ = out.reserve(n as usize);
    for at in 0..=n {
        let _ = out.push(&at);
    }
}

/// A record whose write is longer than its size pass says.
#[derive(Clone, Copy, Debug, PartialEq, daedalus_derive::Type)]
#[type_key("test:liar")]
struct Liar;

impl WireForm for Liar {
    fn wire_len(&self) -> usize {
        2
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        out[..3].fill(7);
        3
    }
    fn read_wire(bytes: &[u8]) -> Option<(Liar, usize)> {
        bytes.get(..2).map(|_| (Liar, 2))
    }
}

#[node(id = "view.liar", inputs(port(name = "n")))]
fn liar(n: u32, mut out: OutView<'_, Liar>) -> Result<(), Refused> {
    out.reserve_bytes(n as usize * 4)?;
    out.push(&Liar)
}

/// Reserves room for `n` items, then for ten times as many: a node that learns its size late.
#[node(id = "view.regrow", inputs(port(name = "n")))]
fn regrow(n: u32, mut out: OutView<'_, f32>) -> Result<(), Refused> {
    out.reserve(n as usize)?;
    out.reserve(n as usize * 10)?;
    out.items_mut(n as usize * 10).map(|_| ())
}

struct Boxes(Vec<Box<[u8]>>);

impl Regions for Boxes {
    fn region(&self, index: usize) -> Option<&[u8]> {
        self.0.get(index).map(|region| &region[..])
    }
}

impl WritableRegions for Boxes {
    fn region_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        self.0.get_mut(index).map(|region| &mut region[..])
    }
}

/// Lent memory, as a worker's would be.
struct Lent(Arc<HeldRegions<Boxes>>);

impl Lend for Lent {
    fn lease(&self, len: usize, items_at: usize) -> Option<Lease> {
        self.0.lease(len, items_at)
    }
    fn threshold(&self) -> usize {
        64
    }
}

fn lent(size: usize) -> Arc<HeldRegions<Boxes>> {
    let regions = (0..2).map(|_| vec![0u8; size].into_boxed_slice()).collect();
    HeldRegions::writable(Boxes(regions), 2)
}

fn step(record: &NodeRecord, keep: &[Keep], lent: &[LentRoom]) -> daedalus_abi::Step {
    let body = record.body.code.downcast_ref::<CpuBody>().unwrap();
    let setup = Setup {
        node: NODE,
        inputs: 1,
        consts: &[],
        keep,
        wire_inputs: 0,
        id: record.decl.id,
        lent,
        readers: None,
    };
    (body.make)(&setup).unwrap()
}

fn room(held: &Arc<HeldRegions<Boxes>>) -> Vec<LentRoom> {
    vec![LentRoom {
        port: PortRef {
            node: NODE,
            port: PortIndex(0),
        },
        room: Arc::new(Lent(held.clone())),
    }]
}

/// Runs one tick with `n` as the input, and gives the output cell.
fn tick(step: &mut daedalus_abi::Step, n: u32, out: &mut Slot, cx: &mut StepCx) -> StepEnd {
    let mut buf = [0; 16];
    let mut cells = [inline(n, &mut buf), *out];
    let end = call(step, &mut cells, cx);
    *out = cells[1];
    end
}

fn list(bytes: &[u8]) -> Vec<f32> {
    let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), 4 + count * 4, "the wire form of a list");
    bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|item| f32::from_le_bytes(*item))
        .collect()
}

fn release(cell: &mut Slot) {
    daedalus_runner::put(cell, Slot::EMPTY);
}

#[test]
fn a_view_in_lent_memory_is_the_value_its_reader_reads_with_no_copy_and_no_allocation() {
    let held = lent(64 * 1024);
    let mut step = step(&FILL_NODE, &[Keep::Wire], &room(&held));
    let mut cx = StepCx::new();
    let mut out = Slot::EMPTY;
    // A tick to warm up: the lease's record is made once.
    assert!(matches!(
        tick(&mut step, 1000, &mut out, &mut cx),
        StepEnd::Done
    ));
    release(&mut out);
    let mut done = 0;
    let ((), allocations) = counted(|| {
        for _ in 0..100 {
            if matches!(tick(&mut step, 1000, &mut out, &mut cx), StepEnd::Done)
                && out.owner == OWNER_HANDED
            {
                done += 1;
            }
            release(&mut out);
        }
    });
    assert_eq!(done, 100, "each tick a hand-over of the lent memory");
    assert_eq!(allocations, 0, "a warm tick allocates nothing");
    assert!(matches!(
        tick(&mut step, 1000, &mut out, &mut cx),
        StepEnd::Done
    ));
    let items = list(out.bytes().expect("bytes"));
    assert_eq!((items.len(), items[999]), (1000, 999.0));
    let shared = daedalus_runner::share_handed(&out).expect("a hand-over");
    let (_, offset) = held.locate(&shared).expect("in the lent memory");
    assert_eq!((offset + 4) % 64, 0, "its items on a cache line");
    drop(shared);
    release(&mut out);
    let counts = held.lease_counts();
    assert_eq!(
        (counts.leased, counts.sealed, counts.abandoned),
        (102, 102, 0)
    );
    assert_eq!(
        held.holders(0) + held.holders(1),
        0,
        "every hold given back"
    );
}

#[test]
fn a_view_with_no_lent_memory_writes_the_wire_form_or_the_list() {
    let mut cx = StepCx::new();
    let mut wire = step(&FILL_NODE, &[Keep::Wire], &[]);
    let mut out = Slot::EMPTY;
    assert!(matches!(
        tick(&mut wire, 100, &mut out, &mut cx),
        StepEnd::Done
    ));
    let expected: Vec<f32> = (0..100).map(|at| at as f32).collect();
    assert_eq!(list(out.bytes().expect("wire bytes")), expected);
    release(&mut out);
    // Below the lent memory's threshold, a value is written as it would be without it.
    let held = lent(4096);
    let mut small = step(&FILL_NODE, &[Keep::Wire], &room(&held));
    assert!(matches!(
        tick(&mut small, 2, &mut out, &mut cx),
        StepEnd::Done
    ));
    assert_eq!(list(out.bytes().expect("inline bytes")), [0.0, 1.0]);
    assert_eq!(held.lease_counts().leased, 0, "too small to lease");
    release(&mut out);
    let mut host = step(&FILL_NODE, &[Keep::Fresh], &[]);
    assert!(matches!(
        tick(&mut host, 100, &mut out, &mut cx),
        StepEnd::Done
    ));
    let value = daedalus_runner::typed_view::<Vec<f32>>(&out).expect("the list");
    assert_eq!(*value, expected);
    release(&mut out);
}

#[test]
fn a_write_past_the_room_fails_the_step_with_words_and_gives_its_lease_back() {
    let held = lent(64 * 1024);
    for record in [&OVER_NODE, &CARELESS_NODE] {
        let mut step = step(record, &[Keep::Wire], &room(&held));
        let mut cx = StepCx::new();
        let mut out = Slot::EMPTY;
        assert!(matches!(
            tick(&mut step, 100, &mut out, &mut cx),
            StepEnd::Fault
        ));
        let reason = cx.reason().to_string();
        assert!(
            reason.contains("wrote 4 bytes past the 400 it reserved"),
            "{reason}"
        );
        assert!(reason.contains(record.decl.id), "{reason}");
        assert_ne!(out.state, SlotState::Full as u32, "nothing is written");
    }
    let counts = held.lease_counts();
    assert_eq!((counts.leased, counts.sealed, counts.abandoned), (2, 0, 2));
    assert_eq!(held.holders(0) + held.holders(1), 0);
}

#[test]
fn a_record_that_writes_another_length_than_its_size_pass_fails_the_step() {
    let mut step = step(&LIAR_NODE, &[Keep::Wire], &[]);
    let mut cx = StepCx::new();
    let mut out = Slot::EMPTY;
    assert!(matches!(
        tick(&mut step, 4, &mut out, &mut cx),
        StepEnd::Fault
    ));
    let reason = cx.reason().to_string();
    assert!(
        reason.contains("wrote an item of 3 bytes where") && reason.contains("said 2"),
        "{reason}"
    );
}

#[test]
fn a_view_grows_in_its_lent_room_when_it_fits_and_is_refused_past_it() {
    let held = lent(4096);
    let mut step = step(&REGROW_NODE, &[Keep::Wire], &room(&held));
    let mut cx = StepCx::new();
    let mut out = Slot::EMPTY;
    // 40 items, then 400: 1604 bytes fit the region, so the room grows in place.
    assert!(matches!(
        tick(&mut step, 40, &mut out, &mut cx),
        StepEnd::Done
    ));
    assert_eq!(out.owner, OWNER_HANDED, "still in the lent memory");
    assert_eq!(list(out.bytes().expect("bytes")).len(), 400);
    release(&mut out);
    // 110 items, then 1100: 4404 bytes are more than the region after where the room starts.
    assert!(matches!(
        tick(&mut step, 110, &mut out, &mut cx),
        StepEnd::Fault
    ));
    let reason = cx.reason().to_string();
    assert!(
        reason.contains("grew to 4404 bytes, past the 444 its lent room holds"),
        "{reason}"
    );
    let counts = held.lease_counts();
    assert_eq!((counts.leased, counts.sealed, counts.abandoned), (2, 1, 1));
    assert_eq!(held.holders(0) + held.holders(1), 0);
}

#[test]
fn a_view_too_large_for_the_lent_memory_is_written_in_the_steps_own() {
    let held = lent(4096);
    let mut step = step(&FILL_NODE, &[Keep::Wire], &room(&held));
    let mut cx = StepCx::new();
    let mut out = Slot::EMPTY;
    assert!(matches!(
        tick(&mut step, 2000, &mut out, &mut cx),
        StepEnd::Done
    ));
    assert_eq!(held.lease_counts().leased, 0, "no region holds 8004 bytes");
    assert_eq!(list(out.bytes().expect("bytes")).len(), 2000);
    release(&mut out);
}

#[test]
fn an_extend_past_the_room_is_refused_wherever_the_view_writes() {
    let held = lent(64 * 1024);
    let lent_room = room(&held);
    let cases: [(&[Keep], &[LentRoom]); 3] = [
        (&[Keep::Wire], &lent_room),
        (&[Keep::Wire], &[]),
        (&[Keep::Fresh], &[]),
    ];
    for (keep, lent) in cases {
        let mut step = step(&SPILL_NODE, keep, lent);
        let mut cx = StepCx::new();
        let mut out = Slot::EMPTY;
        assert!(matches!(
            tick(&mut step, 100, &mut out, &mut cx),
            StepEnd::Fault
        ));
        let reason = cx.reason().to_string();
        assert!(
            reason.contains("wrote 4 bytes past the 400 it reserved"),
            "{keep:?}: {reason}"
        );
        assert_ne!(out.state, SlotState::Full as u32, "nothing is written");
    }
    let counts = held.lease_counts();
    assert_eq!((counts.leased, counts.sealed, counts.abandoned), (1, 0, 1));
    assert_eq!(held.holders(0) + held.holders(1), 0);
}

#[test]
fn a_list_for_cpu_readers_is_refilled_with_no_allocation_once_no_reader_holds_it() {
    let mut cx = StepCx::new();
    let mut host = step(&FILL_NODE, &[Keep::Fresh], &[]);
    let mut out = Slot::EMPTY;
    // Warm: the list is made once, at its longest.
    assert!(matches!(
        tick(&mut host, 1000, &mut out, &mut cx),
        StepEnd::Done
    ));
    let mut done = 0;
    let ((), allocations) = counted(|| {
        for n in [10, 1000, 500, 1000] {
            if matches!(tick(&mut host, n, &mut out, &mut cx), StepEnd::Done) {
                done += 1;
            }
        }
    });
    assert_eq!(done, 4);
    assert_eq!(allocations, 0, "the cell's list is written again each tick");
    let value = daedalus_runner::typed_view::<Vec<f32>>(&out).expect("the list");
    let expected: Vec<f32> = (0..1000).map(|at| at as f32).collect();
    assert_eq!(*value, expected);
    release(&mut out);
}

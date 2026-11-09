//! A step's cells are its own: inputs in order, then outputs in order.

mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{NO_SLOT, OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{SlotKind, SlotSpec, SlotStamp, put_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::Value;
use support::{Fixture, Wiring, cell, tagged};

static BYTES: [u8; 128] = [0; 128];

/// A value `len` bytes long, which a test can take out of a slot and read the length of.
fn sized(len: usize) -> Slot {
    Slot {
        data: BYTES.as_ptr().cast_mut(),
        ..tagged(len)
    }
}

fn fifo() -> SlotSpec {
    SlotSpec {
        cap: 2,
        kind: SlotKind::Fifo,
    }
}

/// Records the length of each input and writes a value of `100 + position` to each output.
struct Record {
    inputs: usize,
    outputs: usize,
    seen: Arc<Mutex<Vec<usize>>>,
}

impl OwnedStep for Record {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let mut seen = self.seen.lock().unwrap();
        for at in 0..self.inputs {
            seen.push(cell(slots, at).len);
        }
        for at in 0..self.outputs {
            *cell(slots, self.inputs + at) = sized(100 + at);
        }
        StepEnd::Done
    }
}

fn record(inputs: usize, outputs: usize, seen: &Arc<Mutex<Vec<usize>>>) -> Step {
    Step::boxed(Record {
        inputs,
        outputs,
        seen: seen.clone(),
    })
}

#[test]
fn inputs_then_outputs_in_the_order_the_plan_lists_them() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[2, 0], &[3, 1]);
    let specs = vec![fifo(); 4];
    let mut fixture = Fixture::new(vec![record(2, 2, &seen)], wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(7)).unwrap();
    runner.push(2, tagged(9)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![9, 7]);
    assert_eq!(
        runner.take_value(3).map(|value| value.raw(0).len),
        Some(100)
    );
    assert_eq!(
        runner.take_value(1).map(|value| value.raw(0).len),
        Some(101)
    );
}

#[test]
fn a_port_with_no_edge_reads_as_empty() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[NO_SLOT, 0], &[]);
    let mut fixture = Fixture::new(vec![record(2, 0, &seen)], wiring, vec![fifo()]);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(5)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0, 5]);
}

struct Counted(Arc<Mutex<u32>>);

impl Drop for Counted {
    fn drop(&mut self) {
        *self.0.lock().unwrap() += 1;
    }
}

/// Writes an owned value to its first output and a tag to its second.
struct Writes(Arc<Mutex<u32>>);

impl OwnedStep for Writes {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let owned = Arc::new(Counted(self.0.clone()));
        put_typed(cell(slots, 0), TypeIndex(0), SpaceIndex(0), owned);
        *cell(slots, 1) = sized(3);
        StepEnd::Done
    }
}

#[test]
fn an_output_with_no_edge_is_released_once() {
    let drops = Arc::new(Mutex::new(0));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[NO_SLOT, 0]);
    let steps = vec![Step::boxed(Writes(drops.clone()))];
    let mut fixture = Fixture::new(steps, wiring, vec![fifo()]);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*drops.lock().unwrap(), 1, "released when the step ended");
    assert_eq!(runner.take_value(0).map(|value| value.raw(0).len), Some(3));
    assert_eq!(*drops.lock().unwrap(), 1, "and never again");
}

#[test]
fn a_published_value_carries_its_slots_type_and_space() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[0]);
    let mut fixture = Fixture::new(vec![record(0, 1, &seen)], wiring, vec![fifo()]);
    let mut runner = fixture.load().unwrap();
    let stamps: &[SlotStamp] = &[SlotStamp {
        ty: TypeIndex(4),
        space: SpaceIndex(2),
    }];
    runner.set_stamps(stamps).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    let value: Value = runner.take_value(0).expect("published");
    let raw = value.raw(0);
    assert_eq!((raw.type_index, raw.space), (4, 2));
}

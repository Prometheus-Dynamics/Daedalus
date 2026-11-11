#![allow(dead_code)] // Each test binary uses part of this module.

use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::ConstOf;
use daedalus_cpu::steps::{CpuBody, Setup};
use daedalus_registry::NodeRecord;
use daedalus_runner::{INLINE_BYTES, read_inline, write_inline};
use daedalus_types::NodeIndex;

pub const NODE: NodeIndex = NodeIndex(3);

pub fn step_with(record: &NodeRecord, inputs: u16, consts: &[ConstOf<'_>]) -> Step {
    let body = record.body.code.downcast_ref::<CpuBody>().unwrap();
    let setup = Setup {
        node: NODE,
        inputs,
        consts,
        keep: &[],
    };
    (body.make)(&setup).unwrap()
}

pub fn step_of(record: &NodeRecord) -> Step {
    step_with(record, record.decl.inputs.len() as u16, &[])
}

pub fn call(step: &mut Step, cells: &mut [Slot], cx: &mut StepCx) -> StepEnd {
    let mut slots = Slots::of(cells);
    run_once(step, &mut slots, cx)
}

/// An inline input. The buffer has to outlive the call.
pub fn inline<T: Copy>(value: T, buf: &mut [u8; INLINE_BYTES]) -> Slot {
    let mut slot = Slot::EMPTY;
    write_inline(&value, buf, &mut slot);
    slot
}

pub fn value<T: Copy>(slot: &Slot) -> T {
    read_inline::<T>(slot).expect("an inline value of this size")
}

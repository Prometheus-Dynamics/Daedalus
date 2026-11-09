//! The fault table: a bounded reason, and no reason inherited from another step.

mod support;

use daedalus_abi::{REASON_CAPACITY, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_runner::{SlotKind, SlotSpec, StepFault};
use support::{Fixture, Wiring};

/// Fails with a reason far past the limit.
struct Long;

impl StepWord for Long {
    fn run(_: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        cx.fault(Some(4), &"x".repeat(REASON_CAPACITY * 2));
        StepEnd::Fault
    }
}

/// Fails without saying why.
struct Silent;

impl StepWord for Silent {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Fault
    }
}

fn specs() -> Vec<SlotSpec> {
    vec![SlotSpec {
        cap: 1,
        kind: SlotKind::Fifo,
    }]
}

#[test]
fn the_reason_is_cut_at_120_bytes_and_a_silent_failure_has_none() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![Step::of::<Long>(0), Step::of::<Silent>(0)];
    let mut fixture = Fixture::new(steps, wiring, specs());
    let mut table = [StepFault::EMPTY; 2];
    let mut runner = fixture.load().unwrap();
    runner.set_fault_log(&mut table).unwrap();
    runner.wake(0);
    runner.wake(1);
    runner.tick().unwrap();

    let long = runner.step_fault(0).unwrap();
    assert_eq!(long.reason().len(), 120);
    assert_eq!(long.at(), Some(4));
    assert_eq!(long.seq, 1);
    assert_eq!(long.tick, 1);

    let silent = runner.step_fault(1).unwrap();
    assert_eq!(silent.reason(), "");
    assert_eq!(silent.at(), None);
}

#[test]
fn without_a_failure_there_is_no_entry() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    let mut fixture = Fixture::new(vec![Step::of::<Long>(0)], wiring, specs());
    let mut table = [StepFault::EMPTY; 1];
    let mut runner = fixture.load().unwrap();
    runner.set_fault_log(&mut table).unwrap();
    assert_eq!(runner.step_fault(0), None);
}

//! What a full edge does to its producer: held back, rejected, or an error.

mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{BackpressureStrategy, RunError, Runner, SlotKind, SlotSpec};
use support::{Fixture, Wiring, cell, tagged};

/// Writes 0, 1, 2 ... as the length of a tagged value.
struct Source {
    next: usize,
    calls: Arc<Mutex<u32>>,
}

impl OwnedStep for Source {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        *self.calls.lock().unwrap() += 1;
        *cell(slots, 0) = tagged(self.next);
        self.next += 1;
        StepEnd::Done
    }
}

struct Sink {
    seen: Arc<Mutex<Vec<usize>>>,
}

impl OwnedStep for Sink {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.seen.lock().unwrap().push(cell(slots, 0).len);
        StepEnd::Done
    }
}

struct Pump {
    calls: Arc<Mutex<u32>>,
    seen: Arc<Mutex<Vec<usize>>>,
    fixture: Fixture,
}

fn pump() -> Pump {
    let calls = Arc::new(Mutex::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[0]);
    wiring.step(&[], &[0], &[]);
    let steps = vec![
        Step::boxed(Source {
            next: 0,
            calls: Arc::clone(&calls),
        }),
        Step::boxed(Sink {
            seen: Arc::clone(&seen),
        }),
    ];
    let specs = vec![SlotSpec {
        cap: 2,
        kind: SlotKind::Fifo,
    }];
    Pump {
        calls,
        seen,
        fixture: Fixture::new(steps, wiring, specs),
    }
}

/// The source runs every tick, and the sink only from tick 3.
fn drive(runner: &mut Runner<'_>, ticks: u32) {
    for tick in 0..ticks {
        runner.wake(0);
        if tick >= 3 {
            runner.wake(1);
        }
        runner.tick().unwrap();
    }
}

#[test]
fn a_blocking_edge_defers_the_producer_instead_of_dropping() {
    let mut pump = pump();
    let mut runner = pump.fixture.load().unwrap();
    runner.set_blocking(0, true).unwrap();
    drive(&mut runner, 6);
    assert_eq!(runner.counters().backpressure_deferrals, 2);
    assert_eq!(runner.counters().dropped, 0);
    assert_eq!(*pump.seen.lock().unwrap(), [0, 1, 2]);
}

#[test]
fn no_step_is_called_while_its_blocking_output_is_full() {
    let mut pump = pump();
    let mut runner = pump.fixture.load().unwrap();
    runner.set_blocking(0, true).unwrap();
    drive(&mut runner, 3);
    assert_eq!(
        *pump.calls.lock().unwrap(),
        2,
        "the third tick found it full"
    );
    assert_eq!(runner.queue(0).unwrap().len, 2);
}

#[test]
fn a_deferred_step_is_tried_again_without_a_new_wake() {
    let mut pump = pump();
    let mut runner = pump.fixture.load().unwrap();
    runner.set_blocking(0, true).unwrap();
    drive(&mut runner, 3);
    runner.wake(1);
    runner.tick().unwrap();
    assert_eq!(runner.counters().backpressure_deferrals, 2);
    runner.tick().unwrap();
    assert_eq!(*pump.calls.lock().unwrap(), 3, "carried, with room now");
}

#[test]
fn a_bounded_queue_rejects_incoming_values_with_an_event() {
    let mut pump = pump();
    let mut runner = pump.fixture.load().unwrap();
    runner
        .set_backpressure(0, BackpressureStrategy::BoundedQueues)
        .unwrap();
    drive(&mut runner, 6);
    assert_eq!(runner.counters().backpressure_deferrals, 0);
    assert_eq!(runner.counters().backpressure_events, 2);
    assert_eq!(runner.rejected(0), Some(2));
    assert_eq!(*pump.calls.lock().unwrap(), 6, "the producer never waits");
    assert_eq!(*pump.seen.lock().unwrap(), [0, 1, 4]);
}

#[test]
fn error_on_overflow_ends_the_tick() {
    let mut pump = pump();
    let mut runner = pump.fixture.load().unwrap();
    runner
        .set_backpressure(0, BackpressureStrategy::ErrorOnOverflow)
        .unwrap();
    for _ in 0..2 {
        runner.wake(0);
        runner.tick().unwrap();
    }
    runner.wake(0);
    assert_eq!(runner.tick(), Err(RunError::Overflow { slot: 0 }));
}

#[test]
fn a_step_with_any_blocked_output_is_held_back() {
    let calls = Arc::new(Mutex::new(0));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[0, 1]);
    let steps = vec![Step::boxed(Source {
        next: 0,
        calls: Arc::clone(&calls),
    })];
    let specs = vec![
        SlotSpec {
            cap: 4,
            kind: SlotKind::Fifo,
        },
        SlotSpec {
            cap: 1,
            kind: SlotKind::Fifo,
        },
    ];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.set_blocking(1, true).unwrap();
    runner
        .feed_value(
            1,
            daedalus_value::Value {
                ty: daedalus_types::TypeIndex(0),
                space: daedalus_types::SpaceIndex(0),
                owner: daedalus_value::Owner::Static(b"x"),
            },
        )
        .unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*calls.lock().unwrap(), 0);
    assert_eq!(runner.counters().backpressure_deferrals, 1);
}

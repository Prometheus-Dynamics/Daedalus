mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{LoadError, RunError, SlotKind, SlotSpec};
use support::{Fixture, Link, Wiring, cell, chain, link_word, tagged};

/// Records the tag of its input, and copies it to an output when it has one.
struct Log {
    seen: Arc<Mutex<Vec<usize>>>,
    input: usize,
    output: Option<usize>,
}

impl OwnedStep for Log {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let value = *cell(slots, self.input);
        self.seen.lock().unwrap().push(value.len);
        if let Some(output) = self.output {
            *cell(slots, output) = value;
        }
        StepEnd::Done
    }
}

/// Fails `remaining` times, then copies its input to its output.
struct Flaky {
    remaining: usize,
    input: usize,
    output: usize,
}

impl OwnedStep for Flaky {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        if self.remaining > 0 {
            self.remaining -= 1;
            cx.fault(None, "flaky");
            return StepEnd::Fault;
        }
        *cell(slots, self.output) = *cell(slots, self.input);
        StepEnd::Done
    }
}

fn fifo(cap: u32) -> SlotSpec {
    SlotSpec {
        cap,
        kind: SlotKind::Fifo,
    }
}

fn log_step(seen: &Arc<Mutex<Vec<usize>>>, input: usize, output: Option<usize>) -> Step {
    Step::boxed(Log {
        seen: seen.clone(),
        input,
        output,
    })
}

#[test]
fn fifo_values_come_out_in_order_over_three_ticks() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[]);
    let mut fixture = Fixture::new(vec![log_step(&seen, 0, None)], wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    for len in 1..=3 {
        runner.push(0, tagged(len)).unwrap();
    }
    for _ in 0..3 {
        runner.wake(0);
        runner.tick().unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(runner.queue(0).unwrap().len, 0);
}

#[test]
fn latest_slot_keeps_its_last_value() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[]);
    let specs = vec![SlotSpec {
        cap: 1,
        kind: SlotKind::Latest,
    }];
    let mut fixture = Fixture::new(vec![log_step(&seen, 0, None)], wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(7)).unwrap();
    for _ in 0..2 {
        runner.wake(0);
        runner.tick().unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), vec![7, 7]);
    assert_eq!(runner.queue(0).unwrap().len, 1);
    assert_eq!(runner.queue(0).unwrap().unconsumed, 0);
}

#[test]
fn idle_tick_calls_no_steps() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    for index in 0..16u32 {
        wiring.step(&[], &[index], &[]);
    }
    let steps = (0..16).map(|i| log_step(&seen, i, None)).collect();
    let specs = (0..16).map(|_| fifo(1)).collect();
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    let report = runner.tick().unwrap();
    assert!(report.idle);
    assert_eq!(report.step_calls, 0);
    assert!(report.word_reads <= 16_usize.div_ceil(32) as u32 + 1);
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn chain_of_sixteen_fed_once_calls_sixteen_steps_and_drains() {
    let (wiring, words) = chain(16);
    let steps = words.into_iter().map(Step::of::<Link>).collect();
    let specs = (0..16).map(|_| fifo(1)).collect();
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 16);
    assert_eq!(report.faults, 0);
    for slot in 0..16 {
        assert_eq!(runner.queue(slot).unwrap().len, 0, "slot {slot} drained");
    }
}

#[test]
fn fault_drops_its_tick_and_wakes_nothing() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[1]);
    wiring.step(&[], &[1], &[]);
    let steps = vec![
        Step::boxed(Flaky {
            remaining: 1,
            input: 0,
            output: 1,
        }),
        log_step(&seen, 1, None),
    ];
    let specs = vec![fifo(2), fifo(2)];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(5)).unwrap();
    runner.wake(0);
    let first = runner.tick().unwrap();
    assert_eq!(first.faults, 1);
    assert_eq!(
        runner.queue(0).unwrap().len,
        0,
        "the faulted tick's input is consumed"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "the consumer was not woken"
    );
    runner.push(0, tagged(6)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![6]);
    assert_eq!(runner.queue(0).unwrap().len, 0);
}

#[test]
fn back_edge_is_rejected_at_load() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[]);
    wiring.step(&[0], &[1], &[]);
    let steps = vec![log_step(&seen, 0, None), log_step(&seen, 1, None)];
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(1), fifo(1)]);
    assert!(matches!(
        fixture.load(),
        Err(LoadError::BackEdge { from: 1, to: 0 })
    ));
}

#[test]
fn output_that_has_no_room_is_an_overflow() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[1]);
    let steps = vec![Step::of::<Link>(link_word(0, Some(1)))];
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(2), fifo(1)]);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.push(0, tagged(2)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    runner.push(0, tagged(3)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick(), Err(RunError::Overflow { slot: 1 }));
}

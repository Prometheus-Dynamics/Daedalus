//! The parallel lanes give the serial tick's results, on seeded random plans and on the invariants
//! ported from the old executor.

#![cfg(feature = "threads")]

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slot, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_runner::{
    Counters, FailurePolicy, Lanes, Pool, Recovery, RunError, Runner, SlotKind, SlotSpec,
    TickReport,
};
use support::{Fixture, Wiring, cell, tagged};

const SEEDS: u64 = 200;
const TICKS: usize = 3;

/// A small deterministic generator, so a failing seed can be replayed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(0x5851_f42d_4c95_7f2d).wrapping_add(1);
        (self.0 >> 33) ^ self.0
    }
    fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit as u64) as usize
    }
}

/// Mixes its inputs with its id, writes the mix to every output, and records it.
struct Mix {
    id: usize,
    inputs: Vec<usize>,
    outputs: Vec<usize>,
    /// Faults when the mix divides by this; zero never.
    fault: usize,
    skip: usize,
    seen: Arc<Vec<AtomicUsize>>,
}

impl OwnedStep for Mix {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let mut mix = self.id.wrapping_mul(31).wrapping_add(7);
        for &input in &self.inputs {
            mix = mix.rotate_left(5) ^ cell(slots, input).len;
        }
        mix &= 0xffff;
        self.seen[self.id].store(mix, Ordering::Relaxed);
        if self.fault != 0 && mix.is_multiple_of(self.fault) {
            return StepEnd::Fault;
        }
        if self.skip != 0 && mix.is_multiple_of(self.skip) {
            return StepEnd::Skipped;
        }
        for &output in &self.outputs {
            *cell(slots, output) = tagged(mix);
        }
        StepEnd::Done
    }
}

/// A random plan: `roots` input slots, and one Fifo slot per edge, each with one consumer.
fn random_plan(seed: u64) -> (Fixture, Vec<u32>, Vec<u32>, Arc<Vec<AtomicUsize>>) {
    let mut rng = Rng(seed.wrapping_mul(2654435761).wrapping_add(11));
    let count = 2 + rng.below(30);
    let seen = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut consumed: Vec<Vec<(u32, usize)>> = vec![Vec::new(); count];
    let mut inputs: Vec<Vec<usize>> = vec![Vec::new(); count];
    let mut specs = Vec::new();
    let mut slot = 0;
    let mut root_slots = Vec::new();
    let mut root_steps = Vec::new();
    #[allow(clippy::needless_range_loop)]
    for step in 0..count {
        let parents = if step == 0 { 0 } else { rng.below(3) };
        for _ in 0..parents {
            let parent = rng.below(step);
            specs.push(SlotSpec {
                cap: 16,
                kind: SlotKind::Fifo,
            });
            consumed[parent].push((step as u32, slot));
            inputs[step].push(slot);
            slot += 1;
        }
        if parents == 0 {
            specs.push(SlotSpec {
                cap: 16,
                kind: SlotKind::Fifo,
            });
            inputs[step].push(slot);
            root_slots.push(slot as u32);
            root_steps.push(step as u32);
            slot += 1;
        }
    }
    let mut wiring = Wiring::default();
    let mut steps = Vec::new();
    let fault = [0, 0, 5, 7][rng.below(4)];
    let skip = [0, 0, 11][rng.below(3)];
    for step in 0..count {
        let wake: Vec<u32> = consumed[step].iter().map(|&(to, _)| to).collect();
        let outputs: Vec<usize> = consumed[step].iter().map(|&(_, slot)| slot).collect();
        let out_slots: Vec<u32> = outputs.iter().map(|&slot| slot as u32).collect();
        let in_slots: Vec<u32> = inputs[step].iter().map(|&slot| slot as u32).collect();
        wiring.step(&wake, &in_slots, &out_slots);
        if rng.below(5) == 0 {
            wiring.set_recovery(step, Recovery::Retry { max: 1 });
        }
        steps.push(Step::boxed(Mix {
            id: step,
            inputs: inputs[step].clone(),
            outputs,
            fault,
            skip,
            seen: Arc::clone(&seen),
        }));
    }
    (
        Fixture::new(steps, wiring, specs),
        root_slots,
        root_steps,
        seen,
    )
}

#[derive(Debug, PartialEq)]
struct Outcome {
    outputs: Vec<usize>,
    reports: Vec<TickReport>,
    counters: Counters,
    health: Vec<(u32, u32)>,
}

fn run_ticks(
    runner: &mut Runner<'_>,
    roots: (&[u32], &[u32]),
    mut tick: impl FnMut(&mut Runner<'_>) -> TickReport,
) -> (Vec<TickReport>, Counters, Vec<(u32, u32)>) {
    let mut reports = Vec::new();
    for round in 0..TICKS {
        for (&slot, &step) in roots.0.iter().zip(roots.1) {
            runner.push(slot, static_of(round, slot)).unwrap();
            runner.wake(step);
        }
        let mut report = tick(runner);
        report.word_reads = 0;
        reports.push(report);
    }
    let health = (0..runner.step_count() as u32)
        .map(|step| {
            let record = runner.health(step).unwrap();
            (record.failures(), record.calls())
        })
        .collect();
    (reports, runner.counters(), health)
}

fn static_of(round: usize, slot: u32) -> Slot {
    tagged(round * 1009 + slot as usize)
}

fn outcome(seed: u64, parallel: bool) -> Outcome {
    let (mut fixture, root_slots, root_steps, seen) = random_plan(seed);
    let mut runner = fixture.load().unwrap();
    runner.set_clock(1);
    let roots = (root_slots.as_slice(), root_steps.as_slice());
    let (reports, counters, health) = if parallel {
        let pool = Arc::new(Pool::new(4));
        let mut lanes = Lanes::new(&runner, pool, &[]).unwrap();
        run_ticks(&mut runner, roots, |runner| {
            runner.tick_parallel(&mut lanes).unwrap()
        })
    } else {
        run_ticks(&mut runner, roots, |runner| runner.tick().unwrap())
    };
    drop(runner);
    Outcome {
        outputs: seen
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .collect(),
        reports,
        counters,
        health,
    }
}

#[test]
fn serial_and_parallel_match_on_random_plans() {
    for seed in 0..SEEDS {
        assert_eq!(outcome(seed, false), outcome(seed, true), "seed {seed}");
    }
}

/// Reads `word & 0xffff` plus one when `word >> 16 != 0` writes that slot.
struct Plus;

impl StepWord for Plus {
    fn run(word: usize, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let input = word & 0xffff;
        let output = word >> 16;
        let value = cell(slots, input).len + 1;
        if output != 0 {
            *cell(slots, output) = tagged(value);
        }
        StepEnd::Done
    }
}

fn plus(input: usize, output: usize) -> Step {
    Step::of::<Plus>(input | (output << 16))
}

/// Faults every time.
struct Fails;

impl StepWord for Fails {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Fault
    }
}

/// Panics every time, like a body with a bug.
struct Panics;

impl StepWord for Panics {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        panic!("body panic");
    }
}

fn parallel<R>(
    fixture: &mut Fixture,
    workers: usize,
    check: impl FnOnce(&mut Runner<'_>, &mut Lanes) -> R,
) -> R {
    let mut runner = fixture.load().unwrap();
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(workers)), &[]).unwrap();
    check(&mut runner, &mut lanes)
}

fn fifo(count: usize) -> Vec<SlotSpec> {
    vec![
        SlotSpec {
            cap: 8,
            kind: SlotKind::Fifo
        };
        count
    ]
}

#[test]
fn chain_runs_end_to_end_in_one_tick() {
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[1]);
    wiring.step(&[2], &[1], &[2]);
    wiring.step(&[], &[2], &[3]);
    let steps = vec![plus(0, 1), plus(1, 2), plus(2, 3)];
    let mut fixture = Fixture::new(steps, wiring, fifo(4));
    parallel(&mut fixture, 3, |runner, lanes| {
        runner.push(0, tagged(10)).unwrap();
        runner.wake(0);
        let report = runner.tick_parallel(lanes).unwrap();
        assert_eq!(report.step_calls, 3);
        assert_eq!(runner.queue(3).unwrap().len, 1);
    });
}

#[test]
fn branch_runs_each_consumer_of_a_shared_slot() {
    let mut wiring = Wiring::default();
    wiring.step(&[1, 2], &[0], &[1]);
    wiring.step(&[], &[1], &[2]);
    wiring.step(&[], &[1], &[3]);
    let steps = vec![plus(0, 1), plus(1, 2), plus(1, 3)];
    let specs = vec![
        SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        },
        SlotSpec {
            cap: 2,
            kind: SlotKind::Latest,
        },
        SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        },
        SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        },
    ];
    let mut fixture = Fixture::new(steps, wiring, specs);
    parallel(&mut fixture, 3, |runner, lanes| {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        let report = runner.tick_parallel(lanes).unwrap();
        assert_eq!(report.step_calls, 3);
        assert_eq!(runner.queue(2).unwrap().len, 1);
        assert_eq!(runner.queue(3).unwrap().len, 1);
    });
}

#[test]
fn bounded_fan_out_reports_overflow_like_the_serial_tick() {
    let build = || {
        let mut wiring = Wiring::default();
        wiring.step(&[1], &[0], &[1]);
        wiring.step(&[], &[], &[]);
        let specs = vec![
            SlotSpec {
                cap: 2,
                kind: SlotKind::Fifo,
            },
            SlotSpec {
                cap: 1,
                kind: SlotKind::Latest,
            },
        ];
        Fixture::new(vec![plus(0, 1), Step::of::<Plus>(0)], wiring, specs)
    };
    let mut serial = build();
    let mut runner = serial.load().unwrap();
    let mut errors = Vec::new();
    for _ in 0..2 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        errors.push(runner.tick().err());
    }
    drop(runner);
    let mut fixture = build();
    parallel(&mut fixture, 2, |runner, lanes| {
        for expected in errors {
            runner.push(0, tagged(1)).unwrap();
            runner.wake(0);
            assert_eq!(runner.tick_parallel(lanes).err(), expected);
        }
    });
}

#[test]
fn independent_failure_leaves_the_other_branch_running() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[0], &[1]);
    let steps = vec![Step::of::<Fails>(0), plus(0, 1)];
    let mut fixture = Fixture::new(steps, wiring, fifo(2));
    parallel(&mut fixture, 2, |runner, lanes| {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.wake(1);
        let report = runner.tick_parallel(lanes).unwrap();
        assert_eq!((report.step_calls, report.faults), (2, 1));
        assert_eq!(runner.queue(1).unwrap().len, 1);
    });
}

#[test]
fn first_failing_step_is_the_error_returned() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![Step::of::<Fails>(0), Step::of::<Fails>(0)];
    let mut fixture = Fixture::new(steps, wiring, fifo(1));
    parallel(&mut fixture, 2, |runner, lanes| {
        runner.set_failure_policy(FailurePolicy::DEFAULT.with_fail_fast(true));
        runner.wake(0);
        runner.wake(1);
        assert_eq!(
            runner.tick_parallel(lanes).unwrap_err(),
            RunError::Fault { step: 0 }
        );
    });
}

#[test]
fn many_failures_are_all_counted_and_backoff_applies() {
    let count = 12;
    let mut wiring = Wiring::default();
    for _ in 0..count {
        wiring.step(&[], &[], &[]);
    }
    let steps = (0..count).map(|_| Step::of::<Fails>(0)).collect();
    let mut fixture = Fixture::new(steps, wiring, fifo(1));
    parallel(&mut fixture, 4, |runner, lanes| {
        for step in 0..count as u32 {
            runner.wake(step);
        }
        let report = runner.tick_parallel(lanes).unwrap();
        assert_eq!(report.faults, count as u32);
        assert_eq!(runner.counters().faults, count as u64);
    });
}

#[test]
fn a_panicking_body_is_that_steps_fault_and_the_pool_lives() {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    let mut fixture = Fixture::new(vec![Step::of::<Panics>(0)], wiring, fifo(1));
    parallel(&mut fixture, 3, |runner, lanes| {
        for _ in 0..3 {
            runner.wake(0);
            let report = runner.tick_parallel(lanes).unwrap();
            assert_eq!(report.faults, 1);
        }
        assert_eq!(runner.counters().faults, 3);
    });
}

/// Records the order bodies ran in.
struct Records {
    id: u32,
    log: Arc<Mutex<Vec<u32>>>,
}

impl OwnedStep for Records {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.log.lock().unwrap().push(self.id);
        StepEnd::Done
    }
}

#[test]
fn priority_orders_ready_steps_with_index_among_equals() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let priorities = [0, 5, 0, 9];
    let mut wiring = Wiring::default();
    let mut steps = Vec::new();
    for id in 0..4u32 {
        wiring.step(&[], &[], &[]);
        steps.push(Step::boxed(Records {
            id,
            log: Arc::clone(&log),
        }));
    }
    let mut fixture = Fixture::new(steps, wiring, fifo(1));
    let mut runner = fixture.load().unwrap();
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(1)), &priorities).unwrap();
    for step in 0..4 {
        runner.wake(step);
    }
    runner.tick_parallel(&mut lanes).unwrap();
    assert_eq!(*log.lock().unwrap(), vec![3, 1, 0, 2]);
}

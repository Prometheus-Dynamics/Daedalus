//! Per-tick slots: a value lives for the tick it was written in, unless its reader was held back.

use crate::support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_abi::{
    Fence, FencePoll, FenceWord, OwnedStep, SlotState, Slots, Step, StepCx, StepEnd,
};
use daedalus_runner::{FailurePolicy, Fire, Runner, SlotKind, SlotSpec};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::Pressure;
use support::{Fixture, Wiring, cell, tagged};

const TY: TypeIndex = TypeIndex(0);
const SP: SpaceIndex = SpaceIndex(0);
/// The reader's trigger, which it cannot run without.
const TRIGGER: u32 = 0;
/// The reader's optional input, on the edge under test.
const OPTIONAL: u32 = 1;
/// The reader's output, where it has one.
const OUT: u32 = 2;

/// Whether the optional input held a value, for each run.
type Seen = Arc<Mutex<Vec<bool>>>;

/// One fence per test that waits, so the tests can run side by side.
static FENCE_READY: [AtomicBool; 2] = [const { AtomicBool::new(false) }; 2];
const READER_FENCE: usize = 0;
const PRODUCER_FENCE: usize = 1;

struct WhenReady;

impl FenceWord for WhenReady {
    fn poll(word: usize) -> FencePoll {
        if FENCE_READY[word].load(Ordering::Relaxed) {
            FencePoll::Ready
        } else {
            FencePoll::Pending
        }
    }
}

/// Reads the trigger and the optional input, and notes whether the optional one was there.
struct Reader {
    seen: Seen,
    /// Runs left that end on a fence.
    pend: u32,
    /// Runs left that fault.
    fault: u32,
    writes: bool,
}

impl OwnedStep for Reader {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        if self.fault > 0 {
            self.fault -= 1;
            return StepEnd::Fault;
        }
        let there = cell(slots, 1).state == SlotState::Full as u32;
        self.seen.lock().unwrap().push(there);
        if self.writes {
            *cell(slots, 2) = tagged(1);
        }
        if self.pend > 0 {
            self.pend -= 1;
            return StepEnd::Pending(Fence::of::<WhenReady>(READER_FENCE));
        }
        StepEnd::Done
    }
}

struct Graph {
    fixture: Fixture,
    seen: Seen,
}

fn graph(pend: u32, fault: u32, writes: bool) -> Graph {
    let seen = Seen::default();
    let mut wiring = Wiring::default();
    let outputs: &[u32] = if writes { &[OUT] } else { &[] };
    if pend > 0 {
        wiring.pending_step(&[], &[TRIGGER, OPTIONAL], outputs);
    } else {
        wiring.step(&[], &[TRIGGER, OPTIONAL], outputs);
    }
    wiring.set_join(0, Fire::All, &[TRIGGER]);
    let steps = vec![Step::boxed(Reader {
        seen: Arc::clone(&seen),
        pend,
        fault,
        writes,
    })];
    let fifo = |cap| SlotSpec {
        cap,
        kind: SlotKind::Fifo,
    };
    Graph {
        fixture: Fixture::new(steps, wiring, vec![fifo(4), fifo(1), fifo(1)]),
        seen,
    }
}

/// The runner with the optional input's slot as a loader sets a `per_tick` edge.
fn per_tick(fixture: &mut Fixture) -> Runner<'_> {
    let mut runner = fixture.load().unwrap();
    runner.set_pressure(OPTIONAL, Pressure::LatestOnly).unwrap();
    runner.set_per_tick(OPTIONAL, true).unwrap();
    runner
}

/// Sends a value on the optional input, as its producer's publish does, and returns a handle that
/// counts who else holds it.
fn send(runner: &mut Runner<'_>) -> Arc<u32> {
    let value = Arc::new(7_u32);
    runner
        .feed_typed(OPTIONAL, TY, SP, Arc::clone(&value))
        .unwrap();
    runner.wake(0);
    value
}

fn trigger(runner: &mut Runner<'_>) {
    runner.push(TRIGGER, tagged(1)).unwrap();
    runner.wake(0);
}

#[test]
fn a_value_written_on_one_tick_and_not_read_is_gone_at_its_end() {
    let mut graph = graph(0, 0, false);
    let mut runner = per_tick(&mut graph.fixture);
    let value = send(&mut runner);
    runner.tick().unwrap();
    // The reader had no trigger, so it did not run, and the slot let its value go.
    assert_eq!(runner.queue(OPTIONAL).unwrap().len, 0);
    assert_eq!(Arc::strong_count(&value), 1, "the slot released it");
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [false]);
}

#[test]
fn a_latest_only_slot_that_is_not_per_tick_keeps_its_value_for_a_later_tick() {
    let mut graph = graph(0, 0, false);
    let mut runner = graph.fixture.load().unwrap();
    runner.set_pressure(OPTIONAL, Pressure::LatestOnly).unwrap();
    let value = send(&mut runner);
    runner.tick().unwrap();
    assert_eq!(Arc::strong_count(&value), 2, "the slot holds it");
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
}

#[test]
fn a_value_fed_between_ticks_is_read_on_the_next_one() {
    let mut graph = graph(0, 0, false);
    let mut runner = per_tick(&mut graph.fixture);
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
    assert_eq!(Arc::strong_count(&value), 1);
}

#[test]
fn a_masked_reader_still_gets_the_value_in_the_first_tick_it_is_free() {
    let mut graph = graph(0, 0, false);
    let mut runner = per_tick(&mut graph.fixture);
    runner.set_activity(Some(&[0])).unwrap();
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(Arc::strong_count(&value), 2, "kept for the masked reader");
    runner.set_activity(None).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
    assert_eq!(Arc::strong_count(&value), 1);
}

#[test]
fn a_reader_behind_a_blocked_output_still_gets_the_value_once_it_can_run() {
    let mut graph = graph(0, 0, true);
    let mut runner = per_tick(&mut graph.fixture);
    runner.set_blocking(OUT, true).unwrap();
    runner.push(OUT, tagged(9)).unwrap();
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert!(graph.seen.lock().unwrap().is_empty(), "held back");
    assert_eq!(Arc::strong_count(&value), 2, "kept for the held reader");
    runner.drop_oldest(OUT).unwrap();
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
}

#[test]
fn a_reader_waiting_on_its_fence_still_gets_the_value_once_the_fence_is_ready() {
    FENCE_READY[READER_FENCE].store(false, Ordering::Relaxed);
    let mut graph = graph(1, 0, false);
    let mut runner = per_tick(&mut graph.fixture);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [false], "the run that waits");
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(Arc::strong_count(&value), 2, "kept while the reader waits");
    FENCE_READY[READER_FENCE].store(true, Ordering::Relaxed);
    for _ in 0..3 {
        runner.tick().unwrap();
    }
    assert_eq!(*graph.seen.lock().unwrap(), [false, true]);
    assert_eq!(Arc::strong_count(&value), 1);
}

#[test]
fn a_reader_backing_off_still_gets_the_value_once_its_backoff_ends() {
    let mut graph = graph(0, 3, false);
    let mut runner = per_tick(&mut graph.fixture);
    runner.set_failure_policy(FailurePolicy::DEFAULT);
    runner.set_clock(0);
    for _ in 0..3 {
        trigger(&mut runner);
        runner.tick().unwrap();
    }
    let health = *runner.health(0).unwrap();
    assert!(health.backing_off());
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(
        Arc::strong_count(&value),
        2,
        "kept while the reader backs off"
    );
    runner.set_clock(health.until_ns());
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
}

#[test]
fn a_value_kept_for_a_held_reader_is_gone_at_the_end_of_the_tick_it_was_free_in() {
    let mut graph = graph(0, 0, false);
    let mut runner = per_tick(&mut graph.fixture);
    runner.set_activity(Some(&[0])).unwrap();
    let value = send(&mut runner);
    runner.tick().unwrap();
    assert_eq!(Arc::strong_count(&value), 2);
    // Free now, and woken, but with no trigger: it does not run, and the value goes.
    runner.set_activity(None).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(Arc::strong_count(&value), 1);
}

/// Sends one value on the optional input and ends on a fence, so the value is hidden until the
/// fence is ready.
struct Working;

impl OwnedStep for Working {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        *cell(slots, 0) = tagged(7);
        StepEnd::Pending(Fence::of::<WhenReady>(PRODUCER_FENCE))
    }
}

#[test]
fn a_value_a_working_step_sent_counts_from_the_tick_it_is_shown_in() {
    FENCE_READY[PRODUCER_FENCE].store(false, Ordering::Relaxed);
    let seen = Seen::default();
    let mut wiring = Wiring::default();
    wiring.pending_step(&[1], &[], &[OPTIONAL]);
    wiring.step(&[], &[TRIGGER, OPTIONAL], &[]);
    wiring.set_join(1, Fire::All, &[TRIGGER]);
    let steps = vec![
        Step::boxed(Working),
        Step::boxed(Reader {
            seen: Arc::clone(&seen),
            pend: 0,
            fault: 0,
            writes: false,
        }),
    ];
    let fifo = |cap| SlotSpec {
        cap,
        kind: SlotKind::Fifo,
    };
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4), fifo(2)]);
    let mut runner = per_tick(&mut fixture);
    runner.wake(0);
    // Two ticks pass while the step works: its value is not shown yet, and is not given up.
    runner.tick().unwrap();
    runner.tick().unwrap();
    assert!(runner.queue(OPTIONAL).unwrap().hidden);
    FENCE_READY[PRODUCER_FENCE].store(true, Ordering::Relaxed);
    runner.push(TRIGGER, tagged(1)).unwrap();
    runner.wake(1);
    for _ in 0..2 {
        runner.tick().unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), [true]);
}

#[test]
fn a_slot_kept_for_several_readers_lets_its_per_tick_value_go_too() {
    let mut graph = graph(0, 0, false);
    graph.fixture.specs[OPTIONAL as usize].kind = SlotKind::Held;
    let mut runner = per_tick(&mut graph.fixture);
    let value = send(&mut runner);
    trigger(&mut runner);
    runner.tick().unwrap();
    // The reader ran and was lent the value; the slot still let it go when the tick ended.
    assert_eq!(*graph.seen.lock().unwrap(), [true]);
    assert_eq!(runner.queue(OPTIONAL).unwrap().len, 0);
    assert_eq!(Arc::strong_count(&value), 1, "the slot released it");
    trigger(&mut runner);
    runner.tick().unwrap();
    assert_eq!(*graph.seen.lock().unwrap(), [true, false]);
}

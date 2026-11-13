//! A step that stops a parallel tick raises the cancel word for the steps running beside it, and
//! the tick clears the word again if it raised it.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{
    CancelWord, FailurePolicy, Lanes, Pool, RunError, SlotKind, SlotSpec, skip_cancelled,
};
use support::{Fixture, Wiring};

/// How long the spinning body waits for a cancel before it gives up.
const DEADLINE: Duration = Duration::from_secs(20);

/// Waits until the other body has started, then faults.
struct Fails {
    other_started: Arc<AtomicBool>,
}

impl OwnedStep for Fails {
    fn run(&mut self, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let start = Instant::now();
        while !self.other_started.load(Ordering::Acquire) && start.elapsed() < DEADLINE {
            std::thread::yield_now();
        }
        cx.fault(None, "failed on purpose");
        StepEnd::Fault
    }
}

/// Spins until it sees a cancel, and records whether it did. Any later run only records whether a
/// cancel was set when it started.
struct Spins {
    started: Arc<AtomicBool>,
    saw_cancel: Arc<AtomicBool>,
}

impl OwnedStep for Spins {
    fn run(&mut self, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        if self.started.swap(true, Ordering::AcqRel) {
            self.saw_cancel.store(cx.cancelled(), Ordering::Release);
            return StepEnd::Done;
        }
        let start = Instant::now();
        while start.elapsed() < DEADLINE {
            if cx.cancelled() {
                self.saw_cancel.store(true, Ordering::Release);
                return skip_cancelled(cx);
            }
            std::thread::yield_now();
        }
        StepEnd::Done
    }
}

struct Case {
    fixture: Fixture,
    saw_cancel: Arc<AtomicBool>,
}

fn case() -> Case {
    let started = Arc::new(AtomicBool::new(false));
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![
        Step::boxed(Fails {
            other_started: Arc::clone(&started),
        }),
        Step::boxed(Spins {
            started,
            saw_cancel: Arc::clone(&saw_cancel),
        }),
    ];
    let slots = vec![SlotSpec {
        cap: 2,
        kind: SlotKind::Fifo,
    }];
    Case {
        fixture: Fixture::new(steps, wiring, slots),
        saw_cancel,
    }
}

#[test]
fn a_failing_step_cancels_the_step_running_beside_it() {
    let mut case = case();
    let word = CancelWord::new();
    let mut runner = case.fixture.load().unwrap();
    runner.set_cancel(&word);
    runner.set_failure_policy(FailurePolicy::DEFAULT.with_fail_fast(true));
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(2)), &[]).unwrap();
    runner.wake(0);
    runner.wake(1);
    let start = Instant::now();
    assert_eq!(
        runner.tick_parallel(&mut lanes),
        Err(RunError::Fault { step: 0 })
    );
    assert!(
        case.saw_cancel.load(Ordering::Acquire),
        "the other body saw the cancel"
    );
    assert!(
        start.elapsed() < DEADLINE,
        "it returned before its deadline"
    );
    assert!(!word.is_set(), "the tick cleared the word it raised");
}

#[test]
fn a_failing_step_cancels_its_neighbour_when_the_host_gave_no_cancel_word() {
    let mut case = case();
    let mut runner = case.fixture.load().unwrap();
    runner.set_failure_policy(FailurePolicy::DEFAULT.with_fail_fast(true));
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(2)), &[]).unwrap();
    runner.wake(0);
    runner.wake(1);
    assert_eq!(
        runner.tick_parallel(&mut lanes),
        Err(RunError::Fault { step: 0 })
    );
    assert!(case.saw_cancel.load(Ordering::Acquire));
    // The tick cleared the word it raised: the same body, run again, sees no cancel.
    runner.wake(1);
    let next = runner.tick_parallel(&mut lanes);
    assert!(!matches!(next, Err(RunError::Cancelled { .. })), "{next:?}");
    assert!(!case.saw_cancel.load(Ordering::Acquire));
}

#[test]
fn a_cancel_the_host_raised_is_left_set() {
    let mut case = case();
    let word = CancelWord::new();
    word.cancel();
    let mut runner = case.fixture.load().unwrap();
    runner.set_cancel(&word);
    runner.set_failure_policy(FailurePolicy::DEFAULT.with_fail_fast(true));
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(2)), &[]).unwrap();
    runner.wake(0);
    runner.wake(1);
    assert!(matches!(
        runner.tick_parallel(&mut lanes),
        Err(RunError::Cancelled { .. })
    ));
    assert!(word.is_set(), "the host's own cancel is untouched");
}

mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{FailurePolicy, PolicyError, Recovery, RunError, SlotKind, SlotSpec};
use support::{Fixture, Wiring, cell, tagged};

const MS: u64 = 1_000_000;
const SEC: u64 = 1_000_000_000;

/// What a call saw: the length of its input (0 if none), and whether it was told to restart.
type Seen = Arc<Mutex<Vec<(usize, bool)>>>;

/// Fails `remaining` times, then copies its input to its output when it has one.
struct Scripted {
    seen: Seen,
    remaining: usize,
    input: Option<usize>,
    output: Option<usize>,
}

impl OwnedStep for Scripted {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let len = match self.input {
            Some(input) => cell(slots, input).len,
            None => 0,
        };
        self.seen.lock().unwrap().push((len, cx.restarting()));
        if self.remaining > 0 {
            self.remaining -= 1;
            cx.fault(None, "scripted");
            return StepEnd::Fault;
        }
        if let (Some(input), Some(output)) = (self.input, self.output) {
            *cell(slots, output) = *cell(slots, input);
        }
        StepEnd::Done
    }
}

fn fifo(cap: u32) -> SlotSpec {
    SlotSpec {
        cap,
        kind: SlotKind::Fifo,
    }
}

fn policy(fail_fast: bool) -> FailurePolicy {
    FailurePolicy::DEFAULT.with_fail_fast(fail_fast)
}

/// One step on slot 0 that writes slot 1, then one consumer step on slot 1 that logs its input.
fn pipeline(first: Scripted, recovery: Recovery, consumer_seen: &Seen) -> (Vec<Step>, Wiring) {
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[1]);
    wiring.step(&[], &[1], &[]);
    wiring.set_recovery(0, recovery);
    let steps = vec![
        Step::boxed(first),
        Step::boxed(Scripted {
            seen: consumer_seen.clone(),
            remaining: 0,
            input: Some(1),
            output: None,
        }),
    ];
    (steps, wiring)
}

fn single(seen: &Seen, remaining: usize, recovery: Recovery) -> (Vec<Step>, Wiring) {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[]);
    wiring.set_recovery(0, recovery);
    let steps = vec![Step::boxed(Scripted {
        seen: seen.clone(),
        remaining,
        input: Some(0),
        output: None,
    })];
    (steps, wiring)
}

#[test]
fn default_contains_a_failure_and_keeps_ticking() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 100, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    for _ in 0..3 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        let report = runner.tick().expect("contained, not fatal");
        assert_eq!(report.faults, 1);
    }
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[test]
fn fail_fast_ends_the_run() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 1, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.set_failure_policy(policy(true));
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick(), Err(RunError::Fault { step: 0 }));
}

#[test]
fn skip_tick_drops_output_and_keeps_running() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let consumer = Arc::new(Mutex::new(Vec::new()));
    let first = Scripted {
        seen: seen.clone(),
        remaining: 1,
        input: Some(0),
        output: Some(1),
    };
    let (steps, wiring) = pipeline(first, Recovery::SkipTick, &consumer);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4), fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.set_failure_policy(policy(true));
    runner.push(0, tagged(3)).unwrap();
    runner.wake(0);
    let report = runner.tick().expect("SkipTick never stops the run");
    assert_eq!(report.faults, 1);
    assert_eq!(runner.queue(1).unwrap().len, 0, "the output was dropped");
    assert!(consumer.lock().unwrap().is_empty());
    runner.push(0, tagged(4)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(
        *consumer.lock().unwrap(),
        vec![(4, false)],
        "the next tick runs"
    );
}

#[test]
fn retry_reruns_on_the_same_inputs() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 2, Recovery::Retry { max: 2 });
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(7)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.retries, 2);
    assert_eq!(report.faults, 0);
    assert_eq!(report.step_calls, 3);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(7, false), (7, false), (7, false)]
    );
    assert_eq!(runner.queue(0).unwrap().len, 0);
}

#[test]
fn exhausted_retries_fail_like_fail() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 9, Recovery::Retry { max: 1 });
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 2);
    assert_eq!(report.faults, 1);
    assert_eq!(runner.health(0).unwrap().failures(), 1);
    runner.set_failure_policy(policy(true));
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick(), Err(RunError::Fault { step: 0 }));
}

#[test]
fn a_failed_tick_does_not_silence_the_next_ones() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 1, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    runner.push(0, tagged(2)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 1);
    assert_eq!(runner.health(0).unwrap().failures(), 0);
}

#[test]
fn third_failure_starts_the_backoff_at_base() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 100, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(8)]);
    let mut runner = fixture.load().unwrap();
    runner.set_clock(0);
    for _ in 0..3 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    let health = *runner.health(0).unwrap();
    assert!(health.backing_off());
    assert_eq!(health.until_ns(), FailurePolicy::DEFAULT.base_ns());
    // Before `until` the step is not called, and its input waits.
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let early = runner.tick().unwrap();
    assert_eq!(early.step_calls, 0);
    assert_eq!(runner.queue(0).unwrap().len, 1);
    // At `until` it is called again.
    runner.set_clock(health.until_ns());
    let due = runner.tick().unwrap();
    assert_eq!(due.step_calls, 1);
}

#[test]
fn backoff_never_exceeds_eight_seconds() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 1000, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(200)]);
    let mut runner = fixture.load().unwrap();
    runner.set_failure_policy(FailurePolicy::new(50 * MS, 8 * SEC, 3, 0, false).unwrap());
    let mut now = 0u64;
    for _ in 0..100 {
        runner.set_clock(now);
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
        let health = *runner.health(0).unwrap();
        if health.backing_off() {
            assert!(health.until_ns() - now <= 8 * SEC);
            now = health.until_ns();
        }
    }
    assert_eq!(runner.health(0).unwrap().failures(), 100);
    assert!(
        !runner.health(0).unwrap().disabled(),
        "never switched off by default"
    );
}

#[test]
fn backoff_doubles_from_base_up_to_the_cap() {
    let policy = FailurePolicy::new(50 * MS, 400 * MS, 1, 0, false).unwrap();
    assert_eq!(policy.delay_ns(1), 50 * MS);
    assert_eq!(policy.delay_ns(2), 100 * MS);
    assert_eq!(policy.delay_ns(3), 200 * MS);
    assert_eq!(policy.delay_ns(4), 400 * MS);
    assert_eq!(policy.delay_ns(40), 400 * MS);
}

#[test]
fn a_cap_above_eight_seconds_is_refused() {
    assert_eq!(
        FailurePolicy::new(50 * MS, 9 * SEC, 3, 0, false),
        Err(PolicyError::CapTooLong { cap_ns: 9 * SEC })
    );
    assert_eq!(
        FailurePolicy::new(50 * MS, 8 * SEC, 0, 0, false),
        Err(PolicyError::NoBackoffThreshold)
    );
}

#[test]
fn manual_off_runs_no_tick_and_consumer_sees_nothing() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let consumer = Arc::new(Mutex::new(Vec::new()));
    let first = Scripted {
        seen: seen.clone(),
        remaining: 0,
        input: Some(0),
        output: Some(1),
    };
    let (steps, wiring) = pipeline(first, Recovery::Fail, &consumer);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4), fifo(4)]);
    let mut runner = fixture.load().unwrap();
    runner.switch_off(0).unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 0);
    assert!(seen.lock().unwrap().is_empty());
    assert!(consumer.lock().unwrap().is_empty());
    assert_eq!(runner.queue(0).unwrap().len, 0, "its inputs were dropped");
    assert!(runner.health(0).unwrap().disabled());
    assert!(!runner.health(0).unwrap().auto_off());
}

#[test]
fn switching_on_clears_backoff_and_keeps_the_count() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 3, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(8)]);
    let mut runner = fixture.load().unwrap();
    runner.set_clock(0);
    for _ in 0..3 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    assert!(runner.health(0).unwrap().backing_off());
    runner.switch_on(0).unwrap();
    let health = *runner.health(0).unwrap();
    assert!(!health.backing_off());
    assert_eq!(health.failures(), 3);
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 1);
}

#[test]
fn auto_off_only_when_the_policy_asks() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 100, Recovery::Fail);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(8)]);
    let mut runner = fixture.load().unwrap();
    runner.set_failure_policy(FailurePolicy::new(50 * MS, 5 * SEC, 3, 2, false).unwrap());
    runner.set_clock(0);
    for _ in 0..2 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    let health = *runner.health(0).unwrap();
    assert!(health.disabled() && health.auto_off());
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 0);
    runner.switch_on(0).unwrap();
    assert!(!runner.health(0).unwrap().disabled());
}

#[test]
fn restart_flag_reaches_the_next_call_only() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (steps, wiring) = single(&seen, 1, Recovery::Restart);
    let mut fixture = Fixture::new(steps, wiring, vec![fifo(4)]);
    let mut runner = fixture.load().unwrap();
    for value in 1..=3 {
        runner.push(0, tagged(value)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(1, false), (2, true), (3, false)]
    );
}

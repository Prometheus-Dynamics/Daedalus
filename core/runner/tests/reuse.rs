//! Output reuse: unchanged inputs replay, a change reruns, and an upstream change invalidates the
//! chain. Replays are by handle and failed runs store nothing.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{OwnedStep, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{
    InputVersion, ReuseEntry, ReuseMarker, ReuseMemory, ReuseReason, Runner, SlotKind, SlotSpec,
    check_reuse,
};
use daedalus_value::{Owner, Value};
use support::{Fixture, Wiring, cell, tagged};

/// The caller-owned reuse buffers, sized from the runner.
struct Buffers {
    entries: Vec<ReuseEntry>,
    stamps: Vec<u64>,
    stored: Vec<Slot>,
    inline: Vec<[u8; 16]>,
    inputs: Vec<InputVersion>,
}

impl Buffers {
    fn new(steps: usize, slots: usize, fixture: &Fixture) -> Buffers {
        let reusable = || fixture.wiring.meta.iter().filter(|meta| meta.reuse);
        let stamps = reusable().map(|meta| meta.inputs.len as usize).sum();
        let stored = reusable().map(|meta| meta.outputs.len as usize).sum();
        Buffers {
            entries: vec![ReuseEntry::EMPTY; steps],
            stamps: vec![0; stamps],
            stored: vec![Slot::EMPTY; stored],
            inline: vec![[0; 16]; stored],
            inputs: vec![InputVersion::default(); slots],
        }
    }

    fn memory(&mut self) -> ReuseMemory<'_> {
        ReuseMemory {
            entries: &mut self.entries,
            stamps: &mut self.stamps,
            stored: &mut self.stored,
            inline: &mut self.inline,
            inputs: &mut self.inputs,
        }
    }
}

/// Backs the static values the bodies write, so taking one reads valid bytes.
static BYTES: [u8; 64] = [0; 64];

/// Copies input to output, counts its calls, and can fault while the flag is up.
struct Pure {
    calls: Arc<AtomicU32>,
    fault: Arc<AtomicU32>,
    output: usize,
}

impl OwnedStep for Pure {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fault.load(Ordering::Relaxed) != 0 {
            return StepEnd::Fault;
        }
        let value = *cell(slots, 0);
        if self.output != 0 {
            let mut copy = tagged(value.len);
            copy.data = BYTES.as_ptr().cast_mut();
            *cell(slots, self.output) = copy;
        }
        StepEnd::Done
    }
}

fn spec(kind: SlotKind) -> SlotSpec {
    SlotSpec { cap: 4, kind }
}

/// One reusable step: reads slot 0, writes slot 1 (an output that keeps its value).
fn single() -> (Fixture, Arc<AtomicU32>, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let fault = Arc::new(AtomicU32::new(0));
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[1]);
    wiring.set_reuse(0);
    let body = Pure {
        calls: Arc::clone(&calls),
        fault: Arc::clone(&fault),
        output: 1,
    };
    let specs = vec![spec(SlotKind::Latest), spec(SlotKind::Fifo)];
    (
        Fixture::new(vec![Step::boxed(body)], wiring, specs),
        calls,
        fault,
    )
}

fn drain(runner: &mut Runner<'_>, slot: u32) -> usize {
    let mut count = 0;
    while runner.take_value(slot).is_some() {
        count += 1;
    }
    count
}

#[test]
fn unchanged_inputs_replay_and_a_changed_input_reruns() {
    let (mut fixture, calls, _) = single();
    let mut buffers = Buffers::new(1, 2, &fixture);
    let mut runner = fixture.load().unwrap();
    runner.set_reuse(buffers.memory(), &[]).unwrap();

    runner.push(0, tagged(7)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 1);
    assert_eq!(drain(&mut runner, 1), 1);

    // Same version on the input: the output is pushed again, the body is not called.
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(runner.counters().reuse_replays, 1);
    assert_eq!(drain(&mut runner, 1), 1);

    // A new value on the input: the body runs.
    runner.clear_held(0).unwrap();
    runner.push(0, tagged(8)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    drop(runner);
}

#[test]
fn an_upstream_change_invalidates_the_chain() {
    let calls_a = Arc::new(AtomicU32::new(0));
    let calls_b = Arc::new(AtomicU32::new(0));
    let none = Arc::new(AtomicU32::new(0));
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[1]);
    wiring.step(&[], &[1], &[2]);
    wiring.set_reuse(0);
    wiring.set_reuse(1);
    let step_a = Step::boxed(Pure {
        calls: Arc::clone(&calls_a),
        fault: Arc::clone(&none),
        output: 1,
    });
    let step_b = Step::boxed(Pure {
        calls: Arc::clone(&calls_b),
        fault: Arc::clone(&none),
        output: 1,
    });
    let specs = vec![
        spec(SlotKind::Latest),
        spec(SlotKind::Fifo),
        spec(SlotKind::Fifo),
    ];
    let mut fixture = Fixture::new(vec![step_a, step_b], wiring, specs);
    let mut buffers = Buffers::new(2, 3, &fixture);
    let mut runner = fixture.load().unwrap();
    runner.set_reuse(buffers.memory(), &[]).unwrap();

    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(drain(&mut runner, 2), 1);

    // A replay keeps the output's version, so B also replays.
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(drain(&mut runner, 2), 1);
    assert_eq!(
        (
            calls_a.load(Ordering::Relaxed),
            calls_b.load(Ordering::Relaxed)
        ),
        (1, 1)
    );
    assert_eq!(runner.counters().reuse_replays, 2);

    // A new value upstream reruns the whole chain.
    runner.clear_held(0).unwrap();
    runner.push(0, tagged(2)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(
        (
            calls_a.load(Ordering::Relaxed),
            calls_b.load(Ordering::Relaxed)
        ),
        (2, 2)
    );
    drop(runner);
}

#[test]
fn a_failed_run_stores_nothing() {
    let (mut fixture, calls, fault) = single();
    let mut buffers = Buffers::new(1, 2, &fixture);
    let mut runner = fixture.load().unwrap();
    runner.set_reuse(buffers.memory(), &[]).unwrap();

    runner.push(0, tagged(5)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    drain(&mut runner, 1);

    fault.store(1, Ordering::Relaxed);
    runner.clear_held(0).unwrap();
    runner.push(0, tagged(6)).unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().faults, 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    // The same input again: nothing was stored for it, so the body runs and no stale value comes.
    fault.store(0, Ordering::Relaxed);
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 1);
    assert_eq!(runner.counters().reuse_replays, 0);
    drop(runner);
}

/// A host version keeps a re-fed value unchanged; with no version, identity decides.
#[test]
fn a_host_input_stamps_by_version_or_by_identity() {
    let (mut fixture, calls, _) = single();
    let mut buffers = Buffers::new(1, 2, &fixture);
    let mut runner = fixture.load().unwrap();
    runner.set_reuse(buffers.memory(), &[0]).unwrap();

    let same: &'static [u8] = b"payload";
    let other: &'static [u8] = b"another payload";
    let feed = |runner: &mut Runner<'_>, bytes: &'static [u8]| {
        runner.clear_held(0).unwrap();
        runner
            .feed_value(
                0,
                Value {
                    ty: daedalus_types::TypeIndex(0),
                    space: daedalus_types::SpaceIndex(0),
                    owner: Owner::Static(bytes),
                },
            )
            .unwrap();
        runner.wake(0);
        runner.tick().unwrap();
        drain(runner, 1);
    };
    feed(&mut runner, same);
    feed(&mut runner, same);
    assert_eq!(calls.load(Ordering::Relaxed), 1, "same identity replays");
    feed(&mut runner, other);
    assert_eq!(calls.load(Ordering::Relaxed), 2, "new identity reruns");

    // A declared version wins over identity, in both directions.
    runner.set_input_version(0, Some(1));
    feed(&mut runner, other);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "version replaces the stamp"
    );
    feed(&mut runner, same);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "same version replays new bytes"
    );
    runner.set_input_version(0, Some(2));
    feed(&mut runner, same);
    assert_eq!(calls.load(Ordering::Relaxed), 4, "a new version reruns");
    drop(runner);
}

#[test]
fn a_shared_output_replays_by_handle_with_no_copy() {
    struct Shares(Arc<[u8]>);
    impl OwnedStep for Shares {
        fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
            daedalus_runner::put_shared(
                cell(slots, 1),
                daedalus_types::TypeIndex(0),
                daedalus_types::SpaceIndex(0),
                Arc::clone(&self.0),
            );
            StepEnd::Done
        }
    }
    let _ = (SlotState::Full, ReuseReason::Device);
    let payload: Arc<[u8]> = Arc::from(&b"shared"[..]);
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0], &[1]);
    wiring.set_reuse(0);
    let specs = vec![spec(SlotKind::Latest), spec(SlotKind::Fifo)];
    let steps = vec![Step::boxed(Shares(Arc::clone(&payload)))];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut buffers = Buffers::new(1, 2, &fixture);
    let mut runner = fixture.load().unwrap();
    runner.set_reuse(buffers.memory(), &[]).unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(runner.counters().reuse_replays, 1);
    let first = runner.take_value(1).unwrap();
    let second = runner.take_value(1).unwrap();
    assert_eq!(
        first.bytes().unwrap().as_ptr(),
        second.bytes().unwrap().as_ptr()
    );
    drop((first, second));
    runner.clear_reuse();
    drop(runner);
    assert_eq!(Arc::strong_count(&payload), 2);
}

#[test]
fn a_load_refuses_bad_markers_and_the_nodes_that_cannot_reuse() {
    let refused =
        |marker, device, bridge| check_reuse("demo.node", marker, device, bridge).unwrap_err();
    assert_eq!(
        refused(ReuseMarker::Other, false, false).reason,
        ReuseReason::NotBoolean
    );
    assert_eq!(
        refused(ReuseMarker::Bool(true), true, false).reason,
        ReuseReason::Device
    );
    assert_eq!(
        refused(ReuseMarker::Bool(true), false, true).reason,
        ReuseReason::HostBridge
    );
    assert_eq!(refused(ReuseMarker::Other, false, false).node, "demo.node");
    assert_eq!(check_reuse("n", ReuseMarker::Absent, true, true), Ok(false));
    assert_eq!(
        check_reuse("n", ReuseMarker::Bool(false), true, true),
        Ok(false)
    );
    assert_eq!(
        check_reuse("n", ReuseMarker::Bool(true), false, false),
        Ok(true)
    );
}

#[test]
fn a_plan_with_no_opt_in_makes_no_reuse_checks() {
    let (wiring, words) = support::chain(4);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<support::Link>).collect();
    let specs = vec![spec(SlotKind::Fifo); 4];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    assert_eq!(runner.reuse_needs(), (0, 0));
    for _ in 0..5 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    assert_eq!(runner.counters().reuse_checks, 0);
}

//! Cancellation stops a tick at a step boundary and loses nothing. Progress cells read back what a
//! body wrote. Neither allocates.

mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{OwnedStep, Progress, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_runner::{CANCEL_REASON, CancelWord, RunError, SlotKind, SlotSpec, skip_cancelled};
use support::{Fixture, Link, Wiring, tagged};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn fifo(count: usize) -> Vec<SlotSpec> {
    vec![
        SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        };
        count
    ]
}

/// Records its id, and asks for a cancel when it is the chosen one.
struct Tick {
    id: u32,
    cancel_at: u32,
    word: Arc<CancelWord>,
    log: Arc<Mutex<Vec<u32>>>,
}

impl OwnedStep for Tick {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.log.lock().unwrap().push(self.id);
        if self.id == self.cancel_at {
            self.word.cancel();
        }
        StepEnd::Done
    }
}

#[test]
fn a_cancel_between_steps_stops_there_and_the_next_tick_resumes_in_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let word = Arc::new(CancelWord::new());
    let mut wiring = Wiring::default();
    let mut steps = Vec::new();
    for id in 0..4 {
        wiring.step(&[], &[], &[]);
        steps.push(Step::boxed(Tick {
            id,
            cancel_at: 1,
            word: Arc::clone(&word),
            log: Arc::clone(&log),
        }));
    }
    let mut fixture = Fixture::new(steps, wiring, fifo(1));
    let mut runner = fixture.load().unwrap();
    runner.set_cancel(&word);
    for step in 0..4 {
        runner.wake(step);
    }
    assert_eq!(runner.tick(), Err(RunError::Cancelled { step: 2 }));
    assert_eq!(*log.lock().unwrap(), vec![0, 1]);
    assert_eq!(runner.counters().cancelled, 2);

    word.clear();
    assert!(!word.is_set());
    runner.tick().unwrap();
    assert_eq!(*log.lock().unwrap(), vec![0, 1, 2, 3]);
}

struct SeesCancel;

impl StepWord for SeesCancel {
    fn run(_: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        if cx.cancelled() {
            skip_cancelled(cx)
        } else {
            StepEnd::Done
        }
    }
}

#[test]
fn a_body_that_sees_cancel_skips_without_a_fault_or_backoff() {
    // The word goes up while the body runs, so the tick's own check at the boundary saw it clear.
    static PTR: AtomicU32 = AtomicU32::new(0);
    struct RaiseThenCheck;
    impl StepWord for RaiseThenCheck {
        fn run(_: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
            let word = cx.cancel;
            // SAFETY: the test attached the word, and it outlives the tick.
            unsafe { (*word).set(1) };
            PTR.fetch_add(1, Ordering::Relaxed);
            let end = SeesCancel::run(0, &mut Slots::of(&mut []), cx);
            assert_eq!(cx.reason(), CANCEL_REASON);
            end
        }
    }
    let word = CancelWord::new();
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    let mut fixture = Fixture::new(vec![Step::of::<RaiseThenCheck>(0)], wiring, fifo(1));
    let mut runner = fixture.load().unwrap();
    runner.set_cancel(&word);
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!((report.skipped, report.faults), (1, 0));
    assert_eq!(runner.health(0).unwrap().failures(), 0);
    assert!(!runner.health(0).unwrap().backing_off());
    assert_eq!(PTR.load(Ordering::Relaxed), 1);
}

struct Reports;

impl StepWord for Reports {
    fn run(word: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        cx.progress(word as u32, 10);
        StepEnd::Done
    }
}

#[test]
fn progress_cells_read_back_what_the_body_wrote() {
    let cells = [Progress::new(), Progress::new()];
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![Step::of::<Reports>(3), Step::of::<Reports>(7)];
    let mut fixture = Fixture::new(steps, wiring, fifo(1));
    let mut runner = fixture.load().unwrap();
    assert_eq!(runner.progress(0), None);
    runner.set_progress(&cells).unwrap();
    runner.wake(0);
    runner.wake(1);
    runner.tick().unwrap();
    assert_eq!(runner.progress(0), Some((3, 10)));
    assert_eq!(runner.progress(1), Some((7, 10)));
}

#[test]
fn cancel_and_progress_never_allocate() {
    let (wiring, words) = support::chain(8);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let mut specs = fifo(8);
    specs[0].cap = 128;
    let mut fixture = Fixture::new(steps, wiring, specs);
    let word = CancelWord::new();
    let cells: Vec<Progress> = (0..8).map(|_| Progress::new()).collect();
    let mut runner = fixture.load().unwrap();
    runner.set_cancel(&word);
    runner.set_progress(&cells).unwrap();
    let ((), allocations) = counted(|| {
        for round in 0..1000usize {
            runner.push(0, tagged(round)).unwrap();
            runner.wake(0);
            if round % 10 == 0 {
                word.cancel();
                let _ = runner.tick();
                word.clear();
                continue;
            }
            runner.tick().unwrap();
        }
    });
    assert_eq!(allocations, 0);
}

#[cfg(feature = "threads")]
#[test]
fn a_parallel_tick_stops_and_keeps_the_rest_dirty() {
    use daedalus_runner::{Lanes, Pool};
    let word = CancelWord::new();
    let (wiring, words) = support::chain(4);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let mut fixture = Fixture::new(steps, wiring, fifo(4));
    let mut runner = fixture.load().unwrap();
    runner.set_cancel(&word);
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(2)), &[]).unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    word.cancel();
    assert_eq!(
        runner.tick_parallel(&mut lanes),
        Err(RunError::Cancelled { step: 0 })
    );
    word.clear();
    assert_eq!(runner.tick_parallel(&mut lanes).unwrap().step_calls, 4);
}

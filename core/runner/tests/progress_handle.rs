//! Progress is read from another thread while a parallel tick runs, without a lock or an
//! allocation, and a handle outlives the runner.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::owned_run::{self, ProgressHandle};
use daedalus_runner::{Lanes, Pool, SlotKind, SlotSpec, SlotStamp};
use daedalus_types::{SpaceIndex, TypeIndex};
use support::Wiring;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const TOTAL: u32 = 2_000;
const DEADLINE: Duration = Duration::from_secs(20);

/// Counts up to `TOTAL`, reporting each step, and waits at each one until the reader has looked
/// at least once, so the reader is certain to see the count part-way.
struct Counts {
    reader_seen: Arc<AtomicBool>,
}

impl OwnedStep for Counts {
    fn run(&mut self, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let start = Instant::now();
        for done in 1..=TOTAL {
            cx.progress(done, TOTAL);
            if done == TOTAL / 2 {
                while !self.reader_seen.load(Ordering::Acquire) && start.elapsed() < DEADLINE {
                    std::thread::yield_now();
                }
            }
        }
        StepEnd::Done
    }
}

/// Runs beside the counting step, and reports nothing.
struct Idles;

impl OwnedStep for Idles {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn build(
    reader_seen: &Arc<AtomicBool>,
) -> (daedalus_runner::Runner<'static>, owned_run::RunBuffers) {
    let mut wiring = Wiring::default();
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![
        Step::boxed(Counts {
            reader_seen: Arc::clone(reader_seen),
        }),
        Step::boxed(Idles),
    ];
    let slots = vec![SlotSpec {
        cap: 2,
        kind: SlotKind::Fifo,
    }];
    let stamps = vec![SlotStamp {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
    }];
    owned_run::build(steps, wiring.meta, wiring.table, slots, stamps, &[], None).unwrap()
}

#[test]
fn another_thread_reads_progress_while_a_parallel_tick_runs() {
    let reader_seen = Arc::new(AtomicBool::new(false));
    let (mut runner, buffers) = build(&reader_seen);
    let handle: ProgressHandle = buffers.progress_handle();
    assert_eq!(handle.steps(), 2);
    assert_eq!(handle.get(0), Some((0, 0)));
    assert_eq!(handle.get(2), None);
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(2)), &[]).unwrap();
    runner.wake(0);
    runner.wake(1);

    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (handle, done, seen) = (handle.clone(), Arc::clone(&done), Arc::clone(&reader_seen));
        std::thread::spawn(move || {
            let mut last = 0;
            let mut part_way = false;
            let start = Instant::now();
            while !done.load(Ordering::Acquire) && start.elapsed() < DEADLINE {
                let (at, total) = handle.get(0).expect("the step has cells");
                assert!(at >= last, "the count never goes back: {at} after {last}");
                assert!(at <= TOTAL, "never past what the body wrote");
                assert!(
                    total == 0 || total == TOTAL,
                    "the total is the body's: {total}"
                );
                last = at;
                if at > 0 && at < TOTAL {
                    part_way = true;
                }
                if at >= TOTAL / 2 {
                    seen.store(true, Ordering::Release);
                }
            }
            part_way
        })
    };
    runner.tick_parallel(&mut lanes).unwrap();
    done.store(true, Ordering::Release);
    let part_way = reader.join().unwrap();
    assert!(part_way, "the reader saw the count while the body ran");
    assert_eq!(handle.get(0), Some((TOTAL, TOTAL)));
    assert_eq!(
        handle.get(1),
        Some((0, 0)),
        "the other step reported nothing"
    );
    assert_eq!(
        runner.progress(0),
        Some((TOTAL, TOTAL)),
        "the runner reads the same cells"
    );

    // Reading allocates nothing.
    let ((), allocations) = counted(|| {
        for _ in 0..1000 {
            std::hint::black_box(handle.get(0));
        }
    });
    assert_eq!(allocations, 0);

    // The handle outlives the runner and its buffers, and reads the last values.
    drop(lanes);
    drop(runner);
    drop(buffers);
    assert_eq!(handle.get(0), Some((TOTAL, TOTAL)));
}

mod support;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slots, Step, StepCx, StepEnd};
use daedalus_runner::{Counters, SlotKind, SlotSpec};
use support::{Fixture, Link, chain, tagged};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[test]
fn counters_have_a_fixed_size() {
    assert_eq!(std::mem::size_of::<Counters>(), 11 * 8);
}

struct Always;

impl daedalus_abi::StepWord for Always {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Fault
    }
}

#[test]
fn counts_match_the_tick_reports() {
    let (wiring, words) = chain(3);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let specs: Vec<SlotSpec> = (0..3)
        .map(|_| SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        })
        .collect();
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let busy = runner.tick().unwrap();
    let idle = runner.tick().unwrap();
    let counters = runner.counters();
    assert_eq!(counters.ticks, 2);
    assert_eq!(counters.idle_ticks, 1);
    assert_eq!(counters.steps_called, u64::from(busy.step_calls));
    assert!(idle.idle);
    assert_eq!(counters.faults, 0);
}

#[test]
fn faults_and_backoff_are_counted() {
    let (wiring, _) = chain(1);
    let steps = vec![Step::of::<Always>(0)];
    let specs = vec![SlotSpec {
        cap: 16,
        kind: SlotKind::Fifo,
    }];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.set_clock(0);
    for _ in 0..4 {
        runner.push(0, tagged(1)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
    }
    let counters = runner.counters();
    assert_eq!(counters.faults, 3, "the fourth tick waited out the backoff");
    assert_eq!(counters.backoff_skips, 1);
    assert_eq!(runner.health(0).unwrap().calls(), 3);
}

#[test]
fn warm_ticks_with_counters_allocate_nothing() {
    let (wiring, words) = chain(16);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let specs: Vec<SlotSpec> = (0..16)
        .map(|_| SlotSpec {
            cap: 1,
            kind: SlotKind::Fifo,
        })
        .collect();
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    let ((), allocations) = counted(|| {
        for round in 0..1_000usize {
            runner.push(0, tagged(round)).unwrap();
            runner.wake(0);
            runner.tick().unwrap();
        }
    });
    assert_eq!(allocations, 0);
}

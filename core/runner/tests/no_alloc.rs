mod support;

use daedalus_abi::Step;
use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_runner::{SlotKind, SlotSpec};
use support::{Fixture, Link, tagged};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[test]
fn ten_thousand_warm_ticks_allocate_nothing() {
    let (wiring, words) = support::chain(16);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let specs: Vec<SlotSpec> = (0..16)
        .map(|_| SlotSpec {
            cap: 1,
            kind: SlotKind::Fifo,
        })
        .collect();
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    // Warm up once, outside the count.
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();

    let ((), allocations) = counted(|| {
        for round in 0..10_000usize {
            runner.push(0, tagged(round)).unwrap();
            runner.wake(0);
            let report = runner.tick().unwrap();
            assert_eq!(report.step_calls, 16);
        }
    });
    assert_eq!(allocations, 0);
}

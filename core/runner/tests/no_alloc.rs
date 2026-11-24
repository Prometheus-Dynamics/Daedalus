use crate::support;

use daedalus_abi::Step;
use daedalus_abi::counting::counted;
use daedalus_runner::{SlotKind, SlotSpec};
use support::{Fixture, Link, tagged};

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

mod fan {
    //! A fanned output: one value to a queue per reader, by reference count. Each way a producer
    //! publishes (plain, hidden behind a fence, replayed, held back by a full reader) allocates
    //! nothing, copies nothing, and leaves no reference behind.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use daedalus_abi::counting::counted;
    use daedalus_abi::{
        Fence, FencePoll, FenceWord, OwnedStep, Slot, SlotState, Slots, Step, StepCx, StepEnd,
    };
    use daedalus_runner::{
        FanOut, InputVersion, ReuseEntry, ReuseMemory, Runner, SlotKind, SlotSpec, put_typed,
    };
    use daedalus_types::{SpaceIndex, TypeIndex};

    use super::support::{Fixture, Wiring, cell};

    const TY: TypeIndex = TypeIndex(0);
    const SP: SpaceIndex = SpaceIndex(0);
    /// The producer's second output is its first, fanned.
    static FANS: [FanOut; 1] = [FanOut {
        step: 0,
        from: 0,
        to: 1,
    }];
    const FIRST: u32 = 1;
    const SECOND: u32 = 2;
    /// The producer's input, in the graph that replays.
    const INPUT: u32 = 3;
    const ROUNDS: u32 = 2_000;

    struct Ready;

    impl FenceWord for Ready {
        fn poll(_: usize) -> FencePoll {
            FencePoll::Ready
        }
    }

    /// Sends its one value, by handle, on its last cell.
    struct Give {
        value: Arc<u64>,
        pend: bool,
        calls: Arc<AtomicU32>,
    }

    impl OwnedStep for Give {
        fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let last = slots.len() - 1;
            put_typed(cell(slots, last), TY, SP, Arc::clone(&self.value));
            if self.pend {
                return StepEnd::Pending(Fence::of::<Ready>(0));
            }
            StepEnd::Done
        }
    }

    /// Reads its input and counts the values it got.
    struct Take(Arc<AtomicU32>);

    impl OwnedStep for Take {
        fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
            if cell(slots, 0).state == SlotState::Full as u32 {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            StepEnd::Done
        }
    }

    struct Graph {
        fixture: Fixture,
        value: Arc<u64>,
        made: Arc<AtomicU32>,
        got: [Arc<AtomicU32>; 2],
    }

    /// A producer whose one output is fanned to two queues, and a reader on each.
    fn graph(pend: bool, reuse: bool, cap: u32) -> Graph {
        let value = Arc::new(7_u64);
        let made = Arc::new(AtomicU32::new(0));
        let got = [Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0))];
        let mut wiring = Wiring::default();
        let inputs: &[u32] = if reuse { &[INPUT] } else { &[] };
        if pend {
            wiring.pending_step(&[1, 2], inputs, &[FIRST, SECOND]);
        } else {
            wiring.step(&[1, 2], inputs, &[FIRST, SECOND]);
        }
        if reuse {
            wiring.set_reuse(0);
        }
        wiring.step(&[], &[FIRST], &[]);
        wiring.step(&[], &[SECOND], &[]);
        let steps = vec![
            Step::boxed(Give {
                value: Arc::clone(&value),
                pend,
                calls: Arc::clone(&made),
            }),
            Step::boxed(Take(Arc::clone(&got[0]))),
            Step::boxed(Take(Arc::clone(&got[1]))),
        ];
        let fifo = |cap| SlotSpec {
            cap,
            kind: SlotKind::Fifo,
        };
        let held = SlotSpec {
            cap: 1,
            kind: SlotKind::Held,
        };
        let specs = vec![fifo(cap), fifo(cap), fifo(4), held];
        Graph {
            fixture: Fixture::new(steps, wiring, specs),
            value,
            made,
            got,
        }
    }

    fn count(counter: &AtomicU32) -> u32 {
        counter.load(Ordering::Relaxed)
    }

    /// Runs `round` for each round with nothing allocated, then checks that both readers got
    /// every value the producer sent and that no queue still holds a reference.
    fn settle(
        got: &[Arc<AtomicU32>; 2],
        runner: &mut Runner<'_>,
        mut round: impl FnMut(&mut Runner<'_>, u32),
    ) {
        round(runner, 0);
        let ((), allocations) = counted(|| {
            for at in 1..ROUNDS {
                round(runner, at);
            }
        });
        assert_eq!(allocations, 0);
        runner.set_activity(None).unwrap();
        for _ in 0..4 {
            runner.wake(1);
            runner.wake(2);
            runner.tick().unwrap();
        }
        assert!(count(&got[0]) >= ROUNDS / 2);
        assert_eq!(count(&got[0]), count(&got[1]));
    }

    #[test]
    fn a_plain_publish_fans_out_with_no_allocation_and_no_copy() {
        let Graph {
            mut fixture,
            value,
            made,
            got,
        } = graph(false, false, 4);
        let mut runner = fixture.load().unwrap();
        runner.set_fans(&FANS).unwrap();
        settle(&got, &mut runner, |runner, _| {
            runner.wake(0);
            runner.tick().unwrap();
        });
        assert_eq!(count(&got[0]), count(&made));
        drop(runner);
        assert_eq!(
            Arc::strong_count(&value),
            2,
            "the producer's and this test's"
        );
    }

    #[test]
    fn a_hidden_publish_fans_out_once_it_is_shown() {
        let Graph {
            mut fixture,
            value,
            made,
            got,
        } = graph(true, false, 4);
        let mut runner = fixture.load().unwrap();
        runner.set_fans(&FANS).unwrap();
        settle(&got, &mut runner, |runner, _| {
            runner.wake(0);
            runner.tick().unwrap();
            runner.tick().unwrap();
        });
        assert_eq!(count(&got[0]), count(&made));
        drop(runner);
        assert_eq!(Arc::strong_count(&value), 2);
    }

    #[test]
    fn a_replayed_publish_fans_out_the_stored_value() {
        let Graph {
            mut fixture,
            value,
            made,
            got,
        } = graph(false, true, 4);
        let mut entries = vec![ReuseEntry::EMPTY; 4];
        let mut stamps = vec![0; 1];
        let mut stored = vec![Slot::EMPTY; 2];
        let mut inline = vec![[0; 16]; 2];
        let mut inputs = vec![InputVersion::default(); 4];
        let mut runner = fixture.load().unwrap();
        runner.set_fans(&FANS).unwrap();
        let memory = ReuseMemory {
            entries: &mut entries,
            stamps: &mut stamps,
            stored: &mut stored,
            inline: &mut inline,
            inputs: &mut inputs,
        };
        runner.set_reuse(memory, &[]).unwrap();
        runner.feed_typed(INPUT, TY, SP, Arc::new(1_u64)).unwrap();
        settle(&got, &mut runner, |runner, _| {
            runner.wake(0);
            runner.tick().unwrap();
        });
        // The body ran once; every later round replayed what it stored.
        assert_eq!(count(&made), 1);
        assert_eq!(count(&got[0]), ROUNDS);
        drop(runner);
        drop(stored);
        assert!(
            Arc::strong_count(&value) <= 4,
            "the producer's, this test's, the stored two"
        );
    }

    #[test]
    fn a_publish_held_back_by_a_full_reader_fans_out_when_there_is_room() {
        let Graph {
            mut fixture,
            value,
            made,
            got,
        } = graph(false, false, 1);
        let mut runner = fixture.load().unwrap();
        runner.set_fans(&FANS).unwrap();
        runner.set_blocking(FIRST, true).unwrap();
        settle(&got, &mut runner, |runner, at| {
            // The first reader is switched out on every other round, so its queue of one stays
            // full and the producer waits.
            let all = [0b111];
            let without_first = [0b101];
            let mask = if at % 2 == 0 { &all } else { &without_first };
            runner.set_activity(Some(mask)).unwrap();
            runner.wake(0);
            runner.wake(1);
            runner.tick().unwrap();
        });
        assert!(runner.counters().backpressure_deferrals > 0);
        assert_eq!(count(&got[0]), count(&made));
        drop(runner);
        assert_eq!(Arc::strong_count(&value), 2);
    }

    #[test]
    fn an_idle_readers_queue_holds_one_reference_a_value_up_to_its_depth_then_overflows() {
        let Graph {
            mut fixture,
            value,
            got,
            ..
        } = graph(false, false, 4);
        let mut runner = fixture.load().unwrap();
        runner.set_fans(&FANS).unwrap();
        // The second reader is switched out: the first takes every value, the second's queue
        // (four deep) fills, one reference a value.
        runner.set_activity(Some(&[0b011])).unwrap();
        for round in 1..=4 {
            runner.wake(0);
            runner.tick().unwrap();
            assert_eq!(runner.queue(SECOND).unwrap().len, round);
            assert_eq!(Arc::strong_count(&value), 2 + round as usize);
        }
        assert_eq!(count(&got[0]), 4);
        // The fifth has no room there, and the edge has no policy that gives one up.
        runner.wake(0);
        assert_eq!(
            runner.tick(),
            Err(daedalus_runner::RunError::Overflow { slot: SECOND })
        );
        // With `LatestOnly` on that edge it holds one value, and so one reference.
        runner
            .set_pressure(SECOND, daedalus_value::Pressure::LatestOnly)
            .unwrap();
        while runner.queue(SECOND).unwrap().len > 1 {
            runner.drop_oldest(SECOND).unwrap();
        }
        for _ in 0..8 {
            runner.wake(0);
            runner.tick().unwrap();
        }
        assert_eq!(runner.queue(SECOND).unwrap().len, 1);
        drop(runner);
        assert_eq!(Arc::strong_count(&value), 2);
    }
}

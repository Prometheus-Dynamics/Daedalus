mod support;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Fence, FencePoll, FenceWord, Slots, Step, StepCx, StepEnd, StepWord};
use support::{Fixture, Wiring};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const WORDS: usize = 8;
/// How many polls each fence word takes before it is ready.
static READY_AFTER: [AtomicUsize; WORDS] = [const { AtomicUsize::new(1) }; WORDS];
/// Nonzero makes the fence word fail on its next poll.
static FAIL: [AtomicUsize; WORDS] = [const { AtomicUsize::new(0) }; WORDS];
static POLLS: [AtomicUsize; WORDS] = [const { AtomicUsize::new(0) }; WORDS];
/// How many times each consumer ran.
static RUNS: [AtomicUsize; WORDS] = [const { AtomicUsize::new(0) }; WORDS];
static ORDER: Mutex<Vec<usize>> = Mutex::new(Vec::new());

struct Poller;

impl FenceWord for Poller {
    fn poll(word: usize) -> FencePoll {
        let polls = POLLS[word].fetch_add(1, Ordering::Relaxed) + 1;
        if FAIL[word].load(Ordering::Relaxed) != 0 {
            FencePoll::Failed
        } else if polls >= READY_AFTER[word].load(Ordering::Relaxed) {
            FencePoll::Ready
        } else {
            FencePoll::Pending
        }
    }
}

/// Starts work whose fence is word `word`, and returns `Pending` at once.
struct Start;

impl StepWord for Start {
    fn run(word: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Pending(Fence::of::<Poller>(word))
    }
}

/// A consumer that counts its runs under its word.
struct Consume;

impl StepWord for Consume {
    fn run(word: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        RUNS[word].fetch_add(1, Ordering::Relaxed);
        ORDER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(word);
        StepEnd::Done
    }
}

/// The same count without the order log, which allocates, for the allocation test.
struct Tally;

impl StepWord for Tally {
    fn run(word: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        RUNS[word].fetch_add(1, Ordering::Relaxed);
        StepEnd::Done
    }
}

fn reset(word: usize, ready_after: usize) {
    READY_AFTER[word].store(ready_after, Ordering::Relaxed);
    FAIL[word].store(0, Ordering::Relaxed);
    POLLS[word].store(0, Ordering::Relaxed);
    RUNS[word].store(0, Ordering::Relaxed);
}

/// Step 0 starts work with fence `a`, then wakes step 2. Step 1 is `b` and wakes step 3.
/// Steps 2 and 3 are consumers, counted under words `a` and `b`.
fn two_producers(a: usize, b: usize) -> Fixture {
    let mut wiring = Wiring::default();
    wiring.pending_step(&[2], &[], &[]);
    wiring.pending_step(&[3], &[], &[]);
    wiring.step(&[], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![
        Step::of::<Start>(a),
        Step::of::<Start>(b),
        Step::of::<Consume>(a),
        Step::of::<Consume>(b),
    ];
    Fixture::new(steps, wiring, Vec::new())
}

#[test]
fn consumers_wait_for_ready() {
    reset(0, 3);
    let mut fixture = {
        let mut wiring = Wiring::default();
        wiring.pending_step(&[1], &[], &[]);
        wiring.step(&[], &[], &[]);
        Fixture::new(
            vec![Step::of::<Start>(0), Step::of::<Consume>(0)],
            wiring,
            Vec::new(),
        )
    };
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    let first = runner.tick().unwrap();
    assert_eq!(first.step_calls, 1);
    assert_eq!(runner.pending(), 1);
    assert_eq!(RUNS[0].load(Ordering::Relaxed), 0);
    let second = runner.tick().unwrap();
    assert!(!second.idle, "a waiting fence keeps the tick from idling");
    assert_eq!(second.step_calls, 0);
    assert_eq!(
        RUNS[0].load(Ordering::Relaxed),
        0,
        "consumer not woken before Ready"
    );
    runner.tick().unwrap();
    assert_eq!(RUNS[0].load(Ordering::Relaxed), 0);
    let ready = runner.tick().unwrap();
    assert_eq!(RUNS[0].load(Ordering::Relaxed), 1, "consumer ran on Ready");
    assert_eq!(ready.step_calls, 1);
    assert_eq!(runner.pending(), 0);
}

#[test]
fn ready_fences_wake_in_index_order() {
    reset(2, 1);
    reset(3, 1);
    ORDER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let mut fixture = two_producers(2, 3);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    runner.wake(1);
    runner.tick().unwrap();
    runner.tick().unwrap();
    assert_eq!(
        *ORDER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        vec![2, 3]
    );
}

#[test]
fn a_failed_fence_faults_only_its_own_step() {
    reset(4, 1);
    reset(5, 1);
    FAIL[4].store(1, Ordering::Relaxed);
    let mut fixture = two_producers(4, 5);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    runner.wake(1);
    runner.tick().unwrap();
    let report = runner.tick().unwrap();
    assert_eq!(report.faults, 1);
    assert_eq!(
        RUNS[4].load(Ordering::Relaxed),
        0,
        "failed step's consumer not woken"
    );
    assert_eq!(
        RUNS[5].load(Ordering::Relaxed),
        1,
        "other step's consumer woken"
    );
    assert_eq!(runner.pending(), 0);
}

#[test]
fn registering_and_polling_allocate_nothing() {
    reset(6, 1);
    let mut wiring = Wiring::default();
    wiring.pending_step(&[1], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![Step::of::<Start>(6), Step::of::<Tally>(6)];
    let mut fixture = Fixture::new(steps, wiring, Vec::new());
    let mut runner = fixture.load().unwrap();
    // Warm up once, outside the count.
    runner.wake(0);
    runner.tick().unwrap();
    runner.tick().unwrap();
    let ((), allocations) = counted(|| {
        for _ in 0..1_000 {
            runner.wake(0);
            runner.tick().unwrap();
            runner.tick().unwrap();
        }
    });
    assert_eq!(allocations, 0);
    assert_eq!(RUNS[6].load(Ordering::Relaxed), 1_001);
}

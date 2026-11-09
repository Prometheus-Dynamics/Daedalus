//! A body returns `Pending` with a fence backed by a future. The future completes on another
//! thread, and the consumers wake once.

mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::thread;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{FutureFence, SlotKind, SlotSpec};
use support::{Fixture, Wiring};

/// A future that completes when a flag is set from another thread.
struct WaitFor(Arc<AtomicBool>);

impl Future for WaitFor {
    type Output = bool;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<bool> {
        if self.0.load(Ordering::Acquire) {
            Poll::Ready(true)
        } else {
            Poll::Pending
        }
    }
}

struct StartsWork {
    fence: FutureFence,
    done: Arc<AtomicBool>,
}

impl OwnedStep for StartsWork {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Pending(self.fence.start(WaitFor(Arc::clone(&self.done))))
    }
}

struct CountsCalls(Arc<AtomicU32>);

impl OwnedStep for CountsCalls {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.0.fetch_add(1, Ordering::Relaxed);
        StepEnd::Done
    }
}

#[test]
fn a_future_completed_on_another_thread_wakes_consumers_once() {
    let done = Arc::new(AtomicBool::new(false));
    let consumer_calls = Arc::new(AtomicU32::new(0));
    let mut wiring = Wiring::default();
    wiring.pending_step(&[1], &[], &[]);
    wiring.step(&[], &[], &[]);
    let steps = vec![
        Step::boxed(StartsWork {
            fence: FutureFence::new(),
            done: Arc::clone(&done),
        }),
        Step::boxed(CountsCalls(Arc::clone(&consumer_calls))),
    ];
    let specs = vec![SlotSpec {
        cap: 1,
        kind: SlotKind::Fifo,
    }];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(runner.pending(), 1);
    assert_eq!(consumer_calls.load(Ordering::Relaxed), 0);

    // Still pending: ticking doesn't wake the consumer.
    runner.tick().unwrap();
    assert_eq!(consumer_calls.load(Ordering::Relaxed), 0);

    let setter = {
        let done = Arc::clone(&done);
        thread::spawn(move || done.store(true, Ordering::Release))
    };
    setter.join().unwrap();

    // The tick that polls Ready wakes the consumer once.
    runner.tick().unwrap();
    assert_eq!(consumer_calls.load(Ordering::Relaxed), 1);
    assert_eq!(runner.pending(), 0);
    runner.tick().unwrap();
    assert_eq!(consumer_calls.load(Ordering::Relaxed), 1);
}

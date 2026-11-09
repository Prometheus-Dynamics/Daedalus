//! A warm parallel tick allocates nothing on any thread. This binary counts every allocation of the
//! process, so it holds one test only.

#![cfg(feature = "threads")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_abi::Step;
use daedalus_runner::{Lanes, Pool, SlotKind, SlotSpec};
use support::{Fixture, Link, tagged};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Everywhere;

// SAFETY: forwards every call to the system allocator, and only adds a count.
unsafe impl GlobalAlloc for Everywhere {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller's contract is passed through unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed through unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Everywhere = Everywhere;

#[test]
fn warm_parallel_ticks_allocate_nothing_on_any_thread() {
    let (wiring, words) = support::chain(16);
    let steps: Vec<Step> = words.into_iter().map(Step::of::<Link>).collect();
    let specs = vec![
        SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo
        };
        17
    ];
    let mut fixture = Fixture::new(steps, wiring, specs);
    let mut runner = fixture.load().unwrap();
    let mut lanes = Lanes::new(&runner, Arc::new(Pool::new(4)), &[]).unwrap();
    for round in 0..200usize {
        runner.push(0, tagged(round)).unwrap();
        runner.wake(0);
        runner.tick_parallel(&mut lanes).unwrap();
    }
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for round in 0..5_000usize {
        runner.push(0, tagged(round)).unwrap();
        runner.wake(0);
        assert_eq!(runner.tick_parallel(&mut lanes).unwrap().step_calls, 16);
    }
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed) - before, 0);
}

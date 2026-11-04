//! Executors ticking on several threads with every Daedalus crate's `std` off: a steady-state
//! tick allocates nothing, on one thread and on each of several. Primitive payloads (the host's
//! input, the node's output) are held inline, and each executor owns its port buffer pool (there
//! are no thread-locals), so no thread's ticks depend on another's.
#![allow(clippy::print_stderr)] // the per-tick counts

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Barrier;

use daedalus_nostd_smoke::{counter_clock, engine_graph};

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards to `System`, counting this thread's allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        // SAFETY: as `System::alloc`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as `System::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        // SAFETY: as `System::realloc`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const TICKS: usize = 4000;
const THREADS: usize = 4;

/// This thread's allocations over `TICKS` push -> tick -> take rounds of a warmed-up graph,
/// started together with the other threads at `start`.
fn tick_allocations(start: &Barrier) -> usize {
    let mut graph = engine_graph(counter_clock()).expect("graph compiles");
    for x in 0..16i64 {
        graph.push("in", x);
        graph.tick().expect("tick");
        assert!(graph.take_payload("out").is_some());
    }
    start.wait();
    let before = ALLOCS.with(Cell::get);
    for x in 0..TICKS as i64 {
        graph.push("in", x);
        graph.tick().expect("tick");
        let out = graph.take_payload("out").expect("output");
        assert_eq!(out.get_ref::<i64>(), Some(&(x + 1)));
    }
    ALLOCS.with(Cell::get) - before
}

#[test]
fn ticks_allocate_nothing_on_one_thread_or_on_four() {
    let alone = tick_allocations(&Barrier::new(1));
    assert_eq!(alone, 0, "a tick allocates on one thread");
    let start = Barrier::new(THREADS);
    let parallel: Vec<usize> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..THREADS)
            .map(|_| scope.spawn(|| tick_allocations(&start)))
            .collect();
        threads.into_iter().map(|t| t.join().unwrap()).collect()
    });
    eprintln!("alone: {alone}; in parallel: {parallel:?} allocations over {TICKS} ticks");
    for allocations in parallel {
        assert_eq!(
            allocations, 0,
            "a thread's ticks allocate on {THREADS} threads"
        );
    }
}

//! Executors ticking on several threads with every Daedalus crate's `std` off: a steady-state
//! tick allocates nothing at each metrics level up to `Hardware`, on one thread and on each of
//! several, once the tick's telemetry is handed back with `HostGraph::recycle_telemetry`. Primitive
//! payloads (the host's input, the node's output) are held inline, and each executor owns its port
//! buffer pool (there are no thread-locals), so no thread's ticks depend on another's.
//!
//! `Profile` and `Trace` are not in the set: their data-lifecycle records own `String`s (port,
//! payload, adapter steps, detail), so each recorded event allocates. A caller that does not hand
//! its telemetry back rebuilds it on every tick (see `ticks_that_drop_their_telemetry_rebuild_it`).
#![allow(clippy::print_stderr)] // the per-tick counts

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Barrier;

use daedalus_engine::MetricsLevel;
use daedalus_nostd_smoke::{counter_clock, engine_graph_at};

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
/// The levels whose steady-state tick allocates nothing once its telemetry is recycled.
const ZERO_LEVELS: [MetricsLevel; 5] = [
    MetricsLevel::Off,
    MetricsLevel::Basic,
    MetricsLevel::Timing,
    MetricsLevel::Detailed,
    MetricsLevel::Hardware,
];

/// This thread's allocations over `TICKS` push -> tick -> take rounds of a warmed-up graph at
/// `level`, started together with the other threads at `start`. With `recycle`, each tick's
/// telemetry is handed back before the next tick; otherwise it is dropped.
fn tick_allocations(level: MetricsLevel, start: &Barrier, recycle: bool) -> usize {
    let mut graph = engine_graph_at(counter_clock(), level).expect("graph compiles");
    for x in 0..16i64 {
        graph.push("in", x);
        let telemetry = graph.tick().expect("tick");
        if recycle {
            graph.recycle_telemetry(telemetry);
        }
        assert!(graph.take_payload("out").is_some());
    }
    start.wait();
    let before = ALLOCS.with(Cell::get);
    for x in 0..TICKS as i64 {
        graph.push("in", x);
        let telemetry = graph.tick().expect("tick");
        if recycle {
            graph.recycle_telemetry(telemetry);
        }
        let out = graph.take_payload("out").expect("output");
        assert_eq!(out.get_ref::<i64>(), Some(&(x + 1)));
    }
    ALLOCS.with(Cell::get) - before
}

#[test]
fn ticks_allocate_nothing_at_each_level_on_one_thread_or_on_four() {
    for level in ZERO_LEVELS {
        let alone = tick_allocations(level, &Barrier::new(1), true);
        assert_eq!(alone, 0, "{level:?}: a tick allocates on one thread");
        let start = Barrier::new(THREADS);
        let parallel: Vec<usize> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..THREADS)
                .map(|_| scope.spawn(|| tick_allocations(level, &start, true)))
                .collect();
            threads.into_iter().map(|t| t.join().unwrap()).collect()
        });
        eprintln!(
            "{level:?}: alone {alone}; on {THREADS} threads {parallel:?} allocations over {TICKS} ticks"
        );
        for allocations in parallel {
            assert_eq!(
                allocations, 0,
                "{level:?}: a thread's ticks allocate on {THREADS} threads"
            );
        }
    }
}

/// A caller that drops each tick's telemetry instead of recycling it pays for the rebuild: at
/// `Basic` the node-metrics table is reserved again every tick, one allocation per tick when the
/// `metrics` feature is built (none without it), so the rebuild costs at most one per tick.
#[test]
fn ticks_that_drop_their_telemetry_rebuild_it() {
    let dropped = tick_allocations(MetricsLevel::Basic, &Barrier::new(1), false);
    assert!(
        dropped <= TICKS,
        "without recycling, Basic rebuilds at most its node-metrics table per tick ({dropped} over {TICKS} ticks)"
    );
}

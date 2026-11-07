//! A counting global allocator for this crate's unit tests: the heap allocations a closure makes
//! on the calling thread. Counting is per thread and off by default, so other threads (wgpu's
//! poll and watcher threads) and the rest of the test run are not counted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

pub(crate) struct CountingAlloc;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

fn bump() {
    // `try_with`: during thread teardown the slot may be gone; then nothing is counted.
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            let _ = COUNT.try_with(|n| n.set(n.get() + 1));
        }
    });
}

// SAFETY: every method forwards to `System` with the same layout, after recording the call.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with `layout` (every method forwards to it).
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Run `f` on this thread and return its result with the number of heap allocations it made.
#[cfg_attr(
    not(all(feature = "gpu-dmabuf", target_os = "linux")),
    allow(dead_code)
)]
pub(crate) fn allocations_in<R>(f: impl FnOnce() -> R) -> (R, u64) {
    COUNT.with(|n| n.set(0));
    COUNTING.with(|on| on.set(true));
    let out = f();
    COUNTING.with(|on| on.set(false));
    (out, COUNT.with(|n| n.get()))
}

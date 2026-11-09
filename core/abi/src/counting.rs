//! A counting allocator for tests that prove a path allocates nothing. It lives here because
//! other crates forbid `unsafe`, and a global allocator can't be written without it.

#![cfg(feature = "count-alloc")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Install with `#[global_allocator]`. Counts per thread, so parallel tests don't disturb it.
pub struct CountingAlloc;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn count() {
    // During thread teardown the counter may be gone, and nothing is counting then.
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
}

// SAFETY: forwards every call to `System` unchanged and only adds a counter.
unsafe impl GlobalAlloc for CountingAlloc {
    // SAFETY: the caller's contract is passed straight through.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.alloc(layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.dealloc(ptr, layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Runs `body` and returns its result with how many allocations this thread made.
pub fn counted<R>(body: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATIONS.with(Cell::get);
    let out = body();
    (out, ALLOCATIONS.with(Cell::get) - before)
}

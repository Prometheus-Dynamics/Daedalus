//! The `alloc-count` build's counting allocator: every allocation this cdylib makes, read through
//! `daedalus_surface_allocations`. The shared-mode test takes the difference across warm frames.
//! Plugin libraries loaded into the surface have their own allocators and are not counted here.
//!
//! The crate's own tests use it too, counting per thread ([`thread_allocations`]), so a test
//! measures its own calls while other tests run beside it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static THREAD_ALLOCS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Counts one allocation.
#[inline(always)]
fn count() {
    ALLOCS.fetch_add(1, Relaxed);
    #[cfg(test)]
    // A thread being torn down has no counter left: its allocations are not counted.
    let _ = THREAD_ALLOCS.try_with(|count| count.set(count.get() + 1));
}

// Safety: forwards to the system allocator, counting allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // Safety: forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // Safety: forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations (and reallocations) this surface has made.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_surface_allocations() -> u64 {
    ALLOCS.load(Relaxed)
}

/// Allocations (and reallocations) made on the calling thread (the frame tests, Linux only).
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn thread_allocations() -> u64 {
    THREAD_ALLOCS.with(std::cell::Cell::get)
}

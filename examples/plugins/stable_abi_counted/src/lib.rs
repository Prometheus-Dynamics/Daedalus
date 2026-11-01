//! The `stable_abi` plugin's `cdylib` with a counting global allocator: the facade's
//! `dylib_stable_allocations` test reads the plugin side's allocations (its own allocator's,
//! invisible to the host's) through the exported `stable_abi_allocations`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

// Links the plugin and its exported entry points.
pub use daedalus_plugins_stable_abi::StableAbiPlugin;

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// Safety: forwards to the system allocator, counting allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations (and reallocations) the plugin has made.
#[unsafe(no_mangle)]
pub extern "C" fn stable_abi_allocations() -> u64 {
    ALLOCS.load(Relaxed)
}

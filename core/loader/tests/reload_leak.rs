//! Reloading a plugin many times does not grow the heap, and does not promote its declarations
//! again. A leak anywhere in a load, a join, or a retirement shows here as bytes that stay live.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_loader::{Loader, plugin_tables_held};
use daedalus_registry::{Registry, promoted_declarations};
use support::copy_fixture;

/// Counts the bytes this thread holds live: allocations add, frees subtract. Per thread, so the
/// test harness's other threads do not disturb the count.
struct Tracking;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
}

fn live_bytes() -> isize {
    LIVE.with(Cell::get)
}

fn add(delta: isize) {
    // During thread teardown the counter may be gone, and nothing is counted then.
    let _ = LIVE.try_with(|live| live.set(live.get() + delta));
}

// SAFETY: forwards every call to `System` unchanged, and only adds to a thread-local counter.
unsafe impl GlobalAlloc for Tracking {
    // SAFETY: the caller's contract is passed straight through.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add(layout.size() as isize);
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.alloc(layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add(-(layout.size() as isize));
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.dealloc(ptr, layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add(new_size as isize - layout.size() as isize);
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Tracking = Tracking;

/// One full reload: open the library, join its nodes into a registry, and drop everything. The
/// plugin's generation retires when this returns.
fn reload(loader: &mut Loader) {
    let plugin = loader.open(copy_fixture("reload")).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    assert!(registry.entry("fixture.pass").is_some());
}

#[test]
fn reloading_a_plugin_many_times_does_not_grow() {
    let mut loader = Loader::new();
    // The first loads fill the caches the loader and the registry keep for the process.
    for _ in 0..3 {
        reload(&mut loader);
    }
    let live = live_bytes();
    let promoted = promoted_declarations();
    let tables = plugin_tables_held();

    for _ in 0..200 {
        reload(&mut loader);
    }

    assert_eq!(
        promoted_declarations(),
        promoted,
        "reloading an unchanged plugin must not promote its declarations again"
    );
    assert_eq!(
        plugin_tables_held(),
        tables,
        "every plugin entry must be released once it is read"
    );
    assert_eq!(
        live_bytes(),
        live,
        "reloading an unchanged plugin must not keep any memory live"
    );
}

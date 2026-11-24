//! Reloading a plugin many times does not grow the heap, and does not promote its declarations
//! again. A leak anywhere in a load, a join, or a retirement shows here as bytes that stay live.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Mutex;

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

/// The rounds a check reloads for. Every count is compared exactly, so a leak of one byte, one
/// table or one declaration a reload shows after one round; the rounds are there for a leak that
/// only shows on some reloads. `a_planted_leak_of_one_byte_a_reload_is_caught` shows the count
/// catches the smallest leak in this many.
const ROUNDS: usize = 20;

/// The counts of tables and declarations are the process's, so the tests take turns.
static SERIAL: Mutex<()> = Mutex::new(());

/// One full reload: open the library, join its nodes into a registry, and drop everything. The
/// plugin's generation retires when this returns.
fn reload(loader: &mut Loader, name: &str) {
    let plugin = loader.open(copy_fixture(name)).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    assert!(registry.entry("fixture.pass").is_some());
}

/// What `ROUNDS` reloads leave behind, after three that fill the caches the loader and the
/// registry keep for the process: live bytes, promoted declarations and held plugin tables.
fn growth(mut once: impl FnMut(&mut Loader)) -> [isize; 3] {
    let mut loader = Loader::new();
    for _ in 0..3 {
        once(&mut loader);
    }
    let counts = || {
        [
            live_bytes(),
            promoted_declarations() as isize,
            plugin_tables_held() as isize,
        ]
    };
    let before = counts();
    for _ in 0..ROUNDS {
        once(&mut loader);
    }
    let after = counts();
    [0, 1, 2].map(|i| after[i] - before[i])
}

#[test]
fn reloading_a_plugin_many_times_does_not_grow() {
    let _turn = SERIAL.lock().unwrap_or_else(|held| held.into_inner());
    let [live, promoted, tables] = growth(|loader| reload(loader, "reload"));
    assert_eq!(
        promoted, 0,
        "reloading an unchanged plugin must not promote its declarations again"
    );
    assert_eq!(
        tables, 0,
        "every plugin entry must be released once it is read"
    );
    assert_eq!(
        live, 0,
        "reloading an unchanged plugin must not keep any memory live"
    );
}

#[test]
fn a_planted_leak_of_one_byte_a_reload_is_caught() {
    let _turn = SERIAL.lock().unwrap_or_else(|held| held.into_inner());
    let [live, promoted, tables] = growth(|loader| {
        reload(loader, "reload-planted");
        std::mem::forget(vec![0u8; 1]);
    });
    assert_eq!(live, ROUNDS as isize, "one byte a reload, kept live");
    assert_eq!((promoted, tables), (0, 0));
}

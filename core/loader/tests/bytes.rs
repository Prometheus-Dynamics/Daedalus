//! A plugin's bytes reach the host without a copy. The host allocates a small header for them, not
//! the bytes; a value keeps the plugin's library open; the plugin frees the bytes when the last value
//! of them is dropped, and a reload leaves a held value readable.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Mutex, MutexGuard};

use daedalus_loader::{Loader, bytes_copied, bytes_released};
use support::copy_fixture;

/// Counts, per thread, the bytes this thread asks the allocator for, and how many times.
struct Counting;

thread_local! {
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards every call to `System` unchanged, and only adds to thread-local counters.
unsafe impl GlobalAlloc for Counting {
    // SAFETY: the caller's contract is passed straight through.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + layout.size()));
        let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.alloc(layout) }
    }

    // SAFETY: the caller's contract is passed straight through.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `body`, and returns its result with the bytes and the calls this thread made to allocate.
fn measured<R>(body: impl FnOnce() -> R) -> (R, usize, usize) {
    let (bytes, calls) = (BYTES.with(Cell::get), CALLS.with(Cell::get));
    let out = body();
    (
        out,
        BYTES.with(Cell::get) - bytes,
        CALLS.with(Cell::get) - calls,
    )
}

/// The tests share the process-wide counts of copied and released bytes, so they run one at a time.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One megabyte, the size the fixture makes for index 0.
const SIZE: usize = 1 << 20;

#[test]
fn the_plugin_bytes_cross_without_a_copy() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("bytes-nocopy")).expect("opens");
    let copied = bytes_copied();
    let (value, bytes_asked, _) = measured(|| plugin.produce(0).expect("the plugin makes bytes"));
    assert_eq!(value.bytes().len(), SIZE);
    assert!(value.bytes().iter().all(|byte| *byte == 7));
    assert_eq!(bytes_copied(), copied, "a hand-over copies no bytes");
    // The host allocates a header for the value. The megabyte stays where the plugin put it, so the
    // host asks for far less than one megabyte.
    assert!(
        bytes_asked < SIZE / 64,
        "a hand-over allocates a header, not the bytes: {bytes_asked} bytes asked for"
    );
    let mut shares = Vec::with_capacity(100);
    let ((), clone_bytes, clone_calls) = measured(|| {
        for _ in 0..100 {
            shares.push(value.clone());
        }
    });
    assert_eq!(
        (clone_bytes, clone_calls),
        (0, 0),
        "sharing the bytes allocates nothing"
    );
}

#[test]
fn the_plugin_frees_its_bytes_on_the_last_drop_and_the_library_stays_open() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("bytes-release")).expect("opens");
    let probe = plugin.generation_probe();
    let released = bytes_released();
    let value = plugin.produce(0).expect("the plugin makes bytes");
    let more = value.clone();
    drop(plugin);
    drop(value);
    assert_eq!(
        bytes_released(),
        released,
        "one value still holds the bytes"
    );
    assert!(
        probe.upgrade().is_some(),
        "the value holds the library open"
    );
    drop(more);
    assert_eq!(
        bytes_released(),
        released + 1,
        "the last drop frees the bytes"
    );
    assert!(
        probe.upgrade().is_none(),
        "the library closes with its last value"
    );
}

#[test]
fn a_reload_leaves_a_held_value_readable_and_frees_it_later() {
    let _serial = serial();
    let mut loader = Loader::new();
    let first = loader.open(copy_fixture("bytes-reload-1")).expect("opens");
    let probe = first.generation_probe();
    let held = first.produce(0).expect("the first plugin makes bytes");
    drop(first);
    let released = bytes_released();

    // A reload: the same plugin opened again, as a new generation, while the old value is held.
    let second = loader
        .open(copy_fixture("bytes-reload-2"))
        .expect("reopens");
    assert_eq!(second.generation().number(), 2);
    let fresh = second.produce(0).expect("the second plugin makes bytes");

    assert_eq!(held.bytes().len(), SIZE, "the held value is still readable");
    assert!(held.bytes().iter().all(|byte| *byte == 7));
    assert!(
        probe.upgrade().is_some(),
        "the old generation stays open for the held value"
    );
    assert_eq!(
        bytes_released(),
        released,
        "nothing is freed while the value is held"
    );

    drop(held);
    assert_eq!(
        bytes_released(),
        released + 1,
        "the old bytes are freed with the value"
    );
    assert!(
        probe.upgrade().is_none(),
        "the old generation closes with its value"
    );
    assert_eq!(
        fresh.bytes().len(),
        SIZE,
        "the new generation's value is unaffected"
    );
}

#[test]
fn a_plugin_that_makes_no_bytes_for_an_index_is_a_typed_error() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("bytes-none")).expect("opens");
    assert!(plugin.produce(1).is_err(), "index 1 is not made");
}

#[test]
fn copies_are_counted_when_the_host_copies() {
    let _serial = serial();
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("bytes-copy")).expect("opens");
    let copied = bytes_copied();
    let value = plugin.share(&[1; 1024]);
    assert_eq!(value.bytes().len(), 1024);
    assert_eq!(bytes_copied(), copied + 1024, "a host copy is counted");
}

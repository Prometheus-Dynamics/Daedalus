//! A warm recycled payload allocates nothing: `Payload::providing_recycled` takes an idle node,
//! clones and lent handles retain it, and the last release gives it back.
//!
//! Counts heap allocations made on the calling thread, so it is robust against other tests running
//! in parallel.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_transport::{Payload, ProvideForeign, RecyclePool, Residency};

struct CountingAlloc;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

daedalus_transport::foreign_interface! {
    pub interface FrameLikeInterface("test:frame_like", version = 1);
    pub struct FrameLikeVTable {
        pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
    }
}

/// A frame whose pixels are inline: the owner itself allocates nothing.
struct Frame {
    width: u32,
    #[allow(dead_code, reason = "the pixels fill the frame, they are not read")]
    pixels: [u8; 256],
}

static DROPS: AtomicUsize = AtomicUsize::new(0);

impl Drop for Frame {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe extern "C" fn frame_width(data: *const c_void) -> u32 {
    // Safety: only used in `Frame`'s vtable, over a live `Frame`.
    unsafe { (*data.cast::<Frame>()).width }
}

// Safety: `frame_width` reads a `Frame`.
unsafe impl ProvideForeign<FrameLikeInterface> for Frame {
    fn vtable() -> &'static FrameLikeVTable {
        &FrameLikeVTable { width: frame_width }
    }
}

fn frame(width: u32) -> Frame {
    Frame {
        width,
        pixels: [width as u8; 256],
    }
}

/// One payload with a clone and a lent handle, kept in `window` for a few frames.
fn push(pool: &RecyclePool<Frame>, window: &mut VecDeque<Payload>, width: u32) {
    let payload = Payload::providing_recycled::<Frame, FrameLikeInterface>(
        "test:frame",
        pool,
        frame(width),
        Residency::Cpu,
    );
    let clone = payload.clone();
    let lent = payload.to_foreign_handle().expect("a handle of the frame");
    let view = lent.view::<FrameLikeInterface>().expect("same interface");
    // Safety: the view checked the interface.
    assert_eq!(unsafe { (view.vtable().width)(view.data()) }, width);
    drop(lent);
    window.push_back(payload);
    window.push_back(clone);
    while window.len() > 8 {
        window.pop_front();
    }
}

#[test]
fn a_warm_recycled_frame_allocates_nothing_and_drops_its_owner_once() {
    let pool = RecyclePool::<Frame>::new();
    // The window is sized before the measured span; the first frames grow the pool.
    let mut window = VecDeque::with_capacity(16);
    for width in 0..16 {
        push(&pool, &mut window, width);
    }
    window.clear();
    assert!(pool.idle() >= 1, "released frames go back to the pool");

    let drops_before = DROPS.load(Ordering::SeqCst);
    let before = allocations();
    for width in 0..10_000 {
        push(&pool, &mut window, width);
    }
    let made = allocations() - before;
    assert_eq!(made, 0, "a warm frame allocates nothing");
    // The window's frames are still held: 8 entries, 4 frames plus their clones and the lent
    // handles already released.
    assert_eq!(DROPS.load(Ordering::SeqCst) - drops_before, 10_000 - 4);
    window.clear();
    assert_eq!(DROPS.load(Ordering::SeqCst) - drops_before, 10_000);
    assert!(
        (1..=5).contains(&pool.idle()),
        "the pool holds at most the frames in flight: {}",
        pool.idle()
    );
}

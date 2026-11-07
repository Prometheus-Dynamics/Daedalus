//! A recycled owner on the stable plugin path: `Arena::encode_foreign` lends it as its owner's
//! interface, through a handle the arena retains (as it does for a `providing` payload), so the
//! pooled node lives as long as the arena holds it, and goes back to its pool when the arena lets
//! go.
#![cfg(feature = "dylib-plugins")]

use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use daedalus::dylib::stable::Arena;
use daedalus::transport::{Payload, ProvideForeign, RecyclePool, Residency};

daedalus::transport::foreign_interface! {
    pub interface LentFrame("test:lent_frame", version = 1);
    pub struct LentFrameVTable {
        pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
    }
}

struct Lent {
    width: u32,
}

static DROPS: AtomicUsize = AtomicUsize::new(0);

impl Drop for Lent {
    fn drop(&mut self) {
        DROPS.fetch_add(1, SeqCst);
    }
}

unsafe extern "C" fn lent_width(data: *const c_void) -> u32 {
    // Safety: only used in `Lent`'s vtable, over a live `Lent`.
    unsafe { (*data.cast::<Lent>()).width }
}

// Safety: `lent_width` reads a `Lent`.
unsafe impl ProvideForeign<LentFrame> for Lent {
    fn vtable() -> &'static LentFrameVTable {
        &LentFrameVTable { width: lent_width }
    }
}

#[test]
fn a_recycled_frame_lends_its_interface_and_the_arena_keeps_its_node() {
    let pool = RecyclePool::<Lent>::new();
    let payload = Payload::providing_recycled::<Lent, LentFrame>(
        "test:lent",
        &pool,
        Lent { width: 640 },
        Residency::Cpu,
    );
    let mut arena = Arena::default();
    let value = arena.encode_foreign(&payload).expect("a foreign value");
    // Safety: the arena holds the handle the value points at until it is cleared or dropped.
    let handle = unsafe { value.as_handle() }.expect("a handle value");
    assert!(
        value.from_sender(),
        "the sending build created the frame's handle"
    );
    let view = handle.view::<LentFrame>().expect("the owner's interface");
    // Safety: the view checked the interface.
    assert_eq!(unsafe { (view.vtable().width)(view.data()) }, 640);

    drop(payload);
    assert_eq!(
        DROPS.load(SeqCst),
        0,
        "the arena's retained handle keeps the frame"
    );
    arena.clear();
    assert_eq!(DROPS.load(SeqCst), 1, "the arena's release is the last one");
    assert_eq!(pool.idle(), 1, "the node is idle again");
}

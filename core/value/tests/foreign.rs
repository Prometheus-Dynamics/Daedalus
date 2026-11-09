use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_abi::{foreign_interface, provide_foreign};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::Value;
use daedalus_value::foreign::*;

foreign_interface! {
    interface Sized32("test:sized", version = 1);
    struct SizedTable {
        size: extern "C" fn(*const core::ffi::c_void) -> u32,
    }
}

foreign_interface! {
    interface Other("test:other", version = 1);
    struct OtherTable {
        size: extern "C" fn(*const core::ffi::c_void) -> u32,
    }
}

struct Thing {
    size: u32,
    drops: Arc<AtomicUsize>,
}

impl Drop for Thing {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

extern "C" fn thing_size(_: *const core::ffi::c_void) -> u32 {
    77
}

static TABLE: SizedTable = SizedTable { size: thing_size };

provide_foreign!(Thing: Sized32 = &TABLE);

fn thing() -> (ForeignHandle, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Thing {
        size: 9,
        drops: drops.clone(),
    });
    assert_eq!(owner.size, 9);
    (ForeignHandle::from_arc::<Thing, Sized32>(owner), drops)
}

#[test]
fn a_view_checks_the_interface() {
    let (handle, _) = thing();
    let view = handle.view::<Sized32>().unwrap();
    assert_eq!((view.vtable().size)(view.data()), 77);
    let mismatch = handle.view::<Other>().unwrap_err();
    assert_eq!(mismatch.found.key(), "test:sized");
    assert_eq!(mismatch.expected.key(), "test:other");
}

#[test]
fn interfaces_compare_by_content() {
    let a = ForeignInterfaceInfo::new("k", 1, "f: u32;", 8, 8);
    let spaced = ForeignInterfaceInfo::new("k", 1, " f:  u32 ; ", 8, 8);
    assert_eq!(
        a, spaced,
        "whitespace in the signature doesn't change the hash"
    );
    assert_ne!(a, ForeignInterfaceInfo::new("k", 2, "f: u32;", 8, 8));
    assert_ne!(a, ForeignInterfaceInfo::new("j", 1, "f: u32;", 8, 8));
    assert_ne!(a, ForeignInterfaceInfo::new("k", 1, "f: u32;", 16, 8));
    assert_ne!(a, ForeignInterfaceInfo::new("k", 1, "g: u32;", 8, 8));
}

#[test]
fn layout_hash_is_pinned() {
    assert_eq!(foreign_layout_hash("", 0, 0), 0x88201fb960ff6465);
}

#[test]
fn a_value_owned_through_a_handle_is_released_exactly_once() {
    let (handle, drops) = thing();
    let a = Value::foreign(TypeIndex(1), SpaceIndex(2), handle);
    let b = a.clone_inline();
    assert!(b.foreign_handle().unwrap().view::<Sized32>().is_ok());
    drop(a);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(b);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cloned_handles_each_hold_a_reference() {
    let (handle, drops) = thing();
    let second = handle.clone();
    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(second.view::<Sized32>().is_ok());
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_plain_value_has_no_handle() {
    let v = Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: daedalus_value::Owner::Static(b"x"),
    };
    assert!(v.foreign_handle().is_none());
}

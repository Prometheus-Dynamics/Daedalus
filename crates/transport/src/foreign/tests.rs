use super::*;
use crate::{BoundaryCapabilities, Residency};
use core::sync::atomic::{AtomicUsize, Ordering};

static DROPS: AtomicUsize = AtomicUsize::new(0);

struct Counter {
    value: i64,
    /// Count drops only for this test's values (tests run in parallel).
    tracked: bool,
}

impl Drop for Counter {
    fn drop(&mut self) {
        if self.tracked {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

crate::foreign_interface! {
    pub interface CounterV1("test:counter", version = 1);
    pub struct CounterVTable {
        pub value: unsafe extern "C" fn(data: *const c_void) -> i64,
    }
}

// Same key and version, different declaration: what a stale or edited copy looks like.
crate::foreign_interface! {
    pub interface CounterEdited("test:counter", version = 1);
    pub struct CounterEditedVTable {
        pub value: unsafe extern "C" fn(data: *const c_void) -> i32,
    }
}

crate::foreign_interface! {
    pub interface CounterV2("test:counter", version = 2);
    pub struct CounterV2VTable {
        pub value: unsafe extern "C" fn(data: *const c_void) -> i64,
    }
}

unsafe extern "C" fn counter_value(data: *const c_void) -> i64 {
    // Safety: only used in `Counter`'s vtable.
    unsafe { (*data.cast::<Counter>()).value }
}

// Safety: `counter_value` reads a `Counter`.
unsafe impl ProvideForeign<CounterV1> for Counter {
    fn vtable() -> &'static CounterVTable {
        &CounterVTable {
            value: counter_value,
        }
    }
}

fn read(handle: &ForeignHandle) -> i64 {
    let view = handle.view::<CounterV1>().expect("same interface");
    // Safety: the view checked the interface.
    unsafe { (view.vtable().value)(view.data()) }
}

#[test]
fn handles_share_the_owner_and_balance_retain_release() {
    let before = DROPS.load(Ordering::SeqCst);
    let value = Arc::new(Counter {
        value: 41,
        tracked: true,
    });
    let handle = ForeignHandle::from_arc::<_, CounterV1>(value.clone());
    assert_eq!(handle.data(), Arc::as_ptr(&value).cast());
    assert_eq!(Arc::strong_count(&value), 2);
    let clones: Vec<_> = (0..3).map(|_| handle.clone()).collect();
    assert_eq!(Arc::strong_count(&value), 5);
    assert!(clones.iter().all(|clone| read(clone) == 41));
    drop(clones);
    assert_eq!(Arc::strong_count(&value), 2);
    drop(value);
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        before,
        "the handle keeps the owner alive"
    );
    assert_eq!(read(&handle), 41);
    drop(handle);
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        before + 1,
        "dropped exactly once"
    );
}

#[test]
fn views_reject_other_versions_and_layouts() {
    let handle = ForeignHandle::from_arc::<_, CounterV1>(Arc::new(Counter {
        value: 1,
        tracked: false,
    }));
    let edited = handle.view::<CounterEdited>().unwrap_err();
    assert_eq!(edited.expected, *CounterEdited::info());
    assert_eq!(edited.found, *CounterV1::info());
    assert_ne!(edited.expected.layout_hash, edited.found.layout_hash);
    let newer = handle.view::<CounterV2>().unwrap_err();
    assert!(
        newer.to_string().contains("expected `test:counter` v2")
            && newer.to_string().contains("found `test:counter` v1"),
        "{newer}"
    );
}

#[test]
fn layout_hash_ignores_whitespace_only() {
    let hash = |sig| foreign_layout_hash(sig, 8, 8);
    assert_eq!(
        hash("value: fn(*const c_void) -> i64;"),
        hash("value:fn(*constc_void)->i64;")
    );
    assert_ne!(hash("value: fn() -> i64;"), hash("value: fn() -> i32;"));
    assert_ne!(
        foreign_layout_hash("a", 8, 8),
        foreign_layout_hash("a", 16, 8)
    );
}

#[test]
fn payloads_carry_handles_built_from_shared_or_boundary_storage() {
    let shared = Payload::shared(
        "test:counter_owner",
        Arc::new(Counter {
            value: 5,
            tracked: false,
        }),
    );
    let handle = ForeignHandle::from_payload::<Counter, CounterV1>(&shared).unwrap();
    assert_eq!(
        handle.data(),
        (shared.get_ref::<Counter>().unwrap() as *const Counter).cast()
    );
    let payload = Payload::foreign(CounterV1::KEY, handle, Residency::External);
    assert_eq!(payload.type_key().as_str(), "test:counter");
    assert_eq!(payload.residency(), Residency::External);
    assert_eq!(read(payload.foreign_handle().unwrap()), 5);
    // The handle is the payload's value, so `get_ref` reaches it through the value's `Any`.
    assert!(core::ptr::eq(
        payload.get_ref::<ForeignHandle>().unwrap(),
        payload.foreign_handle().unwrap()
    ));
    assert!(shared.foreign_handle().is_none());
    // A handle is no Rust value of the key's type, so fed-payload identity checks skip it.
    assert_eq!(payload.storage_rust_type_id(), None);
    assert!(shared.storage_rust_type_id().is_some());

    // Boundary storage has no `Arc` to share: the handle keeps the payload alive instead.
    let boundary = Payload::boundary_owned(
        "test:counter_owner",
        Counter {
            value: 6,
            tracked: false,
        },
        BoundaryCapabilities::rust_value(),
    );
    let handle = ForeignHandle::from_payload::<Counter, CounterV1>(&boundary).unwrap();
    assert!(!boundary.is_storage_unique());
    assert_eq!(read(&handle), 6);
    drop(handle);
    assert!(boundary.is_storage_unique());
    assert!(ForeignHandle::from_payload::<Counter, CounterV1>(&Payload::owned("x", 1u8)).is_none());
}

#[test]
fn provided_payloads_lend_the_owner_value_without_a_handle() {
    let value = Arc::new(Counter {
        value: 9,
        tracked: false,
    });
    let owner = Payload::shared_with(
        "test:counter_owner",
        value.clone(),
        Residency::External,
        None,
        None,
    );
    assert!(owner.foreign_borrow().is_none());
    let provided = owner
        .clone()
        .provide_foreign::<Counter, CounterV1>()
        .unwrap();
    assert_eq!(provided.type_key().as_str(), "test:counter");
    assert_eq!(provided.residency(), Residency::External);
    assert!(provided.shares_storage(&owner) && provided.foreign_handle().is_none());
    // Clones (fanout) keep the provider; the borrow is the owner value itself.
    let borrow = provided
        .clone()
        .foreign_borrow()
        .map(|borrow| borrow.data());
    assert_eq!(borrow, Some(Arc::as_ptr(&value).cast()));
    let view = provided
        .foreign_borrow()
        .unwrap()
        .view::<CounterV1>()
        .unwrap();
    // Safety: the view checked the interface.
    assert_eq!(unsafe { (view.vtable().value)(view.data()) }, 9);
    assert!(
        provided
            .foreign_borrow()
            .unwrap()
            .view::<CounterV2>()
            .is_err()
    );
    assert_eq!(provided.get_ref::<Counter>().unwrap().value, 9);
    assert_eq!(Arc::strong_count(&value), 2, "borrowing retains nothing");
    let handle = provided.to_foreign_handle().unwrap();
    assert_eq!((read(&handle), Arc::strong_count(&value)), (9, 3));
    drop((handle, owner));

    // The storage is still the typed `Counter` storage: unique payloads give it back.
    drop(value);
    let mut provided = provided;
    provided.get_mut::<Counter>().unwrap().value = 10;
    let counter = provided.try_into_owned::<Counter>().ok().unwrap();
    assert_eq!(counter.value, 10);

    // Boundary storage has no shared `Arc`: it is wrapped in a handle payload instead.
    let before = DROPS.load(Ordering::SeqCst);
    let boundary = Payload::boundary_owned(
        "test:counter_owner",
        Counter {
            value: 11,
            tracked: true,
        },
        BoundaryCapabilities::rust_value(),
    )
    .provide_foreign::<Counter, CounterV1>()
    .unwrap();
    assert_eq!(read(boundary.foreign_handle().unwrap()), 11);
    drop(boundary);
    assert_eq!(DROPS.load(Ordering::SeqCst), before + 1);

    let wrong = Payload::owned("test:counter_owner", 1u8);
    assert!(wrong.provide_foreign::<Counter, CounterV1>().is_err());
}

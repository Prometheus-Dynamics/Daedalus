use super::*;
use crate::Payload;
use core::sync::atomic::{AtomicUsize as Count, Ordering as Order};

crate::foreign_interface! {
    pub interface RecycledCounter("test:recycled", version = 1);
    pub struct RecycledCounterVTable {
        pub value: unsafe extern "C" fn(data: *const c_void) -> i64,
    }
}

/// An owner that counts its drops in `drops` (each test has its own counter).
struct Owned {
    value: i64,
    drops: &'static Count,
}

impl Drop for Owned {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Order::SeqCst);
    }
}

/// A second owner type, to check that a node is read only as its own type.
struct Other(i64);

unsafe extern "C" fn owned_value(data: *const c_void) -> i64 {
    // Safety: only used in `Owned`'s vtable, over a live `Owned`.
    unsafe { (*data.cast::<Owned>()).value }
}

unsafe extern "C" fn other_value(data: *const c_void) -> i64 {
    // Safety: only used in `Other`'s vtable, over a live `Other`.
    unsafe { (*data.cast::<Other>()).0 }
}

// Safety: `owned_value` reads an `Owned`.
unsafe impl ProvideForeign<RecycledCounter> for Owned {
    fn vtable() -> &'static RecycledCounterVTable {
        &RecycledCounterVTable { value: owned_value }
    }
}

// Safety: `other_value` reads an `Other`.
unsafe impl ProvideForeign<RecycledCounter> for Other {
    fn vtable() -> &'static RecycledCounterVTable {
        &RecycledCounterVTable { value: other_value }
    }
}

fn read(handle: &ForeignHandle) -> i64 {
    let view = handle.view::<RecycledCounter>().expect("same interface");
    // Safety: the view checked the interface.
    unsafe { (view.vtable().value)(view.data()) }
}

fn owned(value: i64, drops: &'static Count) -> Owned {
    Owned { value, drops }
}

fn recycled(pool: &RecyclePool<Owned>, value: i64, drops: &'static Count) -> Payload {
    Payload::providing_recycled::<Owned, RecycledCounter>(
        "test:owned",
        pool,
        owned(value, drops),
        Residency::Cpu,
    )
}

#[test]
fn a_recycled_payload_reads_as_its_owner_and_lends_interface_i() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 41, &DROPS);
    assert_eq!(payload.type_key().as_str(), "test:owned");
    assert_eq!(payload.get_ref::<Owned>().map(|o| o.value), Some(41));
    assert!(payload.get_ref::<Other>().is_none());
    assert!(payload.get_ref::<i64>().is_none());

    let lent = payload.to_foreign_handle().expect("a handle of the owner");
    assert_eq!(lent.interface().key(), "test:recycled");
    assert_eq!(read(&lent), 41, "the lent view reads the owner");
    assert!(
        payload.foreign_handle().is_none(),
        "an owner is lent, not carried"
    );
    assert!(payload.foreign_borrow().is_some());
    drop((lent, payload));
    assert_eq!(DROPS.load(Order::SeqCst), 1);
}

#[test]
fn typed_reads_that_need_an_arc_do_not_apply_to_a_node() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let mut payload = recycled(&pool, 1, &DROPS);
    assert!(payload.get_arc::<Owned>().is_none());
    assert!(payload.get_mut::<Owned>().is_none());
    assert!(payload.typed_strong_count::<Owned>().is_none());
    assert_eq!(payload.get_ref::<Owned>().map(|o| o.value), Some(1));
}

#[test]
fn a_unique_node_moves_its_owner_out_and_goes_back_to_the_pool() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 7, &DROPS);
    assert!(payload.is_storage_unique());
    let owned = match payload.try_into_owned::<Owned>() {
        Ok(owned) => owned,
        Err(_) => panic!("a unique node moves its owner out"),
    };
    assert_eq!(owned.value, 7);
    assert_eq!(DROPS.load(Order::SeqCst), 0, "moved, not dropped");
    assert_eq!(pool.idle(), 1, "the node is back on the pool");
    drop(owned);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
    // The idle node is reused, and its value slot holds the new owner only.
    let again = recycled(&pool, 8, &DROPS);
    assert_eq!(pool.idle(), 0);
    assert_eq!(again.get_ref::<Owned>().map(|o| o.value), Some(8));
    drop(again);
    assert_eq!(DROPS.load(Order::SeqCst), 2);
}

#[test]
fn a_shared_or_lent_node_is_handed_back_by_try_into_owned() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 3, &DROPS);
    let clone = payload.clone();
    assert!(!payload.is_storage_unique());
    let Err(payload) = payload.try_into_owned::<Owned>() else {
        panic!("a shared node is handed back");
    };
    drop(clone);
    let lent = payload.to_foreign_handle().unwrap();
    let Err(payload) = payload.try_into_owned::<Owned>() else {
        panic!("a lent node is handed back");
    };
    drop(lent);
    let Err(payload) = payload.try_into_owned::<String>() else {
        panic!("another type is handed back");
    };
    assert_eq!(DROPS.load(Order::SeqCst), 0);
    let Ok(owned) = payload.try_into_owned::<Owned>() else {
        panic!("unique again");
    };
    assert_eq!(owned.value, 3);
    drop(owned);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
}

#[test]
fn moving_out_after_the_pool_closed_frees_the_node() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 4, &DROPS);
    drop(pool);
    let Ok(owned) = payload.try_into_owned::<Owned>() else {
        panic!("unique");
    };
    assert_eq!(owned.value, 4);
    drop(owned);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
}

#[test]
fn the_owner_drops_at_the_last_holder_and_the_node_goes_back_to_the_pool() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 5, &DROPS);
    let lent = payload.to_foreign_handle().unwrap();
    let clone = payload.clone();
    drop(payload);
    drop(clone);
    assert_eq!(DROPS.load(Order::SeqCst), 0, "a lent handle still holds it");
    assert_eq!(read(&lent), 5);
    assert_eq!(pool.idle(), 0);
    drop(lent);
    assert_eq!(
        DROPS.load(Order::SeqCst),
        1,
        "the owner drops with its last holder"
    );
    assert_eq!(pool.idle(), 1, "the node is idle");
}

#[test]
fn an_idle_node_is_reused_for_the_next_payload() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let first = recycled(&pool, 1, &DROPS);
    let lent = first.to_foreign_handle().unwrap();
    let address = lent.data();
    drop((lent, first));
    assert_eq!(pool.idle(), 1);
    let second = recycled(&pool, 2, &DROPS);
    assert_eq!(second.to_foreign_handle().unwrap().data(), address);
    assert_eq!(pool.idle(), 0);
    assert_eq!(second.get_ref::<Owned>().map(|o| o.value), Some(2));
    assert_eq!(
        DROPS.load(Order::SeqCst),
        1,
        "the first owner dropped before reuse"
    );
}

#[test]
fn the_last_holder_on_another_thread_drops_the_owner_and_idles_the_node() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 9, &DROPS);
    let on_thread = payload.clone();
    drop(payload);
    assert_eq!(DROPS.load(Order::SeqCst), 0);
    std::thread::spawn(move || {
        assert_eq!(on_thread.get_ref::<Owned>().map(|o| o.value), Some(9));
        drop(on_thread);
    })
    .join()
    .unwrap();
    assert_eq!(DROPS.load(Order::SeqCst), 1);
    assert_eq!(pool.idle(), 1);
}

#[test]
fn a_payload_lent_to_another_thread_reads_across_it() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let payload = recycled(&pool, 12, &DROPS);
    let lent = payload.to_foreign_handle().unwrap();
    let reader = std::thread::spawn(move || read(&lent));
    assert_eq!(reader.join().unwrap(), 12);
    drop(payload);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
    assert_eq!(pool.idle(), 1);
}

#[test]
fn dropping_the_pool_while_payloads_are_alive_frees_idle_nodes_and_keeps_the_rest() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let idle_one = recycled(&pool, 1, &DROPS);
    let live = recycled(&pool, 2, &DROPS);
    drop(idle_one);
    assert_eq!(pool.idle(), 1);
    drop(pool);
    assert_eq!(
        DROPS.load(Order::SeqCst),
        1,
        "the idle node's owner dropped with the node"
    );
    assert_eq!(live.get_ref::<Owned>().map(|o| o.value), Some(2));
    let lent = live.to_foreign_handle().unwrap();
    assert_eq!(read(&lent), 2);
    drop(live);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
    drop(lent);
    assert_eq!(
        DROPS.load(Order::SeqCst),
        2,
        "the last holder frees the node and drops the owner"
    );
}

#[test]
fn a_release_after_the_pool_closed_frees_the_node_instead_of_listing_it() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Owned>::new();
    let inner = pool.inner.clone();
    let payload = recycled(&pool, 3, &DROPS);
    drop(pool);
    drop(payload);
    assert_eq!(DROPS.load(Order::SeqCst), 1);
    assert_eq!(
        lock_recover(&inner.state).len,
        0,
        "a closed pool lists nothing"
    );
}

#[test]
fn foreign_handles_from_elsewhere_are_never_read_as_nodes() {
    static DROPS: Count = Count::new(0);
    // A handle built by `ForeignHandle::from_arc`: its owner is `Arc`'s, not a node.
    let arc = crate::portable::Arc::new(owned(33, &DROPS));
    let handle = ForeignHandle::from_arc::<Owned, RecycledCounter>(arc);
    let payload = Payload::foreign_local("test:owned", handle.clone(), Residency::Cpu);
    assert!(payload.get_ref::<Owned>().is_none());
    assert_eq!(read(&handle), 33);
    drop(payload);
    drop(handle);
    assert_eq!(DROPS.load(Order::SeqCst), 1);

    // A handle whose owner functions are not this module's, and whose owner pointer is null: the
    // read must not touch the pointer at all.
    unsafe extern "C" fn foreign_retain(_: *const c_void) {}
    unsafe extern "C" fn foreign_release(_: *const c_void) {}
    let local = owned(44, &DROPS);
    // Safety: `data` is `local`, which outlives the handle (dropped first), and the owner's
    // functions do nothing.
    let foreign = unsafe {
        ForeignHandle::from_raw_parts::<RecycledCounter>(
            (&local as *const Owned).cast(),
            <Owned as ProvideForeign<RecycledCounter>>::vtable(),
            ForeignOwner {
                ptr: core::ptr::null(),
                retain: foreign_retain,
                release: foreign_release,
            },
        )
    };
    let payload = Payload::foreign_local("test:owned", foreign, Residency::Cpu);
    assert!(payload.get_ref::<Owned>().is_none());
    drop(payload);
    assert_eq!(
        DROPS.load(Order::SeqCst),
        1,
        "the local owner is not dropped by the handle"
    );
}

#[test]
fn recycled_payloads_keep_their_own_owner_types() {
    static DROPS: Count = Count::new(0);
    let pool = RecyclePool::<Other>::new();
    let other = Payload::providing_recycled::<Other, RecycledCounter>(
        "test:other",
        &pool,
        Other(8),
        Residency::Cpu,
    );
    assert_eq!(other.get_ref::<Other>().map(|o| o.0), Some(8));
    assert!(other.get_ref::<Owned>().is_none());
    assert_eq!(read(&other.to_foreign_handle().unwrap()), 8);
    let owned_pool = RecyclePool::new();
    let owned_payload = recycled(&owned_pool, 2, &DROPS);
    assert_eq!(owned_payload.get_ref::<Owned>().map(|o| o.value), Some(2));
    assert!(owned_payload.get_ref::<Other>().is_none());
}

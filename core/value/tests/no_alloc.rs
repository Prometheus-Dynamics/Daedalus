//! Handing values around allocates nothing, counted on the calling thread.

use std::sync::Arc;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::*;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn inline() -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Inline(InlineBytes::from_slice(b"12345678").unwrap()),
    }
}

#[test]
fn inline_values_and_the_fixed_pool_allocate_nothing() {
    let (_, allocations) = counted(|| {
        let mut cells = [const { None }; 4];
        let mut ring = Ring::new(&mut cells);
        for _ in 0..64 {
            assert!(matches!(ring.push(inline()), Push::Stored));
            let copy = ring.peek().unwrap().clone_inline();
            assert_eq!(copy.bytes(), Some(&b"12345678"[..]));
            ring.pop().unwrap();
        }
        let mut pool = FixedPool::<Value, 2>::new();
        for _ in 0..64 {
            assert!(pool.put(inline()).is_ok());
            pool.take().unwrap();
        }
    });
    assert_eq!(allocations, 0);
}

struct Buffer([u8; 256]);

impl Default for Buffer {
    fn default() -> Self {
        Buffer([0; 256])
    }
}

impl Reusable for Buffer {
    fn reset(&mut self) {
        self.0[0] = 0;
    }
}

#[test]
fn recycled_value_allocates_nothing_when_warm() {
    let mut pool = RecyclePool::<Buffer>::new();
    drop(pool.provide(Buffer::default));
    assert_eq!(pool.idle(), 1);
    let (_, allocations) = counted(|| {
        for round in 0..64u8 {
            let held = pool.take().expect("the pool is warm");
            assert_eq!(held.0[0], 0, "reset ran");
            let a = Value {
                ty: TypeIndex(1),
                space: SpaceIndex(0),
                owner: Owner::Typed(held),
            };
            let b = a.clone_inline();
            assert!(b.typed::<Buffer>().is_some());
            assert_eq!(pool.idle(), 0);
            drop((a, b));
            assert_eq!(
                pool.idle(),
                1,
                "the last release gives it back, round {round}"
            );
        }
    });
    assert_eq!(allocations, 0);
}

#[test]
fn a_held_value_is_not_reused() {
    let mut pool = RecyclePool::<Buffer>::new();
    let first = pool.provide(Buffer::default);
    let second = pool.provide(Buffer::default);
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(pool.take().is_none());
    let address = Arc::as_ptr(&first);
    drop(first);
    assert_eq!(Arc::as_ptr(&pool.take().unwrap()), address);
}

#[test]
fn payload_slot_refills_in_place_when_unique() {
    let mut slot = PayloadSlot::<Vec<u8>>::default();
    let first = slot.owned_reused(|| Vec::with_capacity(64), |v| v.push(1));
    let address = Arc::as_ptr(&first);
    let replaced = slot.owned_reused(Vec::new, |v| v.push(2));
    assert_ne!(
        Arc::as_ptr(&replaced),
        address,
        "a held value is left alone"
    );
    assert_eq!(*first, [1]);
    let warm = Arc::as_ptr(&replaced);
    drop(replaced);
    let (again, allocations) = counted(|| slot.owned_reused(|| unreachable!(), |v| v.push(3)));
    assert_eq!(allocations, 0);
    assert_eq!(Arc::as_ptr(&again), warm);
    assert_eq!(*again, [3]);
}

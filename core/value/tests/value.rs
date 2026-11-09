use std::sync::Arc;

use daedalus_abi::{Handle, SlotState};
use daedalus_types::{SlotIndex, SpaceIndex, TypeIndex};
use daedalus_value::*;

fn value(owner: Owner) -> Value {
    Value {
        ty: TypeIndex(3),
        space: SpaceIndex(1),
        owner,
    }
}

fn inline(bytes: &[u8]) -> Value {
    value(Owner::Inline(InlineBytes::from_slice(bytes).unwrap()))
}

#[test]
fn access_satisfies_table() {
    use Access::*;
    let cases = [
        (Read, Read, true),
        (Move, Read, true),
        (Modify, Read, true),
        (View, Read, true),
        (View, View, true),
        (Read, View, false),
        (Modify, View, false),
        (Move, Move, true),
        (Modify, Move, true),
        (Read, Move, false),
        (View, Move, false),
        (Modify, Modify, true),
        (Move, Modify, false),
        (Read, Modify, false),
    ];
    for (offered, required, expected) in cases {
        assert_eq!(
            offered.satisfies(required),
            expected,
            "{offered:?} for {required:?}"
        );
    }
}

#[test]
fn inline_roundtrip() {
    let bytes = 0x1234_5678u32.to_le_bytes();
    let held = InlineBytes::<16>::from_slice(&bytes).unwrap();
    assert_eq!(
        u32::from_le_bytes(held.as_slice().try_into().unwrap()),
        0x1234_5678
    );
    assert!(InlineBytes::<4>::from_slice(&[0; 5]).is_none());
    assert!(InlineBytes::<16>::new().as_slice().is_empty());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn value_fits_its_budget() {
    assert!(
        size_of::<Value>() <= 48,
        "Value is {} bytes",
        size_of::<Value>()
    );
}

#[test]
fn bytes_cover_byte_owners_only() {
    assert_eq!(value(Owner::Static(b"abc")).bytes(), Some(&b"abc"[..]));
    assert_eq!(inline(b"xy").bytes(), Some(&b"xy"[..]));
    assert_eq!(
        value(Owner::Shared(Arc::from(&b"q"[..]))).bytes(),
        Some(&b"q"[..])
    );
    assert_eq!(value(Owner::Host(Handle::new(1, 1))).bytes(), None);
    assert_eq!(value(Owner::Slot(SlotIndex(2))).bytes(), None);
}

#[test]
fn typed_checks_the_rust_type() {
    let v = value(Owner::Typed(Arc::new(41u32)));
    assert_eq!(v.typed::<u32>(), Some(&41));
    assert_eq!(v.typed::<u64>(), None);
    assert_eq!(inline(b"x").typed::<u32>(), None);
}

#[test]
fn clone_inline_copies_or_shares() {
    let a = inline(b"hi");
    assert_eq!(a.clone_inline().bytes(), a.bytes());
    let shared: Arc<[u8]> = Arc::from(&b"s"[..]);
    let b = value(Owner::Shared(shared.clone()));
    let c = b.clone_inline();
    assert_eq!(Arc::strong_count(&shared), 3);
    drop((b, c));
    assert_eq!(Arc::strong_count(&shared), 1);
}

#[test]
fn raw_describes_the_value() {
    let v = inline(b"abcd");
    let raw = v.raw(7);
    assert_eq!(raw.state, SlotState::Full as u32);
    assert_eq!(
        (raw.space, raw.type_index, raw.version, raw.len),
        (1, 3, 7, 4)
    );
    assert_eq!(raw.owner, OwnerKind::Inline as u32);
    assert_eq!(raw.data.cast_const(), v.bytes().unwrap().as_ptr());
    assert_eq!(value(Owner::Host(Handle::new(1, 1))).raw(0).len, 0);
    assert!(!value(Owner::Typed(Arc::new(1u8))).raw(0).data.is_null());
}

#[test]
fn ring_is_first_in_first_out_and_wraps() {
    let mut cells = [const { None }; 2];
    let mut ring = Ring::new(&mut cells);
    assert!(ring.peek().is_none() && ring.pop().is_none());
    assert!(matches!(ring.push(inline(b"1")), Push::Stored));
    assert!(matches!(ring.push(inline(b"2")), Push::Stored));
    assert_eq!(ring.version(), 2);
    match ring.push(inline(b"3")) {
        Push::Full(back) => assert_eq!(back.bytes(), Some(&b"3"[..])),
        other => panic!("{other:?}"),
    }
    assert_eq!(ring.version(), 2, "a refused value is not a new version");
    assert_eq!(ring.peek().unwrap().bytes(), Some(&b"1"[..]));
    assert_eq!(ring.pop().unwrap().bytes(), Some(&b"1"[..]));
    assert!(matches!(ring.push(inline(b"3")), Push::Stored));
    assert_eq!(ring.pop().unwrap().bytes(), Some(&b"2"[..]));
    assert_eq!(ring.pop().unwrap().bytes(), Some(&b"3"[..]));
    assert!(ring.is_empty());
}

#[test]
fn ring_tracks_unconsumed() {
    let mut cells = [const { None }; 3];
    let mut ring = Ring::new(&mut cells);
    let _ = ring.push(inline(b"1"));
    let _ = ring.push(inline(b"2"));
    assert_eq!(ring.unconsumed(), 2);
    ring.mark_consumed();
    assert_eq!((ring.unconsumed(), ring.len()), (0, 2));
    assert!(ring.peek().is_some());
    let _ = ring.pop();
    assert_eq!(ring.unconsumed(), 0);
}

#[test]
fn fixed_pool_gives_back_when_full() {
    let mut pool = FixedPool::<u32, 1>::new();
    assert_eq!(pool.put(1), Ok(()));
    assert_eq!(pool.put(2), Err(2));
    assert_eq!(pool.take(), Some(1));
    assert_eq!(pool.take(), None);
}

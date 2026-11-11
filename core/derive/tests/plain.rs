//! `#[derive(PlainData)]` gives the key and the layout hash the struct's shape implies.

use daedalus_abi::{PlainData, layout_hash};
use daedalus_derive::PlainData;

#[repr(C)]
#[derive(Clone, Copy, PlainData)]
#[plain(key = "test.pair")]
struct Pair {
    a: u32,
    b: u32,
}

#[repr(C)]
#[derive(Clone, Copy, PlainData)]
#[plain(key = "test.mixed")]
struct Mixed {
    tag: u8,
    pad: [u8; 3],
    value: f32,
}

#[test]
fn the_key_is_the_one_named() {
    assert_eq!(Pair::KEY, "test.pair");
    assert_eq!(Mixed::KEY, "test.mixed");
}

#[test]
fn the_layout_hash_comes_from_size_alignment_and_offsets() {
    assert_eq!(Pair::LAYOUT, layout_hash(&[8, 4, 0, 4]));
    assert_eq!(Mixed::LAYOUT, layout_hash(&[8, 4, 0, 1, 4]));
}

#[test]
fn another_shape_has_another_layout() {
    assert_ne!(Pair::LAYOUT, Mixed::LAYOUT);
}

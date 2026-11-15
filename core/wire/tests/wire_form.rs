//! A type's own wire form ([`WireForm`], written by the type or its derive) is byte for byte what
//! the wire codec writes for the same value of the type's expression, and reads back.

use std::collections::BTreeMap;

use daedalus_derive::{Type, WireForm};
use daedalus_types::{Typed, WireForm};
use daedalus_wire::codec::Writer;
use daedalus_wire::{Schema, WireType, WireValue, encode_bare};

/// What the codec writes for `value` as a cell holds it, against what the type writes.
fn same<T: Typed + WireForm + PartialEq + std::fmt::Debug>(value: &T, wire: WireValue) {
    let ty = WireType::from_expr(&T::EXPR);
    let mut out = Writer::new();
    encode_bare(&ty, &wire, &Schema::new(), &mut out).expect("the codec encodes it");
    let mut own = vec![0; value.cell_len()];
    assert_eq!(value.write_cell(&mut own), own.len(), "{value:?}");
    assert_eq!(own, out.buf, "{value:?} as {ty:?}");
    assert_eq!(T::read_cell(&own).as_ref(), Some(value), "reads back");
}

/// Fields of mixed sizes, so a Rust layout has padding or another order: the wire form doesn't.
#[derive(Clone, Copy, Debug, PartialEq, Type, WireForm)]
struct Padded {
    a: u8,
    b: u32,
    c: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Type, WireForm)]
#[repr(C)]
struct KeyPoint {
    x: f32,
    y: f32,
    size: f32,
    angle: f32,
    response: f32,
    octave: i32,
    class: i32,
}

#[derive(Clone, Debug, PartialEq, Type, WireForm)]
struct Labels {
    width: u32,
    height: u32,
    labels: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Type, WireForm)]
struct Meters(f64);

#[derive(Clone, Debug, PartialEq, Type, WireForm)]
enum Shape {
    Empty,
    Circle(f32),
    Box { w: u16, h: u16 },
}

fn s(text: &str) -> WireValue {
    WireValue::String(text.into())
}

#[test]
fn scalars_text_and_bytes_are_what_the_codec_writes() {
    same(&7u32, WireValue::U32(7));
    same(&-2.5f64, WireValue::F64(-2.5));
    same(&true, WireValue::Bool(true));
    same(&String::from("über"), s("über"));
    same(&vec![1u8, 2, 3], WireValue::Bytes(vec![1, 2, 3]));
    same(&usize::MAX, WireValue::USize(u64::MAX));
}

#[test]
fn compounds_are_what_the_codec_writes() {
    same(
        &(3u32, 1.5f32),
        WireValue::Tuple(vec![WireValue::U32(3), WireValue::F32(1.5)]),
    );
    same(
        &vec![String::from("a"), String::from("bc")],
        WireValue::List(vec![s("a"), s("bc")]),
    );
    same(
        &Some(vec![9u8]),
        WireValue::Optional(Some(Box::new(WireValue::Bytes(vec![9])))),
    );
    same(&None::<i64>, WireValue::Optional(None));
    let map = BTreeMap::from([(String::from("k"), 4u32)]);
    same(&map, WireValue::Map(vec![(s("k"), WireValue::U32(4))]));
}

#[test]
fn derived_types_are_their_fields_in_order() {
    let padded = Padded { a: 1, b: 2, c: 3 };
    same(
        &padded,
        WireValue::Struct(vec![WireValue::U8(1), WireValue::U32(2), WireValue::U16(3)]),
    );
    // Seven bytes on the wire, whatever the struct's size in memory.
    assert_eq!(padded.wire_len(), 7);
    const {
        assert!(
            !Padded::FLAT,
            "padded or reordered memory is not the wire form"
        )
    };

    let point = KeyPoint {
        x: 1.0,
        y: 2.0,
        size: 3.0,
        angle: 4.0,
        response: 5.0,
        octave: -1,
        class: 7,
    };
    assert_eq!(point.wire_len(), 28);
    assert_eq!(KeyPoint::FLAT, cfg!(target_endian = "little"));
    same(
        &vec![point; 3],
        WireValue::List(vec![
            WireValue::Struct(vec![
                WireValue::F32(1.0),
                WireValue::F32(2.0),
                WireValue::F32(3.0),
                WireValue::F32(4.0),
                WireValue::F32(5.0),
                WireValue::I32(-1),
                WireValue::I32(7),
            ]);
            3
        ]),
    );

    same(
        &Labels {
            width: 2,
            height: 1,
            labels: vec![5, 6],
        },
        WireValue::Struct(vec![
            WireValue::U32(2),
            WireValue::U32(1),
            WireValue::List(vec![WireValue::U32(5), WireValue::U32(6)]),
        ]),
    );
    same(&Meters(2.0), WireValue::F64(2.0));
    same(
        &Shape::Empty,
        WireValue::Enum {
            variant: 0,
            payload: None,
        },
    );
    same(
        &Shape::Circle(1.5),
        WireValue::Enum {
            variant: 1,
            payload: Some(Box::new(WireValue::F32(1.5))),
        },
    );
    same(
        &Shape::Box { w: 3, h: 4 },
        WireValue::Enum {
            variant: 2,
            payload: Some(Box::new(WireValue::Struct(vec![
                WireValue::U16(3),
                WireValue::U16(4),
            ]))),
        },
    );
}

#[test]
fn a_short_or_wrong_value_is_refused() {
    assert_eq!(u32::read_cell(&[1, 2, 3]), None);
    assert_eq!(bool::read_cell(&[2]), None);
    assert_eq!(String::read_cell(&[0xff]), None);
    // A count that claims more items than there are bytes.
    assert_eq!(Vec::<u32>::read_cell(&[9, 0, 0, 0, 1, 0, 0, 0]), None);
    assert_eq!(Shape::read_cell(&[3, 0, 0, 0]), None);
    // Bytes left over after the value.
    assert_eq!(u16::read_cell(&[1, 0, 0]), None);
}

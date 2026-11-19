//! The value codec through the C calls that a C test makes: each value encodes to the bytes the wire
//! crate's own codec writes, each value outside its type's range is refused, and each malformed
//! byte string is refused on decode.

use crate::support;

use std::ffi::{c_int, c_void};
use std::ptr;

use daedalus_lang_cpp_sdk::ffi::{DdBytes, DdEnum, DdSeq, DdType, DdValue};
use daedalus_types::Scalar;
use daedalus_wire::codec::Writer;
use daedalus_wire::{Schema, WireType, WireValue, encode_value};
use support::*;

// SAFETY: these are this crate's own C-ABI exports, which the test links and calls as a C test would.
unsafe extern "C" {
    fn dd_internal_encode(
        ty: *const DdType,
        v: *const DdValue,
        out: *mut *mut u8,
        len: *mut usize,
    ) -> c_int;
    fn dd_internal_decode(
        ty: *const DdType,
        p: *const u8,
        len: usize,
        out: *mut DdValue,
        keep: *mut *mut c_void,
    ) -> c_int;
    fn dd_internal_free_bytes(p: *mut u8);
    fn dd_internal_free_arena(keep: *mut c_void);
}

/// Encodes a C value as a C type, and returns the bytes, or `None` when the codec refuses it.
fn encode(ty: *const DdType, value: &DdValue) -> Option<Vec<u8>> {
    let mut out: *mut u8 = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: the type and value are valid for the call, and `out` and `len` are locals.
    let status = unsafe { dd_internal_encode(ty, value, &mut out, &mut len) };
    if status != 0 {
        return None;
    }
    // SAFETY: on success `out` holds `len` bytes that the call allocated.
    let bytes = unsafe { std::slice::from_raw_parts(out, len) }.to_vec();
    // SAFETY: `out` came from `dd_internal_encode` and is freed once.
    unsafe { dd_internal_free_bytes(out) };
    Some(bytes)
}

/// Decodes bytes as a C type. The value's pointers live in the arena `keep` holds, freed by [`free`].
fn decode(ty: *const DdType, bytes: &[u8]) -> Option<(DdValue, *mut c_void)> {
    let mut out = DdValue::ZERO;
    let mut keep: *mut c_void = ptr::null_mut();
    // SAFETY: the type is valid, `bytes` is readable for its length, and `out` and `keep` are locals.
    let status =
        unsafe { dd_internal_decode(ty, bytes.as_ptr(), bytes.len(), &mut out, &mut keep) };
    (status == 0).then_some((out, keep))
}

fn free(keep: *mut c_void) {
    // SAFETY: `keep` is an arena `decode` returned, and it is freed once.
    unsafe { dd_internal_free_arena(keep) };
}

/// The bytes the wire crate's codec writes for a value of a wire type.
fn wire_bytes(ty: &WireType, value: &WireValue) -> Vec<u8> {
    let mut out = Writer::new();
    encode_value(ty, value, &Schema::new(), &mut out).expect("the wire encodes the value");
    out.buf
}

fn int(v: i64) -> DdValue {
    int_value(v)
}

fn uint(v: u64) -> DdValue {
    let mut value = DdValue::ZERO;
    value.data.u = v;
    value
}

fn text(s: &str) -> DdValue {
    let mut value = DdValue::ZERO;
    value.data.bytes = DdBytes {
        data: s.as_ptr(),
        len: s.len(),
    };
    value
}

#[test]
fn scalars_encode_as_the_wire_codec_writes_them() {
    let cases: Vec<(*const DdType, DdValue, WireType, WireValue)> = vec![
        (
            scalar(DD_I8),
            int(-5),
            WireType::Scalar(Scalar::I8),
            WireValue::I8(-5),
        ),
        (
            scalar(DD_I16),
            int(-300),
            WireType::Scalar(Scalar::I16),
            WireValue::I16(-300),
        ),
        (
            scalar(DD_I32),
            int(7),
            WireType::Scalar(Scalar::I32),
            WireValue::I32(7),
        ),
        (
            scalar(DD_I64),
            int(-9),
            WireType::Scalar(Scalar::Int),
            WireValue::I64(-9),
        ),
        (
            scalar(DD_U8),
            uint(255),
            WireType::Scalar(Scalar::U8),
            WireValue::U8(255),
        ),
        (
            scalar(DD_U16),
            uint(1000),
            WireType::Scalar(Scalar::U16),
            WireValue::U16(1000),
        ),
        (
            scalar(DD_U32),
            uint(4_294_967_295),
            WireType::Scalar(Scalar::U32),
            WireValue::U32(u32::MAX),
        ),
        (
            scalar(DD_U64),
            uint(u64::MAX),
            WireType::Scalar(Scalar::U64),
            WireValue::U64(u64::MAX),
        ),
        (
            scalar(DD_STRING),
            text("h\u{e9}"),
            WireType::Scalar(Scalar::String),
            WireValue::String("h\u{e9}".to_owned()),
        ),
    ];
    for (ty, value, wire_ty, wire_value) in cases {
        assert_eq!(
            encode(ty, &value).expect("the value is in range"),
            wire_bytes(&wire_ty, &wire_value),
            "{wire_value:?}"
        );
    }
    let mut flag = DdValue::ZERO;
    flag.data.b = true;
    assert_eq!(encode(scalar(DD_BOOL), &flag), Some(vec![1]));
    let mut float = DdValue::ZERO;
    float.data.f = 1.5;
    assert_eq!(
        encode(scalar(DD_F64), &float).expect("a float encodes"),
        wire_bytes(&WireType::Scalar(Scalar::Float), &WireValue::F64(1.5))
    );
}

#[test]
fn a_value_outside_its_type_is_refused() {
    let refused = [
        (scalar(DD_I8), int(128)),
        (scalar(DD_I8), int(-129)),
        (scalar(DD_I16), int(32_768)),
        (scalar(DD_I32), int(2_147_483_648)),
        (scalar(DD_U8), uint(256)),
        (scalar(DD_U16), uint(65_536)),
        (scalar(DD_U32), uint(4_294_967_296)),
    ];
    for (ty, value) in refused {
        assert!(
            encode(ty, &value).is_none(),
            "{} is out of range",
            value_text(&value)
        );
    }
}

fn value_text(value: &DdValue) -> String {
    // SAFETY: the test's values hold integers, which every bit pattern makes readable.
    format!("{}", unsafe { value.data.i })
}

#[test]
fn text_that_is_not_utf8_is_refused() {
    let bad: [u8; 2] = [0xff, 0xfe];
    let mut value = DdValue::ZERO;
    value.data.bytes = DdBytes {
        data: bad.as_ptr(),
        len: bad.len(),
    };
    assert!(encode(scalar(DD_STRING), &value).is_none());
}

#[test]
fn a_list_a_struct_an_enum_an_optional_and_a_map_encode_as_the_wire_writes_them() {
    // A list of u32.
    let mut items = [uint(1), uint(2), uint(3)];
    let mut list = DdValue::ZERO;
    list.data.seq = DdSeq {
        items: items.as_mut_ptr(),
        count: items.len(),
    };
    let list_ty = nested(K_LIST, &[scalar(DD_U32)]);
    assert_eq!(
        encode(list_ty, &list).expect("the list encodes"),
        wire_bytes(
            &WireType::List(Box::new(WireType::Scalar(Scalar::U32))),
            &WireValue::List(vec![
                WireValue::U32(1),
                WireValue::U32(2),
                WireValue::U32(3)
            ]),
        )
    );

    // A map of string to i64, flattened to key and value pairs.
    let key = "k";
    let mut pairs = [text(key), int(5)];
    let mut map = DdValue::ZERO;
    map.data.seq = DdSeq {
        items: pairs.as_mut_ptr(),
        count: pairs.len(),
    };
    let map_ty = nested(K_MAP, &[scalar(DD_STRING), scalar(DD_I64)]);
    assert_eq!(
        encode(map_ty, &map).expect("the map encodes"),
        wire_bytes(
            &WireType::Map(
                Box::new(WireType::Scalar(Scalar::String)),
                Box::new(WireType::Scalar(Scalar::Int)),
            ),
            &WireValue::Map(vec![(WireValue::String(key.to_owned()), WireValue::I64(5))]),
        )
    );

    // An optional holds its one value at its sequence's first item.
    let mut inner = [int(9)];
    let mut some = DdValue::ZERO;
    some.present = true;
    some.data.seq = DdSeq {
        items: inner.as_mut_ptr(),
        count: 1,
    };
    let opt_ty = nested(K_OPTIONAL, &[scalar(DD_I64)]);
    assert_eq!(
        encode(opt_ty, &some).expect("the optional encodes"),
        wire_bytes(
            &WireType::Optional(Box::new(WireType::Scalar(Scalar::Int))),
            &WireValue::Optional(Some(Box::new(WireValue::I64(9)))),
        )
    );
    assert_eq!(encode(opt_ty, &DdValue::ZERO), Some(vec![0]));

    // An enum with a payload on variant 1, and none on variant 0.
    let names = [c_str("none"), c_str("some")];
    let payloads: [*const DdType; 2] = [ptr::null(), scalar(DD_U32)];
    let enum_ty = leak(DdType {
        kind: K_ENUM,
        scalar: 0,
        key: ptr::null(),
        items: payloads.as_ptr(),
        names: names.as_ptr(),
        count: 2,
    });
    let mut payload = uint(7);
    let mut choice = DdValue::ZERO;
    choice.data.en = DdEnum {
        variant: 1,
        payload: ptr::from_mut(&mut payload),
    };
    assert_eq!(
        encode(enum_ty, &choice).expect("the enum encodes"),
        wire_bytes(
            &WireType::Enum(vec![
                ("none".to_owned(), None),
                ("some".to_owned(), Some(WireType::Scalar(Scalar::U32))),
            ]),
            &WireValue::Enum {
                variant: 1,
                payload: Some(Box::new(WireValue::U32(7))),
            },
        )
    );
}

#[test]
fn a_decoded_value_encodes_back_to_the_same_bytes() {
    let mut items = [uint(4), uint(5)];
    let mut list = DdValue::ZERO;
    list.data.seq = DdSeq {
        items: items.as_mut_ptr(),
        count: items.len(),
    };
    let list_ty = nested(K_LIST, &[scalar(DD_U32)]);
    let bytes = encode(list_ty, &list).expect("the list encodes");
    let (decoded, keep) = decode(list_ty, &bytes).expect("the bytes decode");
    let again = encode(list_ty, &decoded).expect("the decoded list encodes");
    free(keep);
    assert_eq!(again, bytes);

    let bytes = encode(scalar(DD_I64), &int(-42)).expect("encodes");
    let (value, keep) = decode(scalar(DD_I64), &bytes).expect("decodes");
    // SAFETY: the decoded i64 is an integer the decoder wrote.
    let got = unsafe { value.data.i };
    free(keep);
    assert_eq!(got, -42);
}

#[test]
fn malformed_bytes_are_refused_on_decode() {
    let refused: Vec<(*const DdType, Vec<u8>)> = vec![
        // A bool is 0 or 1 only.
        (scalar(DD_BOOL), vec![2]),
        // A fixed-width value needs all its bytes.
        (scalar(DD_I32), vec![1, 2, 3]),
        // One value and nothing after it.
        (scalar(DD_U8), vec![1, 2]),
        // A string's length must fit in the bytes after it.
        (scalar(DD_STRING), vec![9, 0, 0, 0, b'a']),
        // A list's count must fit in the bytes after it.
        (
            nested(K_LIST, &[scalar(DD_U8)]),
            vec![0xff, 0xff, 0xff, 0xff],
        ),
        // Empty bytes for a value that needs some.
        (scalar(DD_U64), vec![]),
    ];
    for (ty, bytes) in refused {
        assert!(decode(ty, &bytes).is_none(), "{bytes:?} should not decode");
    }
    let (value, keep) = decode(scalar(DD_U8), &[7]).expect("a correct u8 decodes");
    // SAFETY: the decoded u8 is the value the decoder wrote.
    let got = unsafe { value.data.u };
    free(keep);
    assert_eq!(got, 7);
}

#[test]
fn a_type_nested_past_the_depth_limit_is_refused() {
    let mut ty = scalar(DD_U8);
    for _ in 0..40 {
        ty = nested(K_LIST, &[ty]);
    }
    assert!(encode(ty, &DdValue::ZERO).is_none());
}

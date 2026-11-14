//! Values by reference of any type: between peers that agreed `ANY_TYPE`, a port's value of any
//! type but a fixed scalar has a form byte and may be a place. A place names the value's bytes as
//! the host's cell holds them: the content of a string, the wire form of anything else.

use daedalus_types::Scalar;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::shared::{ANY_TYPE, At, BY_REFERENCE, Place, form, has_form};
use daedalus_wire::{
    Schema, WireType, WireValue, decode_bare, decode_placed, decode_value, encode_bare,
    encode_value,
};

fn s(scalar: Scalar) -> WireType {
    WireType::Scalar(scalar)
}

fn list(ty: WireType) -> WireType {
    WireType::List(Box::new(ty))
}

fn schema(features: u32) -> Schema {
    let mut schema = Schema::new();
    schema.set_features(features);
    schema
}

fn encoded(ty: &WireType, value: &WireValue, schema: &Schema) -> Vec<u8> {
    let mut out = Writer::new();
    encode_value(ty, value, schema, &mut out).expect("encodes");
    out.buf
}

fn round_trip(ty: &WireType, value: &WireValue, schema: &Schema) -> WireValue {
    let bytes = encoded(ty, value, schema);
    let mut input = Reader::new(&bytes);
    let back = decode_value(ty, &mut input, schema).expect("decodes");
    input.finish().expect("nothing left over");
    back
}

fn place() -> WireValue {
    WireValue::Placed(Place {
        at: At::Lent(1),
        offset: 128,
        len: 4 << 20,
    })
}

#[test]
fn which_types_have_a_form_is_decided_by_the_features_alone() {
    let both = BY_REFERENCE | ANY_TYPE;
    let record = WireType::Struct(vec![("a".into(), s(Scalar::U32))]);
    for ty in [s(Scalar::String), list(s(Scalar::F32)), record] {
        assert!(has_form(both, &ty), "{ty:?}");
        assert!(!has_form(BY_REFERENCE, &ty), "{ty:?} needs ANY_TYPE");
        assert!(!has_form(ANY_TYPE, &ty), "ANY_TYPE alone agrees nothing");
    }
    for ty in [s(Scalar::Bytes), WireType::Opaque("t:x".into())] {
        assert!(has_form(BY_REFERENCE, &ty) && has_form(both, &ty), "{ty:?}");
    }
    for scalar in [Scalar::U32, Scalar::Float, Scalar::Bool] {
        assert!(
            !has_form(both, &s(scalar)),
            "a fixed scalar is never placed"
        );
    }
}

#[test]
fn agreed_peers_carry_a_list_inline_or_by_place() {
    let schema = schema(BY_REFERENCE | ANY_TYPE);
    let ty = list(s(Scalar::U32));
    let value = WireValue::List(vec![WireValue::U32(3), WireValue::U32(5)]);
    assert_eq!(round_trip(&ty, &value, &schema), value);
    assert_eq!(round_trip(&ty, &place(), &schema), place());
    // Inside another value a list has no form: the outer value has the one form byte, and the
    // list in it is as it always was.
    let nested = WireType::Struct(vec![("items".into(), ty.clone())]);
    let outer = WireValue::Struct(vec![value.clone()]);
    let mut plain = encoded(&nested, &outer, &Schema::new());
    plain.insert(0, form::INLINE);
    assert_eq!(encoded(&nested, &outer, &schema), plain);
    let mut out = Writer::new();
    let nested_place = WireValue::Struct(vec![place()]);
    assert!(encode_value(&nested, &nested_place, &schema, &mut out).is_err());
}

#[test]
fn without_any_type_a_list_has_no_form_and_a_place_does_not_encode() {
    let schema = schema(BY_REFERENCE);
    let ty = list(s(Scalar::U32));
    let value = WireValue::List(vec![WireValue::U32(3)]);
    assert_eq!(
        encoded(&ty, &value, &schema),
        encoded(&ty, &value, &Schema::new())
    );
    let mut out = Writer::new();
    assert!(encode_value(&ty, &place(), &schema, &mut out).is_err());
}

#[test]
fn a_placed_value_is_its_bare_bytes() {
    let schema = Schema::new();
    let text = WireValue::String("über".into());
    let mut out = Writer::new();
    encode_bare(&s(Scalar::String), &text, &schema, &mut out).expect("encodes");
    assert_eq!(out.buf, "über".as_bytes(), "a string is its content");
    assert_eq!(
        decode_placed(&s(Scalar::String), &out.buf, &schema),
        Ok(text)
    );
    assert!(decode_placed(&s(Scalar::String), &[0xff, 0xfe], &schema).is_err());

    let ty = list(s(Scalar::U32));
    let value = WireValue::List(vec![WireValue::U32(7), WireValue::U32(9)]);
    let mut out = Writer::new();
    encode_bare(&ty, &value, &schema, &mut out).expect("encodes");
    assert_eq!(
        out.buf,
        [2, 0, 0, 0, 7, 0, 0, 0, 9, 0, 0, 0],
        "count, then items"
    );
    assert_eq!(decode_placed(&ty, &out.buf, &schema), Ok(value.clone()));
    let mut input = Reader::new(&out.buf);
    assert_eq!(decode_bare(&ty, &mut input, &schema), Ok(value));
    // Bytes left over after the value are refused.
    let mut longer = out.buf.clone();
    longer.push(0);
    assert!(decode_placed(&ty, &longer, &schema).is_err());
}

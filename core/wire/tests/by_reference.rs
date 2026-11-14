//! Values by reference: between peers that agreed them, a port's `bytes` or opaque value has a
//! form byte and may be a place; inside another value, or without the agreement, nothing changes,
//! and a place doesn't encode.

use daedalus_types::Scalar;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::shared::{At, Place};
use daedalus_wire::{Schema, WireType, WireValue, decode_value, encode_value};

fn round_trip(ty: &WireType, value: &WireValue, schema: &Schema) -> WireValue {
    let mut out = Writer::new();
    encode_value(ty, value, schema, &mut out).expect("encodes");
    let mut input = Reader::new(&out.buf);
    let back = decode_value(ty, &mut input, schema).expect("decodes");
    input.finish().expect("nothing left over");
    back
}

#[test]
fn agreed_peers_carry_bytes_inline_or_by_place() {
    let mut schema = Schema::new();
    schema.set_by_reference(true);
    let bytes = WireType::Scalar(Scalar::Bytes);
    let inline = WireValue::Bytes(vec![1, 2, 3]);
    assert_eq!(round_trip(&bytes, &inline, &schema), inline);
    for at in [At::Lent(2), At::Fd(5)] {
        let placed = WireValue::Placed(Place {
            at,
            offset: 64,
            len: 1 << 20,
        });
        assert_eq!(round_trip(&bytes, &placed, &schema), placed);
        // An opaque value by reference needs no codec: its bytes are never decoded.
        let opaque = WireType::Opaque("t:shot".into());
        assert_eq!(round_trip(&opaque, &placed, &schema), placed);
    }
}

#[test]
fn bytes_inside_another_value_have_no_form_byte() {
    let mut schema = Schema::new();
    schema.set_by_reference(true);
    let list = WireType::List(Box::new(WireType::Scalar(Scalar::Bytes)));
    let value = WireValue::List(vec![WireValue::Bytes(vec![7; 3])]);
    let mut agreed = Writer::new();
    encode_value(&list, &value, &schema, &mut agreed).expect("encodes");
    let mut plain = Writer::new();
    encode_value(&list, &value, &Schema::new(), &mut plain).expect("encodes");
    assert_eq!(
        agreed.buf, plain.buf,
        "a compound value is the same either way"
    );
    assert_eq!(round_trip(&list, &value, &schema), value);
}

#[test]
fn without_the_agreement_bytes_are_as_before_and_a_place_is_refused() {
    let schema = Schema::new();
    let bytes = WireType::Scalar(Scalar::Bytes);
    let mut out = Writer::new();
    encode_value(&bytes, &WireValue::Bytes(vec![9; 4]), &schema, &mut out).expect("encodes");
    let mut plain = Writer::new();
    plain.bytes(&[9; 4]).expect("encodes");
    assert_eq!(out.buf, plain.buf, "no form byte without the agreement");
    let placed = WireValue::Placed(Place {
        at: At::Lent(2),
        offset: 0,
        len: 4,
    });
    assert!(encode_value(&bytes, &placed, &schema, &mut Writer::new()).is_err());
}

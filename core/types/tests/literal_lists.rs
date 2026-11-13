//! Lists, arrays and options decode from constants, and say why when they don't.

use daedalus_types::{Code, FromLiteral, Literal, literal_field};

const THREE: &[Literal] = &[Literal::Int(1), Literal::Int(2), Literal::Int(3)];

#[test]
fn a_list_decodes_item_by_item() {
    let list = Literal::List(THREE);
    assert_eq!(Vec::<u8>::from_literal(&list).unwrap(), [1, 2, 3]);
    assert_eq!(<[i64; 3]>::from_literal(&list).unwrap(), [1, 2, 3]);
    assert_eq!(Vec::<f32>::from_literal(&list).unwrap(), [1.0, 2.0, 3.0]);
    assert_eq!(Vec::<u8>::from_literal(&Literal::List(&[])).unwrap(), []);
}

#[test]
fn lists_nest() {
    const ROWS: &[Literal] = &[Literal::List(THREE), Literal::List(&[])];
    let rows = Vec::<Vec<u16>>::from_literal(&Literal::List(ROWS)).unwrap();
    assert_eq!(rows, [vec![1, 2, 3], vec![]]);
    const PAIRS: &[Literal] = &[Literal::List(THREE), Literal::List(THREE)];
    let pairs = <[[u8; 3]; 2]>::from_literal(&Literal::List(PAIRS)).unwrap();
    assert_eq!(pairs, [[1, 2, 3], [1, 2, 3]]);
}

#[test]
fn the_wrong_kind_is_refused() {
    let error = Vec::<u8>::from_literal(&Literal::Int(1)).unwrap_err();
    assert_eq!(error.code, Code::InvalidLiteral);
    let error = <[u8; 3]>::from_literal(&Literal::Bool(true)).unwrap_err();
    assert_eq!(error.code, Code::InvalidLiteral);
    // The first item that doesn't fit is the error.
    const MIXED: &[Literal] = &[Literal::Int(1), Literal::Str("two"), Literal::Int(300)];
    let error = Vec::<u8>::from_literal(&Literal::List(MIXED)).unwrap_err();
    assert_eq!(error.code, Code::InvalidLiteral);
    let error = <[u8; 3]>::from_literal(&Literal::List(MIXED)).unwrap_err();
    assert_eq!(error.code, Code::InvalidLiteral);
    const BIG: &[Literal] = &[Literal::Int(300)];
    let error = Vec::<u8>::from_literal(&Literal::List(BIG)).unwrap_err();
    assert_eq!(error.code, Code::OutOfRange);
}

#[test]
fn an_array_takes_exactly_its_length() {
    let error = <[u8; 2]>::from_literal(&Literal::List(THREE)).unwrap_err();
    assert_eq!(error.code, Code::OutOfRange);
    let error = <[u8; 4]>::from_literal(&Literal::List(THREE)).unwrap_err();
    assert_eq!(error.code, Code::OutOfRange);
    assert_eq!(<[u8; 0]>::from_literal(&Literal::List(&[])).unwrap(), []);
}

#[test]
fn unit_is_none() {
    assert_eq!(Option::<u8>::from_literal(&Literal::Unit).unwrap(), None);
    assert_eq!(
        Option::<u8>::from_literal(&Literal::Int(4)).unwrap(),
        Some(4)
    );
    assert!(Option::<u8>::from_literal(&Literal::Str("x")).is_err());
}

#[test]
fn a_struct_field_is_found_by_name() {
    const FIELDS: &[(&str, Literal)] = &[("low", Literal::Int(2)), ("taps", Literal::List(THREE))];
    assert_eq!(literal_field::<u8>(FIELDS, "low").unwrap(), 2);
    assert_eq!(literal_field::<Vec<u8>>(FIELDS, "taps").unwrap(), [1, 2, 3]);
    let error = literal_field::<u8>(FIELDS, "high").unwrap_err();
    assert_eq!(error.code, Code::InvalidLiteral);
}

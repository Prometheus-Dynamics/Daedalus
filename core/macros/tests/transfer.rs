#![allow(dead_code)]

use daedalus_author_fixture::sample::WIDEN_TRANSFER;
use daedalus_macros::transfer;
use daedalus_registry::{
    BodyRecord, NoCode, RegError, Registry, SourceAt, TransferClass, TransferRecord,
};
use daedalus_types::Scalar;

use daedalus_author_fixture as fixture;

struct Device;

#[transfer(from = "t.host", to = "t.device", cost = import, backend = fixture)]
fn upload(value: u8, device: &Device) -> Result<u8, String> {
    let _ = device;
    Ok(value)
}

const FROM: &str = "t.named";

#[transfer(from = FROM, to = "t.device", cost = handoff, backend = fixture)]
fn from_const(value: &u32) -> u32 {
    *value
}

const fn by_hand(id: &'static str, at: &'static str, from: &'static str) -> TransferRecord {
    TransferRecord {
        id,
        at: SourceAt(at),
        key: Scalar::U8.key(),
        output: Scalar::U8.key(),
        from,
        to: "t.device",
        class: TransferClass::Copy,
        body: BodyRecord {
            backend: "fixture",
            at: SourceAt(at),
            needs: &[],
            state: None,
            ports: &[],
            memory: &[],
            code: &NoCode,
            text: "",
        },
    }
}

static BY_HAND: TransferRecord = by_hand("by.hand", "hand.rs:1", "t.other");
static CLASHING: TransferRecord = by_hand("clash", "clash.rs:1", "t.host");

#[test]
fn the_record_has_what_the_attribute_said() {
    let record = &UPLOAD_TRANSFER;
    assert_eq!(record.id, "transfer::upload");
    assert_eq!(
        (record.from, record.to, record.class),
        ("t.host", "t.device", TransferClass::Import)
    );
    assert_eq!(record.key, Scalar::U8.key());
    assert_eq!(record.output, Scalar::U8.key());
    assert!(
        record
            .at
            .0
            .replace('\\', "/")
            .starts_with("core/macros/tests/transfer.rs:"),
        "{}",
        record.at.0
    );
    assert_eq!(FROM_CONST_TRANSFER.from, "t.named");
    assert_eq!(FROM_CONST_TRANSFER.class, TransferClass::Handoff);
    assert_eq!(WIDEN_TRANSFER.output, Scalar::U32.key());
}

#[test]
fn it_joins_the_registry_beside_a_hand_written_one() {
    let mut registry = Registry::new();
    registry.register_transfer(&UPLOAD_TRANSFER).unwrap();
    registry.register_transfer(&BY_HAND).unwrap();
    assert_eq!(registry.transfers().count(), 2);
    let found = registry
        .transfer(&Scalar::U8.key(), "t.host", "t.device")
        .unwrap();
    assert_eq!(found.id, "transfer::upload");
    assert!(
        registry
            .transfer(&Scalar::U32.key(), "t.host", "t.device")
            .is_none()
    );
}

#[test]
fn one_type_has_one_handler_between_two_spaces() {
    let mut registry = Registry::new();
    registry.register_transfer(&UPLOAD_TRANSFER).unwrap();
    match registry.register_transfer(&CLASHING).unwrap_err() {
        RegError::DuplicateTransfer {
            first,
            second,
            from,
            ..
        } => {
            assert!(first.0.contains("transfer.rs"));
            assert_eq!((second, from), (SourceAt("clash.rs:1"), "t.host"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn compile_results() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/transfer/pass/*.rs");
    cases.compile_fail("tests/ui/transfer/fail/*.rs");
}

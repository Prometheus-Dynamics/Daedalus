//! The bytes a parameter update is sent as.

#![cfg(feature = "chip-params")]

use daedalus_cpu::chip::wire::{Reader, SliceWriter, WireError, Write};
use daedalus_cpu::chip::{ParamUpdate, Scalar, ScalarKind};

fn encoded(update: ParamUpdate) -> Vec<u8> {
    let mut buf = [0; ParamUpdate::MAX_LEN];
    let len = update.to_slice(&mut buf).unwrap();
    buf[..len].to_vec()
}

#[test]
fn every_kind_round_trips() {
    for kind in ScalarKind::ALL {
        let (low, high) = Scalar::bounds(kind);
        for value in [low, high] {
            let update = ParamUpdate { id: 400, value };
            assert_eq!(
                ParamUpdate::decode(&encoded(update)),
                Ok(update),
                "{kind:?}"
            );
        }
    }
}

#[test]
fn the_golden_message_holds() {
    let update = ParamUpdate {
        id: 1,
        value: Scalar::F32(2.5),
    };
    assert_eq!(encoded(update), [0x01, 0x09, 0x00, 0x00, 0x20, 0x40]);
}

#[test]
fn messages_stay_within_the_longest_size() {
    let longest = ParamUpdate {
        id: u16::MAX,
        value: Scalar::U64(u64::MAX),
    };
    assert_eq!(encoded(longest).len(), ParamUpdate::MAX_LEN);
    let shortest = ParamUpdate {
        id: 0,
        value: Scalar::Bool(true),
    };
    assert_eq!(encoded(shortest).len(), 3);
}

#[test]
fn varints_match_postcard() {
    let mut buf = [0; 16];
    let mut out = SliceWriter::new(&mut buf);
    out.varint(300);
    out.zigzag(-5);
    let len = out.finish().unwrap();
    assert_eq!(buf[..len], [0xac, 0x02, 9]);
    let mut reader = Reader::new(&buf[..len]);
    assert_eq!((reader.varint(), reader.zigzag()), (Ok(300), Ok(-5)));
    assert_eq!(reader.finish(), Ok(()));
}

#[test]
fn bad_input_is_an_error() {
    assert_eq!(Reader::new(&[0x80]).varint(), Err(WireError));
    assert_eq!(Reader::new(&[]).u8(), Err(WireError));
    // A kind that doesn't exist, and a bool that isn't 0 or 1.
    assert_eq!(Reader::new(&[11, 0]).scalar(), Err(WireError));
    assert_eq!(Reader::new(&[0, 2]).scalar(), Err(WireError));
    // Bytes left over.
    assert!(ParamUpdate::decode(&[0, 0, 1, 0]).is_err());
    let mut tiny = [0; 1];
    let mut out = SliceWriter::new(&mut tiny);
    out.varint(300);
    assert_eq!(out.finish(), Err(WireError));
}

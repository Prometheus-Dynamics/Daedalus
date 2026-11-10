//! Corpus files: each is a packet or a body the decoder must refuse with a typed error.

use daedalus_wire::codec::Reader;
use daedalus_wire::{HostMsg, PacketError, Schema, WireError, WorkerMsg, read_packet};

fn packet(bytes: &[u8], cap: u32) -> Result<Vec<u8>, PacketError> {
    let mut buf = Vec::new();
    read_packet(&mut &bytes[..], cap, &mut buf).map(|()| buf)
}

#[test]
fn a_truncated_packet_is_a_typed_error() {
    let bytes = include_bytes!("corpus/truncated_packet.bin");
    assert!(matches!(packet(bytes, 1024), Err(PacketError::Truncated)));
}

#[test]
fn a_length_over_the_cap_is_refused_before_anything_is_read() {
    let bytes = include_bytes!("corpus/length_over_cap.bin");
    match packet(bytes, 16 * 1024 * 1024) {
        Err(PacketError::TooLarge { len, cap }) => {
            assert_eq!(len, u32::MAX);
            assert_eq!(cap, 16 * 1024 * 1024);
        }
        other => panic!("expected too large, got {other:?}"),
    }
}

#[test]
fn invalid_utf8_in_a_string_is_a_typed_error() {
    let body = include_bytes!("corpus/invalid_utf8_log.bin");
    let schema = Schema::new();
    assert_eq!(
        WorkerMsg::decode(body, &schema).unwrap_err(),
        WireError::InvalidUtf8
    );
}

#[test]
fn an_unknown_kind_is_a_typed_error() {
    let body = include_bytes!("corpus/unknown_kind.bin");
    assert_eq!(
        HostMsg::decode(body, &Schema::new()).unwrap_err(),
        WireError::UnknownKind(0x7f)
    );
}

#[test]
fn trailing_bytes_after_a_message_are_a_typed_error() {
    let body = include_bytes!("corpus/trailing_bytes.bin");
    assert_eq!(
        WorkerMsg::decode(body, &Schema::new()).unwrap_err(),
        WireError::TrailingBytes(1)
    );
}

#[test]
fn a_count_larger_than_the_bytes_left_is_refused() {
    // A count of u32::MAX with nothing after it: refused, not allocated.
    let body = [0xffu8, 0xff, 0xff, 0xff];
    let mut input = Reader::new(&body);
    assert!(matches!(
        input.count(),
        Err(WireError::CountTooLarge { .. })
    ));
}

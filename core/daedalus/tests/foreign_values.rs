//! The code of `docs/foreign-values.md`. The doc quotes its Rust blocks verbatim, and the docs test
//! in `docs.rs` checks that each quote is still here, so the doc cannot drift from what compiles.
//! Nothing here touches a device or a dma-buf: an `OwnedFd` from a pipe stands in for one.

#![cfg(unix)]

use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use daedalus::layers::types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
use daedalus::layers::value::{Owner, Value};

struct Buffer {
    fd: OwnedFd,
    width: u32,
    height: u32,
    stride: u32,
}

impl Typed for Buffer {
    const KEY: TypeKey = TypeKey::new("acme:buffer");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

fn hold(buffer: Buffer) -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Typed(Arc::new(buffer)),
    }
}

fn an_buffer() -> Buffer {
    let (reader, _writer) = std::io::pipe().expect("a pipe");
    Buffer {
        fd: reader.into(),
        width: 640,
        height: 480,
        stride: 2560,
    }
}

#[test]
fn an_buffer_is_typed_by_its_key_and_is_opaque() {
    assert_eq!(Buffer::KEY.as_str(), "acme:buffer");
    assert_eq!(Buffer::EXPR, TypeExpr::Opaque(Buffer::KEY));
}

#[test]
fn a_value_holds_the_buffer_and_hands_it_back_by_type() {
    let value = hold(an_buffer());
    let shared = value.clone_inline();
    let seen: &Buffer = value.typed().expect("a buffer");
    assert_eq!((seen.width, seen.height, seen.stride), (640, 480, 2560));
    assert!(seen.fd.as_raw_fd() >= 0);
    assert_eq!(
        shared.typed::<Buffer>().map(|buffer| buffer.width),
        Some(640)
    );
    assert!(value.typed::<u32>().is_none());
}

#[test]
fn dropping_the_last_holder_drops_the_buffer() {
    let kept = Arc::new(an_buffer());
    let value = Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Typed(kept.clone()),
    };
    drop(value);
    assert_eq!(Arc::strong_count(&kept), 1);
}

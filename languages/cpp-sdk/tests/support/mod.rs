//! Shared test helpers: C tables built in Rust, the worker's packets read back, and the C-ABI calls a
//! node makes, which are this crate's own exports linked into the test.

#![allow(dead_code)]

use std::ffi::{CString, c_char, c_int};
use std::io::{self, Write};
use std::ptr;
use std::sync::{Arc, Mutex};

use daedalus_lang_cpp_sdk::ffi::{DdNode, DdNodeFn, DdPort, DdType, DdValue};
use daedalus_lang_cpp_sdk::{DdCtx, serve_io};
use daedalus_wire::{Schema, WorkerMsg};

// The C scalar and kind numbers the header gives.
pub const DD_BOOL: c_int = 1;
pub const DD_I8: c_int = 2;
pub const DD_I16: c_int = 3;
pub const DD_I32: c_int = 4;
pub const DD_I64: c_int = 5;
pub const DD_U8: c_int = 7;
pub const DD_U16: c_int = 8;
pub const DD_U32: c_int = 9;
pub const DD_U64: c_int = 10;
pub const DD_F64: c_int = 13;
pub const DD_STRING: c_int = 14;
pub const DD_BYTES: c_int = 15;
pub const K_SCALAR: c_int = 0;
pub const K_OPTIONAL: c_int = 2;
pub const K_LIST: c_int = 3;
pub const K_MAP: c_int = 4;
pub const K_TUPLE: c_int = 5;
pub const K_STRUCT: c_int = 6;
pub const K_ENUM: c_int = 7;

/// Leaks a value, so a C table can point at it for the rest of the test.
pub fn leak<T>(value: T) -> *const T {
    Box::leak(Box::new(value)) as *const T
}

/// Leaks a slice, so a C table can point at its first item.
pub fn leak_slice<T: Copy>(items: &[T]) -> *const T {
    Box::leak(items.to_vec().into_boxed_slice()).as_ptr()
}

/// A NUL-terminated string that lives for the test.
pub fn c_str(text: &str) -> *const c_char {
    CString::new(text)
        .expect("no NUL in a test name")
        .into_raw()
        .cast_const()
}

/// A scalar type.
pub fn scalar(scalar: c_int) -> *const DdType {
    leak(DdType {
        kind: K_SCALAR,
        scalar,
        key: ptr::null(),
        items: ptr::null(),
        names: ptr::null(),
        count: 0,
    })
}

/// A type with a kind and nested items.
pub fn nested(kind: c_int, items: &[*const DdType]) -> *const DdType {
    leak(DdType {
        kind,
        scalar: 0,
        key: ptr::null(),
        items: leak_slice(items),
        names: ptr::null(),
        count: items.len(),
    })
}

pub fn port(name: &str, ty: *const DdType) -> DdPort {
    DdPort {
        name: c_str(name),
        ty,
    }
}

/// A node with its id, ports and run function.
pub fn node(id: &str, inputs: &[DdPort], outputs: &[DdPort], run: DdNodeFn) -> DdNode {
    DdNode {
        id: c_str(id),
        inputs: leak_slice(inputs),
        input_count: inputs.len(),
        outputs: leak_slice(outputs),
        output_count: outputs.len(),
        state: ptr::null(),
        run: Some(run),
    }
}

/// A node that keeps state of `state`.
pub fn stateful(
    id: &str,
    inputs: &[DdPort],
    outputs: &[DdPort],
    state: *const DdType,
    run: DdNodeFn,
) -> DdNode {
    DdNode {
        state,
        ..node(id, inputs, outputs, run)
    }
}

/// A packet's bytes: a 4-byte little-endian length, then the body.
pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

/// A writer the test can read back after the loop has taken it.
#[derive(Clone, Default)]
pub struct Sink(pub Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("the sink").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Splits a stream of framed packets into their bodies.
pub fn split_packets(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while bytes.len() >= 4 {
        let len = u32::from_le_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
        out.push(bytes[4..4 + len].to_vec());
        bytes = &bytes[4 + len..];
    }
    out
}

/// Runs the loop over `input` and returns the packets it wrote, as bodies.
pub fn serve(nodes: &[DdNode], input: &[u8]) -> Vec<Vec<u8>> {
    let sink = Sink::default();
    let mut reader = io::Cursor::new(input.to_vec());
    serve_io(nodes, &mut reader, Box::new(sink.clone()), "test-worker").expect("the loop serves");
    let written = sink.0.lock().expect("the sink").clone();
    split_packets(&written)
}

/// The schema a hello declares, from the first packet of a served stream.
pub fn schema_of(hello: &[u8]) -> Schema {
    match WorkerMsg::decode(hello, &Schema::new()).expect("the hello decodes") {
        WorkerMsg::Hello(hello) => Schema::from_hello(&hello, &[]),
        other => panic!("the first packet is not a hello: {other:?}"),
    }
}

/// The C-ABI calls a node makes: this crate's own exports, linked into the test binary.
///
/// # Safety
/// The declarations match the crate's exports; each takes the context a node is given.
pub mod abi {
    use super::*;

    // SAFETY: these are this crate's own C-ABI exports, which a node calls through its context.
    unsafe extern "C" {
        pub fn dd_alloc(cx: *mut DdCtx, size: usize) -> *mut std::ffi::c_void;
        pub fn dd_progress(cx: *mut DdCtx, fraction: f32, message: *const c_char);
        pub fn dd_log(cx: *mut DdCtx, level: *const c_char, text: *const c_char);
        pub fn dd_detail(cx: *mut DdCtx, text: *const c_char);
        pub fn dd_fault_code(cx: *mut DdCtx, code: u32);
        pub fn dd_fault(cx: *mut DdCtx, code: *const c_char, reason: *const c_char);
        pub fn dd_state(cx: *mut DdCtx) -> *mut DdValue;
        pub fn dd_set_state(cx: *mut DdCtx, value: DdValue);
    }
}

/// A value holding an integer.
pub fn int_value(v: i64) -> DdValue {
    let mut value = DdValue::ZERO;
    value.data.i = v;
    value
}

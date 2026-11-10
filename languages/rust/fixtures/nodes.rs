//! The canonical fixture nodes this SDK can declare with `#[node]` today. Port types must be
//! `Typed` scalars, strings or byte fields: compound and opaque types, a fallible body, and the
//! `in` port name (a keyword) are not expressible here, so those fixtures are not served.

use daedalus_macros::node;

#[node(id = "fixture.scalar_add", inputs(a, b), outputs(out), backend = daedalus_lang_rust)]
pub fn scalar_add(a: i64, b: i64) -> i64 {
    a.wrapping_add(b)
}

#[node(id = "fixture.multi_output", inputs(x), outputs(a, b), backend = daedalus_lang_rust)]
pub fn multi_output(x: u32) -> (u32, u32) {
    (x, x.wrapping_mul(2))
}

#[node(id = "fixture.raw_io", inputs(payload), outputs(out), backend = daedalus_lang_rust)]
pub fn raw_io(payload: Vec<u8>) -> Vec<u8> {
    payload.into_iter().rev().collect()
}

/// The nodes this worker serves.
pub const NODES: &[&daedalus_registry::NodeRecord] =
    &[&SCALAR_ADD_NODE, &MULTI_OUTPUT_NODE, &RAW_IO_NODE];

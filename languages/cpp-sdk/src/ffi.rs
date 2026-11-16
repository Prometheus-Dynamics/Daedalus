//! The C layouts of the surface in `daedalus.h`. A type, a value, a port and a node are the same
//! bytes on both sides: a C table is read where it lies, and a C value is written where the C side
//! reads it.

use std::ffi::{c_char, c_int};

use crate::ctx::DdCtx;

/// The kinds a type can be, numbered as `dd_kind` numbers them.
pub const KIND_SCALAR: c_int = 0;
pub const KIND_OPAQUE: c_int = 1;
pub const KIND_OPTIONAL: c_int = 2;
pub const KIND_LIST: c_int = 3;
pub const KIND_MAP: c_int = 4;
pub const KIND_TUPLE: c_int = 5;
pub const KIND_STRUCT: c_int = 6;
pub const KIND_ENUM: c_int = 7;

/// The most ports a node may have, and the most nodes a run may name: the limits the C loop has
/// always had.
pub const PORTS: usize = 8;
pub const MAX_NODES: usize = 64;

/// A type, as `dd_type`. Which fields a kind reads is the header's rule: `scalar` for a scalar,
/// `key` for an opaque type, `items` for the kinds that nest, and `names` for a struct or an enum.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdType {
    pub kind: c_int,
    pub scalar: c_int,
    pub key: *const c_char,
    pub items: *const *const DdType,
    pub names: *const *const c_char,
    pub count: usize,
}

/// A value, as `dd_value`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdValue {
    pub present: bool,
    pub data: DdData,
}

impl DdValue {
    /// A value with nothing set: the zero every C value starts as.
    pub const ZERO: DdValue = DdValue {
        present: false,
        data: DdData { u: 0 },
    };
}

/// The payload of a value: the field its type names.
#[repr(C)]
#[derive(Clone, Copy)]
pub union DdData {
    pub i: i64,
    pub u: u64,
    pub f: f64,
    pub b: bool,
    pub bytes: DdBytes,
    pub seq: DdSeq,
    pub en: DdEnum,
}

/// Bytes: a string, a `bytes` value or an opaque value.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdBytes {
    pub data: *const u8,
    pub len: usize,
}

/// A list, tuple, struct or map (flattened to key, value pairs). An optional's one value is kept
/// here too, at `items[0]`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdSeq {
    pub items: *mut DdValue,
    pub count: usize,
}

/// An enum: its variant, and the payload when the variant has one.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdEnum {
    pub variant: usize,
    pub payload: *mut DdValue,
}

/// A node's port: its name and type, as `dd_port`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdPort {
    pub name: *const c_char,
    pub ty: *const DdType,
}

/// A node function, as `dd_node_fn`: the context, the inputs, and the outputs it fills.
pub type DdNodeFn = unsafe extern "C" fn(*mut DdCtx, *const DdValue, *mut DdValue) -> c_int;

/// A node, as `dd_node`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DdNode {
    pub id: *const c_char,
    pub inputs: *const DdPort,
    pub input_count: usize,
    pub outputs: *const DdPort,
    pub output_count: usize,
    pub state: *const DdType,
    pub run: Option<DdNodeFn>,
}

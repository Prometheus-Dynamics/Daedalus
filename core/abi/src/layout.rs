//! The layout of everything that crosses the boundary, one shape at a time.
//!
//! A shape is either frozen or growable. A frozen shape is passed by value, or sits in an array
//! the other side walks with its own stride: it can't change without breaking every build made
//! before. A growable shape is a single value passed by pointer, or a table read through the
//! size its maker declares: it may gain fields at its end, and a build made before the new
//! fields sees a prefix of it. Fields before the `|` in the table below are the ones every build
//! has. Fields after it were added since, in order, and each means "absent" when it is zero.

use core::mem::{align_of, offset_of, size_of};

use crate::foreign::{ForeignHandle, ForeignInterfaceInfo, ForeignOwner};
use crate::shape::{AbiDesc, FieldInfo, ShapeDesc, ShapeInfo};
use crate::{
    AbiVersion, BackendTable, BoundaryPort, BytesC, CodecC, DetailSink, Fence, FencePoll,
    HandedRecord, Handle, NodeC, PluginEntry, PortC, RegionView, Slot, Slots, Status, Step, StepCx,
    StepEnd, Text, UnitOut,
};

/// FNV-1a over the words, each as little-endian bytes.
pub const fn layout_hash(words: &[u64]) -> u64 {
    let mut hash = FNV_START;
    let mut i = 0;
    while i < words.len() {
        hash = mix(hash, words[i]);
        i += 1;
    }
    hash
}

pub(crate) const FNV_START: u64 = 0xcbf2_9ce4_8422_2325;

/// One more word into an FNV-1a hash.
pub(crate) const fn mix(mut hash: u64, word: u64) -> u64 {
    let bytes = word.to_le_bytes();
    let mut b = 0;
    while b < 8 {
        hash ^= bytes[b] as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
        b += 1;
    }
    hash
}

/// The size of a field, from a function that borrows it. The function is never called.
const fn field_size<T, F>(_: fn(&T) -> &F) -> u32 {
    size_of::<F>() as u32
}

macro_rules! one {
    ($field:tt) => {
        1
    };
}

macro_rules! field {
    ($ty:ty, $field:tt) => {
        FieldInfo {
            name: stringify!($field),
            offset: offset_of!($ty, $field) as u32,
            size: field_size::<$ty, _>(|shape| &shape.$field),
        }
    };
}

macro_rules! shapes {
    ($($(#[$meta:meta])* $id:literal $kind:ident $ty:ty { $($base:tt),* $(| $($more:tt),*)? })*) => {
        &[$(
            $(#[$meta])*
            ShapeInfo {
                id: $id,
                name: stringify!($ty),
                frozen: shapes!(@frozen $kind),
                size: size_of::<$ty>() as u32,
                align: align_of::<$ty>() as u32,
                required: 0 $(+ one!($base))*,
                fields: &[$(field!($ty, $base),)* $($(field!($ty, $more),)*)?],
            },
        )*]
    };
    (@frozen frozen) => { true };
    (@frozen growable) => { false };
}

/// Every shape that crosses the boundary. An id is a shape's for good: a shape that goes takes
/// its id with it, and a new shape takes a new one.
pub const SHAPES: &[ShapeInfo] = shapes! {
    1 frozen Status { 0 }
    2 frozen Handle { 0 }
    3 frozen FencePoll { }
    4 frozen Fence { poll, data }
    5 frozen Slot { state, space, type_index, owner, version, reserved, data, len, cap }
    6 frozen Slots { cells, len, reserved }
    7 frozen DetailSink { context, write }
    #[cfg(not(daedalus_abi_baseline))]
    8 growable StepCx { at, reason_len, reason, cancel, progress, flags, code, now_ns, handover | detail, step }
    #[cfg(daedalus_abi_baseline)]
    8 growable StepCx { at, reason_len, reason, cancel, progress, flags, code, now_ns, handover }
    9 frozen StepEnd { }
    10 frozen Step { run, data, release }
    11 frozen AbiVersion { struct_size, major, minor, layout }
    12 frozen ForeignInterfaceInfo { key_ptr, key_len, version, layout_hash }
    13 frozen ForeignOwner { ptr, retain, release }
    14 frozen ForeignHandle { data, vtable, interface, owner }
    15 frozen Text { ptr, len }
    16 frozen PortC { name, key, optional }
    17 growable NodeC { id, source, backend, text, inputs, input_count, outputs, output_count }
    18 frozen BoundaryPort { node, port, reserved, type_index, slot }
    19 growable RegionView { nodes, node_count, inputs, input_count, outputs, output_count }
    20 frozen UnitOut { step, node, reserved }
    21 growable BackendTable { id, can_take, compile }
    22 growable CodecC { key, value_key, wire_key, backend, layout, cost, lossy, to_value, from_value, to_bytes, from_bytes }
    23 frozen BytesC { ptr, len, data, release }
    24 frozen HandedRecord { refs, bytes, keep, keep_drop, recycle, home }
    25 growable PluginEntry { name, nodes, node_count, backends, backend_count, codecs, codec_count, produce, release }
    26 frozen ShapeDesc { id, size, extent, fields, needs, hash }
    27 frozen AbiDesc { shapes, count, reserved }
};

const COUNT: usize = SHAPES.len();

const fn describe_all() -> [ShapeDesc; COUNT] {
    let mut out = [ShapeDesc::NONE; COUNT];
    let mut at = 0;
    while at < COUNT {
        out[at] = SHAPES[at].desc();
        at += 1;
    }
    out
}

/// This build's shapes, as the other side of a boundary reads them.
pub static DESCRIPTORS: [ShapeDesc; COUNT] = describe_all();

/// This build's descriptors, as the table a plugin hands its host.
pub fn abi_desc() -> AbiDesc {
    AbiDesc {
        shapes: DESCRIPTORS.as_ptr(),
        count: COUNT as u32,
        reserved: 0,
    }
}

const fn frozen_hash() -> u64 {
    let mut hash = FNV_START;
    let mut at = 0;
    while at < COUNT {
        let shape = &SHAPES[at];
        hash = mix(hash, shape.id as u64);
        hash = mix(hash, shape.hash_of(shape.required as usize));
        at += 1;
    }
    hash
}

/// One hash over what can't change without breaking a build made before: every frozen shape,
/// and the fields of every growable shape that every build has. A field added to the end of a
/// growable shape doesn't move it.
pub const FROZEN_HASH: u64 = frozen_hash();

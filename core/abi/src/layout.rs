//! One hash over the layout of everything that crosses the boundary.

use core::mem::{align_of, offset_of, size_of};

use crate::foreign::{ForeignHandle, ForeignInterfaceInfo, ForeignOwner};

use crate::{
    AbiVersion, BackendTable, BoundaryPort, Fence, FencePoll, Handle, NodeC, PluginEntry, PortC,
    RegionView, Slot, Slots, Status, Step, StepCx, StepEnd, Text, UnitOut,
};

/// FNV-1a over the words, each as little-endian bytes.
pub const fn layout_hash(words: &[u64]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut i = 0;
    while i < words.len() {
        let bytes = words[i].to_le_bytes();
        let mut b = 0;
        while b < 8 {
            hash ^= bytes[b] as u64;
            hash = hash.wrapping_mul(0x100_0000_01b3);
            b += 1;
        }
        i += 1;
    }
    hash
}

macro_rules! shape {
    ($($ty:ty { $($field:tt),* })*) => {
        [$(
            size_of::<$ty>() as u64,
            align_of::<$ty>() as u64,
            $(offset_of!($ty, $field) as u64,)*
        )*]
    };
}

/// Size, alignment and field offsets of every shape that crosses the boundary.
pub const SHAPES: &[u64] = &shape! {
    Status { 0 }
    Handle { 0 }
    FencePoll { }
    Fence { poll, data }
    Slot { state, space, type_index, owner, version, reserved, data, len, cap }
    Slots { cells, len, reserved }
    StepCx { at, reason_len, reason, cancel, progress, flags, code, now_ns }
    StepEnd { }
    Step { run, data, release }
    AbiVersion { struct_size, major, minor, layout }
    ForeignInterfaceInfo { key_ptr, key_len, version, layout_hash }
    ForeignOwner { ptr, retain, release }
    ForeignHandle { data, vtable, interface, owner }
    Text { ptr, len }
    PortC { name, key, optional }
    NodeC { id, source, backend, text, inputs, input_count, outputs, output_count }
    BoundaryPort { node, port, reserved, type_index, slot }
    RegionView { nodes, node_count, inputs, input_count, outputs, output_count }
    UnitOut { step, node, reserved }
    BackendTable { id, can_take, compile }
    PluginEntry { layout, name, nodes, node_count, backends, backend_count, release }
};

pub const LAYOUT_HASH: u64 = layout_hash(SHAPES);

//! The layout hash of the stable ABI: the handshake's `layout` field (see
//! [`STABLE_HANDSHAKE`](super::STABLE_HANDSHAKE)). It hashes the size, alignment and field
//! offsets of every `#[repr(C)]` type that crosses the stable boundary, with `lemnos-abi`'s
//! [`layout_hash`](lemnos_abi::layout_hash), so a change to any of them changes the hash even
//! under the same [`STABLE_ABI_VERSION`](super::STABLE_ABI_VERSION).
//!
//! A type whose fields are private to another crate (`ForeignHandle`) contributes its size and
//! alignment only: its layout is fixed by its own crate, and the foreign interface hashes cover
//! its vtables.

use std::mem::{align_of, offset_of, size_of};

use super::codecs::{StableCodecEntry, StableCodecTable};
use super::services::{HostServices, MetricRecord};
use super::value::{StableEnum, StableField, StablePodView, StableValue};
use super::{StableExtension, StableExtensionV4, StableHandlers, StableInput, StableOutputSink};
use crate::dylib::StrSink;
use crate::dylib::StrView;
use crate::transport::{ForeignHandle, ForeignInterfaceInfo};

/// Size, alignment, then the offsets of `$field`s, of `$t`, as `u64` words.
macro_rules! shape {
    ($t:ty $(, $field:ident)*) => {
        [size_of::<$t>() as u64, align_of::<$t>() as u64 $(, offset_of!($t, $field) as u64)*]
    };
}

/// Every hashed type, in the order it is hashed.
const SHAPES: &[&[u64]] = &[
    &shape!(StableInput, port, fanin, value),
    &shape!(StableOutputSink, ctx, push),
    &shape!(StableValue, tag, scalar, ptr, len),
    &shape!(StableField, name_ptr, name_len, value),
    &shape!(
        StablePodView,
        hash,
        size,
        align,
        count,
        data,
        name_ptr,
        name_len
    ),
    &shape!(StableEnum, name_ptr, name_len, value),
    &shape!(
        MetricRecord,
        name_ptr,
        name_len,
        kind,
        scalar,
        text_ptr,
        text_len
    ),
    &shape!(HostServices, sink, record_metric, metadata, graph_metadata),
    &shape!(StableHandlers, version, create, invoke, release, unload),
    &shape!(StableExtensionV4, version, set_consts, set_services),
    &shape!(
        StableExtension,
        version,
        set_consts,
        set_services,
        live_objects
    ),
    &shape!(
        StableCodecEntry,
        key,
        schema,
        ctx,
        to_value,
        from_value,
        to_bytes,
        from_bytes
    ),
    &shape!(StableCodecTable, version, entries, len, build),
    &shape!(StrView, ptr, len),
    &shape!(StrSink, context, write),
    &[
        size_of::<ForeignHandle>() as u64,
        align_of::<ForeignHandle>() as u64,
    ],
    &shape!(ForeignInterfaceInfo, version, layout_hash),
];

const fn word_count(parts: &[&[u64]]) -> usize {
    let mut count = 0;
    let mut part = 0;
    while part < parts.len() {
        count += parts[part].len();
        part += 1;
    }
    count
}

const fn concat<const N: usize>(parts: &[&[u64]]) -> [u64; N] {
    let mut out = [0; N];
    let (mut n, mut part) = (0, 0);
    while part < parts.len() {
        let mut word = 0;
        while word < parts[part].len() {
            out[n] = parts[part][word];
            n += 1;
            word += 1;
        }
        part += 1;
    }
    out
}

const WORDS: usize = word_count(SHAPES);
const SHAPE_WORDS: [u64; WORDS] = concat(SHAPES);

/// The stable ABI's layout hash (the handshake's `layout`). Computed at compile time.
pub const STABLE_LAYOUT_HASH: u64 = lemnos_abi::layout_hash(&SHAPE_WORDS);

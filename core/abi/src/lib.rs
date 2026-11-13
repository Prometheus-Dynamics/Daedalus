//! The types that cross a plugin boundary.
//!
//! Everything here is `repr(C)` or `repr(transparent)`, allocates nothing, and is covered by
//! [`SHAPES`], so a host and a plugin built apart can check they fit before any call: see
//! [`fits`].
//! A step is the unit of work: a function pointer and its data.
//!
//! Backends are written without `unsafe`, so [`Step::of`] and, with the `alloc` feature,
//! [`Step::boxed`] make a [`Step`] from safe code.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod backend;
#[cfg(feature = "count-alloc")]
pub mod counting;
mod features;
mod fence;
mod foreign;
mod guard;
mod handed;
mod handle;
#[cfg(not(daedalus_abi_baseline))]
mod kept;
mod layout;
mod message;
mod out;
mod plain;
mod shape;
mod slots;
mod status;
mod step;
mod step_cx;
#[cfg(feature = "test-run")]
pub mod testing;
#[cfg(all(feature = "std", not(daedalus_abi_baseline)))]
pub use kept::{KeptDetail, keep_detail};
mod version;
mod word;

pub use backend::{
    BackendTable, BoundaryPort, ByteCodecFn, BytesC, BytesReleaseFn, CanTakeFn, CodecC, CompileFn,
    ENTRY_SYMBOL, EntryFn, NO_BYTES, NodeC, OLD_ENTRY_SYMBOL, PluginEntry, PortC, ProduceBytesFn,
    RegionView, ReleaseEntryFn, SHAPES_SYMBOL, ShapesFn, Text, UnitOut,
};
pub use features::Features;
pub use fence::{Fence, FencePoll, FenceWord};
pub use foreign::{
    ForeignHandle, ForeignInterface, ForeignInterfaceInfo, ForeignInterfaceMismatch, ForeignOwner,
    ForeignRef, ProvideForeign, foreign_layout_hash,
};
#[cfg(feature = "std")]
pub use guard::panic_message;
pub use guard::{guard, guard_or, guard_step};
pub use handed::{
    HANDOVER_PENDING, HandedRecord, OWNER_HANDED, SharedBytes, handed_release, handed_retain,
};
pub use handle::Handle;
pub use layout::{DESCRIPTORS, FROZEN_HASH, SHAPES, abi_desc, layout_hash};
pub use message::MessageSink;
pub use out::{Written, write_out, write_text};
pub use plain::PlainData;
pub use shape::{
    AbiDesc, FieldInfo, Grown, Mismatch, ShapeDesc, ShapeInfo, find, fits, read_grown,
};
pub use slots::{NO_SLOT, Slot, SlotState, Slots};
pub use status::Status;
pub use step::OwnedStep;
pub use step::{Step, StepEnd, StepWord};
pub use step_cx::{
    DetailSink, Fault, FixedError, NO_NODE, Progress, REASON_CAPACITY, REASON_CUT, RESTART, StepCx,
};
pub use version::{ABI_SYMBOL, AbiVersion, DAEDALUS_ABI};
pub use word::Word;

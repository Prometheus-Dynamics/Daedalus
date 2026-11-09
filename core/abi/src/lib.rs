//! The types that cross a plugin boundary.
//!
//! Everything here is `repr(C)` or `repr(transparent)`, allocates nothing, and is covered by
//! [`LAYOUT_HASH`], so a host and a plugin built apart can check they agree before any call.
//! A step is the unit of work: a function pointer and its data.
//!
//! Backends are written without `unsafe`, so [`Step::of`] and, with the `alloc` feature,
//! [`Step::boxed`] make a [`Step`] from safe code.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "count-alloc")]
pub mod counting;
mod features;
mod fence;
mod foreign;
mod guard;
mod handle;
mod layout;
mod message;
mod out;
mod slots;
mod status;
mod step;
mod step_cx;
#[cfg(feature = "test-run")]
pub mod testing;
mod version;
mod word;

pub use features::Features;
pub use fence::{Fence, FencePoll, FenceWord};
pub use foreign::{
    ForeignHandle, ForeignInterface, ForeignInterfaceInfo, ForeignInterfaceMismatch, ForeignOwner,
    ForeignRef, ProvideForeign, foreign_layout_hash,
};
pub use guard::{guard, guard_or};
pub use handle::Handle;
pub use layout::{LAYOUT_HASH, SHAPES, layout_hash};
pub use message::MessageSink;
pub use out::{Written, write_out, write_text};
pub use slots::{NO_SLOT, Slot, SlotState, Slots};
pub use status::Status;
#[cfg(feature = "alloc")]
pub use step::OwnedStep;
pub use step::{Step, StepEnd, StepWord};
pub use step_cx::{
    ERROR_REASON_CAPACITY, Fault, FixedError, NO_NODE, Progress, REASON_CAPACITY, RESTART, StepCx,
};
pub use version::{ABI_SYMBOL, AbiVersion, DAEDALUS_ABI};
pub use word::Word;

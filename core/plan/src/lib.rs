//! The solved form of a graph: a flat list of steps with numbered slots, and the little-endian
//! Blob that stores it. Reading needs no heap and never panics, whatever the bytes are.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod format;
mod read;
#[cfg(feature = "alloc")]
mod write;

pub use format::{MAGIC, PlanError, Span, StepKind, StepRecord, VERSION};
pub use read::{PlanView, Words};
#[cfg(feature = "alloc")]
pub use write::{PlanData, encode};

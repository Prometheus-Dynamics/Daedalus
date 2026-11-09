//! The CPU backend. A `#[node]` function becomes a step: a wrapper that binds each of its cells
//! to a parameter, calls the function, and writes what it returned.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
mod backend;
mod body;
#[cfg(all(feature = "std", feature = "chip-params"))]
pub mod param_host;
#[cfg(all(feature = "alloc", feature = "chip-params"))]
mod param_mark;
#[cfg(all(feature = "alloc", any(feature = "blob", feature = "source")))]
mod queue_shape;
pub mod scalar_kind;
pub mod shape;
pub mod steps;

#[cfg(feature = "blob")]
pub mod blob;
#[cfg(feature = "chip")]
pub mod chip;
#[cfg(all(feature = "alloc", feature = "merge"))]
pub(crate) mod merge;
#[cfg(feature = "source")]
pub mod source;

#[cfg(feature = "alloc")]
pub use backend::{BACKEND_ID, CpuBackend, HOST_SPACE};
pub use daedalus_abi::{Fault, FixedError, StepCx as Ctx};
pub use steps::ports::{FanIn, FanItem};

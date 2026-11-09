//! The CPU backend. A `#[node]` function becomes a step: a wrapper that binds each of its cells
//! to a parameter, calls the function, and writes what it returned.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod backend;
pub mod steps;

#[cfg(feature = "blob")]
pub mod blob;
#[cfg(feature = "chip")]
pub mod chip;
#[cfg(feature = "merge")]
pub mod merge;
#[cfg(feature = "source")]
pub mod source;

pub use backend::{BACKEND_ID, CpuBackend, HOST_SPACE};
pub use daedalus_abi::{Fault, FixedError, StepCx as Ctx};
pub use steps::ports::{FanIn, FanItem};

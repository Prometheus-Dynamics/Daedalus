//! What `#[node]` expands to for a CPU body, and the types a node signature names.

#![no_std]
#![forbid(unsafe_code)]

mod body;

pub use daedalus_cpu::{Ctx, FanIn, FanItem, Fault, FixedError};

#[doc(hidden)]
pub use daedalus_cpu::steps as __cpu;
#[doc(hidden)]
pub use daedalus_registry as __registry;

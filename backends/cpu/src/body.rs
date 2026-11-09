//! A CPU body's payload in its record.

use daedalus_runner::ArenaBody;

#[cfg(feature = "alloc")]
use crate::steps::Setup;
#[cfg(feature = "alloc")]
use alloc::string::String;
#[cfg(feature = "alloc")]
use daedalus_abi::Step;

pub struct CpuBody {
    /// Builds the node's step for a plan run in this process.
    #[cfg(feature = "alloc")]
    pub make: fn(&Setup<'_>) -> Result<Step, String>,
    /// Runs the node in a fixed block of memory. `None` if a value or the state can't live there:
    /// ports have to be small `Copy` values, and the state must have nothing to drop.
    pub arena: Option<ArenaBody>,
}

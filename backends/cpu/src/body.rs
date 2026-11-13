//! A CPU body's payload in its record.

use daedalus_runner::ArenaBody;

#[cfg(feature = "alloc")]
use crate::shape::BodyShape;

#[cfg(feature = "alloc")]
use crate::steps::Setup;
#[cfg(feature = "alloc")]
use alloc::string::String;
#[cfg(feature = "alloc")]
use daedalus_abi::Step;

/// A body made at run time: a closure that builds the node's step. For a node a host declares
/// while it runs (`Registry::register_owned` with this as the body's code), where a `#[node]`
/// function can't be: the closure can hold whatever the host knew when it made the node.
///
/// It runs as a step in this process. It has no function to name, so it can't be merged with its
/// neighbours' code or written out as a blob or as source.
#[cfg(feature = "alloc")]
pub struct CpuStepBody {
    /// Builds the node's step for a plan run in this process.
    pub make: alloc::boxed::Box<MakeStep>,
}

/// What builds a run-time body's step.
#[cfg(feature = "alloc")]
pub type MakeStep = dyn Fn(&Setup<'_>) -> Result<Step, String> + Send + Sync;

#[cfg(feature = "alloc")]
impl CpuStepBody {
    pub fn new(
        make: impl Fn(&Setup<'_>) -> Result<Step, String> + Send + Sync + 'static,
    ) -> CpuStepBody {
        CpuStepBody {
            make: alloc::boxed::Box::new(make),
        }
    }
}

pub struct CpuBody {
    /// Builds the node's step for a plan run in this process.
    #[cfg(feature = "alloc")]
    pub make: fn(&Setup<'_>) -> Result<Step, String>,
    /// Runs the node in a fixed block of memory. `None` if a value or the state can't live there:
    /// ports have to be small `Copy` values, and the state must have nothing to drop.
    pub arena: Option<ArenaBody>,
    /// How the function is called, for code generated around it.
    #[cfg(feature = "alloc")]
    pub shape: BodyShape,
}

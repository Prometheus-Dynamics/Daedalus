use serde::{Deserialize, Serialize};

/// Compute affinity hint for scheduling/GPU pass.
///
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
pub enum ComputeAffinity {
    /// CPU only.
    #[default]
    CpuOnly,
    /// Prefer a GPU if available, otherwise run on CPU.
    GpuPreferred,
    /// Require a GPU; planning/runtime should fail if unavailable.
    GpuRequired,
}

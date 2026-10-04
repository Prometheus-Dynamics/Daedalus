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

impl ComputeAffinity {
    /// Every variant, in declaration order (the graph JSON Schema lists them from here).
    pub const ALL: [Self; 3] = [Self::CpuOnly, Self::GpuPreferred, Self::GpuRequired];
}

// A new variant fails to compile here until it is added to `ALL`.
const _: fn(ComputeAffinity) = |affinity| match affinity {
    ComputeAffinity::CpuOnly | ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired => {}
};

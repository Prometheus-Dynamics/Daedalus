//! Compute limits and optional shader features of a GPU backend, so a library can pick a kernel
//! variant (or decline its GPU implementation) from facts instead of trial dispatches.

use serde::{Deserialize, Serialize};

/// Compute limits and enabled shader features ([`crate::GpuCapabilities::compute`]).
///
/// Zero limits mean the backend cannot dispatch compute work (noop). On wgpu the limits are the
/// adapter's (the device requests them) and the features are those enabled on the device: the
/// backend requests every optional compute feature in [`GpuComputeFeatures`] the adapter has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuComputeCaps {
    /// Workgroup (shared) memory per workgroup, bytes.
    pub max_workgroup_storage_size: u32,
    pub max_invocations_per_workgroup: u32,
    /// Per-dimension workgroup size limit (x, y, z).
    pub max_workgroup_size: [u32; 3],
    pub max_workgroups_per_dimension: u32,
    /// Largest storage buffer binding, bytes.
    pub max_storage_buffer_binding_size: u64,
    pub max_storage_buffers_per_stage: u32,
    pub max_storage_textures_per_stage: u32,
    pub features: GpuComputeFeatures,
}

impl GpuComputeCaps {
    /// The WebGPU default limits, which every conforming device meets; no optional features.
    /// Kernels within these limits run unchanged on V3D, Mali and desktop GPUs.
    pub const WEBGPU_DEFAULTS: Self = Self {
        max_workgroup_storage_size: 16_384,
        max_invocations_per_workgroup: 256,
        max_workgroup_size: [256, 256, 64],
        max_workgroups_per_dimension: 65_535,
        max_storage_buffer_binding_size: 128 << 20,
        max_storage_buffers_per_stage: 8,
        max_storage_textures_per_stage: 4,
        features: GpuComputeFeatures::NONE,
    };

    /// Whether a `[x, y, z]` workgroup fits the size and invocation limits.
    pub fn fits_workgroup(&self, size: [u32; 3]) -> bool {
        size.iter()
            .zip(self.max_workgroup_size)
            .all(|(&n, max)| n >= 1 && n <= max)
            && size
                .iter()
                .try_fold(1u32, |acc, &n| acc.checked_mul(n))
                .is_some_and(|total| total <= self.max_invocations_per_workgroup)
    }
}

/// Optional shader features enabled on the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuComputeFeatures {
    /// `f16` in shaders (WGSL `enable f16;`).
    pub shader_f16: bool,
    pub shader_f64: bool,
    pub shader_int64: bool,
    /// 64-bit integer atomics (min/max at least).
    pub shader_int64_atomics: bool,
    /// Subgroup operations (WGSL `enable subgroups;`).
    pub subgroups: bool,
    /// Timestamp queries on command encoders.
    pub timestamps: bool,
    /// Timestamp queries inside compute and render passes.
    pub timestamps_inside_passes: bool,
}

impl GpuComputeFeatures {
    pub const NONE: Self = Self {
        shader_f16: false,
        shader_f64: false,
        shader_int64: false,
        shader_int64_atomics: false,
        subgroups: false,
        timestamps: false,
        timestamps_inside_passes: false,
    };
}

#[cfg(test)]
mod tests {
    use super::GpuComputeCaps;

    #[test]
    fn workgroup_fit_checks_each_dimension_and_the_product() {
        let caps = GpuComputeCaps::WEBGPU_DEFAULTS;
        assert!(caps.fits_workgroup([8, 8, 1]));
        assert!(caps.fits_workgroup([256, 1, 1]));
        assert!(!caps.fits_workgroup([16, 32, 1]), "512 invocations");
        assert!(!caps.fits_workgroup([1, 1, 65]), "z over 64");
        assert!(!caps.fits_workgroup([0, 1, 1]));
        assert!(!GpuComputeCaps::default().fits_workgroup([1, 1, 1]));
    }
}

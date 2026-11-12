//! What planning knows about a device without holding one.

use crate::adapter::AdapterInfo;
use crate::profile::DeviceProfile;

/// Compute limits. All zero means no compute at all.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ComputeCaps {
    /// Shared memory per workgroup, in bytes.
    pub max_workgroup_storage_size: u32,
    pub max_invocations_per_workgroup: u32,
    pub max_workgroup_size: [u32; 3],
    pub max_workgroups_per_dimension: u32,
    /// The largest storage buffer binding, in bytes.
    pub max_storage_buffer_binding_size: u64,
    pub max_storage_buffers_per_stage: u32,
    pub max_storage_textures_per_stage: u32,
    pub features: ComputeFeatures,
}

/// Optional shader features enabled on the device.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ComputeFeatures {
    pub shader_f16: bool,
    pub shader_f64: bool,
    pub shader_int64: bool,
    pub shader_int64_atomics: bool,
    pub subgroups: bool,
    pub timestamps: bool,
    pub timestamps_inside_passes: bool,
}

impl ComputeFeatures {
    pub const NONE: ComputeFeatures = ComputeFeatures {
        shader_f16: false,
        shader_f64: false,
        shader_int64: false,
        shader_int64_atomics: false,
        subgroups: false,
        timestamps: false,
        timestamps_inside_passes: false,
    };

    pub(crate) fn from_wgpu(enabled: wgpu::Features) -> ComputeFeatures {
        ComputeFeatures {
            shader_f16: enabled.contains(wgpu::Features::SHADER_F16),
            shader_f64: enabled.contains(wgpu::Features::SHADER_F64),
            shader_int64: enabled.contains(wgpu::Features::SHADER_INT64),
            shader_int64_atomics: enabled.contains(wgpu::Features::SHADER_INT64_ATOMIC_MIN_MAX),
            subgroups: enabled.contains(wgpu::Features::SUBGROUP),
            timestamps: enabled.contains(
                wgpu::Features::TIMESTAMP_QUERY | wgpu::Features::TIMESTAMP_QUERY_INSIDE_ENCODERS,
            ),
            timestamps_inside_passes: enabled
                .contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES),
        }
    }
}

/// The optional features a device is opened with, when its adapter has them.
pub(crate) const OPTIONAL_FEATURES: wgpu::Features = wgpu::Features::SHADER_F16
    .union(wgpu::Features::SHADER_F64)
    .union(wgpu::Features::SHADER_INT64)
    .union(wgpu::Features::SHADER_INT64_ATOMIC_MIN_MAX)
    .union(wgpu::Features::SUBGROUP)
    .union(wgpu::Features::TIMESTAMP_QUERY)
    .union(wgpu::Features::TIMESTAMP_QUERY_INSIDE_ENCODERS)
    .union(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES);

impl ComputeCaps {
    /// The WebGPU default limits, which every conforming device meets, with no optional feature.
    pub const WEBGPU_DEFAULTS: ComputeCaps = ComputeCaps {
        max_workgroup_storage_size: 16_384,
        max_invocations_per_workgroup: 256,
        max_workgroup_size: [256, 256, 64],
        max_workgroups_per_dimension: 65_535,
        max_storage_buffer_binding_size: 128 << 20,
        max_storage_buffers_per_stage: 8,
        max_storage_textures_per_stage: 4,
        features: ComputeFeatures::NONE,
    };

    pub(crate) fn from_wgpu(limits: &wgpu::Limits, enabled: wgpu::Features) -> ComputeCaps {
        ComputeCaps {
            max_workgroup_storage_size: limits.max_compute_workgroup_storage_size,
            max_invocations_per_workgroup: limits.max_compute_invocations_per_workgroup,
            max_workgroup_size: [
                limits.max_compute_workgroup_size_x,
                limits.max_compute_workgroup_size_y,
                limits.max_compute_workgroup_size_z,
            ],
            max_workgroups_per_dimension: limits.max_compute_workgroups_per_dimension,
            max_storage_buffer_binding_size: limits.max_storage_buffer_binding_size,
            max_storage_buffers_per_stage: limits.max_storage_buffers_per_shader_stage,
            max_storage_textures_per_stage: limits.max_storage_textures_per_shader_stage,
            features: ComputeFeatures::from_wgpu(enabled),
        }
    }

    /// Whether an `[x, y, z]` workgroup fits the size and invocation limits.
    pub fn fits_workgroup(&self, size: [u32; 3]) -> bool {
        size.iter()
            .zip(self.max_workgroup_size)
            .all(|(&n, max)| n >= 1 && n <= max)
            && size
                .iter()
                .try_fold(1u32, |total, &n| total.checked_mul(n))
                .is_some_and(|total| total <= self.max_invocations_per_workgroup)
    }

    /// Whether a device with these limits can give a body what it asked for.
    pub fn meets(&self, need: &Need) -> bool {
        let features = &self.features;
        match *need {
            Need::Workgroup(size) => self.fits_workgroup(size),
            Need::WorkgroupStorage(bytes) => bytes <= self.max_workgroup_storage_size,
            Need::StorageBuffers(count) => count <= self.max_storage_buffers_per_stage,
            Need::StorageTextures(count) => count <= self.max_storage_textures_per_stage,
            Need::StorageBinding(bytes) => bytes <= self.max_storage_buffer_binding_size,
            Need::F16 => features.shader_f16,
            Need::F64 => features.shader_f64,
            Need::Int64 => features.shader_int64,
            Need::Int64Atomics => features.shader_int64_atomics,
            Need::Subgroups => features.subgroups,
            Need::Timestamps => features.timestamps,
        }
    }
}

/// One thing a body asks of this backend, written in its record as text:
/// `workgroup=8x8x1`, `workgroup_storage=4096`, `storage_buffers=3`, `storage_textures=2`,
/// `storage_binding=1048576`, `f16`, `f64`, `int64`, `int64_atomics`, `subgroups`, `timestamps`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    Workgroup([u32; 3]),
    WorkgroupStorage(u32),
    StorageBuffers(u32),
    StorageTextures(u32),
    StorageBinding(u64),
    F16,
    F64,
    Int64,
    Int64Atomics,
    Subgroups,
    Timestamps,
}

impl Need {
    /// `None` for text this backend doesn't know. A body with such a need isn't taken.
    pub fn parse(text: &str) -> Option<Need> {
        let (name, value) = match text.split_once('=') {
            Some((name, value)) => (name.trim(), Some(value.trim())),
            None => (text.trim(), None),
        };
        Some(match (name, value) {
            ("workgroup", Some(value)) => {
                let mut size = [1u32; 3];
                let mut parts = value.split('x');
                for slot in &mut size {
                    match parts.next() {
                        Some(part) => *slot = part.trim().parse().ok()?,
                        None => break,
                    }
                }
                if parts.next().is_some() {
                    return None;
                }
                Need::Workgroup(size)
            }
            ("workgroup_storage", Some(value)) => Need::WorkgroupStorage(value.parse().ok()?),
            ("storage_buffers", Some(value)) => Need::StorageBuffers(value.parse().ok()?),
            ("storage_textures", Some(value)) => Need::StorageTextures(value.parse().ok()?),
            ("storage_binding", Some(value)) => Need::StorageBinding(value.parse().ok()?),
            ("f16", None) => Need::F16,
            ("f64", None) => Need::F64,
            ("int64", None) => Need::Int64,
            ("int64_atomics", None) => Need::Int64Atomics,
            ("subgroups", None) => Need::Subgroups,
            ("timestamps", None) => Need::Timestamps,
            _ => return None,
        })
    }
}

/// What one adapter can do, written the first time a device opens on it. This, and never the
/// device, is what planning reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CapsRecord {
    pub adapter: AdapterInfo,
    pub profile: DeviceProfile,
    pub compute: ComputeCaps,
    /// The device imports dma-bufs and the system dma-heap opens: a readback can go into heap
    /// memory. Set when the device opens; a record made before that says false.
    pub dma_heap: bool,
}

impl CapsRecord {
    pub(crate) fn new(
        info: &wgpu::AdapterInfo,
        limits: &wgpu::Limits,
        enabled: wgpu::Features,
    ) -> CapsRecord {
        let adapter = AdapterInfo::from_wgpu(info);
        CapsRecord {
            profile: DeviceProfile::classify(&adapter),
            compute: ComputeCaps::from_wgpu(limits, enabled),
            dma_heap: false,
            adapter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ComputeCaps, Need};

    #[test]
    fn workgroup_fit_checks_each_dimension_and_the_product() {
        let caps = ComputeCaps::WEBGPU_DEFAULTS;
        assert!(caps.fits_workgroup([8, 8, 1]));
        assert!(caps.fits_workgroup([256, 1, 1]));
        assert!(!caps.fits_workgroup([16, 32, 1]), "512 invocations");
        assert!(!caps.fits_workgroup([1, 1, 65]), "z over 64");
        assert!(!caps.fits_workgroup([0, 1, 1]));
        assert!(!ComputeCaps::default().fits_workgroup([1, 1, 1]));
    }

    #[test]
    fn needs_parse_and_are_met_or_not() {
        let caps = ComputeCaps::WEBGPU_DEFAULTS;
        let met = |text| Need::parse(text).map(|need| caps.meets(&need));
        assert_eq!(
            Need::parse("workgroup=8x8"),
            Some(Need::Workgroup([8, 8, 1]))
        );
        assert_eq!(met("workgroup=8x8x1"), Some(true));
        assert_eq!(met("workgroup=32x32"), Some(false));
        assert_eq!(met("storage_buffers=8"), Some(true));
        assert_eq!(met("storage_buffers=9"), Some(false));
        assert_eq!(met("storage_textures=5"), Some(false));
        assert_eq!(met("workgroup_storage=16384"), Some(true));
        assert_eq!(met("storage_binding=134217729"), Some(false));
        assert_eq!(met("f16"), Some(false), "no optional feature by default");
        for unknown in ["", "f16=1", "workgroup", "workgroup=1x2x3x4", "ray_query"] {
            assert_eq!(Need::parse(unknown), None, "{unknown}");
        }
    }
}

use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuBlockInfo, GpuCapabilities, GpuFormat, GpuFormatFeatures,
};

pub(super) fn caps_from_adapter(
    adapter: Option<&wgpu::Adapter>,
    limits: &wgpu::Limits,
) -> GpuCapabilities {
    let formats = [
        GpuFormat::R8Unorm,
        GpuFormat::Rgba8Unorm,
        GpuFormat::Rgba16Float,
        GpuFormat::Depth24Stencil8,
        GpuFormat::Rg8Unorm,
        GpuFormat::Bgra8Unorm,
    ];
    let mut format_features = Vec::new();
    for format in formats {
        let (sampleable, renderable, storage, max_samples) = if let Some(adapter) = adapter {
            let features = adapter.get_texture_format_features(super::map_format(format));
            let allowed = features.allowed_usages;
            let flags = features.flags;
            let max_samples = if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X8) {
                8
            } else if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X4) {
                4
            } else if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X2) {
                2
            } else {
                1
            };
            (
                allowed.contains(wgpu::TextureUsages::TEXTURE_BINDING),
                allowed.contains(wgpu::TextureUsages::RENDER_ATTACHMENT),
                allowed.contains(wgpu::TextureUsages::STORAGE_BINDING),
                max_samples,
            )
        } else {
            (true, true, format != GpuFormat::Depth24Stencil8, 8)
        };
        format_features.push(GpuFormatFeatures {
            format,
            sampleable,
            renderable,
            storage,
            max_samples,
        });
    }

    GpuCapabilities {
        supported_formats: formats.to_vec(),
        format_features,
        format_blocks: GpuBlockInfo::for_formats(&formats),
        max_buffer_size: limits.max_buffer_size,
        max_texture_dimension: limits.max_texture_dimension_2d,
        max_texture_samples: limits.max_texture_dimension_2d.min(8),
        staging_alignment: limits.min_storage_buffer_offset_alignment as u64,
        max_inflight_copies: 8,
        queue_count: 1,
        min_buffer_copy_offset_alignment: wgpu::COPY_BUFFER_ALIGNMENT,
        bytes_per_row_alignment: wgpu::COPY_BYTES_PER_ROW_ALIGNMENT,
        rows_per_image_alignment: 1,
        // wgpu uses a unified queue that supports transfer+compute.
        has_transfer_queue: true,
        compute: crate::GpuComputeCaps {
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
            features: crate::GpuComputeFeatures::NONE,
        },
    }
}

pub(super) fn build_info_from_adapter(
    adapter: &wgpu::Adapter,
) -> (GpuAdapterInfo, wgpu::Features, wgpu::Limits) {
    let info = adapter.get_info();
    let features = adapter.features();
    let limits = adapter.limits();
    let name = format!("{} ({:?})", info.name, info.backend);
    (
        GpuAdapterInfo {
            name,
            backend: GpuBackendKind::Wgpu,
            device_id: Some(format!("{:x}", info.device)),
            vendor_id: Some(info.vendor.to_string()),
        },
        features,
        limits,
    )
}

/// Optional compute features the backend enables when the adapter has them.
pub(super) const OPTIONAL_COMPUTE_FEATURES: wgpu::Features = wgpu::Features::SHADER_F16
    .union(wgpu::Features::SHADER_F64)
    .union(wgpu::Features::SHADER_INT64)
    .union(wgpu::Features::SHADER_INT64_ATOMIC_MIN_MAX)
    .union(wgpu::Features::SUBGROUP)
    .union(wgpu::Features::TIMESTAMP_QUERY)
    .union(wgpu::Features::TIMESTAMP_QUERY_INSIDE_ENCODERS)
    .union(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES);

/// The [`crate::GpuComputeFeatures`] enabled on a device.
pub(super) fn compute_features(enabled: wgpu::Features) -> crate::GpuComputeFeatures {
    crate::GpuComputeFeatures {
        shader_f16: enabled.contains(wgpu::Features::SHADER_F16),
        shader_f64: enabled.contains(wgpu::Features::SHADER_F64),
        shader_int64: enabled.contains(wgpu::Features::SHADER_INT64),
        shader_int64_atomics: enabled.contains(wgpu::Features::SHADER_INT64_ATOMIC_MIN_MAX),
        subgroups: enabled.contains(wgpu::Features::SUBGROUP),
        timestamps: enabled.contains(wgpu::Features::TIMESTAMP_QUERY)
            && enabled.contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_ENCODERS),
        timestamps_inside_passes: enabled.contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES),
    }
}

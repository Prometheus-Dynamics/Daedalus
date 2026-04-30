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
    ];
    let mut format_features = Vec::new();
    for format in formats {
        let (sampleable, renderable, storage, max_samples) = if let Some(adapter) = adapter {
            let tf = match format {
                GpuFormat::R8Unorm => wgpu::TextureFormat::R8Unorm,
                GpuFormat::Rgba8Unorm => wgpu::TextureFormat::Rgba8Unorm,
                GpuFormat::Rgba16Float => wgpu::TextureFormat::Rgba16Float,
                GpuFormat::Depth24Stencil8 => wgpu::TextureFormat::Depth24PlusStencil8,
            };
            let features = adapter.get_texture_format_features(tf);
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
        format_blocks: vec![
            GpuBlockInfo {
                format: GpuFormat::R8Unorm,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 1,
            },
            GpuBlockInfo {
                format: GpuFormat::Rgba8Unorm,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 4,
            },
            GpuBlockInfo {
                format: GpuFormat::Rgba16Float,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 8,
            },
            GpuBlockInfo {
                format: GpuFormat::Depth24Stencil8,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 4,
            },
        ],
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

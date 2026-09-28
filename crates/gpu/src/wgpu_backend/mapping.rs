//! Translation between the backend-neutral [`GpuFormat`]/[`GpuUsage`] and wgpu types.

use crate::{GpuFormat, GpuUsage};

/// wgpu format of a [`GpuFormat`].
pub(crate) fn map_format(format: GpuFormat) -> wgpu::TextureFormat {
    match format {
        GpuFormat::R8Unorm => wgpu::TextureFormat::R8Unorm,
        GpuFormat::Rgba8Unorm => wgpu::TextureFormat::Rgba8Unorm,
        GpuFormat::Rgba16Float => wgpu::TextureFormat::Rgba16Float,
        GpuFormat::Depth24Stencil8 => wgpu::TextureFormat::Depth24PlusStencil8,
        GpuFormat::Rg8Unorm => wgpu::TextureFormat::Rg8Unorm,
        GpuFormat::Bgra8Unorm => wgpu::TextureFormat::Bgra8Unorm,
        GpuFormat::Nv12 => wgpu::TextureFormat::NV12,
    }
}

/// Inverse of [`map_format`]; `None` for wgpu formats with no [`GpuFormat`] equivalent.
pub(crate) fn gpu_format_from_wgpu(format: wgpu::TextureFormat) -> Option<GpuFormat> {
    GpuFormat::ALL
        .into_iter()
        .find(|&candidate| map_format(candidate) == format)
}

/// One-to-one texture usage correspondence shared by both mapping directions.
const TEXTURE_USAGES: [(GpuUsage, wgpu::TextureUsages); 4] = [
    (
        GpuUsage::RENDER_TARGET,
        wgpu::TextureUsages::RENDER_ATTACHMENT,
    ),
    (GpuUsage::UPLOAD, wgpu::TextureUsages::COPY_DST),
    (GpuUsage::DOWNLOAD, wgpu::TextureUsages::COPY_SRC),
    (GpuUsage::STORAGE, wgpu::TextureUsages::STORAGE_BINDING),
];

/// The wgpu flag of each [`GpuUsage`] flag in `usage`, with nothing implied.
pub(crate) fn texture_usage_flags(usage: GpuUsage) -> wgpu::TextureUsages {
    TEXTURE_USAGES
        .iter()
        .filter(|(gpu, _)| usage.contains(*gpu))
        .fold(wgpu::TextureUsages::empty(), |acc, (_, w)| acc | *w)
}

/// Usages for allocating a texture: storage textures are also copy sources and targets.
pub(super) fn map_texture_usage(usage: GpuUsage) -> wgpu::TextureUsages {
    let mut u = texture_usage_flags(usage);
    if usage.contains(GpuUsage::STORAGE) {
        u |= wgpu::TextureUsages::COPY_SRC | wgpu::TextureUsages::COPY_DST;
    }
    u
}

/// The [`GpuUsage`] a texture created with `usage` supports.
pub(super) fn gpu_usage_from_wgpu(usage: wgpu::TextureUsages) -> GpuUsage {
    TEXTURE_USAGES
        .iter()
        .filter(|(_, w)| usage.contains(*w))
        .fold(GpuUsage::empty(), |acc, (gpu, _)| acc | *gpu)
}

pub(super) fn map_usage(usage: GpuUsage) -> wgpu::BufferUsages {
    let mut u = wgpu::BufferUsages::empty();
    if usage.contains(GpuUsage::UPLOAD) {
        u |= wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST;
    }
    if usage.contains(GpuUsage::DOWNLOAD) {
        u |= wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::MAP_READ;
    }
    if usage.contains(GpuUsage::STORAGE) {
        u |= wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST;
    }
    if u.is_empty() {
        u = wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST;
    }
    u
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_gpu_format_round_trips_through_wgpu() {
        for format in GpuFormat::ALL {
            assert_eq!(gpu_format_from_wgpu(map_format(format)), Some(format));
        }
    }

    #[test]
    fn unknown_wgpu_formats_have_no_gpu_format() {
        assert_eq!(gpu_format_from_wgpu(wgpu::TextureFormat::R32Float), None);
        assert_eq!(
            gpu_format_from_wgpu(wgpu::TextureFormat::Rgba8UnormSrgb),
            None
        );
    }

    #[test]
    fn texture_usage_round_trips() {
        for bits in 0..=GpuUsage::all().bits() {
            let usage = GpuUsage::from_bits_truncate(bits);
            assert_eq!(gpu_usage_from_wgpu(texture_usage_flags(usage)), usage);
        }
        assert_eq!(
            gpu_usage_from_wgpu(map_texture_usage(GpuUsage::STORAGE)),
            GpuUsage::STORAGE | GpuUsage::UPLOAD | GpuUsage::DOWNLOAD
        );
    }
}

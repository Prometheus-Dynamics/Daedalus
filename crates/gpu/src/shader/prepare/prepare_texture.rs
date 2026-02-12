use std::sync::Arc;

use crate::traits::GpuBackend;
use crate::{GpuContextHandle, GpuError};

use super::{Access, BindingData, BindingSpec, Prepared, ShaderBinding, temp_pool};

fn should_register_texture_handle(binding: &ShaderBinding, layout: &BindingSpec) -> bool {
    if binding.readback {
        return false;
    }
    matches!(layout.access, Access::WriteOnly | Access::ReadWrite)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_texture_binding(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    backend: Option<&dyn GpuBackend>,
    binding: &ShaderBinding,
    layout: &BindingSpec,
    gpu_ctx: Option<&GpuContextHandle>,
    device_key: usize,
    is_storage_tex: bool,
) -> Result<Prepared, GpuError> {
    match &binding.data {
        BindingData::TextureRgba8 {
            width,
            height,
            bytes,
        } => {
            let size = wgpu::Extent3d {
                width: *width,
                height: *height,
                depth_or_array_layers: 1,
            };
            let mut usage = if binding.readback {
                wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::COPY_DST
                    | wgpu::TextureUsages::COPY_SRC
            } else {
                wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST
            };
            if is_storage_tex {
                usage |= wgpu::TextureUsages::STORAGE_BINDING;
            }
            let texture = if let Ok(mut p) = temp_pool().lock() {
                p.take_texture(
                    device_key,
                    size.width,
                    size.height,
                    wgpu::TextureFormat::Rgba8Unorm,
                    usage,
                )
                .unwrap_or_else(|| {
                    Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                        label: Some("texture-binding"),
                        size,
                        mip_level_count: 1,
                        sample_count: 1,
                        dimension: wgpu::TextureDimension::D2,
                        format: wgpu::TextureFormat::Rgba8Unorm,
                        usage,
                        view_formats: &[],
                    }))
                })
            } else {
                Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                    label: Some("texture-binding"),
                    size,
                    mip_level_count: 1,
                    sample_count: 1,
                    dimension: wgpu::TextureDimension::D2,
                    format: wgpu::TextureFormat::Rgba8Unorm,
                    usage,
                    view_formats: &[],
                }))
            };
            let tex_handle = if should_register_texture_handle(binding, layout) {
                backend.and_then(|b| {
                    b.wgpu_register_texture(
                        texture.clone(),
                        wgpu::TextureFormat::Rgba8Unorm,
                        *width,
                        *height,
                        usage,
                    )
                })
            } else {
                None
            };
            let bytes_per_row = (*width as usize) * 4;
            let align = wgpu::COPY_BYTES_PER_ROW_ALIGNMENT as usize;
            let padded_bpr = bytes_per_row.div_ceil(align) * align;
            let mut padded = vec![0u8; padded_bpr * (*height as usize)];
            for row in 0..*height as usize {
                let src_start = row * bytes_per_row;
                let dst_start = row * padded_bpr;
                padded[dst_start..dst_start + bytes_per_row]
                    .copy_from_slice(&bytes[src_start..src_start + bytes_per_row]);
            }
            queue.write_texture(
                wgpu::TexelCopyTextureInfo {
                    texture: texture.as_ref(),
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &padded,
                wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded_bpr as u32),
                    rows_per_image: Some(*height),
                },
                size,
            );
            let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
            Ok(Prepared::Texture {
                spec: *layout,
                texture,
                view,
                width: *width,
                height: *height,
                usage,
                readback: binding.readback,
                owned: true,
                handle: tex_handle,
            })
        }
        BindingData::TextureAlloc { width, height } => {
            let size = wgpu::Extent3d {
                width: *width,
                height: *height,
                depth_or_array_layers: 1,
            };
            let format = layout
                .texture_format
                .unwrap_or(wgpu::TextureFormat::Rgba8Unorm);
            // Storage textures are commonly used as "outputs". Even when the caller doesn't request
            // explicit readback, downstream plumbing may still copy them (e.g. for debug/export or
            // temporary pooling), so include `COPY_SRC` unconditionally to avoid wgpu validation
            // errors on backends that treat this strictly.
            let usage = wgpu::TextureUsages::STORAGE_BINDING
                | wgpu::TextureUsages::COPY_SRC
                | wgpu::TextureUsages::COPY_DST
                | wgpu::TextureUsages::TEXTURE_BINDING;
            let texture = if let Ok(mut p) = temp_pool().lock() {
                p.take_texture(device_key, size.width, size.height, format, usage)
                    .unwrap_or_else(|| {
                        Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                            label: Some("storage-texture"),
                            size,
                            mip_level_count: 1,
                            sample_count: 1,
                            dimension: wgpu::TextureDimension::D2,
                            format,
                            usage,
                            view_formats: &[],
                        }))
                    })
            } else {
                Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                    label: Some("storage-texture"),
                    size,
                    mip_level_count: 1,
                    sample_count: 1,
                    dimension: wgpu::TextureDimension::D2,
                    format,
                    usage,
                    view_formats: &[],
                }))
            };
            let tex_handle = if should_register_texture_handle(binding, layout) {
                backend.and_then(|b| {
                    b.wgpu_register_texture(texture.clone(), format, *width, *height, usage)
                })
            } else {
                None
            };
            let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
            Ok(Prepared::Texture {
                spec: *layout,
                texture,
                view,
                width: *width,
                height: *height,
                usage,
                readback: binding.readback,
                owned: true,
                handle: tex_handle,
            })
        }
        BindingData::TextureHandle { handle } => {
            if let Some(backend) = backend
                && let Some(tex) = backend.wgpu_get_texture(handle)
            {
                let view = tex.create_view(&wgpu::TextureViewDescriptor::default());
                return Ok(Prepared::Texture {
                    spec: *layout,
                    texture: tex,
                    view,
                    width: handle.width,
                    height: handle.height,
                    usage: wgpu::TextureUsages::empty(),
                    readback: binding.readback,
                    owned: false,
                    handle: Some(handle.clone()),
                });
            }
            let ctx = gpu_ctx.ok_or(GpuError::Unsupported)?;
            let bytes = ctx.read_texture(handle)?;
            let size = wgpu::Extent3d {
                width: handle.width,
                height: handle.height,
                depth_or_array_layers: 1,
            };
            let mut usage = if binding.readback {
                wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::COPY_DST
                    | wgpu::TextureUsages::COPY_SRC
            } else {
                wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST
            };
            if is_storage_tex {
                usage |= wgpu::TextureUsages::STORAGE_BINDING;
            }
            let texture = if let Ok(mut p) = temp_pool().lock() {
                p.take_texture(
                    device_key,
                    size.width,
                    size.height,
                    wgpu::TextureFormat::Rgba8Unorm,
                    usage,
                )
                .unwrap_or_else(|| {
                    Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                        label: Some("texture-binding"),
                        size,
                        mip_level_count: 1,
                        sample_count: 1,
                        dimension: wgpu::TextureDimension::D2,
                        format: wgpu::TextureFormat::Rgba8Unorm,
                        usage,
                        view_formats: &[],
                    }))
                })
            } else {
                Arc::new(device.create_texture(&wgpu::TextureDescriptor {
                    label: Some("texture-binding"),
                    size,
                    mip_level_count: 1,
                    sample_count: 1,
                    dimension: wgpu::TextureDimension::D2,
                    format: wgpu::TextureFormat::Rgba8Unorm,
                    usage,
                    view_formats: &[],
                }))
            };
            let tex_handle = if should_register_texture_handle(binding, layout) {
                backend.and_then(|b| {
                    b.wgpu_register_texture(
                        texture.clone(),
                        wgpu::TextureFormat::Rgba8Unorm,
                        handle.width,
                        handle.height,
                        usage,
                    )
                })
            } else {
                None
            };
            let bytes_per_row = (handle.width as usize) * 4;
            let align = wgpu::COPY_BYTES_PER_ROW_ALIGNMENT as usize;
            let padded_bpr = bytes_per_row.div_ceil(align) * align;
            let mut padded = vec![0u8; padded_bpr * (handle.height as usize)];
            for row in 0..handle.height as usize {
                let src_start = row * bytes_per_row;
                let dst_start = row * padded_bpr;
                padded[dst_start..dst_start + bytes_per_row]
                    .copy_from_slice(&bytes[src_start..src_start + bytes_per_row]);
            }
            queue.write_texture(
                wgpu::TexelCopyTextureInfo {
                    texture: texture.as_ref(),
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &padded,
                wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded_bpr as u32),
                    rows_per_image: Some(handle.height),
                },
                size,
            );
            let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
            Ok(Prepared::Texture {
                spec: *layout,
                texture,
                view,
                width: handle.width,
                height: handle.height,
                usage,
                readback: binding.readback,
                owned: true,
                handle: tex_handle,
            })
        }
        _ => Err(GpuError::Internal("invalid texture binding kind".into())),
    }
}

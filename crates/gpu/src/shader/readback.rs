use std::collections::HashMap;
use std::sync::Arc;

use crate::{GpuError, GpuImageHandle};

use super::prepare::Prepared;
use super::temp_pool;

fn texture_bytes_per_pixel(format: Option<wgpu::TextureFormat>) -> usize {
    // Use wgpu's format footprint to avoid manual byte counts; block_copy_size covers uncompressed formats we support.
    format
        .unwrap_or(wgpu::TextureFormat::Rgba8Unorm)
        .block_copy_size(None)
        .unwrap_or(4) as usize
}

pub(crate) struct ReadbackRequest {
    pub binding: u32,
    pub buffer: wgpu::Buffer,
    pub size: u64,
    pub is_texture: bool,
    pub height: u32,
    pub row_bytes: usize,
    pub padded_bpr: usize,
}

pub(crate) struct PoolReturn {
    pub device_key: usize,
    pub width: u32,
    pub height: u32,
    pub format: wgpu::TextureFormat,
    pub usage: wgpu::TextureUsages,
    pub texture: Arc<wgpu::Texture>,
}

pub(crate) fn enqueue_readbacks(
    device: &wgpu::Device,
    prepared: &[Prepared],
    encoder: &mut wgpu::CommandEncoder,
) -> (
    Vec<ReadbackRequest>,
    Vec<PoolReturn>,
    HashMap<u32, GpuImageHandle>,
) {
    let mut readbacks: Vec<ReadbackRequest> = Vec::new();
    let mut pool_textures_to_return: Vec<PoolReturn> = Vec::new();
    let mut texture_handles: HashMap<u32, GpuImageHandle> = HashMap::new();
    let device_key = device as *const _ as usize;

    for p in prepared {
        match p {
            Prepared::Buffer {
                spec,
                buffer,
                size,
                readback,
            } if *readback => {
                let staging = if let Some(buf) = temp_pool().lock().ok().and_then(|mut p| {
                    p.take_buffer(
                        device_key,
                        *size,
                        wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                    )
                }) {
                    buf
                } else {
                    device.create_buffer(&wgpu::BufferDescriptor {
                        label: Some("readback"),
                        size: *size,
                        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                        mapped_at_creation: false,
                    })
                };
                encoder.copy_buffer_to_buffer(buffer, 0, &staging, 0, *size);
                readbacks.push(ReadbackRequest {
                    binding: spec.binding,
                    buffer: staging,
                    size: *size,
                    is_texture: false,
                    height: 0,
                    row_bytes: 0,
                    padded_bpr: 0,
                });
            }
            Prepared::Texture {
                spec,
                texture,
                width,
                height,
                usage,
                readback,
                owned,
                handle,
                ..
            } if *readback => {
                let bpp = texture_bytes_per_pixel(spec.texture_format);
                let bytes_per_row = (*width as usize) * bpp;
                let align = wgpu::COPY_BYTES_PER_ROW_ALIGNMENT as usize;
                let padded_bpr = bytes_per_row.div_ceil(align) * align;
                let size_bytes = (padded_bpr * (*height as usize)) as u64;
                let staging = if let Ok(mut p) = temp_pool().lock() {
                    if let Some(buf) = p.take_buffer(
                        device_key,
                        size_bytes,
                        wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                    ) {
                        buf
                    } else {
                        device.create_buffer(&wgpu::BufferDescriptor {
                            label: Some("tex-readback"),
                            size: size_bytes,
                            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                            mapped_at_creation: false,
                        })
                    }
                } else {
                    device.create_buffer(&wgpu::BufferDescriptor {
                        label: Some("tex-readback"),
                        size: size_bytes,
                        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                        mapped_at_creation: false,
                    })
                };
                encoder.copy_texture_to_buffer(
                    texture.as_image_copy(),
                    wgpu::TexelCopyBufferInfo {
                        buffer: &staging,
                        layout: wgpu::TexelCopyBufferLayout {
                            offset: 0,
                            bytes_per_row: Some(padded_bpr as u32),
                            rows_per_image: Some(*height),
                        },
                    },
                    wgpu::Extent3d {
                        width: *width,
                        height: *height,
                        depth_or_array_layers: 1,
                    },
                );
                readbacks.push(ReadbackRequest {
                    binding: spec.binding,
                    buffer: staging,
                    size: size_bytes,
                    is_texture: true,
                    height: *height,
                    row_bytes: bytes_per_row,
                    padded_bpr,
                });
                // Textures that are exported via a `GpuImageHandle` must not be returned to the
                // temp pool; doing so allows the pool to hand them out again while still bound,
                // triggering wgpu validation errors (and sometimes use-after-free patterns).
                if *owned && handle.is_none() {
                    let fmt = spec
                        .texture_format
                        .unwrap_or(wgpu::TextureFormat::Rgba8Unorm);
                    pool_textures_to_return.push(PoolReturn {
                        device_key,
                        width: *width,
                        height: *height,
                        format: fmt,
                        usage: *usage,
                        texture: texture.clone(),
                    });
                }
                if let Some(h) = handle {
                    texture_handles.insert(spec.binding, h.clone());
                }
            }
            Prepared::Texture {
                spec,
                texture,
                width,
                height,
                usage,
                owned,
                handle,
                ..
            } => {
                if let Some(h) = handle {
                    texture_handles.insert(spec.binding, h.clone());
                }
                if *owned && handle.is_none() {
                    let fmt = spec
                        .texture_format
                        .unwrap_or(wgpu::TextureFormat::Rgba8Unorm);
                    pool_textures_to_return.push(PoolReturn {
                        device_key,
                        width: *width,
                        height: *height,
                        format: fmt,
                        usage: *usage,
                        texture: texture.clone(),
                    });
                }
            }
            _ => {}
        }
    }

    (readbacks, pool_textures_to_return, texture_handles)
}

pub(crate) fn resolve_readbacks(
    device: &wgpu::Device,
    readbacks: Vec<ReadbackRequest>,
) -> Result<HashMap<u32, Vec<u8>>, GpuError> {
    let device_key = device as *const _ as usize;
    let mut result = HashMap::new();
    for ReadbackRequest {
        binding,
        buffer,
        size,
        is_texture,
        height,
        row_bytes,
        padded_bpr,
    } in readbacks
    {
        let slice = buffer.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |res| {
            let _ = tx.send(res);
        });
        // Block until the GPU completes the map; this is still synchronous but avoids extra copies.
        let _ = device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .map_err(|e| GpuError::Internal(format!("map canceled: {e}")))?
            .map_err(|e| GpuError::Internal(format!("map failed: {e:?}")))?;

        {
            let data = slice.get_mapped_range();
            if is_texture {
                let mut trimmed = Vec::with_capacity(row_bytes * height as usize);
                for row in 0..height as usize {
                    let start = row * padded_bpr;
                    trimmed.extend_from_slice(&data[start..start + row_bytes]);
                }
                result.insert(binding, trimmed);
            } else {
                let len = size.min(data.len() as u64) as usize;
                let mut out = Vec::with_capacity(len);
                out.extend_from_slice(&data[..len]);
                result.insert(binding, out);
            }
        }

        buffer.unmap();
        if let Ok(mut p) = temp_pool().lock() {
            p.put_buffer(device_key, size, buffer);
        }
    }
    Ok(result)
}

pub(crate) fn return_pooled_textures(pool_textures_to_return: Vec<PoolReturn>) {
    if let Ok(mut p) = temp_pool().lock() {
        for PoolReturn {
            device_key,
            width,
            height,
            format,
            usage,
            texture,
        } in pool_textures_to_return
        {
            p.put_texture(device_key, width, height, format, usage, texture);
        }
    }
}

#[cfg(feature = "gpu-async")]
pub(crate) use crate::shader::readback_async::resolve_readbacks_async;

use std::collections::HashMap;
use std::sync::Arc;

use crate::traits::GpuBackend;
use crate::{GpuContextHandle, GpuError, GpuImageHandle};
use wgpu::util::DeviceExt;

use super::{Access, BindingData, BindingKind, BindingSpec, BufferInit, ShaderBinding, temp_pool};
mod prepare_texture;
use prepare_texture::{TexturePrepareContext, prepare_texture_binding};

pub(crate) enum Prepared {
    Buffer {
        spec: BindingSpec,
        buffer: wgpu::Buffer,
        size: u64,
        readback: bool,
    },
    Texture {
        spec: BindingSpec,
        texture: Arc<wgpu::Texture>,
        view: wgpu::TextureView,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
        readback: bool,
        owned: bool,
        handle: Option<GpuImageHandle>,
    },
    Sampler {
        spec: BindingSpec,
        sampler: wgpu::Sampler,
    },
}

pub(crate) fn prepare_resources(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    backend: Option<&dyn GpuBackend>,
    bindings: &[ShaderBinding],
    layout_bindings: &[BindingSpec],
    gpu_ctx: Option<&GpuContextHandle>,
) -> Result<Vec<Prepared>, GpuError> {
    let mut binding_map: HashMap<u32, BindingSpec> = HashMap::new();
    for b in layout_bindings {
        binding_map.insert(b.binding, *b);
    }
    let device_key = super::device_key(device);

    let mut prepared: Vec<Prepared> = Vec::new();
    for binding in bindings {
        let Some(layout) = binding_map.get(&binding.binding) else {
            return Err(GpuError::Internal(format!(
                "binding {} not declared in shader layout",
                binding.binding
            )));
        };
        if layout.kind != binding.kind || layout.access != binding.access {
            return Err(GpuError::Internal(format!(
                "binding {} kind/access mismatch (layout {:?}/{:?}, provided {:?}/{:?})",
                binding.binding, layout.kind, layout.access, binding.kind, binding.access
            )));
        }
        if matches!(layout.kind, BindingKind::StorageTexture2D) && layout.texture_format.is_none() {
            return Err(GpuError::Internal(format!(
                "binding {} storage texture requires texture_format",
                binding.binding
            )));
        }
        let is_storage_tex = matches!(layout.kind, BindingKind::StorageTexture2D);

        if prepared.iter().any(|p| match p {
            Prepared::Buffer { spec, .. }
            | Prepared::Texture { spec, .. }
            | Prepared::Sampler { spec, .. } => spec.binding == binding.binding,
        }) {
            return Err(GpuError::Internal(format!(
                "binding {} provided multiple times",
                binding.binding
            )));
        }

        match &binding.data {
            BindingData::Buffer(contents) => {
                let usage_base = match binding.kind {
                    BindingKind::Storage => wgpu::BufferUsages::STORAGE,
                    BindingKind::Uniform => wgpu::BufferUsages::UNIFORM,
                    _ => {
                        return Err(GpuError::Internal("invalid buffer kind".into()));
                    }
                };

                let (buffer, size_bytes) = match contents {
                    BufferInit::Bytes(bytes) => {
                        let mut usage = usage_base | wgpu::BufferUsages::COPY_DST;
                        if binding.readback
                            || matches!(binding.access, Access::WriteOnly | Access::ReadWrite)
                        {
                            usage |= wgpu::BufferUsages::COPY_SRC;
                        }
                        let buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
                            label: Some("binding"),
                            contents: bytes,
                            usage,
                        });
                        (buf, bytes.len() as u64)
                    }
                    BufferInit::Empty(size) | BufferInit::Zeroed(size) => {
                        let mut usage = usage_base;
                        if matches!(binding.access, Access::WriteOnly | Access::ReadWrite)
                            || binding.readback
                        {
                            usage |= wgpu::BufferUsages::COPY_SRC;
                        }
                        if matches!(binding.access, Access::ReadOnly) {
                            usage |= wgpu::BufferUsages::COPY_DST;
                        }
                        let buf = device.create_buffer(&wgpu::BufferDescriptor {
                            label: Some("binding"),
                            size: *size,
                            usage,
                            mapped_at_creation: true,
                        });
                        {
                            // Match the documented behavior for Empty/Zeroed: start zeroed to avoid
                            // undefined contents reaching the shader.
                            let mut view = buf.slice(..).get_mapped_range_mut();
                            view.copy_from_slice(&vec![0; view.len()]);
                        }
                        buf.unmap();
                        (buf, *size)
                    }
                };
                prepared.push(Prepared::Buffer {
                    spec: *layout,
                    buffer,
                    size: size_bytes,
                    readback: binding.readback,
                });
            }
            BindingData::BufferDevice {
                buffer,
                size,
                device_key: buf_device_key,
            } => {
                if *buf_device_key != device_key {
                    return Err(GpuError::Internal(
                        "buffer provided from different device than dispatch".into(),
                    ));
                }
                prepared.push(Prepared::Buffer {
                    spec: *layout,
                    buffer: buffer.as_ref().clone(),
                    size: *size,
                    readback: binding.readback,
                });
            }
            BindingData::TextureRgba8 { .. }
            | BindingData::TextureAlloc { .. }
            | BindingData::TextureHandle { .. } => {
                prepared.push(prepare_texture_binding(
                    TexturePrepareContext {
                        device,
                        queue,
                        backend,
                        gpu_ctx,
                        device_key,
                        is_storage_tex,
                    },
                    binding,
                    layout,
                )?);
            }
            BindingData::Sampler(desc) => {
                let sampler = device.create_sampler(&wgpu::SamplerDescriptor {
                    address_mode_u: desc.address_u,
                    address_mode_v: desc.address_v,
                    address_mode_w: desc.address_w,
                    mag_filter: desc.mag_filter,
                    min_filter: desc.min_filter,
                    mipmap_filter: desc.mipmap_filter,
                    ..wgpu::SamplerDescriptor::default()
                });
                prepared.push(Prepared::Sampler {
                    spec: *layout,
                    sampler,
                });
            }
        }
    }

    // Ensure every declared layout binding has data.
    for layout in layout_bindings {
        if !prepared.iter().any(|p| match p {
            Prepared::Buffer { spec, .. }
            | Prepared::Texture { spec, .. }
            | Prepared::Sampler { spec, .. } => spec.binding == layout.binding,
        }) {
            return Err(GpuError::Internal(format!(
                "missing data for binding {}",
                layout.binding
            )));
        }
    }

    Ok(prepared)
}

#[cfg(feature = "gpu-async")]
pub(crate) async fn prepare_resources_async<'a>(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    backend: Option<&dyn GpuBackend>,
    bindings: &'a [ShaderBinding<'a>],
    layout_bindings: &[BindingSpec],
    gpu_ctx: Option<&GpuContextHandle>,
) -> Result<Vec<Prepared>, GpuError> {
    let mut materialized = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let mut binding = binding.clone();
        if let BindingData::TextureHandle { handle } = &binding.data {
            let can_use_native_texture = backend
                .and_then(|backend| backend.wgpu_get_texture(handle))
                .is_some();
            if !can_use_native_texture {
                let gpu_ctx = gpu_ctx.ok_or(GpuError::Unsupported)?;
                tracing::debug!(
                    target: "daedalus_gpu::prepare",
                    texture_id = %handle.id,
                    width = handle.width,
                    height = handle.height,
                    "materializing texture handle for async shader binding"
                );
                let bytes = gpu_ctx.read_texture_async(handle.clone()).await?;
                binding.data = BindingData::TextureRgba8 {
                    width: handle.width,
                    height: handle.height,
                    bytes: std::borrow::Cow::Owned(bytes),
                };
            }
        }
        materialized.push(binding);
    }

    prepare_resources(
        device,
        queue,
        backend,
        &materialized,
        layout_bindings,
        gpu_ctx,
    )
}

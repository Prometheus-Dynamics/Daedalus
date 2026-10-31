//! Node-owned persistent storage buffers.

use std::sync::Arc;

use crate::{GpuContextHandle, GpuError};

use super::{Access, BindingData, BindingKind, ShaderBinding};

/// A sized storage buffer a node keeps on the engine's device for its lifetime: kept in the
/// node's `state(T)` across ticks (a TSDF volume, cost-volume scratch). Dropping it (on the
/// node's `Restart`, or when a reload drops the node's state) frees the buffer.
///
/// Created zeroed. Bind it to Daedalus dispatches with [`GpuStorage::binding`], or to the
/// node's own wgpu pipelines with [`GpuStorage::buffer`].
pub struct GpuStorage {
    buffer: Arc<wgpu::Buffer>,
    size: u64,
    label: Option<String>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    device_key: usize,
}

impl std::fmt::Debug for GpuStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuStorage")
            .field("size", &self.size)
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl GpuStorage {
    /// A zeroed buffer of `size` bytes on `gpu`'s device. `Unsupported` on a non-wgpu backend;
    /// `InvalidInput` when `size` is 0, not a multiple of 4, or over the device's storage
    /// binding limit.
    pub fn new(gpu: &GpuContextHandle, size: u64, label: Option<&str>) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        let max = gpu.capabilities().compute.max_storage_buffer_binding_size;
        if size == 0 || !size.is_multiple_of(4) || size > max {
            return Err(GpuError::InvalidInput(format!(
                "GpuStorage size {size} must be a non-zero multiple of 4 up to {max}"
            )));
        }
        let buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label,
            size,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        Ok(Self {
            buffer: Arc::new(buffer),
            size,
            label: label.map(str::to_owned),
            device: device.clone(),
            queue: queue.clone(),
            device_key: super::device_key(device),
        })
    }

    /// A buffer holding `bytes` (padded with zeros to a multiple of 4).
    pub fn with_data(
        gpu: &GpuContextHandle,
        bytes: &[u8],
        label: Option<&str>,
    ) -> Result<Self, GpuError> {
        let storage = Self::new(gpu, (bytes.len() as u64).next_multiple_of(4), label)?;
        let aligned = bytes.len() / 4 * 4;
        storage.write(0, &bytes[..aligned])?;
        if aligned < bytes.len() {
            let mut tail = [0u8; 4];
            tail[..bytes.len() - aligned].copy_from_slice(&bytes[aligned..]);
            storage.write(aligned as u64, &tail)?;
        }
        Ok(storage)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The wgpu buffer, for the node's own pipelines and bind groups.
    pub fn buffer(&self) -> &wgpu::Buffer {
        &self.buffer
    }

    /// The device the buffer lives on.
    pub fn device(&self) -> &wgpu::Device {
        &self.device
    }

    pub fn queue(&self) -> &wgpu::Queue {
        &self.queue
    }

    /// Queue a write of `bytes` at `offset`; it lands before the next submission on the queue.
    /// `offset` and the length must be multiples of 4 and fit the buffer.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), GpuError> {
        let end = offset.checked_add(bytes.len() as u64);
        if !offset.is_multiple_of(4)
            || !bytes.len().is_multiple_of(4)
            || end.is_none_or(|end| end > self.size)
        {
            return Err(GpuError::InvalidInput(format!(
                "GpuStorage write of {} bytes at {offset} into {} bytes (4-byte aligned)",
                bytes.len(),
                self.size
            )));
        }
        self.queue.write_buffer(&self.buffer, offset, bytes);
        Ok(())
    }

    /// Grow the buffer to at least `size` bytes, zeroed (contents are not kept). Returns whether
    /// it reallocated: a node can call this every tick with its current need.
    pub fn ensure_size(&mut self, size: u64) -> Result<bool, GpuError> {
        if size <= self.size {
            return Ok(false);
        }
        let size = size.next_multiple_of(4);
        let buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: self.label.as_deref(),
            size,
            usage: self.buffer.usage(),
            mapped_at_creation: false,
        });
        self.buffer = Arc::new(buffer);
        self.size = size;
        Ok(true)
    }

    /// A storage binding for Daedalus's shader dispatch helpers.
    pub fn binding(&self, binding: u32, access: Access) -> ShaderBinding<'static> {
        ShaderBinding {
            binding,
            kind: BindingKind::Storage,
            access,
            data: BindingData::BufferDevice {
                buffer: self.buffer.clone(),
                size: self.size,
                device_key: self.device_key,
            },
            readback: false,
        }
    }
}

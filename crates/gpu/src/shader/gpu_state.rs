use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::GpuError;

use super::{Access, BindingData, BindingKind, ShaderBinding, ctx, staging_size, temp_pool};

/// Marker for GPU state structs; derive sets whether to zero-init and default readback.
pub trait GpuStateful: bytemuck::Pod + Copy {
    /// If true, `GpuState::new_stateful()` will zero-initialize instead of using Default.
    const ZEROED: bool = false;
    /// Default readback flag used by `binding_stateful`.
    const READBACK: bool = false;
}

struct StatePool {
    per_device: HashMap<usize, Vec<(u64, Arc<wgpu::Buffer>)>>,
}

/// Maximum pooled GPU state buffers per device before evicting oldest.
pub static GPU_STATE_POOL_LIMIT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(64);

pub fn set_gpu_state_pool_limit(limit: usize) -> usize {
    GPU_STATE_POOL_LIMIT.swap(limit.max(1), std::sync::atomic::Ordering::Relaxed)
}

pub fn gpu_state_pool_limit() -> usize {
    GPU_STATE_POOL_LIMIT
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(1)
}

impl StatePool {
    fn new() -> Self {
        Self {
            per_device: HashMap::new(),
        }
    }

    fn pool_mut(&mut self, device_key: usize) -> &mut Vec<(u64, Arc<wgpu::Buffer>)> {
        self.per_device.entry(device_key).or_default()
    }

    fn take(
        &mut self,
        device_key: usize,
        size: u64,
        usage: wgpu::BufferUsages,
    ) -> Option<Arc<wgpu::Buffer>> {
        let pool = self.pool_mut(device_key);
        if let Some(idx) = pool
            .iter()
            .position(|(s, b)| *s >= size && b.usage().contains(usage))
        {
            let (_, buf) = pool.swap_remove(idx);
            return Some(buf);
        }
        None
    }

    fn put(&mut self, device_key: usize, size: u64, buffer: Arc<wgpu::Buffer>) {
        let pool = self.pool_mut(device_key);
        if pool.len()
            >= GPU_STATE_POOL_LIMIT
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(1)
        {
            pool.swap_remove(0);
        }
        pool.push((size, buffer));
    }
}

static STATE_POOL: OnceLock<parking_lot::Mutex<StatePool>> = OnceLock::new();

fn state_pool() -> &'static parking_lot::Mutex<StatePool> {
    STATE_POOL.get_or_init(|| parking_lot::Mutex::new(StatePool::new()))
}

/// Persistent GPU-side state for small POD types; keeps a device buffer alive across dispatches.
pub struct GpuState<T: bytemuck::Pod + Copy> {
    buffer: Arc<wgpu::Buffer>,
    size: u64,
    device: wgpu::Device,
    queue: wgpu::Queue,
    device_key: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: bytemuck::Pod + Copy> GpuState<T> {
    pub fn new(initial: T) -> Result<Self, GpuError> {
        let ctx = ctx()?;
        Self::new_with_device(initial, ctx.device.as_ref(), ctx.queue.as_ref())
    }

    #[cfg(feature = "gpu-wgpu")]
    pub fn new_with_gpu(initial: T, gpu: &crate::GpuContextHandle) -> Result<Self, GpuError> {
        let backend = gpu.backend_ref();
        let wgpu_backend = backend
            .as_any()
            .downcast_ref::<crate::wgpu_backend::WgpuBackend>()
            .ok_or(GpuError::Unsupported)?;
        let (device, queue) = wgpu_backend.device_queue();
        Self::new_with_device(initial, device, queue)
    }

    fn new_with_device(
        initial: T,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
    ) -> Result<Self, GpuError> {
        let size = std::mem::size_of::<T>() as u64;
        let usage = wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::COPY_SRC;
        let pooled = state_pool()
            .lock()
            .take(super::device_key(device), size, usage);
        let buffer = if let Some(buf) = pooled {
            queue.write_buffer(buf.as_ref(), 0, bytemuck::bytes_of(&initial));
            buf
        } else {
            let buf = device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("gpu-state"),
                size,
                usage,
                mapped_at_creation: false,
            });
            queue.write_buffer(&buf, 0, bytemuck::bytes_of(&initial));
            Arc::new(buf)
        };
        Ok(Self {
            buffer,
            size,
            device: device.clone(),
            queue: queue.clone(),
            device_key: super::device_key(device),
            _marker: std::marker::PhantomData,
        })
    }

    /// Copy the current value into a pooled MAP_READ staging buffer and submit the copy.
    fn copy_to_staging(&self) -> wgpu::Buffer {
        let alloc = staging_size(self.size);
        let pooled = temp_pool().lock().take_buffer(
            self.device_key,
            alloc,
            wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        );
        let staging = pooled.unwrap_or_else(|| {
            self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("gpu-state-readback"),
                size: alloc,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        });
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("gpu-state-readback-encoder"),
            });
        encoder.copy_buffer_to_buffer(self.buffer.as_ref(), 0, &staging, 0, self.size);
        self.queue.submit([encoder.finish()]);
        staging
    }

    /// Decode the value from a mapped staging slice.
    fn value_from_mapped(&self, slice: wgpu::BufferSlice<'_>) -> Result<T, GpuError> {
        let data = slice.get_mapped_range()?;
        Ok(bytemuck::pod_read_unaligned::<T>(
            &data[..self.size as usize],
        ))
    }

    fn return_staging(&self, staging: wgpu::Buffer) {
        temp_pool().lock().put_buffer(self.device_key, staging);
    }

    /// Read back the current value from GPU. Blocks the calling thread until the copy completes.
    pub fn read(&self) -> Result<T, GpuError> {
        let staging = self.copy_to_staging();
        let slice = staging.slice(..staging_size(self.size));
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |res| {
            let _ = tx.send(res);
        });
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .unwrap_or(Err(wgpu::BufferAsyncError))
            .map_err(|e| GpuError::Internal(format!("map failed: {e:?}")))?;
        let val = self.value_from_mapped(slice)?;
        staging.unmap();
        self.return_staging(staging);
        Ok(val)
    }

    /// Read back the current value from GPU without blocking the calling thread.
    #[cfg(feature = "gpu-async")]
    pub async fn read_async(&self) -> Result<T, GpuError> {
        let staging = self.copy_to_staging();
        let slice = staging.slice(..staging_size(self.size));
        super::map_read_async(&self.device, slice).await?;
        let val = self.value_from_mapped(slice)?;
        staging.unmap();
        self.return_staging(staging);
        Ok(val)
    }

    /// Write a new value to the GPU buffer.
    pub fn write(&self, value: T) -> Result<(), GpuError> {
        self.queue
            .write_buffer(&self.buffer, 0, bytemuck::bytes_of(&value));
        Ok(())
    }

    /// Produce a binding to use this state in a shader dispatch.
    pub fn binding(&self, binding: u32, access: Access, readback: bool) -> ShaderBinding<'static> {
        ShaderBinding {
            binding,
            kind: BindingKind::Storage,
            access,
            data: BindingData::BufferDevice {
                buffer: self.buffer.clone(),
                size: self.size,
                device_key: self.device_key,
            },
            readback,
        }
    }
}

pub(crate) fn clear_gpu_state_pool_for_device(device_key: usize) {
    if let Some(pool) = STATE_POOL.get() {
        pool.lock().per_device.remove(&device_key);
    }
}

impl<T> GpuState<T>
where
    T: GpuStateful + bytemuck::Zeroable + Default + Copy,
{
    /// Create a GPU state using either zeroed or Default depending on the derive flags.
    pub fn new_stateful() -> Result<Self, GpuError> {
        let init = if T::ZEROED { T::zeroed() } else { T::default() };
        Self::new(init)
    }

    #[cfg(feature = "gpu-wgpu")]
    pub fn new_stateful_with_gpu(gpu: &crate::GpuContextHandle) -> Result<Self, GpuError> {
        let init = if T::ZEROED { T::zeroed() } else { T::default() };
        Self::new_with_gpu(init, gpu)
    }

    /// Binding that uses the type's default readback flag.
    pub fn binding_stateful(&self, binding: u32, access: Access) -> ShaderBinding<'static> {
        self.binding(binding, access, T::READBACK)
    }
}

impl<T: bytemuck::Pod + Copy> Drop for GpuState<T> {
    fn drop(&mut self) {
        // Only return to pool if this is the last Arc reference.
        if Arc::strong_count(&self.buffer) == 1 {
            state_pool()
                .lock()
                .put(self.device_key, self.size, self.buffer.clone());
        }
    }
}

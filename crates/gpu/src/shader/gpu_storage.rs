//! Node-owned persistent storage buffers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::{GpuContextHandle, GpuError};

use super::{Access, BindingData, BindingKind, ShaderBinding};

#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
mod dmabuf;

/// A sized storage buffer a node keeps on the engine's device for its lifetime: kept in the
/// node's `state(T)` across ticks (a TSDF volume, cost-volume scratch). Dropping it (on the
/// node's `Restart`, or when a reload drops the node's state) frees the buffer.
///
/// Created zeroed. Bind it to Daedalus dispatches with [`GpuStorage::binding`], or to the
/// node's own wgpu pipelines with [`GpuStorage::buffer`].
pub struct GpuStorage {
    pub(super) buffer: Arc<wgpu::Buffer>,
    pub(super) size: u64,
    label: Option<String>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    device_key: usize,
    staging: Arc<StagingRing>,
}

/// Reusable `MAP_READ` buffers for readbacks: a readback takes one and returns it when done.
#[derive(Default)]
pub(super) struct StagingRing {
    free: Mutex<Vec<wgpu::Buffer>>,
    created: AtomicUsize,
}

impl StagingRing {
    /// A free staging buffer of at least `size` bytes, or a new one.
    pub(super) fn take(&self, device: &wgpu::Device, size: u64) -> wgpu::Buffer {
        let mut free = self.free.lock();
        if let Some(at) = free.iter().position(|b| b.size() >= size) {
            return free.swap_remove(at);
        }
        drop(free);
        self.created.fetch_add(1, Ordering::Relaxed);
        device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("gpu-readback-staging"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        })
    }

    pub(super) fn created(&self) -> usize {
        self.created.load(Ordering::Relaxed)
    }
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
            staging: Arc::default(),
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
        // Readbacks in flight return their (smaller) staging buffers to the old ring.
        self.staging = Arc::default();
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

    /// Copy `out.len()` bytes from `offset` into `out`, waiting for the GPU. For setup, shutdown
    /// and tests: a tick uses [`GpuStorage::start_read`] and polls the [`GpuReadback`].
    pub fn read_into(&self, offset: u64, out: &mut [u8]) -> Result<(), GpuError> {
        let mut readback = self.start_read(offset, out.len() as u64)?;
        readback.wait();
        readback
            .try_read_into(out)
            .unwrap_or_else(|| Err(GpuError::Internal("readback did not complete".into())))
    }

    /// Start copying `len` bytes from `offset` into a reusable staging buffer and return without
    /// waiting. Work queued on this device before the call (dispatches writing the buffer) lands
    /// first. `offset` must be a multiple of 4; the range must fit the buffer.
    pub fn start_read(&self, offset: u64, len: u64) -> Result<GpuReadback, GpuError> {
        let copy = len.next_multiple_of(4);
        let end = offset.checked_add(copy);
        if !offset.is_multiple_of(4) || len == 0 || end.is_none_or(|end| end > self.size) {
            return Err(GpuError::InvalidInput(format!(
                "GpuStorage read of {len} bytes at {offset} from {} bytes (4-byte aligned offset)",
                self.size
            )));
        }
        let staging = self.staging.take(&self.device, self.size);
        let mut encoder = self.device.create_command_encoder(&Default::default());
        encoder.copy_buffer_to_buffer(&self.buffer, offset, &staging, 0, copy);
        self.queue.submit([encoder.finish()]);
        Ok(GpuReadback::begin(
            &self.device,
            &self.staging,
            staging,
            len as usize,
            copy,
            None,
        ))
    }

    /// Staging buffers this storage has created for readbacks: constant once readbacks reuse
    /// them (one per readback in flight).
    pub fn staging_buffers_created(&self) -> usize {
        self.staging.created()
    }
}

const MAP_PENDING: u8 = 0;
const MAP_OK: u8 = 1;
const MAP_FAILED: u8 = 2;

/// A readback started by [`GpuStorage::start_read`]. Poll it each tick with
/// [`GpuReadback::try_read_into`]; it never waits.
pub struct GpuReadback {
    staging: Option<wgpu::Buffer>,
    ring: Arc<StagingRing>,
    state: Arc<AtomicU8>,
    len: usize,
    copy: u64,
    /// `(padded, unpadded)` row bytes for a texture copy, whose rows are 256-byte aligned in
    /// the staging buffer.
    rows: Option<(usize, usize)>,
    device: wgpu::Device,
}

impl std::fmt::Debug for GpuReadback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuReadback")
            .field("len", &self.len)
            .field("ready", &self.is_ready())
            .finish_non_exhaustive()
    }
}

impl GpuReadback {
    /// Map the first `copy` bytes of `staging` (the copy into it is already submitted).
    pub(super) fn begin(
        device: &wgpu::Device,
        ring: &Arc<StagingRing>,
        staging: wgpu::Buffer,
        len: usize,
        copy: u64,
        rows: Option<(usize, usize)>,
    ) -> Self {
        let state = Arc::new(AtomicU8::new(MAP_PENDING));
        let flag = state.clone();
        staging
            .slice(..copy)
            .map_async(wgpu::MapMode::Read, move |result| {
                let value = if result.is_ok() { MAP_OK } else { MAP_FAILED };
                flag.store(value, Ordering::Release);
            });
        Self {
            staging: Some(staging),
            ring: ring.clone(),
            state,
            len,
            copy,
            rows,
            device: device.clone(),
        }
    }

    /// Bytes this readback delivers.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the data has arrived (as of the last device poll).
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) != MAP_PENDING
    }

    /// `None` while the copy is in flight (after one non-blocking device poll). Once it has
    /// arrived, copies it into `out` (exactly [`GpuReadback::len`] bytes) and returns
    /// `Some(Ok(()))`; the staging buffer goes back for reuse, and later calls return
    /// `Some(Err(..))`.
    pub fn try_read_into(&mut self, out: &mut [u8]) -> Option<Result<(), GpuError>> {
        if !self.is_ready() {
            let _ = self.device.poll(wgpu::PollType::Poll);
            if !self.is_ready() {
                return None;
            }
        }
        let Some(staging) = self.staging.take() else {
            return Some(Err(GpuError::InvalidInput(
                "readback already consumed".into(),
            )));
        };
        if self.state.load(Ordering::Acquire) == MAP_FAILED {
            return Some(Err(GpuError::Internal("readback map failed".into())));
        }
        if out.len() != self.len {
            self.staging = Some(staging);
            return Some(Err(GpuError::InvalidInput(format!(
                "readback of {} bytes into a {}-byte buffer",
                self.len,
                out.len()
            ))));
        }
        let result = match staging.slice(..self.copy).get_mapped_range() {
            Ok(view) => {
                match self.rows {
                    None => out.copy_from_slice(&view[..self.len]),
                    Some((padded, unpadded)) => {
                        for (dst, src) in out.chunks_mut(unpadded).zip(view.chunks(padded)) {
                            dst.copy_from_slice(&src[..unpadded]);
                        }
                    }
                }
                Ok(())
            }
            Err(err) => Err(GpuError::Internal(format!("readback map failed: {err:?}"))),
        };
        staging.unmap();
        self.ring.free.lock().push(staging);
        Some(result)
    }

    /// Wait for the data to arrive. For shutdown and tests.
    pub fn wait(&self) {
        if !self.is_ready() {
            let _ = self.device.poll(wgpu::PollType::Wait {
                submission_index: None,
                timeout: None,
            });
        }
    }
}

impl Drop for GpuReadback {
    fn drop(&mut self) {
        // A mapped, unread staging buffer goes back to the ring; one still mapping is dropped
        // (wgpu frees it once the map settles).
        if let Some(staging) = self.staging.take()
            && self.state.load(Ordering::Acquire) == MAP_OK
        {
            staging.unmap();
            self.ring.free.lock().push(staging);
        }
    }
}

//! Storage buffers, in each of the backend's memory spaces.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::WgpuBackend;
use crate::context::{Context, OpenError};
use crate::memory::{SPACE_DEVICE, SPACE_HOST_CACHED, SPACE_HOST_MAPPED};
use crate::staging::staging_size;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    Open(OpenError),
    /// The size isn't a non-zero multiple of 4 up to the device's limit.
    Size {
        size: u64,
        max: u64,
    },
    /// An offset or length that is outside the buffer or not 4-byte aligned.
    Range {
        offset: u64,
        len: u64,
        size: u64,
    },
    /// The device couldn't give the memory, or lost the readback.
    Device(String),
    /// This machine can't make that kind of buffer. The text says what is missing.
    Unsupported(&'static str),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Open(error) => error.fmt(f),
            StorageError::Size { size, max } => {
                write!(
                    f,
                    "a buffer of {size} bytes, where a size is a multiple of 4 from 4 to {max}"
                )
            }
            StorageError::Range { offset, len, size } => {
                write!(
                    f,
                    "{len} bytes at {offset} of a {size}-byte buffer, 4-byte aligned"
                )
            }
            StorageError::Device(reason) => f.write_str(reason),
            StorageError::Unsupported(what) => f.write_str(what),
        }
    }
}

impl std::error::Error for StorageError {}

/// How the host reaches a buffer's memory in place.
#[cfg(target_os = "linux")]
enum Mapping {
    /// The device's own memory, mapped.
    Device(daedalus_vk_sys::memory::HostMapping),
    /// A dma-buf, which wants to be told around each host access.
    Heap {
        buf: daedalus_dma_sys::DmaBuf,
        map: daedalus_dma_sys::Mapping,
    },
}
#[cfg(not(target_os = "linux"))]
enum Mapping {}

#[cfg(target_os = "linux")]
impl Mapping {
    fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), StorageError> {
        use daedalus_dma_sys::Access;
        let failed = |error: daedalus_dma_sys::Error| StorageError::Device(error.to_string());
        match self {
            Mapping::Device(map) => map
                .write(offset, bytes)
                .map_err(|error| StorageError::Device(error.to_string())),
            Mapping::Heap { buf, map } => {
                buf.as_ref()
                    .begin_cpu_access(Access::Write)
                    .map_err(failed)?;
                let wrote = map.write(offset, bytes);
                buf.as_ref().end_cpu_access(Access::Write).map_err(failed)?;
                wrote.map_err(failed)
            }
        }
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), StorageError> {
        use daedalus_dma_sys::Access;
        let failed = |error: daedalus_dma_sys::Error| StorageError::Device(error.to_string());
        match self {
            Mapping::Device(map) => map
                .read(offset, out)
                .map_err(|error| StorageError::Device(error.to_string())),
            Mapping::Heap { buf, map } => {
                buf.as_ref()
                    .begin_cpu_access(Access::Read)
                    .map_err(failed)?;
                let read = map.read(offset, out);
                buf.as_ref().end_cpu_access(Access::Read).map_err(failed)?;
                read.map_err(failed)
            }
        }
    }
}

/// A storage buffer on the backend's device. Made at load or setup: nothing here is called while
/// a region runs except [`Storage::write`] and the reads.
pub struct Storage {
    pub(crate) buffer: wgpu::Buffer,
    /// The size asked for. The buffer behind it may be larger, when it came from a pool.
    pub(crate) size: u64,
    /// Unique to this buffer, for the bind group cache.
    pub(crate) id: u64,
    space: &'static str,
    mapping: Option<Mapping>,
    pub(crate) context: Arc<Context>,
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Storage")
            .field("size", &self.size)
            .field("space", &self.space)
            .finish_non_exhaustive()
    }
}

fn check_size(context: &Context, size: u64) -> Result<(), StorageError> {
    let max = context
        .limits
        .max_storage_buffer_binding_size
        .min(context.limits.max_buffer_size);
    if size == 0 || !size.is_multiple_of(4) || size > max {
        return Err(StorageError::Size { size, max });
    }
    Ok(())
}

impl Storage {
    /// A zeroed buffer in device memory.
    pub fn new(backend: &WgpuBackend, size: u64) -> Result<Storage, StorageError> {
        let context = backend.context().map_err(StorageError::Open)?;
        Storage::zeroed(context, size)
    }

    pub(crate) fn zeroed(context: Arc<Context>, size: u64) -> Result<Storage, StorageError> {
        check_size(&context, size)?;
        let buffer = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        Ok(Storage {
            buffer,
            size,
            id: Context::next_id(),
            space: SPACE_DEVICE,
            mapping: None,
            context,
        })
    }

    /// A buffer in device memory that nothing has cleared. Only for a buffer its producer writes
    /// in full before anything reads it: skipping the clear is the point. Off Vulkan this is
    /// [`Storage::new`].
    pub fn new_uninit(backend: &WgpuBackend, size: u64) -> Result<Storage, StorageError> {
        Storage::placed(backend, size, SPACE_DEVICE)
    }

    /// A buffer the host reads and writes in place, with no staging copy. Off Vulkan this is
    /// [`Storage::new`], and [`Storage::is_host_mapped`] says so.
    pub fn new_host_mapped(backend: &WgpuBackend, size: u64) -> Result<Storage, StorageError> {
        Storage::placed(backend, size, SPACE_HOST_MAPPED)
    }

    /// Host-mapped in cached memory, for buffers the host reads a lot. A device with no cached
    /// memory of its own gets a buffer from the system dma-heap, and failing that plain
    /// host-mapped memory.
    pub fn new_host_cached(backend: &WgpuBackend, size: u64) -> Result<Storage, StorageError> {
        #[cfg(target_os = "linux")]
        {
            let context = backend.context().map_err(StorageError::Open)?;
            if !daedalus_vk_sys::memory::has_host_cached(&context.device)
                && let Ok(storage) = Storage::new_dma_heap(backend, size)
            {
                return Ok(storage);
            }
        }
        Storage::placed(backend, size, SPACE_HOST_CACHED)
    }

    /// A buffer whose memory is a dma-buf from the system heap: mapped cached for the host,
    /// imported for the device with no copy. `Unsupported` where there is no heap to open or the
    /// device can't import a dma-buf.
    #[cfg(target_os = "linux")]
    pub fn new_dma_heap(backend: &WgpuBackend, size: u64) -> Result<Storage, StorageError> {
        use daedalus_vk_sys::Error;
        let context = backend.context().map_err(StorageError::Open)?;
        check_size(&context, size)?;
        let failed = |error: daedalus_dma_sys::Error| StorageError::Device(error.to_string());
        let heap = daedalus_dma_sys::DmaHeap::open("system")
            .map_err(|_| StorageError::Unsupported("no /dev/dma_heap/system to allocate from"))?;
        let buf = heap.allocate(size).map_err(failed)?;
        let map = buf.as_ref().map().map_err(failed)?;
        let fd = buf.try_clone().map_err(failed)?.into_fd();
        let buffer = daedalus_vk_sys::memory::import_dma_buf(&context.device, fd, size, None)
            .map_err(|error| match error {
                Error::NoDmaBufImport | Error::NotVulkan => {
                    StorageError::Unsupported("the device can't import a dma-buf")
                }
                other => StorageError::Device(other.to_string()),
            })?;
        Ok(Storage {
            buffer,
            size,
            id: Context::next_id(),
            space: crate::memory::SPACE_DMA_HEAP,
            mapping: Some(Mapping::Heap { buf, map }),
            context,
        })
    }

    /// A dma-heap buffer. There are none off Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn new_dma_heap(_: &WgpuBackend, _: u64) -> Result<Storage, StorageError> {
        Err(StorageError::Unsupported("dma-heaps are a Linux interface"))
    }

    #[cfg(target_os = "linux")]
    fn placed(
        backend: &WgpuBackend,
        size: u64,
        space: &'static str,
    ) -> Result<Storage, StorageError> {
        use daedalus_vk_sys::Error;
        use daedalus_vk_sys::memory::{Placement, storage_buffer};

        let context = backend.context().map_err(StorageError::Open)?;
        check_size(&context, size)?;
        let placement = match space {
            SPACE_HOST_MAPPED => Placement::HostMapped,
            SPACE_HOST_CACHED => Placement::HostCached,
            _ => Placement::Device,
        };
        match storage_buffer(&context.device, size, placement, None) {
            Ok((buffer, mapping)) => Ok(Storage {
                buffer,
                size,
                id: Context::next_id(),
                space,
                mapping: mapping.map(Mapping::Device),
                context,
            }),
            Err(Error::NotVulkan) => Storage::zeroed(context, size),
            Err(error) => Err(StorageError::Device(error.to_string())),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn placed(backend: &WgpuBackend, size: u64, _: &'static str) -> Result<Storage, StorageError> {
        Storage::new(backend, size)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The id of the memory space this buffer is in.
    pub fn space(&self) -> &'static str {
        if self.mapping.is_some() {
            self.space
        } else {
            SPACE_DEVICE
        }
    }

    pub fn is_host_mapped(&self) -> bool {
        self.mapping.is_some()
    }

    /// Which device this is on. Two values with one key share a device.
    pub fn device_key(&self) -> u64 {
        self.context.key
    }

    fn range(&self, offset: u64, len: u64) -> Result<(), StorageError> {
        let inside = offset.checked_add(len).is_some_and(|end| end <= self.size);
        if !inside || !offset.is_multiple_of(4) || !len.is_multiple_of(4) || len == 0 {
            return Err(StorageError::Range {
                offset,
                len,
                size: self.size,
            });
        }
        Ok(())
    }

    /// Copies `bytes` into the buffer at `offset`. Both are multiples of 4. In a host-mapped
    /// buffer the device sees them at once. Otherwise they travel with the next submit.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), StorageError> {
        self.range(offset, bytes.len() as u64)?;
        #[cfg(target_os = "linux")]
        if let Some(mapping) = &self.mapping {
            return mapping.write(offset, bytes);
        }
        self.context.queue.write_buffer(&self.buffer, offset, bytes);
        Ok(())
    }

    /// Copies the buffer from `offset` into `out`, whose length is a multiple of 4. A host-mapped
    /// buffer is read in place, and the work that wrote it must have finished. Any other buffer
    /// is copied to a staging buffer and this waits for the copy, which also waits for the
    /// device's earlier work.
    pub fn read_into(&self, offset: u64, out: &mut [u8]) -> Result<(), StorageError> {
        self.range(offset, out.len() as u64)?;
        #[cfg(target_os = "linux")]
        if let Some(mapping) = &self.mapping {
            return mapping.read(offset, out);
        }
        self.read_staged(offset, out)
    }

    fn read_staged(&self, offset: u64, out: &mut [u8]) -> Result<(), StorageError> {
        let context = &self.context;
        let len = out.len() as u64;
        let lost = |what: &str| StorageError::Device(format!("the readback {what}"));
        let _place = context.copies.acquire();
        let wanted = staging_size(len);
        let staging = context.staging().take(wanted).unwrap_or_else(|| {
            context.device.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size: wanted,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        });
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        encoder.copy_buffer_to_buffer(&self.buffer, offset, &staging, 0, len);
        let mapped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&mapped);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
        }
        {
            // Mapping flushes pending writes, which is a submit.
            let _held = context.submit.lock();
            staging
                .slice(..wanted)
                .map_async(wgpu::MapMode::Read, move |result| {
                    flag.store(result.is_ok(), Ordering::Release);
                });
        }
        context
            .device
            .poll(wgpu::PollType::wait_indefinitely())
            .map_err(|_| lost("was lost with the device"))?;
        if !mapped.load(Ordering::Acquire) {
            return Err(lost("could not be mapped"));
        }
        {
            let view = staging
                .slice(..wanted)
                .get_mapped_range()
                .map_err(|_| lost("could not be mapped"))?;
            let bytes = view
                .get(..out.len())
                .ok_or_else(|| lost("came back short"))?;
            out.copy_from_slice(bytes);
        }
        staging.unmap();
        let size = staging.size();
        context.staging().put(size, staging);
        Ok(())
    }
}

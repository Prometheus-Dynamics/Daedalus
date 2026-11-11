//! Storage buffers, in each of the backend's memory spaces.

use std::fmt;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_os = "linux")]
use std::sync::{Mutex, PoisonError};

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
        /// Set whenever the device is given the buffer, and cleared by a read's sync. While it is
        /// clear the device can't have written the buffer since the host last synced, so a read
        /// has nothing to invalidate. Starts set: a new buffer may be written before it is read.
        device_may_have_written: AtomicBool,
        /// How many read syncs have run, for tests and budgets.
        syncs: AtomicU64,
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
            Mapping::Heap { buf, map, .. } => {
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
            Mapping::Heap {
                buf,
                map,
                device_may_have_written,
                syncs,
            } => {
                // Nothing the device did can be waiting in the cache, so the sync is skipped.
                if !device_may_have_written.swap(false, Ordering::AcqRel) {
                    return map.read(offset, out).map_err(failed);
                }
                syncs.fetch_add(1, Ordering::Relaxed);
                // A sync that fails leaves the buffer as possibly written, so the next read syncs.
                if let Err(error) = buf.as_ref().begin_cpu_access(Access::Read) {
                    device_may_have_written.store(true, Ordering::Release);
                    return Err(failed(error));
                }
                let read = map.read(offset, out);
                if let Err(error) = buf.as_ref().end_cpu_access(Access::Read) {
                    device_may_have_written.store(true, Ordering::Release);
                    return Err(failed(error));
                }
                read.map_err(failed)
            }
        }
    }
}

/// A storage buffer on the backend's device. Made at load or setup: nothing here is called while
/// a region runs except [`Storage::write`] and the reads.
pub struct Storage {
    /// The buffer. Outside this module it is reached through [`Storage::buffer`], which first
    /// settles what a new buffer still owes.
    raw: wgpu::Buffer,
    /// The size asked for. The buffer behind it may be larger, when it came from a pool.
    pub(crate) size: u64,
    /// Unique to this buffer, for the bind group cache.
    pub(crate) id: u64,
    space: &'static str,
    mapping: Option<Mapping>,
    /// Set for device memory this backend allocated itself.
    #[cfg(target_os = "linux")]
    fresh: Option<Fresh>,
    pub(crate) context: Arc<Context>,
}

/// Device memory of this backend's own, which wgpu neither clears nor tracks. Until the device
/// is first shown the buffer, the host has it to itself: a write goes straight into its memory
/// where that is mapped, and zeroes are only written if something would see what was there.
/// When its `Storage` is dropped the buffer goes back to the device, to be handed out again.
#[cfg(target_os = "linux")]
struct Fresh {
    /// Set once the device has been shown the buffer. From then on it is written through the
    /// queue, in order with the device's work.
    seen: AtomicBool,
    /// Whether every byte has been written since the buffer was handed out. Zeroes are owed
    /// while it is false. Held while the host writes.
    filled: Mutex<bool>,
    /// The buffer's memory, where the device's memory is the host's too.
    map: Option<daedalus_vk_sys::memory::HostMapping>,
}

#[cfg(target_os = "linux")]
impl Drop for Storage {
    fn drop(&mut self) {
        if let Some(fresh) = self.fresh.take() {
            self.context.keep_spare((self.raw.clone(), fresh.map));
        }
    }
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
        #[cfg(target_os = "linux")]
        match Storage::fresh(&context, size, false) {
            Ok(storage) => return Ok(storage),
            Err(daedalus_vk_sys::Error::NotVulkan) => {}
            Err(error) => return Err(StorageError::Device(error.to_string())),
        }
        // Off Vulkan the buffer is wgpu's, which clears it before its first use.
        let raw = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        Ok(Storage {
            raw,
            size,
            id: Context::next_id(),
            space: SPACE_DEVICE,
            mapping: None,
            #[cfg(target_os = "linux")]
            fresh: None,
            context,
        })
    }

    /// Device memory of this backend's own: a buffer that was let go, or a new one. `filled`
    /// says whether its holder takes it as it is, or is owed zeroes.
    #[cfg(target_os = "linux")]
    fn fresh(
        context: &Arc<Context>,
        size: u64,
        filled: bool,
    ) -> Result<Storage, daedalus_vk_sys::Error> {
        use daedalus_vk_sys::memory::{Placement, storage_buffer};
        let (raw, map) = match context.take_spare(size) {
            Some(kept) => kept,
            None => storage_buffer(&context.device, size, Placement::DeviceReachable, None)?,
        };
        Ok(Storage {
            raw,
            size,
            id: Context::next_id(),
            space: SPACE_DEVICE,
            mapping: None,
            fresh: Some(Fresh {
                seen: AtomicBool::new(false),
                filled: Mutex::new(filled),
                map,
            }),
            context: Arc::clone(context),
        })
    }

    /// The buffer, for the device to use. The first call for a new buffer writes the zeroes it
    /// still owes.
    pub(crate) fn buffer(&self) -> &wgpu::Buffer {
        // Every device use of the buffer comes through here, so the flag is right for the sync.
        #[cfg(target_os = "linux")]
        if let Some(Mapping::Heap {
            device_may_have_written,
            ..
        }) = &self.mapping
        {
            device_may_have_written.store(true, Ordering::Release);
        }
        #[cfg(target_os = "linux")]
        if let Some(fresh) = &self.fresh
            && !fresh.seen.load(Ordering::Acquire)
        {
            self.show(fresh);
        }
        &self.raw
    }

    /// How large the buffer behind this is, which is the size asked for or more.
    pub(crate) fn capacity(&self) -> u64 {
        self.raw.size()
    }

    #[cfg(target_os = "linux")]
    fn show(&self, fresh: &Fresh) {
        let mut filled = fresh.filled.lock().unwrap_or_else(PoisonError::into_inner);
        if !*filled {
            let context = &self.context;
            let mut encoder = context
                .device
                .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
            encoder.clear_buffer(&self.raw, 0, None);
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
            *filled = true;
        }
        fresh.seen.store(true, Ordering::Release);
    }

    /// Writes into a new buffer the device hasn't been shown, and says whether that is all of
    /// the write: where the buffer's memory is mapped the bytes go straight in. A write of the
    /// whole buffer leaves no zeroes owed either way.
    #[cfg(target_os = "linux")]
    fn write_unseen(&self, offset: u64, bytes: &[u8]) -> Result<bool, StorageError> {
        let Some(fresh) = &self.fresh else {
            return Ok(false);
        };
        let mut filled = fresh.filled.lock().unwrap_or_else(PoisonError::into_inner);
        if fresh.seen.load(Ordering::Acquire) {
            return Ok(false);
        }
        let whole = offset == 0 && bytes.len() as u64 == self.size;
        let failed = |error: daedalus_vk_sys::Error| StorageError::Device(error.to_string());
        let Some(map) = &fresh.map else {
            *filled |= whole;
            return Ok(false);
        };
        if !*filled && !whole {
            map.zero(0, self.size as usize).map_err(failed)?;
        }
        map.write(offset, bytes).map_err(failed)?;
        *filled = true;
        Ok(true)
    }

    /// A buffer whose memory was imported from outside. The device reads it where it is.
    #[cfg(target_os = "linux")]
    pub(crate) fn imported(context: Arc<Context>, buffer: wgpu::Buffer, size: u64) -> Storage {
        Storage {
            raw: buffer,
            size,
            id: Context::next_id(),
            space: SPACE_DEVICE,
            mapping: None,
            fresh: None,
            context,
        }
    }

    /// A buffer in device memory that nothing has cleared. Only for a buffer its producer writes
    /// in full before anything reads it: skipping the clear is the point. It may be a buffer an
    /// earlier holder let go, with that holder's bytes still in it. Off Vulkan this is
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
        let context = backend.context().map_err(StorageError::Open)?;
        Storage::dma_heap_on(context, size)
    }

    /// A dma-heap buffer on `context`'s device. See [`Storage::new_dma_heap`].
    #[cfg(target_os = "linux")]
    pub(crate) fn dma_heap_on(context: Arc<Context>, size: u64) -> Result<Storage, StorageError> {
        use daedalus_vk_sys::Error;
        check_size(&context, size)?;
        let failed = |error: daedalus_dma_sys::Error| StorageError::Device(error.to_string());
        let heap = daedalus_dma_sys::DmaHeap::open("system")
            .map_err(|_| StorageError::Unsupported("no /dev/dma_heap/system to allocate from"))?;
        let unsupported = |error: Error| match error {
            Error::NoDmaBufImport | Error::NotVulkan => {
                StorageError::Unsupported("the device can't import a dma-buf")
            }
            other => StorageError::Device(other.to_string()),
        };
        // The device may need more memory than the buffer has bytes: V3D pads it.
        let need = daedalus_vk_sys::memory::dma_buf_bytes_needed(&context.device, size)
            .map_err(unsupported)?;
        let buf = heap.allocate(need.max(size)).map_err(failed)?;
        let map = buf.as_ref().map().map_err(failed)?;
        let have = buf.as_ref().len().map_err(failed)?;
        let fd = buf.try_clone().map_err(failed)?.into_fd();
        let buffer = daedalus_vk_sys::memory::import_dma_buf(&context.device, fd, size, have, None)
            .map_err(unsupported)?;
        Ok(Storage {
            raw: buffer,
            size,
            id: Context::next_id(),
            space: crate::memory::SPACE_DMA_HEAP,
            mapping: Some(Mapping::Heap {
                buf,
                map,
                device_may_have_written: AtomicBool::new(true),
                syncs: AtomicU64::new(0),
            }),
            #[cfg(target_os = "linux")]
            fresh: None,
            context,
        })
    }

    /// A dma-heap buffer. There are none off Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn new_dma_heap(_: &WgpuBackend, _: u64) -> Result<Storage, StorageError> {
        Err(StorageError::Unsupported("dma-heaps are a Linux interface"))
    }

    fn placed(
        backend: &WgpuBackend,
        size: u64,
        space: &'static str,
    ) -> Result<Storage, StorageError> {
        let context = backend.context().map_err(StorageError::Open)?;
        Storage::placed_on(context, size, space)
    }

    /// A buffer in `space` on `context`'s device.
    #[cfg(target_os = "linux")]
    pub(crate) fn placed_on(
        context: Arc<Context>,
        size: u64,
        space: &'static str,
    ) -> Result<Storage, StorageError> {
        use daedalus_vk_sys::Error;
        use daedalus_vk_sys::memory::{Placement, storage_buffer};

        check_size(&context, size)?;
        let placement = match space {
            SPACE_HOST_MAPPED => Placement::HostMapped,
            SPACE_HOST_CACHED => Placement::HostCached,
            // Nothing cleared, and nothing owed: its holder writes all of it.
            _ => {
                return match Storage::fresh(&context, size, true) {
                    Ok(storage) => Ok(storage),
                    Err(Error::NotVulkan) => Storage::zeroed(context, size),
                    Err(error) => Err(StorageError::Device(error.to_string())),
                };
            }
        };
        match storage_buffer(&context.device, size, placement, None) {
            Ok((raw, mapping)) => Ok(Storage {
                raw,
                size,
                id: Context::next_id(),
                space,
                mapping: mapping.map(Mapping::Device),
                fresh: None,
                context,
            }),
            Err(Error::NotVulkan) => Storage::zeroed(context, size),
            Err(error) => Err(StorageError::Device(error.to_string())),
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn placed_on(
        context: Arc<Context>,
        size: u64,
        _: &'static str,
    ) -> Result<Storage, StorageError> {
        Storage::zeroed(context, size)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The wgpu buffer, for a recording body or a fragment pass of your own to bind. It has
    /// storage and copy usage. Don't keep it past this `Storage`: its memory may be handed to
    /// another one.
    pub fn wgpu(&self) -> &wgpu::Buffer {
        self.buffer()
    }

    /// How many CPU-access syncs the host's reads of this buffer have run. Zero for a buffer that
    /// is not a dma-heap buffer. A test uses it to see that a read with no device use in between
    /// does no sync.
    pub fn cpu_access_syncs(&self) -> u64 {
        #[cfg(target_os = "linux")]
        if let Some(Mapping::Heap { syncs, .. }) = &self.mapping {
            return syncs.load(Ordering::Relaxed);
        }
        0
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
        #[cfg(target_os = "linux")]
        if self.write_unseen(offset, bytes)? {
            return Ok(());
        }
        self.context
            .queue
            .write_buffer(self.buffer(), offset, bytes);
        self.context.note_write(bytes.len() as u64);
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
        encoder.copy_buffer_to_buffer(self.buffer(), offset, &staging, 0, len);
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
            .wait("a readback")
            .map_err(|error| StorageError::Device(error.to_string()))?;
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
        context.moved.download(len);
        Ok(())
    }
}

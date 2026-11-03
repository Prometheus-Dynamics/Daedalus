//! `GpuStorage` in host-visible memory, mapped once for its lifetime (zero-copy upload and
//! readback on unified-memory GPUs: V3D, Mali, integrated, lavapipe), and device buffers
//! without wgpu's zero-initialization clear.
//!
//! Both are Vulkan memory created through wgpu's hal layer, like the dmabuf import: wgpu frees
//! it when the buffer drops, and treats the contents as initialized (no lazy clear). Where the
//! device has no host-cached memory (v3dv), host-cached storage is a system dma-heap buffer
//! instead: imported for the GPU, mapped cached for the CPU, with `DMA_BUF_IOCTL_SYNC` around
//! each CPU access (feature `gpu-dmabuf`).

use std::sync::Arc;

use ash::vk;
use wgpu::hal::{api::Vulkan, vulkan as hal_vk};

use crate::{GpuContextHandle, GpuError};

use super::GpuStorage;

/// A persistent CPU mapping of a host-visible [`GpuStorage`].
pub(crate) struct HostMapping {
    ptr: *mut u8,
    len: usize,
    /// A dma-heap buffer and its own mapping (`None`: Vulkan's, unmapped when wgpu frees the
    /// memory). The mapping unmaps when this drops.
    #[cfg(feature = "gpu-dmabuf")]
    dmabuf: Option<(lemnos_linux::dma::DmaBuf, lemnos_linux::dma::Mapping)>,
}

/// Which way a CPU access goes, for a dma-heap mapping's cache maintenance.
#[derive(Clone, Copy)]
enum CpuAccess {
    Read,
    Write,
}

impl HostMapping {
    /// Start (`end` false) or end a CPU access: `DMA_BUF_IOCTL_SYNC` (cache maintenance) for a
    /// dma-heap mapping; nothing for coherent Vulkan memory.
    #[cfg_attr(not(feature = "gpu-dmabuf"), allow(unused_variables))]
    fn sync(&self, access: CpuAccess, end: bool) {
        #[cfg(feature = "gpu-dmabuf")]
        if let Some((dmabuf, _)) = &self.dmabuf {
            use lemnos_linux::dma::Access;
            let access = match access {
                CpuAccess::Read => Access::Read,
                CpuAccess::Write => Access::Write,
            };
            // Cache maintenance on a buffer we allocated; it fails only on a bad descriptor.
            let _ = if end {
                dmabuf.end_cpu_access(access)
            } else {
                dmabuf.begin_cpu_access(access)
            };
        }
    }
}

// SAFETY: the mapping is plain memory owned by the buffer; access goes through `GpuStorage`'s
// copy methods, whose contract rules out racing GPU work.
unsafe impl Send for HostMapping {}
// SAFETY: as above.
unsafe impl Sync for HostMapping {}

/// Which memory a hal-created buffer gets.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// Host-visible and coherent (device-local too where the GPU has such memory: UMA).
    HostMapped,
    /// Host-visible, cached and coherent where the device has it: fast CPU reads.
    HostCached,
    /// Device-local; no CPU access.
    Device,
}

impl GpuStorage {
    /// A storage buffer in host-visible, coherent memory, mapped for its whole life:
    /// [`write_mapped`](Self::write_mapped) and [`read_mapped`](Self::read_mapped) copy straight
    /// into and out of it, with no staging buffer and no queue copy. On unified-memory GPUs (V3D,
    /// Mali, integrated, lavapipe) that memory is also the GPU's own, so this is the zero-copy
    /// path; on discrete GPUs the GPU reads it over the bus. The contents start undefined (no
    /// zero-initialization clear). `Unsupported` off Vulkan.
    pub fn new_host_mapped(
        gpu: &GpuContextHandle,
        size: u64,
        label: Option<&str>,
    ) -> Result<Self, GpuError> {
        create(gpu, size, label, Placement::HostMapped)
    }

    /// [`GpuStorage::new_host_mapped`] in host-cached memory: for results the CPU reads back.
    /// Uncached host memory, the usual kind on UMA, is slow for CPU reads. Where the device has
    /// no host-cached memory type (v3dv), the storage is a system dma-heap buffer the GPU writes
    /// through an import and the CPU reads through a cached mapping (with the
    /// `gpu-dmabuf` feature and `/dev/dma_heap/system` access): 3x faster reads on the CM5. Else
    /// it falls back to the plain host-mapped kind.
    pub fn new_host_cached(
        gpu: &GpuContextHandle,
        size: u64,
        label: Option<&str>,
    ) -> Result<Self, GpuError> {
        #[cfg(feature = "gpu-dmabuf")]
        if !has_host_cached_memory(gpu)
            && let Ok(storage) = dma_heap_storage(gpu, size, label)
        {
            return Ok(storage);
        }
        create(gpu, size, label, Placement::HostCached)
    }

    /// A device-local storage buffer whose contents start undefined: no zero-initialization
    /// clear on first use. For buffers the caller fully writes before reading.
    /// `Unsupported` off Vulkan (use [`GpuStorage::new`]).
    pub fn new_uninit(
        gpu: &GpuContextHandle,
        size: u64,
        label: Option<&str>,
    ) -> Result<Self, GpuError> {
        create(gpu, size, label, Placement::Device)
    }

    /// Whether the buffer is host-mapped ([`GpuStorage::new_host_mapped`]).
    pub fn is_host_mapped(&self) -> bool {
        self.host.is_some()
    }

    /// Copy `bytes` into a host-mapped buffer at `offset`. GPU work submitted afterwards sees
    /// them (the memory is coherent). Do not call while submitted work that reads or writes
    /// the range is in flight: wait for its [`GpuTicket`](crate::GpuTicket) first.
    /// `InvalidInput` when not host-mapped or out of range.
    pub fn write_mapped(&self, offset: u64, bytes: &[u8]) -> Result<(), GpuError> {
        let (host, start) = self.mapped_range(offset, bytes.len())?;
        host.sync(CpuAccess::Write, false);
        // SAFETY: `start..start + len` lies in the live mapping (checked); the caller keeps GPU
        // work off the range.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), host.ptr.add(start), bytes.len()) };
        host.sync(CpuAccess::Write, true);
        Ok(())
    }

    /// Copy `out.len()` bytes at `offset` out of a host-mapped buffer: a readback without a
    /// staging copy. Call after the GPU work that wrote the range completed. Make buffers the
    /// CPU reads with [`GpuStorage::new_host_cached`]: reading uncached host memory is one to
    /// two orders of magnitude slower.
    pub fn read_mapped(&self, offset: u64, out: &mut [u8]) -> Result<(), GpuError> {
        let (host, start) = self.mapped_range(offset, out.len())?;
        host.sync(CpuAccess::Read, false);
        // SAFETY: as in `write_mapped`.
        unsafe { std::ptr::copy_nonoverlapping(host.ptr.add(start), out.as_mut_ptr(), out.len()) };
        host.sync(CpuAccess::Read, true);
        Ok(())
    }

    fn mapped_range(&self, offset: u64, len: usize) -> Result<(&HostMapping, usize), GpuError> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| GpuError::InvalidInput("the buffer is not host-mapped".into()))?;
        let start = offset as usize;
        if offset > host.len as u64 || len > host.len - start {
            return Err(GpuError::InvalidInput(format!(
                "range {offset}+{len} of a {}-byte buffer",
                host.len
            )));
        }
        Ok((host, start))
    }
}

fn create(
    gpu: &GpuContextHandle,
    size: u64,
    label: Option<&str>,
    placement: Placement,
) -> Result<GpuStorage, GpuError> {
    let (device, queue) = gpu.wgpu_locked().ok_or(GpuError::Unsupported)?;
    let max = gpu.capabilities().compute.max_storage_buffer_binding_size;
    if size == 0 || !size.is_multiple_of(4) || size > max {
        return Err(GpuError::InvalidInput(format!(
            "GpuStorage size {size} must be a non-zero multiple of 4 up to {max}"
        )));
    }
    // SAFETY: the hal device outlives this block; the buffer and memory made here go to wgpu
    // (`from_raw_managed`), which destroys and frees them (unmapping first) when the buffer drops.
    let (hal_buffer, host) = unsafe {
        let hal = device.as_hal::<Vulkan>().ok_or(GpuError::Unsupported)?;
        allocate(&hal, size, placement)?
    };
    // SAFETY: created on this device with storage and transfer usages matching the descriptor,
    // with its memory bound.
    let buffer = unsafe {
        device.create_buffer_from_hal::<Vulkan>(
            hal_buffer,
            &wgpu::BufferDescriptor {
                label,
                size,
                usage: wgpu::BufferUsages::STORAGE
                    | wgpu::BufferUsages::COPY_SRC
                    | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            },
        )
    };
    Ok(GpuStorage {
        buffer: Arc::new(buffer),
        size,
        label: label.map(str::to_owned),
        device: device.clone(),
        queue: queue.clone(),
        device_key: super::super::device_key(device),
        staging: Arc::default(),
        host: host.map(Arc::new),
    })
}

/// Create the `VkBuffer`, allocate dedicated memory for `placement`, bind it and (host-mapped)
/// map it.
///
/// # Safety
/// `hal` must be the device the returned buffer is handed to.
unsafe fn allocate(
    hal: &hal_vk::Device,
    size: u64,
    placement: Placement,
) -> Result<(hal_vk::Buffer, Option<HostMapping>), GpuError> {
    let instance = hal.shared_instance().raw_instance();
    let raw = hal.raw_device();
    let failed = |what: &str, err: vk::Result| GpuError::Internal(format!("{what}: {err}"));
    let info = vk::BufferCreateInfo::default()
        .size(size)
        .usage(
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
        )
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    // SAFETY: valid device and create info.
    let buffer =
        unsafe { raw.create_buffer(&info, None) }.map_err(|e| failed("vkCreateBuffer", e))?;
    // SAFETY: `buffer` belongs to `raw`.
    let req = unsafe { raw.get_buffer_memory_requirements(buffer) };
    // SAFETY: valid physical device; query only.
    let props =
        unsafe { instance.get_physical_device_memory_properties(hal.raw_physical_device()) };
    let types = &props.memory_types[..props.memory_type_count as usize];
    let pick = |want: vk::MemoryPropertyFlags| {
        types.iter().enumerate().position(|(i, t)| {
            req.memory_type_bits & (1 << i) != 0 && t.property_flags.contains(want)
        })
    };
    let host = vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_COHERENT;
    let index = match placement {
        // Unified memory first (device-local and host-visible), else plain host memory.
        Placement::HostMapped => {
            pick(host | vk::MemoryPropertyFlags::DEVICE_LOCAL).or_else(|| pick(host))
        }
        // Cached for CPU reads; else as host-mapped.
        Placement::HostCached => pick(host | vk::MemoryPropertyFlags::HOST_CACHED)
            .or_else(|| pick(host | vk::MemoryPropertyFlags::DEVICE_LOCAL))
            .or_else(|| pick(host)),
        Placement::Device => pick(vk::MemoryPropertyFlags::DEVICE_LOCAL)
            .or_else(|| pick(vk::MemoryPropertyFlags::empty())),
    };
    let Some(index) = index else {
        // SAFETY: created above, unused.
        unsafe { raw.destroy_buffer(buffer, None) };
        return Err(GpuError::Unsupported);
    };
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(req.size)
        .memory_type_index(index as u32)
        .push_next(&mut dedicated);
    // SAFETY: valid allocate info.
    let memory = match unsafe { raw.allocate_memory(&alloc, None) } {
        Ok(memory) => memory,
        Err(err) => {
            // SAFETY: created above, unused.
            unsafe { raw.destroy_buffer(buffer, None) };
            return Err(failed("vkAllocateMemory", err));
        }
    };
    let cleanup = || {
        // SAFETY: both created above on `raw`, unused by the GPU.
        unsafe {
            raw.free_memory(memory, None);
            raw.destroy_buffer(buffer, None);
        }
    };
    // SAFETY: fresh buffer and memory of the required size.
    if let Err(err) = unsafe { raw.bind_buffer_memory(buffer, memory, 0) } {
        cleanup();
        return Err(failed("vkBindBufferMemory", err));
    }
    let mapping = if placement != Placement::Device {
        // SAFETY: host-visible memory, mapped once; `vkFreeMemory` unmaps it when wgpu frees it.
        match unsafe { raw.map_memory(memory, 0, vk::WHOLE_SIZE, vk::MemoryMapFlags::empty()) } {
            Ok(ptr) => Some(HostMapping {
                ptr: ptr.cast(),
                len: size as usize,
                #[cfg(feature = "gpu-dmabuf")]
                dmabuf: None,
            }),
            Err(err) => {
                cleanup();
                return Err(failed("vkMapMemory", err));
            }
        }
    } else {
        None
    };
    // SAFETY: wgpu takes ownership of `buffer` and `memory` (destroyed and freed on drop).
    Ok((
        unsafe { hal_vk::Buffer::from_raw_managed(buffer, memory, 0, req.size) },
        mapping,
    ))
}

/// Whether the device has a host-visible, host-cached memory type (not on v3dv).
#[cfg(feature = "gpu-dmabuf")]
fn has_host_cached_memory(gpu: &GpuContextHandle) -> bool {
    let Some((device, _)) = gpu.wgpu() else {
        return false;
    };
    // SAFETY: a query on wgpu's live device.
    unsafe {
        let Some(hal) = device.as_hal::<Vulkan>() else {
            return false;
        };
        let props = hal
            .shared_instance()
            .raw_instance()
            .get_physical_device_memory_properties(hal.raw_physical_device());
        let want = vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_CACHED;
        props.memory_types[..props.memory_type_count as usize]
            .iter()
            .any(|t| t.property_flags.contains(want))
    }
}

/// Host-cached storage from a system dma-heap buffer: imported as the GPU's storage, mapped
/// (cached) for the CPU.
#[cfg(feature = "gpu-dmabuf")]
fn dma_heap_storage(
    gpu: &GpuContextHandle,
    size: u64,
    label: Option<&str>,
) -> Result<GpuStorage, GpuError> {
    use lemnos_linux::dma::DmaHeap;
    use std::os::fd::AsFd;
    let failed = |what: &str| GpuError::Internal(format!("dma-heap storage: {what}"));
    let need = GpuStorage::dmabuf_import_len(gpu, size)
        .map_err(|err| failed(&err.to_string()))?
        .next_multiple_of(4096);
    let dmabuf = DmaHeap::open("system")
        .and_then(|heap| heap.allocate(need as usize))
        .map_err(|err| failed(&format!("DMA_HEAP_IOCTL_ALLOC: {err}")))?;
    let mut map = dmabuf
        .map()
        .map_err(|err| failed(&format!("mmap: {err}")))?;
    let fd = dmabuf
        .as_fd()
        .try_clone_to_owned()
        .map_err(|err| failed(&err.to_string()))?;
    let mapping = HostMapping {
        ptr: map.as_mut_slice().as_mut_ptr(),
        len: size as usize,
        dmabuf: Some((dmabuf, map)),
    };
    let mut storage =
        GpuStorage::import_dmabuf(gpu, fd, 0, size, None, std::time::Duration::ZERO, label)
            .map_err(|err| failed(&err.to_string()))?;
    storage.host = Some(Arc::new(mapping));
    Ok(storage)
}

#[cfg(all(test, feature = "gpu-dmabuf"))]
#[allow(clippy::print_stderr)] // skip reasons
mod tests {
    use crate::{GpuBackendKind, GpuKernel, GpuKernels, GpuOptions, GpuStorage, select_backend};

    /// The dma-heap form of host-cached storage (what v3dv gets), forced: the GPU writes it and
    /// the CPU reads the result, the CPU writes it and the GPU reads it.
    #[test]
    fn dma_heap_host_cached_storage_round_trips() {
        let Ok(ctx) = select_backend(&GpuOptions {
            preferred_backend: Some(GpuBackendKind::Wgpu),
            ..Default::default()
        }) else {
            return eprintln!("skipping: no GPU");
        };
        let size = 256 << 10;
        let storage = match super::dma_heap_storage(&ctx, size, Some("readback")) {
            Ok(storage) => storage,
            Err(err) => return eprintln!("skipping: {err}"),
        };
        assert!(storage.is_host_mapped());
        let kernels = GpuKernels::new(&ctx).unwrap();
        let data: Vec<u8> = (0..size as usize).map(|i| (i * 13 + 5) as u8).collect();
        let src = GpuStorage::with_data(&ctx, &data, None).unwrap();
        kernels
            .run(GpuKernel::Copy, &src, &storage, size)
            .unwrap()
            .wait();
        let mut out = vec![0u8; size as usize];
        storage.read_mapped(0, &mut out).unwrap();
        assert_eq!(out, data, "GPU write, CPU read");
        let back: Vec<u8> = data.iter().map(|b| !b).collect();
        storage.write_mapped(0, &back).unwrap();
        let dst = GpuStorage::new(&ctx, size, None).unwrap();
        kernels
            .run(GpuKernel::Copy, &storage, &dst, size)
            .unwrap()
            .wait();
        dst.read_into(0, &mut out).unwrap();
        assert_eq!(out, back, "CPU write, GPU read");
        assert_eq!(storage.staging_buffers_created(), 0);
    }
}

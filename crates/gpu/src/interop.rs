//! Buffers that CUDA and Vulkan share (Linux, `gpu-vulkan-raw` and `gpu-cuda`).
//!
//! wgpu does not export the memory of its own buffers, so a shared buffer is allocated on the
//! wgpu context's Vulkan device with device-local memory that is exportable as an opaque fd
//! (`VK_KHR_external_memory_fd`). The same allocation is imported into CUDA
//! ([`CudaBackend::import_opaque_fd`]), where a kernel works on it in place: CUDA and Vulkan
//! address one memory, and nothing is copied at the handoff.
//!
//! The host reaches the shared memory through a host-visible staging buffer and a Vulkan copy
//! ([`SharedBuffer::write`], [`SharedBuffer::read`]). That is the only copy, and it is on the
//! host's side of the handoff, not CUDA's. Host-visible memory cannot be the shared memory: on
//! NVIDIA a CUDA kernel kept reading the contents of its first host write after later host
//! writes, so the host never writes the exported memory directly.
//!
//! # Handoff
//!
//! Two ways to order the handoff. By the host: [`SharedBuffer::write`] returns after the copy has
//! finished, before CUDA work is queued, and a read follows a
//! [`CudaFence::wait`](crate::CudaFence::wait) on the CUDA work. On the GPU: a [`SharedSemaphore`]
//! (a Vulkan timeline semaphore exported as an opaque fd) is imported into CUDA as a
//! [`CudaSemaphore`](crate::CudaSemaphore); Vulkan submits set and wait its values
//! ([`RawSubmitter::submit_synced`](crate::raw_vulkan::RawSubmitter::submit_synced)) and CUDA waits and signals
//! them on its stream, so the host does not wait at the boundary.
//!
//! [`CudaBackend::import_opaque_fd`]: crate::CudaBackend::import_opaque_fd

use std::os::fd::{FromRawFd, OwnedFd};

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;

use crate::raw_vulkan::RawVulkan;
use crate::{GpuContextHandle, GpuError};

/// How the shared memory is exported: an opaque fd (CUDA's `OPAQUE_FD`), or a dma-buf (the Linux
/// dmabuf every other API can import).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareKind {
    OpaqueFd,
    DmaBuf,
}

impl ShareKind {
    fn handle_type(self) -> vk::ExternalMemoryHandleTypeFlags {
        match self {
            ShareKind::OpaqueFd => vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD,
            ShareKind::DmaBuf => vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
        }
    }
}

/// A device-local, exportable buffer of one wgpu context's Vulkan device, with a host-visible
/// staging buffer for the host's writes and reads.
///
/// It keeps the context alive, so the device outlives the buffers. Drop any CUDA import of it
/// (see [`CudaImport`](crate::CudaImport)) first: the import refers to this memory.
pub struct SharedBuffer {
    raw: RawVulkan,
    shared: Owned,
    staging: Owned,
    kind: ShareKind,
    device_uuid: [u8; 16],
    /// Declared last: the device is destroyed only after the buffers' objects are.
    ctx: GpuContextHandle,
}

impl std::fmt::Debug for SharedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBuffer")
            .field("bytes", &self.shared.size)
            .finish_non_exhaustive()
    }
}

/// A buffer and its memory, freed on drop (after unmapping, if mapped).
struct Owned {
    device: ash::Device,
    buffer: vk::Buffer,
    memory: vk::DeviceMemory,
    size: u64,
    mapped: *mut u8,
}

impl Drop for Owned {
    fn drop(&mut self) {
        // SAFETY: each object was created on `self.device` by `allocate` and is destroyed once.
        // The CUDA import of an exported memory is the caller's to drop before this.
        unsafe {
            if !self.mapped.is_null() {
                self.device.unmap_memory(self.memory);
            }
            self.device.destroy_buffer(self.buffer, None);
            self.device.free_memory(self.memory, None);
        }
    }
}

fn vk_err(what: &str, err: vk::Result) -> GpuError {
    GpuError::Internal(format!("{what}: {err}"))
}

/// A buffer of `bytes` with memory of the type that has `want` (any allowed type when none does),
/// exported as an opaque fd when `export`, and mapped when the type is host-visible.
fn allocate(
    raw: &RawVulkan,
    bytes: u64,
    usage: vk::BufferUsageFlags,
    want: vk::MemoryPropertyFlags,
    export: Option<ShareKind>,
) -> Result<Owned, GpuError> {
    let device = &raw.device;
    let export_kind = export.map(ShareKind::handle_type);
    let mut external = vk::ExternalMemoryBufferCreateInfo::default()
        .handle_types(export_kind.unwrap_or(vk::ExternalMemoryHandleTypeFlags::empty()));
    let info = vk::BufferCreateInfo::default()
        .size(bytes)
        .usage(usage)
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    let info = if export.is_some() {
        info.push_next(&mut external)
    } else {
        info
    };
    // SAFETY: valid create info on the wgpu device.
    let buffer =
        unsafe { device.create_buffer(&info, None) }.map_err(|e| vk_err("vkCreateBuffer", e))?;
    // SAFETY: `buffer` is valid.
    let req = unsafe { device.get_buffer_memory_requirements(buffer) };
    // SAFETY: a valid physical device of the instance, queried only.
    let props = unsafe {
        raw.instance
            .get_physical_device_memory_properties(raw.physical_device)
    };
    let allowed = |i: u32| req.memory_type_bits & (1u32 << i) != 0;
    let flags = |i: u32| props.memory_types[i as usize].property_flags;
    let index = (0..props.memory_type_count)
        .filter(|&i| allowed(i))
        .find(|&i| flags(i).contains(want))
        .or_else(|| (0..props.memory_type_count).find(|&i| allowed(i)));
    let Some(index) = index else {
        // SAFETY: created above, unused.
        unsafe { device.destroy_buffer(buffer, None) };
        return Err(GpuError::AllocationFailed);
    };

    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let mut export_info = vk::ExportMemoryAllocateInfo::default()
        .handle_types(export_kind.unwrap_or(vk::ExternalMemoryHandleTypeFlags::empty()));
    let alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(req.size)
        .memory_type_index(index)
        .push_next(&mut dedicated);
    let alloc = if export.is_some() {
        alloc.push_next(&mut export_info)
    } else {
        alloc
    };
    // SAFETY: valid allocate info; freed by `Owned::drop` (or on the paths below).
    let memory = match unsafe { device.allocate_memory(&alloc, None) } {
        Ok(memory) => memory,
        Err(e) => {
            // SAFETY: created above, unused.
            unsafe { device.destroy_buffer(buffer, None) };
            return Err(vk_err("vkAllocateMemory", e));
        }
    };
    let mut owned = Owned {
        device: device.clone(),
        buffer,
        memory,
        size: req.size,
        mapped: std::ptr::null_mut(),
    };
    // SAFETY: fresh buffer and memory of matching requirements, bound at offset 0.
    unsafe { device.bind_buffer_memory(buffer, memory, 0) }
        .map_err(|e| vk_err("vkBindBufferMemory", e))?;
    if flags(index).contains(vk::MemoryPropertyFlags::HOST_VISIBLE) {
        // SAFETY: host-visible memory of `req.size` bytes, mapped whole and kept mapped.
        owned.mapped =
            unsafe { device.map_memory(memory, 0, req.size, vk::MemoryMapFlags::empty()) }
                .map_err(|e| vk_err("vkMapMemory", e))?
                .cast::<u8>();
    }
    Ok(owned)
}

impl SharedBuffer {
    /// A shared buffer of `bytes` on `ctx`'s Vulkan device, and its staging buffer.
    ///
    /// Fails with `Unsupported` when `ctx` is not wgpu on Vulkan, or when the device lacks the
    /// external-memory extension (it is enabled by the `gpu-dmabuf` device request).
    pub fn create(ctx: &GpuContextHandle, bytes: u64) -> Result<Self, GpuError> {
        Self::create_for(ctx, bytes, ShareKind::OpaqueFd)
    }

    /// [`SharedBuffer::create`] with the memory exported as `kind`.
    pub fn create_for(
        ctx: &GpuContextHandle,
        bytes: u64,
        kind: ShareKind,
    ) -> Result<Self, GpuError> {
        if bytes == 0 {
            return Err(GpuError::InvalidInput("zero-sized shared buffer".into()));
        }
        // SAFETY: the handles are only read here; the buffers are destroyed before `ctx` drops.
        let raw = unsafe { ctx.raw_vulkan() }.ok_or(GpuError::Unsupported)?;
        let usage = vk::BufferUsageFlags::STORAGE_BUFFER
            | vk::BufferUsageFlags::TRANSFER_SRC
            | vk::BufferUsageFlags::TRANSFER_DST;
        let shared = allocate(
            &raw,
            bytes,
            usage,
            vk::MemoryPropertyFlags::DEVICE_LOCAL,
            Some(kind),
        )?;
        let staging = allocate(
            &raw,
            bytes,
            vk::BufferUsageFlags::TRANSFER_SRC | vk::BufferUsageFlags::TRANSFER_DST,
            vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_COHERENT,
            None,
        )?;
        if staging.mapped.is_null() {
            return Err(GpuError::AllocationFailed);
        }
        let device_uuid = physical_device_uuid(&raw.instance, raw.physical_device);
        Ok(Self {
            raw,
            shared,
            staging,
            kind,
            device_uuid,
            ctx: ctx.clone(),
        })
    }

    /// The buffer's size in bytes.
    pub fn bytes(&self) -> u64 {
        self.shared.size
    }

    /// The UUID of the Vulkan physical device the memory lives on (compare with
    /// [`CudaBackend::device_uuid`](crate::CudaBackend::device_uuid)).
    pub fn device_uuid(&self) -> [u8; 16] {
        self.device_uuid
    }

    /// A descriptor of the shared memory (`vkGetMemoryFdKHR`, of the buffer's [`ShareKind`]): a new
    /// one each call, owned by the caller. An opaque fd imports into CUDA with
    /// [`CudaBackend::import_opaque_fd`](crate::CudaBackend::import_opaque_fd).
    pub fn export_fd(&self) -> Result<OwnedFd, GpuError> {
        let fd_api =
            ash::khr::external_memory_fd::Device::new(&self.raw.instance, &self.raw.device);
        let info = vk::MemoryGetFdInfoKHR::default()
            .memory(self.shared.memory)
            .handle_type(self.kind.handle_type());
        // SAFETY: `memory` is a live export-capable allocation of this device.
        let fd =
            unsafe { fd_api.get_memory_fd(&info) }.map_err(|e| vk_err("vkGetMemoryFdKHR", e))?;
        // SAFETY: Vulkan returned a new descriptor that nothing else owns.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Write `data` at the start of the shared memory: host bytes into staging, then a Vulkan
    /// copy into the shared buffer, complete when this returns.
    ///
    /// # Panics
    /// When `data` is longer than the buffer.
    pub fn write(&self, data: &[u8]) -> Result<(), GpuError> {
        assert!(
            data.len() as u64 <= self.bytes(),
            "write past the shared buffer"
        );
        // SAFETY: the staging mapping covers `bytes()` bytes, at least `data.len()`.
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), self.staging.mapped, data.len()) };
        self.copy(self.staging.buffer, self.shared.buffer, data.len() as u64)
    }

    /// The shared memory's bytes: a Vulkan copy into staging, then the host's read of it.
    pub fn read(&self) -> Result<Vec<u8>, GpuError> {
        let len = self.bytes();
        self.copy(self.shared.buffer, self.staging.buffer, len)?;
        let mut out = vec![0u8; len as usize];
        // SAFETY: the staging mapping covers `len` bytes; `out` has exactly that many.
        unsafe { std::ptr::copy_nonoverlapping(self.staging.mapped, out.as_mut_ptr(), out.len()) };
        Ok(out)
    }

    /// Copy `len` bytes between two buffers of the device, and wait for the copy.
    fn copy(&self, from: vk::Buffer, to: vk::Buffer, len: u64) -> Result<(), GpuError> {
        // SAFETY: the recording only copies between live buffers of this device, and the ticket
        // is waited on before this returns, so both outlive the copy.
        let ticket = unsafe {
            self.ctx.submit_raw(|device, cmd| {
                let region = vk::BufferCopy::default().size(len);
                device.cmd_copy_buffer(cmd, from, to, &[region]);
            })
        }?;
        ticket.wait();
        Ok(())
    }
}

/// A Vulkan timeline semaphore of one wgpu context's device, exported as an opaque fd for CUDA
/// ([`CudaBackend::import_timeline_semaphore`](crate::CudaBackend::import_timeline_semaphore)).
///
/// Its values are set by [`RawSubmitter::submit_synced`](crate::raw_vulkan::RawSubmitter::submit_synced) and by
/// CUDA signals. Drop the CUDA import before the semaphore; the context outlives both.
pub struct SharedSemaphore {
    semaphore: vk::Semaphore,
    fd_api: khr::external_semaphore_fd::Device,
    device: ash::Device,
    /// Declared last: the device is destroyed only after the semaphore is.
    _ctx: GpuContextHandle,
}

impl std::fmt::Debug for SharedSemaphore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSemaphore").finish_non_exhaustive()
    }
}

impl SharedSemaphore {
    /// A timeline semaphore on `ctx`'s device, exportable as an opaque fd, at value 0.
    ///
    /// Fails with `Unsupported` when `ctx` is not wgpu on Vulkan, or when the device does not
    /// have `VK_KHR_external_semaphore_fd` (the `gpu-dmabuf` device request enables it).
    pub fn create(ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        let (wgpu_device, _) = ctx.wgpu().ok_or(GpuError::Unsupported)?;
        // SAFETY: only reads the device's enabled extensions.
        let enabled = unsafe { wgpu_device.as_hal::<Vulkan>() }
            .ok_or(GpuError::Unsupported)?
            .enabled_device_extensions()
            .contains(&khr::external_semaphore_fd::NAME);
        if !enabled {
            return Err(GpuError::Unsupported);
        }
        // SAFETY: only reads the handles; the semaphore is destroyed before the context.
        let raw = unsafe { ctx.raw_vulkan() }.ok_or(GpuError::Unsupported)?;
        let mut kind = vk::SemaphoreTypeCreateInfo::default()
            .semaphore_type(vk::SemaphoreType::TIMELINE)
            .initial_value(0);
        let mut export = vk::ExportSemaphoreCreateInfo::default()
            .handle_types(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
        let info = vk::SemaphoreCreateInfo::default()
            .push_next(&mut kind)
            .push_next(&mut export);
        // SAFETY: valid create info on the context's live device.
        let semaphore = unsafe { raw.device.create_semaphore(&info, None) }
            .map_err(|e| vk_err("vkCreateSemaphore (timeline, opaque fd)", e))?;
        let fd_api = khr::external_semaphore_fd::Device::new(&raw.instance, &raw.device);
        Ok(Self {
            semaphore,
            fd_api,
            device: raw.device.clone(),
            _ctx: ctx.clone(),
        })
    }

    /// The Vulkan semaphore, for [`RawSubmitter::submit_synced`](crate::raw_vulkan::RawSubmitter::submit_synced).
    pub fn raw_handle(&self) -> vk::Semaphore {
        self.semaphore
    }

    /// A new opaque fd of the semaphore (`vkGetSemaphoreFdKHR`), owned by the caller. It imports
    /// into CUDA with [`CudaBackend::import_timeline_semaphore`](crate::CudaBackend::import_timeline_semaphore).
    pub fn export_fd(&self) -> Result<OwnedFd, GpuError> {
        let info = vk::SemaphoreGetFdInfoKHR::default()
            .semaphore(self.semaphore)
            .handle_type(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
        // SAFETY: the semaphore is live and exportable (created with the opaque fd handle type).
        let fd = unsafe { self.fd_api.get_semaphore_fd(&info) }
            .map_err(|e| vk_err("vkGetSemaphoreFdKHR", e))?;
        // SAFETY: Vulkan returned a new descriptor that nothing else owns.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

impl Drop for SharedSemaphore {
    fn drop(&mut self) {
        // SAFETY: created on `self.device` by `create` and destroyed once. The CUDA import is the
        // caller's to drop first (see the type docs).
        unsafe { self.device.destroy_semaphore(self.semaphore, None) };
    }
}

/// The physical device's `deviceUUID` (`VkPhysicalDeviceIDProperties`).
fn physical_device_uuid(instance: &ash::Instance, pd: vk::PhysicalDevice) -> [u8; 16] {
    let mut id = vk::PhysicalDeviceIDProperties::default();
    {
        let mut props = vk::PhysicalDeviceProperties2::default().push_next(&mut id);
        // SAFETY: a valid physical device of `instance`, queried only.
        unsafe { instance.get_physical_device_properties2(pd, &mut props) };
    }
    id.device_uuid
}

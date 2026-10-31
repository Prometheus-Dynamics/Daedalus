//! Import a `LINEAR` dmabuf (a camera frame) as a [`GpuStorage`] without copying it.

use std::os::fd::{AsFd, AsRawFd, IntoRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use ash::{khr, vk};
use wgpu::hal::{api::Vulkan, vulkan as hal_vk};

use crate::{ExternalImportError, GpuContextHandle};

use super::GpuStorage;

impl GpuStorage {
    /// Import `len` bytes of the dmabuf `fd`, starting at `offset`, as a storage buffer on
    /// `gpu`'s device, without a copy. A compute or fragment kernel then reads the frame's packed
    /// rows directly (pass the stride in a parameter buffer).
    ///
    /// - The dmabuf must extend past the range by the driver's buffer padding. On v3dv that is
    ///   256 bytes (Styx pads its dma-heap frames for this). `InvalidDescriptor` names the
    ///   size the import needs.
    /// - `offset` must be a multiple of the buffer's memory alignment and `len` of 4.
    /// - `acquire_fence` (a `sync_file` from the producer) is waited on the CPU, for up to
    ///   `timeout`, before the import; without one the frame must already be complete.
    /// - The buffer keeps the dmabuf memory alive until it is dropped. The producer must not
    ///   reuse the frame while GPU work reading it is in flight.
    ///
    /// `Unsupported` when the device cannot import dmabuf memory into a storage buffer (not
    /// Vulkan, or the driver does not report it importable).
    pub fn import_dmabuf(
        gpu: &GpuContextHandle,
        fd: OwnedFd,
        offset: u64,
        len: u64,
        acquire_fence: Option<OwnedFd>,
        timeout: Duration,
        label: Option<&str>,
    ) -> Result<Self, ExternalImportError> {
        let unsupported = |reason: String| ExternalImportError::Unsupported { reason };
        let (device, queue) = gpu
            .wgpu()
            .ok_or_else(|| unsupported("not a wgpu backend".into()))?;
        if len == 0 || !len.is_multiple_of(4) {
            return Err(ExternalImportError::invalid(format!(
                "import length {len} must be a non-zero multiple of 4"
            )));
        }
        if let Some(fence) = &acquire_fence {
            crate::external::wait_sync_file(fence.as_fd(), timeout)?;
        }
        let dmabuf_len = dmabuf_len(&fd)?;
        // SAFETY: the hal device outlives this block; the buffer and memory created here go to
        // wgpu (`from_raw_managed`), which destroys and frees them when the buffer drops.
        let hal_buffer = unsafe {
            let hal = device
                .as_hal::<Vulkan>()
                .ok_or_else(|| unsupported("the wgpu device is not using Vulkan".into()))?;
            importable(&hal)?;
            import(&hal, fd, offset, len, dmabuf_len, label)?
        };
        // SAFETY: `hal_buffer` was created on this device with storage and transfer usages
        // matching the descriptor, and its memory is bound.
        let buffer = unsafe {
            device.create_buffer_from_hal::<Vulkan>(
                hal_buffer,
                &wgpu::BufferDescriptor {
                    label,
                    size: len,
                    usage: wgpu::BufferUsages::STORAGE
                        | wgpu::BufferUsages::COPY_SRC
                        | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                },
            )
        };
        Ok(Self {
            buffer: Arc::new(buffer),
            size: len,
            label: label.map(str::to_owned),
            device: device.clone(),
            queue: queue.clone(),
            device_key: super::super::device_key(device),
            staging: Arc::default(),
        })
    }
}

/// Size of a dmabuf (`lseek(SEEK_END)`); the offset is reset to 0.
fn dmabuf_len(fd: &OwnedFd) -> Result<u64, ExternalImportError> {
    // SAFETY: plain lseek calls on a valid descriptor.
    let len = unsafe { libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_END) };
    // SAFETY: as above.
    if len < 0 || unsafe { libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_SET) } < 0 {
        return Err(ExternalImportError::invalid(format!(
            "cannot determine dmabuf size (lseek): {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(len as u64)
}

/// Whether the driver imports dmabuf memory into storage buffers.
fn importable(hal: &hal_vk::Device) -> Result<(), ExternalImportError> {
    let instance = hal.shared_instance().raw_instance();
    let info = vk::PhysicalDeviceExternalBufferInfo::default()
        .usage(vk::BufferUsageFlags::STORAGE_BUFFER | vk::BufferUsageFlags::TRANSFER_SRC)
        .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let mut props = vk::ExternalBufferProperties::default();
    // SAFETY: valid physical device; query only.
    unsafe {
        instance.get_physical_device_external_buffer_properties(
            hal.raw_physical_device(),
            &info,
            &mut props,
        )
    };
    let features = props.external_memory_properties.external_memory_features;
    if features.contains(vk::ExternalMemoryFeatureFlags::IMPORTABLE) {
        Ok(())
    } else {
        Err(ExternalImportError::Unsupported {
            reason: format!("the driver cannot import dmabuf storage buffers ({features:?})"),
        })
    }
}

/// Create the `VkBuffer`, import the dmabuf as its (dedicated) memory and bind it.
///
/// # Safety
/// `hal` must be the device the returned buffer is handed to.
unsafe fn import(
    hal: &hal_vk::Device,
    fd: OwnedFd,
    offset: u64,
    len: u64,
    dmabuf_len: u64,
    _label: Option<&str>,
) -> Result<hal_vk::Buffer, ExternalImportError> {
    let instance = hal.shared_instance().raw_instance();
    let raw = hal.raw_device();
    let handle_type = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
    let failed = |what: &str, err: vk::Result| ExternalImportError::ImportFailed {
        reason: format!("{what}: {err}"),
    };
    let mut external = vk::ExternalMemoryBufferCreateInfo::default().handle_types(handle_type);
    let info = vk::BufferCreateInfo::default()
        .size(len)
        .usage(
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
        )
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .push_next(&mut external);
    // SAFETY: valid device and create info.
    let buffer =
        unsafe { raw.create_buffer(&info, None) }.map_err(|e| failed("vkCreateBuffer", e))?;
    let destroy = |memory: Option<vk::DeviceMemory>| {
        // SAFETY: both were created above on `raw` and are unused.
        unsafe {
            if let Some(memory) = memory {
                raw.free_memory(memory, None);
            }
            raw.destroy_buffer(buffer, None);
        }
    };
    // SAFETY: `buffer` is a valid buffer of `raw`.
    let req = unsafe { raw.get_buffer_memory_requirements(buffer) };
    if !offset.is_multiple_of(req.alignment.max(1)) || offset + req.size > dmabuf_len {
        destroy(None);
        return Err(ExternalImportError::invalid(format!(
            "a {len}-byte buffer at offset {offset} needs {} bytes of dmabuf memory (alignment {}), \
             the dmabuf has {dmabuf_len}: allocate frames with {} bytes of slack",
            offset + req.size,
            req.alignment,
            req.size.saturating_sub(len)
        )));
    }
    let fd_api = khr::external_memory_fd::Device::new(instance, raw);
    let mut fd_props = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: `fd` is a valid dmabuf; query only.
    if let Err(err) =
        unsafe { fd_api.get_memory_fd_properties(handle_type, fd.as_raw_fd(), &mut fd_props) }
    {
        destroy(None);
        return Err(failed("vkGetMemoryFdPropertiesKHR", err));
    }
    let types = req.memory_type_bits & fd_props.memory_type_bits;
    if types == 0 {
        destroy(None);
        return Err(ExternalImportError::ImportFailed {
            reason: format!(
                "no memory type fits both the buffer (0x{:x}) and the dmabuf (0x{:x})",
                req.memory_type_bits, fd_props.memory_type_bits
            ),
        });
    }
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(handle_type)
        .fd(fd.as_raw_fd());
    let alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(dmabuf_len)
        .memory_type_index(types.trailing_zeros())
        .push_next(&mut import)
        .push_next(&mut dedicated);
    // SAFETY: valid allocate info; on success Vulkan owns the fd.
    let memory = match unsafe { raw.allocate_memory(&alloc, None) } {
        Ok(memory) => {
            let _ = fd.into_raw_fd();
            memory
        }
        Err(err) => {
            destroy(None);
            return Err(failed("vkAllocateMemory (dmabuf import)", err));
        }
    };
    // SAFETY: fresh buffer and memory; `offset` is aligned and the range fits.
    if let Err(err) = unsafe { raw.bind_buffer_memory(buffer, memory, offset) } {
        destroy(Some(memory));
        return Err(failed("vkBindBufferMemory", err));
    }
    // SAFETY: wgpu takes ownership of `buffer` and `memory` (destroyed and freed on drop).
    Ok(unsafe { hal_vk::Buffer::from_raw_managed(buffer, memory, offset, req.size) })
}

#[cfg(test)]
#[allow(clippy::print_stderr)] // skip reasons
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::Duration;

    use crate::{ExternalImportError, GpuBackendKind, GpuOptions, GpuStorage, select_backend};

    /// `_IOWR('H', 0, struct dma_heap_allocation_data)`.
    const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800;

    #[repr(C)]
    struct Alloc {
        len: u64,
        fd: u32,
        fd_flags: u32,
        heap_flags: u64,
    }

    /// A dma-heap buffer of `len` bytes holding `fill(i)` at byte `i`.
    fn dmabuf(len: usize, fill: impl Fn(usize) -> u8) -> std::io::Result<OwnedFd> {
        let heap =
            std::env::var("DAEDALUS_DMA_HEAP").unwrap_or_else(|_| "/dev/dma_heap/system".into());
        let heap = std::fs::File::options().read(true).write(true).open(heap)?;
        let mut data = Alloc {
            len: len as u64,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // SAFETY: valid heap fd and a correctly laid out argument struct.
        if unsafe { libc::ioctl(heap.as_raw_fd(), DMA_HEAP_IOCTL_ALLOC as _, &mut data) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the kernel returned a fresh fd we own.
        let fd = unsafe { OwnedFd::from_raw_fd(data.fd as i32) };
        // SAFETY: mapping `len` bytes of a dmabuf we own; unmapped below.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the mapping is `len` bytes and ours.
        let bytes = unsafe { std::slice::from_raw_parts_mut(ptr.cast::<u8>(), len) };
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = fill(i);
        }
        // SAFETY: unmapping the mapping above.
        unsafe { libc::munmap(ptr, len) };
        Ok(fd)
    }

    #[test]
    #[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
    fn dmabuf_storage_import_reads_the_frame_in_place() {
        let ctx = select_backend(&GpuOptions {
            preferred_backend: Some(GpuBackendKind::Wgpu),
            ..Default::default()
        })
        .unwrap();
        if ctx.wgpu().is_none() {
            return eprintln!("skipping: no wgpu device");
        }
        // A 640x480 XRGB8888 frame (1228800 B, page-aligned like a camera's) and one page of
        // slack, as Styx allocates.
        let frame = 640 * 480 * 4;
        let pattern = |i: usize| (i % 251) as u8;
        let fd = match dmabuf(frame + 4096, pattern) {
            Ok(fd) => fd,
            Err(err) => return eprintln!("skipping: dma-heap alloc failed: {err}"),
        };
        let storage = match GpuStorage::import_dmabuf(
            &ctx,
            fd.try_clone().unwrap(),
            0,
            frame as u64,
            None,
            Duration::from_secs(1),
            Some("frame"),
        ) {
            Ok(storage) => storage,
            Err(ExternalImportError::Unsupported { reason }) => {
                return eprintln!("skipping: {reason}");
            }
            Err(err) => panic!("import: {err}"),
        };
        assert_eq!(storage.size(), frame as u64);
        let mut out = vec![0u8; 4096];
        storage.read_into(frame as u64 - 4096, &mut out).unwrap();
        let want: Vec<u8> = (frame - 4096..frame).map(pattern).collect();
        assert_eq!(out, want, "the buffer aliases the dmabuf");

        // A frame-sized dmabuf without slack is refused with the size it needs (on drivers that
        // pad buffers, as v3dv does), or imports when the driver needs no padding.
        let exact = dmabuf(frame, pattern).unwrap();
        match GpuStorage::import_dmabuf(
            &ctx,
            exact,
            0,
            frame as u64,
            None,
            Duration::from_secs(1),
            None,
        ) {
            Ok(storage) => assert_eq!(storage.size(), frame as u64),
            Err(ExternalImportError::InvalidDescriptor { reason }) => {
                assert!(reason.contains("slack"), "{reason}")
            }
            Err(err) => panic!("unexpected: {err}"),
        }
        // Lengths must be 4-byte multiples.
        assert!(matches!(
            GpuStorage::import_dmabuf(&ctx, fd, 0, 6, None, Duration::from_secs(1), None),
            Err(ExternalImportError::InvalidDescriptor { .. })
        ));
    }
}

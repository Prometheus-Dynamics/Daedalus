//! Import a `LINEAR` dmabuf (a camera frame) as a [`GpuStorage`] without copying it.

use std::os::fd::{AsFd, AsRawFd, IntoRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use ash::{khr, vk};
use wgpu::hal::{api::Vulkan, vulkan as hal_vk};

use crate::wgpu_backend::dmabuf::settle_refused_fd;
use crate::{ExternalImportError, GpuContextHandle};

use super::GpuStorage;

impl GpuStorage {
    /// Import `len` bytes of the dmabuf `fd`, starting at `offset`, as a storage buffer on
    /// `gpu`'s device, without a copy. A compute or fragment kernel then reads the frame's packed
    /// rows directly (pass the stride in a parameter buffer).
    ///
    /// - The dmabuf must extend past the range by the driver's buffer padding. On v3dv that is
    ///   256 bytes (Styx pads its dma-heap frames for this). [`GpuStorage::dmabuf_import_len`]
    ///   gives the size an import needs; `InvalidDescriptor` names it too.
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
            .wgpu_locked()
            .ok_or_else(|| unsupported("not a wgpu backend".into()))?;
        if len == 0 || !len.is_multiple_of(4) {
            return Err(ExternalImportError::invalid(format!(
                "import length {len} must be a non-zero multiple of 4"
            )));
        }
        if let Some(fence) = &acquire_fence {
            crate::external::wait_sync_file(fence.as_fd(), timeout)?;
        }
        let (fd, dmabuf_len) = dmabuf_len(fd)?;
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
            host: None,
        })
    }
}

impl GpuStorage {
    /// The dmabuf bytes an [`import_dmabuf`](Self::import_dmabuf) of `len` bytes at offset 0
    /// needs on `gpu`'s device: `len` plus the driver's buffer padding (256 B on v3dv; none on
    /// drivers whose requirement is the size itself). Vulkan binds a buffer only to memory
    /// covering its whole requirement, so a dmabuf shorter than this cannot be imported whole:
    /// size frame buffers to at least this (for V4L2, `VIDIOC_CREATE_BUFS` with a larger
    /// `sizeimage`). vb2 rounds buffers up to whole pages, which often leaves the room already.
    pub fn dmabuf_import_len(gpu: &GpuContextHandle, len: u64) -> Result<u64, ExternalImportError> {
        let unsupported = |reason: String| ExternalImportError::Unsupported { reason };
        let (device, _) = gpu
            .wgpu()
            .ok_or_else(|| unsupported("not a wgpu backend".into()))?;
        if len == 0 || !len.is_multiple_of(4) {
            return Err(ExternalImportError::invalid(format!(
                "import length {len} must be a non-zero multiple of 4"
            )));
        }
        // SAFETY: a buffer created only to read its requirements, destroyed here.
        unsafe {
            let hal = device
                .as_hal::<Vulkan>()
                .ok_or_else(|| unsupported("the wgpu device is not using Vulkan".into()))?;
            let raw = hal.raw_device();
            let mut external = vk::ExternalMemoryBufferCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
            let info = buffer_info(len).push_next(&mut external);
            let buffer = raw.create_buffer(&info, None).map_err(|err| {
                ExternalImportError::ImportFailed {
                    reason: format!("vkCreateBuffer: {err}"),
                }
            })?;
            let req = raw.get_buffer_memory_requirements(buffer);
            raw.destroy_buffer(buffer, None);
            Ok(req.size.max(len))
        }
    }
}

/// The create info of an imported storage buffer of `len` bytes.
fn buffer_info<'a>(len: u64) -> vk::BufferCreateInfo<'a> {
    vk::BufferCreateInfo::default()
        .size(len)
        .usage(
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
        )
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
}

/// Size of a dmabuf (`lseek(SEEK_END)`), with the descriptor handed back.
fn dmabuf_len(fd: OwnedFd) -> Result<(OwnedFd, u64), ExternalImportError> {
    let dmabuf = lemnos_linux::dma::DmaBuf::from_fd(fd).map_err(|err| {
        ExternalImportError::invalid(format!("cannot determine dmabuf size (lseek): {err}"))
    })?;
    let len = dmabuf.len() as u64;
    Ok((dmabuf.into_fd(), len))
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

/// Create the `VkBuffer`, import the dmabuf as its memory and bind it. The allocation is the
/// whole dmabuf; it is dedicated only when the driver requires that, since a dedicated allocation
/// must be exactly the buffer's size and bound at offset 0
/// (`VUID-VkMemoryDedicatedAllocateInfo-buffer-02965`).
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
    let info = buffer_info(len).push_next(&mut external);
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
    let mut dedicated_req = vk::MemoryDedicatedRequirements::default();
    let mut req2 = vk::MemoryRequirements2::default().push_next(&mut dedicated_req);
    // SAFETY: `buffer` is a valid buffer of `raw` (Vulkan 1.1, which wgpu requires).
    unsafe {
        raw.get_buffer_memory_requirements2(
            &vk::BufferMemoryRequirementsInfo2::default().buffer(buffer),
            &mut req2,
        );
    }
    let req = req2.memory_requirements;
    let requires_dedicated = dedicated_req.requires_dedicated_allocation == vk::TRUE;
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
    if requires_dedicated && (offset != 0 || req.size != dmabuf_len) {
        destroy(None);
        return Err(ExternalImportError::invalid(format!(
            "the driver requires a dedicated allocation, which must be the whole dmabuf: a \
             {len}-byte buffer at offset {offset} needs {} bytes, the dmabuf has {dmabuf_len}",
            req.size
        )));
    }
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(handle_type)
        .fd(fd.as_raw_fd());
    let mut alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(dmabuf_len)
        .memory_type_index(types.trailing_zeros())
        .push_next(&mut import);
    if requires_dedicated {
        alloc = alloc.push_next(&mut dedicated);
    }
    // SAFETY: valid allocate info; on success Vulkan owns the fd.
    let memory = match unsafe { raw.allocate_memory(&alloc, None) } {
        Ok(memory) => {
            let _ = fd.into_raw_fd();
            memory
        }
        Err(err) => {
            destroy(None);
            // SAFETY: the driver refused the import, so the fd is still ours to settle.
            settle_refused_fd(instance, hal.raw_physical_device(), fd);
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
    use std::io::{Seek, SeekFrom};
    use std::os::fd::OwnedFd;
    use std::time::Duration;

    use crate::{ExternalImportError, GpuBackendKind, GpuOptions, GpuStorage, select_backend};

    /// A dma-heap buffer of `len` bytes holding `fill(i)` at byte `i`.
    fn dmabuf(len: usize, fill: impl Fn(usize) -> u8) -> std::io::Result<OwnedFd> {
        let heap =
            std::env::var("DAEDALUS_DMA_HEAP").unwrap_or_else(|_| "/dev/dma_heap/system".into());
        let buf = lemnos_linux::dma::DmaHeap::open_path(heap)?.allocate(len)?;
        let mut map = buf.map()?;
        for (i, b) in map.as_mut_slice().iter_mut().enumerate() {
            *b = fill(i);
        }
        drop(map);
        Ok(buf.into_fd())
    }

    /// The driver refused the import of a `len`-byte dmabuf: skip, with the reason. NVIDIA's
    /// proprietary driver (595.104.02) refuses some dma-heap fds with `ERROR_OUT_OF_DEVICE_MEMORY`.
    /// The refusal must leave the caller's fd open and naming the same dmabuf, which is checked.
    fn skip_refused(fd: &OwnedFd, len: u64, reason: &str) {
        eprintln!("skipping: the driver refused the dmabuf import: {reason}");
        let clone = fd
            .try_clone()
            .expect("the refused import closed the caller's fd");
        let mut file = std::fs::File::from(clone);
        assert_eq!(
            file.seek(SeekFrom::End(0)).unwrap(),
            len,
            "the fd still names the dmabuf"
        );
    }

    /// The driver's refusal of a dmabuf import (`vkAllocateMemory`), as opposed to a bad request.
    fn refused(err: &ExternalImportError) -> bool {
        matches!(err, ExternalImportError::ImportFailed { reason } if reason.starts_with("vkAllocateMemory"))
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
            Err(err) if refused(&err) => {
                return skip_refused(&fd, frame as u64 + 4096, &err.to_string());
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
            exact.try_clone().unwrap(),
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
            Err(err) if refused(&err) => {
                return skip_refused(&exact, frame as u64, &err.to_string());
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

//! Storage buffers in memory wgpu wouldn't pick: mapped for the host for the buffer's whole life,
//! or device memory with no zero-fill. And buffers whose memory is a dma-buf.
//!
//! wgpu frees the memory when the buffer drops, and treats the contents as written, so it never
//! clears them.

use std::os::fd::{AsRawFd, BorrowedFd, IntoRawFd, OwnedFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use crate::Error;
use crate::device::Raw;

/// Where a buffer's memory comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Placement {
    /// Device memory, not mapped and not cleared. For buffers their producer writes in full.
    Device,
    /// Device memory, not cleared, and mapped where the device's memory is the host's too: on
    /// a GPU that shares memory with the CPU, every device-local type is host-visible. On any
    /// other GPU it is [`Placement::Device`].
    DeviceReachable,
    /// Host-visible memory, device-local when there is such a type. On a GPU that shares memory
    /// with the CPU, uploads and readbacks are then plain copies.
    HostMapped,
    /// Host-visible and cached, for memory the host reads a lot. Falls back to host-mapped.
    HostCached,
}

/// A buffer's memory, mapped for as long as the buffer lives.
pub struct HostMapping {
    ptr: *mut u8,
    len: usize,
    /// One host access at a time.
    access: Mutex<()>,
    /// How many [`Lent`]s are alive. The host doesn't write while any is.
    lent: AtomicUsize,
    /// The mapping is the buffer's memory.
    _buffer: wgpu::Buffer,
}

// SAFETY: the pointer is to memory that lives as long as `_buffer`, and every access through it
// is a bounds-checked copy made under `access`, or a read through a `Lent`, during which the
// host writes nothing.
unsafe impl Send for HostMapping {}
// SAFETY: as above.
unsafe impl Sync for HostMapping {}

impl HostMapping {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn range(&self, offset: u64, len: usize) -> Result<usize, Error> {
        let start = usize::try_from(offset).map_err(|_| Error::Range)?;
        match start.checked_add(len) {
            Some(end) if end <= self.len => Ok(start),
            _ => Err(Error::Range),
        }
    }

    /// Copies `bytes` into the buffer at `offset`. The GPU sees them with no upload.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        let start = self.range(offset, bytes.len())?;
        let _one = self.access.lock().unwrap_or_else(PoisonError::into_inner);
        if self.lent.load(Ordering::Acquire) != 0 {
            return Err(Error::Lent);
        }
        // SAFETY: `start + bytes.len()` is within the mapping, which is live and can't overlap
        // a Rust slice.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(start), bytes.len()) };
        Ok(())
    }

    /// Writes `len` zero bytes at `offset`.
    pub fn zero(&self, offset: u64, len: usize) -> Result<(), Error> {
        let start = self.range(offset, len)?;
        let _one = self.access.lock().unwrap_or_else(PoisonError::into_inner);
        if self.lent.load(Ordering::Acquire) != 0 {
            return Err(Error::Lent);
        }
        // SAFETY: `start + len` is within the mapping, which is live.
        unsafe { std::ptr::write_bytes(self.ptr.add(start), 0, len) };
        Ok(())
    }

    /// Copies the buffer from `offset` into `out`. GPU work that wrote it must have finished.
    pub fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        let start = self.range(offset, out.len())?;
        let _one = self.access.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: as in `write`.
        unsafe { std::ptr::copy_nonoverlapping(self.ptr.add(start), out.as_mut_ptr(), out.len()) };
        Ok(())
    }

    /// The mapping's bytes in place, until the [`Lent`] drops. The host's writes refuse while
    /// any is alive. The holder sees to it that no device work writes the buffer meanwhile: GPU
    /// work that wrote it must have finished, and the buffer isn't given to the device again
    /// until the `Lent` is gone.
    pub fn lend(self: &Arc<Self>) -> Lent {
        let _one = self.access.lock().unwrap_or_else(PoisonError::into_inner);
        self.lent.fetch_add(1, Ordering::AcqRel);
        Lent {
            mapping: Arc::clone(self),
        }
    }
}

/// A [`HostMapping`]'s bytes, read in place.
pub struct Lent {
    mapping: Arc<HostMapping>,
}

impl std::ops::Deref for Lent {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let map = &self.mapping;
        // The host writes nothing while a `Lent` is alive (`write` and `zero` refuse). A device
        // write meanwhile breaks `lend`'s terms, and even then only changes bytes under the
        // reader, as with `read`: every byte value is a `u8`.
        // SAFETY: the pointer and length are the live mapping's, which `mapping` keeps alive.
        unsafe { std::slice::from_raw_parts(map.ptr, map.len) }
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        self.mapping.lent.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for Lent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lent")
            .field("len", &self.mapping.len)
            .finish()
    }
}

fn descriptor(size: u64, label: Option<&str>) -> wgpu::BufferDescriptor<'_> {
    wgpu::BufferDescriptor {
        label,
        size,
        usage: wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    }
}

fn buffer_info(size: u64) -> vk::BufferCreateInfo<'static> {
    vk::BufferCreateInfo::default()
        .size(size)
        .usage(
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::UNIFORM_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
        )
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
}

/// A storage buffer of `size` bytes in `placement`, and its mapping unless it's device memory.
/// `size` is a non-zero multiple of 4.
pub fn storage_buffer(
    device: &wgpu::Device,
    size: u64,
    placement: Placement,
    label: Option<&str>,
) -> Result<(wgpu::Buffer, Option<HostMapping>), Error> {
    if size == 0 || !size.is_multiple_of(4) || usize::try_from(size).is_err() {
        return Err(Error::Range);
    }
    let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
    let vk_device = &raw.device;
    // SAFETY: a valid create info on a live device. The buffer is destroyed on every error path
    // below, and owned by wgpu after `from_raw_managed`.
    let buffer = unsafe { vk_device.create_buffer(&buffer_info(size), None) }?;
    // SAFETY: queries on the buffer just made and the device's physical device.
    let (needs, memory) = unsafe {
        (
            vk_device.get_buffer_memory_requirements(buffer),
            raw.instance
                .get_physical_device_memory_properties(raw.physical),
        )
    };
    let types = memory
        .memory_types
        .get(..memory.memory_type_count as usize)
        .unwrap_or(&[]);
    let pick = |want: vk::MemoryPropertyFlags| {
        types.iter().enumerate().position(|(index, ty)| {
            needs.memory_type_bits & (1 << index) != 0 && ty.property_flags.contains(want)
        })
    };
    let host = vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_COHERENT;
    let local = vk::MemoryPropertyFlags::DEVICE_LOCAL;
    // Shared memory: there is no device-local type the host can't see.
    let shared = types
        .iter()
        .all(|ty| !ty.property_flags.contains(local) || ty.property_flags.contains(host));
    let index = match placement {
        Placement::DeviceReachable if shared => pick(host | local).or_else(|| pick(local)),
        Placement::Device | Placement::DeviceReachable => {
            pick(local).or_else(|| pick(vk::MemoryPropertyFlags::empty()))
        }
        Placement::HostMapped => pick(host | local).or_else(|| pick(host)),
        Placement::HostCached => pick(host | vk::MemoryPropertyFlags::HOST_CACHED)
            .or_else(|| pick(host | local))
            .or_else(|| pick(host)),
    };
    let Some(index) = index else {
        // SAFETY: the buffer was made above and nothing else has it.
        unsafe { vk_device.destroy_buffer(buffer, None) };
        return Err(Error::NoMemoryType);
    };
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let allocate = vk::MemoryAllocateInfo::default()
        .allocation_size(needs.size)
        .memory_type_index(index as u32)
        .push_next(&mut dedicated);
    // SAFETY: a valid allocation for this buffer's requirements.
    let memory = match unsafe { vk_device.allocate_memory(&allocate, None) } {
        Ok(memory) => memory,
        Err(error) => {
            // SAFETY: as above.
            unsafe { vk_device.destroy_buffer(buffer, None) };
            return Err(error.into());
        }
    };
    let undo = || {
        // SAFETY: both were made above, and nothing else has them yet.
        unsafe {
            vk_device.destroy_buffer(buffer, None);
            vk_device.free_memory(memory, None);
        }
    };
    // SAFETY: the memory was allocated for this buffer and is bound once.
    if let Err(error) = unsafe { vk_device.bind_buffer_memory(buffer, memory, 0) } {
        undo();
        return Err(error.into());
    }
    let mut mapped = None;
    let visible = types
        .get(index)
        .is_some_and(|ty| ty.property_flags.contains(host));
    let map = match placement {
        Placement::Device => false,
        Placement::DeviceReachable => shared && visible,
        Placement::HostMapped | Placement::HostCached => true,
    };
    if map {
        // SAFETY: host-visible memory, mapped once and never unmapped before it is freed.
        let map =
            unsafe { vk_device.map_memory(memory, 0, vk::WHOLE_SIZE, vk::MemoryMapFlags::empty()) };
        match map {
            Ok(ptr) => mapped = Some(ptr.cast::<u8>()),
            Err(error) => {
                undo();
                return Err(error.into());
            }
        }
    }
    let buffer = wrap(device, buffer, memory, needs.size, size, label);
    let mapping = mapped.map(|ptr| HostMapping {
        ptr,
        len: size as usize,
        access: Mutex::new(()),
        lent: AtomicUsize::new(0),
        _buffer: buffer.clone(),
    });
    Ok((buffer, mapping))
}

/// Hands a bound buffer and its memory to wgpu, which frees both when the buffer drops.
fn wrap(
    device: &wgpu::Device,
    buffer: vk::Buffer,
    memory: vk::DeviceMemory,
    allocated: u64,
    size: u64,
    label: Option<&str>,
) -> wgpu::Buffer {
    // SAFETY: the buffer is bound to `memory` at offset 0, both belong to this device, and from
    // here wgpu owns them. The descriptor's size and usage match how the buffer was created.
    unsafe {
        let hal_buffer = hal_vk::Buffer::from_raw_managed(buffer, memory, 0, allocated);
        device.create_buffer_from_hal::<Vulkan>(hal_buffer, &descriptor(size, label))
    }
}

/// Whether the device can import a dma-buf as memory: the extensions that let it are enabled.
pub fn imports_dma_buf(device: &wgpu::Device) -> bool {
    Raw::of(device).is_some_and(|raw| {
        raw.has(khr::external_memory_fd::NAME) && raw.has(ash::ext::external_memory_dma_buf::NAME)
    })
}

/// Whether the device has host-visible memory that is also cached. V3D has none, and reading
/// its uncached memory from the host is slow.
pub fn has_host_cached(device: &wgpu::Device) -> bool {
    let Some(raw) = Raw::of(device) else {
        return false;
    };
    // SAFETY: a property query on the device's own physical device.
    let memory = unsafe {
        raw.instance
            .get_physical_device_memory_properties(raw.physical)
    };
    let wanted = vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_CACHED;
    memory
        .memory_types
        .iter()
        .take(memory.memory_type_count as usize)
        .any(|ty| ty.property_flags.contains(wanted))
}

/// Which memory types of the device could hold the dma-buf behind `fd`, as a bit per type.
pub fn dma_buf_memory_types(device: &wgpu::Device, fd: BorrowedFd<'_>) -> Result<u32, Error> {
    let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
    if !raw.has(khr::external_memory_fd::NAME) || !raw.has(ash::ext::external_memory_dma_buf::NAME)
    {
        return Err(Error::NoDmaBufImport);
    }
    let table = khr::external_memory_fd::Device::new(&raw.instance, &raw.device);
    let mut properties = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: the extension is enabled, and the fd is open for the length of the call.
    unsafe {
        table.get_memory_fd_properties(
            vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
            fd.as_raw_fd(),
            &mut properties,
        )
    }
    .map_err(|error| match error {
        vk::Result::ERROR_INVALID_EXTERNAL_HANDLE => Error::Fd,
        other => Error::Vulkan(other),
    })?;
    Ok(properties.memory_type_bits)
}

/// How many bytes of memory the device needs behind an imported storage buffer of `size`
/// bytes. A device may need more than `size`: V3D pads a buffer, so a dma-buf of exactly the
/// buffer's size is refused there. Allocate a dma-buf this large to import as `size` bytes.
pub fn dma_buf_bytes_needed(device: &wgpu::Device, size: u64) -> Result<u64, Error> {
    if size == 0 || !size.is_multiple_of(4) {
        return Err(Error::Range);
    }
    let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
    if !raw.has(khr::external_memory_fd::NAME) || !raw.has(ash::ext::external_memory_dma_buf::NAME)
    {
        return Err(Error::NoDmaBufImport);
    }
    let vk_device = &raw.device;
    let handle = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
    let mut external = vk::ExternalMemoryBufferCreateInfo::default().handle_types(handle);
    let info = buffer_info(size).push_next(&mut external);
    // SAFETY: a valid create info on a live device. The buffer is destroyed before returning.
    let buffer = unsafe { vk_device.create_buffer(&info, None) }?;
    // SAFETY: a query on the buffer just made, which is then destroyed, with nothing bound.
    let needs = unsafe {
        let needs = vk_device.get_buffer_memory_requirements(buffer);
        vk_device.destroy_buffer(buffer, None);
        needs
    };
    Ok(needs.size)
}

/// The largest storage buffer, in bytes, that a dma-buf of `have` bytes can be imported as on
/// this device: `have` rounded down to a word where the device needs no more than the buffer,
/// and less where it pads.
pub fn dma_buf_size_that_fits(device: &wgpu::Device, have: u64) -> Result<u64, Error> {
    let whole = have / 4 * 4;
    let need = dma_buf_bytes_needed(device, whole)?;
    if need <= have {
        return Ok(whole);
    }
    // The device needs `need - whole` more than the buffer: take that off and ask again.
    let smaller = have.saturating_sub(need - whole) / 4 * 4;
    if smaller == 0 {
        return Err(Error::TooSmall { have, need });
    }
    match dma_buf_bytes_needed(device, smaller)? {
        need if need <= have => Ok(smaller),
        need => Err(Error::TooSmall { have, need }),
    }
}

/// A storage buffer whose memory is the dma-buf behind `fd`, with no copy. `size` is a non-zero
/// multiple of 4, and `have` is the dma-buf's own length, which has to cover what
/// [`dma_buf_bytes_needed`] says for `size`. The buffer owns the fd from here.
pub fn import_dma_buf(
    device: &wgpu::Device,
    fd: OwnedFd,
    size: u64,
    have: u64,
    label: Option<&str>,
) -> Result<wgpu::Buffer, Error> {
    if size == 0 || !size.is_multiple_of(4) {
        return Err(Error::Range);
    }
    let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
    if !raw.has(khr::external_memory_fd::NAME) || !raw.has(ash::ext::external_memory_dma_buf::NAME)
    {
        return Err(Error::NoDmaBufImport);
    }
    let vk_device = &raw.device;
    let handle = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
    let mut external = vk::ExternalMemoryBufferCreateInfo::default().handle_types(handle);
    let info = buffer_info(size).push_next(&mut external);
    // SAFETY: a valid create info on a live device. Destroyed on every error path below.
    let buffer = unsafe { vk_device.create_buffer(&info, None) }?;
    // SAFETY: a query on the buffer just made.
    let needs = unsafe { vk_device.get_buffer_memory_requirements(buffer) };
    // Checked before the driver is asked about the fd: it would refuse a short one without
    // saying why.
    let allowed = match needs.size > have {
        true => Err(Error::TooSmall {
            have,
            need: needs.size,
        }),
        false => dma_buf_memory_types(device, std::os::fd::AsFd::as_fd(&fd)),
    };
    let both = match allowed {
        Ok(allowed) if needs.memory_type_bits & allowed != 0 => needs.memory_type_bits & allowed,
        other => {
            // SAFETY: made above, and nothing else has it.
            unsafe { vk_device.destroy_buffer(buffer, None) };
            return Err(other.err().unwrap_or(Error::NoMemoryType));
        }
    };
    let raw_fd = fd.into_raw_fd();
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(handle)
        .fd(raw_fd);
    let allocate = vk::MemoryAllocateInfo::default()
        .allocation_size(needs.size)
        .memory_type_index(both.trailing_zeros())
        .push_next(&mut import);
    // SAFETY: an import of an fd this call owns. On success Vulkan owns the fd.
    let memory = match unsafe { vk_device.allocate_memory(&allocate, None) } {
        Ok(memory) => memory,
        Err(error) => {
            // SAFETY: the import failed, so the fd is still this call's to close, and the buffer
            // was made above.
            unsafe {
                drop(<OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(raw_fd));
                vk_device.destroy_buffer(buffer, None);
            }
            return Err(match error {
                vk::Result::ERROR_INVALID_EXTERNAL_HANDLE => Error::Fd,
                other => other.into(),
            });
        }
    };
    // SAFETY: the memory was imported for this buffer and is bound once.
    if let Err(error) = unsafe { vk_device.bind_buffer_memory(buffer, memory, 0) } {
        // SAFETY: both were made above, and nothing else has them.
        unsafe {
            vk_device.destroy_buffer(buffer, None);
            vk_device.free_memory(memory, None);
        }
        return Err(error.into());
    }
    Ok(wrap(device, buffer, memory, needs.size, size, label))
}

/// The Vulkan buffer behind a wgpu one, for commands recorded through
/// [`Submitter`](crate::submit::Submitter). `None` off Vulkan. Using it is the recording's
/// responsibility: the wgpu buffer has to outlive the commands.
pub fn handle(buffer: &wgpu::Buffer) -> Option<vk::Buffer> {
    // SAFETY: reads a handle, which stays wgpu's.
    unsafe { buffer.as_hal::<Vulkan>().map(|hal| hal.raw_handle()) }
}

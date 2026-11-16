//! Device memory made and freed by the raw calls alone, for the no-Daedalus floors the board's
//! timings are compared with.
//!
//! A block is placed exactly as `Storage` places a buffer of the same size and placement (see
//! `memory::place`): the same memory type, the same usage, mapped where
//! the storage is mapped. What it doesn't have is wgpu: nothing wraps the buffer, nothing clears
//! it, and nothing tracks it. The memory is freed when the block drops.

use std::time::Duration;

use ash::vk;

use crate::Error;
use crate::device::Raw;
use crate::memory::{Placement, handle, place};
use crate::submit::Submitter;

/// How long a copy may take before it is reported as a timeout. A copy that fails outright, such
/// as on a lost device, is reported the same way: the wait can't tell them apart.
const COPY_WAIT: Duration = Duration::from_secs(60);

/// A buffer and its memory, made by the raw calls on a wgpu device and freed when it drops.
pub struct Block {
    device: ash::Device,
    /// The wgpu device the calls were made on. Kept alive while the memory is.
    _keep: wgpu::Device,
    buffer: vk::Buffer,
    memory: vk::DeviceMemory,
    size: u64,
    type_index: u32,
    flags: vk::MemoryPropertyFlags,
    ptr: Option<*mut u8>,
}

impl Block {
    /// A block of `size` bytes in `placement`, on `device`, which must be a Vulkan device.
    pub fn new(device: &wgpu::Device, size: u64, placement: Placement) -> Result<Block, Error> {
        if size == 0 || !size.is_multiple_of(4) || usize::try_from(size).is_err() {
            return Err(Error::Range);
        }
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        let placed = place(&raw, size, placement)?;
        Ok(Block {
            device: raw.device.clone(),
            _keep: device.clone(),
            buffer: placed.buffer,
            memory: placed.memory,
            size,
            type_index: placed.type_index,
            flags: placed.flags,
            ptr: placed.ptr,
        })
    }

    /// The block's size in bytes.
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Whether the block has no bytes. Blocks are never empty, so this is always false.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Whether the host can read and write the block through a mapping.
    pub fn is_mapped(&self) -> bool {
        self.ptr.is_some()
    }

    /// The index of the memory type the block's memory was allocated in.
    pub fn memory_type(&self) -> u32 {
        self.type_index
    }

    /// The memory type's properties, by name, such as `DEVICE_LOCAL | HOST_VISIBLE`.
    pub fn memory_flags(&self) -> String {
        let names = [
            (vk::MemoryPropertyFlags::DEVICE_LOCAL, "DEVICE_LOCAL"),
            (vk::MemoryPropertyFlags::HOST_VISIBLE, "HOST_VISIBLE"),
            (vk::MemoryPropertyFlags::HOST_COHERENT, "HOST_COHERENT"),
            (vk::MemoryPropertyFlags::HOST_CACHED, "HOST_CACHED"),
        ];
        let mut found: Vec<&str> = names
            .iter()
            .filter(|(flag, _)| self.flags.contains(*flag))
            .map(|(_, name)| *name)
            .collect();
        if found.is_empty() {
            found.push("none");
        }
        found.join(" | ")
    }

    /// Copies `bytes` into the block at `offset`. The block's memory must not be in use by the
    /// device meanwhile, which a copy that was waited for ensures.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        let start = self.range(offset, bytes.len())?;
        let ptr = self.ptr.ok_or(Error::Unsupported)?;
        // SAFETY: `start` plus the length is inside the mapping, which lives as long as the
        // block, and the slice is the caller's, so the two can't overlap.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.add(start), bytes.len()) };
        Ok(())
    }

    /// Copies the block from `offset` into `out`. Device work that wrote the block must have
    /// finished, as for any mapped read.
    pub fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        let start = self.range(offset, out.len())?;
        let ptr = self.ptr.ok_or(Error::Unsupported)?;
        // SAFETY: as in `write`, with the block's bytes as the source.
        unsafe { std::ptr::copy_nonoverlapping(ptr.add(start), out.as_mut_ptr(), out.len()) };
        Ok(())
    }

    fn range(&self, offset: u64, len: usize) -> Result<usize, Error> {
        let start = usize::try_from(offset).map_err(|_| Error::Range)?;
        let end = offset.checked_add(len as u64).ok_or(Error::Range)?;
        if end > self.size {
            return Err(Error::Range);
        }
        Ok(start)
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // SAFETY: the buffer and the memory were made on this device and bound once, and nothing
        // else has them. The caller has no device work left on them: every copy into one is
        // waited for before it returns. Freeing the memory unmaps it.
        unsafe {
            self.device.destroy_buffer(self.buffer, None);
            self.device.free_memory(self.memory, None);
        }
    }
}

/// Copies the first `bytes` of the wgpu buffer `source` into `block`, in a raw submit of its own,
/// and waits for the copy. Both must be on the submitter's device. A host write into `source` only
/// reaches the device in a submit: a write into a mapped buffer is at once, and any other write
/// travels with wgpu's next submit, which this one doesn't wait for.
pub fn copy_into(
    submitter: &Submitter,
    source: &wgpu::Buffer,
    block: &Block,
    bytes: u64,
) -> Result<(), Error> {
    if bytes == 0 || bytes > block.size || bytes > source.size() {
        return Err(Error::Range);
    }
    let from = handle(source).ok_or(Error::NotVulkan)?;
    let to = block.buffer;
    let record = move |device: &ash::Device, commands: vk::CommandBuffer| {
        let region = vk::BufferCopy::default().size(bytes);
        // SAFETY: both buffers are live on this device and the commands are recording on it. The
        // submitter's barriers order the copy with the work around it.
        unsafe { device.cmd_copy_buffer(commands, from, to, &[region]) };
    };
    // SAFETY: the recording is valid on the submitter's device. `source` and `block` are borrowed
    // until the wait below has returned, so both outlive the work that uses them.
    let ticket = unsafe { submitter.submit(record, &[]) }?;
    if ticket.wait(COPY_WAIT) {
        Ok(())
    } else {
        Err(Error::Vulkan(vk::Result::TIMEOUT))
    }
}

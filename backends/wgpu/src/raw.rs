//! Raw submits on the backend's device: commands recorded with Vulkan itself, for kernels
//! compiled outside Daedalus, on Daedalus's buffers.
//!
//! The recording and the submit are `backends/vk-sys`'s, which is where the `unsafe` is. This
//! is the safe part: the ring is made on the backend's device with the device's own submit
//! lock, so a raw submit never overlaps one of wgpu's. Linux and Vulkan only.

use std::os::fd::OwnedFd;
use std::sync::Arc;

use daedalus_vk_sys::ash::vk;
use daedalus_vk_sys::compute::ComputePipeline;
use daedalus_vk_sys::device::Extras;
use daedalus_vk_sys::submit::Submitter;

use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::OpenError;

/// A chain of compute stages run in one submit, recorded with Vulkan itself. See
/// `daedalus_vk_sys::chain`. Its runs are begun, given a dispatch per stage, and submitted.
pub use daedalus_vk_sys::chain::{Arg, Chain};

/// Why there is no raw submitter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawError {
    Open(OpenError),
    /// The device isn't on Vulkan, or Vulkan refused. The text is the reason.
    Vulkan(String),
}

impl std::fmt::Display for RawError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawError::Open(error) => error.fmt(f),
            RawError::Vulkan(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for RawError {}

impl WgpuBackend {
    /// A ring of `slots` command buffers on this backend's device, each with a fence. A submit
    /// through it allocates nothing, and takes the same lock as every other submit on the
    /// device. Data given to `Storage::write` on a device buffer reaches the device with wgpu's
    /// next submit, so a raw recording sees it only after that.
    pub fn raw_submitter(&self, slots: usize) -> Result<Submitter, RawError> {
        let context = self.context().map_err(RawError::Open)?;
        Submitter::new(&context.device, &context.queue, &context.submit, slots)
            .map_err(|error| RawError::Vulkan(error.to_string()))
    }

    /// A raw chain of one compute stage per `(wgsl source, entry point)` in `stages`, on this
    /// backend's device, with `sets` sets of buffers for each stage to remember. The chain
    /// takes the device's submit lock, so its submits never overlap wgpu's.
    pub fn raw_chain(&self, stages: &[(&str, &str)], sets: usize) -> Result<Chain, RawError> {
        let context = self.context().map_err(RawError::Open)?;
        let mut pipelines = Vec::with_capacity(stages.len());
        for (source, entry) in stages {
            let pipeline = ComputePipeline::from_wgsl(&context.device, source, entry)
                .map_err(|error| RawError::Vulkan(error.to_string()))?;
            pipelines.push(Arc::new(pipeline));
        }
        Chain::new(
            &context.device,
            &context.queue,
            &context.submit,
            &pipelines,
            sets,
        )
        .map_err(|error| RawError::Vulkan(error.to_string()))
    }

    /// Whether shaders on this device may read 8-bit types from a storage buffer. Check before
    /// making a raw pipeline that does: without the feature the pipeline is invalid.
    pub fn storage_buffer_8bit(&self) -> bool {
        self.context()
            .ok()
            .and_then(|context| Extras::of(&context.device))
            .is_some_and(|extras| extras.eight_bit.storage_buffer)
    }
}

impl Storage {
    /// The Vulkan buffer behind this one, for a raw recording to bind. `None` off Vulkan. The
    /// storage has to outlive every submit that uses the handle.
    pub fn raw_buffer(&self) -> Option<vk::Buffer> {
        daedalus_vk_sys::memory::handle(self.buffer())
    }

    /// The wgpu buffer behind this storage, for a raw chain's [`Arg::Storage`]. The storage has
    /// to outlive every run that binds it.
    pub fn wgpu_buffer(&self) -> &wgpu::Buffer {
        self.buffer()
    }
}

/// Device memory the raw calls make and free on a backend's device, with no wgpu buffer on it. It
/// is placed the way a [`Storage`] of the same size and placement is, and nothing else of wgpu's
/// touches it: the baselines the timed budgets are compared with. See `daedalus_vk_sys::block`.
pub use daedalus_vk_sys::block::Block as RawBlock;
/// Where a raw block is placed, by the names [`Storage`] places by.
pub use daedalus_vk_sys::memory::Placement as RawPlacement;
/// A sync fd held as a semaphore that a raw submit waits on, on the device. See
/// [`WgpuBackend::import_sync_fd`].
pub use daedalus_vk_sys::semaphore::Semaphore as RawSemaphore;

impl WgpuBackend {
    /// A raw block of `size` bytes in `placement`, made on this backend's device. Its memory is
    /// freed when it drops.
    pub fn raw_block(&self, size: u64, placement: RawPlacement) -> Result<RawBlock, RawError> {
        let context = self.context().map_err(RawError::Open)?;
        RawBlock::new(&context.device, size, placement)
            .map_err(|error| RawError::Vulkan(error.to_string()))
    }

    /// The sync fd `fence` (a sync_file, such as one a dma-buf exports) as a semaphore a raw
    /// submit on this backend's device can wait on. The semaphore owns the fd from here. Refused
    /// where the device can't import a sync fd.
    pub fn import_sync_fd(&self, fence: OwnedFd) -> Result<RawSemaphore, RawError> {
        let context = self.context().map_err(RawError::Open)?;
        RawSemaphore::import_sync_fd(&context.device, Some(fence))
            .map_err(|error| RawError::Vulkan(error.to_string()))
    }
}

/// Copies the first `bytes` of `source` into `block`, in a raw submit of its own on `submitter`,
/// and waits for the copy. The submitter, the storage and the block all come from one backend.
pub fn raw_copy(
    submitter: &Submitter,
    source: &Storage,
    block: &RawBlock,
    bytes: u64,
) -> Result<(), RawError> {
    daedalus_vk_sys::block::copy_into(submitter, source.wgpu_buffer(), block, bytes)
        .map_err(|error| RawError::Vulkan(error.to_string()))
}

/// [`raw_copy`], with the copy's submit waiting on the device for each of `waits`, and no host wait
/// before it. `meanwhile` runs after the submit and before the host waits for the copy.
pub fn raw_copy_after(
    submitter: &Submitter,
    source: &Storage,
    block: &RawBlock,
    bytes: u64,
    waits: &[&RawSemaphore],
    meanwhile: impl FnOnce(),
) -> Result<(), RawError> {
    daedalus_vk_sys::block::copy_into_after(
        submitter,
        source.wgpu_buffer(),
        block,
        bytes,
        waits,
        meanwhile,
    )
    .map_err(|error| RawError::Vulkan(error.to_string()))
}

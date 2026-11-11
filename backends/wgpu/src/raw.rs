//! Raw submits on the backend's device: commands recorded with Vulkan itself, for kernels
//! compiled outside Daedalus, on Daedalus's buffers.
//!
//! The recording and the submit are `backends/vk-sys`'s, which is where the `unsafe` is. This
//! is the safe part: the ring is made on the backend's device with the device's own submit
//! lock, so a raw submit never overlaps one of wgpu's. Linux and Vulkan only.

use daedalus_vk_sys::ash::vk;
use daedalus_vk_sys::device::Extras;
use daedalus_vk_sys::submit::Submitter;

use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::OpenError;

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
}

use std::fmt;

use ash::vk;

/// Why a raw call didn't happen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The device isn't a Vulkan device.
    NotVulkan,
    /// The device was opened without dma-buf import. lavapipe is one that can't.
    NoDmaBufImport,
    /// The device was opened without sync fd import.
    NoSyncFdImport,
    /// No memory type fits the request.
    NoMemoryType,
    /// A size or range that can't be used.
    Range,
    /// The fd was refused.
    Fd,
    /// The dma-buf is smaller than the memory the device needs for the buffer.
    TooSmall { have: u64, need: u64 },
    /// The bytes start at an offset in the dma-buf that the device can't bind a buffer at: it
    /// wants a multiple of `alignment`.
    Offset { offset: u64, alignment: u64 },
    /// Something this crate doesn't do, such as a shader that binds a texture where only
    /// buffers are bound.
    Unsupported,
    /// The host can't write a mapping while its bytes are lent out.
    Lent,
    /// Vulkan said no.
    Vulkan(vk::Result),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotVulkan => f.write_str("not a Vulkan device"),
            Error::NoDmaBufImport => f.write_str("the device can't import a dma-buf"),
            Error::NoSyncFdImport => f.write_str("the device can't import a sync fd"),
            Error::NoMemoryType => f.write_str("no memory type fits"),
            Error::Range => f.write_str("a size or range that can't be used"),
            Error::Fd => f.write_str("the fd was refused"),
            Error::TooSmall { have, need } => write!(
                f,
                "the dma-buf is {have} bytes, and the device needs {need} for the buffer"
            ),
            Error::Offset { offset, alignment } => write!(
                f,
                "the bytes start {offset} into the dma-buf, and the device binds a buffer only at a multiple of {alignment}"
            ),
            Error::Unsupported => f.write_str("not supported here"),
            Error::Lent => {
                f.write_str("the buffer's bytes are lent out, so the host can't write them")
            }
            Error::Vulkan(result) => write!(f, "Vulkan: {result:?}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<vk::Result> for Error {
    fn from(result: vk::Result) -> Error {
        Error::Vulkan(result)
    }
}

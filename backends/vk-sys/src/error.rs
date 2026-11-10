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

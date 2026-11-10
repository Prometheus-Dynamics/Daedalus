//! External memory as a texture, with no copy.
//!
//! The caller describes an image: its dma-buf fds, where each plane starts and how far apart its
//! rows are, its size, its fourcc and its modifier. The backend imports it as it is. It knows
//! nothing about whose memory it is or what type it came from.

mod fence;
#[cfg(target_os = "linux")]
mod import;
mod layout;
mod modifiers;
mod refused;

pub use fence::{Acquired, DEFAULT_ACQUIRE_TIMEOUT, FenceWait, acquire};
#[cfg(target_os = "linux")]
pub use import::{ExternalImage, ExternalPlane, ImportExternal, ImportStats, import_transfer};
pub use layout::{Described, Fourcc, ImageFormat, ImportError, Layout, PlaneFormat};
pub use modifiers::{Importer, MAX_MEMORY_PLANES, MOD_INVALID, MOD_LINEAR, vendor_of};
pub use refused::{Settled, settle};

#[cfg(target_os = "linux")]
pub(crate) use import::Imports;

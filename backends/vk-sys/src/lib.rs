//! Raw Vulkan and wgpu-hal calls, kept in one crate. See the README.

mod lock;
pub use lock::{DriverHeld, SubmitGuard, SubmitLock};

#[cfg(target_os = "linux")]
pub mod device;
#[cfg(target_os = "linux")]
mod error;
#[cfg(target_os = "linux")]
pub mod memory;
#[cfg(target_os = "linux")]
pub mod semaphore;
#[cfg(target_os = "linux")]
pub mod submit;
#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(target_os = "linux")]
pub use ash;
#[cfg(target_os = "linux")]
pub use error::Error;

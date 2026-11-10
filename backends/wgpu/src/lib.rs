//! The wgpu backend. See the README for the rules it keeps.

#![forbid(unsafe_code)]

mod adapter;
mod backend;
mod buffer;
mod caps;
mod context;
mod copy_limiter;
mod driver;
mod memory;
mod pool;
mod profile;
mod staging;
mod storage_pool;
mod texture;

pub use adapter::{AdapterInfo, Api, Apis, Candidate, DeviceKind, Options, is_software, rank};
pub use backend::{Handle, WgpuBackend};
pub use buffer::{Storage, StorageError};
pub use caps::{CapsRecord, ComputeCaps, ComputeFeatures, Need};
pub use context::OpenError;
pub use driver::{DriverGuard, devices_created, driver_lock, with_driver_lock};
pub use memory::{MemoryHints, SPACE_DEVICE, SPACE_HOST_CACHED, SPACE_HOST_MAPPED};
pub use pool::TempPoolLimits;
pub use profile::{DeviceProfile, Family, vendor_ids};
pub use staging::{StagingConfig, StagingStats};
pub use storage_pool::{StoragePool, StoragePoolStats};
pub use texture::{Format, Texture, TextureDesc, TextureError};

//! The wgpu backend. See the README for the rules it keeps.

#![forbid(unsafe_code)]

mod adapter;
mod backend;
mod buffer;
mod caps;
mod context;
mod copy_limiter;
pub mod dmabuf;
mod driver;
mod fence;
#[cfg(test)]
mod fence_tests;
pub mod fragment;
pub mod image;
mod inflight;
pub mod kernels;
mod loss;
mod memory;
pub mod merge;
mod pool;
mod profile;
#[cfg(target_os = "linux")]
mod raw;
mod readback;
pub mod record;
mod region;
pub mod shader;
mod staging;
mod storage_pool;
mod texture;
mod timer;
mod transfer;
mod upload;
mod wait;

pub use adapter::{AdapterInfo, Api, Apis, Candidate, DeviceKind, Options, is_software, rank};
pub use backend::{Handle, WgpuBackend};
pub use buffer::{Storage, StorageError};
pub use caps::{CapsRecord, ComputeCaps, ComputeFeatures, Need};
pub use context::OpenError;
pub use driver::{DriverGuard, devices_created, driver_lock, with_driver_lock};
pub use fence::Ticket;
pub use loss::{RETRY_CAP, RETRY_FIRST, retry_delay};
pub use memory::{
    MemoryHints, SPACE_DEVICE, SPACE_DMA_HEAP, SPACE_HOST_CACHED, SPACE_HOST_MAPPED, SPACE_TEXTURE,
};
pub use pool::TempPoolLimits;
pub use profile::{DeviceProfile, Family, vendor_ids};
#[cfg(target_os = "linux")]
pub use raw::RawError;
pub use staging::{StagingConfig, StagingStats};
pub use storage_pool::{StoragePool, StoragePoolStats};
pub use texture::{Format, Texture, TextureDesc, TextureError};
pub use timer::TimeSource;
pub use transfer::{
    HOST_SPACE, MAP, MAP_CACHED, MAP_HEAP, READBACK, TransferStats, UPLOAD, UPLOAD_MAPPED,
};
pub use wait::{DEFAULT_WAIT_TIMEOUT, WaitError};

#[cfg(test)]
#[global_allocator]
static ALLOC: daedalus_abi::counting::CountingAlloc = daedalus_abi::counting::CountingAlloc;

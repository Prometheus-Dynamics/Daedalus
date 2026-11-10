//! The wgpu backend. See the README for the rules it keeps.

#![forbid(unsafe_code)]

mod adapter;
mod backend;
mod caps;
mod context;
mod driver;
mod profile;

pub use adapter::{AdapterInfo, Api, Apis, Candidate, DeviceKind, Options, is_software, rank};
pub use backend::{Handle, WgpuBackend};
pub use caps::{CapsRecord, ComputeCaps, ComputeFeatures, Need};
pub use context::OpenError;
pub use driver::{DriverGuard, devices_created, driver_lock, with_driver_lock};
pub use profile::{DeviceProfile, Family, vendor_ids};

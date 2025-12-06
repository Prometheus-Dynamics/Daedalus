//! Unified node/value registry. See `PLAN.md` for roadmap and layering rules.
//! Deterministic ordering is required; no backend/GPU types appear here.
//!
//! # Feature Matrix
//! - `default`: core registry (nodes/values/converters).
//! - `bundle`: bundle loader APIs.
//! - `plugin`: plugin adapters (alias `plugins` available).
//! - `ffi`: FFI adapters.
//! - `gpu`: only to forward feature flags to converter graph; no GPU backend types leak.
//!
//! Concurrency: wrap `Registry` in `Arc<RwLock<_>>` if shared; internal structures are deterministic and `Send + Sync`.

pub mod convert;
pub mod diagnostics;
pub mod ids;
pub mod store;

#[cfg(feature = "bundle")]
pub mod bundle;
#[cfg(feature = "ffi")]
pub mod ffi;
#[cfg(feature = "plugin")]
pub mod plugin;

pub mod prelude {
    pub use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
    pub use crate::ids::NodeId;
    pub use crate::store::{NodeDescriptor, Registry, RegistryView};
    pub use daedalus_data::convert::{ConversionResolution, ConverterId};
    pub use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
    pub use daedalus_data::model::TypeExpr;
}

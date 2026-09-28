//! Data model and descriptors for the Daedalus pipeline.
//!
//! This crate is the single source of truth for `Value`, `ValueType`, and
//! `TypeExpr`, plus descriptors, codecs, converters, and units. It is feature-
//! light: schema/proto emission and GPU integration are feature-gated. No
//! concrete GPU backend types live here; only trait/handle shims.
//! Deterministic ordering is required so planner/runtime goldens stay stable.
//!
//! # Feature Matrix (compile-checked)
//! - `default` (includes `json`): core types, descriptors, converters, units, JSON codec.
//! - `json`: enable JSON codec/base64 support (on by default).
//! - `gpu`: GPU handle shims; no concrete backend types.
//! - `async`: async resolver wrappers.
//! - `schema`: JSON Schema emission.
//! - `proto`: proto3 type emission.

pub mod convert;
pub mod daedalus_type;
pub mod descriptor;
pub mod errors;
pub mod model;
pub mod named_types;
pub mod to_value;
pub mod typing;
pub mod units;

/// Cargo features of `daedalus-data` enabled in this build, in `Cargo.toml` declaration order.
///
/// Dynamic plugin loaders fold this into their build fingerprint so hosts and plugins built
/// with different feature sets are rejected before any Rust type crosses the boundary.
pub const ENABLED_FEATURES: &[&str] = &[
    #[cfg(feature = "schema")]
    "schema",
    #[cfg(feature = "proto")]
    "proto",
    #[cfg(feature = "gpu")]
    "gpu",
    #[cfg(feature = "async")]
    "async",
    #[cfg(feature = "json")]
    "json",
];

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "proto")]
pub mod proto;
#[cfg(feature = "schema")]
pub mod schema;

#[cfg(feature = "gpu")]
pub mod gpu;

pub mod prelude {
    pub use crate::daedalus_type::DaedalusTypeExpr;
    pub use crate::descriptor::{
        DataDescriptor, DescriptorId, DescriptorVersion, GpuHints, MemoryLocation,
    };
    pub use crate::errors::{DataError, DataErrorCode, DataResult};
    pub use crate::model::{EnumValue, StructFieldValue, TypeExpr, Value, ValueRef, ValueType};
    pub use crate::named_types::{HostExportPolicy, NamedType};
    pub use crate::to_value::ToValue;
}

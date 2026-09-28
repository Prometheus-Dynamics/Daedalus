//! Transport capability registry.
//! Deterministic ordering is required; backend execution state does not appear here.
//!
//! # Feature Matrix
//! - `default`: transport capabilities.
//! - `plugin`: plugin adapters.
//! - `gpu`: no GPU backend types leak.

pub mod capability;
pub mod diagnostics;
pub mod ids;

#[cfg(feature = "plugin")]
pub mod plugin;

/// Cargo features of `daedalus-registry` enabled in this build, in `Cargo.toml` declaration order.
///
/// Dynamic plugin loaders fold this into their build fingerprint so hosts and plugins built
/// with different feature sets are rejected before any Rust type crosses the boundary.
pub const ENABLED_FEATURES: &[&str] = &[
    #[cfg(feature = "bundle")]
    "bundle",
    #[cfg(feature = "plugin")]
    "plugin",
    #[cfg(feature = "gpu")]
    "gpu",
];

/// Convert a Daedalus type expression into the stable transport identity used by
/// manifests, adapters, and runtime payloads.
///
/// Opaque expressions are already transport identities. Structured expressions
/// are normalized and serialized as JSON so the key is stable across crates and
/// does not depend on Rust `Debug` formatting.
pub fn typeexpr_transport_key(ty: &daedalus_data::model::TypeExpr) -> daedalus_transport::TypeKey {
    match ty {
        daedalus_data::model::TypeExpr::Opaque(name) => {
            daedalus_transport::TypeKey::new(name.clone())
        }
        other => {
            let normalized = other.clone().normalize();
            let encoded = serde_json::to_string(&normalized)
                .expect("normalized TypeExpr serialization should not fail");
            daedalus_transport::TypeKey::new(format!("typeexpr:{encoded}"))
        }
    }
}

pub mod prelude {
    pub use crate::capability::{
        AdapterDecl, AdapterRegistry, CapabilityRegistry, CapabilityRegistrySnapshot, DeviceDecl,
        DeviceRegistry, ExportPolicy, FanInDecl, NODE_EXECUTION_KIND_META_KEY, NodeDecl,
        NodeExecutionKind, NodeRegistry, PluginManifest, PluginRegistry, PortDecl, SerializerDecl,
        SerializerRegistry, TypeDecl, TypeRegistry,
    };
    pub use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
    pub use crate::ids::{GroupId, IdValidationError, NodeId};
    pub use crate::typeexpr_transport_key;
    pub use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
}

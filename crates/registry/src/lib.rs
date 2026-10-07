//! Transport capability registry.
//! Deterministic ordering is required; backend execution state does not appear here.
//!
//! # Feature Matrix
//! - `default`: transport capabilities.
//! - `plugin`: plugin adapters.
//! - `gpu`: no GPU backend types leak.
//!
//! `no_std` + `alloc` without the default `std` feature (see "Portability" in
//! docs/development.md).
#![cfg_attr(not(any(feature = "std", test)), no_std)]

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate alloc;

pub mod capability;
pub mod diagnostics;
pub mod ids;

#[cfg(feature = "plugin")]
pub mod plugin;

daedalus_core::build_facts!();

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

/// The type expression a transport key stands for: the inverse of [`typeexpr_transport_key`]
/// (`typeexpr:` keys decode to their structure, every other key is `Opaque(key)`).
pub fn transport_key_typeexpr(key: &daedalus_transport::TypeKey) -> daedalus_data::model::TypeExpr {
    key.as_str()
        .strip_prefix("typeexpr:")
        .and_then(|encoded| serde_json::from_str(encoded).ok())
        .unwrap_or_else(|| daedalus_data::model::TypeExpr::opaque(key.as_str()))
}

pub mod prelude {
    pub use crate::capability::{
        AdapterDecl, AdapterRegistry, CapabilityRegistry, CapabilityRegistrySnapshot, DeviceDecl,
        DeviceRegistry, ExportPolicy, FanInDecl, NODE_EXECUTION_KIND_META_KEY, NODE_FIRE_META_KEY,
        NODE_REQUIRED_INPUTS_META_KEY, NODE_SHAREABLE_META_KEY, NodeDecl, NodeExecutionKind,
        NodeFire, NodeRegistry, PluginManifest, PluginRegistry, PortDecl, SerializerDecl,
        SerializerRegistry, TypeDecl, TypeRegistry,
    };
    pub use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
    pub use crate::ids::{IdValidationError, NodeId};
    pub use crate::{transport_key_typeexpr, typeexpr_transport_key};
    pub use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
}

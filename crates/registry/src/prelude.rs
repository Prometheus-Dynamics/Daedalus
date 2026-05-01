//! Convenience re-exports for registry consumers.
pub use crate::capability::{
    AdapterDecl, AdapterRegistry, CapabilityRegistry, CapabilityRegistrySnapshot, DeviceDecl,
    DeviceRegistry, ExportPolicy, FanInDecl, NodeDecl, NodeRegistry, PluginManifest,
    PluginRegistry, PortDecl, SerializerDecl, SerializerRegistry, TypeDecl, TypeRegistry,
};
pub use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
pub use crate::ids::NodeId;
pub use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
pub use daedalus_data::model::TypeExpr;

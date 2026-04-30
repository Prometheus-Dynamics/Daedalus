mod declarations;
mod registry;
mod resolution;
mod support;

pub use declarations::{
    AdapterDecl, AdapterPathResolution, AdapterPathStep, DeviceDecl, ExportPolicy, FanInDecl,
    NODE_EXECUTION_KIND_META_KEY, NodeDecl, NodeExecutionKind, PluginManifest, PortDecl,
    SerializerDecl, TypeDecl,
};
pub use registry::{
    AdapterRegistry, CapabilityRegistry, CapabilityRegistrySnapshot, DeviceRegistry, NodeRegistry,
    PluginRegistry, SerializerRegistry, TypeRegistry,
};

#[cfg(test)]
#[path = "capability_tests.rs"]
mod tests;

//! Helpers to translate node descriptors into registry-friendly builders.
//! Feature-gated to avoid pulling registry/data dependencies into minimal builds.

use crate::NodeDescriptor;
use daedalus_registry::store::NodeDescriptorBuilder;

/// Create a registry builder pre-populated with id/label from a descriptor.
///
/// Ports, feature flags, and versions remain caller-defined to align with the registry schema.
pub fn registry_builder(desc: &NodeDescriptor) -> NodeDescriptorBuilder {
    let mut builder =
        NodeDescriptorBuilder::new(desc.id.0.clone()).default_compute(desc.default_compute);
    if let Some(label) = &desc.label {
        builder = builder.label(label.clone());
    }
    builder
}

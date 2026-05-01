use std::collections::BTreeMap;

use daedalus_transport::{AdapterId, TypeKey};
use serde::{Deserialize, Serialize};

use crate::diagnostics::{ConflictKind, RegistryResult};
use crate::ids::NodeId;

use super::declarations::{
    AdapterDecl, DeviceDecl, NodeDecl, PluginManifest, SerializerDecl, TypeDecl,
};
use super::support::{
    ActiveFeatureSet, active_feature_set, duplicate_error, features_enabled, missing_dependency,
};

/// Plugin manifest registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRegistry {
    entries: BTreeMap<String, PluginManifest>,
}

impl PluginRegistry {
    pub fn register(&mut self, manifest: PluginManifest) -> RegistryResult<()> {
        let manifest = manifest.normalize();
        let key = manifest.id.clone();
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Plugin, key));
        }
        self.entries.insert(manifest.id.clone(), manifest);
        Ok(())
    }

    pub fn replace(&mut self, manifest: PluginManifest) {
        let manifest = manifest.normalize();
        self.entries.insert(manifest.id.clone(), manifest);
    }

    pub fn get(&self, id: &str) -> Option<&PluginManifest> {
        self.entries.get(id)
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub fn values(&self) -> impl Iterator<Item = &PluginManifest> {
        self.entries.values()
    }

    pub fn snapshot(&self) -> Vec<PluginManifest> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<PluginManifest> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<PluginManifest> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Transport type declaration registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeRegistry {
    entries: BTreeMap<TypeKey, TypeDecl>,
}

impl TypeRegistry {
    pub fn register(&mut self, decl: TypeDecl) -> RegistryResult<()> {
        let decl = decl.normalize();
        let key = decl.key.clone();
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Type, key.to_string()));
        }
        self.entries.insert(key, decl);
        Ok(())
    }

    pub fn replace(&mut self, decl: TypeDecl) {
        let decl = decl.normalize();
        self.entries.insert(decl.key.clone(), decl);
    }

    pub fn get(&self, key: &TypeKey) -> Option<&TypeDecl> {
        self.entries.get(key)
    }

    pub fn contains_key(&self, key: &TypeKey) -> bool {
        self.entries.contains_key(key)
    }

    pub fn snapshot(&self) -> Vec<TypeDecl> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<TypeDecl> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<TypeDecl> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Transport adapter declaration registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRegistry {
    entries: BTreeMap<AdapterId, AdapterDecl>,
}

impl AdapterRegistry {
    pub fn register(&mut self, decl: AdapterDecl) -> RegistryResult<()> {
        let decl = decl.normalize();
        let key = decl.id.clone();
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Adapter, key.to_string()));
        }
        self.entries.insert(key, decl);
        Ok(())
    }

    pub fn replace(&mut self, decl: AdapterDecl) {
        let decl = decl.normalize();
        self.entries.insert(decl.id.clone(), decl);
    }

    pub fn get(&self, id: &AdapterId) -> Option<&AdapterDecl> {
        self.entries.get(id)
    }

    pub fn contains_key(&self, id: &AdapterId) -> bool {
        self.entries.contains_key(id)
    }

    pub fn values(&self) -> impl Iterator<Item = &AdapterDecl> {
        self.entries.values()
    }

    pub fn snapshot(&self) -> Vec<AdapterDecl> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<AdapterDecl> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<AdapterDecl> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Transport-aware node declaration registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRegistry {
    entries: BTreeMap<NodeId, NodeDecl>,
}

impl NodeRegistry {
    pub fn register(&mut self, decl: NodeDecl) -> RegistryResult<()> {
        let decl = decl.normalize();
        let key = decl.id.clone();
        key.validate()?;
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Node, key.0.clone()));
        }
        self.entries.insert(key, decl);
        Ok(())
    }

    pub fn replace(&mut self, decl: NodeDecl) {
        let decl = decl.normalize();
        self.entries.insert(decl.id.clone(), decl);
    }

    pub fn get(&self, id: &NodeId) -> Option<&NodeDecl> {
        self.entries.get(id)
    }

    pub fn contains_key(&self, id: &NodeId) -> bool {
        self.entries.contains_key(id)
    }

    pub fn values(&self) -> impl Iterator<Item = &NodeDecl> {
        self.entries.values()
    }

    pub fn snapshot(&self) -> Vec<NodeDecl> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<NodeDecl> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<NodeDecl> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Boundary serializer declaration registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializerRegistry {
    entries: BTreeMap<String, SerializerDecl>,
}

impl SerializerRegistry {
    pub fn register(&mut self, decl: SerializerDecl) -> RegistryResult<()> {
        let decl = decl.normalize();
        let key = decl.id.clone();
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Serializer, key));
        }
        self.entries.insert(decl.id.clone(), decl);
        Ok(())
    }

    pub fn replace(&mut self, decl: SerializerDecl) {
        let decl = decl.normalize();
        self.entries.insert(decl.id.clone(), decl);
    }

    pub fn values(&self) -> impl Iterator<Item = &SerializerDecl> {
        self.entries.values()
    }

    pub fn get(&self, id: &str) -> Option<&SerializerDecl> {
        self.entries.get(id)
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub fn snapshot(&self) -> Vec<SerializerDecl> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<SerializerDecl> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<SerializerDecl> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Device capability declaration registry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRegistry {
    entries: BTreeMap<String, DeviceDecl>,
}

impl DeviceRegistry {
    pub fn register(&mut self, decl: DeviceDecl) -> RegistryResult<()> {
        let decl = decl.normalize();
        let key = decl.id.clone();
        if self.entries.contains_key(&key) {
            return Err(duplicate_error(ConflictKind::Device, key));
        }
        self.entries.insert(decl.id.clone(), decl);
        Ok(())
    }

    pub fn replace(&mut self, decl: DeviceDecl) {
        let decl = decl.normalize();
        self.entries.insert(decl.id.clone(), decl);
    }

    pub fn values(&self) -> impl Iterator<Item = &DeviceDecl> {
        self.entries.values()
    }

    pub fn get(&self, id: &str) -> Option<&DeviceDecl> {
        self.entries.get(id)
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub fn snapshot(&self) -> Vec<DeviceDecl> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> Vec<DeviceDecl> {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> Vec<DeviceDecl> {
        self.entries
            .values()
            .filter(|decl| features_enabled(&decl.feature_flags, active_features))
            .cloned()
            .collect()
    }
}

/// Engine-owned capability registry for the new transport model.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRegistry {
    plugins: PluginRegistry,
    types: TypeRegistry,
    adapters: AdapterRegistry,
    nodes: NodeRegistry,
    serializers: SerializerRegistry,
    devices: DeviceRegistry,
}

/// Deterministic frozen capability snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRegistrySnapshot {
    pub plugins: Vec<PluginManifest>,
    pub types: Vec<TypeDecl>,
    pub adapters: Vec<AdapterDecl>,
    pub nodes: Vec<NodeDecl>,
    pub serializers: Vec<SerializerDecl>,
    pub devices: Vec<DeviceDecl>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn plugins(&self) -> &PluginRegistry {
        &self.plugins
    }

    pub fn types(&self) -> &TypeRegistry {
        &self.types
    }

    pub fn adapters(&self) -> &AdapterRegistry {
        &self.adapters
    }

    pub fn nodes(&self) -> &NodeRegistry {
        &self.nodes
    }

    pub fn serializers(&self) -> &SerializerRegistry {
        &self.serializers
    }

    pub fn devices(&self) -> &DeviceRegistry {
        &self.devices
    }

    pub fn register_plugin(&mut self, manifest: PluginManifest) -> RegistryResult<()> {
        self.plugins.register(manifest)
    }

    pub fn replace_plugin(&mut self, manifest: PluginManifest) {
        self.plugins.replace(manifest);
    }

    pub fn register_type(&mut self, decl: TypeDecl) -> RegistryResult<()> {
        self.types.register(decl)
    }

    pub fn replace_type(&mut self, decl: TypeDecl) {
        self.types.replace(decl);
    }

    pub fn register_adapter(&mut self, decl: AdapterDecl) -> RegistryResult<()> {
        self.adapters.register(decl)
    }

    pub fn replace_adapter(&mut self, decl: AdapterDecl) {
        self.adapters.replace(decl);
    }

    pub fn register_node(&mut self, decl: NodeDecl) -> RegistryResult<()> {
        self.nodes.register(decl)
    }

    pub fn replace_node(&mut self, decl: NodeDecl) {
        self.nodes.replace(decl);
    }

    pub fn register_serializer(&mut self, decl: SerializerDecl) -> RegistryResult<()> {
        self.serializers.register(decl)
    }

    pub fn replace_serializer(&mut self, decl: SerializerDecl) {
        self.serializers.replace(decl);
    }

    pub fn register_device(&mut self, decl: DeviceDecl) -> RegistryResult<()> {
        self.devices.register(decl)
    }

    pub fn replace_device(&mut self, decl: DeviceDecl) {
        self.devices.replace(decl);
    }

    pub fn type_decl(&self, key: &TypeKey) -> Option<&TypeDecl> {
        self.types.get(key)
    }

    pub fn plugin_manifest(&self, id: &str) -> Option<&PluginManifest> {
        self.plugins.get(id)
    }

    pub fn adapter_decl(&self, id: &AdapterId) -> Option<&AdapterDecl> {
        self.adapters.get(id)
    }

    pub fn node_decl(&self, id: &NodeId) -> Option<&NodeDecl> {
        self.nodes.get(id)
    }

    pub fn serializer_decl(&self, id: &str) -> Option<&SerializerDecl> {
        self.serializers.get(id)
    }

    pub fn device_decl(&self, id: &str) -> Option<&DeviceDecl> {
        self.devices.get(id)
    }

    pub fn snapshot(&self) -> CapabilityRegistrySnapshot {
        CapabilityRegistrySnapshot {
            plugins: self.plugins.snapshot(),
            types: self.types.snapshot(),
            adapters: self.adapters.snapshot(),
            nodes: self.nodes.snapshot(),
            serializers: self.serializers.snapshot(),
            devices: self.devices.snapshot(),
        }
    }

    pub fn snapshot_filtered(&self, active_features: &[String]) -> CapabilityRegistrySnapshot {
        let active_features = active_feature_set(active_features);
        self.snapshot_filtered_with_features(&active_features)
    }

    fn snapshot_filtered_with_features(
        &self,
        active_features: &ActiveFeatureSet<'_>,
    ) -> CapabilityRegistrySnapshot {
        CapabilityRegistrySnapshot {
            plugins: self
                .plugins
                .snapshot_filtered_with_features(active_features),
            types: self.types.snapshot_filtered_with_features(active_features),
            adapters: self
                .adapters
                .snapshot_filtered_with_features(active_features),
            nodes: self.nodes.snapshot_filtered_with_features(active_features),
            serializers: self
                .serializers
                .snapshot_filtered_with_features(active_features),
            devices: self
                .devices
                .snapshot_filtered_with_features(active_features),
        }
    }

    /// Validate cross-capability references and return an immutable deterministic snapshot.
    pub fn freeze(&self) -> RegistryResult<CapabilityRegistrySnapshot> {
        for plugin in self.plugins.values() {
            for dep in &plugin.dependencies {
                if !self.plugins.contains_key(dep) {
                    return Err(missing_dependency(format!(
                        "plugin {} dependency {}",
                        plugin.id, dep
                    )));
                }
            }
            for ty in &plugin.provided_types {
                if !self.types.contains_key(ty) {
                    return Err(missing_dependency(format!(
                        "plugin {} provided type {}",
                        plugin.id, ty
                    )));
                }
            }
            for node in &plugin.provided_nodes {
                if self.nodes.get(node).is_none() {
                    return Err(missing_dependency(format!(
                        "plugin {} provided node {}",
                        plugin.id, node
                    )));
                }
            }
            for adapter in &plugin.provided_adapters {
                if !self.adapters.contains_key(adapter) {
                    return Err(missing_dependency(format!(
                        "plugin {} provided adapter {}",
                        plugin.id, adapter
                    )));
                }
            }
            for serializer in &plugin.provided_serializers {
                if !self.serializers.contains_key(serializer) {
                    return Err(missing_dependency(format!(
                        "plugin {} provided serializer {}",
                        plugin.id, serializer
                    )));
                }
            }
            for device in &plugin.provided_devices {
                if !self.devices.contains_key(device) {
                    return Err(missing_dependency(format!(
                        "plugin {} provided device {}",
                        plugin.id, device
                    )));
                }
            }
        }

        for adapter in self.adapters.values() {
            if !self.types.contains_key(&adapter.from) {
                return Err(missing_dependency(format!(
                    "adapter {} source type {}",
                    adapter.id, adapter.from
                )));
            }
            if !self.types.contains_key(&adapter.to) {
                return Err(missing_dependency(format!(
                    "adapter {} target type {}",
                    adapter.id, adapter.to
                )));
            }
        }

        for node in self.nodes.values() {
            for port in node.inputs.iter().chain(node.outputs.iter()) {
                if !self.types.contains_key(&port.type_key) {
                    return Err(missing_dependency(format!(
                        "node {} port {} type {}",
                        node.id.0, port.name, port.type_key
                    )));
                }
            }
        }

        for serializer in self.serializers.values() {
            if !self.types.contains_key(&serializer.type_key) {
                return Err(missing_dependency(format!(
                    "serializer {} type {}",
                    serializer.id, serializer.type_key
                )));
            }
        }

        for device in self.devices.values() {
            if !self.types.contains_key(&device.cpu) {
                return Err(missing_dependency(format!(
                    "device {} cpu type {}",
                    device.id, device.cpu
                )));
            }
            if !self.types.contains_key(&device.device) {
                return Err(missing_dependency(format!(
                    "device {} device type {}",
                    device.id, device.device
                )));
            }
            if !self.adapters.contains_key(&device.upload) {
                return Err(missing_dependency(format!(
                    "device {} upload adapter {}",
                    device.id, device.upload
                )));
            }
            if !self.adapters.contains_key(&device.download) {
                return Err(missing_dependency(format!(
                    "device {} download adapter {}",
                    device.id, device.download
                )));
            }
        }

        Ok(self.snapshot())
    }

    pub fn freeze_filtered(
        &self,
        active_features: &[String],
    ) -> RegistryResult<CapabilityRegistrySnapshot> {
        self.freeze()?;
        Ok(self.snapshot_filtered(active_features))
    }
}

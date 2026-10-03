//! Plugin installation mechanics and manifest discovery.

use super::*;

impl RegistryPluginExt for PluginRegistry {
    fn install_plugin<P: Plugin + ?Sized>(&mut self, plugin: &P) -> PluginResult<()> {
        self.ensure_open()?;
        let prev = self.current_prefix.take();
        let combined_prefix = if let Some(parent) = &prev {
            crate::apply_node_prefix(parent, plugin.id())
        } else {
            plugin.id().to_string()
        };
        let before = InstalledCapabilityKeys::from_registry(self);
        let before_overrides = self.overridden_capabilities.clone();
        self.current_prefix = Some(combined_prefix);
        let mut ctx = PluginInstallContext::new(self, plugin.manifest());
        let res = plugin.install(&mut ctx);
        let mut declared_manifest = ctx.into_manifest();
        let registry = self;
        registry.current_prefix = prev;
        if res.is_ok() {
            let after = InstalledCapabilityKeys::from_registry(registry);
            let mut discovered = after.diff_manifest(plugin.id(), &before);
            let override_diff = registry
                .overridden_capabilities
                .diff_manifest(plugin.id(), &before_overrides);
            discovered = merge_plugin_manifests(discovered, override_diff);
            declared_manifest = merge_plugin_manifests(declared_manifest, discovered);
            registry.plugin_manifests.insert(
                plugin.id().to_string(),
                normalize_plugin_manifest(declared_manifest),
            );
            registry
                .provider_source_kinds
                .insert(plugin.id().to_string(), CapabilitySourceKind::UserPlugin);
        }
        res
    }
}

fn merge_plugin_manifests(mut base: PluginManifest, discovered: PluginManifest) -> PluginManifest {
    base.provided_types.extend(discovered.provided_types);
    base.provided_nodes.extend(discovered.provided_nodes);
    base.provided_adapters.extend(discovered.provided_adapters);
    base.provided_serializers
        .extend(discovered.provided_serializers);
    base.provided_devices.extend(discovered.provided_devices);
    base.boundary_contracts
        .extend(discovered.boundary_contracts);
    base.feature_flags.extend(discovered.feature_flags);
    normalize_plugin_manifest(base)
}

pub(super) fn normalize_plugin_manifest(mut manifest: PluginManifest) -> PluginManifest {
    manifest.dependencies.sort();
    manifest.dependencies.dedup();
    manifest.provided_types.sort();
    manifest.provided_types.dedup();
    manifest.provided_nodes.sort();
    manifest.provided_nodes.dedup();
    manifest.provided_adapters.sort();
    manifest.provided_adapters.dedup();
    manifest.provided_serializers.sort();
    manifest.provided_serializers.dedup();
    manifest.provided_devices.sort();
    manifest.provided_devices.dedup();
    manifest
        .boundary_contracts
        .sort_by(|a, b| a.type_key.cmp(&b.type_key));
    manifest
        .boundary_contracts
        .dedup_by(|a, b| a.type_key == b.type_key);
    manifest.required_host_capabilities.sort();
    manifest.required_host_capabilities.dedup();
    manifest.feature_flags.sort();
    manifest.feature_flags.dedup();
    manifest
}

#[derive(Clone, Default)]
pub(super) struct InstalledCapabilityKeys {
    pub(super) types: BTreeSet<TypeKey>,
    pub(super) nodes: BTreeSet<NodeId>,
    pub(super) adapters: BTreeSet<AdapterId>,
    pub(super) serializers: BTreeSet<String>,
    pub(super) devices: BTreeSet<String>,
}

impl InstalledCapabilityKeys {
    pub(super) fn from_registry(registry: &PluginRegistry) -> Self {
        let mut keys = Self::default();
        keys.extend(registry.transport_capabilities.snapshot());
        keys
    }

    pub(super) fn extend(&mut self, snapshot: CapabilityRegistrySnapshot) {
        self.types
            .extend(snapshot.types.into_iter().map(|decl| decl.key));
        self.nodes
            .extend(snapshot.nodes.into_iter().map(|decl| decl.id));
        self.adapters
            .extend(snapshot.adapters.into_iter().map(|decl| decl.id));
        self.serializers
            .extend(snapshot.serializers.into_iter().map(|decl| decl.id));
        self.devices
            .extend(snapshot.devices.into_iter().map(|decl| decl.id));
    }

    pub(super) fn diff_manifest(&self, id: &str, before: &Self) -> PluginManifest {
        let mut manifest = PluginManifest::new(id);
        for key in self.types.difference(&before.types) {
            manifest = manifest.provided_type(key.clone());
        }
        for node in self.nodes.difference(&before.nodes) {
            manifest = manifest.provided_node(node.0.clone());
        }
        for adapter in self.adapters.difference(&before.adapters) {
            manifest = manifest.provided_adapter(adapter.as_str());
        }
        for serializer in self.serializers.difference(&before.serializers) {
            manifest = manifest.provided_serializer(serializer.clone());
        }
        for device in self.devices.difference(&before.devices) {
            manifest = manifest.provided_device(device.clone());
        }
        manifest
    }
}

/// Install a set of plugins, accumulating handlers. Stops at the first error.
pub fn install_all<P: Plugin>(
    registry: &mut PluginRegistry,
    plugins: impl IntoIterator<Item = P>,
) -> PluginResult<HandlerRegistry> {
    for plugin in plugins {
        registry.install_plugin(&plugin)?;
    }
    registry.freeze()?;
    let mut handlers = HandlerRegistry::new();
    handlers.merge(std::mem::take(&mut registry.handlers));
    Ok(handlers)
}

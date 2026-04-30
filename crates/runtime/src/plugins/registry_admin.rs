use super::*;

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        let mut registry = Self::bare();
        registry
            .install_standard_builtins()
            .expect("standard built-in capability installation failed");
        registry
    }

    pub fn bare() -> Self {
        Self {
            handlers: HandlerRegistry::new(),
            runtime_transport: RuntimeTransport::new(),
            transport_capabilities: TransportCapabilityRegistry::new(),
            type_registry: TypeRegistry::new(),
            plugin_manifests: BTreeMap::new(),
            boundary_contracts: BTreeMap::new(),
            current_prefix: None,
            capabilities: RuntimeCapabilityRegistry::new(),
            const_coercers: crate::io::new_const_coercer_map(),
            value_serializers: crate::host_bridge::new_value_serializer_map(),
            provider_source_kinds: BTreeMap::new(),
            overridden_capabilities: InstalledCapabilityKeys::default(),
            frozen: false,
        }
    }

    pub fn uninstall<P: CapabilityProviderRef>(&mut self, provider: P) -> PluginResult<()> {
        self.ensure_open()?;
        let id = provider.provider_id();
        let manifest = self
            .plugin_manifests
            .remove(id)
            .ok_or(PluginError::CapabilityProviderNotInstalled)?;
        self.provider_source_kinds.remove(id);
        self.rebuild_transport_capabilities_without(&manifest)
    }

    pub(super) fn ensure_open(&self) -> PluginResult<()> {
        if self.frozen {
            Err(PluginError::RegistryFrozen)
        } else {
            Ok(())
        }
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub fn install<I: PluginInstallable + ?Sized>(&mut self, installable: &I) -> PluginResult<()> {
        installable.install_into(self)
    }

    pub fn install_and_freeze<I: PluginInstallable + ?Sized>(
        &mut self,
        installable: &I,
    ) -> PluginResult<CapabilityRegistrySnapshot> {
        self.install(installable)?;
        self.freeze()
    }

    pub fn freeze(&mut self) -> PluginResult<CapabilityRegistrySnapshot> {
        let merged = self.combined_transport_capabilities()?;
        let snapshot = merged.freeze().map_err(|source| {
            PluginError::registry("plugin registry freeze validation failed", source)
        })?;
        self.frozen = true;
        Ok(snapshot)
    }

    pub fn merge<N: NodeInstall>(&mut self) -> PluginResult<()> {
        self.ensure_open()?;
        N::register(self)
    }

    pub fn take_handlers(&mut self) -> HandlerRegistry {
        std::mem::take(&mut self.handlers)
    }

    pub fn handlers(&self) -> HandlerRegistry {
        self.handlers.clone()
    }

    pub fn take_runtime_transport(&mut self) -> RuntimeTransport {
        std::mem::take(&mut self.runtime_transport)
    }

    pub fn take_transport_capabilities(&mut self) -> TransportCapabilityRegistry {
        std::mem::take(&mut self.transport_capabilities)
    }
}

impl PluginRegistry {
    fn rebuild_transport_capabilities_without(
        &mut self,
        manifest: &PluginManifest,
    ) -> PluginResult<()> {
        let removed_types = manifest
            .provided_types
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_nodes = manifest
            .provided_nodes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_adapters = manifest
            .provided_adapters
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_serializers = manifest
            .provided_serializers
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_devices = manifest
            .provided_devices
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        let snapshot = self.transport_capabilities.snapshot();
        let mut rebuilt = TransportCapabilityRegistry::new();

        for plugin in snapshot.plugins {
            if plugin.id != manifest.id {
                rebuilt.register_plugin(plugin).map_err(|source| {
                    PluginError::registry("transport plugin capability rebuild failed", source)
                })?;
            }
        }
        for decl in snapshot.types {
            if !removed_types.contains(&decl.key) {
                rebuilt.register_type(decl).map_err(|source| {
                    PluginError::registry("transport type capability rebuild failed", source)
                })?;
            }
        }
        for decl in snapshot.adapters {
            if !removed_adapters.contains(&decl.id) {
                rebuilt.register_adapter(decl).map_err(|source| {
                    PluginError::registry("transport adapter capability rebuild failed", source)
                })?;
            }
        }
        for decl in snapshot.nodes {
            if !removed_nodes.contains(&decl.id) {
                rebuilt.register_node(decl).map_err(|source| {
                    PluginError::registry("transport node capability rebuild failed", source)
                })?;
            }
        }
        for decl in snapshot.serializers {
            if !removed_serializers.contains(&decl.id) {
                rebuilt.register_serializer(decl).map_err(|source| {
                    PluginError::registry("transport serializer capability rebuild failed", source)
                })?;
            }
        }
        for decl in snapshot.devices {
            if !removed_devices.contains(&decl.id) {
                rebuilt.register_device(decl).map_err(|source| {
                    PluginError::registry("transport device capability rebuild failed", source)
                })?;
            }
        }

        self.transport_capabilities = rebuilt;
        Ok(())
    }

    pub(super) fn remove_builtin_type_source(&mut self, key: &TypeKey) -> bool {
        self.remove_builtin_source(|manifest| remove_item(&mut manifest.provided_types, key))
    }

    pub(super) fn remove_builtin_node_source(&mut self, id: &NodeId) -> bool {
        self.remove_builtin_source(|manifest| remove_item(&mut manifest.provided_nodes, id))
    }

    pub(super) fn remove_builtin_adapter_source(&mut self, id: &AdapterId) -> bool {
        self.remove_builtin_source(|manifest| remove_item(&mut manifest.provided_adapters, id))
    }

    pub(super) fn remove_builtin_device_source(&mut self, id: &str) -> bool {
        self.remove_builtin_source(|manifest| remove_item(&mut manifest.provided_devices, id))
    }

    fn remove_builtin_source(
        &mut self,
        mut remove: impl FnMut(&mut PluginManifest) -> bool,
    ) -> bool {
        let mut removed = false;
        let mut updated = Vec::new();
        for manifest in self.plugin_manifests.values_mut() {
            if self
                .provider_source_kinds
                .get(&manifest.id)
                .is_some_and(|kind| *kind == CapabilitySourceKind::BuiltIn)
                && remove(manifest)
            {
                updated.push(normalize_plugin_manifest(manifest.clone()));
                removed = true;
            }
        }
        for manifest in updated {
            self.transport_capabilities.replace_plugin(manifest);
        }
        removed
    }

    /// Return the plugin-native transport capability registry plus plugin manifests.
    pub fn combined_transport_capabilities(&self) -> PluginResult<TransportCapabilityRegistry> {
        let mut merged = self.transport_capabilities.clone();
        for plugin in self.plugin_manifests.values() {
            if merged.plugin_manifest(&plugin.id).is_none() {
                merged.register_plugin(plugin.clone()).map_err(|source| {
                    PluginError::registry("transport plugin capability merge failed", source)
                })?;
            }
        }

        merged.freeze().map_err(|source| {
            PluginError::registry("combined transport capability validation failed", source)
        })?;
        Ok(merged)
    }

    /// Attach this plugin registry's combined transport capabilities to a planner config.
    ///
    /// Direct planner callers should use this instead of planning against `registry` alone; plugin
    /// adapters and devices are native transport capabilities.
    pub fn planner_config_with_transport(
        &self,
        mut config: daedalus_planner::PlannerConfig,
    ) -> PluginResult<daedalus_planner::PlannerConfig> {
        config.transport_capabilities = Some(self.combined_transport_capabilities()?);
        Ok(config)
    }

    pub fn capability_sources(&self) -> PluginResult<Vec<CapabilitySource>> {
        let snapshot = self.combined_transport_capabilities()?.snapshot();
        let mut sources = Vec::new();
        let mut provided_types = BTreeSet::new();
        let mut provided_nodes = BTreeSet::new();
        let mut provided_adapters = BTreeSet::new();
        let mut provided_serializers = BTreeSet::new();
        let mut provided_devices = BTreeSet::new();

        for plugin in &snapshot.plugins {
            let source_kind = self.provider_source_kind(&plugin.id);
            for key in &plugin.provided_types {
                provided_types.insert(key.clone());
                sources.push(CapabilitySource {
                    capability_kind: "type",
                    capability_id: key.to_string(),
                    source_kind,
                    provider_id: Some(plugin.id.clone()),
                });
            }
            for id in &plugin.provided_nodes {
                provided_nodes.insert(id.clone());
                sources.push(CapabilitySource {
                    capability_kind: "node",
                    capability_id: id.to_string(),
                    source_kind,
                    provider_id: Some(plugin.id.clone()),
                });
            }
            for id in &plugin.provided_adapters {
                provided_adapters.insert(id.clone());
                sources.push(CapabilitySource {
                    capability_kind: "adapter",
                    capability_id: id.to_string(),
                    source_kind,
                    provider_id: Some(plugin.id.clone()),
                });
            }
            for id in &plugin.provided_serializers {
                provided_serializers.insert(id.clone());
                sources.push(CapabilitySource {
                    capability_kind: "serializer",
                    capability_id: id.clone(),
                    source_kind,
                    provider_id: Some(plugin.id.clone()),
                });
            }
            for id in &plugin.provided_devices {
                provided_devices.insert(id.clone());
                sources.push(CapabilitySource {
                    capability_kind: "device",
                    capability_id: id.clone(),
                    source_kind,
                    provider_id: Some(plugin.id.clone()),
                });
            }
        }

        for decl in snapshot.types {
            if !provided_types.contains(&decl.key) {
                sources.push(manual_capability_source("type", decl.key.to_string()));
            }
        }
        for decl in snapshot.nodes {
            if !provided_nodes.contains(&decl.id) {
                sources.push(manual_capability_source("node", decl.id.to_string()));
            }
        }
        for decl in snapshot.adapters {
            if !provided_adapters.contains(&decl.id) {
                sources.push(manual_capability_source("adapter", decl.id.to_string()));
            }
        }
        for decl in snapshot.serializers {
            if !provided_serializers.contains(&decl.id) {
                sources.push(manual_capability_source("serializer", decl.id));
            }
        }
        for decl in snapshot.devices {
            if !provided_devices.contains(&decl.id) {
                sources.push(manual_capability_source("device", decl.id));
            }
        }

        sources.sort_by(|a, b| {
            a.capability_kind
                .cmp(b.capability_kind)
                .then_with(|| a.capability_id.cmp(&b.capability_id))
                .then_with(|| a.provider_id.cmp(&b.provider_id))
        });
        Ok(sources)
    }

    /// Build graphs against plugin-native node declarations.
    pub fn graph_builder(&self) -> PluginResult<GraphBuilder> {
        Ok(GraphBuilder::new(self.combined_transport_capabilities()?))
    }

    fn provider_source_kind(&self, id: &str) -> CapabilitySourceKind {
        self.provider_source_kinds
            .get(id)
            .copied()
            .unwrap_or(CapabilitySourceKind::UserPlugin)
    }
}

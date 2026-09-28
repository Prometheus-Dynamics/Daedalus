//! Plugin groups and the install context handed to [`super::Plugin::install`].

use super::*;

/// Installable plugin family.
pub struct PluginGroup<'a> {
    id: &'static str,
    plugins: Vec<&'a dyn Plugin>,
}

impl<'a> PluginGroup<'a> {
    pub fn new(id: &'static str) -> Self {
        Self {
            id,
            plugins: Vec::new(),
        }
    }

    pub fn plugin(mut self, plugin: &'a dyn Plugin) -> Self {
        self.plugins.push(plugin);
        self
    }
}

/// Unified install target for plugins and plugin groups.
pub trait PluginInstallable {
    fn install_into(&self, registry: &mut PluginRegistry) -> PluginResult<()>;
}

impl<T: Plugin> PluginInstallable for T {
    fn install_into(&self, registry: &mut PluginRegistry) -> PluginResult<()> {
        registry.install_plugin(self)
    }
}

impl PluginInstallable for PluginGroup<'_> {
    fn install_into(&self, registry: &mut PluginRegistry) -> PluginResult<()> {
        registry.ensure_open()?;
        for plugin in &self.plugins {
            registry.install_plugin(*plugin)?;
        }
        let mut manifest = PluginManifest::new(self.id);
        for plugin in &self.plugins {
            manifest.dependencies.push(plugin.id().to_string());
        }
        let manifest = normalize_plugin_manifest(manifest);
        registry
            .transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| {
                PluginError::registry("plugin group manifest register failed", source)
            })?;
        registry
            .plugin_manifests
            .insert(self.id.to_string(), manifest);
        registry
            .provider_source_kinds
            .insert(self.id.to_string(), CapabilitySourceKind::PluginGroup);
        Ok(())
    }
}

pub struct PluginInstallContext<'a> {
    registry: &'a mut PluginRegistry,
    manifest: PluginManifest,
}

impl<'a> PluginInstallContext<'a> {
    pub(super) fn new(registry: &'a mut PluginRegistry, manifest: PluginManifest) -> Self {
        Self { registry, manifest }
    }

    pub fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    pub fn manifest_mut(&mut self) -> &mut PluginManifest {
        &mut self.manifest
    }

    pub fn dependency(&mut self, id: impl Into<String>) -> &mut Self {
        self.manifest.dependencies.push(id.into());
        self
    }

    pub fn required_host_capability(&mut self, capability: impl Into<String>) -> &mut Self {
        self.manifest
            .required_host_capabilities
            .push(capability.into());
        self
    }

    pub fn feature_flag(&mut self, flag: impl Into<String>) -> &mut Self {
        self.manifest.feature_flags.push(flag.into());
        self
    }

    pub fn boundary_contract(&mut self, contract: BoundaryTypeContract) -> PluginResult<&mut Self> {
        self.registry.register_boundary_contract(contract.clone())?;
        self.manifest.boundary_contracts.push(contract);
        Ok(self)
    }

    pub(super) fn into_manifest(self) -> PluginManifest {
        self.manifest
    }
}

impl Deref for PluginInstallContext<'_> {
    type Target = PluginRegistry;

    fn deref(&self) -> &Self::Target {
        self.registry
    }
}

impl DerefMut for PluginInstallContext<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.registry
    }
}

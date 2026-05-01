//! Plugin abstraction: a self-contained bundle that installs descriptors into the
//! registry and returns handlers that the runtime can execute.
use crate::capabilities::CapabilityRegistry as RuntimeCapabilityRegistry;
use crate::graph_builder::GraphBuilder;
use crate::handler_registry::HandlerRegistry;
use crate::transport::RuntimeTransport;
use daedalus_data::daedalus_type::DaedalusTypeExpr;
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_data::named_types::{HostExportPolicy, NamedTypeRegistry};
use daedalus_data::to_value::ToValue;
use daedalus_data::typing::TypeRegistry;
use daedalus_registry::capability::{
    AdapterDecl, CapabilityRegistry as TransportCapabilityRegistry, CapabilityRegistrySnapshot,
    DeviceDecl, ExportPolicy, NodeDecl, NodeExecutionKind, PluginManifest, SerializerDecl,
    TypeDecl,
};
use daedalus_registry::diagnostics::RegistryError;
use daedalus_registry::ids::NodeId;
use daedalus_transport::{
    AccessMode, AdaptCost, AdaptKind, AdaptRequest, AdapterId, BoundaryContractError,
    BoundaryTypeContract, BranchKind, BranchPayload, Layout, Payload, Residency, TransferFrom,
    TransferTo, TransportError, TypeKey,
};
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

mod builtins;
mod registry_admin;
mod registry_transport;

pub const BUILTIN_PRIMITIVE_TYPES_ID: &str = "daedalus.builtin.primitive_types";
pub const BUILTIN_PRIMITIVE_SERIALIZERS_ID: &str = "daedalus.builtin.primitive_serializers";
pub const BUILTIN_STD_BRANCH_ID: &str = "daedalus.builtin.std_branch";
pub const BUILTIN_HOST_BOUNDARY_ID: &str = "daedalus.builtin.host_boundary";

pub type PluginResult<T> = Result<T, PluginError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PluginError {
    #[error("plugin registry is frozen")]
    RegistryFrozen,
    #[error("capability provider is not installed")]
    CapabilityProviderNotInstalled,
    #[error("{operation}: {source}")]
    Registry {
        operation: &'static str,
        source: RegistryError,
    },
    #[error("transport adapter register failed: {source}")]
    TransportAdapterRegister { source: TransportError },
    #[error("boundary contract register failed: {source}")]
    BoundaryContract { source: BoundaryContractError },
    #[error("named type register failed: {message}")]
    NamedType { message: String },
    #[error("plugin install failed: {message}")]
    Install { message: String },
    #[error("{0}")]
    Message(&'static str),
}

impl PluginError {
    pub const fn registry(operation: &'static str, source: RegistryError) -> Self {
        Self::Registry { operation, source }
    }
}

impl From<&'static str> for PluginError {
    fn from(message: &'static str) -> Self {
        match message {
            "plugin registry is frozen" => Self::RegistryFrozen,
            "capability provider is not installed" => Self::CapabilityProviderNotInstalled,
            other => Self::Message(other),
        }
    }
}

impl From<RegistryError> for PluginError {
    fn from(source: RegistryError) -> Self {
        Self::Registry {
            operation: "capability registry operation failed",
            source,
        }
    }
}

impl From<BoundaryContractError> for PluginError {
    fn from(source: BoundaryContractError) -> Self {
        Self::BoundaryContract { source }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BuiltinCapability {
    PrimitiveTypes,
    PrimitiveSerializers,
    StdBranch,
    HostBoundary,
}

impl BuiltinCapability {
    pub const fn id(self) -> &'static str {
        match self {
            Self::PrimitiveTypes => BUILTIN_PRIMITIVE_TYPES_ID,
            Self::PrimitiveSerializers => BUILTIN_PRIMITIVE_SERIALIZERS_ID,
            Self::StdBranch => BUILTIN_STD_BRANCH_ID,
            Self::HostBoundary => BUILTIN_HOST_BOUNDARY_ID,
        }
    }
}

pub trait CapabilityProviderRef {
    fn provider_id(&self) -> &str;
}

impl CapabilityProviderRef for BuiltinCapability {
    fn provider_id(&self) -> &str {
        self.id()
    }
}

impl CapabilityProviderRef for str {
    fn provider_id(&self) -> &str {
        self
    }
}

impl CapabilityProviderRef for &str {
    fn provider_id(&self) -> &str {
        self
    }
}

impl CapabilityProviderRef for String {
    fn provider_id(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilitySourceKind {
    BuiltIn,
    UserPlugin,
    PluginGroup,
    Manual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilitySource {
    pub capability_kind: &'static str,
    pub capability_id: String,
    pub source_kind: CapabilitySourceKind,
    pub provider_id: Option<String>,
}

/// A plugin is the unit of composition for node bundles.
pub trait Plugin {
    /// Stable identifier for the plugin (e.g., bundle name).
    fn id(&self) -> &'static str;

    /// Declared plugin manifest. The install context augments this with concrete capabilities
    /// registered during installation.
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(self.id())
    }

    /// Install node descriptors, handlers, adapters, and device ops into an isolated install
    /// context. The context owns the mutable phase; consumers should use the frozen registry after
    /// all installs are complete.
    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()>;
}

/// Installable slice of a larger plugin.
pub trait PluginPart {
    fn install_part(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()>;
}

impl<F> PluginPart for F
where
    F: Fn(&mut PluginInstallContext<'_>) -> PluginResult<()>,
{
    fn install_part(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        self(ctx)
    }
}

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
    fn new(registry: &'a mut PluginRegistry, manifest: PluginManifest) -> Self {
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

    fn into_manifest(self) -> PluginManifest {
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

#[derive(Clone, Debug)]
pub struct TypedDeviceTransport {
    pub device_id: String,
    pub cpu: TypeExpr,
    pub device: TypeExpr,
    pub upload_id: String,
    pub download_id: String,
}

impl TypedDeviceTransport {
    pub fn new(
        device_id: impl Into<String>,
        cpu: TypeExpr,
        device: TypeExpr,
        upload_id: impl Into<String>,
        download_id: impl Into<String>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            cpu,
            device,
            upload_id: upload_id.into(),
            download_id: download_id.into(),
        }
    }
}

/// Extension trait so callers can say `registry.install_plugin(&plugin)` rather
/// than invoking the plugin directly.
pub trait RegistryPluginExt {
    fn install_plugin<P: Plugin + ?Sized>(&mut self, plugin: &P) -> PluginResult<()>;
}

/// Register a set of `DaedalusTypeExpr + ToValue` types as host-exportable values.
///
/// This is a convenience macro to reduce boilerplate in plugin `install()` functions.
///
#[macro_export]
macro_rules! register_daedalus_values {
    ($registry:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_daedalus_value::<$ty>()?; )+
        Ok::<(), $crate::plugins::PluginError>(())
    }};
}

/// Register a set of `DaedalusTypeExpr` types as named schemas (no `ToValue` required).
///
/// Useful for non-host-serialized types (e.g. large binary payloads) where you still want a
/// stable `TypeExpr::Opaque(<key>)` identity for UI typing and graph validation.
///
#[macro_export]
macro_rules! register_daedalus_types {
    ($registry:expr, $export:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_daedalus_type::<$ty>($export)?; )+
        Ok::<(), $crate::plugins::PluginError>(())
    }};
}

/// Register `ToValue` serializers for container/derived types that do not have stable type keys.
///
#[macro_export]
macro_rules! register_to_value_serializers {
    ($registry:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_to_value_serializer::<$ty>(); )+
    }};
}

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

fn normalize_plugin_manifest(mut manifest: PluginManifest) -> PluginManifest {
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
struct InstalledCapabilityKeys {
    types: BTreeSet<TypeKey>,
    nodes: BTreeSet<NodeId>,
    adapters: BTreeSet<AdapterId>,
    serializers: BTreeSet<String>,
    devices: BTreeSet<String>,
}

impl InstalledCapabilityKeys {
    fn from_registry(registry: &PluginRegistry) -> Self {
        let mut keys = Self::default();
        keys.extend(registry.transport_capabilities.snapshot());
        keys
    }

    fn extend(&mut self, snapshot: CapabilityRegistrySnapshot) {
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

    fn diff_manifest(&self, id: &str, before: &Self) -> PluginManifest {
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

/// Container for descriptors + handlers. All nodes are installed via plugins.
pub struct PluginRegistry {
    pub handlers: HandlerRegistry,
    pub runtime_transport: RuntimeTransport,
    pub transport_capabilities: TransportCapabilityRegistry,
    pub type_registry: TypeRegistry,
    pub named_type_registry: NamedTypeRegistry,
    pub plugin_manifests: BTreeMap<String, PluginManifest>,
    pub boundary_contracts: BTreeMap<TypeKey, BoundaryTypeContract>,
    pub current_prefix: Option<String>,
    pub capabilities: RuntimeCapabilityRegistry,
    pub const_coercers: crate::io::ConstCoercerMap,
    pub value_serializers: crate::host_bridge::ValueSerializerMap,
    provider_source_kinds: BTreeMap<String, CapabilitySourceKind>,
    overridden_capabilities: InstalledCapabilityKeys,
    frozen: bool,
}

/// Planner and capability metadata for a registered transport adapter.
#[derive(Clone, Debug)]
pub struct TransportAdapterOptions {
    pub cost: AdaptCost,
    pub access: AccessMode,
    pub requires_gpu: bool,
    pub residency: Option<Residency>,
    pub layout: Option<Layout>,
    pub feature_flags: Vec<String>,
}

impl Default for TransportAdapterOptions {
    fn default() -> Self {
        Self {
            cost: AdaptCost::materialize(),
            access: AccessMode::Read,
            requires_gpu: false,
            residency: None,
            layout: None,
            feature_flags: Vec::new(),
        }
    }
}

impl TransportAdapterOptions {
    pub fn kind(mut self, kind: AdaptKind) -> Self {
        self.cost.kind = kind;
        self
    }

    pub fn cost(mut self, cost: AdaptCost) -> Self {
        self.cost = cost;
        self
    }

    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn requires_gpu(mut self, requires_gpu: bool) -> Self {
        self.requires_gpu = requires_gpu;
        self
    }

    pub fn residency(mut self, residency: Residency) -> Self {
        self.residency = Some(residency);
        self
    }

    pub fn layout(mut self, layout: impl Into<Layout>) -> Self {
        self.layout = Some(layout.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    fn normalized(mut self) -> Self {
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Declared complex adapter surface for plugins.
///
/// A smart adapter is still just a payload-to-payload runtime function, but it also carries the
/// compile-time facts the planner needs to decide where it can be inserted. This keeps adapter
/// selection declared and deterministic instead of probing payloads at runtime.
pub trait SmartAdapter: Send + Sync + 'static {
    const ID: &'static str;
    const FROM: &'static str;
    const TO: &'static str;

    fn kind() -> AdaptKind {
        AdaptKind::Materialize
    }

    fn access() -> AccessMode {
        AccessMode::Read
    }

    fn cost() -> AdaptCost {
        AdaptCost::new(Self::kind())
    }

    fn requires_gpu() -> bool {
        false
    }

    fn residency() -> Option<Residency> {
        None
    }

    fn layout() -> Option<Layout> {
        None
    }

    fn feature_flags() -> &'static [&'static str] {
        &[]
    }

    fn adapt(payload: Payload, request: &AdaptRequest) -> Result<Payload, TransportError>;

    fn options() -> TransportAdapterOptions {
        let mut options = TransportAdapterOptions::default()
            .cost(Self::cost())
            .access(Self::access())
            .requires_gpu(Self::requires_gpu());
        if let Some(residency) = Self::residency() {
            options = options.residency(residency);
        }
        if let Some(layout) = Self::layout() {
            options = options.layout(layout);
        }
        for flag in Self::feature_flags() {
            options = options.feature_flag(*flag);
        }
        options
    }
}

pub trait NodeInstall {
    fn register(into: &mut PluginRegistry) -> PluginResult<()>;
}

fn typeexpr_transport_key(ty: &TypeExpr) -> TypeKey {
    daedalus_registry::typeexpr_transport_key(ty)
}

fn primitive_type_decls() -> impl IntoIterator<Item = ValueType> {
    daedalus_data::typing::BUILTIN_VALUE_TYPES.iter().copied()
}

fn host_export_policy_to_transport(policy: HostExportPolicy) -> ExportPolicy {
    match policy {
        HostExportPolicy::Value => ExportPolicy::Value,
        HostExportPolicy::Bytes => ExportPolicy::Bytes,
        HostExportPolicy::None => ExportPolicy::None,
        _ => ExportPolicy::None,
    }
}

fn remove_item<T, Q>(items: &mut Vec<T>, item: &Q) -> bool
where
    T: std::borrow::Borrow<Q>,
    Q: PartialEq + ?Sized,
{
    let len = items.len();
    items.retain(|entry| entry.borrow() != item);
    len != items.len()
}

fn manual_capability_source(
    capability_kind: &'static str,
    capability_id: String,
) -> CapabilitySource {
    CapabilitySource {
        capability_kind,
        capability_id,
        source_kind: CapabilitySourceKind::Manual,
        provider_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::Value;

    #[test]
    fn plugin_registry_type_registries_are_isolated() {
        struct LocalType;

        let mut left = PluginRegistry::bare();
        let right = PluginRegistry::bare();
        left.type_registry
            .register_type::<LocalType>(TypeExpr::Scalar(ValueType::Bool));

        assert_eq!(
            left.type_registry.lookup_type::<LocalType>(),
            Some(TypeExpr::Scalar(ValueType::Bool))
        );
        assert_eq!(right.type_registry.lookup_type::<LocalType>(), None);
    }

    #[test]
    fn plugin_registry_named_type_registries_are_isolated() {
        let mut left = PluginRegistry::bare();
        let right = PluginRegistry::bare();

        left.register_named_type(
            "test:registry:named",
            TypeExpr::Scalar(ValueType::Bool),
            HostExportPolicy::Value,
        )
        .expect("register named type");

        assert!(
            left.named_type_registry
                .lookup("test:registry:named")
                .is_some()
        );
        assert!(
            right
                .named_type_registry
                .lookup("test:registry:named")
                .is_none()
        );
    }

    #[test]
    fn plugin_registry_value_serializers_are_isolated() {
        #[derive(Clone)]
        struct LocalType(bool);

        let mut left = PluginRegistry::bare();
        let right = PluginRegistry::bare();
        left.register_value_serializer::<LocalType, _>(|value| Value::Bool(value.0));

        assert!(
            left.value_serializers
                .read()
                .expect("serializer lock")
                .contains_key(&std::any::TypeId::of::<LocalType>())
        );
        assert!(
            !right
                .value_serializers
                .read()
                .expect("serializer lock")
                .contains_key(&std::any::TypeId::of::<LocalType>())
        );
    }

    #[test]
    fn plugin_registry_transport_capabilities_are_isolated() {
        let mut left = PluginRegistry::bare();
        let right = PluginRegistry::bare();
        let key = TypeKey::new("test:isolated:type");

        left.register_transport_type_decl(key.clone(), TypeExpr::Scalar(ValueType::Bool))
            .expect("register transport type");

        assert!(left.transport_capabilities.type_decl(&key).is_some());
        assert!(right.transport_capabilities.type_decl(&key).is_none());
    }
}

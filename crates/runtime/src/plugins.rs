//! Plugin abstraction: a self-contained bundle that installs descriptors into the
//! registry and returns handlers that the runtime can execute.
use crate::capabilities::CapabilityRegistry as RuntimeCapabilityRegistry;
use crate::graph_builder::GraphBuilder;
use crate::handler_registry::HandlerRegistry;
use crate::prelude::*;
use crate::transport::RuntimeTransport;
use alloc::collections::{BTreeMap, BTreeSet};
use core::any::Any;
use core::ops::{Deref, DerefMut};
use daedalus_data::daedalus_type::{DaedalusTypeExpr, DaedalusTypeVisitor};
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
use daedalus_registry::typeexpr_transport_key;
use daedalus_transport::{
    AccessMode, AdaptCost, AdaptKind, AdaptRequest, AdapterId, BoundaryContractError,
    BoundaryTypeContract, BranchKind, BranchPayload, Layout, Payload, Residency, TransferFrom,
    TransferTo, TransportError, TypeKey,
};
use serde::de::DeserializeOwned;
use thiserror::Error;

mod adapters;
mod boundary_types;
mod builtins;
mod context;
mod crate_builds;
mod foreign;
mod install;
mod registry_admin;
mod registry_transport;
mod requirements;
mod stable_codecs;

pub use adapters::{SmartAdapter, TransportAdapterOptions};
pub use boundary_types::{BoundaryTypeConflict, ExternalTypeRef, PortTypeUse};
pub use context::{PluginGroup, PluginInstallContext, PluginInstallable};
pub use crate_builds::{CrateBuildDiff, CrateBuildInfo};
pub use install::install_all;
use install::{InstalledCapabilityKeys, normalize_plugin_manifest};
pub use stable_codecs::{StableCodec, StableDecodeFn, StableEncodeFn};

pub const BUILTIN_PRIMITIVE_TYPES_ID: &str = "daedalus.builtin.primitive_types";
pub const BUILTIN_PRIMITIVE_SERIALIZERS_ID: &str = "daedalus.builtin.primitive_serializers";
pub const BUILTIN_STD_BRANCH_ID: &str = "daedalus.builtin.std_branch";
pub const BUILTIN_NUMERIC_WIDENING_ID: &str = "daedalus.builtin.numeric_widening";
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
    /// A node port or adapter uses a type from another crate that declares no type key, so its
    /// key would be the order-dependent `rust:` fallback.
    #[error(
        "`{owner}` port `{port}` uses `{rust_type}` from another crate, which declares no type \
         key (it fell back to `{key}`, which depends on registration order); enable the owning \
         crate's `daedalus` integration (a `#[type_key]`/`DaedalusTypeExpr` key), set the port's \
         key (`port(name = \"{port}\", type_key = \"...\")`), or map the type on the plugin \
         (`#[plugin(foreign_types(Type = \"...\"))]`); a dynamic plugin whose dependency plugin \
         maps it links that plugin (`export_plugin!(Plugin, deps [DependencyPlugin])`)"
    )]
    UnkeyedForeignType {
        owner: String,
        port: String,
        rust_type: &'static str,
        key: TypeKey,
    },
    /// One transport key was recorded for two different Rust types.
    #[error(
        "type key `{}` is used for two Rust types, {} and {}; a type key must name exactly one \
         Rust type",
        .0.key, .0.registered, .0.new
    )]
    BoundaryTypeConflict(BoundaryTypeConflict),
    /// A Rust type was given a second key of its own.
    #[error(
        "Rust type `{rust_type}` already owns type key `{existing}` and cannot also own `{new}`; \
         a type owns one key (set a port's `type_key` to use another key at one port)"
    )]
    TypeKeyedTwice {
        rust_type: &'static str,
        existing: TypeKey,
        new: TypeKey,
    },
    /// A type key was declared again with another schema or export policy.
    #[error(
        "type key `{key}` is already declared as {existing:?} (export {existing_export:?}); it \
         cannot be redeclared as {new:?} (export {new_export:?})"
    )]
    TypeDeclarationConflict {
        key: TypeKey,
        existing: Option<TypeExpr>,
        existing_export: ExportPolicy,
        new: TypeExpr,
        new_export: ExportPolicy,
    },
    /// One foreign interface key was used with two versions or vtable layouts.
    #[error(
        "foreign interface {existing} conflicts with {new}; every user of an interface key must \
         agree on its version and vtable"
    )]
    ForeignInterfaceConflict {
        existing: daedalus_transport::ForeignInterfaceInfo,
        new: daedalus_transport::ForeignInterfaceInfo,
    },
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
    NumericWidening,
    HostBoundary,
}

impl BuiltinCapability {
    pub const fn id(self) -> &'static str {
        match self {
            Self::PrimitiveTypes => BUILTIN_PRIMITIVE_TYPES_ID,
            Self::PrimitiveSerializers => BUILTIN_PRIMITIVE_SERIALIZERS_ID,
            Self::StdBranch => BUILTIN_STD_BRANCH_ID,
            Self::NumericWidening => BUILTIN_NUMERIC_WIDENING_ID,
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

/// Container for descriptors + handlers. All nodes are installed via plugins.
pub struct PluginRegistry {
    pub handlers: HandlerRegistry,
    pub runtime_transport: RuntimeTransport,
    pub transport_capabilities: TransportCapabilityRegistry,
    pub type_registry: TypeRegistry,
    pub named_type_registry: NamedTypeRegistry,
    pub plugin_manifests: BTreeMap<String, PluginManifest>,
    pub boundary_contracts: BTreeMap<TypeKey, BoundaryTypeContract>,
    boundary_types: BTreeMap<TypeKey, daedalus_transport::RustTypeIdentity>,
    /// Keys each Rust type was recorded under (see `PluginRegistry::type_index`).
    type_key_uses: HashMap<core::any::TypeId, BTreeSet<TypeKey>>,
    foreign_interfaces: BTreeMap<TypeKey, daedalus_transport::ForeignInterfaceInfo>,
    /// Builds of third-party crates (see `PluginRegistry::register_crate_build`).
    crate_builds: BTreeMap<&'static str, CrateBuildInfo>,
    /// `Some` while extracting a dynamic plugin's schema: unkeyed foreign port types are
    /// recorded here instead of failing (see [`PluginRegistry::record_external_types`]).
    external_types: Option<Vec<ExternalTypeRef>>,
    /// `Some` in a dynamic plugin's stable `invoke` registry: node port codecs (see
    /// [`PluginRegistry::record_stable_codecs`]).
    stable_codecs: Option<stable_codecs::StableCodecMap>,
    pub current_prefix: Option<String>,
    pub capabilities: RuntimeCapabilityRegistry,
    pub const_coercers: crate::io::ConstCoercerMap,
    pub value_serializers: crate::host_bridge::ValueSerializerMap,
    provider_source_kinds: BTreeMap<String, CapabilitySourceKind>,
    overridden_capabilities: InstalledCapabilityKeys,
    frozen: bool,
}

pub trait NodeInstall {
    fn register(into: &mut PluginRegistry) -> PluginResult<()>;
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
    T: alloc::borrow::Borrow<Q>,
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
mod tests;

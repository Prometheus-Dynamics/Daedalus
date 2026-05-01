#![cfg(feature = "plugins")]

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_data::named_types::HostExportPolicy;
use daedalus_data::prelude::{DaedalusTypeExpr, ToValue};
use daedalus_registry::capability::{NodeDecl, PluginManifest, PortDecl};
use daedalus_runtime::plugins::{
    BuiltinCapability, CapabilitySourceKind, Plugin, PluginInstallContext, PluginRegistry,
    PluginResult, RegistryPluginExt,
};

fn scalar_int_key() -> daedalus_transport::TypeKey {
    daedalus_registry::typeexpr_transport_key(&TypeExpr::Scalar(ValueType::Int))
}

struct ManifestPlugin;

impl Plugin for ManifestPlugin {
    fn id(&self) -> &'static str {
        "phase7.demo"
    }

    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(self.id())
            .version("1.0.0")
            .dependency("phase7.base")
            .required_host_capability("host.payload")
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(
            NodeDecl::new("phase7.demo:source")
                .output(PortDecl::new("out", "phase7:i32").schema(TypeExpr::opaque("phase7:i32"))),
        )
    }
}

struct OverridePrimitivePlugin;

impl Plugin for OverridePrimitivePlugin {
    fn id(&self) -> &'static str {
        "phase7.override_primitive"
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_named_type(
            scalar_int_key().as_str(),
            TypeExpr::Scalar(ValueType::Int),
            HostExportPolicy::Value,
        )
    }
}

struct GroupNamedUserPlugin;

impl Plugin for GroupNamedUserPlugin {
    fn id(&self) -> &'static str {
        "phase7.group"
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(NodeDecl::new("phase7.group:source"))
    }
}

#[derive(Clone)]
struct IsolatedPayload(i64);

impl DaedalusTypeExpr for IsolatedPayload {
    const TYPE_KEY: &'static str = "phase7:isolated_payload";

    fn type_expr() -> TypeExpr {
        TypeExpr::Scalar(ValueType::Int)
    }
}

impl ToValue for IsolatedPayload {
    fn to_value(&self) -> Value {
        Value::Int(self.0)
    }
}

#[test]
fn install_context_finalizes_manifest_with_declared_and_provided_capabilities() {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&ManifestPlugin).expect("install");

    let manifest = registry
        .plugin_manifests
        .get("phase7.demo")
        .expect("manifest");
    assert_eq!(manifest.version.as_deref(), Some("1.0.0"));
    assert_eq!(manifest.dependencies, vec!["phase7.base"]);
    assert_eq!(manifest.required_host_capabilities, vec!["host.payload"]);
    assert!(
        manifest
            .provided_nodes
            .iter()
            .any(|id| id.0 == "phase7.demo:source")
    );
    assert!(
        manifest
            .provided_types
            .iter()
            .any(|key| key.as_str() == "phase7:i32")
    );
}

#[test]
fn frozen_registry_rejects_late_install_and_registration() {
    let mut registry = PluginRegistry::new();
    registry
        .register_node_decl(NodeDecl::new("phase7.base:source"))
        .expect("register node");
    registry.freeze().expect("freeze registry");

    assert!(registry.is_frozen());
    assert!(registry.install_plugin(&ManifestPlugin).is_err());
    assert!(registry.register_node_decl(NodeDecl::new("late")).is_err());
}

#[test]
fn new_registry_installs_and_uninstalls_builtins() {
    let mut registry = PluginRegistry::new();
    assert!(
        registry
            .plugin_manifests
            .contains_key("daedalus.builtin.primitive_types")
    );
    assert!(
        registry
            .combined_transport_capabilities()
            .expect("capabilities")
            .type_decl(&scalar_int_key())
            .is_some()
    );

    registry
        .uninstall(BuiltinCapability::PrimitiveTypes)
        .expect("uninstall primitive types");
    assert!(
        !registry
            .plugin_manifests
            .contains_key("daedalus.builtin.primitive_types")
    );
}

#[test]
fn user_plugin_can_override_builtin_capability_source() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&OverridePrimitivePlugin)
        .expect("install override");

    let manifest = registry
        .plugin_manifests
        .get("phase7.override_primitive")
        .expect("manifest");
    assert!(
        manifest
            .provided_types
            .iter()
            .any(|key| key == &scalar_int_key())
    );

    let sources = registry.capability_sources().expect("sources");
    let scalar_int_key = scalar_int_key().to_string();
    assert!(sources.iter().any(|source| {
        source.capability_kind == "type"
            && source.capability_id == scalar_int_key
            && source.source_kind == CapabilitySourceKind::UserPlugin
            && source.provider_id.as_deref() == Some("phase7.override_primitive")
    }));
    assert!(!sources.iter().any(|source| {
        source.capability_kind == "type"
            && source.capability_id == scalar_int_key
            && source.source_kind == CapabilitySourceKind::BuiltIn
    }));
}

#[test]
fn capability_source_kind_uses_install_metadata_not_provider_id_strings() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&GroupNamedUserPlugin)
        .expect("install group-named user plugin");

    let sources = registry.capability_sources().expect("sources");
    assert!(sources.iter().any(|source| {
        source.capability_kind == "node"
            && source.capability_id == "phase7.group:source"
            && source.source_kind == CapabilitySourceKind::UserPlugin
            && source.provider_id.as_deref() == Some("phase7.group")
    }));
}

#[test]
fn plugin_registries_do_not_leak_types_capabilities_or_serializers() {
    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();

    left.register_daedalus_value::<IsolatedPayload>()
        .expect("register isolated value");

    assert_eq!(
        left.type_registry.lookup_type::<IsolatedPayload>(),
        Some(TypeExpr::Opaque(IsolatedPayload::TYPE_KEY.to_string()))
    );
    assert_eq!(right.type_registry.lookup_type::<IsolatedPayload>(), None);

    let key = daedalus_transport::TypeKey::new(IsolatedPayload::TYPE_KEY);
    assert!(left.transport_capabilities.type_decl(&key).is_some());
    assert!(right.transport_capabilities.type_decl(&key).is_none());

    assert!(
        left.value_serializers
            .read()
            .expect("left serializer lock")
            .contains_key(&std::any::TypeId::of::<IsolatedPayload>())
    );
    assert!(
        !right
            .value_serializers
            .read()
            .expect("right serializer lock")
            .contains_key(&std::any::TypeId::of::<IsolatedPayload>())
    );
}

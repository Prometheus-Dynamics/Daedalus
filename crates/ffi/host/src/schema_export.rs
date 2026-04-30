use std::collections::{BTreeMap, BTreeSet};

use daedalus_data::model::TypeExpr;
use daedalus_ffi_core::{
    BackendKind, NodeSchema, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION, WirePort,
};
use daedalus_registry::capability::{
    CapabilityRegistry, CapabilityRegistrySnapshot, NodeDecl, PluginManifest, PortDecl,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaExportError {
    #[error("plugin manifest not found: {plugin_id}")]
    MissingPlugin { plugin_id: String },
    #[error("plugin {plugin_id} references missing node {node_id}")]
    MissingNode { plugin_id: String, node_id: String },
    #[error("invalid exported schema: {0}")]
    InvalidSchema(#[from] daedalus_ffi_core::FfiContractError),
    #[error("failed to serialize schema json: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn export_registry_plugin_schema(
    registry: &CapabilityRegistry,
    plugin_id: &str,
) -> Result<PluginSchema, SchemaExportError> {
    export_snapshot_plugin_schema(&registry.snapshot(), plugin_id)
}

pub fn export_registry_plugin_schema_json(
    registry: &CapabilityRegistry,
    plugin_id: &str,
) -> Result<String, SchemaExportError> {
    let schema = export_registry_plugin_schema(registry, plugin_id)?;
    Ok(serde_json::to_string_pretty(&schema)?)
}

pub fn export_snapshot_plugin_schema(
    snapshot: &CapabilityRegistrySnapshot,
    plugin_id: &str,
) -> Result<PluginSchema, SchemaExportError> {
    let plugin = snapshot
        .plugins
        .iter()
        .find(|plugin| plugin.id == plugin_id)
        .ok_or_else(|| SchemaExportError::MissingPlugin {
            plugin_id: plugin_id.to_string(),
        })?;
    plugin_schema_from_manifest(plugin, &snapshot.nodes)
}

pub fn plugin_schema_from_manifest(
    plugin: &PluginManifest,
    nodes: &[NodeDecl],
) -> Result<PluginSchema, SchemaExportError> {
    let node_by_id: BTreeMap<_, _> = nodes
        .iter()
        .map(|node| (node.id.to_string(), node))
        .collect();
    let mut schema_nodes = Vec::with_capacity(plugin.provided_nodes.len());
    for node_id in &plugin.provided_nodes {
        let node_id = node_id.to_string();
        let node = node_by_id
            .get(&node_id)
            .ok_or_else(|| SchemaExportError::MissingNode {
                plugin_id: plugin.id.clone(),
                node_id: node_id.clone(),
            })?;
        schema_nodes.push(node_schema_from_decl(node));
    }

    let mut dependencies = plugin.dependencies.clone();
    dependencies.sort();
    dependencies.dedup();
    let mut required_host_capabilities = plugin.required_host_capabilities.clone();
    required_host_capabilities.sort();
    required_host_capabilities.dedup();
    let mut feature_flags = plugin.feature_flags.clone();
    feature_flags.sort();
    feature_flags.dedup();

    let mut schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: plugin.id.clone(),
            version: plugin.version.clone(),
            description: None,
            metadata: plugin_metadata(plugin),
        },
        dependencies,
        required_host_capabilities,
        feature_flags,
        boundary_contracts: plugin.boundary_contracts.clone(),
        nodes: schema_nodes,
    };
    normalize_schema(&mut schema);
    schema.validate()?;
    Ok(schema)
}

pub fn node_schema_from_decl(node: &NodeDecl) -> NodeSchema {
    NodeSchema {
        id: node.id.to_string(),
        backend: BackendKind::Rust,
        entrypoint: node.id.to_string(),
        label: node.label.clone(),
        stateful: false,
        feature_flags: sorted(node.feature_flags.clone()),
        inputs: node.inputs.iter().map(wire_port_from_decl).collect(),
        outputs: node.outputs.iter().map(wire_port_from_decl).collect(),
        metadata: metadata_json(&node.metadata_json),
    }
}

fn wire_port_from_decl(port: &PortDecl) -> WirePort {
    WirePort {
        name: port.name.clone(),
        ty: port
            .schema
            .clone()
            .unwrap_or_else(|| TypeExpr::opaque(port.type_key.to_string())),
        type_key: Some(port.type_key.clone()),
        optional: false,
        access: port.access,
        residency: port.residency,
        layout: port.layout.clone(),
        source: port.source.clone(),
        const_value: port
            .const_value_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
    }
}

fn plugin_metadata(plugin: &PluginManifest) -> BTreeMap<String, serde_json::Value> {
    let mut metadata = BTreeMap::new();
    insert_string_array(
        &mut metadata,
        "provided_types",
        plugin.provided_types.iter(),
    );
    insert_string_array(
        &mut metadata,
        "provided_adapters",
        plugin.provided_adapters.iter(),
    );
    insert_string_array(
        &mut metadata,
        "provided_serializers",
        plugin.provided_serializers.iter(),
    );
    insert_string_array(
        &mut metadata,
        "provided_devices",
        plugin.provided_devices.iter(),
    );
    metadata
}

fn metadata_json(input: &BTreeMap<String, String>) -> BTreeMap<String, serde_json::Value> {
    input
        .iter()
        .map(|(key, value)| {
            let value = serde_json::from_str(value)
                .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
            (key.clone(), value)
        })
        .collect()
}

fn insert_string_array<'a, T>(
    metadata: &mut BTreeMap<String, serde_json::Value>,
    key: &str,
    values: impl Iterator<Item = &'a T>,
) where
    T: ToString + 'a,
{
    let values = values.map(ToString::to_string).collect::<Vec<_>>();
    if !values.is_empty() {
        metadata.insert(key.into(), serde_json::json!(values));
    }
}

fn normalize_schema(schema: &mut PluginSchema) {
    schema.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    schema
        .boundary_contracts
        .sort_by(|a, b| a.type_key.cmp(&b.type_key));
    schema
        .boundary_contracts
        .dedup_by(|a, b| a.type_key == b.type_key);
    schema.required_host_capabilities = sorted(schema.required_host_capabilities.clone());
    schema.dependencies = sorted(schema.dependencies.clone());
    schema.feature_flags = sorted(schema.feature_flags.clone());
}

fn sorted(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, Value, ValueType};
    use daedalus_registry::capability::{NodeExecutionKind, PortDecl};
    use daedalus_transport::{AccessMode, BoundaryCapabilities, BoundaryTypeContract, TypeKey};

    #[test]
    fn exports_plugin_schema_from_registry_manifest_and_nodes() {
        let mut registry = CapabilityRegistry::new();
        let contract = BoundaryTypeContract::new(
            TypeKey::new("demo.scalar"),
            "demo.scalar.layout",
            BoundaryCapabilities::default(),
        );
        registry
            .register_plugin(
                PluginManifest::new("demo.plugin")
                    .version("1.2.3")
                    .dependency("dep.plugin")
                    .provided_type(TypeKey::new("demo.scalar"))
                    .provided_node("demo:add")
                    .boundary_contract(contract.clone())
                    .required_host_capability("camera")
                    .feature_flag("fast-path"),
            )
            .expect("register plugin");
        registry
            .register_node(
                NodeDecl::new("demo:add")
                    .label("Add")
                    .execution_kind(NodeExecutionKind::HandlerRequired)
                    .input(
                        PortDecl::new("a", TypeKey::new("demo.scalar"))
                            .schema(TypeExpr::scalar(ValueType::Int))
                            .access(AccessMode::Read),
                    )
                    .output(
                        PortDecl::new("out", TypeKey::new("demo.scalar"))
                            .schema(TypeExpr::scalar(ValueType::Int))
                            .const_value(Value::Int(0)),
                    )
                    .feature_flag("scalar")
                    .metadata("category", Value::String("math".into())),
            )
            .expect("register node");

        let schema =
            export_registry_plugin_schema(&registry, "demo.plugin").expect("export schema");

        assert_eq!(schema.plugin.name, "demo.plugin");
        assert_eq!(schema.plugin.version.as_deref(), Some("1.2.3"));
        assert_eq!(schema.nodes.len(), 1);
        assert_eq!(schema.nodes[0].backend, BackendKind::Rust);
        assert_eq!(schema.nodes[0].entrypoint, "demo:add");
        assert_eq!(
            schema.nodes[0].inputs[0]
                .type_key
                .as_ref()
                .unwrap()
                .as_str(),
            "demo.scalar"
        );
        assert_eq!(
            schema.nodes[0].metadata.get("category"),
            Some(&serde_json::json!({"type": "String", "value": "math"}))
        );
        assert_eq!(schema.required_host_capabilities, vec!["camera"]);
        assert_eq!(schema.boundary_contracts, vec![contract]);
        assert!(schema.plugin.metadata.contains_key("provided_types"));

        let json = export_registry_plugin_schema_json(&registry, "demo.plugin").expect("json");
        assert!(json.contains("\"demo.plugin\""));
    }

    #[test]
    fn export_reports_missing_plugin_and_missing_node() {
        let registry = CapabilityRegistry::new();
        assert!(matches!(
            export_registry_plugin_schema(&registry, "missing"),
            Err(SchemaExportError::MissingPlugin { .. })
        ));

        let mut registry = CapabilityRegistry::new();
        registry
            .register_plugin(PluginManifest::new("demo").provided_node("missing.node"))
            .expect("register plugin");
        assert!(matches!(
            export_registry_plugin_schema(&registry, "demo"),
            Err(SchemaExportError::MissingNode { .. })
        ));
    }
}

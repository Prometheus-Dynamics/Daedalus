//! Node.js FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PackageArtifact,
    PackageArtifactKind, PluginPackage, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION,
    WirePayloadHandle, WirePort, bundled_artifact_path, validate_language_backends,
};
use thiserror::Error;

pub use daedalus_ffi_core as core;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePayloadTransport {
    pub buffer: bool,
    pub shared_memory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub source_files: Vec<String>,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeResolvedPayload {
    pub id: String,
    pub type_key: String,
    pub access: String,
    pub view: NodePayloadView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodePayloadView {
    Buffer { bytes_estimate: u64 },
    SharedMemory { name: String, offset: u64, len: u64 },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum NodePayloadResolveError {
    #[error("node payload transport supports neither Buffer nor shared memory")]
    UnsupportedTransport,
    #[error("payload handle `{0}` is missing `{1}` metadata")]
    MissingMetadata(String, &'static str),
}

impl NodePayloadTransport {
    pub fn buffer_and_shared_memory() -> Self {
        Self {
            buffer: true,
            shared_memory: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "payload_transport".into(),
            serde_json::json!({
                "buffer": self.buffer,
                "shared_memory": self.shared_memory,
            }),
        )])
    }
}

pub fn resolve_node_payload_handle(
    handle: &WirePayloadHandle,
    transport: &NodePayloadTransport,
) -> Result<NodeResolvedPayload, NodePayloadResolveError> {
    let view = if transport.shared_memory {
        if let Some(name) = metadata_string(handle, "shared_memory_name") {
            Some(NodePayloadView::SharedMemory {
                name,
                offset: metadata_u64(handle, "shared_memory_offset").unwrap_or(0),
                len: metadata_u64(handle, "shared_memory_len")
                    .or_else(|| metadata_u64(handle, "bytes_estimate"))
                    .ok_or_else(|| {
                        NodePayloadResolveError::MissingMetadata(
                            handle.id.clone(),
                            "shared_memory_len",
                        )
                    })?,
            })
        } else {
            None
        }
    } else {
        None
    };
    let view = match view {
        Some(view) => view,
        None if transport.buffer => NodePayloadView::Buffer {
            bytes_estimate: metadata_u64(handle, "bytes_estimate").ok_or_else(|| {
                NodePayloadResolveError::MissingMetadata(handle.id.clone(), "bytes_estimate")
            })?,
        },
        None => return Err(NodePayloadResolveError::UnsupportedTransport),
    };
    Ok(NodeResolvedPayload {
        id: handle.id.clone(),
        type_key: handle.type_key.to_string(),
        access: handle.access.to_string(),
        view,
    })
}

fn metadata_string(handle: &WirePayloadHandle, key: &'static str) -> Option<String> {
    handle
        .metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn metadata_u64(handle: &WirePayloadHandle, key: &'static str) -> Option<u64> {
    handle.metadata.get(key).and_then(serde_json::Value::as_u64)
}

pub fn validate_node_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Node)
}

pub fn node_worker_backend_config(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Node,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(module_path.into()),
        entry_class: None,
        entry_symbol: Some(function_name.into()),
        executable: Some("node".into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

pub fn node_worker_backend_config_with_transport(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
    transport: NodePayloadTransport,
) -> BackendConfig {
    let mut backend = node_worker_backend_config(module_path, function_name);
    backend.options.extend(transport.backend_options());
    backend
}

pub fn node_node_schema(
    node_id: impl Into<String>,
    function_name: impl Into<String>,
    inputs: Vec<WirePort>,
    outputs: Vec<WirePort>,
) -> NodeSchema {
    NodeSchema {
        id: node_id.into(),
        backend: BackendKind::Node,
        entrypoint: function_name.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs,
        outputs,
        metadata: BTreeMap::new(),
    }
}

pub fn node_plugin_schema(
    plugin_name: impl Into<String>,
    version: Option<String>,
    nodes: Vec<NodeSchema>,
) -> Result<PluginSchema, FfiContractError> {
    let mut schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: plugin_name.into(),
            version,
            description: None,
            metadata: BTreeMap::new(),
        },
        dependencies: Vec::new(),
        required_host_capabilities: Vec::new(),
        feature_flags: Vec::new(),
        boundary_contracts: Vec::new(),
        nodes,
    };
    schema.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    schema.validate_backend_kind(BackendKind::Node)?;
    Ok(schema)
}

pub fn node_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    NodePackageInput {
        schema,
        backends,
        source_files,
        lockfile: None,
        metadata: BTreeMap::new(),
    }
    .build()
}

impl NodePackageInput {
    pub fn build(self) -> Result<PluginPackage, FfiContractError> {
        validate_language_backends(&self.schema, &self.backends, BackendKind::Node)?;
        let mut metadata = self.metadata;
        metadata.insert("language".into(), serde_json::json!("node"));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!("daedalus-ffi-node"),
        );

        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(self.schema),
            backends: self.backends,
            artifacts: source_file_artifacts(BackendKind::Node, self.source_files)?,
            lockfile: self.lockfile.or_else(|| Some("plugin.lock.json".into())),
            manifest_hash: None,
            signature: None,
            metadata,
        };
        package.validate()?;
        package.manifest_hash = Some(package.compute_manifest_hash()?);
        Ok(package)
    }
}

pub fn node_complete_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    NodePackageInput {
        schema,
        backends,
        source_files,
        lockfile: Some("plugin.lock.json".into()),
        metadata: BTreeMap::new(),
    }
    .build()
}

fn source_file_artifacts(
    backend: BackendKind,
    source_files: Vec<String>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    source_files
        .into_iter()
        .map(|path| {
            Ok(PackageArtifact {
                path: bundled_artifact_path(PackageArtifactKind::SourceFile, &path, None)?,
                kind: PackageArtifactKind::SourceFile,
                backend: Some(backend.clone()),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{FixtureLanguage, generate_language_fixture, scalar_add_fixture_spec};

    #[test]
    fn validates_node_schema_and_backends() {
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo.node".into(),
                version: None,
                description: None,
                metadata: Default::default(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![NodeSchema {
                id: "demo:add".into(),
                backend: BackendKind::Node,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: Vec::new(),
                outputs: vec![WirePort {
                    name: "out".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                metadata: Default::default(),
            }],
        };
        let backends = BTreeMap::from([(
            "demo:add".into(),
            BackendConfig {
                backend: BackendKind::Node,
                runtime_model: BackendRuntimeModel::PersistentWorker,
                entry_module: Some("demo.mjs".into()),
                entry_class: None,
                entry_symbol: Some("add".into()),
                executable: Some("node".into()),
                args: Vec::new(),
                classpath: Vec::new(),
                native_library_paths: Vec::new(),
                working_dir: None,
                env: Default::default(),
                options: Default::default(),
            },
        )]);

        validate_node_schema(&schema, &backends).expect("valid node schema");
        assert!(matches!(
            validate_node_schema(&schema, &BTreeMap::new()),
            Err(FfiContractError::MissingBackendConfig { .. })
        ));
    }

    #[test]
    fn sdk_builders_match_rust_baseline_schema_surface() {
        let spec = scalar_add_fixture_spec();
        let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
        let node_fixture =
            generate_language_fixture(&spec, FixtureLanguage::Node).expect("node fixture");
        let baseline = &rust.schema.nodes[0];

        let node = node_node_schema(
            baseline.id.clone(),
            node_fixture.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = node_plugin_schema(
            "ffi.conformance.node.scalar_add",
            Some("1.0.0".into()),
            vec![node],
        )
        .expect("schema");
        let backend = node_worker_backend_config("scalar_add.mjs", "add");
        let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
        let package = node_plugin_package(
            schema.clone(),
            backends.clone(),
            vec!["scalar_add.mjs".into()],
        )
        .expect("package");

        assert_eq!(schema.nodes[0].id, baseline.id);
        assert_eq!(schema.nodes[0].inputs, baseline.inputs);
        assert_eq!(schema.nodes[0].outputs, baseline.outputs);
        assert_eq!(schema.nodes[0].stateful, baseline.stateful);
        assert_eq!(backend, node_fixture.backends[&baseline.id]);
        assert_eq!(package.schema.as_ref(), Some(&schema));
        assert_eq!(package.backends, backends);
        assert_eq!(package.artifacts[0].path, "_bundle/src/scalar_add.mjs");
        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        validate_node_schema(&schema, &package.backends).expect("valid package schema");
    }

    #[test]
    fn node_transport_options_enable_buffer_and_shared_memory() {
        let backend = node_worker_backend_config_with_transport(
            "plugin.mjs",
            "run",
            NodePayloadTransport::buffer_and_shared_memory(),
        );
        assert_eq!(
            backend.options.get("payload_transport"),
            Some(&serde_json::json!({"buffer": true, "shared_memory": true}))
        );
    }

    #[test]
    fn node_resolves_payload_handles_to_shared_memory_or_buffer_views() {
        let shared_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-1",
            "type_key": "bytes",
            "access": "read",
            "metadata": {
                "shared_memory_name": "daedalus-payload-1",
                "shared_memory_offset": 4,
                "shared_memory_len": 128,
                "bytes_estimate": 128
            }
        }))
        .expect("handle");
        let resolved = resolve_node_payload_handle(
            &shared_handle,
            &NodePayloadTransport::buffer_and_shared_memory(),
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            NodePayloadView::SharedMemory {
                name: "daedalus-payload-1".into(),
                offset: 4,
                len: 128
            }
        );

        let buffer_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-2",
            "type_key": "bytes",
            "access": "view",
            "metadata": {"bytes_estimate": 16}
        }))
        .expect("handle");
        let resolved = resolve_node_payload_handle(
            &buffer_handle,
            &NodePayloadTransport {
                buffer: true,
                shared_memory: false,
            },
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            NodePayloadView::Buffer { bytes_estimate: 16 }
        );
        assert_eq!(resolved.access, "view");
    }

    #[test]
    fn complete_node_package_emits_lockfile_hash_and_language_metadata() {
        let spec = scalar_add_fixture_spec();
        let fixture =
            generate_language_fixture(&spec, FixtureLanguage::Node).expect("node fixture");
        let package = node_complete_plugin_package(
            fixture.schema.clone(),
            fixture.backends.clone(),
            vec!["src/plugin.ts".into(), "src/build-package.ts".into()],
        )
        .expect("complete package");
        let lock = package.generate_lockfile();

        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus-ffi-node"))
        );
        assert_eq!(package.artifacts.len(), 2);
        assert_eq!(
            lock.plugin_name.as_deref(),
            Some("ffi.conformance.node.scalar_add")
        );
        assert_eq!(lock.artifacts.len(), 2);
    }
}

//! Node.js FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, FfiContractError, LanguagePackageInput, LanguagePackager,
    MappedPayloadKeys, PackageArtifactKind, PayloadResolveError, PayloadView, PluginPackage,
    PluginSchema, ResolvedPayload, WirePayloadHandle, package_artifacts, payload_transport_options,
};

pub use daedalus_ffi_core as core;

pub const NODE_PACKAGER: LanguagePackager =
    LanguagePackager::new(BackendKind::Node, "node", "daedalus-ffi-node");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePayloadTransport {
    pub buffer: bool,
    pub shared_memory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodePayloadView {
    Buffer { bytes_estimate: u64 },
    SharedMemory { name: String, offset: u64, len: u64 },
}

impl NodePayloadTransport {
    pub fn buffer_and_shared_memory() -> Self {
        Self {
            buffer: true,
            shared_memory: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        payload_transport_options(&[
            ("buffer", self.buffer),
            ("shared_memory", self.shared_memory),
        ])
    }
}

pub fn resolve_node_payload_handle(
    handle: &WirePayloadHandle,
    transport: &NodePayloadTransport,
) -> Result<ResolvedPayload<NodePayloadView>, PayloadResolveError> {
    let mapped = transport
        .shared_memory
        .then_some(MappedPayloadKeys::SHARED_MEMORY);
    handle.resolve_view(mapped, transport.buffer, |view| match view {
        PayloadView::Mapped {
            location,
            offset,
            len,
        } => NodePayloadView::SharedMemory {
            name: location,
            offset,
            len,
        },
        PayloadView::Buffer { bytes_estimate } => NodePayloadView::Buffer { bytes_estimate },
    })
}

pub fn node_worker_backend_config(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
) -> BackendConfig {
    BackendConfig::persistent_worker(BackendKind::Node, "node", function_name)
        .with_entry_module(module_path)
}

pub fn node_worker_backend_config_with_transport(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
    transport: NodePayloadTransport,
) -> BackendConfig {
    node_worker_backend_config(module_path, function_name).with_options(transport.backend_options())
}

/// Package Node.js source files with the default lockfile.
pub fn node_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    let artifacts = package_artifacts(
        PackageArtifactKind::SourceFile,
        &BackendKind::Node,
        source_files,
    )?;
    NODE_PACKAGER.build(LanguagePackageInput::new(schema, backends, artifacts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{
        FixtureLanguage, NodeSchema, WirePort, generate_language_fixture, scalar_add_fixture_spec,
        validate_language_backends,
    };

    fn validate_node_schema(
        schema: &PluginSchema,
        backends: &BTreeMap<String, BackendConfig>,
    ) -> Result<(), FfiContractError> {
        validate_language_backends(schema, backends, BackendKind::Node)
    }

    #[test]
    fn validates_node_schema_and_backends() {
        let output = WirePort::new("out", TypeExpr::scalar(ValueType::Int));
        let node = NodeSchema::new(
            "demo:add",
            BackendKind::Node,
            "add",
            Vec::new(),
            vec![output],
        );
        let schema = PluginSchema::new("demo.node", None, vec![node]);
        let backends = BTreeMap::from([(
            "demo:add".into(),
            node_worker_backend_config("demo.mjs", "add"),
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

        let node = NodeSchema::new(
            baseline.id.clone(),
            BackendKind::Node,
            node_fixture.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = PluginSchema::for_backend(
            "ffi.conformance.node.scalar_add",
            Some("1.0.0".into()),
            vec![node],
            BackendKind::Node,
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
        let package = node_plugin_package(
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

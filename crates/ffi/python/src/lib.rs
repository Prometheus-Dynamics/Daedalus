//! Python FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PackageArtifact,
    PackageArtifactKind, PluginPackage, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION,
    WirePayloadHandle, WirePort, bundled_artifact_path, validate_language_backends,
};
use thiserror::Error;

pub use daedalus_ffi_core as core;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonPayloadTransport {
    pub memoryview: bool,
    pub mmap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonPackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub source_files: Vec<String>,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonResolvedPayload {
    pub id: String,
    pub type_key: String,
    pub access: String,
    pub view: PythonPayloadView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PythonPayloadView {
    MemoryView { bytes_estimate: u64 },
    Mmap { path: String, offset: u64, len: u64 },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PythonPayloadResolveError {
    #[error("python payload transport supports neither memoryview nor mmap")]
    UnsupportedTransport,
    #[error("payload handle `{0}` is missing `{1}` metadata")]
    MissingMetadata(String, &'static str),
}

impl PythonPayloadTransport {
    pub fn memoryview_and_mmap() -> Self {
        Self {
            memoryview: true,
            mmap: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "payload_transport".into(),
            serde_json::json!({
                "memoryview": self.memoryview,
                "mmap": self.mmap,
            }),
        )])
    }
}

pub fn resolve_python_payload_handle(
    handle: &WirePayloadHandle,
    transport: &PythonPayloadTransport,
) -> Result<PythonResolvedPayload, PythonPayloadResolveError> {
    let view = if transport.mmap {
        if let Some(path) = metadata_string(handle, "mmap_path") {
            Some(PythonPayloadView::Mmap {
                path,
                offset: metadata_u64(handle, "mmap_offset").unwrap_or(0),
                len: metadata_u64(handle, "mmap_len")
                    .or_else(|| metadata_u64(handle, "bytes_estimate"))
                    .ok_or_else(|| {
                        PythonPayloadResolveError::MissingMetadata(handle.id.clone(), "mmap_len")
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
        None if transport.memoryview => PythonPayloadView::MemoryView {
            bytes_estimate: metadata_u64(handle, "bytes_estimate").ok_or_else(|| {
                PythonPayloadResolveError::MissingMetadata(handle.id.clone(), "bytes_estimate")
            })?,
        },
        None => return Err(PythonPayloadResolveError::UnsupportedTransport),
    };
    Ok(PythonResolvedPayload {
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

pub fn validate_python_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Python)
}

pub fn python_worker_backend_config(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(module_path.into()),
        entry_class: None,
        entry_symbol: Some(function_name.into()),
        executable: Some("python".into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

pub fn python_worker_backend_config_with_transport(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
    transport: PythonPayloadTransport,
) -> BackendConfig {
    let mut backend = python_worker_backend_config(module_path, function_name);
    backend.options.extend(transport.backend_options());
    backend
}

pub fn python_node_schema(
    node_id: impl Into<String>,
    function_name: impl Into<String>,
    inputs: Vec<WirePort>,
    outputs: Vec<WirePort>,
) -> NodeSchema {
    NodeSchema {
        id: node_id.into(),
        backend: BackendKind::Python,
        entrypoint: function_name.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs,
        outputs,
        metadata: BTreeMap::new(),
    }
}

pub fn python_plugin_schema(
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
    schema.validate_backend_kind(BackendKind::Python)?;
    Ok(schema)
}

pub fn python_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    PythonPackageInput {
        schema,
        backends,
        source_files,
        lockfile: None,
        metadata: BTreeMap::new(),
    }
    .build()
}

impl PythonPackageInput {
    pub fn build(self) -> Result<PluginPackage, FfiContractError> {
        validate_language_backends(&self.schema, &self.backends, BackendKind::Python)?;
        let mut metadata = self.metadata;
        metadata.insert("language".into(), serde_json::json!("python"));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!("daedalus-ffi-python"),
        );

        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(self.schema),
            backends: self.backends,
            artifacts: source_file_artifacts(BackendKind::Python, self.source_files)?,
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

pub fn python_complete_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    PythonPackageInput {
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
    use std::path::{Path, PathBuf};
    use std::process::Command;

    fn schema_for_backend(backend: BackendKind) -> PluginSchema {
        PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: "demo.python".into(),
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
                backend,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: vec![WirePort {
                    name: "a".into(),
                    ty: TypeExpr::scalar(ValueType::Int),
                    type_key: None,
                    optional: false,
                    access: Default::default(),
                    residency: None,
                    layout: None,
                    source: None,
                    const_value: None,
                }],
                outputs: Vec::new(),
                metadata: Default::default(),
            }],
        }
    }

    fn backend(backend: BackendKind) -> BackendConfig {
        BackendConfig {
            backend,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("demo".into()),
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: Some("python".into()),
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: Default::default(),
            options: Default::default(),
        }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("daedalus_{prefix}_{nanos}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn python_available() -> Option<String> {
        let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
        Command::new(&python)
            .arg("--version")
            .output()
            .ok()
            .map(|_| python)
    }

    fn repo_root_from_manifest_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root")
            .to_path_buf()
    }

    #[test]
    fn validates_python_schema_and_backends() {
        let schema = schema_for_backend(BackendKind::Python);
        let backends = BTreeMap::from([("demo:add".into(), backend(BackendKind::Python))]);
        validate_python_schema(&schema, &backends).expect("valid python schema");

        let bad = schema_for_backend(BackendKind::Node);
        assert!(matches!(
            validate_python_schema(&bad, &backends),
            Err(FfiContractError::UnexpectedBackendKind { .. })
        ));
    }

    #[test]
    fn sdk_builders_match_rust_baseline_schema_surface() {
        let spec = scalar_add_fixture_spec();
        let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
        let python =
            generate_language_fixture(&spec, FixtureLanguage::Python).expect("python fixture");
        let baseline = &rust.schema.nodes[0];

        let node = python_node_schema(
            baseline.id.clone(),
            python.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = python_plugin_schema(
            "ffi.conformance.python.scalar_add",
            Some("1.0.0".into()),
            vec![node],
        )
        .expect("schema");
        let backend = python_worker_backend_config("scalar_add.py", "add");
        let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
        let package = python_plugin_package(
            schema.clone(),
            backends.clone(),
            vec!["scalar_add.py".into()],
        )
        .expect("package");

        assert_eq!(schema.nodes[0].id, baseline.id);
        assert_eq!(schema.nodes[0].inputs, baseline.inputs);
        assert_eq!(schema.nodes[0].outputs, baseline.outputs);
        assert_eq!(schema.nodes[0].stateful, baseline.stateful);
        assert_eq!(backend, python.backends[&baseline.id]);
        assert_eq!(package.schema.as_ref(), Some(&schema));
        assert_eq!(package.backends, backends);
        assert_eq!(package.artifacts[0].path, "_bundle/src/scalar_add.py");
        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        validate_python_schema(&schema, &package.backends).expect("valid package schema");
    }

    #[test]
    fn python_sdk_descriptor_round_trips_through_rust_package_validation() {
        let Some(python) = python_available() else {
            return;
        };
        let root = repo_root_from_manifest_dir();
        let sdk_path = root.join("crates/ffi/python/sdk");
        let dir = temp_dir("python_sdk_descriptor");
        let descriptor_path = dir.join("plugin.json");
        let script_path = dir.join("emit_descriptor.py");
        std::fs::write(
            &script_path,
            format!(
                r#"
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, {sdk_path:?})

from daedalus_ffi import Config, State, bytes_payload, node, plugin, type_key

@dataclass
class ScaleConfig(Config):
    factor: int = Config.port(default=2)

class AccumState(State):
    total: int = 0

@type_key("test.Point")
@dataclass
class Point:
    x: float
    y: float

@node(id="scale", inputs=["value", ScaleConfig], outputs=["out"])
def scale(value: int, config: ScaleConfig) -> int:
    return value * config.factor

@node(id="accum", inputs=["value"], outputs=["sum"], state=AccumState)
def accum(value: int, state: AccumState) -> int:
    return value

@node(id="payload_len", inputs=["payload"], outputs=["len"], access="view", transport="memoryview")
def payload_len(payload: memoryview) -> int:
    return len(payload)

@node(id="cow", inputs=["payload"], outputs=["payload"], access="modify")
def cow(payload: bytes_payload.CowView) -> bytes_payload.CowView:
    return payload

plugin("test_python_sdk", [scale, accum, payload_len, cow]) \
    .type_contract("test.Point", ["host_read", "worker_write"]) \
    .artifact("_bundle/src/plugin.py") \
    .transport(memoryview=True, mmap=True) \
    .write(Path({descriptor_path:?}))
"#,
                sdk_path = sdk_path.display().to_string(),
                descriptor_path = descriptor_path.display().to_string(),
            ),
        )
        .expect("write python descriptor script");
        let output = Command::new(python)
            .arg(&script_path)
            .output()
            .expect("run python sdk descriptor script");
        assert!(
            output.status.success(),
            "python sdk descriptor script failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let descriptor = std::fs::read_to_string(&descriptor_path).expect("read descriptor");
        let package: PluginPackage =
            serde_json::from_str(&descriptor).expect("plugin package json");
        package.validate().expect("rust package validation");
        let schema = package.schema.as_ref().expect("schema");
        validate_python_schema(schema, &package.backends).expect("rust python schema validation");
        assert_eq!(schema.plugin.name, "test_python_sdk");
        assert_eq!(schema.nodes.len(), 4);
        assert!(schema.nodes.iter().any(|node| node.stateful));
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus_ffi.python"))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn python_transport_options_enable_memoryview_and_mmap() {
        let backend = python_worker_backend_config_with_transport(
            "plugin.py",
            "run",
            PythonPayloadTransport::memoryview_and_mmap(),
        );
        assert_eq!(
            backend.options.get("payload_transport"),
            Some(&serde_json::json!({"memoryview": true, "mmap": true}))
        );
    }

    #[test]
    fn python_resolves_payload_handles_to_mmap_or_memoryview_views() {
        let mmap_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-1",
            "type_key": "bytes",
            "access": "read",
            "metadata": {
                "mmap_path": "/tmp/daedalus-payload",
                "mmap_offset": 8,
                "mmap_len": 64,
                "bytes_estimate": 64
            }
        }))
        .expect("handle");
        let resolved = resolve_python_payload_handle(
            &mmap_handle,
            &PythonPayloadTransport::memoryview_and_mmap(),
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            PythonPayloadView::Mmap {
                path: "/tmp/daedalus-payload".into(),
                offset: 8,
                len: 64
            }
        );

        let memoryview_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-2",
            "type_key": "bytes",
            "access": "view",
            "metadata": {"bytes_estimate": 32}
        }))
        .expect("handle");
        let resolved = resolve_python_payload_handle(
            &memoryview_handle,
            &PythonPayloadTransport {
                memoryview: true,
                mmap: false,
            },
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            PythonPayloadView::MemoryView { bytes_estimate: 32 }
        );
        assert_eq!(resolved.access, "view");
    }

    #[test]
    fn complete_python_package_emits_lockfile_hash_and_language_metadata() {
        let spec = scalar_add_fixture_spec();
        let fixture =
            generate_language_fixture(&spec, FixtureLanguage::Python).expect("python fixture");
        let package = python_complete_plugin_package(
            fixture.schema.clone(),
            fixture.backends.clone(),
            vec!["ffi_showcase.py".into(), "build_package.py".into()],
        )
        .expect("complete package");
        let lock = package.generate_lockfile();

        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus-ffi-python"))
        );
        assert_eq!(package.artifacts.len(), 2);
        assert_eq!(
            lock.plugin_name.as_deref(),
            Some("ffi.conformance.python.scalar_add")
        );
        assert_eq!(lock.artifacts.len(), 2);
    }
}

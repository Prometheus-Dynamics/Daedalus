//! Python FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, FfiContractError, LanguagePackageInput, LanguagePackager,
    MappedPayloadKeys, PackageArtifactKind, PayloadResolveError, PayloadView, PluginPackage,
    PluginSchema, ResolvedPayload, WirePayloadHandle, package_artifacts, payload_transport_options,
};

pub use daedalus_ffi_core as core;

pub const PYTHON_PACKAGER: LanguagePackager =
    LanguagePackager::new(BackendKind::Python, "python", "daedalus-ffi-python");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonPayloadTransport {
    pub memoryview: bool,
    pub mmap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PythonPayloadView {
    MemoryView { bytes_estimate: u64 },
    Mmap { path: String, offset: u64, len: u64 },
}

impl PythonPayloadTransport {
    pub fn memoryview_and_mmap() -> Self {
        Self {
            memoryview: true,
            mmap: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        payload_transport_options(&[("memoryview", self.memoryview), ("mmap", self.mmap)])
    }
}

pub fn resolve_python_payload_handle(
    handle: &WirePayloadHandle,
    transport: &PythonPayloadTransport,
) -> Result<ResolvedPayload<PythonPayloadView>, PayloadResolveError> {
    let mapped = transport.mmap.then_some(MappedPayloadKeys::MMAP);
    handle.resolve_view(mapped, transport.memoryview, |view| match view {
        PayloadView::Mapped {
            location,
            offset,
            len,
        } => PythonPayloadView::Mmap {
            path: location,
            offset,
            len,
        },
        PayloadView::Buffer { bytes_estimate } => PythonPayloadView::MemoryView { bytes_estimate },
    })
}

pub fn python_worker_backend_config(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
) -> BackendConfig {
    BackendConfig::persistent_worker(BackendKind::Python, "python", function_name)
        .with_entry_module(module_path)
}

pub fn python_worker_backend_config_with_transport(
    module_path: impl Into<String>,
    function_name: impl Into<String>,
    transport: PythonPayloadTransport,
) -> BackendConfig {
    python_worker_backend_config(module_path, function_name)
        .with_options(transport.backend_options())
}

/// Package Python source files with the default lockfile.
pub fn python_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    let artifacts = package_artifacts(
        PackageArtifactKind::SourceFile,
        &BackendKind::Python,
        source_files,
    )?;
    PYTHON_PACKAGER.build(LanguagePackageInput::new(schema, backends, artifacts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{
        FixtureLanguage, NodeSchema, WirePort, WireValue, generate_language_fixture,
        scalar_add_fixture_spec, validate_language_backends,
    };
    use std::path::{Path, PathBuf};
    use std::process::Command;

    fn schema_for_backend(backend: BackendKind) -> PluginSchema {
        let input = WirePort::new("a", TypeExpr::scalar(ValueType::Int));
        let node = NodeSchema::new("demo:add", backend, "add", vec![input], Vec::new());
        PluginSchema::new("demo.python", None, vec![node])
    }

    fn validate_python_schema(
        schema: &PluginSchema,
        backends: &BTreeMap<String, BackendConfig>,
    ) -> Result<(), FfiContractError> {
        validate_language_backends(schema, backends, BackendKind::Python)
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
        let backends = BTreeMap::from([(
            "demo:add".into(),
            python_worker_backend_config("demo", "add"),
        )]);
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

        let node = NodeSchema::new(
            baseline.id.clone(),
            BackendKind::Python,
            python.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = PluginSchema::for_backend(
            "ffi.conformance.python.scalar_add",
            Some("1.0.0".into()),
            vec![node],
            BackendKind::Python,
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

    /// Runs the SDK's unit tests, then checks its wire encoder against serde: unsigned values
    /// above `i64::MAX` and `u64` ports encode as `WireValue::UInt`.
    #[test]
    fn python_sdk_tests_pass_and_wire_values_decode_in_rust() {
        let Some(python) = python_available() else {
            return;
        };
        let sdk = repo_root_from_manifest_dir().join("crates/ffi/python/sdk");
        let tests = Command::new(&python)
            .args(["-m", "unittest", "discover", "-s", "tests"])
            .current_dir(&sdk)
            .output()
            .expect("run python sdk tests");
        assert!(
            tests.status.success(),
            "python sdk tests failed:\n{}",
            String::from_utf8_lossy(&tests.stderr)
        );
        let script = "import json\nfrom daedalus_ffi import to_wire, u64\n\
            print(json.dumps([to_wire((1 << 64) - 1), to_wire(5, u64), to_wire(-3), to_wire([7], list[u64])]))";
        let output = Command::new(&python)
            .args(["-c", script])
            .current_dir(&sdk)
            .output()
            .expect("run python wire encoder");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let values: Vec<WireValue> = serde_json::from_slice(&output.stdout).expect("wire values");
        assert_eq!(
            values,
            [
                WireValue::UInt(u64::MAX),
                WireValue::UInt(5),
                WireValue::Int(-3),
                WireValue::List(vec![WireValue::UInt(7)])
            ]
        );
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
        let package = python_plugin_package(
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

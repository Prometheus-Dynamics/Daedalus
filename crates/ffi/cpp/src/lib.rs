//! C and C++ FFI ABI and packaging integration.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PackageArtifact,
    PackageArtifactKind, PluginPackage, PluginSchema, PluginSchemaInfo, SCHEMA_VERSION,
    WirePayloadHandle, WirePort, bundled_artifact_path, validate_language_backends,
};
use daedalus_transport::{AccessMode, Payload};
use thiserror::Error;

pub use daedalus_ffi_core as core;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CppPointerLengthAbi {
    pub pointer_type: String,
    pub length_type: String,
    pub mutable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CppPackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub shared_libraries: Vec<String>,
    pub source_files: Vec<String>,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CppResolvedPointerView<'a> {
    pub ptr: *const u8,
    pub mut_ptr: Option<*mut u8>,
    pub len: usize,
    pub access: AccessMode,
    lifetime: PhantomData<&'a [u8]>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CppPayloadResolveError {
    #[error(
        "payload handle `{handle_id}` type `{handle_type}` does not match payload type `{payload_type}`"
    )]
    TypeMismatch {
        handle_id: String,
        handle_type: String,
        payload_type: String,
    },
    #[error(
        "payload handle `{handle_id}` access `{found}` does not satisfy requested `{required}`"
    )]
    AccessMismatch {
        handle_id: String,
        required: AccessMode,
        found: AccessMode,
    },
    #[error("payload handle `{0}` does not point at byte-addressable storage")]
    NotByteAddressable(String),
    #[error("mutable pointer access for `{0}` requires unique byte storage")]
    MutableRequiresUniqueStorage(String),
}

impl CppPointerLengthAbi {
    pub fn bytes_view() -> Self {
        Self {
            pointer_type: "const uint8_t*".into(),
            length_type: "size_t".into(),
            mutable: false,
        }
    }

    pub fn mutable_bytes() -> Self {
        Self {
            pointer_type: "uint8_t*".into(),
            length_type: "size_t".into(),
            mutable: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "pointer_length_abi".into(),
            serde_json::json!({
                "pointer_type": self.pointer_type,
                "length_type": self.length_type,
                "mutable": self.mutable,
            }),
        )])
    }
}

pub fn resolve_cpp_payload_handle<'a>(
    handle: &WirePayloadHandle,
    payload: &'a Payload,
    required_access: AccessMode,
) -> Result<CppResolvedPointerView<'a>, CppPayloadResolveError> {
    if &handle.type_key != payload.type_key() {
        return Err(CppPayloadResolveError::TypeMismatch {
            handle_id: handle.id.clone(),
            handle_type: handle.type_key.to_string(),
            payload_type: payload.type_key().to_string(),
        });
    }
    if !handle.access.satisfies(required_access) {
        return Err(CppPayloadResolveError::AccessMismatch {
            handle_id: handle.id.clone(),
            required: required_access,
            found: handle.access,
        });
    }
    let bytes = payload
        .value_any()
        .and_then(|value| value.downcast_ref::<Arc<[u8]>>())
        .ok_or_else(|| CppPayloadResolveError::NotByteAddressable(handle.id.clone()))?;
    let mut_ptr = if matches!(required_access, AccessMode::Modify | AccessMode::Move) {
        return Err(CppPayloadResolveError::MutableRequiresUniqueStorage(
            handle.id.clone(),
        ));
    } else {
        None
    };
    Ok(CppResolvedPointerView {
        ptr: bytes.as_ptr(),
        mut_ptr,
        len: bytes.len(),
        access: required_access,
        lifetime: PhantomData,
    })
}

pub fn resolve_cpp_payload_handle_mut<'a>(
    handle: &WirePayloadHandle,
    bytes: &'a mut [u8],
) -> Result<CppResolvedPointerView<'a>, CppPayloadResolveError> {
    if !handle.access.satisfies(AccessMode::Modify) {
        return Err(CppPayloadResolveError::AccessMismatch {
            handle_id: handle.id.clone(),
            required: AccessMode::Modify,
            found: handle.access,
        });
    }
    Ok(CppResolvedPointerView {
        ptr: bytes.as_ptr(),
        mut_ptr: Some(bytes.as_mut_ptr()),
        len: bytes.len(),
        access: AccessMode::Modify,
        lifetime: PhantomData,
    })
}

pub fn cpp_in_process_backend_config(
    library_path: impl Into<String>,
    symbol: impl Into<String>,
) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::CCpp,
        runtime_model: BackendRuntimeModel::InProcessAbi,
        entry_module: Some(library_path.into()),
        entry_class: None,
        entry_symbol: Some(symbol.into()),
        executable: None,
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

pub fn cpp_in_process_backend_config_with_pointer_abi(
    library_path: impl Into<String>,
    symbol: impl Into<String>,
    abi: CppPointerLengthAbi,
) -> BackendConfig {
    let mut backend = cpp_in_process_backend_config(library_path, symbol);
    backend.options.extend(abi.backend_options());
    backend
}

pub fn cpp_node_schema(
    node_id: impl Into<String>,
    symbol: impl Into<String>,
    inputs: Vec<WirePort>,
    outputs: Vec<WirePort>,
) -> NodeSchema {
    NodeSchema {
        id: node_id.into(),
        backend: BackendKind::CCpp,
        entrypoint: symbol.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs,
        outputs,
        metadata: BTreeMap::new(),
    }
}

pub fn cpp_plugin_schema(
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
    schema.validate_backend_kind(BackendKind::CCpp)?;
    Ok(schema)
}

pub fn validate_cpp_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::CCpp)
}

pub fn cpp_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    shared_libraries: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    CppPackageInput {
        schema,
        backends,
        shared_libraries,
        source_files: Vec::new(),
        lockfile: None,
        metadata: BTreeMap::new(),
    }
    .build()
}

impl CppPackageInput {
    pub fn build(self) -> Result<PluginPackage, FfiContractError> {
        validate_language_backends(&self.schema, &self.backends, BackendKind::CCpp)?;
        let mut metadata = self.metadata;
        metadata.insert("language".into(), serde_json::json!("c_cpp"));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!("daedalus-ffi-cpp"),
        );

        let mut artifacts = shared_library_artifacts(self.shared_libraries)?;
        artifacts.extend(source_file_artifacts(self.source_files)?);
        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(self.schema),
            backends: self.backends,
            artifacts,
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

pub fn cpp_complete_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    shared_libraries: Vec<String>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    CppPackageInput {
        schema,
        backends,
        shared_libraries,
        source_files,
        lockfile: Some("plugin.lock.json".into()),
        metadata: BTreeMap::new(),
    }
    .build()
}

fn shared_library_artifacts(
    shared_libraries: Vec<String>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    shared_libraries
        .into_iter()
        .map(|path| {
            Ok(PackageArtifact {
                path: bundled_artifact_path(PackageArtifactKind::SharedLibrary, &path, None)?,
                kind: PackageArtifactKind::SharedLibrary,
                backend: Some(BackendKind::CCpp),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            })
        })
        .collect()
}

fn source_file_artifacts(
    source_files: Vec<String>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    source_files
        .into_iter()
        .map(|path| {
            Ok(PackageArtifact {
                path: bundled_artifact_path(PackageArtifactKind::SourceFile, &path, None)?,
                kind: PackageArtifactKind::SourceFile,
                backend: Some(BackendKind::CCpp),
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

    fn port(name: &str) -> WirePort {
        WirePort {
            name: name.into(),
            ty: TypeExpr::scalar(ValueType::Int),
            type_key: None,
            optional: false,
            access: Default::default(),
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        }
    }

    #[test]
    fn builds_and_validates_cpp_schema_helpers() {
        let node = cpp_node_schema("demo:add", "add_i32", vec![port("a")], vec![port("out")]);
        let schema =
            cpp_plugin_schema("demo.cpp", Some("1.0.0".into()), vec![node]).expect("schema");
        let backends = BTreeMap::from([(
            "demo:add".into(),
            cpp_in_process_backend_config("libdemo.so", "add_i32"),
        )]);

        validate_cpp_schema(&schema, &backends).expect("valid cpp schema");
        assert!(matches!(
            cpp_plugin_schema(
                "bad",
                None,
                vec![NodeSchema {
                    id: "bad:add".into(),
                    backend: BackendKind::Python,
                    entrypoint: "add".into(),
                    label: None,
                    stateful: false,
                    feature_flags: Vec::new(),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    metadata: BTreeMap::new(),
                }],
            ),
            Err(FfiContractError::UnexpectedBackendKind { .. })
        ));
    }

    #[test]
    fn sdk_builders_match_rust_baseline_schema_surface() {
        let spec = scalar_add_fixture_spec();
        let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
        let cpp = generate_language_fixture(&spec, FixtureLanguage::CCpp).expect("cpp fixture");
        let baseline = &rust.schema.nodes[0];

        let node = cpp_node_schema(
            baseline.id.clone(),
            cpp.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = cpp_plugin_schema(
            "ffi.conformance.c_cpp.scalar_add",
            Some("1.0.0".into()),
            vec![node],
        )
        .expect("schema");
        let backend = cpp_in_process_backend_config("libscalar_add.so", "add_i64");
        let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
        let package = cpp_plugin_package(
            schema.clone(),
            backends.clone(),
            vec!["libscalar_add.so".into()],
        )
        .expect("package");

        assert_eq!(schema.nodes[0].id, baseline.id);
        assert_eq!(schema.nodes[0].inputs, baseline.inputs);
        assert_eq!(schema.nodes[0].outputs, baseline.outputs);
        assert_eq!(schema.nodes[0].stateful, baseline.stateful);
        assert_eq!(backend, cpp.backends[&baseline.id]);
        assert_eq!(package.schema.as_ref(), Some(&schema));
        assert_eq!(package.backends, backends);
        assert_eq!(
            package.artifacts[0].path,
            "_bundle/native/any/libscalar_add.so"
        );
        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        validate_cpp_schema(&schema, &package.backends).expect("valid package schema");
    }

    #[test]
    fn cpp_transport_options_describe_pointer_length_abi() {
        let backend = cpp_in_process_backend_config_with_pointer_abi(
            "libplugin.so",
            "run",
            CppPointerLengthAbi::bytes_view(),
        );
        assert_eq!(
            backend.options.get("pointer_length_abi"),
            Some(&serde_json::json!({
                "pointer_type": "const uint8_t*",
                "length_type": "size_t",
                "mutable": false
            }))
        );
    }

    #[test]
    fn cpp_resolves_payload_handles_to_pointer_length_views() {
        let bytes = Arc::<[u8]>::from(vec![1_u8, 2, 3, 4]);
        let payload = Payload::bytes_with_type_key("bytes", bytes.clone());
        let handle = WirePayloadHandle::from_payload("lease-1", &payload, AccessMode::Read);

        let view = resolve_cpp_payload_handle(&handle, &payload, AccessMode::Read)
            .expect("resolve read pointer");
        assert_eq!(view.ptr, bytes.as_ptr());
        assert_eq!(view.len, 4);
        assert_eq!(view.mut_ptr, None);
        assert_eq!(view.access, AccessMode::Read);

        assert!(matches!(
            resolve_cpp_payload_handle(&handle, &payload, AccessMode::Modify),
            Err(CppPayloadResolveError::AccessMismatch { .. })
        ));
    }

    #[test]
    fn cpp_resolves_mutable_payload_handles_to_mut_pointer_length_views() {
        let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![1_u8]));
        let handle = WirePayloadHandle::from_payload("lease-2", &payload, AccessMode::Modify);
        let mut bytes = vec![1_u8, 2, 3];
        let ptr = bytes.as_ptr();
        let mut_ptr = bytes.as_mut_ptr();

        let view =
            resolve_cpp_payload_handle_mut(&handle, &mut bytes).expect("resolve mut pointer");
        assert_eq!(view.ptr, ptr);
        assert_eq!(view.mut_ptr, Some(mut_ptr));
        assert_eq!(view.len, 3);
        assert_eq!(view.access, AccessMode::Modify);
    }

    #[test]
    fn complete_cpp_package_emits_lockfile_hash_sources_and_language_metadata() {
        let spec = scalar_add_fixture_spec();
        let fixture = generate_language_fixture(&spec, FixtureLanguage::CCpp).expect("cpp fixture");
        let package = cpp_complete_plugin_package(
            fixture.schema.clone(),
            fixture.backends.clone(),
            vec!["build/libffi_showcase.so".into()],
            vec!["src/showcase.cpp".into(), "build-package.cpp".into()],
        )
        .expect("complete package");
        let lock = package.generate_lockfile();

        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus-ffi-cpp"))
        );
        assert_eq!(package.artifacts.len(), 3);
        assert_eq!(
            lock.plugin_name.as_deref(),
            Some("ffi.conformance.c_cpp.scalar_add")
        );
        assert_eq!(lock.artifacts.len(), 3);
    }
}

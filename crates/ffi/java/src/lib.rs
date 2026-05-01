//! Java FFI worker and packaging integration.

use std::collections::BTreeMap;
use std::path::Path;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PackageArtifact,
    PackageArtifactKind, PackagePlatform, PluginPackage, PluginSchema, PluginSchemaInfo,
    SCHEMA_VERSION, WirePayloadHandle, WirePort, bundled_artifact_path, validate_language_backends,
};
use thiserror::Error;

pub use daedalus_ffi_core as core;

const JAVA_BUNDLE_DIR: &str = "_bundle/java";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaPayloadTransport {
    pub direct_byte_buffer: bool,
    pub mmap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaCompletePackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub package: JavaPackageInput,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaResolvedPayload {
    pub id: String,
    pub type_key: String,
    pub access: String,
    pub view: JavaPayloadView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JavaPayloadView {
    DirectByteBuffer { bytes_estimate: u64 },
    Mmap { path: String, offset: u64, len: u64 },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum JavaPayloadResolveError {
    #[error("java payload transport supports neither direct ByteBuffer nor mmap")]
    UnsupportedTransport,
    #[error("payload handle `{0}` is missing `{1}` metadata")]
    MissingMetadata(String, &'static str),
}

impl JavaPayloadTransport {
    pub fn direct_byte_buffer_and_mmap() -> Self {
        Self {
            direct_byte_buffer: true,
            mmap: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "payload_transport".into(),
            serde_json::json!({
                "direct_byte_buffer": self.direct_byte_buffer,
                "mmap": self.mmap,
            }),
        )])
    }
}

pub fn resolve_java_payload_handle(
    handle: &WirePayloadHandle,
    transport: &JavaPayloadTransport,
) -> Result<JavaResolvedPayload, JavaPayloadResolveError> {
    let view = if transport.mmap {
        if let Some(path) = metadata_string(handle, "mmap_path") {
            Some(JavaPayloadView::Mmap {
                path,
                offset: metadata_u64(handle, "mmap_offset").unwrap_or(0),
                len: metadata_u64(handle, "mmap_len")
                    .or_else(|| metadata_u64(handle, "bytes_estimate"))
                    .ok_or_else(|| {
                        JavaPayloadResolveError::MissingMetadata(handle.id.clone(), "mmap_len")
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
        None if transport.direct_byte_buffer => JavaPayloadView::DirectByteBuffer {
            bytes_estimate: metadata_u64(handle, "bytes_estimate").ok_or_else(|| {
                JavaPayloadResolveError::MissingMetadata(handle.id.clone(), "bytes_estimate")
            })?,
        },
        None => return Err(JavaPayloadResolveError::UnsupportedTransport),
    };
    Ok(JavaResolvedPayload {
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JavaPackageInput {
    pub entry_class: String,
    pub entry_method: String,
    pub classpath: Vec<JavaClasspathEntry>,
    pub native_libraries: Vec<JavaNativeLibrary>,
    pub maven_coordinates: Vec<String>,
    pub gradle_projects: Vec<String>,
    pub executable: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JavaClasspathEntry {
    Jar(String),
    ClassesDir(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaNativeLibrary {
    pub path: String,
    pub platform: Option<PackagePlatform>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaWorkerLaunch {
    pub executable: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaRuntimeDiagnostic {
    pub kind: JavaRuntimeDiagnosticKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JavaRuntimeDiagnosticKind {
    ClassNotFound,
    MethodNotFound,
    NativeLibraryLoad,
    Invocation,
    Process,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JavaPackageError {
    #[error("java entry class must not be empty")]
    MissingEntryClass,
    #[error("java entry method must not be empty")]
    MissingEntryMethod,
    #[error("java package needs at least one classpath entry")]
    MissingClasspath,
    #[error("path must have a file name: {path}")]
    MissingFileName { path: String },
    #[error("failed to derive bundle path: {0}")]
    BundlePath(#[from] FfiContractError),
}

impl JavaPackageInput {
    pub fn backend_config(&self) -> Result<BackendConfig, JavaPackageError> {
        validate_java_input(self)?;
        Ok(BackendConfig {
            backend: BackendKind::Java,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: None,
            entry_class: Some(self.entry_class.clone()),
            entry_symbol: Some(self.entry_method.clone()),
            executable: Some(self.executable.clone().unwrap_or_else(|| "java".into())),
            args: Vec::new(),
            classpath: self
                .classpath
                .iter()
                .map(JavaClasspathEntry::path)
                .collect(),
            native_library_paths: self
                .native_libraries
                .iter()
                .map(|library| library.path.clone())
                .collect(),
            working_dir: None,
            env: BTreeMap::new(),
            options: java_metadata_options(self),
        })
    }

    pub fn package_artifacts(&self) -> Result<Vec<PackageArtifact>, JavaPackageError> {
        validate_java_input(self)?;
        let mut artifacts = Vec::new();
        for entry in &self.classpath {
            let path = entry.path();
            let bundled_path = bundled_artifact_path(entry.artifact_kind(), &path, None)?;
            artifacts.push(PackageArtifact {
                path: bundled_path,
                kind: entry.artifact_kind(),
                backend: Some(BackendKind::Java),
                platform: None,
                sha256: None,
                metadata: java_metadata_options(self),
            });
        }
        for library in &self.native_libraries {
            artifacts.push(PackageArtifact {
                path: bundled_artifact_path(
                    PackageArtifactKind::NativeLibrary,
                    &library.path,
                    library.platform.as_ref(),
                )?,
                kind: PackageArtifactKind::NativeLibrary,
                backend: Some(BackendKind::Java),
                platform: library.platform.clone(),
                sha256: None,
                metadata: BTreeMap::new(),
            });
        }
        Ok(artifacts)
    }
}

pub fn validate_java_schema(
    schema: &core::PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Java)
}

pub fn java_node_schema(
    node_id: impl Into<String>,
    method_name: impl Into<String>,
    inputs: Vec<WirePort>,
    outputs: Vec<WirePort>,
) -> NodeSchema {
    NodeSchema {
        id: node_id.into(),
        backend: BackendKind::Java,
        entrypoint: method_name.into(),
        label: None,
        stateful: false,
        feature_flags: Vec::new(),
        inputs,
        outputs,
        metadata: BTreeMap::new(),
    }
}

pub fn java_plugin_schema(
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
    schema.validate_backend_kind(BackendKind::Java)?;
    Ok(schema)
}

pub fn java_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    input: &JavaPackageInput,
) -> Result<PluginPackage, JavaPackageError> {
    JavaCompletePackageInput {
        schema,
        backends,
        package: input.clone(),
        lockfile: None,
        metadata: BTreeMap::new(),
    }
    .build()
}

impl JavaCompletePackageInput {
    pub fn build(self) -> Result<PluginPackage, JavaPackageError> {
        validate_language_backends(&self.schema, &self.backends, BackendKind::Java)?;
        let mut metadata = java_metadata_options(&self.package);
        metadata.extend(self.metadata);
        metadata.insert("language".into(), serde_json::json!("java"));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!("daedalus-ffi-java"),
        );

        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(self.schema),
            backends: self.backends,
            artifacts: self.package.package_artifacts()?,
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

pub fn java_complete_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    input: JavaPackageInput,
) -> Result<PluginPackage, JavaPackageError> {
    JavaCompletePackageInput {
        schema,
        backends,
        package: input,
        lockfile: Some("plugin.lock.json".into()),
        metadata: BTreeMap::new(),
    }
    .build()
}

impl JavaClasspathEntry {
    pub fn jar(path: impl Into<String>) -> Self {
        Self::Jar(path.into())
    }

    pub fn classes_dir(path: impl Into<String>) -> Self {
        Self::ClassesDir(path.into())
    }

    pub fn path(&self) -> String {
        match self {
            Self::Jar(path) | Self::ClassesDir(path) => path.clone(),
        }
    }

    fn artifact_kind(&self) -> PackageArtifactKind {
        match self {
            Self::Jar(_) => PackageArtifactKind::Jar,
            Self::ClassesDir(_) => PackageArtifactKind::ClassesDir,
        }
    }
}

pub fn java_worker_launch(
    backend: &BackendConfig,
    worker_main_class: impl Into<String>,
) -> JavaWorkerLaunch {
    let mut args = Vec::new();
    if !backend.classpath.is_empty() {
        args.push("-cp".into());
        args.push(join_java_paths(&backend.classpath));
    }
    if !backend.native_library_paths.is_empty() {
        args.push(format!(
            "-Djava.library.path={}",
            join_java_paths(&backend.native_library_paths)
        ));
    }
    args.push(worker_main_class.into());

    JavaWorkerLaunch {
        executable: backend.executable.clone().unwrap_or_else(|| "java".into()),
        args,
    }
}

pub fn java_backend_config_with_transport(
    input: &JavaPackageInput,
    transport: JavaPayloadTransport,
) -> Result<BackendConfig, JavaPackageError> {
    let mut backend = input.backend_config()?;
    backend.options.extend(transport.backend_options());
    Ok(backend)
}

pub fn bundled_java_path(path: &str) -> Result<String, JavaPackageError> {
    Ok(format!("{JAVA_BUNDLE_DIR}/{}", file_name(path)?))
}

pub fn bundled_native_path(
    path: &str,
    platform: Option<&PackagePlatform>,
) -> Result<String, JavaPackageError> {
    Ok(bundled_artifact_path(
        PackageArtifactKind::NativeLibrary,
        path,
        platform,
    )?)
}

pub fn classify_java_runtime_diagnostic(
    stderr: impl AsRef<str>,
    fallback: impl Into<String>,
) -> JavaRuntimeDiagnostic {
    let stderr = stderr.as_ref();
    let message = if stderr.trim().is_empty() {
        fallback.into()
    } else {
        stderr.trim().to_string()
    };
    let kind = if contains_any(
        stderr,
        &[
            "ClassNotFoundException",
            "NoClassDefFoundError",
            "Could not find or load main class",
        ],
    ) {
        JavaRuntimeDiagnosticKind::ClassNotFound
    } else if contains_any(
        stderr,
        &[
            "NoSuchMethodException",
            "NoSuchMethodError",
            "method not found",
        ],
    ) {
        JavaRuntimeDiagnosticKind::MethodNotFound
    } else if contains_any(
        stderr,
        &[
            "UnsatisfiedLinkError",
            "java.library.path",
            " in java.library.path",
        ],
    ) {
        JavaRuntimeDiagnosticKind::NativeLibraryLoad
    } else if contains_any(
        stderr,
        &[
            "InvocationTargetException",
            "Exception in thread",
            "RuntimeException",
        ],
    ) {
        JavaRuntimeDiagnosticKind::Invocation
    } else {
        JavaRuntimeDiagnosticKind::Process
    };

    JavaRuntimeDiagnostic { kind, message }
}

fn validate_java_input(input: &JavaPackageInput) -> Result<(), JavaPackageError> {
    if input.entry_class.trim().is_empty() {
        return Err(JavaPackageError::MissingEntryClass);
    }
    if input.entry_method.trim().is_empty() {
        return Err(JavaPackageError::MissingEntryMethod);
    }
    if input.classpath.is_empty() {
        return Err(JavaPackageError::MissingClasspath);
    }
    for entry in &input.classpath {
        file_name(&entry.path())?;
    }
    for library in &input.native_libraries {
        file_name(&library.path)?;
    }
    Ok(())
}

fn java_metadata_options(input: &JavaPackageInput) -> BTreeMap<String, serde_json::Value> {
    let mut options = BTreeMap::new();
    if !input.maven_coordinates.is_empty() {
        options.insert(
            "maven_coordinates".into(),
            serde_json::json!(input.maven_coordinates),
        );
    }
    if !input.gradle_projects.is_empty() {
        options.insert(
            "gradle_projects".into(),
            serde_json::json!(input.gradle_projects),
        );
    }
    options
}

fn join_java_paths(paths: &[String]) -> String {
    let separator = if cfg!(windows) { ";" } else { ":" };
    paths.join(separator)
}

fn file_name(path: &str) -> Result<String, JavaPackageError> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| JavaPackageError::MissingFileName { path: path.into() })
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_ffi_core::{FixtureLanguage, generate_language_fixture, scalar_add_fixture_spec};

    fn input() -> JavaPackageInput {
        JavaPackageInput {
            entry_class: "com.example.Nodes".into(),
            entry_method: "add".into(),
            classpath: vec![
                JavaClasspathEntry::jar("build/libs/demo.jar"),
                JavaClasspathEntry::classes_dir("build/classes/java/main"),
            ],
            native_libraries: vec![JavaNativeLibrary {
                path: "native/linux-x86_64/libopencv_java.so".into(),
                platform: Some(PackagePlatform {
                    os: Some("linux".into()),
                    arch: Some("x86_64".into()),
                    abi: Some("gnu".into()),
                }),
            }],
            maven_coordinates: vec!["org.opencv:opencv:4.10.0".into()],
            gradle_projects: vec![":plugin".into()],
            executable: None,
        }
    }

    #[test]
    fn java_backend_config_records_classpath_native_paths_and_metadata() {
        let backend = input().backend_config().expect("backend config");

        assert_eq!(backend.backend, BackendKind::Java);
        assert_eq!(backend.runtime_model, BackendRuntimeModel::PersistentWorker);
        assert_eq!(backend.entry_class.as_deref(), Some("com.example.Nodes"));
        assert_eq!(backend.entry_symbol.as_deref(), Some("add"));
        assert_eq!(
            backend.classpath,
            vec![
                String::from("build/libs/demo.jar"),
                String::from("build/classes/java/main")
            ]
        );
        assert_eq!(
            backend.native_library_paths,
            vec![String::from("native/linux-x86_64/libopencv_java.so")]
        );
        assert!(backend.options.contains_key("maven_coordinates"));
        assert!(backend.options.contains_key("gradle_projects"));
    }

    #[test]
    fn java_package_artifacts_use_deterministic_bundle_paths() {
        let artifacts = input().package_artifacts().expect("artifacts");

        assert_eq!(artifacts[0].path, "_bundle/java/demo.jar");
        assert_eq!(artifacts[0].kind, PackageArtifactKind::Jar);
        assert_eq!(artifacts[1].path, "_bundle/java/main");
        assert_eq!(artifacts[1].kind, PackageArtifactKind::ClassesDir);
        assert_eq!(
            artifacts[2].path,
            "_bundle/native/linux-x86_64-gnu/libopencv_java.so"
        );
        assert_eq!(artifacts[2].kind, PackageArtifactKind::NativeLibrary);
    }

    #[test]
    fn java_worker_launch_uses_classpath_and_library_path_args() {
        let backend = input().backend_config().expect("backend config");
        let launch = java_worker_launch(&backend, "daedalus.worker.Main");

        assert_eq!(launch.executable, "java");
        assert_eq!(launch.args[0], "-cp");
        assert!(launch.args[1].contains("build/libs/demo.jar"));
        assert!(launch.args[1].contains("build/classes/java/main"));
        assert!(
            launch.args[2].starts_with("-Djava.library.path=native/linux-x86_64/libopencv_java.so")
        );
        assert_eq!(launch.args[3], "daedalus.worker.Main");
    }

    #[test]
    fn java_package_input_rejects_missing_classpath() {
        let mut input = input();
        input.classpath.clear();

        assert_eq!(
            input.backend_config().expect_err("missing classpath"),
            JavaPackageError::MissingClasspath
        );
    }

    #[test]
    fn validates_java_schema_and_backends() {
        let schema = core::PluginSchema {
            schema_version: core::SCHEMA_VERSION,
            plugin: core::PluginSchemaInfo {
                name: "demo.java".into(),
                version: None,
                description: None,
                metadata: Default::default(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes: vec![core::NodeSchema {
                id: "demo:add".into(),
                backend: BackendKind::Java,
                entrypoint: "add".into(),
                label: None,
                stateful: false,
                feature_flags: Vec::new(),
                inputs: Vec::new(),
                outputs: Vec::new(),
                metadata: Default::default(),
            }],
        };
        let backends = BTreeMap::from([(
            "demo:add".into(),
            JavaPackageInput {
                entry_class: "demo.Nodes".into(),
                entry_method: "add".into(),
                classpath: vec![JavaClasspathEntry::jar("demo.jar")],
                ..Default::default()
            }
            .backend_config()
            .expect("backend config"),
        )]);

        validate_java_schema(&schema, &backends).expect("valid java schema");
    }

    #[test]
    fn java_diagnostics_classify_common_runtime_failures() {
        assert_eq!(
            classify_java_runtime_diagnostic(
                "java.lang.ClassNotFoundException: demo.Missing",
                "fallback"
            )
            .kind,
            JavaRuntimeDiagnosticKind::ClassNotFound
        );
        assert_eq!(
            classify_java_runtime_diagnostic(
                "java.lang.NoSuchMethodException: demo.Nodes.add()",
                "fallback"
            )
            .kind,
            JavaRuntimeDiagnosticKind::MethodNotFound
        );
        assert_eq!(
            classify_java_runtime_diagnostic(
                "java.lang.UnsatisfiedLinkError: no opencv_java in java.library.path",
                "fallback"
            )
            .kind,
            JavaRuntimeDiagnosticKind::NativeLibraryLoad
        );
        assert_eq!(
            classify_java_runtime_diagnostic(
                "java.lang.reflect.InvocationTargetException",
                "fallback"
            )
            .kind,
            JavaRuntimeDiagnosticKind::Invocation
        );
        assert_eq!(
            classify_java_runtime_diagnostic("", "process failed"),
            JavaRuntimeDiagnostic {
                kind: JavaRuntimeDiagnosticKind::Process,
                message: "process failed".into(),
            }
        );
    }

    #[test]
    fn sdk_builders_match_rust_baseline_schema_surface() {
        let spec = scalar_add_fixture_spec();
        let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
        let java_fixture =
            generate_language_fixture(&spec, FixtureLanguage::Java).expect("java fixture");
        let baseline = &rust.schema.nodes[0];

        let node = java_node_schema(
            baseline.id.clone(),
            java_fixture.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = java_plugin_schema(
            "ffi.conformance.java.scalar_add",
            Some("1.0.0".into()),
            vec![node],
        )
        .expect("schema");
        let input = JavaPackageInput {
            entry_class: "ffi.conformance.ScalarAdd".into(),
            entry_method: "add".into(),
            classpath: vec![JavaClasspathEntry::classes_dir("classes")],
            ..Default::default()
        };
        let backend = input.backend_config().expect("backend config");
        let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
        let package =
            java_plugin_package(schema.clone(), backends.clone(), &input).expect("package");

        assert_eq!(schema.nodes[0].id, baseline.id);
        assert_eq!(schema.nodes[0].inputs, baseline.inputs);
        assert_eq!(schema.nodes[0].outputs, baseline.outputs);
        assert_eq!(schema.nodes[0].stateful, baseline.stateful);
        assert_eq!(backend, java_fixture.backends[&baseline.id]);
        assert_eq!(package.schema.as_ref(), Some(&schema));
        assert_eq!(package.backends, backends);
        assert_eq!(package.artifacts[0].path, "_bundle/java/classes");
        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        validate_java_schema(&schema, &package.backends).expect("valid package schema");
    }

    #[test]
    fn java_transport_options_enable_direct_byte_buffer_and_mmap() {
        let backend = java_backend_config_with_transport(
            &input(),
            JavaPayloadTransport::direct_byte_buffer_and_mmap(),
        )
        .expect("backend config");
        assert_eq!(
            backend.options.get("payload_transport"),
            Some(&serde_json::json!({"direct_byte_buffer": true, "mmap": true}))
        );
    }

    #[test]
    fn java_resolves_payload_handles_to_mmap_or_direct_byte_buffer_views() {
        let mmap_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-1",
            "type_key": "bytes",
            "access": "read",
            "metadata": {
                "mmap_path": "/tmp/daedalus-payload",
                "mmap_offset": 12,
                "mmap_len": 96,
                "bytes_estimate": 96
            }
        }))
        .expect("handle");
        let resolved = resolve_java_payload_handle(
            &mmap_handle,
            &JavaPayloadTransport::direct_byte_buffer_and_mmap(),
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            JavaPayloadView::Mmap {
                path: "/tmp/daedalus-payload".into(),
                offset: 12,
                len: 96
            }
        );

        let direct_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
            "id": "lease-2",
            "type_key": "bytes",
            "access": "view",
            "metadata": {"bytes_estimate": 48}
        }))
        .expect("handle");
        let resolved = resolve_java_payload_handle(
            &direct_handle,
            &JavaPayloadTransport {
                direct_byte_buffer: true,
                mmap: false,
            },
        )
        .expect("resolve");
        assert_eq!(
            resolved.view,
            JavaPayloadView::DirectByteBuffer { bytes_estimate: 48 }
        );
        assert_eq!(resolved.access, "view");
    }

    #[test]
    fn complete_java_package_emits_lockfile_hash_and_language_metadata() {
        let spec = scalar_add_fixture_spec();
        let fixture =
            generate_language_fixture(&spec, FixtureLanguage::Java).expect("java fixture");
        let input = JavaPackageInput {
            entry_class: "ffi.conformance.ScalarAdd".into(),
            entry_method: "add".into(),
            classpath: vec![
                JavaClasspathEntry::classes_dir("build/classes/java/main"),
                JavaClasspathEntry::jar("build/libs/ffi-showcase.jar"),
            ],
            native_libraries: vec![JavaNativeLibrary {
                path: "build/native/libffi_showcase_jni.so".into(),
                platform: None,
            }],
            ..Default::default()
        };
        let package =
            java_complete_plugin_package(fixture.schema.clone(), fixture.backends.clone(), input)
                .expect("complete package");
        let lock = package.generate_lockfile();

        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus-ffi-java"))
        );
        assert_eq!(package.artifacts.len(), 3);
        assert_eq!(
            lock.plugin_name.as_deref(),
            Some("ffi.conformance.java.scalar_add")
        );
        assert_eq!(lock.artifacts.len(), 3);
    }
}

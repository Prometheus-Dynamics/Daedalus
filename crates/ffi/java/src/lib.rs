//! Java FFI worker and packaging integration.

use std::collections::BTreeMap;
use std::path::Path;

use core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FfiContractError, NodeSchema, PackageArtifact,
    PackageArtifactKind, PackagePlatform, PluginPackage, PluginSchema, PluginSchemaInfo,
    SCHEMA_VERSION, WirePort, bundled_artifact_path, validate_language_backends,
};
use thiserror::Error;

pub use daedalus_ffi_core as core;

mod diagnostics;
mod payload;

pub use diagnostics::{
    JavaRuntimeDiagnostic, JavaRuntimeDiagnosticKind, classify_java_runtime_diagnostic,
};
pub use payload::{
    JavaPayloadResolveError, JavaPayloadTransport, JavaPayloadView, JavaResolvedPayload,
    resolve_java_payload_handle,
};

const JAVA_BUNDLE_DIR: &str = "_bundle/java";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaCompletePackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub package: JavaPackageInput,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
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

#[cfg(test)]
mod tests;

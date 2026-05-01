use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use daedalus_data::model::TypeExpr;
use daedalus_transport::{AccessMode, BoundaryTypeContract, Layout, Residency, TypeKey};
use serde::{Deserialize, Serialize};

use super::*;
use crate::SCHEMA_VERSION;

/// Core schema boundary for plugin metadata and node shape.
///
/// Runtime process details and physical packaging are deliberately excluded from this type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSchema {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub plugin: PluginSchemaInfo,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub required_host_capabilities: Vec<String>,
    #[serde(default)]
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub boundary_contracts: Vec<BoundaryTypeContract>,
    #[serde(default)]
    pub nodes: Vec<NodeSchema>,
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

impl PluginSchema {
    pub fn validate(&self) -> Result<(), FfiContractError> {
        validate_schema_version("plugin schema", self.schema_version)?;
        validate_non_empty("plugin.name", &self.plugin.name)?;

        let mut node_ids = std::collections::BTreeSet::new();
        for node in &self.nodes {
            validate_non_empty("node.id", &node.id)?;
            validate_non_empty("node.entrypoint", &node.entrypoint)?;
            if !node_ids.insert(node.id.as_str()) {
                return Err(FfiContractError::DuplicateNode {
                    node_id: node.id.clone(),
                });
            }
            validate_ports(&node.id, "input", &node.inputs)?;
            validate_ports(&node.id, "output", &node.outputs)?;
        }

        Ok(())
    }

    pub fn validate_backend_kind(&self, expected: BackendKind) -> Result<(), FfiContractError> {
        self.validate()?;
        for node in &self.nodes {
            if node.backend != expected {
                return Err(FfiContractError::UnexpectedBackendKind {
                    node_id: node.id.clone(),
                    expected: expected.clone(),
                    found: node.backend.clone(),
                });
            }
        }
        Ok(())
    }
}

pub fn validate_language_backends(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
    expected: BackendKind,
) -> Result<(), FfiContractError> {
    schema.validate_backend_kind(expected)?;
    let expected_nodes = schema
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    for node in &schema.nodes {
        let backend =
            backends
                .get(&node.id)
                .ok_or_else(|| FfiContractError::MissingBackendConfig {
                    node_id: node.id.clone(),
                })?;
        if backend.backend != node.backend {
            return Err(FfiContractError::BackendMismatch {
                node_id: node.id.clone(),
                schema_backend: node.backend.clone(),
                config_backend: backend.backend.clone(),
            });
        }
        backend.validate_for_node(&node.id)?;
    }

    for node_id in backends.keys() {
        if !expected_nodes.contains(node_id.as_str()) {
            return Err(FfiContractError::MissingBackendConfig {
                node_id: node_id.clone(),
            });
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSchemaInfo {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeSchema {
    pub id: String,
    pub backend: BackendKind,
    pub entrypoint: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub stateful: bool,
    #[serde(default)]
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<WirePort>,
    #[serde(default)]
    pub outputs: Vec<WirePort>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WirePort {
    pub name: String,
    pub ty: TypeExpr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_key: Option<TypeKey>,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub const_value: Option<serde_json::Value>,
}

/// Backend runtime config is deliberately separate from the schema surface.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendConfig {
    pub backend: BackendKind,
    pub runtime_model: BackendRuntimeModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_module: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classpath: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_library_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

impl BackendConfig {
    pub fn validate_for_node(&self, node_id: &str) -> Result<(), FfiContractError> {
        match (&self.backend, self.runtime_model) {
            (BackendKind::Python, BackendRuntimeModel::PersistentWorker) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::Node, BackendRuntimeModel::PersistentWorker) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::Java, BackendRuntimeModel::PersistentWorker) => {
                if self.classpath.is_empty() {
                    return Err(FfiContractError::MissingBackendField {
                        node_id: node_id.to_string(),
                        field: "classpath",
                    });
                }
                require_some(node_id, "entry_class", &self.entry_class)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
                require_some(node_id, "executable", &self.executable)?;
            }
            (BackendKind::CCpp, BackendRuntimeModel::InProcessAbi) => {
                require_some(node_id, "entry_module", &self.entry_module)?;
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
            }
            (BackendKind::Rust, BackendRuntimeModel::InProcessAbi)
            | (BackendKind::Shader, BackendRuntimeModel::InProcessAbi) => {
                require_some(node_id, "entry_symbol", &self.entry_symbol)?;
            }
            (backend, model) => {
                if matches!(model, BackendRuntimeModel::PersistentWorker) {
                    require_some(node_id, "executable", &self.executable)?;
                }
                if matches!(backend, BackendKind::Other(_)) && self.entry_symbol.is_none() {
                    return Err(FfiContractError::MissingBackendField {
                        node_id: node_id.to_string(),
                        field: "entry_symbol",
                    });
                }
            }
        }
        Ok(())
    }
}

/// Physical package descriptor for an FFI plugin.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginPackage {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub schema: Option<PluginSchema>,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendConfig>,
    #[serde(default)]
    pub artifacts: Vec<PackageArtifact>,
    #[serde(default)]
    pub lockfile: Option<String>,
    #[serde(default)]
    pub manifest_hash: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustPackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub compiled_modules: Vec<String>,
    pub source_files: Vec<String>,
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl RustPackageInput {
    pub fn build(self) -> Result<PluginPackage, FfiContractError> {
        validate_language_backends(&self.schema, &self.backends, BackendKind::Rust)?;
        let mut metadata = self.metadata;
        metadata.insert("language".into(), serde_json::json!("rust"));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!("daedalus-ffi-core"),
        );

        let mut artifacts = rust_compiled_module_artifacts(self.compiled_modules)?;
        artifacts.extend(rust_source_file_artifacts(self.source_files)?);
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

pub fn rust_complete_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    compiled_modules: Vec<String>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    RustPackageInput {
        schema,
        backends,
        compiled_modules,
        source_files,
        lockfile: Some("plugin.lock.json".into()),
        metadata: BTreeMap::new(),
    }
    .build()
}

impl PluginPackage {
    pub fn validate(&self) -> Result<(), FfiContractError> {
        validate_schema_version("plugin package", self.schema_version)?;

        if let Some(schema) = &self.schema {
            schema.validate()?;
            for node in &schema.nodes {
                let backend = self.backends.get(&node.id).ok_or_else(|| {
                    FfiContractError::MissingBackendConfig {
                        node_id: node.id.clone(),
                    }
                })?;
                if backend.backend != node.backend {
                    return Err(FfiContractError::BackendMismatch {
                        node_id: node.id.clone(),
                        schema_backend: node.backend.clone(),
                        config_backend: backend.backend.clone(),
                    });
                }
                backend.validate_for_node(&node.id)?;
            }
        }

        for (node_id, backend) in &self.backends {
            validate_non_empty("backend node id", node_id)?;
            backend.validate_for_node(node_id)?;
        }

        for artifact in &self.artifacts {
            validate_non_empty("artifact.path", &artifact.path)?;
        }

        if let Some(lockfile) = &self.lockfile {
            validate_non_empty("package.lockfile", lockfile)?;
        }
        if let Some(hash) = &self.manifest_hash {
            validate_non_empty("package.manifest_hash", hash)?;
        }

        Ok(())
    }

    pub fn validate_artifact_files(
        &self,
        base_dir: impl AsRef<Path>,
    ) -> Result<(), FfiContractError> {
        self.validate()?;
        let base_dir = base_dir.as_ref();
        for artifact in &self.artifacts {
            validate_package_relative_path(&artifact.path)?;
            if !base_dir.join(&artifact.path).exists() {
                return Err(FfiContractError::MissingPackageArtifact {
                    path: artifact.path.clone(),
                    base_dir: base_dir.display().to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn rewrite_artifact_paths_for_bundle(&mut self) -> Result<(), FfiContractError> {
        for artifact in &mut self.artifacts {
            artifact.path =
                bundled_artifact_path(artifact.kind, &artifact.path, artifact.platform.as_ref())?;
        }
        Ok(())
    }

    pub fn stamp_integrity(&mut self, base_dir: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.validate_artifact_files(base_dir.as_ref())?;
        let base_dir = base_dir.as_ref();
        for artifact in &mut self.artifacts {
            artifact.sha256 = Some(sha256_file_hex(base_dir.join(&artifact.path))?);
        }
        self.manifest_hash = Some(self.compute_manifest_hash()?);
        Ok(())
    }

    pub fn verify_integrity(&self, base_dir: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.validate_artifact_files(base_dir.as_ref())?;
        let base_dir = base_dir.as_ref();
        for artifact in &self.artifacts {
            let Some(expected) = &artifact.sha256 else {
                continue;
            };
            let actual = sha256_file_hex(base_dir.join(&artifact.path))?;
            if &actual != expected {
                return Err(FfiContractError::PackageHashMismatch {
                    path: artifact.path.clone(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        if let Some(expected) = &self.manifest_hash {
            let actual = self.compute_manifest_hash()?;
            if &actual != expected {
                return Err(FfiContractError::PackageHashMismatch {
                    path: "manifest".into(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        Ok(())
    }

    pub fn compute_manifest_hash(&self) -> Result<String, FfiContractError> {
        let mut package = self.clone();
        package.manifest_hash = None;
        package.signature = None;
        let bytes =
            serde_json::to_vec(&package).map_err(|err| FfiContractError::PackageHashError {
                message: err.to_string(),
            })?;
        Ok(sha256_bytes_hex(&bytes))
    }

    pub fn read_descriptor(path: impl AsRef<Path>) -> Result<Self, FfiContractError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })?;
        serde_json::from_slice(&bytes).map_err(|err| FfiContractError::PackageJson {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn write_descriptor(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        let path = path.as_ref();
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|err| FfiContractError::PackageJson {
                path: path.display().to_string(),
                message: err.to_string(),
            })?;
        fs::write(path, bytes).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn read_descriptor_and_verify(
        path: impl AsRef<Path>,
        base_dir: impl AsRef<Path>,
    ) -> Result<Self, FfiContractError> {
        let package = Self::read_descriptor(path)?;
        package.verify_integrity(base_dir)?;
        Ok(package)
    }

    pub fn generate_lockfile(&self) -> PluginLockfile {
        PluginLockfile::from_package(self)
    }

    pub fn write_lockfile(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        self.generate_lockfile().write(path)
    }
}

fn rust_compiled_module_artifacts(
    compiled_modules: Vec<String>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    compiled_modules
        .into_iter()
        .map(|path| {
            Ok(PackageArtifact {
                path: bundled_artifact_path(PackageArtifactKind::CompiledModule, &path, None)?,
                kind: PackageArtifactKind::CompiledModule,
                backend: Some(BackendKind::Rust),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            })
        })
        .collect()
}

fn rust_source_file_artifacts(
    source_files: Vec<String>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    source_files
        .into_iter()
        .map(|path| {
            Ok(PackageArtifact {
                path: bundled_artifact_path(PackageArtifactKind::SourceFile, &path, None)?,
                kind: PackageArtifactKind::SourceFile,
                backend: Some(BackendKind::Rust),
                platform: None,
                sha256: None,
                metadata: BTreeMap::new(),
            })
        })
        .collect()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginLockfile {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub plugin_name: Option<String>,
    #[serde(default)]
    pub plugin_version: Option<String>,
    #[serde(default)]
    pub manifest_hash: Option<String>,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendLockEntry>,
    #[serde(default)]
    pub artifacts: Vec<PackageLockArtifact>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl PluginLockfile {
    pub fn from_package(package: &PluginPackage) -> Self {
        let mut artifacts: Vec<_> = package
            .artifacts
            .iter()
            .map(PackageLockArtifact::from_artifact)
            .collect();
        artifacts.sort_by(|a, b| {
            a.path
                .cmp(&b.path)
                .then_with(|| format!("{:?}", a.kind).cmp(&format!("{:?}", b.kind)))
        });

        Self {
            schema_version: package.schema_version,
            plugin_name: package
                .schema
                .as_ref()
                .map(|schema| schema.plugin.name.clone()),
            plugin_version: package
                .schema
                .as_ref()
                .and_then(|schema| schema.plugin.version.clone()),
            manifest_hash: package.manifest_hash.clone(),
            backends: package
                .backends
                .iter()
                .map(|(node_id, backend)| (node_id.clone(), BackendLockEntry::from(backend)))
                .collect(),
            artifacts,
            metadata: package.metadata.clone(),
        }
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, FfiContractError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })?;
        serde_json::from_slice(&bytes).map_err(|err| FfiContractError::PackageJson {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }

    pub fn write(&self, path: impl AsRef<Path>) -> Result<(), FfiContractError> {
        let path = path.as_ref();
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|err| FfiContractError::PackageJson {
                path: path.display().to_string(),
                message: err.to_string(),
            })?;
        fs::write(path, bytes).map_err(|err| FfiContractError::PackageIo {
            path: path.display().to_string(),
            message: err.to_string(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendLockEntry {
    pub backend: BackendKind,
    pub runtime_model: BackendRuntimeModel,
    #[serde(default)]
    pub entry_module: Option<String>,
    #[serde(default)]
    pub entry_class: Option<String>,
    #[serde(default)]
    pub entry_symbol: Option<String>,
    #[serde(default)]
    pub executable: Option<String>,
    #[serde(default)]
    pub classpath: Vec<String>,
    #[serde(default)]
    pub native_library_paths: Vec<String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

impl From<&BackendConfig> for BackendLockEntry {
    fn from(backend: &BackendConfig) -> Self {
        Self {
            backend: backend.backend.clone(),
            runtime_model: backend.runtime_model,
            entry_module: backend.entry_module.clone(),
            entry_class: backend.entry_class.clone(),
            entry_symbol: backend.entry_symbol.clone(),
            executable: backend.executable.clone(),
            classpath: backend.classpath.clone(),
            native_library_paths: backend.native_library_paths.clone(),
            options: backend.options.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageLockArtifact {
    pub path: String,
    pub kind: PackageArtifactKind,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub platform: Option<PackagePlatform>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl PackageLockArtifact {
    fn from_artifact(artifact: &PackageArtifact) -> Self {
        Self {
            path: artifact.path.clone(),
            kind: artifact.kind,
            backend: artifact.backend.clone(),
            platform: artifact.platform.clone(),
            sha256: artifact.sha256.clone(),
            metadata: artifact.metadata.clone(),
        }
    }
}

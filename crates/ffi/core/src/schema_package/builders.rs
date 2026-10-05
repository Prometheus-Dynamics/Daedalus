//! Constructors shared by the language SDK crates, host tests and fixtures.
//!
//! The schema and package types keep public fields so descriptors stay plain data; these helpers
//! only fill in the defaults so callers spell out what differs.

use std::collections::BTreeMap;

use daedalus_data::model::TypeExpr;

use super::*;
use crate::SCHEMA_VERSION;

impl WirePort {
    /// A required, read-access port with no transport hints.
    pub fn new(name: impl Into<String>, ty: TypeExpr) -> Self {
        Self {
            name: name.into(),
            ty,
            type_key: None,
            optional: false,
            access: Default::default(),
            residency: None,
            layout: None,
            source: None,
            const_value: None,
        }
    }
}

impl NodeSchema {
    /// A stateless node with no label, feature flags or metadata.
    pub fn new(
        id: impl Into<String>,
        backend: BackendKind,
        entrypoint: impl Into<String>,
        inputs: Vec<WirePort>,
        outputs: Vec<WirePort>,
    ) -> Self {
        Self {
            id: id.into(),
            backend,
            entrypoint: entrypoint.into(),
            label: None,
            stateful: false,
            feature_flags: Vec::new(),
            inputs,
            outputs,
            metadata: BTreeMap::new(),
        }
    }
}

impl PluginSchemaInfo {
    pub fn new(name: impl Into<String>, version: Option<String>) -> Self {
        Self {
            name: name.into(),
            version,
            description: None,
            metadata: BTreeMap::new(),
        }
    }
}

impl PluginSchema {
    /// An unvalidated schema with no dependencies, capabilities, flags or contracts.
    pub fn new(name: impl Into<String>, version: Option<String>, nodes: Vec<NodeSchema>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo::new(name, version),
            dependencies: Vec::new(),
            required_host_capabilities: Vec::new(),
            feature_flags: Vec::new(),
            boundary_contracts: Vec::new(),
            nodes,
        }
    }

    /// A schema whose nodes are sorted by id and all use `backend`.
    pub fn for_backend(
        name: impl Into<String>,
        version: Option<String>,
        mut nodes: Vec<NodeSchema>,
        backend: BackendKind,
    ) -> Result<Self, FfiContractError> {
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        let schema = Self::new(name, version, nodes);
        schema.validate_backend_kind(backend)?;
        Ok(schema)
    }
}

impl BackendConfig {
    /// A config with only the backend and runtime model set.
    pub fn new(backend: BackendKind, runtime_model: BackendRuntimeModel) -> Self {
        Self {
            backend,
            runtime_model,
            entry_module: None,
            entry_class: None,
            entry_symbol: None,
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        }
    }

    /// A persistent worker launched with `executable` that invokes `entry_symbol`.
    pub fn persistent_worker(
        backend: BackendKind,
        executable: impl Into<String>,
        entry_symbol: impl Into<String>,
    ) -> Self {
        let mut config = Self::new(backend, BackendRuntimeModel::PersistentWorker);
        config.executable = Some(executable.into());
        config.entry_symbol = Some(entry_symbol.into());
        config
    }

    /// An in-process ABI entry point named `entry_symbol`.
    pub fn in_process(backend: BackendKind, entry_symbol: impl Into<String>) -> Self {
        let mut config = Self::new(backend, BackendRuntimeModel::InProcessAbi);
        config.entry_symbol = Some(entry_symbol.into());
        config
    }

    pub fn with_entry_module(mut self, entry_module: impl Into<String>) -> Self {
        self.entry_module = Some(entry_module.into());
        self
    }

    pub fn with_entry_class(mut self, entry_class: impl Into<String>) -> Self {
        self.entry_class = Some(entry_class.into());
        self
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_classpath(
        mut self,
        classpath: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.classpath = classpath.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    pub fn with_options(
        mut self,
        options: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.options.extend(options);
        self
    }
}

impl PackageArtifact {
    /// An unhashed, platform-independent artifact at `path`.
    pub fn new(
        path: impl Into<String>,
        kind: PackageArtifactKind,
        backend: Option<BackendKind>,
    ) -> Self {
        Self {
            path: path.into(),
            kind,
            backend,
            platform: None,
            sha256: None,
            metadata: BTreeMap::new(),
        }
    }

    /// An artifact at its deterministic bundle path for `kind`.
    pub fn bundled(
        kind: PackageArtifactKind,
        backend: Option<BackendKind>,
        original_path: &str,
        platform: Option<PackagePlatform>,
    ) -> Result<Self, FfiContractError> {
        let path = bundled_artifact_path(kind, original_path, platform.as_ref())?;
        Ok(Self {
            platform,
            ..Self::new(path, kind, backend)
        })
    }
}

/// Bundle artifacts of one kind for one backend.
pub fn package_artifacts(
    kind: PackageArtifactKind,
    backend: &BackendKind,
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<PackageArtifact>, FfiContractError> {
    paths
        .into_iter()
        .map(|path| PackageArtifact::bundled(kind, Some(backend.clone()), path.as_ref(), None))
        .collect()
}

impl PluginPackage {
    /// An unhashed package with no artifacts, lockfile or metadata.
    pub fn new(schema: PluginSchema, backends: BTreeMap<String, BackendConfig>) -> Self {
        Self {
            schema: Some(schema),
            backends,
            ..Self::default()
        }
    }
}

impl Default for PluginPackage {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            schema: None,
            backends: BTreeMap::new(),
            artifacts: Vec::new(),
            lockfile: None,
            manifest_hash: None,
            signature: None,
            metadata: BTreeMap::new(),
        }
    }
}

/// The lockfile a language package builder writes unless told otherwise.
pub const DEFAULT_LOCKFILE: &str = "plugin.lock.json";

/// Inputs shared by every language package builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguagePackageInput {
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub artifacts: Vec<PackageArtifact>,
    /// Defaults to [`DEFAULT_LOCKFILE`].
    pub lockfile: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

/// The language identity a package builder stamps into its descriptors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguagePackager {
    pub backend: BackendKind,
    pub language: &'static str,
    pub package_builder: &'static str,
}

impl LanguagePackageInput {
    pub fn new(
        schema: PluginSchema,
        backends: BTreeMap<String, BackendConfig>,
        artifacts: Vec<PackageArtifact>,
    ) -> Self {
        Self {
            schema,
            backends,
            artifacts,
            lockfile: None,
            metadata: BTreeMap::new(),
        }
    }
}

impl LanguagePackager {
    pub const fn new(
        backend: BackendKind,
        language: &'static str,
        package_builder: &'static str,
    ) -> Self {
        Self {
            backend,
            language,
            package_builder,
        }
    }

    /// Validate the input against this backend and emit a hashed package descriptor.
    #[cfg(feature = "integrity")]
    pub fn build(&self, input: LanguagePackageInput) -> Result<PluginPackage, FfiContractError> {
        validate_language_backends(&input.schema, &input.backends, self.backend.clone())?;
        let mut metadata = input.metadata;
        metadata.insert("language".into(), serde_json::json!(self.language));
        metadata.insert(
            "package_builder".into(),
            serde_json::json!(self.package_builder),
        );
        let mut package = PluginPackage {
            schema_version: SCHEMA_VERSION,
            schema: Some(input.schema),
            backends: input.backends,
            artifacts: input.artifacts,
            lockfile: Some(input.lockfile.unwrap_or_else(|| DEFAULT_LOCKFILE.into())),
            manifest_hash: None,
            signature: None,
            metadata,
        };
        package.validate()?;
        package.manifest_hash = Some(package.compute_manifest_hash()?);
        Ok(package)
    }
}

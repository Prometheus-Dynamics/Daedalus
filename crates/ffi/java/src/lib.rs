//! Java FFI worker and packaging integration.

use std::collections::BTreeMap;

use core::{
    BackendConfig, BackendKind, FfiContractError, LanguagePackageInput, LanguagePackager,
    PackageArtifact, PackageArtifactKind, PackagePlatform, PluginPackage, PluginSchema,
};
use thiserror::Error;

pub use daedalus_ffi_core as core;

mod diagnostics;
mod payload;

pub use diagnostics::{
    JavaRuntimeDiagnostic, JavaRuntimeDiagnosticKind, classify_java_runtime_diagnostic,
};
pub use payload::{JavaPayloadTransport, JavaPayloadView, resolve_java_payload_handle};

pub const JAVA_PACKAGER: LanguagePackager =
    LanguagePackager::new(BackendKind::Java, "java", "daedalus-ffi-java");

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
    #[error("failed to derive bundle path: {0}")]
    BundlePath(#[from] FfiContractError),
}

impl JavaPackageInput {
    pub fn backend_config(&self) -> Result<BackendConfig, JavaPackageError> {
        // Bundling the artifacts validates the entry point and every classpath/native path.
        self.package_artifacts()?;
        Ok(BackendConfig {
            native_library_paths: self
                .native_libraries
                .iter()
                .map(|library| library.path.clone())
                .collect(),
            ..BackendConfig::persistent_worker(
                BackendKind::Java,
                self.executable.as_deref().unwrap_or("java"),
                &self.entry_method,
            )
            .with_entry_class(&self.entry_class)
            .with_classpath(self.classpath.iter().map(JavaClasspathEntry::path))
            .with_options(self.metadata_options())
        })
    }

    pub fn package_artifacts(&self) -> Result<Vec<PackageArtifact>, JavaPackageError> {
        if self.entry_class.trim().is_empty() {
            return Err(JavaPackageError::MissingEntryClass);
        }
        if self.entry_method.trim().is_empty() {
            return Err(JavaPackageError::MissingEntryMethod);
        }
        if self.classpath.is_empty() {
            return Err(JavaPackageError::MissingClasspath);
        }
        let backend = Some(BackendKind::Java);
        let mut artifacts = Vec::new();
        for entry in &self.classpath {
            let mut artifact = PackageArtifact::bundled(
                entry.artifact_kind(),
                backend.clone(),
                &entry.path(),
                None,
            )?;
            artifact.metadata = self.metadata_options();
            artifacts.push(artifact);
        }
        for library in &self.native_libraries {
            artifacts.push(PackageArtifact::bundled(
                PackageArtifactKind::NativeLibrary,
                backend.clone(),
                &library.path,
                library.platform.clone(),
            )?);
        }
        Ok(artifacts)
    }

    fn metadata_options(&self) -> BTreeMap<String, serde_json::Value> {
        let mut options = BTreeMap::new();
        if !self.maven_coordinates.is_empty() {
            options.insert(
                "maven_coordinates".into(),
                serde_json::json!(self.maven_coordinates),
            );
        }
        if !self.gradle_projects.is_empty() {
            options.insert(
                "gradle_projects".into(),
                serde_json::json!(self.gradle_projects),
            );
        }
        options
    }
}

/// Package a Java classpath and native libraries with the default lockfile.
pub fn java_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    input: &JavaPackageInput,
) -> Result<PluginPackage, JavaPackageError> {
    let mut package = LanguagePackageInput::new(schema, backends, input.package_artifacts()?);
    package.metadata = input.metadata_options();
    Ok(JAVA_PACKAGER.build(package)?)
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
    Ok(input
        .backend_config()?
        .with_options(transport.backend_options()))
}

fn join_java_paths(paths: &[String]) -> String {
    let separator = if cfg!(windows) { ";" } else { ":" };
    paths.join(separator)
}

#[cfg(test)]
mod tests;

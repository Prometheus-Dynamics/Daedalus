use std::fs;
use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

use crate::c_cpp::{CppManifestError, CppManifestPlugin};
use crate::java::{JavaManifestError, JavaManifestPlugin};
use crate::manifest::Manifest;
use crate::node::{NodeManifestError, NodeManifestPlugin};
use crate::python::{PythonManifestError, PythonManifestPlugin};
use daedalus_runtime::plugins::{Plugin, PluginInstallContext, PluginResult};

#[derive(Debug, Error)]
pub enum ManifestDispatchError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Python(#[from] PythonManifestError),
    #[error(transparent)]
    Java(#[from] JavaManifestError),
    #[error(transparent)]
    Node(#[from] NodeManifestError),
    #[error(transparent)]
    Cpp(#[from] CppManifestError),
    #[error("unknown manifest language '{0}'")]
    UnknownLanguage(String),
}

#[derive(Debug, Deserialize)]
struct ManifestProbe {
    #[serde(default)]
    language: Option<String>,
}

/// Load any manifest, dispatching by the `language` field (defaults to Python).
pub fn load_manifest_plugin(
    path: impl AsRef<Path>,
) -> Result<ManifestPlugin, ManifestDispatchError> {
    let text = fs::read_to_string(&path)?;
    let probe: ManifestProbe = serde_json::from_str(&text)?;
    let lang = probe.language.as_deref().unwrap_or("python");
    let base = path.as_ref().parent().map(|p| p.to_path_buf());
    match lang {
        "python" => {
            let manifest: Manifest = serde_json::from_str(&text)?;
            Ok(ManifestPlugin::Python(
                PythonManifestPlugin::from_manifest_with_base(manifest, base),
            ))
        }
        "java" => {
            let manifest: Manifest = serde_json::from_str(&text)?;
            Ok(ManifestPlugin::Java(
                JavaManifestPlugin::from_manifest_with_base(manifest, base),
            ))
        }
        "node" | "js" | "javascript" | "ts" | "typescript" => {
            let manifest: Manifest = serde_json::from_str(&text)?;
            Ok(ManifestPlugin::Node(
                NodeManifestPlugin::from_manifest_with_base(manifest, base),
            ))
        }
        "c" | "cpp" | "c++" | "c_cpp" => {
            let manifest: Manifest = serde_json::from_str(&text)?;
            Ok(ManifestPlugin::Cpp(
                CppManifestPlugin::from_manifest_with_base(manifest, base),
            ))
        }
        other => Err(ManifestDispatchError::UnknownLanguage(other.to_string())),
    }
}

/// Wrapper for language-dispatched manifest plugins.
pub enum ManifestPlugin {
    Python(PythonManifestPlugin),
    Java(JavaManifestPlugin),
    Node(NodeManifestPlugin),
    Cpp(CppManifestPlugin),
}

impl Plugin for ManifestPlugin {
    fn id(&self) -> &'static str {
        match self {
            ManifestPlugin::Python(p) => p.id(),
            ManifestPlugin::Java(j) => j.id(),
            ManifestPlugin::Node(n) => n.id(),
            ManifestPlugin::Cpp(c) => c.id(),
        }
    }

    fn install(&self, registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        match self {
            ManifestPlugin::Python(p) => p.install(registry),
            ManifestPlugin::Java(j) => j.install(registry),
            ManifestPlugin::Node(n) => n.install(registry),
            ManifestPlugin::Cpp(c) => c.install(registry),
        }
    }
}

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::*;
use crate::SCHEMA_VERSION;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum FfiContractError {
    #[error("{surface} version mismatch: expected {expected}, found {found}")]
    VersionMismatch {
        surface: &'static str,
        expected: u32,
        found: u32,
    },
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("duplicate node id `{node_id}`")]
    DuplicateNode { node_id: String },
    #[error("duplicate {direction} port `{port}` on node `{node_id}`")]
    DuplicatePort {
        node_id: String,
        direction: &'static str,
        port: String,
    },
    #[error("node `{node_id}` backend config missing `{field}`")]
    MissingBackendField {
        node_id: String,
        field: &'static str,
    },
    #[error("missing backend config for node `{node_id}`")]
    MissingBackendConfig { node_id: String },
    #[error(
        "backend mismatch for node `{node_id}`: schema has {schema_backend:?}, config has {config_backend:?}"
    )]
    BackendMismatch {
        node_id: String,
        schema_backend: BackendKind,
        config_backend: BackendKind,
    },
    #[error("node `{node_id}` uses backend {found:?}, expected {expected:?}")]
    UnexpectedBackendKind {
        node_id: String,
        expected: BackendKind,
        found: BackendKind,
    },
    #[error("package artifact path must be relative and stay inside package: `{path}`")]
    UnsafePackagePath { path: String },
    #[error("package artifact path must have a file name: `{path}`")]
    MissingArtifactFileName { path: String },
    #[error("package artifact `{path}` is missing under `{base_dir}`")]
    MissingPackageArtifact { path: String, base_dir: String },
    #[error("package hash mismatch for `{path}`: expected {expected}, found {actual}")]
    PackageHashMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("package hash error: {message}")]
    PackageHashError { message: String },
    #[error("package I/O error at `{path}`: {message}")]
    PackageIo { path: String, message: String },
    #[error("package JSON error at `{path}`: {message}")]
    PackageJson { path: String, message: String },
}

pub(crate) fn validate_schema_version(
    surface: &'static str,
    found: u32,
) -> Result<(), FfiContractError> {
    if found != SCHEMA_VERSION {
        return Err(FfiContractError::VersionMismatch {
            surface,
            expected: SCHEMA_VERSION,
            found,
        });
    }
    Ok(())
}

pub(crate) fn validate_non_empty(field: &'static str, value: &str) -> Result<(), FfiContractError> {
    if value.trim().is_empty() {
        return Err(FfiContractError::EmptyField { field });
    }
    Ok(())
}

pub(crate) fn validate_package_relative_path(path: &str) -> Result<(), FfiContractError> {
    let path_ref = Path::new(path);
    if path_ref.is_absolute()
        || path_ref.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(FfiContractError::UnsafePackagePath {
            path: path.to_string(),
        });
    }
    Ok(())
}

pub fn bundled_artifact_path(
    kind: PackageArtifactKind,
    original_path: &str,
    platform: Option<&PackagePlatform>,
) -> Result<String, FfiContractError> {
    let file_name = package_file_name(original_path)?;
    let dir = match kind {
        PackageArtifactKind::SourceFile => "_bundle/src".to_string(),
        PackageArtifactKind::CompiledModule => "_bundle/modules".to_string(),
        PackageArtifactKind::Jar | PackageArtifactKind::ClassesDir => "_bundle/java".to_string(),
        PackageArtifactKind::SharedLibrary | PackageArtifactKind::NativeLibrary => {
            format!("_bundle/native/{}", package_platform_dir(platform))
        }
        PackageArtifactKind::ShaderAsset => "_bundle/shaders".to_string(),
        PackageArtifactKind::Lockfile => "_bundle/locks".to_string(),
        PackageArtifactKind::Other => "_bundle/assets".to_string(),
    };
    Ok(format!("{dir}/{file_name}"))
}

pub fn package_platform_dir(platform: Option<&PackagePlatform>) -> String {
    let Some(platform) = platform else {
        return "any".into();
    };
    [
        platform.os.as_deref().unwrap_or("any"),
        platform.arch.as_deref().unwrap_or("any"),
        platform.abi.as_deref().unwrap_or("any"),
    ]
    .join("-")
}

fn package_file_name(path: &str) -> Result<String, FfiContractError> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| FfiContractError::MissingArtifactFileName { path: path.into() })
}

pub(crate) fn sha256_file_hex(path: impl AsRef<Path>) -> Result<String, FfiContractError> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(|err| FfiContractError::PackageHashError {
        message: format!("failed to open {}: {err}", path.display()),
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| FfiContractError::PackageHashError {
                message: format!("failed to read {}: {err}", path.display()),
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

pub(crate) fn sha256_bytes_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn validate_ports(
    node_id: &str,
    direction: &'static str,
    ports: &[WirePort],
) -> Result<(), FfiContractError> {
    let mut names = std::collections::BTreeSet::new();
    for port in ports {
        validate_non_empty("port.name", &port.name)?;
        if !names.insert(port.name.as_str()) {
            return Err(FfiContractError::DuplicatePort {
                node_id: node_id.to_string(),
                direction,
                port: port.name.clone(),
            });
        }
    }
    Ok(())
}

pub(crate) fn require_some(
    node_id: &str,
    field: &'static str,
    value: &Option<String>,
) -> Result<(), FfiContractError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(FfiContractError::MissingBackendField {
            node_id: node_id.to_string(),
            field,
        }),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageArtifact {
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageArtifactKind {
    SourceFile,
    CompiledModule,
    Jar,
    ClassesDir,
    SharedLibrary,
    NativeLibrary,
    ShaderAsset,
    Lockfile,
    Other,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackagePlatform {
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub abi: Option<String>,
}

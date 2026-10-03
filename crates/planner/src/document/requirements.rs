//! Plugin requirements declared by a [`GraphDocument`](super::GraphDocument).
//!
//! Version requirements are intentionally small (no semver dependency):
//! - `None`, `""` or `"*"`: any installed version (including an unversioned plugin),
//! - `"=1.2.3"`: exactly this version,
//! - `">=1.2.3"` or bare `"1.2.3"`: at least this version.
//!
//! Versions compare numerically per dot-separated component; missing trailing components are
//! treated as `0` and any pre-release/build suffix (`-…`, `+…`) is ignored.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A plugin that must be loaded for a graph document to be usable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginRequirement {
    /// Plugin id, as passed to `declare_plugin!(Type, "plugin.id", ...)` / `Plugin::id()`.
    pub id: String,
    /// Optional version requirement (`"=x.y.z"`, `">=x.y.z"`, bare `"x.y.z"` meaning `>=`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl PluginRequirement {
    /// Require a plugin by id, any version.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
        }
    }

    /// Attach a version requirement.
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Validate the requirement syntax (non-empty id, parseable version requirement).
    pub fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("plugin id must not be empty".into());
        }
        if let Some(version) = &self.version {
            VersionReq::parse(version)?;
        }
        Ok(())
    }

    /// Check this requirement against an installed plugin version.
    pub fn check_version(&self, installed: Option<&str>) -> Result<(), UnmetReason> {
        let Some(raw_req) = self.version.as_deref() else {
            return Ok(());
        };
        let req = VersionReq::parse(raw_req).map_err(UnmetReason::InvalidRequirement)?;
        let (op, wanted) = match req {
            VersionReq::Any => return Ok(()),
            VersionReq::Exact(v) => (Ordering::Equal, v),
            VersionReq::AtLeast(v) => (Ordering::Greater, v),
        };
        let Some(installed_raw) = installed else {
            return Err(UnmetReason::VersionUnknown);
        };
        let installed =
            parse_version(installed_raw).map_err(|_| UnmetReason::InvalidInstalledVersion {
                installed: installed_raw.to_string(),
            })?;
        let ordering = compare_versions(&installed, &wanted);
        let ok = match op {
            Ordering::Equal => ordering == Ordering::Equal,
            _ => ordering != Ordering::Less,
        };
        if ok {
            Ok(())
        } else {
            Err(UnmetReason::VersionMismatch {
                installed: installed_raw.to_string(),
            })
        }
    }
}

impl fmt::Display for PluginRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.version {
            Some(version) => write!(f, "{} ({version})", self.id),
            None => f.write_str(&self.id),
        }
    }
}

/// Why a single plugin requirement was not satisfied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnmetReason {
    /// No plugin with the requested id is loaded.
    NotInstalled,
    /// The plugin is loaded but its version does not satisfy the requirement.
    VersionMismatch { installed: String },
    /// The requirement pins a version but the loaded plugin declares none.
    VersionUnknown,
    /// The loaded plugin's version string could not be parsed.
    InvalidInstalledVersion { installed: String },
    /// The requirement's version string could not be parsed.
    InvalidRequirement(String),
}

impl fmt::Display for UnmetReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => f.write_str("not loaded"),
            Self::VersionMismatch { installed } => write!(f, "loaded version {installed}"),
            Self::VersionUnknown => f.write_str("loaded plugin declares no version"),
            Self::InvalidInstalledVersion { installed } => {
                write!(f, "loaded version `{installed}` is not parseable")
            }
            Self::InvalidRequirement(err) => write!(f, "invalid requirement: {err}"),
        }
    }
}

/// A requirement paired with the reason it is unmet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnmetRequirement {
    pub requirement: PluginRequirement,
    pub reason: UnmetReason,
}

/// Error returned when one or more plugin requirements are not satisfied.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("missing required plugins: {}", format_unmet(.unmet))]
pub struct MissingPlugins {
    /// Unmet requirements, in declaration order.
    pub unmet: Vec<UnmetRequirement>,
}

impl MissingPlugins {
    /// Ids of all unmet requirements.
    pub fn ids(&self) -> Vec<&str> {
        self.unmet
            .iter()
            .map(|u| u.requirement.id.as_str())
            .collect()
    }
}

fn format_unmet(unmet: &[UnmetRequirement]) -> String {
    unmet
        .iter()
        .map(|u| format!("{} [{}]", u.requirement, u.reason))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Check `requires` against the loaded plugins.
///
/// `lookup` returns `None` when no plugin with the id is loaded, or `Some(version)` (where
/// `version` may itself be `None` for unversioned plugins) when it is.
pub fn check_plugin_requirements<'a, F>(
    requires: &[PluginRequirement],
    mut lookup: F,
) -> Result<(), MissingPlugins>
where
    F: FnMut(&str) -> Option<Option<&'a str>>,
{
    let unmet: Vec<_> = requires
        .iter()
        .filter_map(|req| {
            let reason = match lookup(&req.id) {
                None => UnmetReason::NotInstalled,
                Some(installed) => req.check_version(installed).err()?,
            };
            Some(UnmetRequirement {
                requirement: req.clone(),
                reason,
            })
        })
        .collect();
    if unmet.is_empty() {
        Ok(())
    } else {
        Err(MissingPlugins { unmet })
    }
}

enum VersionReq {
    Any,
    Exact(Vec<u64>),
    AtLeast(Vec<u64>),
}

impl VersionReq {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() || raw == "*" {
            return Ok(Self::Any);
        }
        if let Some(rest) = raw.strip_prefix(">=") {
            return parse_version(rest).map(Self::AtLeast);
        }
        if let Some(rest) = raw.strip_prefix('=') {
            return parse_version(rest).map(Self::Exact);
        }
        parse_version(raw).map(Self::AtLeast)
    }
}

fn parse_version(raw: &str) -> Result<Vec<u64>, String> {
    let raw = raw.trim();
    let core = raw.split(['-', '+']).next().unwrap_or_default();
    if core.is_empty() {
        return Err(format!("`{raw}` is not a version (expected x.y.z)"));
    }
    core.split('.')
        .map(|part| {
            part.parse::<u64>()
                .map_err(|_| format!("`{raw}` is not a version (expected x.y.z)"))
        })
        .collect()
}

fn compare_versions(a: &[u64], b: &[u64]) -> Ordering {
    let len = a.len().max(b.len());
    (0..len)
        .map(|i| {
            let x = a.get(i).copied().unwrap_or(0);
            let y = b.get(i).copied().unwrap_or(0);
            x.cmp(&y)
        })
        .find(|o| *o != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

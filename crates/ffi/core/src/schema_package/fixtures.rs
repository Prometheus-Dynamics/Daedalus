use std::collections::BTreeMap;

use daedalus_transport::BoundaryTypeContract;
use serde::{Deserialize, Serialize};

use super::*;
use crate::{InvokeEvent, InvokeRequest, InvokeResponse, WireValue};

/// Explicit layer map for the FFI rewrite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FfiLayer {
    PackageDiscovery,
    Schema,
    HostCore,
    BackendRuntime,
    Transport,
}

/// Backend execution models supported as first-class paths.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRuntimeModel {
    InProcessAbi,
    PersistentWorker,
}

/// Language/backend family for a node or plugin schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Rust,
    Python,
    Node,
    Java,
    CCpp,
    Shader,
    Other(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureLanguage {
    Rust,
    Python,
    Node,
    Java,
    CCpp,
}

impl FixtureLanguage {
    pub fn backend(self) -> BackendKind {
        match self {
            Self::Rust => BackendKind::Rust,
            Self::Python => BackendKind::Python,
            Self::Node => BackendKind::Node,
            Self::Java => BackendKind::Java,
            Self::CCpp => BackendKind::CCpp,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
            Self::Node => "node",
            Self::Java => "java",
            Self::CCpp => "c_cpp",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalFixtureKind {
    Scalar,
    Bytes,
    Image,
    Struct,
    Enum,
    Optional,
    List,
    Tuple,
    Map,
    MultiOutput,
    RawIo,
    Stateful,
    Shader,
    CapabilityBacked,
    CustomTypeKey,
    BoundaryContract,
    PackageArtifact,
    FailureDiagnostic,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CanonicalFixtureSpec {
    pub kind: CanonicalFixtureKind,
    pub name: String,
    pub node_id: String,
    pub inputs: Vec<WirePort>,
    pub outputs: Vec<WirePort>,
    pub request_inputs: BTreeMap<String, WireValue>,
    pub expected_outputs: BTreeMap<String, WireValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_state: Option<WireValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_state: Option<WireValue>,
    #[serde(default)]
    pub expected_events: Vec<InvokeEvent>,
    #[serde(default)]
    pub required_host_capabilities: Vec<String>,
    #[serde(default)]
    pub boundary_contracts: Vec<BoundaryTypeContract>,
    #[serde(default)]
    pub package_artifacts: Vec<PackageArtifact>,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub stateful: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedFixtureFile {
    pub path: String,
    pub contents: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GeneratedLanguageFixture {
    pub language: FixtureLanguage,
    pub schema: PluginSchema,
    pub backends: BTreeMap<String, BackendConfig>,
    pub request: InvokeRequest,
    pub expected_response: InvokeResponse,
    pub files: Vec<GeneratedFixtureFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedPackageFixture {
    pub language: FixtureLanguage,
    pub package: PluginPackage,
    pub files: Vec<GeneratedFixtureFile>,
}

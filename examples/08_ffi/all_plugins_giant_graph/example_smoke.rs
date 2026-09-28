#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi_core::{
    BackendConfig, BackendKind, BackendRuntimeModel, FixtureLanguage, InvokeEvent, InvokeRequest,
    InvokeResponse, NodeSchema, PackageArtifact, PackageArtifactKind, PluginPackage, PluginSchema,
    PluginSchemaInfo, SCHEMA_VERSION, WORKER_PROTOCOL_VERSION, WirePort, WireValue,
};
use serde::Deserialize;
use thiserror::Error;

use daedalus_ffi_host::{
    BackendRunner, BackendRunnerFactory, FfiHostTelemetry, HostInstallError, HostInstallPlan,
    ResponseDecodeError, RunnerHealth, RunnerKey, RunnerPool, RunnerPoolError, decode_response,
    install_package, install_package_with_ffi_telemetry, install_plan_runners,
};
use daedalus_runtime::{FfiBackendTelemetry, FfiTelemetryReport};

use crate::giant_graph_coverage::{GiantGraphCoverageSummary, GiantGraphLanguageCoverage};

mod support;
use support::*;

const EXAMPLE_LANGUAGES: &[ExampleLanguage] = &[
    ExampleLanguage {
        fixture: FixtureLanguage::Rust,
        dir_name: "rust",
    },
    ExampleLanguage {
        fixture: FixtureLanguage::Python,
        dir_name: "python",
    },
    ExampleLanguage {
        fixture: FixtureLanguage::Node,
        dir_name: "node",
    },
    ExampleLanguage {
        fixture: FixtureLanguage::Java,
        dir_name: "java",
    },
    ExampleLanguage {
        fixture: FixtureLanguage::CCpp,
        dir_name: "cpp",
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExampleSmokeReport {
    pub packages_loaded: usize,
    pub nodes_invoked: usize,
    pub expected_errors_checked: usize,
    pub languages: Vec<FixtureLanguage>,
    pub runner_start_count: u64,
    pub runner_reuse_count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GiantGraphSmokeReport {
    pub packages_loaded: usize,
    pub nodes_invoked: usize,
    pub edges_validated: usize,
    pub package_artifacts_checked: usize,
    pub expected_errors_checked: usize,
    pub coverage: GiantGraphCoverageSummary,
    pub telemetry: FfiTelemetryReport,
}

#[derive(Debug, Error)]
pub enum ExampleSmokeError {
    #[error("failed to read transcript `{path}`: {source}")]
    ReadTranscript {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse transcript `{path}`: {source}")]
    ParseTranscript {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("invalid transcript `{path}`: {message}")]
    InvalidTranscript { path: PathBuf, message: String },
    #[error("failed to prepare package artifact `{path}`: {source}")]
    PrepareArtifact {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("package artifact validation failed for {language:?}: {message}")]
    ArtifactValidation {
        language: FixtureLanguage,
        message: String,
    },
    #[error("failed to install example package for {language:?}: {source}")]
    Install {
        language: FixtureLanguage,
        source: HostInstallError,
    },
    #[error("failed to invoke example node `{node_id}` for {language:?}: {source}")]
    Runner {
        language: FixtureLanguage,
        node_id: String,
        source: RunnerPoolError,
    },
    #[error("failed to decode example node `{node_id}` for {language:?}: {source}")]
    Decode {
        language: FixtureLanguage,
        node_id: String,
        source: ResponseDecodeError,
    },
    #[error(
        "example output mismatch for `{node_id}` in {language:?}: expected {expected:?}, found {found:?}"
    )]
    OutputMismatch {
        language: FixtureLanguage,
        node_id: String,
        expected: BTreeMap<String, WireValue>,
        found: BTreeMap<String, WireValue>,
    },
    #[error(
        "example event mismatch for `{node_id}` in {language:?}: expected {expected:?}, found {found:?}"
    )]
    EventMismatch {
        language: FixtureLanguage,
        node_id: String,
        expected: Vec<InvokeEvent>,
        found: Vec<InvokeEvent>,
    },
    #[error(
        "example state mismatch for `{node_id}` in {language:?}: expected {expected:?}, found {found:?}"
    )]
    StateMismatch {
        language: FixtureLanguage,
        node_id: String,
        expected: Box<Option<WireValue>>,
        found: Box<Option<WireValue>>,
    },
    #[error(
        "example error mismatch for `{node_id}` in {language:?}: expected code {expected}, found {found:?}"
    )]
    ErrorMismatch {
        language: FixtureLanguage,
        node_id: String,
        expected: String,
        found: Option<String>,
    },
}

#[derive(Clone, Copy)]
struct ExampleLanguage {
    fixture: FixtureLanguage,
    dir_name: &'static str,
}

#[derive(Clone, Debug, Deserialize)]
struct TranscriptEntry {
    node_id: String,
    #[serde(default)]
    args: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    outputs: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    state: Option<serde_json::Value>,
    #[serde(default)]
    events: Vec<InvokeEvent>,
    #[serde(default)]
    error: Option<TranscriptError>,
}

#[derive(Clone, Debug, Deserialize)]
struct TranscriptError {
    code: String,
}

pub fn run_example_package_smoke_test() -> Result<ExampleSmokeReport, ExampleSmokeError> {
    run_example_package_smoke_test_from_root(repo_root_from_manifest_dir())
}

pub fn run_example_giant_graph_smoke_test() -> Result<GiantGraphSmokeReport, ExampleSmokeError> {
    run_example_giant_graph_smoke_test_from_root(repo_root_from_manifest_dir())
}

pub fn run_example_package_smoke_test_from_root(
    repo_root: impl AsRef<Path>,
) -> Result<ExampleSmokeReport, ExampleSmokeError> {
    let examples_root = repo_root.as_ref().join("examples/08_ffi");
    let mut packages_loaded = 0;
    let mut nodes_invoked = 0;
    let mut expected_errors_checked = 0;
    let mut languages = Vec::new();
    let mut runner_start_count = 0;
    let mut runner_reuse_count = 0;

    for language in EXAMPLE_LANGUAGES {
        let transcript_path = examples_root
            .join(language.dir_name)
            .join("complex_plugin/expected-transcript.json");
        let transcript = read_transcript(&transcript_path)?;
        let package = package_from_transcript(language.fixture, &transcript, &transcript_path)?;
        let mut registry = daedalus_registry::capability::CapabilityRegistry::new();
        let plan = install_package(&mut registry, &package).map_err(|source| {
            ExampleSmokeError::Install {
                language: language.fixture,
                source,
            }
        })?;
        let responses = transcript_responses(&transcript)?;
        let factory = ExampleRunnerFactory { responses };
        let mut pool = RunnerPool::new();
        install_plan_runners(&mut pool, &plan, &factory).map_err(|source| {
            ExampleSmokeError::Install {
                language: language.fixture,
                source,
            }
        })?;

        for entry in &transcript {
            invoke_transcript_entry(language.fixture, entry, &plan, &pool, None)?;
            nodes_invoked += 1;
            if entry.error.is_some() {
                expected_errors_checked += 1;
            }
        }
        let telemetry = pool.telemetry();
        runner_start_count += telemetry.starts;
        runner_reuse_count += telemetry.reuses;
        packages_loaded += 1;
        languages.push(language.fixture);
    }

    let unique = languages.iter().copied().collect::<BTreeSet<_>>();
    languages = unique.into_iter().collect();
    Ok(ExampleSmokeReport {
        packages_loaded,
        nodes_invoked,
        expected_errors_checked,
        languages,
        runner_start_count,
        runner_reuse_count,
    })
}

pub fn validate_showcase_descriptors_against_rust_baseline() -> Result<(), ExampleSmokeError> {
    validate_showcase_descriptors_against_rust_baseline_from_root(repo_root_from_manifest_dir())
}

pub fn validate_showcase_descriptors_against_rust_baseline_from_root(
    repo_root: impl AsRef<Path>,
) -> Result<(), ExampleSmokeError> {
    let examples_root = repo_root.as_ref().join("examples/08_ffi");
    let rust_transcript_path = examples_root
        .join("rust")
        .join("complex_plugin/expected-transcript.json");
    let rust_transcript = read_transcript(&rust_transcript_path)?;
    let rust_package = package_from_transcript(
        FixtureLanguage::Rust,
        &rust_transcript,
        &rust_transcript_path,
    )?;
    let rust_schema = rust_package
        .schema
        .as_ref()
        .expect("package_from_transcript always emits schema");
    let baseline_nodes = rust_schema
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.as_str(),
                node.inputs.len(),
                node.outputs.len(),
                node.stateful,
            )
        })
        .collect::<Vec<_>>();

    for language in EXAMPLE_LANGUAGES {
        let transcript_path = examples_root
            .join(language.dir_name)
            .join("complex_plugin/expected-transcript.json");
        let transcript = read_transcript(&transcript_path)?;
        let package = package_from_transcript(language.fixture, &transcript, &transcript_path)?;
        package
            .validate()
            .map_err(|source| ExampleSmokeError::InvalidTranscript {
                path: transcript_path.clone(),
                message: source.to_string(),
            })?;
        let schema = package
            .schema
            .as_ref()
            .expect("package_from_transcript always emits schema");
        let language_nodes = schema
            .nodes
            .iter()
            .map(|node| {
                (
                    node.id.as_str(),
                    node.inputs.len(),
                    node.outputs.len(),
                    node.stateful,
                )
            })
            .collect::<Vec<_>>();

        if language_nodes != baseline_nodes {
            return Err(ExampleSmokeError::InvalidTranscript {
                path: transcript_path,
                message: format!(
                    "{:?} descriptor surface does not match Rust baseline",
                    language.fixture
                ),
            });
        }
        if package.backends.len() != baseline_nodes.len() {
            return Err(ExampleSmokeError::InvalidTranscript {
                path: transcript_path,
                message: format!(
                    "{:?} descriptor has {} backends, expected {}",
                    language.fixture,
                    package.backends.len(),
                    baseline_nodes.len()
                ),
            });
        }
        if !package
            .backends
            .values()
            .all(|backend| backend.backend == language.fixture.backend())
        {
            return Err(ExampleSmokeError::InvalidTranscript {
                path: transcript_path,
                message: format!(
                    "{:?} descriptor contains wrong backend kind",
                    language.fixture
                ),
            });
        }
    }

    Ok(())
}

pub fn run_example_giant_graph_smoke_test_from_root(
    repo_root: impl AsRef<Path>,
) -> Result<GiantGraphSmokeReport, ExampleSmokeError> {
    let examples_root = repo_root.as_ref().join("examples/08_ffi");
    let artifact_root = temp_artifact_root()?;
    let mut packages_loaded = 0;
    let mut nodes_invoked = 0;
    let mut expected_errors_checked = 0;
    let mut package_artifacts_checked = 0;
    let mut coverage = GiantGraphCoverageSummary::default();
    let mut telemetry_report = FfiTelemetryReport::default();

    for language in EXAMPLE_LANGUAGES {
        let transcript_path = examples_root
            .join(language.dir_name)
            .join("complex_plugin/expected-transcript.json");
        let transcript = read_transcript(&transcript_path)?;
        let package = package_from_transcript(language.fixture, &transcript, &transcript_path)?;
        prepare_package_artifacts(&artifact_root, &package)?;
        package
            .validate_artifact_files(&artifact_root)
            .map_err(|source| ExampleSmokeError::ArtifactValidation {
                language: language.fixture,
                message: source.to_string(),
            })?;
        package_artifacts_checked += package.artifacts.len();

        let mut registry = daedalus_registry::capability::CapabilityRegistry::new();
        let telemetry = FfiHostTelemetry::new();
        let plan = install_package_with_ffi_telemetry(&mut registry, &package, &telemetry)
            .map_err(|source| ExampleSmokeError::Install {
                language: language.fixture,
                source,
            })?;
        let responses = transcript_responses(&transcript)?;
        let factory = ExampleRunnerFactory { responses };
        let mut pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
        install_plan_runners(&mut pool, &plan, &factory).map_err(|source| {
            ExampleSmokeError::Install {
                language: language.fixture,
                source,
            }
        })?;

        for entry in &transcript {
            invoke_transcript_entry(language.fixture, entry, &plan, &pool, Some(&telemetry))?;
            nodes_invoked += 1;
            if entry.error.is_some() {
                expected_errors_checked += 1;
            }
        }
        coverage.record_language(
            language.fixture.as_str(),
            coverage_from_transcript(&transcript),
        );
        telemetry_report.merge(telemetry.snapshot());
        packages_loaded += 1;
    }

    let edges_validated = giant_graph_edge_count();
    coverage = coverage.with_structure(packages_loaded, nodes_invoked, edges_validated);
    coverage
        .validate(
            &["rust", "python", "node", "java", "c_cpp"],
            expected_node_categories(),
            edges_validated,
        )
        .map_err(|source| ExampleSmokeError::InvalidTranscript {
            path: examples_root.join("all_plugins_giant_graph/giant_graph.rs"),
            message: source.to_string(),
        })?;
    let _ = fs::remove_dir_all(&artifact_root);
    Ok(GiantGraphSmokeReport {
        packages_loaded,
        nodes_invoked,
        edges_validated,
        package_artifacts_checked,
        expected_errors_checked,
        coverage,
        telemetry: telemetry_report,
    })
}

fn repo_root_from_manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("ffi host crate should live under crates/ffi/host")
        .to_path_buf()
}

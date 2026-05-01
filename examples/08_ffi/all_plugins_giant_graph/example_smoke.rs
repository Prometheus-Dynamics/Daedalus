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

fn read_transcript(path: &Path) -> Result<Vec<TranscriptEntry>, ExampleSmokeError> {
    let contents =
        fs::read_to_string(path).map_err(|source| ExampleSmokeError::ReadTranscript {
            path: path.into(),
            source,
        })?;
    serde_json::from_str(&contents).map_err(|source| ExampleSmokeError::ParseTranscript {
        path: path.into(),
        source,
    })
}

fn package_from_transcript(
    language: FixtureLanguage,
    transcript: &[TranscriptEntry],
    path: &Path,
) -> Result<PluginPackage, ExampleSmokeError> {
    if transcript.is_empty() {
        return Err(ExampleSmokeError::InvalidTranscript {
            path: path.into(),
            message: "transcript must contain at least one node".into(),
        });
    }
    let backend_kind = language.backend();
    let nodes = transcript
        .iter()
        .map(|entry| node_schema_from_entry(entry, backend_kind.clone()))
        .collect::<Vec<_>>();
    let schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: format!("ffi_showcase_{}", language.as_str()),
            version: Some("1.0.0".into()),
            description: Some("generated host smoke-test package".into()),
            metadata: BTreeMap::new(),
        },
        dependencies: Vec::new(),
        required_host_capabilities: Vec::new(),
        feature_flags: Vec::new(),
        boundary_contracts: Vec::new(),
        nodes,
    };
    let backends = transcript
        .iter()
        .map(|entry| {
            (
                entry.node_id.clone(),
                backend_config(language, &entry.node_id, backend_kind.clone()),
            )
        })
        .collect();
    let mut package = PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(schema),
        backends,
        artifacts: package_artifacts(language),
        lockfile: Some("plugin.lock.json".into()),
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::from([
            ("language".into(), serde_json::json!(language.as_str())),
            (
                "source".into(),
                serde_json::json!("examples/08_ffi expected transcript"),
            ),
        ]),
    };
    package
        .validate()
        .map_err(|source| ExampleSmokeError::InvalidTranscript {
            path: path.into(),
            message: source.to_string(),
        })?;
    package.manifest_hash = Some(package.compute_manifest_hash().map_err(|source| {
        ExampleSmokeError::InvalidTranscript {
            path: path.into(),
            message: source.to_string(),
        }
    })?);
    Ok(package)
}

fn package_artifacts(language: FixtureLanguage) -> Vec<PackageArtifact> {
    let paths = match language {
        FixtureLanguage::Rust => vec![
            (
                "_bundle/native/any/libffi_showcase.so",
                PackageArtifactKind::CompiledModule,
            ),
            ("_bundle/src/lib.rs", PackageArtifactKind::SourceFile),
            (
                "_bundle/src/build-package.rs",
                PackageArtifactKind::SourceFile,
            ),
        ],
        FixtureLanguage::Python => vec![
            (
                "_bundle/src/ffi_showcase.py",
                PackageArtifactKind::SourceFile,
            ),
            (
                "_bundle/src/build_package.py",
                PackageArtifactKind::SourceFile,
            ),
        ],
        FixtureLanguage::Node => vec![
            ("_bundle/src/plugin.ts", PackageArtifactKind::SourceFile),
            (
                "_bundle/src/build-package.ts",
                PackageArtifactKind::SourceFile,
            ),
            ("_bundle/assets/package.json", PackageArtifactKind::Other),
        ],
        FixtureLanguage::Java => vec![
            ("_bundle/java/ffi-showcase.jar", PackageArtifactKind::Jar),
            ("_bundle/java/main", PackageArtifactKind::ClassesDir),
            (
                "_bundle/native/any/libffi_showcase_jni.so",
                PackageArtifactKind::NativeLibrary,
            ),
        ],
        FixtureLanguage::CCpp => vec![
            (
                "_bundle/native/any/libffi_showcase.so",
                PackageArtifactKind::SharedLibrary,
            ),
            ("_bundle/src/showcase.cpp", PackageArtifactKind::SourceFile),
            (
                "_bundle/src/build-package.cpp",
                PackageArtifactKind::SourceFile,
            ),
        ],
    };
    paths
        .into_iter()
        .map(|(path, kind)| PackageArtifact {
            path: path.into(),
            kind,
            backend: Some(language.backend()),
            platform: None,
            sha256: None,
            metadata: BTreeMap::new(),
        })
        .collect()
}

fn prepare_package_artifacts(
    root: &Path,
    package: &PluginPackage,
) -> Result<(), ExampleSmokeError> {
    for artifact in &package.artifacts {
        let path = root.join(&artifact.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ExampleSmokeError::PrepareArtifact {
                path: parent.into(),
                source,
            })?;
        }
        if matches!(artifact.kind, PackageArtifactKind::ClassesDir) {
            fs::create_dir_all(&path).map_err(|source| ExampleSmokeError::PrepareArtifact {
                path: path.clone(),
                source,
            })?;
        } else {
            fs::write(&path, b"ffi smoke artifact").map_err(|source| {
                ExampleSmokeError::PrepareArtifact {
                    path: path.clone(),
                    source,
                }
            })?;
        }
    }
    Ok(())
}

fn node_schema_from_entry(entry: &TranscriptEntry, backend: BackendKind) -> NodeSchema {
    NodeSchema {
        id: entry.node_id.clone(),
        backend,
        entrypoint: entry.node_id.clone(),
        label: None,
        stateful: entry.state.is_some(),
        feature_flags: Vec::new(),
        inputs: entry
            .args
            .keys()
            .map(|name| smoke_port(name))
            .collect::<Vec<_>>(),
        outputs: entry
            .outputs
            .keys()
            .map(|name| smoke_port(name))
            .collect::<Vec<_>>(),
        metadata: BTreeMap::new(),
    }
}

fn smoke_port(name: &str) -> WirePort {
    WirePort {
        name: name.into(),
        ty: TypeExpr::scalar(ValueType::String),
        type_key: None,
        optional: false,
        access: Default::default(),
        residency: None,
        layout: None,
        source: None,
        const_value: None,
    }
}

fn backend_config(language: FixtureLanguage, node_id: &str, backend: BackendKind) -> BackendConfig {
    match language {
        FixtureLanguage::Rust => BackendConfig {
            backend,
            runtime_model: BackendRuntimeModel::InProcessAbi,
            entry_module: None,
            entry_class: None,
            entry_symbol: Some(node_id.into()),
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
        FixtureLanguage::Python => worker_backend(backend, "ffi_showcase.py", node_id, "python"),
        FixtureLanguage::Node => worker_backend(backend, "src/plugin.ts", node_id, "node"),
        FixtureLanguage::Java => BackendConfig {
            backend,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: None,
            entry_class: Some("ffi.showcase.Plugin".into()),
            entry_symbol: Some(node_id.into()),
            executable: Some("java".into()),
            args: Vec::new(),
            classpath: vec!["build/classes/java/main".into()],
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
        FixtureLanguage::CCpp => BackendConfig {
            backend,
            runtime_model: BackendRuntimeModel::InProcessAbi,
            entry_module: Some("build/libffi_showcase.so".into()),
            entry_class: None,
            entry_symbol: Some(node_id.into()),
            executable: None,
            args: Vec::new(),
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
    }
}

fn worker_backend(
    backend: BackendKind,
    entry_module: &str,
    node_id: &str,
    executable: &str,
) -> BackendConfig {
    BackendConfig {
        backend,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(entry_module.into()),
        entry_class: None,
        entry_symbol: Some(node_id.into()),
        executable: Some(executable.into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

fn transcript_responses(
    transcript: &[TranscriptEntry],
) -> Result<BTreeMap<String, ExampleInvocation>, ExampleSmokeError> {
    transcript
        .iter()
        .map(|entry| {
            Ok((
                entry.node_id.clone(),
                ExampleInvocation {
                    response: response_from_entry(entry)?,
                    error_code: entry.error.as_ref().map(|error| error.code.clone()),
                },
            ))
        })
        .collect()
}

fn response_from_entry(entry: &TranscriptEntry) -> Result<InvokeResponse, ExampleSmokeError> {
    Ok(InvokeResponse {
        protocol_version: WORKER_PROTOCOL_VERSION,
        correlation_id: None,
        outputs: entry
            .outputs
            .iter()
            .map(|(name, value)| Ok((name.clone(), wire_value(value.clone())?)))
            .collect::<Result<_, ExampleSmokeError>>()?,
        state: entry.state.clone().map(wire_value).transpose()?,
        events: entry.events.clone(),
    })
}

fn invoke_transcript_entry(
    language: FixtureLanguage,
    entry: &TranscriptEntry,
    plan: &HostInstallPlan,
    pool: &RunnerPool,
    telemetry: Option<&FfiHostTelemetry>,
) -> Result<(), ExampleSmokeError> {
    let backend = plan
        .backends
        .get(&entry.node_id)
        .expect("package install validated backend");
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: entry.node_id.clone(),
        correlation_id: Some(format!("{}:{}", language.as_str(), entry.node_id)),
        args: entry
            .args
            .iter()
            .map(|(name, value)| Ok((name.clone(), wire_value(value.clone())?)))
            .collect::<Result<_, ExampleSmokeError>>()?,
        state: None,
        context: BTreeMap::new(),
    };
    let response = if backend.runtime_model == BackendRuntimeModel::InProcessAbi {
        let invoke_started = Instant::now();
        let invocation = transcript_responses_for_node(entry)?;
        if let Some(expected) = &entry.error {
            return check_expected_error(language, &entry.node_id, expected, invocation.error_code);
        }
        let mut response = invocation.response;
        response.correlation_id = request.correlation_id.clone();
        if let (Some(telemetry), Ok(key)) = (telemetry, RunnerKey::from_backend(backend)) {
            telemetry.record_in_process_abi(
                &key,
                FfiBackendTelemetry {
                    backend_key: key.as_str().to_owned(),
                    backend_kind: Some(language.as_str().to_owned()),
                    language: Some(language.as_str().to_owned()),
                    node_id: Some(entry.node_id.clone()),
                    invokes: 1,
                    abi_call_duration: invoke_started.elapsed(),
                    pointer_length_payload_calls: u64::from(
                        entry.node_id.contains("zero_copy")
                            || entry.node_id.contains("shared")
                            || entry.node_id.contains("cow")
                            || entry.node_id.contains("mutable")
                            || entry.node_id.contains("owned"),
                    ),
                    ..Default::default()
                },
            );
        }
        response
    } else {
        match pool.invoke(backend, request.clone()) {
            Ok(response) => response,
            Err(source) => {
                if let Some(expected) = &entry.error {
                    return check_expected_error(
                        language,
                        &entry.node_id,
                        expected,
                        Some(source.to_string()),
                    );
                }
                return Err(ExampleSmokeError::Runner {
                    language,
                    node_id: entry.node_id.clone(),
                    source,
                });
            }
        }
    };
    if let Some(expected) = &entry.error {
        return check_expected_error(language, &entry.node_id, expected, None);
    }
    let decoded =
        decode_response(response, request.correlation_id.as_deref()).map_err(|source| {
            ExampleSmokeError::Decode {
                language,
                node_id: entry.node_id.clone(),
                source,
            }
        })?;
    let expected_outputs = entry
        .outputs
        .iter()
        .map(|(name, value)| Ok((name.clone(), wire_value(value.clone())?)))
        .collect::<Result<_, ExampleSmokeError>>()?;
    if decoded.outputs() != &expected_outputs {
        return Err(ExampleSmokeError::OutputMismatch {
            language,
            node_id: entry.node_id.clone(),
            expected: expected_outputs,
            found: decoded.outputs().clone(),
        });
    }
    let expected_state = entry.state.clone().map(wire_value).transpose()?;
    let found_state = decoded.state().cloned();
    if found_state != expected_state {
        return Err(ExampleSmokeError::StateMismatch {
            language,
            node_id: entry.node_id.clone(),
            expected: Box::new(expected_state),
            found: Box::new(found_state),
        });
    }
    if decoded.events() != entry.events {
        return Err(ExampleSmokeError::EventMismatch {
            language,
            node_id: entry.node_id.clone(),
            expected: entry.events.clone(),
            found: decoded.events().to_vec(),
        });
    }
    Ok(())
}

fn transcript_responses_for_node(
    entry: &TranscriptEntry,
) -> Result<ExampleInvocation, ExampleSmokeError> {
    Ok(ExampleInvocation {
        response: response_from_entry(entry)?,
        error_code: entry.error.as_ref().map(|error| error.code.clone()),
    })
}

fn check_expected_error(
    language: FixtureLanguage,
    node_id: &str,
    expected: &TranscriptError,
    found: Option<String>,
) -> Result<(), ExampleSmokeError> {
    if found
        .as_deref()
        .is_some_and(|found| found.contains(&expected.code))
    {
        Ok(())
    } else {
        Err(ExampleSmokeError::ErrorMismatch {
            language,
            node_id: node_id.into(),
            expected: expected.code.clone(),
            found,
        })
    }
}

fn expected_node_categories() -> usize {
    20
}

fn giant_graph_edge_count() -> usize {
    let languages = EXAMPLE_LANGUAGES.len();
    let categories = expected_node_categories();
    let category_chain_edges = categories * (languages - 1);
    let payload_ref_edges = (languages - 1) * 2;
    let adapter_edges = (languages - 1) * 2;
    let metrics_edges = categories * languages;
    category_chain_edges + payload_ref_edges + adapter_edges + metrics_edges
}

fn coverage_from_transcript(transcript: &[TranscriptEntry]) -> GiantGraphLanguageCoverage {
    GiantGraphLanguageCoverage {
        node_count: transcript.len(),
        adapter_nodes: transcript
            .iter()
            .filter(|entry| entry.node_id.contains("adapter"))
            .count() as u64,
        gpu_nodes: has_node(transcript, "gpu_tint") as u64,
        stateful_nodes: transcript
            .iter()
            .filter(|entry| entry.state.is_some())
            .count() as u64,
        zero_copy_nodes: has_node(transcript, "zero_copy_len") as u64,
        shared_reference_nodes: has_node(transcript, "shared_ref_len") as u64,
        cow_nodes: has_node(transcript, "cow_append_marker") as u64,
        mutable_nodes: has_node(transcript, "mutable_brighten") as u64,
        owned_nodes: has_node(transcript, "owned_bytes_len") as u64,
        typed_error_nodes: transcript
            .iter()
            .filter(|entry| entry.error.is_some())
            .count() as u64,
        raw_events: transcript
            .iter()
            .map(|entry| entry.events.len() as u64)
            .sum(),
    }
}

fn has_node(transcript: &[TranscriptEntry], node_id: &str) -> bool {
    transcript.iter().any(|entry| entry.node_id == node_id)
}

fn temp_artifact_root() -> Result<PathBuf, ExampleSmokeError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!(
        "daedalus-ffi-giant-graph-smoke-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&path).map_err(|source| ExampleSmokeError::PrepareArtifact {
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

fn wire_value(value: serde_json::Value) -> Result<WireValue, ExampleSmokeError> {
    Ok(match value {
        serde_json::Value::Null => WireValue::Unit,
        serde_json::Value::Bool(value) => WireValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                WireValue::Int(value)
            } else if let Some(value) = value.as_f64() {
                WireValue::Float(value)
            } else {
                WireValue::String(value.to_string())
            }
        }
        serde_json::Value::String(value) => WireValue::String(value),
        serde_json::Value::Array(items) => WireValue::List(
            items
                .into_iter()
                .map(wire_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(fields) => WireValue::Record(
            fields
                .into_iter()
                .map(|(name, value)| Ok((name, wire_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, ExampleSmokeError>>()?,
        ),
    })
}

#[derive(Clone)]
struct ExampleInvocation {
    response: InvokeResponse,
    error_code: Option<String>,
}

#[derive(Clone)]
struct ExampleRunnerFactory {
    responses: BTreeMap<String, ExampleInvocation>,
}

impl BackendRunnerFactory for ExampleRunnerFactory {
    fn build_runner(
        &self,
        node_id: &str,
        _backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        Ok(Arc::new(ExampleRunner {
            node_id: node_id.into(),
            invocation: self
                .responses
                .get(node_id)
                .cloned()
                .ok_or_else(|| RunnerPoolError::Runner(format!("missing node {node_id}")))?,
        }))
    }
}

struct ExampleRunner {
    node_id: String,
    invocation: ExampleInvocation,
}

impl BackendRunner for ExampleRunner {
    fn health(&self) -> RunnerHealth {
        RunnerHealth::Ready
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        Some(vec![self.node_id.clone()])
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        if let Some(code) = &self.invocation.error_code {
            return Err(RunnerPoolError::Runner(format!("worker error {code}")));
        }
        let mut response = self.invocation.response.clone();
        response.correlation_id = request.correlation_id;
        Ok(response)
    }
}

fn repo_root_from_manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("ffi host crate should live under crates/ffi/host")
        .to_path_buf()
}

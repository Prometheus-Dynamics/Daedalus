//! Transcript parsing, package fixtures and the canned runner behind the example smoke tests.

use super::*;

pub(super) fn read_transcript(path: &Path) -> Result<Vec<TranscriptEntry>, ExampleSmokeError> {
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

pub(super) fn package_from_transcript(
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

pub(super) fn package_artifacts(language: FixtureLanguage) -> Vec<PackageArtifact> {
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

pub(super) fn prepare_package_artifacts(
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

pub(super) fn node_schema_from_entry(entry: &TranscriptEntry, backend: BackendKind) -> NodeSchema {
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

pub(super) fn smoke_port(name: &str) -> WirePort {
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

pub(super) fn backend_config(
    language: FixtureLanguage,
    node_id: &str,
    backend: BackendKind,
) -> BackendConfig {
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

pub(super) fn worker_backend(
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

pub(super) fn transcript_responses(
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

pub(super) fn response_from_entry(
    entry: &TranscriptEntry,
) -> Result<InvokeResponse, ExampleSmokeError> {
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

pub(super) fn invoke_transcript_entry(
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

pub(super) fn transcript_responses_for_node(
    entry: &TranscriptEntry,
) -> Result<ExampleInvocation, ExampleSmokeError> {
    Ok(ExampleInvocation {
        response: response_from_entry(entry)?,
        error_code: entry.error.as_ref().map(|error| error.code.clone()),
    })
}

pub(super) fn check_expected_error(
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

pub(super) fn expected_node_categories() -> usize {
    20
}

pub(super) fn giant_graph_edge_count() -> usize {
    let languages = EXAMPLE_LANGUAGES.len();
    let categories = expected_node_categories();
    let category_chain_edges = categories * (languages - 1);
    let payload_ref_edges = (languages - 1) * 2;
    let adapter_edges = (languages - 1) * 2;
    let metrics_edges = categories * languages;
    category_chain_edges + payload_ref_edges + adapter_edges + metrics_edges
}

pub(super) fn coverage_from_transcript(
    transcript: &[TranscriptEntry],
) -> GiantGraphLanguageCoverage {
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

pub(super) fn has_node(transcript: &[TranscriptEntry], node_id: &str) -> bool {
    transcript.iter().any(|entry| entry.node_id == node_id)
}

pub(super) fn temp_artifact_root() -> Result<PathBuf, ExampleSmokeError> {
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

pub(super) fn wire_value(value: serde_json::Value) -> Result<WireValue, ExampleSmokeError> {
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
pub(super) struct ExampleInvocation {
    response: InvokeResponse,
    error_code: Option<String>,
}

#[derive(Clone)]
pub(super) struct ExampleRunnerFactory {
    pub(super) responses: BTreeMap<String, ExampleInvocation>,
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

pub(super) struct ExampleRunner {
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

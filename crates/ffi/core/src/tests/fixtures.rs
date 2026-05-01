use super::*;

#[test]
fn layer_and_runtime_model_are_explicit() {
    let layers = [
        FfiLayer::PackageDiscovery,
        FfiLayer::Schema,
        FfiLayer::HostCore,
        FfiLayer::BackendRuntime,
        FfiLayer::Transport,
    ];

    let json = serde_json::to_string(&layers).expect("serialize layers");
    assert!(json.contains("package_discovery"));
    assert!(json.contains("backend_runtime"));

    let model_json =
        serde_json::to_string(&BackendRuntimeModel::PersistentWorker).expect("serialize model");
    assert_eq!(model_json, "\"persistent_worker\"");
}

#[test]
fn canonical_fixture_specs_cover_declared_matrix_and_validate_contracts() {
    let specs = canonical_fixture_specs();
    let kinds = specs
        .iter()
        .map(|spec| spec.kind)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        kinds,
        [
            CanonicalFixtureKind::Scalar,
            CanonicalFixtureKind::Bytes,
            CanonicalFixtureKind::Image,
            CanonicalFixtureKind::Struct,
            CanonicalFixtureKind::Enum,
            CanonicalFixtureKind::Optional,
            CanonicalFixtureKind::List,
            CanonicalFixtureKind::Tuple,
            CanonicalFixtureKind::Map,
            CanonicalFixtureKind::MultiOutput,
            CanonicalFixtureKind::RawIo,
            CanonicalFixtureKind::Stateful,
            CanonicalFixtureKind::Shader,
            CanonicalFixtureKind::CapabilityBacked,
            CanonicalFixtureKind::CustomTypeKey,
            CanonicalFixtureKind::BoundaryContract,
            CanonicalFixtureKind::PackageArtifact,
            CanonicalFixtureKind::FailureDiagnostic,
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(specs.len(), 18);

    let mut names = std::collections::BTreeSet::new();
    let mut node_ids = std::collections::BTreeSet::new();
    for spec in &specs {
        assert!(names.insert(spec.name.clone()), "duplicate fixture name");
        assert!(node_ids.insert(spec.node_id.clone()), "duplicate node id");
        assert!(!spec.inputs.is_empty(), "{} missing inputs", spec.name);
        assert!(!spec.outputs.is_empty(), "{} missing outputs", spec.name);

        let node = NodeSchema {
            id: spec.node_id.clone(),
            backend: spec.backend.clone().unwrap_or(BackendKind::Python),
            entrypoint: "run".into(),
            label: None,
            stateful: spec.stateful,
            feature_flags: Vec::new(),
            inputs: spec.inputs.clone(),
            outputs: spec.outputs.clone(),
            metadata: BTreeMap::new(),
        };
        let schema = PluginSchema {
            schema_version: SCHEMA_VERSION,
            plugin: PluginSchemaInfo {
                name: format!("ffi.conformance.{}", spec.name),
                version: Some("1.0.0".into()),
                description: None,
                metadata: BTreeMap::new(),
            },
            dependencies: Vec::new(),
            required_host_capabilities: spec.required_host_capabilities.clone(),
            feature_flags: Vec::new(),
            boundary_contracts: spec.boundary_contracts.clone(),
            nodes: vec![node.clone()],
        };
        schema.validate().expect("fixture schema validates");
        if !spec.package_artifacts.is_empty() {
            let package = PluginPackage {
                schema_version: SCHEMA_VERSION,
                schema: Some(schema.clone()),
                backends: BTreeMap::from([(
                    spec.node_id.clone(),
                    BackendConfig {
                        backend: node.backend.clone(),
                        runtime_model: if matches!(node.backend, BackendKind::Shader) {
                            BackendRuntimeModel::InProcessAbi
                        } else {
                            BackendRuntimeModel::PersistentWorker
                        },
                        entry_module: Some("fixture".into()),
                        entry_class: None,
                        entry_symbol: Some("run".into()),
                        executable: Some("fixture-worker".into()),
                        args: Vec::new(),
                        classpath: Vec::new(),
                        native_library_paths: Vec::new(),
                        working_dir: None,
                        env: BTreeMap::new(),
                        options: BTreeMap::new(),
                    },
                )]),
                artifacts: spec.package_artifacts.clone(),
                lockfile: None,
                manifest_hash: None,
                signature: None,
                metadata: BTreeMap::new(),
            };
            package.validate().expect("fixture package validates");
        }

        let request = InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: spec.node_id.clone(),
            correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
            args: spec.request_inputs.clone(),
            state: spec.request_state.clone(),
            context: BTreeMap::new(),
        };
        request
            .validate_against_node(&node)
            .expect("fixture request validates");

        let response = InvokeResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
            outputs: spec.expected_outputs.clone(),
            state: spec.expected_state.clone(),
            events: spec.expected_events.clone(),
        };
        response
            .validate_against_node(&node)
            .expect("fixture response validates");
    }

    let raw_io = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::RawIo)
        .expect("raw io spec");
    assert_eq!(raw_io.expected_events.len(), 1);

    let stateful = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::Stateful)
        .expect("stateful spec");
    assert!(stateful.stateful);
    assert!(stateful.request_state.is_some());
    assert!(stateful.expected_state.is_some());

    let capability = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::CapabilityBacked)
        .expect("capability spec");
    assert_eq!(
        capability.required_host_capabilities,
        vec![String::from("camera.read")]
    );

    let shader = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::Shader)
        .expect("shader spec");
    assert_eq!(shader.backend, Some(BackendKind::Shader));

    let custom_type = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::CustomTypeKey)
        .expect("custom type spec");
    assert_eq!(
        custom_type.inputs[0].type_key,
        Some(TypeKey::new("example.Point"))
    );

    let boundary = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::BoundaryContract)
        .expect("boundary contract spec");
    assert_eq!(boundary.boundary_contracts.len(), 1);
    assert_eq!(boundary.inputs[0].access, AccessMode::Modify);
    assert_eq!(boundary.inputs[0].residency, Some(Residency::Gpu));
    assert_eq!(boundary.inputs[0].layout, Some(Layout::new("rgba8-hwc")));

    let artifact = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::PackageArtifact)
        .expect("package artifact spec");
    assert_eq!(artifact.package_artifacts.len(), 1);
    assert_eq!(
        artifact.package_artifacts[0].kind,
        PackageArtifactKind::ShaderAsset
    );

    let failure = specs
        .iter()
        .find(|spec| spec.kind == CanonicalFixtureKind::FailureDiagnostic)
        .expect("failure diagnostic spec");
    assert_eq!(failure.expected_events[0].level, InvokeEventLevel::Error);
}

#[test]
fn generates_scalar_add_conformance_fixtures_for_all_languages() {
    let fixtures = generate_scalar_add_fixtures().expect("fixtures");
    assert_eq!(fixtures.len(), 5);

    let languages = fixtures
        .iter()
        .map(|fixture| fixture.language)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        languages,
        [
            FixtureLanguage::Rust,
            FixtureLanguage::Python,
            FixtureLanguage::Node,
            FixtureLanguage::Java,
            FixtureLanguage::CCpp,
        ]
        .into_iter()
        .collect()
    );

    for fixture in &fixtures {
        fixture.schema.validate().expect("schema validates");
        assert_eq!(fixture.schema.nodes.len(), 1);
        let node = &fixture.schema.nodes[0];
        assert_eq!(node.id, "ffi.conformance.scalar_add:add");
        assert_eq!(node.backend, fixture.language.backend());
        fixture
            .backends
            .get(&node.id)
            .expect("backend")
            .validate_for_node(&node.id)
            .expect("backend validates");
        assert_eq!(fixture.request.args.get("a"), Some(&WireValue::Int(2)));
        assert_eq!(fixture.request.args.get("b"), Some(&WireValue::Int(40)));
        assert_eq!(
            fixture.expected_response.outputs.get("out"),
            Some(&WireValue::Int(42))
        );
        assert_eq!(fixture.files.len(), 1);
    }
}

#[test]
fn generates_canonical_conformance_fixtures_for_every_language() {
    let specs = canonical_fixture_specs();
    let fixtures = generate_canonical_fixtures().expect("fixtures");
    assert_eq!(fixtures.len(), specs.len() * fixture_languages().len());

    let mut seen = std::collections::BTreeSet::new();
    for fixture in fixtures {
        let node = &fixture.schema.nodes[0];
        let backend = fixture.backends.get(&node.id).expect("backend");
        assert_eq!(node.backend, backend.backend);
        fixture.schema.validate().expect("schema");
        backend.validate_for_node(&node.id).expect("backend");
        fixture
            .request
            .validate_against_node(node)
            .expect("request");
        fixture
            .expected_response
            .validate_against_node(node)
            .expect("response");
        seen.insert((fixture.language, node.id.clone()));
    }

    for spec in specs {
        for language in fixture_languages() {
            assert!(
                seen.contains(&(language, spec.node_id.clone())),
                "missing {:?} fixture for {}",
                language,
                spec.node_id
            );
        }
    }
}

#[test]
fn fixture_schema_and_backend_snapshots_are_stable() {
    let fixtures = generate_scalar_add_fixtures().expect("fixtures");
    let text = serde_json::to_string_pretty(&fixtures).expect("snapshot");
    for needle in [
        "ffi.conformance.python.scalar_add",
        "ffi.conformance.node.scalar_add",
        "ffi.conformance.java.scalar_add",
        "ffi.conformance.c_cpp.scalar_add",
        "ffi.conformance.rust.scalar_add",
        "scalar_add.py",
        "scalar_add.mjs",
        "ScalarAdd.java",
        "scalar_add.cpp",
    ] {
        assert!(text.contains(needle), "snapshot missing {needle}");
    }
}

#[test]
fn generated_package_fixtures_validate_and_stamp_integrity_for_all_languages() {
    let fixtures = generate_scalar_add_package_fixtures().expect("package fixtures");
    assert_eq!(fixtures.len(), 5);

    for mut fixture in fixtures {
        let root = tempfile::tempdir().expect("temp package root");
        for file in &fixture.files {
            let path = root.path().join(&file.path);
            fs::create_dir_all(path.parent().expect("artifact parent")).expect("mkdir");
            fs::write(path, &file.contents).expect("write artifact");
        }
        fixture
            .package
            .validate_artifact_files(root.path())
            .expect("artifact files");
        fixture
            .package
            .stamp_integrity(root.path())
            .expect("stamp integrity");
        fixture
            .package
            .verify_integrity(root.path())
            .expect("verify integrity");
        assert!(fixture.package.manifest_hash.is_some());
        assert!(
            fixture
                .package
                .artifacts
                .iter()
                .all(|artifact| artifact.path.starts_with("_bundle/src/"))
        );
    }
}

#[test]
fn generated_package_fixtures_reject_missing_language_required_fields() {
    let fixtures = generate_scalar_add_package_fixtures().expect("package fixtures");
    assert_eq!(fixtures.len(), fixture_languages().len());

    for fixture in fixtures {
        let node_id = fixture.package.schema.as_ref().expect("schema").nodes[0]
            .id
            .clone();

        let mut missing_backend = fixture.package.clone();
        missing_backend.backends.remove(&node_id);
        assert!(
            matches!(
                missing_backend.validate(),
                Err(FfiContractError::MissingBackendConfig { .. })
            ),
            "missing backend should fail for {:?}",
            fixture.language
        );

        let mut missing_artifact_path = fixture.package.clone();
        missing_artifact_path.artifacts[0].path.clear();
        assert!(
            matches!(
                missing_artifact_path.validate(),
                Err(FfiContractError::EmptyField {
                    field: "artifact.path"
                })
            ),
            "missing artifact path should fail for {:?}",
            fixture.language
        );

        let empty_root = tempfile::tempdir().expect("empty package root");
        assert!(
            matches!(
                fixture.package.validate_artifact_files(empty_root.path()),
                Err(FfiContractError::MissingPackageArtifact { .. })
            ),
            "missing artifact file should fail for {:?}",
            fixture.language
        );

        let mut missing_language_field = fixture.package.clone();
        let backend = missing_language_field
            .backends
            .get_mut(&node_id)
            .expect("backend");
        match fixture.language {
            FixtureLanguage::Rust => backend.entry_symbol = None,
            FixtureLanguage::Python | FixtureLanguage::Node => backend.executable = None,
            FixtureLanguage::Java => backend.classpath.clear(),
            FixtureLanguage::CCpp => backend.entry_module = None,
        }
        assert!(
            matches!(
                missing_language_field.validate(),
                Err(FfiContractError::MissingBackendField { .. })
            ),
            "missing language field should fail for {:?}",
            fixture.language
        );
    }
}

#[test]
fn generated_plugin_json_descriptor_snapshots_are_stable() {
    let fixtures = generate_scalar_add_package_fixtures().expect("package fixtures");
    let mut snapshot = BTreeMap::new();
    for fixture in fixtures {
        let package_json = serde_json::to_value(&fixture.package).expect("package descriptor json");
        snapshot.insert(fixture.language.as_str().to_string(), package_json);
    }

    let text = serde_json::to_string_pretty(&snapshot).expect("snapshot");
    for needle in [
        "\"rust\"",
        "\"python\"",
        "\"node\"",
        "\"java\"",
        "\"c_cpp\"",
        "\"schema_version\": 1",
        "\"runtime_model\": \"persistent_worker\"",
        "\"runtime_model\": \"in_process_abi\"",
        "\"plugin\":",
        "\"backends\":",
        "\"artifacts\":",
        "\"_bundle/src/scalar_add.py\"",
        "\"_bundle/src/scalar_add.mjs\"",
        "\"_bundle/src/ScalarAdd.java\"",
        "\"_bundle/src/scalar_add.cpp\"",
    ] {
        assert!(
            text.contains(needle),
            "plugin.json snapshot missing {needle}"
        );
    }
}

#[test]
fn generated_failure_fixtures_cover_bad_input_missing_node_and_schema_validation() {
    let fixture = generate_scalar_add_fixtures()
        .expect("fixtures")
        .into_iter()
        .find(|fixture| fixture.language == FixtureLanguage::Python)
        .expect("python fixture");

    let mut bad_input = fixture.request.clone();
    bad_input
        .args
        .insert("a".into(), WireValue::String("wrong".into()));
    assert!(matches!(
        bad_input.args.get("a"),
        Some(WireValue::String(value)) if value == "wrong"
    ));

    let mut missing_node_package = PluginPackage {
        schema_version: SCHEMA_VERSION,
        schema: Some(fixture.schema.clone()),
        backends: fixture.backends.clone(),
        artifacts: Vec::new(),
        lockfile: None,
        manifest_hash: None,
        signature: None,
        metadata: BTreeMap::new(),
    };
    missing_node_package.backends.clear();
    assert!(matches!(
        missing_node_package.validate(),
        Err(FfiContractError::MissingBackendConfig { .. })
    ));

    let mut invalid_schema = fixture.schema;
    invalid_schema.nodes[0].inputs[0].name.clear();
    assert!(matches!(
        invalid_schema.validate(),
        Err(FfiContractError::EmptyField { field: "port.name" })
    ));
}

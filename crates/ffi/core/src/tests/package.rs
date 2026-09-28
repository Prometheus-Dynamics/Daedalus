use super::*;

#[test]
fn plugin_schema_stays_separate_from_backend_config() {
    let image = WirePort::new("image", TypeExpr::opaque("image"));
    let mut schema = PluginSchema::new(
        "demo",
        Some("0.1.0".into()),
        vec![NodeSchema {
            label: Some("Blur".into()),
            stateful: true,
            ..NodeSchema::new(
                "demo.blur",
                BackendKind::Python,
                "blur",
                vec![image.clone()],
                vec![image],
            )
        }],
    );
    schema.plugin.description = Some("demo plugin".into());

    let backend = BackendConfig {
        env: BTreeMap::from([(String::from("PYTHONUNBUFFERED"), String::from("1"))]),
        ..BackendConfig::persistent_worker(BackendKind::Python, "python", "blur")
            .with_entry_module("plugin")
            .with_working_dir("examples/plugins/demo")
    };

    let schema_json = serde_json::to_value(&schema).expect("serialize schema");
    let backend_json = serde_json::to_value(&backend).expect("serialize backend config");

    assert!(schema_json.get("plugin").is_some());
    assert!(schema_json.get("nodes").is_some());
    assert!(schema_json.get("runtime_model").is_none());
    assert!(backend_json.get("runtime_model").is_some());
    assert!(backend_json.get("plugin").is_none());
}

#[test]
fn plugin_schema_validation_rejects_duplicate_nodes_and_ports() {
    let duplicated_port = WirePort::new(
        "value",
        TypeExpr::Scalar(daedalus_data::model::ValueType::Int),
    );
    let node = NodeSchema::new(
        "demo.node",
        BackendKind::Python,
        "run",
        vec![duplicated_port.clone(), duplicated_port],
        Vec::new(),
    );
    let schema = PluginSchema::new("demo", None, vec![node]);

    assert!(matches!(
        schema.validate(),
        Err(FfiContractError::DuplicatePort {
            node_id,
            direction: "input",
            port
        }) if node_id == "demo.node" && port == "value"
    ));

    let duplicate_nodes = PluginSchema {
        nodes: vec![
            NodeSchema::new("demo.node", BackendKind::Python, "run", vec![], vec![]),
            NodeSchema::new("demo.node", BackendKind::Node, "run", vec![], vec![]),
        ],
        ..schema
    };

    assert!(matches!(
        duplicate_nodes.validate(),
        Err(FfiContractError::DuplicateNode { node_id }) if node_id == "demo.node"
    ));
}

#[test]
fn backend_validation_enforces_language_entrypoints() {
    let backend = BackendConfig::persistent_worker(BackendKind::Java, "java", "add")
        .with_entry_class("demo.Nodes");

    assert!(matches!(
        backend.validate_for_node("demo:add"),
        Err(FfiContractError::MissingBackendField {
            node_id,
            field: "classpath"
        }) if node_id == "demo:add"
    ));
}

#[test]
fn package_validation_matches_schema_nodes_to_backends() {
    let schema = PluginSchema::new(
        "demo",
        None,
        vec![NodeSchema::new(
            "demo.add",
            BackendKind::Python,
            "add",
            vec![],
            vec![],
        )],
    );
    let package = PluginPackage::new(schema, BTreeMap::new());

    assert!(matches!(
        package.validate(),
        Err(FfiContractError::MissingBackendConfig { node_id }) if node_id == "demo.add"
    ));
}

#[test]
fn plugin_package_records_physical_artifacts_separately() {
    let package = PluginPackage {
        artifacts: vec![PackageArtifact {
            sha256: Some("abc123".into()),
            ..PackageArtifact::new(
                "_bundle/java/demo.jar",
                PackageArtifactKind::Jar,
                Some(BackendKind::Java),
            )
        }],
        lockfile: Some("plugin.lock".into()),
        manifest_hash: Some("hash".into()),
        ..PluginPackage::default()
    };

    let json = serde_json::to_value(&package).expect("serialize package");
    assert!(json.get("artifacts").is_some());
    assert!(json.get("schema").is_some());
    assert!(json.get("runtime_model").is_none());
}

#[test]
fn rust_package_emits_backend_artifacts_lockfile_and_manifest_hash() {
    let spec = scalar_add_fixture_spec();
    let fixture = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
    let package = rust_plugin_package(
        fixture.schema.clone(),
        fixture.backends.clone(),
        vec!["target/release/libffi_showcase.so".into()],
        vec!["src/lib.rs".into(), "build-package.rs".into()],
    )
    .expect("rust package");
    let lock = package.generate_lockfile();

    assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
    assert!(package.manifest_hash.is_some());
    assert_eq!(
        package.metadata.get("package_builder"),
        Some(&serde_json::json!("daedalus-ffi-core"))
    );
    assert_eq!(package.artifacts.len(), 3);
    assert_eq!(
        package.artifacts[0].kind,
        PackageArtifactKind::CompiledModule
    );
    assert_eq!(
        package.artifacts[0].path,
        "_bundle/modules/libffi_showcase.so"
    );
    assert_eq!(
        lock.plugin_name.as_deref(),
        Some("ffi.conformance.rust.scalar_add")
    );
    assert_eq!(lock.artifacts.len(), 3);
    validate_language_backends(
        package.schema.as_ref().expect("schema"),
        &package.backends,
        BackendKind::Rust,
    )
    .expect("rust backend package validates");
}

#[test]
fn package_validation_accepts_existing_artifact_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let artifact_dir = dir.path().join("_bundle/java");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
    std::fs::write(artifact_dir.join("demo.jar"), b"jar").expect("write artifact");

    let package = PluginPackage {
        artifacts: vec![PackageArtifact {
            sha256: Some("abc123".into()),
            ..PackageArtifact::new(
                "_bundle/java/demo.jar",
                PackageArtifactKind::Jar,
                Some(BackendKind::Java),
            )
        }],
        manifest_hash: Some("manifest-hash".into()),
        ..PluginPackage::default()
    };

    package
        .validate_artifact_files(dir.path())
        .expect("artifact exists");
}

#[test]
fn package_validation_rejects_missing_artifact_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let package = PluginPackage {
        artifacts: vec![PackageArtifact::new(
            "_bundle/java/missing.jar",
            PackageArtifactKind::Jar,
            Some(BackendKind::Java),
        )],
        ..PluginPackage::default()
    };

    assert!(matches!(
        package.validate_artifact_files(dir.path()),
        Err(FfiContractError::MissingPackageArtifact { path, .. })
            if path == "_bundle/java/missing.jar"
    ));
}

#[test]
fn package_validation_rejects_artifact_paths_outside_package() {
    let dir = tempfile::tempdir().expect("tempdir");
    let package = PluginPackage {
        artifacts: vec![PackageArtifact::new(
            "../outside.jar",
            PackageArtifactKind::Jar,
            Some(BackendKind::Java),
        )],
        ..PluginPackage::default()
    };

    assert!(matches!(
        package.validate_artifact_files(dir.path()),
        Err(FfiContractError::UnsafePackagePath { path }) if path == "../outside.jar"
    ));
}

#[test]
fn package_artifact_bundle_paths_are_deterministic_by_kind() {
    let platform = PackagePlatform {
        os: Some("linux".into()),
        arch: Some("x86_64".into()),
        abi: Some("gnu".into()),
    };

    assert_eq!(
        bundled_artifact_path(PackageArtifactKind::SourceFile, "src/main.py", None)
            .expect("source path"),
        "_bundle/src/main.py"
    );
    assert_eq!(
        bundled_artifact_path(PackageArtifactKind::Jar, "build/libs/demo.jar", None)
            .expect("jar path"),
        "_bundle/java/demo.jar"
    );
    assert_eq!(
        bundled_artifact_path(
            PackageArtifactKind::NativeLibrary,
            "target/libopencv_java.so",
            Some(&platform),
        )
        .expect("native path"),
        "_bundle/native/linux-x86_64-gnu/libopencv_java.so"
    );
    assert_eq!(
        bundled_artifact_path(
            PackageArtifactKind::ShaderAsset,
            "shaders/invert.wgsl",
            None
        )
        .expect("shader path"),
        "_bundle/shaders/invert.wgsl"
    );
}

#[test]
fn package_rewrites_artifact_paths_for_bundle_layout() {
    let mut package = PluginPackage {
        artifacts: vec![
            PackageArtifact::new(
                "rt.py",
                PackageArtifactKind::SourceFile,
                Some(BackendKind::Python),
            ),
            PackageArtifact::new(
                "build/classes/java/main",
                PackageArtifactKind::ClassesDir,
                Some(BackendKind::Java),
            ),
        ],
        ..PluginPackage::default()
    };

    package
        .rewrite_artifact_paths_for_bundle()
        .expect("rewrite paths");

    assert_eq!(package.artifacts[0].path, "_bundle/src/rt.py");
    assert_eq!(package.artifacts[1].path, "_bundle/java/main");
}

#[test]
fn package_integrity_stamps_and_verifies_artifact_hashes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let artifact_dir = dir.path().join("_bundle/assets");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
    std::fs::write(artifact_dir.join("data.bin"), b"payload").expect("write artifact");

    let mut package = PluginPackage {
        artifacts: vec![PackageArtifact::new(
            "_bundle/assets/data.bin",
            PackageArtifactKind::Other,
            None,
        )],
        ..PluginPackage::default()
    };

    package
        .stamp_integrity(dir.path())
        .expect("stamp integrity");

    assert_eq!(package.artifacts[0].sha256.as_ref().unwrap().len(), 64);
    assert_eq!(package.manifest_hash.as_ref().unwrap().len(), 64);
    package
        .verify_integrity(dir.path())
        .expect("integrity verifies");
}

#[test]
fn package_integrity_rejects_tampered_artifacts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let artifact_dir = dir.path().join("_bundle/assets");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
    std::fs::write(artifact_dir.join("data.bin"), b"payload").expect("write artifact");

    let mut package = PluginPackage {
        artifacts: vec![PackageArtifact::new(
            "_bundle/assets/data.bin",
            PackageArtifactKind::Other,
            None,
        )],
        ..PluginPackage::default()
    };
    package
        .stamp_integrity(dir.path())
        .expect("stamp integrity");

    std::fs::write(artifact_dir.join("data.bin"), b"tampered").expect("tamper artifact");

    assert!(matches!(
        package.verify_integrity(dir.path()),
        Err(FfiContractError::PackageHashMismatch { path, .. })
            if path == "_bundle/assets/data.bin"
    ));
}

#[test]
fn package_descriptor_loads_from_unpacked_root_without_repo_paths() {
    let root = tempfile::tempdir().expect("tempdir");
    let artifact_dir = root.path().join("_bundle/java");
    std::fs::create_dir_all(&artifact_dir).expect("create bundle dir");
    std::fs::write(artifact_dir.join("demo.jar"), b"jar").expect("write jar");

    let schema = PluginSchema::new(
        "demo.java",
        Some("0.1.0".into()),
        vec![NodeSchema::new(
            "demo.add",
            BackendKind::Java,
            "add",
            vec![],
            vec![],
        )],
    );
    let backend = BackendConfig::persistent_worker(BackendKind::Java, "java", "add")
        .with_entry_class("demo.Nodes")
        .with_classpath(["_bundle/java/demo.jar"]);
    let mut package = PluginPackage {
        artifacts: vec![PackageArtifact::new(
            "_bundle/java/demo.jar",
            PackageArtifactKind::Jar,
            Some(BackendKind::Java),
        )],
        ..PluginPackage::new(schema, BTreeMap::from([("demo.add".into(), backend)]))
    };
    package.stamp_integrity(root.path()).expect("stamp package");
    let descriptor_path = root.path().join("plugin.json");
    package
        .write_descriptor(&descriptor_path)
        .expect("write descriptor");

    let loaded = PluginPackage::read_descriptor_and_verify(&descriptor_path, root.path())
        .expect("load package");

    assert_eq!(loaded.schema.as_ref().unwrap().plugin.name, "demo.java");
    assert_eq!(loaded.artifacts[0].path, "_bundle/java/demo.jar");
    assert_eq!(
        loaded.backends["demo.add"].classpath,
        vec![String::from("_bundle/java/demo.jar")]
    );
}

#[test]
fn package_lockfile_generation_is_deterministic_and_language_aware() {
    let backends = BTreeMap::from([
        (
            "cpp.node".into(),
            BackendConfig::in_process(BackendKind::CCpp, "run")
                .with_entry_module("_bundle/native/linux-x86_64-gnu/libdemo.so"),
        ),
        (
            "java.node".into(),
            BackendConfig {
                native_library_paths: vec!["_bundle/native/linux-x86_64-gnu/libopencv.so".into()],
                ..BackendConfig::persistent_worker(BackendKind::Java, "java", "add")
                    .with_entry_class("demo.Nodes")
                    .with_classpath(["_bundle/java/demo.jar"])
                    .with_options([(
                        "maven_coordinates".into(),
                        serde_json::json!(["org.demo:demo:1.0.0"]),
                    )])
            },
        ),
        (
            "node.node".into(),
            BackendConfig::persistent_worker(BackendKind::Node, "node", "run")
                .with_entry_module("_bundle/src/index.mjs")
                .with_options([(
                    "package".into(),
                    serde_json::json!({"name":"demo-node","version":"1.0.0"}),
                )]),
        ),
        (
            "python.node".into(),
            BackendConfig::persistent_worker(BackendKind::Python, "python", "run")
                .with_entry_module("_bundle/src/rt.py")
                .with_options([("requirements".into(), serde_json::json!(["numpy==2.0.0"]))]),
        ),
    ]);
    let package = PluginPackage {
        artifacts: vec![
            PackageArtifact {
                sha256: Some("b".repeat(64)),
                ..PackageArtifact::new(
                    "_bundle/src/rt.py",
                    PackageArtifactKind::SourceFile,
                    Some(BackendKind::Python),
                )
            },
            PackageArtifact {
                sha256: Some("a".repeat(64)),
                ..PackageArtifact::new(
                    "_bundle/java/demo.jar",
                    PackageArtifactKind::Jar,
                    Some(BackendKind::Java),
                )
            },
        ],
        lockfile: Some("plugin.lock.json".into()),
        manifest_hash: Some("c".repeat(64)),
        ..PluginPackage::new(
            PluginSchema::new("demo.multi", Some("1.0.0".into()), Vec::new()),
            backends,
        )
    };

    let lock = package.generate_lockfile();

    assert_eq!(lock.plugin_name.as_deref(), Some("demo.multi"));
    assert_eq!(lock.plugin_version.as_deref(), Some("1.0.0"));
    assert_eq!(lock.backends["python.node"].backend, BackendKind::Python);
    assert_eq!(lock.backends["node.node"].backend, BackendKind::Node);
    assert_eq!(
        lock.backends["java.node"].classpath,
        vec!["_bundle/java/demo.jar"]
    );
    assert_eq!(lock.backends["cpp.node"].backend, BackendKind::CCpp);
    assert_eq!(lock.artifacts[0].path, "_bundle/java/demo.jar");
    assert_eq!(lock.artifacts[1].path, "_bundle/src/rt.py");
}

#[test]
fn package_lockfile_round_trips_on_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("plugin.lock.json");
    let lock = PluginLockfile {
        schema_version: SCHEMA_VERSION,
        plugin_name: Some("demo".into()),
        plugin_version: Some("0.1.0".into()),
        manifest_hash: Some("a".repeat(64)),
        backends: BTreeMap::new(),
        artifacts: vec![PackageLockArtifact {
            path: "_bundle/src/rt.py".into(),
            kind: PackageArtifactKind::SourceFile,
            backend: Some(BackendKind::Python),
            platform: None,
            sha256: Some("b".repeat(64)),
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    };

    lock.write(&path).expect("write lockfile");
    let loaded = PluginLockfile::read(&path).expect("read lockfile");

    assert_eq!(loaded, lock);
}

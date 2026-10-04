use super::*;
use daedalus_ffi_core::{
    BackendRuntimeModel, FixtureLanguage, NodeSchema, WirePayloadHandle, generate_language_fixture,
    scalar_add_fixture_spec, validate_language_backends,
};

fn validate_java_schema(
    schema: &PluginSchema,
    backends: &BTreeMap<String, BackendConfig>,
) -> Result<(), FfiContractError> {
    validate_language_backends(schema, backends, BackendKind::Java)
}

fn input() -> JavaPackageInput {
    JavaPackageInput {
        entry_class: "com.example.Nodes".into(),
        entry_method: "add".into(),
        classpath: vec![
            JavaClasspathEntry::jar("build/libs/demo.jar"),
            JavaClasspathEntry::classes_dir("build/classes/java/main"),
        ],
        native_libraries: vec![JavaNativeLibrary {
            path: "native/linux-x86_64/libopencv_java.so".into(),
            platform: Some(PackagePlatform {
                os: Some("linux".into()),
                arch: Some("x86_64".into()),
                abi: Some("gnu".into()),
            }),
        }],
        maven_coordinates: vec!["org.opencv:opencv:4.10.0".into()],
        gradle_projects: vec![":plugin".into()],
        executable: None,
    }
}

#[test]
fn java_backend_config_records_classpath_native_paths_and_metadata() {
    let backend = input().backend_config().expect("backend config");

    assert_eq!(backend.backend, BackendKind::Java);
    assert_eq!(backend.runtime_model, BackendRuntimeModel::PersistentWorker);
    assert_eq!(backend.entry_class.as_deref(), Some("com.example.Nodes"));
    assert_eq!(backend.entry_symbol.as_deref(), Some("add"));
    assert_eq!(
        backend.classpath,
        vec![
            String::from("build/libs/demo.jar"),
            String::from("build/classes/java/main")
        ]
    );
    assert_eq!(
        backend.native_library_paths,
        vec![String::from("native/linux-x86_64/libopencv_java.so")]
    );
    assert!(backend.options.contains_key("maven_coordinates"));
    assert!(backend.options.contains_key("gradle_projects"));
}

#[test]
fn java_package_artifacts_use_deterministic_bundle_paths() {
    let artifacts = input().package_artifacts().expect("artifacts");

    assert_eq!(artifacts[0].path, "_bundle/java/demo.jar");
    assert_eq!(artifacts[0].kind, PackageArtifactKind::Jar);
    assert_eq!(artifacts[1].path, "_bundle/java/main");
    assert_eq!(artifacts[1].kind, PackageArtifactKind::ClassesDir);
    assert_eq!(
        artifacts[2].path,
        "_bundle/native/linux-x86_64-gnu/libopencv_java.so"
    );
    assert_eq!(artifacts[2].kind, PackageArtifactKind::NativeLibrary);
}

#[test]
fn java_worker_launch_uses_classpath_and_library_path_args() {
    let backend = input().backend_config().expect("backend config");
    let launch = java_worker_launch(&backend, "daedalus.worker.Main");

    assert_eq!(launch.executable, "java");
    assert_eq!(launch.args[0], "-cp");
    assert!(launch.args[1].contains("build/libs/demo.jar"));
    assert!(launch.args[1].contains("build/classes/java/main"));
    assert!(
        launch.args[2].starts_with("-Djava.library.path=native/linux-x86_64/libopencv_java.so")
    );
    assert_eq!(launch.args[3], "daedalus.worker.Main");
}

#[test]
fn java_package_input_rejects_missing_classpath() {
    let mut input = input();
    input.classpath.clear();

    assert_eq!(
        input.backend_config().expect_err("missing classpath"),
        JavaPackageError::MissingClasspath
    );
}

#[test]
fn validates_java_schema_and_backends() {
    let node = NodeSchema::new("demo:add", BackendKind::Java, "add", vec![], vec![]);
    let schema = PluginSchema::new("demo.java", None, vec![node]);
    let backends = BTreeMap::from([(
        "demo:add".into(),
        JavaPackageInput {
            entry_class: "demo.Nodes".into(),
            entry_method: "add".into(),
            classpath: vec![JavaClasspathEntry::jar("demo.jar")],
            ..Default::default()
        }
        .backend_config()
        .expect("backend config"),
    )]);

    validate_java_schema(&schema, &backends).expect("valid java schema");
}

#[test]
fn java_diagnostics_classify_common_runtime_failures() {
    assert_eq!(
        classify_java_runtime_diagnostic(
            "java.lang.ClassNotFoundException: demo.Missing",
            "fallback"
        )
        .kind,
        JavaRuntimeDiagnosticKind::ClassNotFound
    );
    assert_eq!(
        classify_java_runtime_diagnostic(
            "java.lang.NoSuchMethodException: demo.Nodes.add()",
            "fallback"
        )
        .kind,
        JavaRuntimeDiagnosticKind::MethodNotFound
    );
    assert_eq!(
        classify_java_runtime_diagnostic(
            "java.lang.UnsatisfiedLinkError: no opencv_java in java.library.path",
            "fallback"
        )
        .kind,
        JavaRuntimeDiagnosticKind::NativeLibraryLoad
    );
    assert_eq!(
        classify_java_runtime_diagnostic("java.lang.reflect.InvocationTargetException", "fallback")
            .kind,
        JavaRuntimeDiagnosticKind::Invocation
    );
    assert_eq!(
        classify_java_runtime_diagnostic("", "process failed"),
        JavaRuntimeDiagnostic {
            kind: JavaRuntimeDiagnosticKind::Process,
            message: "process failed".into(),
        }
    );
}

#[test]
fn sdk_builders_match_rust_baseline_schema_surface() {
    let spec = scalar_add_fixture_spec();
    let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
    let java_fixture =
        generate_language_fixture(&spec, FixtureLanguage::Java).expect("java fixture");
    let baseline = &rust.schema.nodes[0];

    let node = NodeSchema::new(
        baseline.id.clone(),
        BackendKind::Java,
        java_fixture.schema.nodes[0].entrypoint.clone(),
        baseline.inputs.clone(),
        baseline.outputs.clone(),
    );
    let schema = PluginSchema::for_backend(
        "ffi.conformance.java.scalar_add",
        Some("1.0.0".into()),
        vec![node],
        BackendKind::Java,
    )
    .expect("schema");
    let input = JavaPackageInput {
        entry_class: "ffi.conformance.ScalarAdd".into(),
        entry_method: "add".into(),
        classpath: vec![JavaClasspathEntry::classes_dir("classes")],
        ..Default::default()
    };
    let backend = input.backend_config().expect("backend config");
    let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
    let package = java_plugin_package(schema.clone(), backends.clone(), &input).expect("package");

    assert_eq!(schema.nodes[0].id, baseline.id);
    assert_eq!(schema.nodes[0].inputs, baseline.inputs);
    assert_eq!(schema.nodes[0].outputs, baseline.outputs);
    assert_eq!(schema.nodes[0].stateful, baseline.stateful);
    assert_eq!(backend, java_fixture.backends[&baseline.id]);
    assert_eq!(package.schema.as_ref(), Some(&schema));
    assert_eq!(package.backends, backends);
    assert_eq!(package.artifacts[0].path, "_bundle/java/classes");
    assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
    assert!(package.manifest_hash.is_some());
    validate_java_schema(&schema, &package.backends).expect("valid package schema");
}

#[test]
fn java_transport_options_enable_direct_byte_buffer_and_mmap() {
    let backend = java_backend_config_with_transport(
        &input(),
        JavaPayloadTransport::direct_byte_buffer_and_mmap(),
    )
    .expect("backend config");
    assert_eq!(
        backend.options.get("payload_transport"),
        Some(&serde_json::json!({"direct_byte_buffer": true, "mmap": true}))
    );
}

#[test]
fn java_resolves_payload_handles_to_mmap_or_direct_byte_buffer_views() {
    let mmap_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
        "id": "lease-1",
        "type_key": "bytes",
        "access": "read",
        "metadata": {
            "mmap_path": "/tmp/daedalus-payload",
            "mmap_offset": 12,
            "mmap_len": 96,
            "bytes_estimate": 96
        }
    }))
    .expect("handle");
    let resolved = resolve_java_payload_handle(
        &mmap_handle,
        &JavaPayloadTransport::direct_byte_buffer_and_mmap(),
    )
    .expect("resolve");
    assert_eq!(
        resolved.view,
        JavaPayloadView::Mmap {
            path: "/tmp/daedalus-payload".into(),
            offset: 12,
            len: 96
        }
    );

    let direct_handle: WirePayloadHandle = serde_json::from_value(serde_json::json!({
        "id": "lease-2",
        "type_key": "bytes",
        "access": "view",
        "metadata": {"bytes_estimate": 48}
    }))
    .expect("handle");
    let resolved = resolve_java_payload_handle(
        &direct_handle,
        &JavaPayloadTransport {
            direct_byte_buffer: true,
            mmap: false,
        },
    )
    .expect("resolve");
    assert_eq!(
        resolved.view,
        JavaPayloadView::DirectByteBuffer { bytes_estimate: 48 }
    );
    assert_eq!(resolved.access, "view");
}

#[test]
fn complete_java_package_emits_lockfile_hash_and_language_metadata() {
    let spec = scalar_add_fixture_spec();
    let fixture = generate_language_fixture(&spec, FixtureLanguage::Java).expect("java fixture");
    let input = JavaPackageInput {
        entry_class: "ffi.conformance.ScalarAdd".into(),
        entry_method: "add".into(),
        classpath: vec![
            JavaClasspathEntry::classes_dir("build/classes/java/main"),
            JavaClasspathEntry::jar("build/libs/ffi-showcase.jar"),
        ],
        native_libraries: vec![JavaNativeLibrary {
            path: "build/native/libffi_showcase_jni.so".into(),
            platform: None,
        }],
        ..Default::default()
    };
    let package = java_plugin_package(fixture.schema.clone(), fixture.backends.clone(), &input)
        .expect("complete package");
    let lock = package.generate_lockfile();

    assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
    assert!(package.manifest_hash.is_some());
    assert_eq!(
        package.metadata.get("package_builder"),
        Some(&serde_json::json!("daedalus-ffi-java"))
    );
    assert_eq!(package.artifacts.len(), 3);
    assert_eq!(
        lock.plugin_name.as_deref(),
        Some("ffi.conformance.java.scalar_add")
    );
    assert_eq!(lock.artifacts.len(), 3);
}

/// Compiles the Java SDK with `PackageBuilderTest` (descriptor shape and width-exact port types)
/// and runs it; skipped when no JDK is installed.
#[test]
fn java_sdk_package_builder_test_passes() {
    use std::process::Command;
    let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".into());
    let java = std::env::var("JAVA").unwrap_or_else(|_| "java".into());
    if Command::new(&javac).arg("--version").output().is_err() {
        return;
    }
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/src");
    let mut sources = Vec::new();
    for set in ["main", "test"] {
        for entry in std::fs::read_dir(sdk.join(set).join("java/dev/daedalus/plugin")).unwrap() {
            sources.push(entry.unwrap().path());
        }
    }
    let classes = std::env::temp_dir().join(format!("daedalus-java-sdk-{}", std::process::id()));
    let compiled = Command::new(&javac)
        .arg("-d")
        .arg(&classes)
        .args(&sources)
        .status()
        .expect("spawn javac");
    assert!(compiled.success(), "javac failed for the Java SDK");
    let ran = Command::new(&java)
        .args(["-ea", "-cp"])
        .arg(&classes)
        .arg("dev.daedalus.plugin.PackageBuilderTest")
        .status()
        .expect("spawn java");
    let _ = std::fs::remove_dir_all(&classes);
    assert!(ran.success(), "Java SDK PackageBuilderTest failed");
}

use super::*;

#[test]
fn persistent_python_worker_resolves_payload_handle_to_memoryview_mmap() {
    let Some(python) = python_available() else {
        return;
    };
    let dir = temp_dir("persistent_python_payload_worker");
    let backing = dir.join("payload.bin");
    std::fs::write(&backing, [9_u8, 2, 3, 4]).expect("write backing payload");
    let worker = write_python_payload_worker(&dir);
    let config = BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(worker.display().to_string()),
        entry_class: None,
        entry_symbol: Some("payload_len".into()),
        executable: Some(python),
        args: vec![worker.display().to_string()],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

    runner.start().expect("start");
    let hello = runner.hello().expect("hello");
    assert!(hello.capabilities.contains(&"memoryview".into()));
    assert!(hello.capabilities.contains(&"mmap".into()));
    let response = runner
        .invoke(payload_request(&backing, 4))
        .expect("payload invoke");

    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(9)));
    std::fs::remove_file(&backing).expect("remove backing payload");
    assert!(matches!(
        runner.invoke(payload_request(&backing, 4)),
        Err(RunnerPoolError::Runner(message))
            if message.contains("payload_lease_expired")
    ));
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_python_worker_exercises_payload_ownership_modes() {
    let Some(python) = python_available() else {
        return;
    };
    let dir = temp_dir("persistent_python_payload_modes");
    let worker = write_python_payload_worker(&dir);
    let config = BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(worker.display().to_string()),
        entry_class: None,
        entry_symbol: Some("payload_modes".into()),
        executable: Some(python),
        args: vec![worker.display().to_string()],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend(&config)
        .expect("runner")
        .with_ffi_telemetry(telemetry.clone());

    runner.start().expect("start");
    assert_payload_ownership_modes(&runner, &dir);
    assert_payload_ownership_telemetry(&telemetry);
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_node_worker_resolves_payload_handle_to_buffer() {
    let Some(node) = node_available() else {
        return;
    };
    let dir = temp_dir("persistent_node_payload_worker");
    let backing = dir.join("payload.bin");
    std::fs::write(&backing, [11_u8, 2, 3, 4]).expect("write backing payload");
    let worker = write_node_payload_worker(&dir);
    let config = BackendConfig {
        backend: BackendKind::Node,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(worker.display().to_string()),
        entry_class: None,
        entry_symbol: Some("payload_len".into()),
        executable: Some(node),
        args: vec![worker.display().to_string()],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

    runner.start().expect("start");
    let hello = runner.hello().expect("hello");
    assert!(hello.capabilities.contains(&"buffer".into()));
    let response = runner
        .invoke(payload_request(&backing, 4))
        .expect("payload invoke");

    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(11)));
    std::fs::remove_file(&backing).expect("remove backing payload");
    assert!(matches!(
        runner.invoke(payload_request(&backing, 4)),
        Err(RunnerPoolError::Runner(message))
            if message.contains("payload_lease_expired")
    ));
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_node_worker_exercises_payload_ownership_modes() {
    let Some(node) = node_available() else {
        return;
    };
    let dir = temp_dir("persistent_node_payload_modes");
    let worker = write_node_payload_worker(&dir);
    let config = BackendConfig {
        backend: BackendKind::Node,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(worker.display().to_string()),
        entry_class: None,
        entry_symbol: Some("payload_modes".into()),
        executable: Some(node),
        args: vec![worker.display().to_string()],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend(&config)
        .expect("runner")
        .with_ffi_telemetry(telemetry.clone());

    runner.start().expect("start");
    assert_payload_ownership_modes(&runner, &dir);
    assert_payload_ownership_telemetry(&telemetry);
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_java_worker_resolves_payload_handle_to_direct_byte_buffer_mmap() {
    let Some((javac, java)) = java_available() else {
        return;
    };
    let dir = temp_dir("persistent_java_payload_worker");
    let backing = dir.join("payload.bin");
    std::fs::write(&backing, [13_u8, 2, 3, 4]).expect("write backing payload");
    let classes = write_java_payload_worker(&dir, &javac);
    let config = BackendConfig {
        backend: BackendKind::Java,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: None,
        entry_class: Some("PayloadWorker".into()),
        entry_symbol: Some("payload_len".into()),
        executable: Some(java),
        args: vec![
            "-cp".into(),
            classes.display().to_string(),
            "PayloadWorker".into(),
        ],
        classpath: vec![classes.display().to_string()],
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

    runner.start().expect("start");
    let hello = runner.hello().expect("hello");
    assert!(hello.capabilities.contains(&"direct_byte_buffer".into()));
    assert!(hello.capabilities.contains(&"mmap".into()));
    let response = runner
        .invoke(payload_request(&backing, 4))
        .expect("payload invoke");

    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(13)));
    std::fs::remove_file(&backing).expect("remove backing payload");
    assert!(matches!(
        runner.invoke(payload_request(&backing, 4)),
        Err(RunnerPoolError::Runner(message))
            if message.contains("payload_lease_expired")
    ));
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_java_worker_exercises_payload_ownership_modes() {
    let Some((javac, java)) = java_available() else {
        return;
    };
    let dir = temp_dir("persistent_java_payload_modes");
    let classes = write_java_payload_worker(&dir, &javac);
    let config = BackendConfig {
        backend: BackendKind::Java,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: None,
        entry_class: Some("PayloadWorker".into()),
        entry_symbol: Some("payload_modes".into()),
        executable: Some(java),
        args: vec![
            "-cp".into(),
            classes.display().to_string(),
            "PayloadWorker".into(),
        ],
        classpath: vec![classes.display().to_string()],
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend(&config)
        .expect("runner")
        .with_ffi_telemetry(telemetry.clone());

    runner.start().expect("start");
    assert_payload_ownership_modes(&runner, &dir);
    assert_payload_ownership_telemetry(&telemetry);
    runner.shutdown().expect("shutdown");
}

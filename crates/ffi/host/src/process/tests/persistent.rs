use super::*;

#[test]
fn persistent_python_worker_loads_module_once_and_invokes_repeatedly() {
    let Some(python) = python_available() else {
        return;
    };
    let dir = temp_dir("persistent_python_worker");
    let worker = write_python_worker(&dir);
    let module = dir.join("demo_module.py");
    let config = BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(module.display().to_string()),
        entry_class: None,
        entry_symbol: Some("add".into()),
        executable: Some(python),
        args: vec![worker.display().to_string(), module.display().to_string()],
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
    let hello = runner.hello().expect("hello");
    assert_eq!(hello.worker_id.as_deref(), Some("python-test-worker"));
    assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

    let first = runner.invoke(request()).expect("first invoke");
    let second = runner.invoke(request()).expect("second invoke");

    assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(first.state, Some(WireValue::Int(1)));
    assert_eq!(second.state, Some(WireValue::Int(2)));
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].level, InvokeEventLevel::Info);
    let report = telemetry.snapshot();
    let worker = report.workers.values().next().expect("worker telemetry");
    assert_eq!(worker.handshakes, 1);
    assert!(worker.request_bytes > 0);
    assert!(worker.response_bytes > 0);
    assert!(worker.raw_io_events >= 2);
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_node_worker_imports_module_once_and_invokes_repeatedly() {
    let Some(node) = node_available() else {
        return;
    };
    let dir = temp_dir("persistent_node_worker");
    let worker = write_node_worker(&dir);
    let module = dir.join("demo_module.mjs");
    let config = BackendConfig {
        backend: BackendKind::Node,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(module.display().to_string()),
        entry_class: None,
        entry_symbol: Some("add".into()),
        executable: Some(node),
        args: vec![worker.display().to_string(), module.display().to_string()],
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
    let hello = runner.hello().expect("hello");
    assert_eq!(hello.worker_id.as_deref(), Some("node-test-worker"));
    assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

    let first = runner.invoke(request()).expect("first invoke");
    let second = runner.invoke(request()).expect("second invoke");

    assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(first.state, Some(WireValue::Int(1)));
    assert_eq!(second.state, Some(WireValue::Int(2)));
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].level, InvokeEventLevel::Info);
    let report = telemetry.snapshot();
    let worker = report.workers.values().next().expect("worker telemetry");
    assert_eq!(worker.handshakes, 1);
    assert!(worker.request_bytes > 0);
    assert!(worker.response_bytes > 0);
    assert!(worker.raw_io_events >= 2);
    runner.shutdown().expect("shutdown");
}

#[test]
fn persistent_java_worker_loads_classpath_once_and_invokes_repeatedly() {
    let Some((javac, java)) = java_available() else {
        return;
    };
    let dir = temp_dir("persistent_java_worker");
    let classes = write_java_worker(&dir, &javac);
    let config = BackendConfig {
        backend: BackendKind::Java,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: None,
        entry_class: Some("DemoModule".into()),
        entry_symbol: Some("add".into()),
        executable: Some(java),
        args: vec![
            "-cp".into(),
            classes.display().to_string(),
            "Worker".into(),
            "DemoModule".into(),
            "add".into(),
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
    let hello = runner.hello().expect("hello");
    assert_eq!(hello.worker_id.as_deref(), Some("java-test-worker"));
    assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

    let first = runner.invoke(request()).expect("first invoke");
    let second = runner.invoke(request()).expect("second invoke");

    assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
    assert_eq!(first.state, Some(WireValue::Int(1)));
    assert_eq!(second.state, Some(WireValue::Int(2)));
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].level, InvokeEventLevel::Info);
    let report = telemetry.snapshot();
    let worker = report.workers.values().next().expect("worker telemetry");
    assert_eq!(worker.handshakes, 1);
    assert!(worker.request_bytes > 0);
    assert!(worker.response_bytes > 0);
    assert!(worker.raw_io_events >= 2);
    runner.shutdown().expect("shutdown");
}

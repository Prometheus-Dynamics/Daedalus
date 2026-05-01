use super::*;

#[test]
fn persistent_worker_restarts_exited_python_node_and_java_backends() {
    for (backend, worker_id) in [
        (BackendKind::Python, "python-restart-worker"),
        (BackendKind::Node, "node-restart-worker"),
        (BackendKind::Java, "java-restart-worker"),
    ] {
        let dir = temp_dir(worker_id);
        let worker = write_one_shot_worker(&dir, &backend, worker_id);
        let config = one_shot_worker_config(backend.clone(), &worker, &dir);
        let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

        runner.start().expect("first start");
        let first_hello = runner.hello().expect("first hello");
        assert_eq!(first_hello.worker_id.as_deref(), Some(worker_id));
        assert_eq!(first_hello.backend, Some(backend.clone()));
        let first = runner.invoke(request()).expect("first invoke");
        assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(first.events.len(), 1);

        wait_for_stopped(&runner);
        runner.start().expect("restart");
        let second_hello = runner.hello().expect("second hello");
        assert_eq!(second_hello.worker_id.as_deref(), Some(worker_id));
        assert_eq!(second_hello.backend, Some(backend));
        let second = runner.invoke(request()).expect("second invoke");
        assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(second.events.len(), 1);

        runner.shutdown().expect("shutdown");
    }
}

#[test]
fn persistent_worker_reports_crash_and_malformed_messages() {
    let crash_telemetry = FfiHostTelemetry::new();
    let crash = PersistentWorkerRunner::from_backend(&BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some("crash".into()),
        entry_class: None,
        entry_symbol: Some("run".into()),
        executable: Some("/bin/sh".into()),
        args: vec![
            "-c".into(),
            "printf 'worker failed with a long diagnostic' >&2; exit 9".into(),
        ],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    })
    .expect("crash runner")
    .with_ffi_telemetry(crash_telemetry.clone());
    assert!(matches!(
        crash.start(),
        Err(RunnerPoolError::Runner(message))
            if message.contains("stdout closed")
                && message.contains("worker failed with a long diagnostic")
    ));
    let crash_report = crash_telemetry.snapshot();
    let crash_worker = crash_report
        .workers
        .values()
        .next()
        .expect("crash worker telemetry");
    assert_eq!(crash_worker.stderr_events, 1);

    let malformed_telemetry = FfiHostTelemetry::new();
    let malformed = PersistentWorkerRunner::from_backend(&BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some("bad".into()),
        entry_class: None,
        entry_symbol: Some("run".into()),
        executable: Some("/bin/sh".into()),
        args: vec![
            "-c".into(),
            "printf 'not-json\\n'; while read line; do :; done".into(),
        ],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    })
    .expect("malformed runner")
    .with_ffi_telemetry(malformed_telemetry.clone());
    assert!(matches!(
        malformed.start(),
        Err(RunnerPoolError::Runner(message))
            if message.contains("failed to decode worker message")
    ));
    let malformed_report = malformed_telemetry.snapshot();
    let malformed_worker = malformed_report
        .workers
        .values()
        .next()
        .expect("malformed worker telemetry");
    assert_eq!(malformed_worker.malformed_responses, 1);
}

#[test]
fn persistent_worker_rejects_unsupported_runner_limits() {
    let config = BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some("limits".into()),
        entry_class: None,
        entry_symbol: Some("run".into()),
        executable: Some("/bin/sh".into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    };

    let queue_depth = PersistentWorkerRunner::from_backend_with_limits(
        &config,
        &RunnerLimits {
            queue_depth: 2,
            ..RunnerLimits::default()
        },
    );
    assert!(matches!(
        queue_depth,
        Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "queue_depth",
            ..
        })
    ));

    let restart_policy = PersistentWorkerRunner::from_backend_with_limits(
        &config,
        &RunnerLimits {
            restart_policy: RunnerRestartPolicy::OnFailure,
            ..RunnerLimits::default()
        },
    );
    assert!(matches!(
        restart_policy,
        Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "restart_policy",
            ..
        })
    ));
}

#[test]
fn persistent_worker_honors_request_timeout() {
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend_with_limits_and_telemetry(
        &BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("timeout".into()),
            entry_class: None,
            entry_symbol: Some("run".into()),
            executable: Some("/bin/sh".into()),
            args: vec![
                "-c".into(),
                r#"printf '%s\n' '{"protocol_version":1,"correlation_id":"startup","payload":{"type":"hello","payload":{"protocol_version":1,"min_protocol_version":1,"worker_id":"timeout-worker","backend":"python","supported_nodes":["demo:add"],"capabilities":["persistent_worker"]}}}'; while read line; do :; done"#.into(),
            ],
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        },
        &RunnerLimits {
            request_timeout: Some(Duration::from_millis(20)),
            ..RunnerLimits::default()
        },
        telemetry.clone(),
    )
    .expect("timeout runner");

    runner.start().expect("start timeout worker");
    assert!(matches!(
        runner.invoke(request()),
        Err(RunnerPoolError::RequestTimedOut { timeout }) if timeout == Duration::from_millis(20)
    ));
    assert_eq!(runner.health(), RunnerHealth::Starting);

    let report = telemetry.snapshot();
    let worker = report.workers.values().next().expect("worker telemetry");
    assert_eq!(worker.timeout_failures, 1);
}

#[test]
fn persistent_worker_drains_stderr_while_waiting_for_stdout() {
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend_with_limits_and_telemetry(
            &BackendConfig {
                backend: BackendKind::Python,
                runtime_model: BackendRuntimeModel::PersistentWorker,
                entry_module: Some("stderr-heavy".into()),
                entry_class: None,
                entry_symbol: Some("run".into()),
                executable: Some("/bin/sh".into()),
                args: vec![
                    "-c".into(),
                    r#"head -c 200000 /dev/zero | tr '\000' x >&2
printf '%s\n' '{"protocol_version":1,"correlation_id":"startup","payload":{"type":"hello","payload":{"protocol_version":1,"min_protocol_version":1,"worker_id":"stderr-heavy-worker","backend":"python","supported_nodes":["demo:add"],"capabilities":["persistent_worker"]}}}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"ack"'*) continue ;;
  esac
  printf '%s\n' '{"protocol_version":1,"correlation_id":"req-1","payload":{"type":"response","payload":{"protocol_version":1,"correlation_id":"req-1","outputs":{"out":{"kind":"int","value":42}},"events":[]}}}'
  exit 0
done
"#
                    .into(),
                ],
                classpath: Vec::new(),
                native_library_paths: Vec::new(),
                working_dir: None,
                env: BTreeMap::new(),
                options: BTreeMap::new(),
            },
            &RunnerLimits {
                stderr_capture_bytes: 64,
                ..RunnerLimits::default()
            },
            telemetry.clone(),
        )
        .expect("stderr-heavy runner");

    runner.start().expect("start drains stderr before hello");
    let response = runner.invoke(request()).expect("invoke after stderr flood");
    assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(42)));
    runner.shutdown().expect("shutdown");

    let report = telemetry.snapshot();
    let worker = report.workers.values().next().expect("worker telemetry");
    assert_eq!(worker.handshakes, 1);
}

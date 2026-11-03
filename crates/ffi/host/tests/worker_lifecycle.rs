//! Persistent worker lifecycle against fake workers: restart after exit, a crash with a
//! diagnostic, a malformed message, a request timeout, and a stderr flood before the handshake.
//!
//! The fake worker is this test binary itself (`harness = false`): started with
//! `--worker <mode> ...` it speaks the worker protocol on stdin/stdout and nothing else, so the
//! tests run the same on every OS without a shell.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::time::Duration;

use daedalus_ffi_host::core::{
    BackendConfig, BackendKind, InvokeRequest, WORKER_PROTOCOL_VERSION, WireValue,
};
use daedalus_ffi_host::{
    BackendRunner, FfiHostTelemetry, PersistentWorkerRunner, RunnerHealth, RunnerLimits,
    RunnerPoolError,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--worker") {
        worker(&args[2..]);
        return;
    }
    let tests: [(&str, fn()); 5] = [
        ("restarts_exited_python_node_and_java_backends", restarts),
        ("reports_a_crash_with_its_diagnostic", crash),
        ("reports_a_malformed_message", malformed),
        ("honors_the_request_timeout", timeout),
        ("drains_stderr_while_waiting_for_stdout", stderr_flood),
    ];
    let filter = args.iter().skip(1).find(|a| !a.starts_with('-'));
    for (name, test) in tests {
        if filter.is_some_and(|f| !name.contains(f.as_str())) {
            continue;
        }
        test();
        println!("test worker_lifecycle::{name} ... ok");
    }
}

// ---- The fake worker ----

fn hello(worker_id: &str, backend: &str, capabilities: &str) -> String {
    format!(
        r#"{{"protocol_version":1,"correlation_id":"startup","payload":{{"type":"hello","payload":{{"protocol_version":1,"min_protocol_version":1,"worker_id":"{worker_id}","backend":"{backend}","supported_nodes":["demo:add"],"capabilities":[{capabilities}]}}}}}}"#
    )
}

const RESPONSE: &str = r#"{"protocol_version":1,"correlation_id":"req-1","payload":{"type":"response","payload":{"protocol_version":1,"correlation_id":"req-1","outputs":{"out":{"kind":"int","value":42}},"events":[]}}}"#;

fn event(worker_id: &str) -> String {
    format!(
        r#"{{"protocol_version":1,"correlation_id":"req-1","payload":{{"type":"event","payload":{{"level":"info","message":"restart fixture invoked","metadata":{{"worker_id":"{worker_id}"}}}}}}}}"#
    )
}

/// The first line on stdin that is not an ack (a request), or `None` at end of input.
fn next_request() -> Option<String> {
    std::io::stdin()
        .lock()
        .lines()
        .map_while(Result::ok)
        .find(|line| !line.contains(r#""type":"ack""#))
}

fn drain_stdin() {
    for _ in std::io::stdin().lock().lines().map_while(Result::ok) {}
}

fn worker(args: &[String]) {
    let mut out = std::io::stdout().lock();
    match args[0].as_str() {
        // Answers one request (an event, then the response) and exits.
        "one-shot" => {
            let (id, backend) = (&args[1], &args[2]);
            let caps = r#""persistent_worker","restart""#;
            writeln!(out, "{}", hello(id, backend, caps)).unwrap();
            out.flush().unwrap();
            if next_request().is_some() {
                writeln!(out, "{}\n{RESPONSE}", event(id)).unwrap();
            }
        }
        "crash" => {
            eprint!("worker failed with a long diagnostic");
            std::process::exit(9);
        }
        "malformed" => {
            writeln!(out, "not-json").unwrap();
            out.flush().unwrap();
            drain_stdin();
        }
        // Says hello, then never answers.
        "silent" => {
            let caps = r#""persistent_worker""#;
            writeln!(out, "{}", hello("timeout-worker", "python", caps)).unwrap();
            out.flush().unwrap();
            drain_stdin();
        }
        // 200 000 bytes of stderr before the handshake, then one answer.
        "stderr-flood" => {
            std::io::stderr().write_all(&[b'x'; 200_000]).unwrap();
            let caps = r#""persistent_worker""#;
            writeln!(out, "{}", hello("stderr-heavy-worker", "python", caps)).unwrap();
            out.flush().unwrap();
            if next_request().is_some() {
                writeln!(out, "{RESPONSE}").unwrap();
            }
        }
        mode => panic!("unknown worker mode {mode}"),
    }
    out.flush().unwrap();
}

// ---- The tests ----

fn request() -> InvokeRequest {
    InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: "demo:add".into(),
        correlation_id: Some("req-1".into()),
        args: BTreeMap::from([
            ("a".into(), WireValue::Int(2)),
            ("b".into(), WireValue::Int(40)),
        ]),
        state: None,
        context: BTreeMap::new(),
    }
}

/// A persistent worker running this binary in `mode`.
fn fake(backend: BackendKind, mode: &[&str], entry: &str) -> BackendConfig {
    let exe = std::env::current_exe().expect("test binary path");
    let mut args = vec!["--worker".to_string()];
    args.extend(mode.iter().map(|a| a.to_string()));
    let config =
        BackendConfig::persistent_worker(backend.clone(), exe.display().to_string(), "run")
            .with_args(args);
    match backend {
        BackendKind::Java => config.with_entry_class(entry),
        _ => config.with_entry_module(entry),
    }
}

fn wait_for_stopped(runner: &PersistentWorkerRunner) {
    for _ in 0..200 {
        if runner.health() == RunnerHealth::Stopped {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(runner.health(), RunnerHealth::Stopped);
}

fn restarts() {
    for (backend, name, worker_id) in [
        (BackendKind::Python, "python", "python-restart-worker"),
        (BackendKind::Node, "node", "node-restart-worker"),
        (BackendKind::Java, "java", "java-restart-worker"),
    ] {
        let config = fake(
            backend.clone(),
            &["one-shot", worker_id, name],
            "RestartFixture",
        );
        let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");
        for round in ["first", "restart"] {
            runner.start().expect(round);
            let hello = runner.hello().expect("hello");
            assert_eq!(hello.worker_id.as_deref(), Some(worker_id));
            assert_eq!(hello.backend, Some(backend.clone()));
            let response = runner.invoke(request()).expect("invoke");
            assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(42)));
            assert_eq!(response.events.len(), 1);
            wait_for_stopped(&runner);
        }
        runner.shutdown().expect("shutdown");
    }
}

fn crash() {
    let telemetry = FfiHostTelemetry::new();
    let runner =
        PersistentWorkerRunner::from_backend(&fake(BackendKind::Python, &["crash"], "crash"))
            .expect("crash runner")
            .with_ffi_telemetry(telemetry.clone());
    assert!(matches!(
        runner.start(),
        Err(RunnerPoolError::Runner(message))
            if message.contains("stdout closed")
                && message.contains("worker failed with a long diagnostic")
    ));
    let report = telemetry.snapshot();
    let worker = report
        .workers
        .values()
        .next()
        .expect("crash worker telemetry");
    assert_eq!(worker.stderr_events, 1);
}

fn malformed() {
    let telemetry = FfiHostTelemetry::new();
    let runner =
        PersistentWorkerRunner::from_backend(&fake(BackendKind::Python, &["malformed"], "bad"))
            .expect("malformed runner")
            .with_ffi_telemetry(telemetry.clone());
    assert!(matches!(
        runner.start(),
        Err(RunnerPoolError::Runner(message)) if message.contains("failed to decode worker message")
    ));
    let report = telemetry.snapshot();
    let worker = report
        .workers
        .values()
        .next()
        .expect("malformed worker telemetry");
    assert_eq!(worker.malformed_responses, 1);
}

fn timeout() {
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend_with_limits_and_telemetry(
        &fake(BackendKind::Python, &["silent"], "timeout"),
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

fn stderr_flood() {
    let telemetry = FfiHostTelemetry::new();
    let runner = PersistentWorkerRunner::from_backend_with_limits_and_telemetry(
        &fake(BackendKind::Python, &["stderr-flood"], "stderr-heavy"),
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

use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_ffi_core::{
    BackendKind, BackendRuntimeModel, InvokeRequest, InvokeResponse, WireValue,
};
use daedalus_transport::TypeKey;

use super::*;

struct FakeRunner {
    invokes: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
    state: Arc<Mutex<Option<WireValue>>>,
    health: RunnerHealth,
    fail: bool,
}

impl FakeRunner {
    fn ready(invokes: Arc<AtomicUsize>) -> Self {
        Self {
            invokes,
            shutdowns: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(None)),
            health: RunnerHealth::Ready,
            fail: false,
        }
    }

    fn stopped(invokes: Arc<AtomicUsize>) -> Self {
        Self {
            invokes,
            shutdowns: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(None)),
            health: RunnerHealth::Stopped,
            fail: false,
        }
    }

    fn ready_with_shutdowns(invokes: Arc<AtomicUsize>, shutdowns: Arc<AtomicUsize>) -> Self {
        Self {
            invokes,
            shutdowns,
            state: Arc::new(Mutex::new(None)),
            health: RunnerHealth::Ready,
            fail: false,
        }
    }

    fn failing(invokes: Arc<AtomicUsize>) -> Self {
        Self {
            invokes,
            shutdowns: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(None)),
            health: RunnerHealth::Ready,
            fail: true,
        }
    }

    fn ready_with_state(invokes: Arc<AtomicUsize>, state: Arc<Mutex<Option<WireValue>>>) -> Self {
        Self {
            invokes,
            shutdowns: Arc::new(AtomicUsize::new(0)),
            state,
            health: RunnerHealth::Ready,
            fail: false,
        }
    }
}

impl BackendRunner for FakeRunner {
    fn health(&self) -> RunnerHealth {
        self.health
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        self.invokes.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(RunnerPoolError::Runner("fake failure".into()));
        }
        Ok(InvokeResponse {
            protocol_version: request.protocol_version,
            correlation_id: request.correlation_id,
            outputs: BTreeMap::from([("out".into(), WireValue::Int(42))]),
            state: None,
            events: Vec::new(),
        })
    }

    fn shutdown(&self) -> Result<(), RunnerPoolError> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn export_state(&self, _node_id: &str) -> Result<Option<WireValue>, RunnerPoolError> {
        self.state
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)
            .map(|state| state.clone())
    }

    fn import_state(&self, _node_id: &str, state: WireValue) -> Result<(), RunnerPoolError> {
        *self
            .state
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)? = Some(state);
        Ok(())
    }
}

fn backend_config(module: &str) -> BackendConfig {
    BackendConfig {
        backend: BackendKind::Python,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module: Some(module.into()),
        entry_class: None,
        entry_symbol: Some("add".into()),
        executable: Some("python".into()),
        args: Vec::new(),
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

fn request() -> InvokeRequest {
    InvokeRequest {
        protocol_version: daedalus_ffi_core::WORKER_PROTOCOL_VERSION,
        node_id: "demo:add".into(),
        correlation_id: Some("req-1".into()),
        args: BTreeMap::new(),
        state: None,
        context: BTreeMap::new(),
    }
}

fn request_with_correlation(id: &str) -> InvokeRequest {
    InvokeRequest {
        correlation_id: Some(id.into()),
        ..request()
    }
}

#[test]
fn runner_key_is_stable_for_equivalent_backend_config() {
    let left = RunnerKey::from_backend(&backend_config("plugin.py")).expect("left key");
    let right = RunnerKey::from_backend(&backend_config("plugin.py")).expect("right key");
    let different = RunnerKey::from_backend(&backend_config("other.py")).expect("different key");

    assert_eq!(left, right);
    assert_ne!(left, different);
    assert!(left.as_str().contains("plugin.py"));
}

#[test]
fn runner_pool_invokes_registered_runner() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready(invokes.clone()))
        .expect("insert runner");

    let response = pool.invoke(&config, request()).expect("invoke runner");

    assert_eq!(response.correlation_id.as_deref(), Some("req-1"));
    assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(42)));
    assert_eq!(invokes.load(Ordering::SeqCst), 1);
}

#[test]
fn runner_pool_reports_missing_and_not_ready_runners() {
    let config = backend_config("plugin.py");
    let missing = RunnerPool::new();
    assert!(matches!(
        missing.invoke(&config, request()),
        Err(RunnerPoolError::MissingRunner)
    ));

    let invokes = Arc::new(AtomicUsize::new(0));
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::stopped(invokes.clone()))
        .expect("insert runner");

    assert!(matches!(
        pool.invoke(&config, request()),
        Err(RunnerPoolError::RunnerNotReady(RunnerHealth::Stopped))
    ));
    assert_eq!(invokes.load(Ordering::SeqCst), 0);
}

#[test]
fn runner_pool_shutdown_removes_registered_runners() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready(invokes))
        .expect("insert runner");

    assert_eq!(pool.len(), 1);
    pool.shutdown_all().expect("shutdown runners");
    assert!(pool.is_empty());
}

#[test]
fn runner_pool_drop_shuts_down_remaining_runners() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");

    {
        let mut pool = RunnerPool::new();
        pool.insert(
            &config,
            FakeRunner::ready_with_shutdowns(invokes, shutdowns.clone()),
        )
        .expect("insert runner");
    }

    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn runner_pool_reports_health_and_shutdown_for_one_runner() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(
        &config,
        FakeRunner::ready_with_shutdowns(invokes, shutdowns.clone()),
    )
    .expect("insert runner");

    assert_eq!(
        pool.health(&config).expect("runner health"),
        RunnerHealth::Ready
    );
    pool.shutdown(&config).expect("shutdown runner");

    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    assert!(pool.is_empty());
}

#[test]
fn runner_pool_prunes_idle_runners() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::with_options(RunnerPoolOptions {
        idle_timeout: Some(Duration::ZERO),
        limits: RunnerLimits::default(),
    });
    pool.insert(
        &config,
        FakeRunner::ready_with_shutdowns(invokes, shutdowns.clone()),
    )
    .expect("insert runner");

    assert_eq!(pool.prune_idle().expect("prune idle"), 1);
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    assert!(pool.is_empty());
}

#[test]
fn runner_pool_options_carry_limits_for_future_process_runners() {
    let options = RunnerPoolOptions {
        idle_timeout: Some(Duration::from_secs(30)),
        limits: RunnerLimits {
            queue_depth: 8,
            request_timeout: Some(Duration::from_secs(2)),
            stderr_capture_bytes: 4096,
            restart_policy: RunnerRestartPolicy::OnFailure,
        },
    };
    let pool = RunnerPool::with_options(options.clone());

    assert_eq!(pool.options(), &options);
}

#[test]
fn runner_pool_telemetry_tracks_invokes_failures_and_bytes() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready(invokes))
        .expect("insert runner");

    let before = pool.telemetry();
    assert_eq!(before.starts, 1);
    assert_eq!(before.invokes, 0);
    assert_eq!(before.reuses, 0);

    let response = pool.invoke(&config, request()).expect("invoke runner");
    assert_eq!(response.outputs.get("out"), Some(&WireValue::Int(42)));

    let after = pool.telemetry();
    assert_eq!(after.starts, 1);
    assert_eq!(after.invokes, 1);
    assert_eq!(after.reuses, 1);
    assert_eq!(after.failures, 0);
    assert!(after.bytes_sent > 0);
    assert!(after.bytes_received > 0);
}

#[test]
fn runner_pool_records_shared_ffi_telemetry() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let telemetry = FfiHostTelemetry::new();
    let mut pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
    pool.insert(&config, FakeRunner::ready(invokes))
        .expect("insert runner");

    pool.invoke(&config, request()).expect("invoke runner");

    let key = RunnerKey::from_backend(&config).expect("runner key");
    let report = telemetry.snapshot();
    let backend = report
        .backends
        .get(key.as_str())
        .expect("backend telemetry");
    assert_eq!(backend.backend_kind.as_deref(), Some("python"));
    assert_eq!(backend.language.as_deref(), Some("python"));
    assert_eq!(backend.runner_starts, 1);
    assert_eq!(backend.runner_reuses, 1);
    assert_eq!(backend.invokes, 1);
    assert_eq!(backend.idle_runners, 1);
    assert_eq!(backend.capacity, Some(1));
    assert!(backend.checkout_wait_duration > Duration::ZERO);
    assert!(backend.invoke_duration > Duration::ZERO);
    assert!(backend.bytes_sent > 0);
    assert!(backend.bytes_received > 0);
}

#[test]
fn runner_pool_telemetry_tracks_not_ready_failure_shutdown_and_prune() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let stopped_config = backend_config("stopped.py");
    let failing_config = backend_config("failing.py");
    let pruned_config = backend_config("pruned.py");
    let mut pool = RunnerPool::with_options(RunnerPoolOptions {
        idle_timeout: Some(Duration::ZERO),
        limits: RunnerLimits::default(),
    });
    pool.insert(&stopped_config, FakeRunner::stopped(invokes.clone()))
        .expect("insert stopped runner");
    pool.insert(&failing_config, FakeRunner::failing(invokes.clone()))
        .expect("insert failing runner");
    pool.insert(&pruned_config, FakeRunner::ready(invokes))
        .expect("insert pruned runner");

    assert!(matches!(
        pool.invoke(&stopped_config, request()),
        Err(RunnerPoolError::RunnerNotReady(RunnerHealth::Stopped))
    ));
    assert!(matches!(
        pool.invoke(&failing_config, request()),
        Err(RunnerPoolError::Runner(_))
    ));
    pool.shutdown(&stopped_config)
        .expect("shutdown stopped runner");
    assert_eq!(pool.prune_idle().expect("prune idle"), 2);

    let telemetry = pool.telemetry();
    assert_eq!(telemetry.starts, 3);
    assert_eq!(telemetry.invokes, 0);
    assert_eq!(telemetry.reuses, 2);
    assert_eq!(telemetry.not_ready, 1);
    assert_eq!(telemetry.failures, 1);
    assert_eq!(telemetry.shutdowns, 3);
    assert_eq!(telemetry.pruned, 2);
}

#[test]
fn runner_pool_forwards_state_export_and_import() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(Mutex::new(None));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready_with_state(invokes, state))
        .expect("insert runner");

    assert_eq!(
        pool.export_state(&config, "demo:add")
            .expect("export empty state"),
        None
    );

    pool.import_state(&config, "demo:add", WireValue::Int(7))
        .expect("import state");

    assert_eq!(
        pool.export_state(&config, "demo:add")
            .expect("export state"),
        Some(WireValue::Int(7))
    );
}

#[test]
fn runner_pool_invokes_batches_with_default_runner_fallback() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready(invokes.clone()))
        .expect("insert runner");

    let responses = pool
        .invoke_batch(
            &config,
            vec![
                request_with_correlation("req-1"),
                request_with_correlation("req-2"),
                request_with_correlation("req-3"),
            ],
        )
        .expect("invoke batch");

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0].correlation_id.as_deref(), Some("req-1"));
    assert_eq!(responses[2].correlation_id.as_deref(), Some("req-3"));
    assert_eq!(invokes.load(Ordering::SeqCst), 3);

    let telemetry = pool.telemetry();
    assert_eq!(telemetry.invokes, 3);
    assert_eq!(telemetry.reuses, 1);
    assert!(telemetry.bytes_sent > 0);
    assert!(telemetry.bytes_received > 0);
}

#[test]
fn runner_pool_reuses_runner_for_equivalent_backend_configs() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let config = backend_config("plugin.py");
    let equivalent = backend_config("plugin.py");
    let mut pool = RunnerPool::new();
    pool.insert(&config, FakeRunner::ready(invokes.clone()))
        .expect("insert runner");

    pool.invoke(&config, request_with_correlation("req-1"))
        .expect("first invoke");
    pool.invoke(&equivalent, request_with_correlation("req-2"))
        .expect("second invoke");

    let telemetry = pool.telemetry();
    assert_eq!(invokes.load(Ordering::SeqCst), 2);
    assert_eq!(telemetry.starts, 1);
    assert_eq!(telemetry.invokes, 2);
    assert_eq!(telemetry.reuses, 2);
}

#[test]
fn runner_pool_tracks_payload_ref_leases() {
    let pool = RunnerPool::new();
    let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![1, 2, 3]));
    let wire = pool
        .lease_payload(
            "payload-1",
            payload.clone(),
            AccessMode::Read,
            PayloadLeaseScope::Invoke,
        )
        .expect("lease payload");
    let WireValue::Handle(handle) = wire else {
        panic!("expected handle");
    };

    assert_eq!(handle.id, "payload-1");
    assert_eq!(handle.type_key, TypeKey::new("bytes"));
    assert_eq!(pool.telemetry().active_payload_leases, 1);

    let resolved = pool.resolve_payload_ref(&handle).expect("resolve payload");
    assert_eq!(resolved.type_key(), payload.type_key());

    assert_eq!(pool.release_invoke_payload_refs().expect("release"), 1);
    let telemetry = pool.telemetry();
    assert_eq!(telemetry.active_payload_leases, 0);
    assert_eq!(telemetry.released_payload_leases, 1);
    assert!(matches!(
        pool.resolve_payload_ref(&handle),
        Err(RunnerPoolError::MissingPayloadLease(id)) if id == "payload-1"
    ));
}

#[test]
fn runner_pool_records_payload_lease_ffi_telemetry() {
    let telemetry = FfiHostTelemetry::new();
    let pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
    let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![1, 2, 3]));
    let wire = pool
        .lease_payload(
            "payload-1",
            payload,
            AccessMode::View,
            PayloadLeaseScope::Invoke,
        )
        .expect("lease payload");
    let WireValue::Handle(handle) = wire else {
        panic!("expected handle");
    };

    pool.resolve_payload_ref(&handle).expect("resolve payload");
    pool.release_payload_ref("payload-1")
        .expect("release payload");

    let report = telemetry.snapshot();
    assert_eq!(report.payloads.handles_created, 1);
    assert_eq!(report.payloads.handles_resolved, 1);
    assert_eq!(report.payloads.borrows, 1);
    assert_eq!(report.payloads.releases, 1);
    assert_eq!(report.payloads.active_leases, 1);
    assert_eq!(report.payloads.zero_copy_hits, 1);
    assert_eq!(report.payloads.by_access_mode.get("view"), Some(&1));
}

#[test]
fn host_telemetry_records_adapter_and_in_process_abi_metrics() {
    let telemetry = FfiHostTelemetry::new();
    telemetry.record_adapter(
        "demo.external_to_internal",
        FfiAdapterTelemetry {
            source_type_key: Some("external:type".into()),
            target_type_key: Some("internal:type".into()),
            origin: Some("external_plugin".into()),
            calls: 1,
            duration: Duration::from_micros(12),
            ..Default::default()
        },
    );

    let key = RunnerKey("abi-runner".into());
    telemetry.record_in_process_abi(
        &key,
        FfiBackendTelemetry {
            backend_kind: Some("c_cpp".into()),
            language: Some("c_cpp".into()),
            dynamic_library_load_duration: Duration::from_micros(30),
            symbol_lookup_duration: Duration::from_micros(4),
            abi_call_duration: Duration::from_micros(9),
            pointer_length_payload_calls: 2,
            abi_error_codes: 1,
            panic_boundary_errors: 1,
            ..Default::default()
        },
    );

    let report = telemetry.snapshot();
    let adapter = report
        .adapters
        .get("demo.external_to_internal")
        .expect("adapter telemetry");
    assert_eq!(adapter.adapter_id, "demo.external_to_internal");
    assert_eq!(adapter.calls, 1);
    assert_eq!(adapter.origin.as_deref(), Some("external_plugin"));

    let backend = report.backends.get("abi-runner").expect("abi telemetry");
    assert_eq!(backend.backend_key, "abi-runner");
    assert_eq!(backend.backend_kind.as_deref(), Some("c_cpp"));
    assert_eq!(backend.pointer_length_payload_calls, 2);
    assert_eq!(backend.abi_error_codes, 1);
    assert_eq!(backend.panic_boundary_errors, 1);
    assert!(backend.dynamic_library_load_duration > Duration::ZERO);
    assert!(backend.symbol_lookup_duration > Duration::ZERO);
    assert!(backend.abi_call_duration > Duration::ZERO);
}

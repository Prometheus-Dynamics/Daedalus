//! Host-side FFI plugin installation and runner orchestration.
//!
//! This crate owns the shared installer, runner pool, persistent worker process runner, response
//! decoding, state synchronization, and registry schema export surface.

mod conformance;
mod installer;
mod process;
mod response;
mod schema_export;
mod state;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_ffi_core::{BackendConfig, InvokeRequest, InvokeResponse, WireValue};
use thiserror::Error;

pub use conformance::{
    FixtureHarnessError, FixtureHarnessReport, run_generated_fixture_harness,
    run_scalar_add_generated_fixture_harness,
};
pub use daedalus_ffi_core as core;
pub use installer::{
    BackendRunnerFactory, HostInstallError, HostInstallPlan, install_language_package,
    install_language_schema, install_package, install_plan_runners, install_schema,
    install_schema_with_backends, node_decl_from_schema, node_decls_from_schema,
    plugin_manifest_from_schema, port_decl_from_schema,
};
pub use process::PersistentWorkerRunner;
pub use response::{DecodedInvokeResponse, ResponseDecodeError, decode_response};
pub use schema_export::{
    SchemaExportError, export_registry_plugin_schema, export_registry_plugin_schema_json,
    export_snapshot_plugin_schema, node_schema_from_decl, plugin_schema_from_manifest,
};
pub use state::{
    StateSyncError, StateSyncPolicy, StateSyncResult, export_runner_state, sync_response_state,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RunnerKey(String);

impl RunnerKey {
    pub fn from_backend(config: &BackendConfig) -> Result<Self, RunnerPoolError> {
        serde_json::to_string(config)
            .map(Self)
            .map_err(RunnerPoolError::Key)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerHealth {
    Ready,
    Starting,
    Degraded,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerRestartPolicy {
    Never,
    OnFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerLimits {
    pub queue_depth: usize,
    pub request_timeout: Option<Duration>,
    pub stderr_capture_bytes: usize,
    pub restart_policy: RunnerRestartPolicy,
}

impl Default for RunnerLimits {
    fn default() -> Self {
        Self {
            queue_depth: 1,
            request_timeout: None,
            stderr_capture_bytes: 64 * 1024,
            restart_policy: RunnerRestartPolicy::Never,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunnerPoolOptions {
    pub idle_timeout: Option<Duration>,
    pub limits: RunnerLimits,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunnerPoolTelemetry {
    pub starts: u64,
    pub invokes: u64,
    pub reuses: u64,
    pub failures: u64,
    pub not_ready: u64,
    pub shutdowns: u64,
    pub pruned: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

pub trait BackendRunner: Send + Sync + 'static {
    fn start(&self) -> Result<(), RunnerPoolError> {
        Ok(())
    }

    fn health(&self) -> RunnerHealth {
        RunnerHealth::Ready
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError>;

    fn invoke_batch(
        &self,
        requests: Vec<InvokeRequest>,
    ) -> Result<Vec<InvokeResponse>, RunnerPoolError> {
        requests
            .into_iter()
            .map(|request| self.invoke(request))
            .collect()
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        None
    }

    fn export_state(&self, _node_id: &str) -> Result<Option<WireValue>, RunnerPoolError> {
        Ok(None)
    }

    fn import_state(&self, _node_id: &str, _state: WireValue) -> Result<(), RunnerPoolError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), RunnerPoolError> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RunnerPoolError {
    #[error("failed to derive runner key: {0}")]
    Key(serde_json::Error),
    #[error("runner missing for backend config")]
    MissingRunner,
    #[error("runner is not ready: {0:?}")]
    RunnerNotReady(RunnerHealth),
    #[error("runner lock poisoned")]
    LockPoisoned,
    #[error("runner failed: {0}")]
    Runner(String),
}

#[derive(Default)]
pub struct RunnerPool {
    options: RunnerPoolOptions,
    runners: BTreeMap<RunnerKey, RunnerEntry>,
    telemetry: Arc<RunnerTelemetryCounters>,
}

struct RunnerEntry {
    runner: Arc<dyn BackendRunner>,
    last_used: Mutex<Instant>,
}

#[derive(Default)]
struct RunnerTelemetryCounters {
    starts: AtomicU64,
    invokes: AtomicU64,
    reuses: AtomicU64,
    failures: AtomicU64,
    not_ready: AtomicU64,
    shutdowns: AtomicU64,
    pruned: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

impl RunnerPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(options: RunnerPoolOptions) -> Self {
        Self {
            options,
            runners: BTreeMap::new(),
            telemetry: Arc::default(),
        }
    }

    pub fn options(&self) -> &RunnerPoolOptions {
        &self.options
    }

    pub fn insert<R>(
        &mut self,
        config: &BackendConfig,
        runner: R,
    ) -> Result<RunnerKey, RunnerPoolError>
    where
        R: BackendRunner,
    {
        self.insert_shared(config, Arc::new(runner))
    }

    pub fn insert_shared(
        &mut self,
        config: &BackendConfig,
        runner: Arc<dyn BackendRunner>,
    ) -> Result<RunnerKey, RunnerPoolError> {
        let key = RunnerKey::from_backend(config)?;
        runner.start()?;
        self.telemetry.starts.fetch_add(1, Ordering::Relaxed);
        self.runners.insert(
            key.clone(),
            RunnerEntry {
                runner,
                last_used: Mutex::new(Instant::now()),
            },
        );
        Ok(key)
    }

    pub fn get(&self, config: &BackendConfig) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        let key = RunnerKey::from_backend(config)?;
        let entry = self
            .runners
            .get(&key)
            .ok_or(RunnerPoolError::MissingRunner)?;
        entry.touch()?;
        self.telemetry.reuses.fetch_add(1, Ordering::Relaxed);
        Ok(entry.runner.clone())
    }

    pub fn health(&self, config: &BackendConfig) -> Result<RunnerHealth, RunnerPoolError> {
        Ok(self.get(config)?.health())
    }

    pub fn invoke(
        &self,
        config: &BackendConfig,
        request: InvokeRequest,
    ) -> Result<InvokeResponse, RunnerPoolError> {
        let runner = self.get(config)?;
        let bytes_sent = estimate_json_bytes(&request);
        match runner.health() {
            RunnerHealth::Ready => match runner.invoke(request) {
                Ok(response) => {
                    self.telemetry.invokes.fetch_add(1, Ordering::Relaxed);
                    self.telemetry
                        .bytes_sent
                        .fetch_add(bytes_sent, Ordering::Relaxed);
                    self.telemetry
                        .bytes_received
                        .fetch_add(estimate_json_bytes(&response), Ordering::Relaxed);
                    Ok(response)
                }
                Err(err) => {
                    self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    Err(err)
                }
            },
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                Err(RunnerPoolError::RunnerNotReady(health))
            }
        }
    }

    pub fn invoke_batch(
        &self,
        config: &BackendConfig,
        requests: Vec<InvokeRequest>,
    ) -> Result<Vec<InvokeResponse>, RunnerPoolError> {
        let runner = self.get(config)?;
        let bytes_sent = requests.iter().map(estimate_json_bytes).sum();
        match runner.health() {
            RunnerHealth::Ready => match runner.invoke_batch(requests) {
                Ok(responses) => {
                    self.telemetry
                        .invokes
                        .fetch_add(responses.len() as u64, Ordering::Relaxed);
                    self.telemetry
                        .bytes_sent
                        .fetch_add(bytes_sent, Ordering::Relaxed);
                    self.telemetry.bytes_received.fetch_add(
                        responses.iter().map(estimate_json_bytes).sum(),
                        Ordering::Relaxed,
                    );
                    Ok(responses)
                }
                Err(err) => {
                    self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    Err(err)
                }
            },
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                Err(RunnerPoolError::RunnerNotReady(health))
            }
        }
    }

    pub fn shutdown(&mut self, config: &BackendConfig) -> Result<(), RunnerPoolError> {
        let key = RunnerKey::from_backend(config)?;
        let entry = self
            .runners
            .remove(&key)
            .ok_or(RunnerPoolError::MissingRunner)?;
        entry.runner.shutdown()?;
        self.telemetry.shutdowns.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn export_state(
        &self,
        config: &BackendConfig,
        node_id: &str,
    ) -> Result<Option<WireValue>, RunnerPoolError> {
        let runner = self.get(config)?;
        match runner.health() {
            RunnerHealth::Ready => runner.export_state(node_id),
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                Err(RunnerPoolError::RunnerNotReady(health))
            }
        }
    }

    pub fn import_state(
        &self,
        config: &BackendConfig,
        node_id: &str,
        state: WireValue,
    ) -> Result<(), RunnerPoolError> {
        let runner = self.get(config)?;
        match runner.health() {
            RunnerHealth::Ready => runner.import_state(node_id, state),
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                Err(RunnerPoolError::RunnerNotReady(health))
            }
        }
    }

    pub fn prune_idle(&mut self) -> Result<usize, RunnerPoolError> {
        let Some(timeout) = self.options.idle_timeout else {
            return Ok(0);
        };

        let mut idle = Vec::new();
        for (key, entry) in &self.runners {
            if entry.idle_for()? >= timeout {
                idle.push(key.clone());
            }
        }

        let removed = idle.len();
        for key in idle {
            if let Some(entry) = self.runners.remove(&key) {
                entry.runner.shutdown()?;
                self.telemetry.shutdowns.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.telemetry
            .pruned
            .fetch_add(removed as u64, Ordering::Relaxed);
        Ok(removed)
    }

    pub fn shutdown_all(&mut self) -> Result<(), RunnerPoolError> {
        let runners = std::mem::take(&mut self.runners);
        for entry in runners.into_values() {
            entry.runner.shutdown()?;
            self.telemetry.shutdowns.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.runners.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runners.is_empty()
    }

    pub fn telemetry(&self) -> RunnerPoolTelemetry {
        self.telemetry.snapshot()
    }
}

impl Drop for RunnerPool {
    fn drop(&mut self) {
        let _ = self.shutdown_all();
    }
}

impl RunnerEntry {
    fn touch(&self) -> Result<(), RunnerPoolError> {
        let mut last_used = self
            .last_used
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        *last_used = Instant::now();
        Ok(())
    }

    fn idle_for(&self) -> Result<Duration, RunnerPoolError> {
        let last_used = self
            .last_used
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        Ok(last_used.elapsed())
    }
}

impl RunnerTelemetryCounters {
    fn snapshot(&self) -> RunnerPoolTelemetry {
        RunnerPoolTelemetry {
            starts: self.starts.load(Ordering::Relaxed),
            invokes: self.invokes.load(Ordering::Relaxed),
            reuses: self.reuses.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            not_ready: self.not_ready.load(Ordering::Relaxed),
            shutdowns: self.shutdowns.load(Ordering::Relaxed),
            pruned: self.pruned.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

fn estimate_json_bytes<T: serde::Serialize>(value: &T) -> u64 {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use daedalus_ffi_core::{
        BackendKind, BackendRuntimeModel, InvokeRequest, InvokeResponse, WireValue,
    };

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

        fn ready_with_state(
            invokes: Arc<AtomicUsize>,
            state: Arc<Mutex<Option<WireValue>>>,
        ) -> Self {
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
        let different =
            RunnerKey::from_backend(&backend_config("other.py")).expect("different key");

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
}

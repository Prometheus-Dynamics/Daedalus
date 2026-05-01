use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_ffi_core::{
    BackendConfig, BackendKind, InvokeRequest, InvokeResponse, WirePayloadHandle, WireValue,
};
use daedalus_runtime::{
    FfiAdapterTelemetry, FfiBackendTelemetry, FfiPayloadTelemetry, FfiTelemetryReport,
    FfiWorkerTelemetry,
};
use daedalus_transport::{AccessMode, Payload};
use thiserror::Error;

mod payload_leases;
pub use payload_leases::{PayloadLease, PayloadLeaseScope, PayloadLeaseTable};

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
    pub active_payload_leases: u64,
    pub released_payload_leases: u64,
}

/// Shared collector for host-side FFI telemetry.
///
/// Clones share one report, which lets installer, runner pool, worker, adapter, and in-process ABI
/// paths contribute to the same runtime `FfiTelemetryReport`.
#[derive(Clone, Debug, Default)]
pub struct FfiHostTelemetry {
    report: Arc<Mutex<FfiTelemetryReport>>,
}

impl FfiHostTelemetry {
    /// Create an empty shared FFI telemetry collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a point-in-time copy of the accumulated FFI telemetry.
    pub fn snapshot(&self) -> FfiTelemetryReport {
        self.report
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    /// Merge a partial FFI telemetry report into the shared collector.
    pub fn merge(&self, update: FfiTelemetryReport) {
        self.report
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .merge(update);
    }

    fn record_backend(
        &self,
        key: &RunnerKey,
        backend: &BackendConfig,
        update: FfiBackendTelemetry,
    ) {
        let mut report = FfiTelemetryReport::default();
        let mut update = update;
        if update.backend_key.is_empty() {
            update.backend_key = key.as_str().to_owned();
        }
        update.backend_kind = update
            .backend_kind
            .or_else(|| Some(format_backend_kind(&backend.backend).to_owned()));
        update.language = update
            .language
            .or_else(|| Some(format_backend_kind(&backend.backend).to_owned()));
        report.backends.insert(key.as_str().to_owned(), update);
        self.merge(report);
    }

    pub(crate) fn record_payloads(&self, update: FfiPayloadTelemetry) {
        let report = FfiTelemetryReport {
            payloads: update,
            ..Default::default()
        };
        self.merge(report);
    }

    /// Record persistent-worker process metrics under `worker_id`.
    pub fn record_worker(&self, worker_id: impl Into<String>, update: FfiWorkerTelemetry) {
        let worker_id = worker_id.into();
        let mut report = FfiTelemetryReport::default();
        report.workers.insert(worker_id, update);
        self.merge(report);
    }

    /// Record adapter metrics under `adapter_id`.
    pub fn record_adapter(&self, adapter_id: impl Into<String>, mut update: FfiAdapterTelemetry) {
        let adapter_id = adapter_id.into();
        if update.adapter_id.is_empty() {
            update.adapter_id = adapter_id.clone();
        }
        let mut report = FfiTelemetryReport::default();
        report.adapters.insert(adapter_id, update);
        self.merge(report);
    }

    /// Record in-process ABI backend metrics for a Rust/C/C++ dynamic plugin runner.
    pub fn record_in_process_abi(&self, key: &RunnerKey, mut update: FfiBackendTelemetry) {
        if update.backend_key.is_empty() {
            update.backend_key = key.as_str().to_owned();
        }
        let mut report = FfiTelemetryReport::default();
        report.backends.insert(key.as_str().to_owned(), update);
        self.merge(report);
    }
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
    #[error("runner request timed out after {timeout:?}")]
    RequestTimedOut { timeout: Duration },
    #[error("runner limit `{limit}` is not supported by this runner: {message}")]
    UnsupportedRunnerLimit {
        limit: &'static str,
        message: String,
    },
    #[error("runner lock poisoned")]
    LockPoisoned,
    #[error("runner failed: {0}")]
    Runner(String),
    #[error("payload lease `{0}` is missing")]
    MissingPayloadLease(String),
}

#[derive(Default)]
pub struct RunnerPool {
    options: RunnerPoolOptions,
    runners: BTreeMap<RunnerKey, RunnerEntry>,
    payload_leases: PayloadLeaseTable,
    telemetry: Arc<RunnerTelemetryCounters>,
    ffi_telemetry: Option<FfiHostTelemetry>,
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
    released_payload_leases: AtomicU64,
}

impl RunnerPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(options: RunnerPoolOptions) -> Self {
        Self {
            options,
            runners: BTreeMap::new(),
            payload_leases: PayloadLeaseTable::default(),
            telemetry: Arc::default(),
            ffi_telemetry: None,
        }
    }

    pub fn options(&self) -> &RunnerPoolOptions {
        &self.options
    }

    pub fn with_ffi_telemetry(mut self, telemetry: FfiHostTelemetry) -> Self {
        self.ffi_telemetry = Some(telemetry);
        self
    }

    pub fn set_ffi_telemetry(&mut self, telemetry: FfiHostTelemetry) {
        self.ffi_telemetry = Some(telemetry);
    }

    pub fn ffi_telemetry(&self) -> Option<FfiTelemetryReport> {
        self.ffi_telemetry.as_ref().map(FfiHostTelemetry::snapshot)
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
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_backend(
                &key,
                config,
                FfiBackendTelemetry {
                    backend_key: key.as_str().to_owned(),
                    runner_starts: 1,
                    idle_runners: self.runners.len() as u64,
                    capacity: Some(self.options.limits.queue_depth as u64),
                    ..Default::default()
                },
            );
        }
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
        let checkout_started = Instant::now();
        let key = RunnerKey::from_backend(config)?;
        let entry = self
            .runners
            .get(&key)
            .ok_or(RunnerPoolError::MissingRunner)?;
        entry.touch()?;
        let checkout_wait_duration = checkout_started.elapsed();
        self.telemetry.reuses.fetch_add(1, Ordering::Relaxed);
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_backend(
                &key,
                config,
                FfiBackendTelemetry {
                    backend_key: key.as_str().to_owned(),
                    runner_reuses: 1,
                    checkout_wait_duration,
                    idle_runners: self.runners.len() as u64,
                    capacity: Some(self.options.limits.queue_depth as u64),
                    ..Default::default()
                },
            );
        }
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
        let key = RunnerKey::from_backend(config)?;
        let bytes_sent = estimate_json_bytes(&request);
        let started = Instant::now();
        match runner.health() {
            RunnerHealth::Ready => match runner.invoke(request) {
                Ok(response) => {
                    let duration = started.elapsed();
                    let bytes_received = estimate_json_bytes(&response);
                    self.telemetry.invokes.fetch_add(1, Ordering::Relaxed);
                    self.telemetry
                        .bytes_sent
                        .fetch_add(bytes_sent, Ordering::Relaxed);
                    self.telemetry
                        .bytes_received
                        .fetch_add(bytes_received, Ordering::Relaxed);
                    if let Some(telemetry) = &self.ffi_telemetry {
                        telemetry.record_backend(
                            &key,
                            config,
                            FfiBackendTelemetry {
                                backend_key: key.as_str().to_owned(),
                                invokes: 1,
                                invoke_duration: duration,
                                bytes_sent,
                                bytes_received,
                                ..Default::default()
                            },
                        );
                    }
                    Ok(response)
                }
                Err(err) => {
                    self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    if let Some(telemetry) = &self.ffi_telemetry {
                        telemetry.record_backend(
                            &key,
                            config,
                            FfiBackendTelemetry {
                                backend_key: key.as_str().to_owned(),
                                runner_failures: 1,
                                ..Default::default()
                            },
                        );
                    }
                    Err(err)
                }
            },
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                if let Some(telemetry) = &self.ffi_telemetry {
                    telemetry.record_backend(
                        &key,
                        config,
                        FfiBackendTelemetry {
                            backend_key: key.as_str().to_owned(),
                            runner_not_ready: 1,
                            ..Default::default()
                        },
                    );
                }
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
        let key = RunnerKey::from_backend(config)?;
        let bytes_sent = requests.iter().map(estimate_json_bytes).sum();
        let started = Instant::now();
        match runner.health() {
            RunnerHealth::Ready => match runner.invoke_batch(requests) {
                Ok(responses) => {
                    let duration = started.elapsed();
                    let bytes_received = responses.iter().map(estimate_json_bytes).sum();
                    self.telemetry
                        .invokes
                        .fetch_add(responses.len() as u64, Ordering::Relaxed);
                    self.telemetry
                        .bytes_sent
                        .fetch_add(bytes_sent, Ordering::Relaxed);
                    self.telemetry
                        .bytes_received
                        .fetch_add(bytes_received, Ordering::Relaxed);
                    if let Some(telemetry) = &self.ffi_telemetry {
                        telemetry.record_backend(
                            &key,
                            config,
                            FfiBackendTelemetry {
                                backend_key: key.as_str().to_owned(),
                                invokes: responses.len() as u64,
                                invoke_duration: duration,
                                bytes_sent,
                                bytes_received,
                                ..Default::default()
                            },
                        );
                    }
                    Ok(responses)
                }
                Err(err) => {
                    self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    if let Some(telemetry) = &self.ffi_telemetry {
                        telemetry.record_backend(
                            &key,
                            config,
                            FfiBackendTelemetry {
                                backend_key: key.as_str().to_owned(),
                                runner_failures: 1,
                                ..Default::default()
                            },
                        );
                    }
                    Err(err)
                }
            },
            health => {
                self.telemetry.not_ready.fetch_add(1, Ordering::Relaxed);
                if let Some(telemetry) = &self.ffi_telemetry {
                    telemetry.record_backend(
                        &key,
                        config,
                        FfiBackendTelemetry {
                            backend_key: key.as_str().to_owned(),
                            runner_not_ready: 1,
                            ..Default::default()
                        },
                    );
                }
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
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_backend(
                &key,
                config,
                FfiBackendTelemetry {
                    backend_key: key.as_str().to_owned(),
                    runner_shutdowns: 1,
                    ..Default::default()
                },
            );
        }
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
                if let Some(telemetry) = &self.ffi_telemetry {
                    let mut report = FfiTelemetryReport::default();
                    report.backends.insert(
                        key.as_str().to_owned(),
                        FfiBackendTelemetry {
                            backend_key: key.as_str().to_owned(),
                            runner_shutdowns: 1,
                            runner_pruned: 1,
                            ..Default::default()
                        },
                    );
                    telemetry.merge(report);
                }
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
        let mut snapshot = self.telemetry.snapshot();
        snapshot.active_payload_leases = self.payload_leases.len() as u64;
        snapshot
    }

    pub fn lease_payload(
        &self,
        lease_id: impl Into<String>,
        payload: Payload,
        access: AccessMode,
        scope: PayloadLeaseScope,
    ) -> Result<WireValue, RunnerPoolError> {
        let value = self
            .payload_leases
            .insert(lease_id, payload, access, scope)?;
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_payloads(FfiPayloadTelemetry {
                handles_created: 1,
                active_leases: self.payload_leases.len() as u64,
                ..Default::default()
            });
        }
        Ok(value)
    }

    pub fn resolve_payload_ref(
        &self,
        handle: &WirePayloadHandle,
    ) -> Result<Payload, RunnerPoolError> {
        let payload = self.payload_leases.resolve(handle)?;
        if let Some(telemetry) = &self.ffi_telemetry {
            let mut by_access_mode = BTreeMap::new();
            by_access_mode.insert(format_access_mode(handle.access), 1);
            let mut by_residency = BTreeMap::new();
            if let Some(residency) = &handle.residency {
                by_residency.insert(residency.as_str().to_owned(), 1);
            }
            let mut by_layout = BTreeMap::new();
            if let Some(layout) = &handle.layout {
                by_layout.insert(layout.as_str().to_owned(), 1);
            }
            telemetry.record_payloads(FfiPayloadTelemetry {
                handles_resolved: 1,
                borrows: 1,
                active_leases: self.payload_leases.len() as u64,
                zero_copy_hits: u64::from(matches!(handle.access, AccessMode::View)),
                shared_reference_hits: u64::from(matches!(handle.access, AccessMode::Read)),
                cow_materializations: u64::from(is_cow_payload_handle(handle)),
                mutable_in_place_hits: u64::from(
                    matches!(handle.access, AccessMode::Modify) && !is_cow_payload_handle(handle),
                ),
                owned_moves: u64::from(matches!(handle.access, AccessMode::Move)),
                by_access_mode,
                by_residency,
                by_layout,
                ..Default::default()
            });
        }
        Ok(payload)
    }

    pub fn release_payload_ref(&self, lease_id: &str) -> Result<Payload, RunnerPoolError> {
        let payload = self.payload_leases.release(lease_id)?;
        self.telemetry
            .released_payload_leases
            .fetch_add(1, Ordering::Relaxed);
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_payloads(FfiPayloadTelemetry {
                releases: 1,
                active_leases: self.payload_leases.len() as u64,
                ..Default::default()
            });
        }
        Ok(payload)
    }

    pub fn release_invoke_payload_refs(&self) -> Result<usize, RunnerPoolError> {
        let released = self
            .payload_leases
            .release_scope(PayloadLeaseScope::Invoke)?;
        self.telemetry
            .released_payload_leases
            .fetch_add(released as u64, Ordering::Relaxed);
        if let Some(telemetry) = &self.ffi_telemetry {
            telemetry.record_payloads(FfiPayloadTelemetry {
                releases: released as u64,
                active_leases: self.payload_leases.len() as u64,
                ..Default::default()
            });
        }
        Ok(released)
    }
}

impl Drop for RunnerPool {
    fn drop(&mut self) {
        let _ = self.shutdown_all();
        let _ = self.release_invoke_payload_refs();
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
            active_payload_leases: 0,
            released_payload_leases: self.released_payload_leases.load(Ordering::Relaxed),
        }
    }
}

fn estimate_json_bytes<T: serde::Serialize>(value: &T) -> u64 {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn format_backend_kind(kind: &BackendKind) -> &str {
    match kind {
        BackendKind::Rust => "rust",
        BackendKind::Python => "python",
        BackendKind::Node => "node",
        BackendKind::Java => "java",
        BackendKind::CCpp => "c_cpp",
        BackendKind::Shader => "shader",
        BackendKind::Other(value) => value.as_str(),
    }
}

fn format_access_mode(access: AccessMode) -> String {
    access.as_str().to_owned()
}

fn is_cow_payload_handle(handle: &WirePayloadHandle) -> bool {
    handle
        .metadata
        .get("ownership_mode")
        .and_then(serde_json::Value::as_str)
        == Some("cow")
}

#[cfg(test)]
mod tests;

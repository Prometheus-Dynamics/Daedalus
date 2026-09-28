use super::*;

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

    pub(super) fn record_backend(
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

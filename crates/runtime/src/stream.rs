use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_transport::{
    FeedOutcome, FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy, TypeKey,
};

use crate::RuntimePlan;
use crate::executor::{ExecuteError, ExecutionTelemetry, NodeHandler, OwnedExecutor};
use crate::host_bridge::{
    HostBridgeConfig, HostBridgeEvent, HostBridgeHandle, HostBridgeManager, HostBridgeStats,
};

mod worker;
pub use worker::{
    StreamGraphWorker, StreamWorkerConfig, StreamWorkerDiagnostics, StreamWorkerStopError,
};

// Keep synchronous graph polling and host IO handles in this module. Continuous worker lifecycle
// and shutdown behavior live in `stream::worker`.
pub const DEFAULT_STREAM_IDLE_SLEEP: Duration = Duration::from_millis(100);
pub(super) const STREAM_NO_PROGRESS_WARNING: &str =
    "stream tick left host inbound pending unchanged; pausing drain loop to avoid a busy spin";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamGraphState {
    #[default]
    Created,
    Running,
    Paused,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputStats {
    pub accepted: u64,
    pub dropped: u64,
    pub pending: usize,
    pub closed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OutputStats {
    pub delivered: u64,
    pub dropped: u64,
    pub pending: usize,
    pub closed: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamWorkerState {
    #[default]
    Idle,
    Running,
    Paused,
    Closed,
    BlockedInExecution,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamExecutionMode {
    #[default]
    RetainedSerial,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamTelemetrySummary {
    pub nodes_executed: usize,
    pub backpressure_events: usize,
    pub warnings: usize,
    pub errors: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGraphDiagnostics {
    pub state: StreamGraphState,
    pub worker_state: StreamWorkerState,
    pub execution_mode: StreamExecutionMode,
    pub executor_busy: bool,
    pub pending_inbound: usize,
    pub pending_outbound: usize,
    pub host_stats: HostBridgeStats,
    pub host_config: HostBridgeConfig,
    pub current_execution_elapsed: Option<Duration>,
    pub last_execution_duration: Option<Duration>,
    pub last_error: Option<String>,
    pub last_telemetry: Option<StreamTelemetrySummary>,
}

#[derive(Clone)]
pub struct GraphInput {
    handle: HostBridgeHandle,
    port: String,
}

impl GraphInput {
    pub fn set_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.handle
            .set_input_policy(self.port.clone(), pressure, freshness)
    }

    pub fn feed(&self, payload: Payload) -> Result<FeedOutcome, ExecuteError> {
        Ok(self.handle.feed_payload(self.port.clone(), payload))
    }

    pub fn feed_typed<T>(
        &self,
        type_key: impl Into<TypeKey>,
        value: T,
    ) -> Result<FeedOutcome, ExecuteError>
    where
        T: Send + Sync + 'static,
    {
        self.feed(Payload::owned(type_key, value))
    }

    pub fn close(&self) -> Result<(), ExecuteError> {
        self.handle.close_input(self.port.clone());
        Ok(())
    }

    pub fn stats(&self) -> InputStats {
        let stats = self.handle.stats();
        InputStats {
            accepted: stats.inbound_accepted,
            dropped: stats.inbound_dropped,
            pending: self.handle.pending_inbound(),
            closed: self.handle.is_input_closed(&self.port),
        }
    }
}

#[derive(Clone)]
pub struct GraphOutput {
    handle: HostBridgeHandle,
    port: String,
}

impl GraphOutput {
    pub fn set_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.handle
            .set_output_policy(self.port.clone(), pressure, freshness)
    }

    pub fn try_recv(&self) -> Result<Option<Payload>, ExecuteError> {
        Ok(self.handle.try_pop_payload(&self.port))
    }

    pub fn try_recv_typed<T>(&self) -> Result<Option<T>, ExecuteError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self
            .try_recv()?
            .and_then(|payload| payload.get_ref::<T>().cloned()))
    }

    /// Receive an output payload by blocking until data is delivered or `timeout` expires.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<Payload>, ExecuteError> {
        Ok(self.handle.recv_payload_timeout(&self.port, timeout))
    }

    pub fn subscribe(&self) -> OutputSubscription {
        OutputSubscription {
            output: self.clone(),
        }
    }

    pub fn stats(&self) -> OutputStats {
        let stats = self.handle.stats();
        OutputStats {
            delivered: stats.outbound_delivered,
            dropped: stats.outbound_dropped,
            pending: self.handle.pending_outbound(),
            closed: stats.closed,
        }
    }
}

pub struct OutputSubscription {
    output: GraphOutput,
}

impl OutputSubscription {
    pub fn try_recv(&self) -> Result<Option<Payload>, ExecuteError> {
        self.output.try_recv()
    }
}

/// Retained synchronous stream graph.
///
/// This lower-level runtime API always executes each stream tick with the retained serial
/// executor. Use the engine `HostGraph`/`CompiledRun` APIs when a graph should honor engine
/// `RuntimeMode::Parallel` or `RuntimeMode::Adaptive` settings.
pub struct StreamGraph<H: NodeHandler> {
    executor: Option<OwnedExecutor<H>>,
    bridges: HostBridgeManager,
    host_alias: String,
    state: StreamGraphState,
    last_telemetry: Option<ExecutionTelemetry>,
    last_error: Option<String>,
    current_execution_started_at: Option<Instant>,
    last_execution_duration: Option<Duration>,
    _handler: PhantomData<H>,
}

impl<H: NodeHandler> StreamGraph<H> {
    pub fn new(plan: Arc<RuntimePlan>, handler: H) -> Self {
        Self::with_host_alias(plan, handler, "host")
    }

    pub fn with_host_alias(
        plan: Arc<RuntimePlan>,
        handler: H,
        host_alias: impl Into<String>,
    ) -> Self {
        let host_alias = host_alias.into();
        let bridges = HostBridgeManager::new();
        bridges.populate_from_plan(&plan);
        bridges.ensure_handle(host_alias.clone());
        let executor = OwnedExecutor::new(plan, handler).with_host_bridges(bridges.clone());
        Self {
            executor: Some(executor),
            bridges,
            host_alias,
            state: StreamGraphState::Created,
            last_telemetry: None,
            last_error: None,
            current_execution_started_at: None,
            last_execution_duration: None,
            _handler: PhantomData,
        }
    }

    pub fn input(&self, port: impl Into<String>) -> Result<GraphInput, ExecuteError> {
        Ok(GraphInput {
            handle: self.bridges.ensure_handle(self.host_alias.clone()),
            port: port.into(),
        })
    }

    pub fn output(&self, port: impl Into<String>) -> Result<GraphOutput, ExecuteError> {
        Ok(GraphOutput {
            handle: self.bridges.ensure_handle(self.host_alias.clone()),
            port: port.into(),
        })
    }

    pub fn start(&mut self) -> Result<(), ExecuteError> {
        self.state = StreamGraphState::Running;
        self.bridges
            .ensure_handle(self.host_alias.clone())
            .notify_waiters();
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), ExecuteError> {
        if self.state == StreamGraphState::Running {
            self.state = StreamGraphState::Paused;
        }
        Ok(())
    }

    pub fn resume(&mut self) -> Result<(), ExecuteError> {
        if self.state == StreamGraphState::Paused {
            self.state = StreamGraphState::Running;
            self.bridges
                .ensure_handle(self.host_alias.clone())
                .notify_waiters();
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), ExecuteError> {
        self.bridges.ensure_handle(self.host_alias.clone()).close();
        self.state = StreamGraphState::Closed;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), ExecuteError> {
        self.close()
    }

    pub fn state(&self) -> StreamGraphState {
        self.state
    }

    pub fn poll(&mut self) -> Result<Option<&ExecutionTelemetry>, ExecuteError> {
        if self.state != StreamGraphState::Running {
            return Ok(self.last_telemetry.as_ref());
        }
        if !self
            .bridges
            .ensure_handle(self.host_alias.clone())
            .has_pending_inbound()
        {
            return Ok(self.last_telemetry.as_ref());
        }
        if self.executor.is_none() {
            return Err(stream_executor_unavailable());
        }
        let started = self.begin_execution();
        let result = self
            .executor
            .as_mut()
            .expect("stream executor availability checked")
            .run_in_place();
        self.finish_execution(started);
        let telemetry = match result {
            Ok(telemetry) => telemetry,
            Err(err) => {
                self.last_error = Some(err.to_string());
                return Err(err);
            }
        };
        self.last_error = None;
        self.last_telemetry = Some(telemetry);
        Ok(self.last_telemetry.as_ref())
    }

    pub fn run_available(&mut self) -> Result<Option<&ExecutionTelemetry>, ExecuteError> {
        self.drain()
    }

    pub fn drain(&mut self) -> Result<Option<&ExecutionTelemetry>, ExecuteError> {
        let mut ran = false;
        loop {
            let handle = self.bridges.ensure_handle(self.host_alias.clone());
            let pending_before = handle.pending_inbound();
            if pending_before == 0 {
                break;
            }
            if self.executor.is_none() {
                return Err(stream_executor_unavailable());
            }
            let started = self.begin_execution();
            let result = self
                .executor
                .as_mut()
                .expect("stream executor availability checked")
                .run_in_place();
            self.finish_execution(started);
            let mut telemetry = match result {
                Ok(telemetry) => telemetry,
                Err(err) => {
                    self.last_error = Some(err.to_string());
                    return Err(err);
                }
            };
            let pending_after = handle.pending_inbound();
            if pending_after >= pending_before {
                tracing::warn!(
                    target: "daedalus_runtime::stream",
                    host_alias = %self.host_alias,
                    pending_before,
                    pending_after,
                    "stream drain made no host-inbound progress"
                );
                telemetry
                    .warnings
                    .push(STREAM_NO_PROGRESS_WARNING.to_string());
                self.last_error = None;
                self.last_telemetry = Some(telemetry);
                ran = true;
                break;
            }
            self.last_error = None;
            self.last_telemetry = Some(telemetry);
            ran = true;
        }
        if !ran {
            return Ok(self.last_telemetry.as_ref());
        }
        Ok(self.last_telemetry.as_ref())
    }

    pub fn last_telemetry(&self) -> Option<&ExecutionTelemetry> {
        self.last_telemetry.as_ref()
    }

    pub fn profiler_snapshot(&self) -> Option<ExecutionTelemetry> {
        self.last_telemetry.clone()
    }

    pub fn export_profile_json(&self) -> Result<Option<String>, serde_json::Error> {
        self.last_telemetry
            .as_ref()
            .map(serde_json::to_string_pretty)
            .transpose()
    }

    pub fn host_stats(&self) -> HostBridgeStats {
        self.bridges.ensure_handle(self.host_alias.clone()).stats()
    }

    pub fn host_config(&self) -> HostBridgeConfig {
        self.bridges
            .ensure_handle(self.host_alias.clone())
            .config_snapshot()
    }

    pub fn host_events(&self) -> Vec<HostBridgeEvent> {
        self.bridges.ensure_handle(self.host_alias.clone()).events()
    }

    pub fn apply_host_config(
        &self,
        config: &HostBridgeConfig,
    ) -> Result<(), PolicyValidationError> {
        self.bridges.apply_config(config)
    }

    pub fn diagnostics(&self) -> StreamGraphDiagnostics {
        let handle = self.bridges.ensure_handle(self.host_alias.clone());
        let pending_inbound = handle.pending_inbound();
        let pending_outbound = handle.pending_outbound();
        let executor_busy = self.executor.is_none();
        let worker_state = match self.state {
            StreamGraphState::Closed => StreamWorkerState::Closed,
            StreamGraphState::Paused => StreamWorkerState::Paused,
            StreamGraphState::Created => StreamWorkerState::Idle,
            StreamGraphState::Running if executor_busy => StreamWorkerState::BlockedInExecution,
            StreamGraphState::Running if pending_inbound > 0 => StreamWorkerState::Running,
            StreamGraphState::Running => StreamWorkerState::Idle,
        };

        StreamGraphDiagnostics {
            state: self.state,
            worker_state,
            execution_mode: StreamExecutionMode::RetainedSerial,
            executor_busy,
            pending_inbound,
            pending_outbound,
            host_stats: handle.stats(),
            host_config: handle.config_snapshot(),
            current_execution_elapsed: self.current_execution_elapsed(),
            last_execution_duration: self.last_execution_duration,
            last_error: self.last_error.clone(),
            last_telemetry: self
                .last_telemetry
                .as_ref()
                .map(|telemetry| StreamTelemetrySummary {
                    nodes_executed: telemetry.nodes_executed,
                    backpressure_events: telemetry.backpressure_events,
                    warnings: telemetry.warnings.len(),
                    errors: telemetry.errors.len(),
                }),
        }
    }

    fn begin_execution(&mut self) -> Instant {
        let started = Instant::now();
        self.current_execution_started_at = Some(started);
        started
    }

    fn finish_execution(&mut self, started: Instant) {
        self.current_execution_started_at = None;
        self.last_execution_duration = Some(started.elapsed());
    }

    fn current_execution_elapsed(&self) -> Option<Duration> {
        self.current_execution_started_at
            .map(|started| started.elapsed())
    }
}

pub type SharedStreamGraph<H> = Arc<Mutex<StreamGraph<H>>>;

fn stream_executor_unavailable() -> ExecuteError {
    ExecuteError::HandlerFailed {
        node: "stream_graph".into(),
        error: crate::executor::NodeError::Handler("stream executor already running".into()),
    }
}

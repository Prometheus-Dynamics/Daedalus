use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use daedalus_runtime::ExecutionTelemetry;
use daedalus_runtime::executor::{DirectHostRoute, NodeHandler};
use daedalus_runtime::host_bridge::{HostBridgeHandle, HostBridgeManager};
use daedalus_runtime::transport::typeexpr_transport_key;
use daedalus_runtime::{RuntimePlan, RuntimePlanExplanation, RuntimeSink};
use daedalus_transport::{
    FeedOutcome, FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy, TypeKey,
};

use crate::compiled_run::{CompiledRun, RunResult};
use crate::error::EngineError;

/// In-process graph runner for host-driven applications.
///
/// Prefer the high-level flows first:
///
/// - `run_once(("in", value), "out")` for one input and one output batch.
/// - `bind_input`/`bind_output` for repeated typed feeds and drains.
/// - `bind_lane` plus `tick_direct_lane` for hot single-input/single-output routes.
///
/// Lower-level `push`, `tick`, and `drain_*` methods remain available for multi-input,
/// demand-selected, or diagnostic workflows.
pub struct HostGraph<H: NodeHandler> {
    pub(crate) runner: CompiledRun<H>,
    pub(crate) bridges: HostBridgeManager,
    pub(crate) host: HostBridgeHandle,
    pub(crate) node_labels: Arc<[String]>,
}

pub struct HostGraphSubscription {
    host: HostBridgeHandle,
    port: String,
}

pub struct HostGraphInput<T> {
    host: HostBridgeHandle,
    port: String,
    type_key: TypeKey,
    _ty: PhantomData<T>,
}

impl<T> HostGraphInput<T>
where
    T: Send + Sync + 'static,
{
    pub fn push(&self, value: T) -> FeedOutcome {
        self.host
            .feed_payload_ref(&self.port, Payload::owned(self.type_key.clone(), value))
    }

    pub fn port(&self) -> &str {
        &self.port
    }
}

pub struct HostGraphPayloadInput {
    host: HostBridgeHandle,
    port: String,
}

impl HostGraphPayloadInput {
    pub fn push(&self, payload: Payload) -> FeedOutcome {
        self.host.feed_payload_ref(&self.port, payload)
    }

    pub fn port(&self) -> &str {
        &self.port
    }
}

pub struct HostGraphOutput<T> {
    host: HostBridgeHandle,
    port: String,
    _ty: PhantomData<T>,
}

impl<T> HostGraphOutput<T>
where
    T: Send + Sync + 'static,
{
    pub fn try_take(&self) -> Result<Option<T>, Box<Payload>> {
        self.host.try_pop_owned::<T>(&self.port)
    }

    pub fn port(&self) -> &str {
        &self.port
    }
}

pub struct HostGraphPayloadOutput {
    host: HostBridgeHandle,
    port: String,
}

impl HostGraphPayloadOutput {
    pub fn try_take(&self) -> Option<Payload> {
        self.host.try_pop_payload(&self.port)
    }

    pub fn port(&self) -> &str {
        &self.port
    }
}

pub struct HostGraphLane<I> {
    route: DirectHostRoute,
    type_key: TypeKey,
    _input: PhantomData<I>,
}

impl HostGraphSubscription {
    pub fn try_recv_payload(&self) -> Option<Payload> {
        self.host.try_pop_payload(&self.port)
    }

    pub fn try_recv<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.try_pop(&self.port)
    }
}

pub trait HostGraphRunInput {
    type Value;

    fn into_parts(self) -> (String, TypeKey, Self::Value);
}

impl<I> HostGraphRunInput for (&str, I)
where
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self) -> (String, TypeKey, Self::Value) {
        (self.0.to_string(), type_key_for::<I>(), self.1)
    }
}

impl<I> HostGraphRunInput for (String, I)
where
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self) -> (String, TypeKey, Self::Value) {
        (self.0, type_key_for::<I>(), self.1)
    }
}

impl<I, K> HostGraphRunInput for (&str, K, I)
where
    I: Send + Sync + 'static,
    K: Into<TypeKey>,
{
    type Value = I;

    fn into_parts(self) -> (String, TypeKey, Self::Value) {
        (self.0.to_string(), self.1.into(), self.2)
    }
}

impl<I, K> HostGraphRunInput for (String, K, I)
where
    I: Send + Sync + 'static,
    K: Into<TypeKey>,
{
    type Value = I;

    fn into_parts(self) -> (String, TypeKey, Self::Value) {
        (self.0, self.1.into(), self.2)
    }
}

fn type_key_for<T: 'static>() -> TypeKey {
    typeexpr_transport_key(&daedalus_data::typing::type_expr::<T>())
        .unwrap_or_else(|_| TypeKey::new(std::any::type_name::<T>()))
}

pub struct HostGraphStep<T> {
    pub outputs: Vec<T>,
    pub metrics: HostGraphStepMetrics,
}

pub struct HostGraphStepMetrics {
    pub feed_duration: Duration,
    pub run_duration: Duration,
    pub drain_duration: Duration,
    pub telemetry: Option<ExecutionTelemetry>,
    node_labels: Arc<[String]>,
}

impl fmt::Debug for HostGraphStepMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(telemetry) = self.telemetry.as_ref() else {
            return f
                .debug_struct("HostGraphStepMetrics")
                .field("feed", &DurationDebug(self.feed_duration))
                .field("run", &DurationDebug(self.run_duration))
                .field("drain", &DurationDebug(self.drain_duration))
                .field("graph", &"idle")
                .finish();
        };

        let node_total = telemetry
            .node_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.total_duration)
            });
        let edge_wait = telemetry
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.total_wait)
            });
        let edge_transport = telemetry
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.transport_apply_duration)
            });
        let adapter = telemetry
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.adapter_duration)
            });
        let transport_bytes: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.transport_bytes)
            .sum();
        let copied_bytes: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.copied_bytes)
            .sum();
        let payload_clones: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.payload_clone_count)
            .sum();
        let unique_handoffs: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.unique_handoffs)
            .sum();
        let shared_handoffs: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.shared_handoffs)
            .sum();
        let queue_peak: u64 = telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.peak_queue_bytes)
            .max()
            .unwrap_or(0);
        let top_node = telemetry
            .node_metrics
            .iter()
            .max_by_key(|(_, metrics)| metrics.total_duration)
            .map(|(idx, metrics)| {
                let label = self
                    .node_labels
                    .get(*idx)
                    .map(String::as_str)
                    .unwrap_or("unknown");
                format!("{label}:{}", format_duration(metrics.total_duration))
            })
            .unwrap_or_else(|| "none".to_string());

        f.debug_struct("HostGraphStepMetrics")
            .field("feed", &DurationDebug(self.feed_duration))
            .field("run", &DurationDebug(self.run_duration))
            .field("drain", &DurationDebug(self.drain_duration))
            .field("graph", &DurationDebug(telemetry.graph_duration))
            .field("nodes", &DurationDebug(node_total))
            .field("edge_wait", &DurationDebug(edge_wait))
            .field("edge_xfer", &DurationDebug(edge_transport))
            .field("adapters", &DurationDebug(adapter))
            .field(
                "unattributed",
                &DurationDebug(telemetry.unattributed_runtime_duration),
            )
            .field("peak_queue_bytes", &queue_peak)
            .field("transport_bytes", &transport_bytes)
            .field("copied_bytes", &copied_bytes)
            .field("payload_clones", &payload_clones)
            .field("unique_handoffs", &unique_handoffs)
            .field("shared_handoffs", &shared_handoffs)
            .field("top_node", &top_node)
            .finish()
    }
}

struct DurationDebug(Duration);

impl fmt::Debug for DurationDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_duration(self.0))
    }
}

fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.2}us", nanos as f64 / 1_000.0)
    } else {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    }
}

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    pub fn prepare(&mut self) -> Result<(), EngineError> {
        #[cfg(feature = "executor-pool")]
        self.runner.executor.prewarm_worker_pool()?;
        self.runner.executor.reset();
        Ok(())
    }

    pub fn runtime_plan(&self) -> &RuntimePlan {
        self.runner.runtime_plan()
    }

    pub fn node_labels(&self) -> Vec<String> {
        self.node_labels.to_vec()
    }

    pub fn explain_plan(&self) -> RuntimePlanExplanation {
        self.runtime_plan().explain()
    }

    pub fn explain_selected(
        &self,
        sinks: impl IntoIterator<Item = RuntimeSink>,
    ) -> Result<RuntimePlanExplanation, EngineError> {
        let sinks = sinks.into_iter().collect::<Vec<_>>();
        self.runtime_plan()
            .explain_selected(&sinks)
            .map_err(|err| EngineError::Config(err.to_string()))
    }

    pub fn bridge_manager(&self) -> &HostBridgeManager {
        &self.bridges
    }

    pub fn host(&self) -> &HostBridgeHandle {
        &self.host
    }

    /// Bind a typed host input once and reuse it across ticks.
    pub fn bind_input<T>(&self, port: impl Into<String>) -> HostGraphInput<T>
    where
        T: Send + Sync + 'static,
    {
        HostGraphInput {
            host: self.host.clone(),
            port: port.into(),
            type_key: type_key_for::<T>(),
            _ty: PhantomData,
        }
    }

    /// Bind a raw payload input for dynamic or plugin-driven payloads.
    pub fn bind_payload_input(&self, port: impl Into<String>) -> HostGraphPayloadInput {
        HostGraphPayloadInput {
            host: self.host.clone(),
            port: port.into(),
        }
    }

    /// Bind a typed host output once and reuse it across ticks.
    pub fn bind_output<T>(&self, port: impl Into<String>) -> HostGraphOutput<T>
    where
        T: Send + Sync + 'static,
    {
        HostGraphOutput {
            host: self.host.clone(),
            port: port.into(),
            _ty: PhantomData,
        }
    }

    /// Bind a raw payload output for dynamic or plugin-driven payloads.
    pub fn bind_payload_output(&self, port: impl Into<String>) -> HostGraphPayloadOutput {
        HostGraphPayloadOutput {
            host: self.host.clone(),
            port: port.into(),
        }
    }

    pub fn set_input_policy(
        &self,
        port: impl Into<String>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.host.set_input_policy(port, pressure, freshness)
    }

    pub fn set_output_policy(
        &self,
        port: impl Into<String>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.host.set_output_policy(port, pressure, freshness)
    }

    pub fn set_latest_input(&self, port: impl Into<String>) -> Result<(), PolicyValidationError> {
        self.set_input_policy(
            port,
            PressurePolicy::LatestOnly,
            FreshnessPolicy::LatestByTimestamp,
        )
    }

    pub fn set_latest_output(&self, port: impl Into<String>) -> Result<(), PolicyValidationError> {
        self.set_output_policy(
            port,
            PressurePolicy::LatestOnly,
            FreshnessPolicy::LatestByTimestamp,
        )
    }

    /// Low-level typed feed. Prefer `run_once` for one-shot calls or `bind_input` for hot loops.
    pub fn push<T>(&self, port: impl Into<String>, value: T) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.host.push(port, value)
    }

    /// Low-level typed feed with an explicit transport type key.
    pub fn push_as<T>(
        &self,
        port: impl Into<String>,
        type_key: impl Into<TypeKey>,
        value: T,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.host.push_as(port, type_key, value)
    }

    pub fn push_payload(&self, port: impl Into<String>, payload: Payload) -> FeedOutcome {
        self.host.feed_payload(port, payload)
    }

    /// Execute one graph tick. Pair with `push`/`drain_*` for advanced multi-input workflows.
    pub fn tick(&mut self) -> Result<ExecutionTelemetry, EngineError> {
        self.runner.run_telemetry()
    }

    pub fn tick_direct_payload(
        &mut self,
        input_port: &str,
        payload: Payload,
        output_port: &str,
    ) -> Result<Option<(ExecutionTelemetry, Option<Payload>)>, EngineError> {
        self.runner
            .executor
            .run_direct_host_payload(input_port, payload, output_port)
            .map_err(EngineError::Runtime)
    }

    pub fn direct_host_route(
        &self,
        input_port: &str,
        output_port: &str,
    ) -> Option<DirectHostRoute> {
        self.runner
            .executor
            .direct_host_route(input_port, output_port)
    }

    /// Bind a direct host lane for repeated single-input/single-output calls.
    pub fn bind_lane<I>(&self, input_port: &str, output_port: &str) -> Option<HostGraphLane<I>>
    where
        I: Send + Sync + 'static,
    {
        self.direct_host_route(input_port, output_port)
            .map(|route| HostGraphLane {
                route,
                type_key: type_key_for::<I>(),
                _input: PhantomData,
            })
    }

    pub fn tick_direct_route(
        &mut self,
        route: &DirectHostRoute,
        payload: Payload,
    ) -> Result<(ExecutionTelemetry, Option<Payload>), EngineError> {
        self.runner
            .executor
            .run_direct_host_route(route, payload)
            .map_err(EngineError::Runtime)
    }

    pub fn tick_direct_route_payload(
        &mut self,
        route: &DirectHostRoute,
        payload: Payload,
    ) -> Result<Option<Payload>, EngineError> {
        self.runner
            .executor
            .run_direct_host_route_payload(route, payload)
            .map_err(EngineError::Runtime)
    }

    /// Run a previously bound direct lane and return the raw output payload.
    pub fn run_lane<I>(
        &mut self,
        lane: &HostGraphLane<I>,
        input: I,
    ) -> Result<Option<Payload>, EngineError>
    where
        I: Send + Sync + 'static,
    {
        self.tick_direct_route_payload(&lane.route, Payload::owned(lane.type_key.clone(), input))
    }

    /// Run a previously bound direct lane and downcast the output payload into an owned value.
    pub fn run_lane_owned<I, O>(
        &mut self,
        lane: &HostGraphLane<I>,
        input: I,
    ) -> Result<Option<O>, EngineError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        self.run_lane(lane, input)?
            .map(|payload| {
                payload.try_into_owned::<O>().map_err(|payload| {
                    EngineError::Config(format!(
                        "expected unique direct lane output payload, got type_key={} rust_type={:?}",
                        payload.type_key(),
                        payload.storage_rust_type_name()
                    ))
                })
            })
            .transpose()
    }

    /// Run one typed value through a direct host route when the graph shape supports it.
    pub fn run_direct_once<I, O>(
        &mut self,
        input_port: &str,
        output_port: &str,
        input: I,
    ) -> Result<Option<O>, EngineError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        let route = self
            .direct_host_route(input_port, output_port)
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "no direct host route from '{input_port}' to '{output_port}'"
                ))
            })?;
        let output =
            self.tick_direct_route_payload(&route, Payload::owned(type_key_for::<I>(), input))?;
        output
            .map(|payload| {
                payload.try_into_owned::<O>().map_err(|payload| {
                    EngineError::Config(format!(
                        "expected unique direct output payload on '{output_port}', got type_key={} rust_type={:?}",
                        payload.type_key(),
                        payload.storage_rust_type_name()
                    ))
                })
            })
            .transpose()
    }

    pub fn tick_if_ready(&mut self) -> Result<Option<ExecutionTelemetry>, EngineError> {
        if self.host.has_pending_inbound() {
            self.tick().map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn run_available(&mut self) -> Result<Option<ExecutionTelemetry>, EngineError> {
        self.tick_if_ready()
    }

    pub fn tick_until_idle(&mut self) -> Result<Option<ExecutionTelemetry>, EngineError> {
        let mut last = None;
        while self.host.has_pending_inbound() {
            last = Some(self.tick()?);
        }
        Ok(last)
    }

    pub fn tick_selected(
        &mut self,
        sinks: impl IntoIterator<Item = RuntimeSink>,
    ) -> Result<ExecutionTelemetry, EngineError> {
        let sinks = sinks.into_iter().collect::<Vec<_>>();
        let slice = self
            .runtime_plan()
            .demand_slice_for_sinks(&sinks)
            .map_err(|err| EngineError::Config(err.to_string()))?;
        let demand = self.runtime_plan().demand_summary_for_slice(&sinks, &slice);
        self.runner
            .executor
            .set_active_nodes_mask(Some(Arc::new(slice.active_nodes.clone())));
        self.runner
            .executor
            .set_active_edges_mask(Some(Arc::new(slice.active_edges.clone())));
        self.runner
            .executor
            .set_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())));
        self.runner
            .executor
            .set_selected_host_output_ports(Some(Arc::new(
                slice
                    .host_output_ports
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>(),
            )));
        let result = self.tick().map(|mut telemetry| {
            telemetry.demand = demand;
            telemetry
        });
        self.runner.executor.set_active_nodes_mask(None);
        self.runner.executor.set_active_edges_mask(None);
        self.runner.executor.set_active_direct_edges_mask(None);
        self.runner.executor.set_selected_host_output_ports(None);
        result
    }

    pub fn profiled_feed_tick_drain_owned<I, O>(
        &mut self,
        input_port: impl Into<String>,
        type_key: impl Into<TypeKey>,
        value: I,
        output_port: impl AsRef<str>,
    ) -> Result<HostGraphStep<O>, EngineError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        let feed_start = Instant::now();
        self.push_as(input_port, type_key, value);
        let feed_duration = feed_start.elapsed();

        let run_start = Instant::now();
        let telemetry = self.run_available()?;
        let run_duration = run_start.elapsed();

        let drain_start = Instant::now();
        let outputs = self.drain_owned(output_port)?;
        let drain_duration = drain_start.elapsed();

        Ok(HostGraphStep {
            outputs,
            metrics: HostGraphStepMetrics {
                feed_duration,
                run_duration,
                drain_duration,
                telemetry,
                node_labels: self.node_labels.clone(),
            },
        })
    }

    pub fn run_executor_once(&mut self) -> Result<RunResult, EngineError> {
        self.runner.run()
    }

    /// Feed one typed input, run until idle, and drain a typed output batch.
    pub fn run_once<I, O>(
        &mut self,
        input: I,
        output_port: impl AsRef<str>,
    ) -> Result<Vec<O>, EngineError>
    where
        I: HostGraphRunInput,
        I::Value: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        let (input_port, type_key, value) = input.into_parts();
        self.push_as(input_port, type_key, value);
        self.tick_until_idle()?;
        self.drain_owned(output_port)
    }

    pub fn take_payload(&self, port: impl AsRef<str>) -> Option<Payload> {
        self.host.try_pop_payload(port)
    }

    pub fn take<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.try_pop(port)
    }

    pub fn take_owned<T>(&self, port: impl AsRef<str>) -> Result<Option<T>, EngineError>
    where
        T: Send + Sync + 'static,
    {
        self.host.try_pop_owned(port).map_err(|payload| {
            EngineError::Config(format!(
                "expected unique payload on host output, got type_key={} rust_type={:?}",
                payload.type_key(),
                payload.storage_rust_type_name()
            ))
        })
    }

    pub fn latest<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.drain(port).into_iter().last()
    }

    pub fn subscribe(&self, port: impl Into<String>) -> HostGraphSubscription {
        HostGraphSubscription {
            host: self.host.clone(),
            port: port.into(),
        }
    }

    pub fn drain_payloads(&self, port: impl AsRef<str>) -> Vec<Payload> {
        self.host.drain_payloads(port)
    }

    pub fn drain_owned<T>(&self, port: impl AsRef<str>) -> Result<Vec<T>, EngineError>
    where
        T: Send + Sync + 'static,
    {
        self.drain_payloads(port)
            .into_iter()
            .map(|payload| {
                payload.try_into_owned::<T>().map_err(|payload| {
                    EngineError::Config(format!(
                        "expected unique payload on host output, got type_key={} rust_type={:?}",
                        payload.type_key(),
                        payload.storage_rust_type_name()
                    ))
                })
            })
            .collect()
    }

    pub fn drain<T>(&self, port: impl AsRef<str>) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.drain(port)
    }
}

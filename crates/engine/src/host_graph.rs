use crate::portable::Arc;
use crate::prelude::*;
use core::fmt;
use core::marker::PhantomData;
use core::time::Duration;

use daedalus_runtime::ExecutionTelemetry;
use daedalus_runtime::executor::{DirectHostRoute, NodeHandler};
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::{
    HostBatchOutcomes, HostBatchRejected, HostBridgeHandle, HostBridgeManager, HostInputBatch,
    ValueSerializerMap,
};
use daedalus_runtime::{RuntimePlan, RuntimePlanExplanation, RuntimeSink, TypeIndex};
use daedalus_transport::{
    FeedOutcome, FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy, TypeKey,
};

use crate::compiled_run::{CompiledRun, RunResult};
use crate::error::EngineError;

mod bindings;
mod drive;
mod frame_overhead;
mod introspect;
#[cfg(any(feature = "threads", all(feature = "std", target_os = "linux")))]
mod multicam;

pub use bindings::{
    HostGraphInput, HostGraphLane, HostGraphOutput, HostGraphPayloadInput, HostGraphPayloadOutput,
    HostGraphRunInput, HostGraphSubscription,
};
pub use drive::{HostGraphDriveExit, HostGraphStopHandle, HostGraphTurn};
pub use frame_overhead::DEFAULT_FRAME_OVERHEAD_WINDOW;
pub(crate) use frame_overhead::FrameOverheadState;

/// In-process graph runner for host-driven applications.
///
/// Prefer the high-level flows first:
///
/// - `run_once(("in", value), "out")` for one input and one output batch.
/// - `bind_input`/`bind_output` for repeated typed feeds and drains.
/// - `bind_lane` plus `run_lane` for hot single-input/single-output routes.
///
/// Lower-level `push`, `tick`, and `drain_*` methods remain available for multi-input,
/// demand-selected, or diagnostic workflows.
///
/// Context inputs (resource state, IMU, ...) that every tick should see: make them held
/// (`set_held_input`) and push a frame with its context atomically (`batch`, `push_batch`).
///
/// Port arguments: write/bind paths (`push*`, `set_*_policy`, `set_latest_*`, `bind_input`,
/// `bind_output`, `subscribe`) take `impl Into<PortId>`, so a reused `PortId` never allocates;
/// read/lookup paths (`take*`, `drain*`, `latest`, `direct_host_route`, `bind_lane`) take
/// `impl AsRef<str>` and look ports up without allocating.
pub struct HostGraph<H: NodeHandler> {
    pub(crate) runner: CompiledRun<H>,
    pub(crate) bridges: HostBridgeManager,
    pub(crate) host: HostBridgeHandle,
    pub(crate) node_labels: Arc<[String]>,
    /// Serializers used by `inspect_payload`; shared with the registry the graph was compiled
    /// from when compiled through a `PluginRegistry`.
    pub(crate) value_serializers: ValueSerializerMap,
    /// The registry's type index (builtins only without a registry): resolves typed feeds and
    /// checks raw payloads fed into the graph.
    pub(crate) types: TypeIndex,
    /// Frame-path overhead recording ([`HostGraph::enable_frame_overhead`]).
    pub(crate) frame_overhead: Option<Box<FrameOverheadState>>,
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
                    .get(idx)
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

    /// The type index typed feeds resolve through (`PluginRegistry::type_index` of the registry
    /// the graph was compiled from; builtins only otherwise).
    pub fn type_index(&self) -> &TypeIndex {
        &self.types
    }

    /// Bind a typed host input once and reuse it across ticks. Resolves `T`'s key now, so an
    /// unknown type fails here rather than on every push.
    pub fn bind_input<T>(&self, port: impl Into<PortId>) -> Result<HostGraphInput<T>, EngineError>
    where
        T: Send + Sync + 'static,
    {
        Ok(HostGraphInput {
            host: self.host.clone(),
            port: port.into(),
            type_key: self.types.key_of::<T>()?,
            _ty: PhantomData,
        })
    }

    /// Bind a raw payload input for dynamic or plugin-driven payloads.
    pub fn bind_payload_input(&self, port: impl Into<PortId>) -> HostGraphPayloadInput {
        HostGraphPayloadInput {
            host: self.host.clone(),
            port: port.into(),
        }
    }

    /// Bind a typed host output once and reuse it across ticks.
    pub fn bind_output<T>(&self, port: impl Into<PortId>) -> HostGraphOutput<T>
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
    pub fn bind_payload_output(&self, port: impl Into<PortId>) -> HostGraphPayloadOutput {
        HostGraphPayloadOutput {
            host: self.host.clone(),
            port: port.into(),
        }
    }

    pub fn set_input_policy(
        &self,
        port: impl Into<PortId>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.host.set_input_policy(port, pressure, freshness)
    }

    pub fn set_output_policy(
        &self,
        port: impl Into<PortId>,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.host.set_output_policy(port, pressure, freshness)
    }

    pub fn set_latest_input(&self, port: impl Into<PortId>) -> Result<(), PolicyValidationError> {
        self.set_input_policy(
            port,
            PressurePolicy::LatestOnly,
            FreshnessPolicy::LatestByTimestamp,
        )
    }

    pub fn set_latest_output(&self, port: impl Into<PortId>) -> Result<(), PolicyValidationError> {
        self.set_output_policy(
            port,
            PressurePolicy::LatestOnly,
            FreshnessPolicy::LatestByTimestamp,
        )
    }

    /// Make host input `port` held: its last pushed value persists across ticks and reaches its
    /// consumers every tick until a push replaces it or [`Self::clear_input`] drops it. Held
    /// pushes never trigger a tick by themselves. See [`HostBridgeHandle::set_held_input`];
    /// graphs can declare it instead (`GraphBuilder::held_input`).
    pub fn set_held_input(&self, port: impl Into<PortId>) {
        self.host.set_held_input(port);
    }

    /// Drop host input `port`'s held value (or queued payloads) without closing it.
    pub fn clear_input(&self, port: impl AsRef<str>) {
        self.host.clear_input(port);
    }

    /// Start an atomic multi-port push: `graph.batch().push("frame", f).push("imu", s).commit()`.
    /// See [`HostBridgeHandle::push_batch`].
    pub fn batch(&self) -> HostInputBatch<'_> {
        self.host.batch()
    }

    /// Push several payloads atomically, so no tick sees part of them; a payload failing the type
    /// check rejects the whole batch. See [`HostBridgeHandle::push_batch`].
    pub fn push_batch<P: Into<PortId>>(
        &self,
        entries: impl IntoIterator<Item = (P, Payload)>,
    ) -> Result<HostBatchOutcomes, HostBatchRejected> {
        self.host.push_batch(entries)
    }

    /// Low-level typed feed under `T`'s key in the graph's registry; an unknown type is
    /// [`FeedOutcome::Rejected`]. Prefer `run_once` for one-shot calls or `bind_input` for hot
    /// loops.
    pub fn push<T>(&self, port: impl Into<PortId>, value: T) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.host.push(port, value)
    }

    /// Low-level typed feed with an explicit transport type key.
    pub fn push_as<T>(
        &self,
        port: impl Into<PortId>,
        type_key: impl Into<TypeKey>,
        value: T,
    ) -> FeedOutcome
    where
        T: Send + Sync + 'static,
    {
        self.host.push_as(port, type_key, value)
    }

    pub fn push_payload(&self, port: impl Into<PortId>, payload: Payload) -> FeedOutcome {
        self.host.feed_payload(port, payload)
    }

    /// Execute one graph tick. Pair with `push`/`drain_*` for advanced multi-input workflows.
    pub fn tick(&mut self) -> Result<ExecutionTelemetry, EngineError> {
        self.frame_commit();
        self.runner.run_telemetry()
    }

    pub fn tick_direct_payload(
        &mut self,
        input_port: impl AsRef<str>,
        payload: Payload,
        output_port: impl AsRef<str>,
    ) -> Result<Option<(ExecutionTelemetry, Option<Payload>)>, EngineError> {
        self.types.check_payload(&payload)?;
        self.frame_commit();
        self.runner
            .executor
            .run_direct_host_payload(input_port.as_ref(), payload, output_port.as_ref())
            .map_err(EngineError::Runtime)
    }

    pub fn direct_host_route(
        &self,
        input_port: impl AsRef<str>,
        output_port: impl AsRef<str>,
    ) -> Option<DirectHostRoute> {
        self.runner
            .executor
            .direct_host_route(input_port.as_ref(), output_port.as_ref())
    }

    /// Bind a direct host lane for repeated single-input/single-output calls.
    ///
    /// The lane resolves the route and `I`'s key once and does not keep the port names, so
    /// ports are looked up by reference like other read paths.
    pub fn bind_lane<I>(
        &self,
        input_port: impl AsRef<str>,
        output_port: impl AsRef<str>,
    ) -> Result<HostGraphLane<I>, EngineError>
    where
        I: Send + Sync + 'static,
    {
        Ok(HostGraphLane {
            route: self.required_direct_route(input_port.as_ref(), output_port.as_ref())?,
            type_key: self.types.key_of::<I>()?,
            _input: PhantomData,
        })
    }

    fn required_direct_route(
        &self,
        input_port: &str,
        output_port: &str,
    ) -> Result<DirectHostRoute, EngineError> {
        self.direct_host_route(input_port, output_port)
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "no direct host route from '{input_port}' to '{output_port}'"
                ))
            })
    }

    pub fn tick_direct_route(
        &mut self,
        route: &DirectHostRoute,
        payload: Payload,
    ) -> Result<(ExecutionTelemetry, Option<Payload>), EngineError> {
        self.types.check_payload(&payload)?;
        self.frame_commit();
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
        self.types.check_payload(&payload)?;
        self.frame_commit();
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
        let payload =
            Payload::owned(lane.type_key.clone(), input).stamp(self.runner.executor.clock());
        self.tick_direct_route_payload(&lane.route, payload)
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
            .map(|payload| into_owned_or_err::<O>(payload, "direct lane output payload"))
            .transpose()
    }

    /// Run one typed value through a direct host route when the graph shape supports it.
    pub fn run_direct_once<I, O>(
        &mut self,
        input_port: impl AsRef<str>,
        output_port: impl AsRef<str>,
        input: I,
    ) -> Result<Option<O>, EngineError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        let (input_port, output_port) = (input_port.as_ref(), output_port.as_ref());
        let route = self.required_direct_route(input_port, output_port)?;
        let payload =
            Payload::owned(self.types.key_of::<I>()?, input).stamp(self.runner.executor.clock());
        let output = self.tick_direct_route_payload(&route, payload)?;
        output
            .map(|payload| {
                into_owned_or_err::<O>(
                    payload,
                    format_args!("direct output payload on '{output_port}'"),
                )
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
            .try_set_active_nodes_mask(Some(Arc::new(slice.active_nodes.clone())))?;
        self.runner
            .executor
            .try_set_active_edges_mask(Some(Arc::new(slice.active_edges.clone())))?;
        self.runner
            .executor
            .try_set_active_direct_edges_mask(Some(Arc::new(slice.direct_edges.clone())))?;
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
        self.runner.executor.try_set_active_nodes_mask(None)?;
        self.runner.executor.try_set_active_edges_mask(None)?;
        self.runner
            .executor
            .try_set_active_direct_edges_mask(None)?;
        self.runner.executor.set_selected_host_output_ports(None);
        result
    }

    pub fn profiled_feed_tick_drain_owned<I, O>(
        &mut self,
        input_port: impl Into<PortId>,
        type_key: impl Into<TypeKey>,
        value: I,
        output_port: impl AsRef<str>,
    ) -> Result<HostGraphStep<O>, EngineError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        let clock = self.runner.executor.clock().clone();
        let feed_start = clock.now();
        rejected(self.push_as(input_port, type_key, value))?;
        let feed_duration = clock.elapsed(feed_start);

        let run_start = clock.now();
        let telemetry = self.run_available()?;
        let run_duration = clock.elapsed(run_start);

        let drain_start = clock.now();
        let outputs = self.drain_owned(output_port)?;
        let drain_duration = clock.elapsed(drain_start);

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
        let (input_port, type_key, value) = input.into_parts(&self.types)?;
        rejected(self.push_as(input_port, type_key, value))?;
        self.tick_until_idle()?;
        self.drain_owned(output_port)
    }

    /// Feed one typed input, run until idle, and return the latest typed output.
    pub fn run_once_latest<I, O>(
        &mut self,
        input: I,
        output_port: impl AsRef<str>,
    ) -> Result<Option<O>, EngineError>
    where
        I: HostGraphRunInput,
        I::Value: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        Ok(self
            .run_once::<I, O>(input, output_port)?
            .into_iter()
            .last())
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
        self.host
            .try_pop_payload(port)
            .map(|payload| into_owned_or_err(payload, "payload on host output"))
            .transpose()
    }

    pub fn latest<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.drain(port).into_iter().last()
    }

    pub fn subscribe(&self, port: impl Into<PortId>) -> HostGraphSubscription {
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
            .map(|payload| into_owned_or_err::<T>(payload, "payload on host output"))
            .collect()
    }

    pub fn drain<T>(&self, port: impl AsRef<str>) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.drain(port)
    }
}

/// Turn a [`FeedOutcome::Rejected`] into an error; queue-policy outcomes (drops, replacements)
/// stay successes.
fn rejected(outcome: FeedOutcome) -> Result<FeedOutcome, EngineError> {
    match outcome {
        FeedOutcome::Rejected(error) => Err((*error).into()),
        outcome => Ok(outcome),
    }
}

/// Take ownership of `payload` as `T`, or report what was expected (`what`) and what was found.
fn into_owned_or_err<T>(payload: Payload, what: impl core::fmt::Display) -> Result<T, EngineError>
where
    T: Send + Sync + 'static,
{
    payload.try_into_owned::<T>().map_err(|payload| {
        EngineError::Config(format!(
            "expected unique {what}, got type_key={} rust_type={:?}",
            payload.type_key(),
            payload.storage_rust_type_name()
        ))
    })
}

use crate::portable::Arc;
use crate::prelude::*;
use daedalus_planner::{ComputeAffinity, NodeRef};

use super::direct_slot::DirectSlotAccess;
use super::{
    ConstInputStore, ExecuteError, ExecutionTelemetry, Executor, ExecutorCore, NodeFailure,
    NodeHandler, ProbeCount, ProbeTime,
};
use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeEdgeTransport, RuntimeNode};

mod edges;
mod fused;
mod host_io;
mod resilience;

use edges::{collect_inputs, publish_outputs};
pub(crate) use fused::Carry;
use fused::FusedOutputs;
#[cfg(feature = "threads")]
pub(crate) use host_io::drain_host_outputs;
pub(crate) use host_io::{HostNodeIo, adapt_host_output, inject_host_inputs, resolve_host_nodes};

/// The executor state a serial run reads and writes apart from its handler. The helpers take
/// this rather than `Executor<H>`, so each is compiled once instead of once per handler type.
pub(crate) struct SerialView<'a> {
    pub(crate) core: &'a mut ExecutorCore,
    pub(crate) nodes: &'a [RuntimeNode],
    pub(crate) edges: &'a [RuntimeEdge],
    pub(crate) edge_transports: &'a [Option<RuntimeEdgeTransport>],
    pub(crate) incoming_edges: &'a [Vec<usize>],
    pub(crate) outgoing_edges: &'a [Vec<usize>],
    pub(crate) const_inputs: &'a ConstInputStore,
    pub(crate) backpressure: &'a BackpressureStrategy,
    pub(crate) direct_slot_access: DirectSlotAccess,
}

/// `exec`'s serial view, for the helpers' calls.
pub(crate) fn view<'a, H: NodeHandler>(exec: &'a mut Executor<'_, H>) -> SerialView<'a> {
    SerialView {
        core: &mut exec.core,
        nodes: &exec.nodes,
        edges: exec.edges,
        edge_transports: exec.edge_transports,
        incoming_edges: &exec.incoming_edges,
        outgoing_edges: &exec.outgoing_edges,
        const_inputs: &exec.const_inputs,
        backpressure: &exec.backpressure,
        direct_slot_access: exec.direct_slot_access,
    }
}

pub fn run<H: NodeHandler>(mut exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError> {
    run_with_boundaries(&mut exec)
}

/// Inject host inputs and run the whole schedule on `exec`, which stays usable for draining host
/// outputs afterwards.
pub(crate) fn run_with_boundaries<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    run_with_boundaries_timed(exec, None)
}

/// [`run_with_boundaries`], adding each node's wall time to its segment in `costs`.
pub(crate) fn run_with_boundaries_timed<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    costs: Option<SegmentCosts<'_>>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    let _scope = super::runtime_alloc_scope();
    let inject_start = exec
        .core
        .run_config
        .frame_probe
        .is_some()
        .then(|| exec.core.clock.now());
    inject_host_inputs(exec)?;
    if let (Some(probe), Some(start)) = (&exec.core.run_config.frame_probe, inject_start) {
        probe.add_time(ProbeTime::Inject, exec.core.clock.elapsed(start));
    }
    let order = exec.schedule_order;
    run_order_telemetry_timed(exec, order, costs, true)
}

/// Per-segment wall-time accumulators (ns) and the segment of each node.
pub(crate) struct SegmentCosts<'c> {
    pub(crate) segment_of: &'c [usize],
    pub(crate) costs: &'c mut [u64],
}

/// Run `order` on `exec`, leaving its host outputs queued, and add its telemetry to the
/// executor's: a parallel worker runs several segments this way, then its telemetry is taken once.
#[cfg(feature = "threads")]
pub(crate) fn run_order<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
) -> Result<(), ExecuteError> {
    run_order_timed(exec, order, None, false)
}

/// `run_order` as a whole run: its telemetry is finished and taken. With `drain_outputs`, the
/// host outputs it produced are drained into that telemetry first.
pub(crate) fn run_order_telemetry<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
    drain_outputs: bool,
) -> Result<ExecutionTelemetry, ExecuteError> {
    run_order_telemetry_timed(exec, order, None, drain_outputs)
}

fn run_order_telemetry_timed<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
    costs: Option<SegmentCosts<'_>>,
    drain_outputs: bool,
) -> Result<ExecutionTelemetry, ExecuteError> {
    run_order_timed(exec, order, costs, drain_outputs)?;
    Ok(finish_telemetry(&mut exec.core))
}

/// Finish the telemetry a run recorded into `core`: its unattributed time, its group totals (once,
/// over the nodes the run recorded), and take it, keeping the metrics level for the next run.
pub(crate) fn finish_telemetry(core: &mut ExecutorCore) -> ExecutionTelemetry {
    core.telemetry.recompute_unattributed_runtime_duration();
    core.telemetry.aggregate_group_ids(&core.node_groups);
    core.telemetry.node_health = core.node_health.reports(&core.node_ids, &core.clock);
    #[cfg(feature = "gpu")]
    {
        core.telemetry.gpu_accesses = core
            .gpu_source
            .as_ref()
            .map_or(0, |source| source.accesses());
    }
    let level = core.telemetry.metrics_level;
    let telemetry = core::mem::take(&mut core.telemetry);
    core.telemetry.metrics_level = level;
    telemetry
}

/// Run `order` on `exec`'s core, adding to its telemetry (not finished, see [`finish_telemetry`]).
fn run_order_timed<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
    mut costs: Option<SegmentCosts<'_>>,
    drain_outputs: bool,
) -> Result<(), ExecuteError> {
    let handler = Arc::clone(&exec.handler);
    let nodes = Arc::clone(&exec.nodes);
    let schedule_fusion = exec.schedule.fusion.clone();
    let mut v = view(exec);
    let graph_span = crate::trace::debug_span!(
        target: "daedalus_runtime::executor",
        "runtime_graph_run",
        nodes = order.len(),
        fail_fast = v.core.run_config.fail_fast,
        metrics_level = ?v.core.run_config.metrics_level,
    );
    let _graph_span = graph_span.enter();
    let _scope = super::runtime_alloc_scope();
    let probe = v.core.run_config.frame_probe.clone();
    let collect_basic_metrics =
        cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_basic();
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed();
    let collect_trace = cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_trace();
    let clock = v.core.clock.clone();
    let graph_start = (collect_basic_metrics || collect_trace).then(|| clock.now());
    let contexts = v.core.contexts.clone();
    if collect_basic_metrics {
        v.core.telemetry.node_metrics.reserve_nodes(nodes.len());
    }
    // Profile and trace levels record each edge's enqueue and dequeue, so they run unfused.
    let fusion = schedule_fusion.filter(|_| {
        !(cfg!(feature = "metrics")
            && (v.core.run_config.metrics_level.is_profile() || collect_trace))
    });
    let mut carry = ::core::mem::take(&mut v.core.fused_carry);
    let blocking = v.core.run_config.policies.blocking();
    // Without recovery policies every node fails as before: skip the per-node lookups.
    let recovering = v.core.run_config.policies.any_recovery();
    let uniform = v.core.run_config.policies.uniform_recovery();

    for (pos, node_ref) in order.iter().copied().enumerate() {
        let node_idx = node_ref.0;
        if !node_is_active(&v, node_idx) {
            continue;
        }
        let Some(node) = nodes.get(node_idx) else {
            continue;
        };
        let node_span = crate::trace::debug_span!(
            target: "daedalus_runtime::executor",
            "runtime_node_run",
            node_index = node_idx,
            node_id = %node.id,
            compute = ?node.compute,
        );
        let _node_span = node_span.enter();
        let cost_start = costs.is_some().then(|| clock.now());

        let wait_all = v
            .core
            .required_inputs
            .get(node_idx)
            .is_some_and(|required| required.wait_all);
        if wait_all && !required_inputs_held(&v, node_idx) {
            // `fire = "all"`: wait without popping, so what arrived stays queued for a later tick.
            crate::trace::trace!(target: "daedalus_runtime::executor", node_id = %node.id, "node waiting for inputs");
            continue;
        }
        if let Some(blocking) = blocking
            .as_ref()
            .and_then(super::NodePolicies::blocking_edges)
            && carry.is_empty()
            && resilience::output_blocked(&v, blocking, fusion.as_deref(), node_idx)
        {
            // Blocking backpressure: a bounded output queue is full, so leave the inputs queued
            // and run again once the consumer has drained it.
            crate::trace::trace!(target: "daedalus_runtime::executor", node_id = %node.id, "node deferred by blocking backpressure");
            v.core.telemetry.backpressure_deferrals += 1;
            continue;
        }
        let recovery = if recovering {
            uniform.unwrap_or_else(|| v.core.run_config.policies.recovery_of(node_idx))
        } else {
            super::NodeRecovery::Fail
        };
        let collect_start = probe.is_some().then(|| clock.now());
        let inputs = collect_inputs(&mut v, node_idx, wait_all, &mut carry)?;
        debug_assert!(carry.is_empty(), "fused payloads go to the next node");
        // A backed-off node skips its frame: its inputs are dropped, and its consumers see no
        // output this tick. Once its delay passes, the next attempt runs.
        let Some(probing) = v.core.node_health.admit(node_idx, &v.core.clock) else {
            v.core.telemetry.recovery_mut().backoff_skips += 1;
            v.core.port_pool.recycle(inputs);
            continue;
        };
        if let (Some(probe), Some(start)) = (&probe, collect_start) {
            probe.add_time(ProbeTime::Collect, clock.elapsed(start));
        }
        if !required_inputs_ready(&v, node_idx, &inputs) {
            // Not ready this tick: a connected required input has no value (its producer was
            // skipped or pushed nothing). Optional inputs never block.
            crate::trace::trace!(target: "daedalus_runtime::executor", node_id = %node.id, "node not ready");
            v.core.port_pool.recycle(inputs);
            continue;
        }

        match node.compute {
            ComputeAffinity::CpuOnly => {
                v.core.telemetry.cpu_segments = v.core.telemetry.cpu_segments.saturating_add(1);
            }
            ComputeAffinity::GpuPreferred => {
                if v.core.gpu_attached() {
                    v.core.telemetry.gpu_segments = v.core.telemetry.gpu_segments.saturating_add(1);
                } else {
                    v.core.telemetry.gpu_fallbacks =
                        v.core.telemetry.gpu_fallbacks.saturating_add(1);
                    v.core.telemetry.warnings.push(format!(
                        "gpu_preferred node {} executed on CPU because no GPU handle is available",
                        node.id
                    ));
                    v.core.telemetry.cpu_segments = v.core.telemetry.cpu_segments.saturating_add(1);
                }
            }
            ComputeAffinity::GpuRequired => {
                if v.core.gpu_attached() {
                    v.core.telemetry.gpu_segments = v.core.telemetry.gpu_segments.saturating_add(1);
                } else {
                    return Err(ExecuteError::GpuUnavailable {
                        segment: vec![NodeRef(node_idx)],
                    });
                }
            }
        }

        if collect_detailed_metrics {
            v.core.telemetry.start_node_call(node_idx);
        }
        let node_start =
            (collect_basic_metrics || collect_trace || probe.is_some()).then(|| clock.now());
        let cpu_start = v
            .core
            .run_config
            .debug_config
            .node_cpu_time
            .then(super::thread_cpu_time)
            .flatten();
        let perf_guard = if crate::perf::node_perf_enabled(v.core.run_config.debug_config) {
            crate::perf::PerfCounterGuard::start().ok()
        } else {
            None
        };
        let consts = super::node_consts(v.const_inputs, node_idx);
        let retry_inputs = recovering
            .then(|| resilience::retry_inputs(recovery, &v.core.port_pool, &inputs))
            .flatten();
        let mut io = v.core.node_io(node_idx, inputs, consts.as_ref());
        // Output reuse (`reuse`): a reusable node whose inputs are unchanged replays its last
        // outputs instead of running. Without a reuse table this is one `None` check.
        let reuse = v.core.reuse.clone();
        let slot = reuse.as_deref().and_then(|table| table.slot(node_idx));
        let mut stamp = None;
        let replayed = match slot {
            Some(slot) if slot.replay(&mut io) => true,
            Some(slot) => {
                stamp = Some(slot.stamp(&io));
                false
            }
            None => false,
        };
        let ctx = &contexts[node_idx];
        if collect_basic_metrics {
            v.core.state.clear_node_custom_metrics(&ctx.node_id);
        }

        let handler_start = (collect_detailed_metrics || probe.is_some()).then(|| clock.now());
        let handler_span = crate::trace::debug_span!(
            target: "daedalus_runtime::executor",
            "runtime_handler_call",
            node_index = node_idx,
            node_id = %node.id,
        );
        let run_result = if replayed {
            Ok(())
        } else {
            let _handler_span = handler_span.enter();
            let _scope = super::node_alloc_scope();
            if !recovering {
                handler.run(node, ctx, &mut io)
            } else {
                let result = resilience::run_handler(
                    &*handler,
                    node,
                    ctx,
                    &mut io,
                    recovery,
                    &mut v.core.telemetry,
                );
                match retry_inputs {
                    Some(saved) => resilience::retry(
                        &mut v,
                        &*handler,
                        node_idx,
                        node,
                        ctx,
                        consts.as_ref(),
                        &mut io,
                        result,
                        saved,
                        recovery,
                    ),
                    None => result,
                }
            }
        };
        if let Some(handler_start) = handler_start {
            let handler_duration = clock.elapsed(handler_start);
            if collect_detailed_metrics {
                v.core
                    .telemetry
                    .record_node_handler_duration(node_idx, handler_duration);
            }
            if let Some(probe) = &probe {
                probe.add_time(ProbeTime::Handlers, handler_duration);
            }
        }
        let flush_result = if run_result.is_ok() {
            io.flush().err()
        } else {
            None
        };
        let outputs = io.take_outputs();

        if let Err(error) = run_result {
            if let Some(slot) = slot {
                slot.forget();
            }
            resilience::handle_failure(&mut v, node_idx, &node.id, error, recovery)?;
        } else if let Some(error) = flush_result {
            if let Some(slot) = slot {
                slot.forget();
            }
            let recovery = resilience::after_delivery(recovery);
            resilience::handle_failure(&mut v, node_idx, &node.id, error, recovery)?;
        } else {
            if let (Some(slot), Some(stamp)) = (slot, stamp.take()) {
                slot.store(stamp, outputs.clone());
            }
            if replayed {
                v.core.telemetry.nodes_reused = v.core.telemetry.nodes_reused.saturating_add(1);
            }
            let fused = FusedOutputs::of(&v, fusion.as_deref(), node_idx, order, pos, &mut carry);
            match publish_outputs(&mut v, node_idx, outputs, fused) {
                Ok(()) if probing => v.core.node_health.on_success(node_idx),
                Ok(()) => {}
                Err(error) => {
                    if let Some(slot) = slot {
                        slot.forget();
                    }
                    let recovery = resilience::after_delivery(recovery);
                    resilience::handle_failure(&mut v, node_idx, &node.id, error, recovery)?;
                }
            }
        }

        let elapsed = node_start
            .map(|start| clock.elapsed(start))
            .unwrap_or_default();
        if let Some(probe) = &probe {
            probe.add_time(ProbeTime::NodeRuns, elapsed);
            probe.add_count(ProbeCount::Nodes, 1);
        }
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = super::thread_cpu_time()
        {
            v.core
                .telemetry
                .record_node_cpu_duration(node_idx, cpu_end.saturating_sub(cpu_start));
        }
        if let Some(perf_guard) = perf_guard
            && let Ok(sample) = perf_guard.finish()
        {
            v.core.telemetry.record_node_perf(node_idx, sample);
        }
        if collect_basic_metrics {
            let metrics = v.core.state.drain_node_custom_metrics(&ctx.node_id);
            v.core
                .telemetry
                .record_node_custom_metrics(node_idx, metrics);
            v.core.telemetry.record_node_duration(node_idx, elapsed);
        }
        if collect_trace
            && let (Some(graph_start), Some(node_start)) =
                (graph_start.as_ref(), node_start.as_ref())
        {
            v.core.telemetry.record_trace_event(
                node_idx,
                node_start.saturating_duration_since(*graph_start),
                elapsed,
            );
        }
        v.core.telemetry.nodes_executed = v.core.telemetry.nodes_executed.saturating_add(1);
        if let (Some(start), Some(costs)) = (cost_start, costs.as_mut())
            && let Some(cost) = costs
                .segment_of
                .get(node_idx)
                .and_then(|&segment| costs.costs.get_mut(segment))
        {
            *cost += clock.elapsed(start).as_nanos() as u64;
        }
    }
    // Back for the next run, with its buffer (a run that fails early drops it).
    carry.clear();
    v.core.fused_carry = carry;

    if let Some(graph_start) = graph_start {
        // The longest segment's time for a parallel worker, which runs several segments on one
        // telemetry: the merged run keeps the longest.
        v.core.telemetry.graph_duration = v
            .core
            .telemetry
            .graph_duration
            .max(clock.elapsed(graph_start));
    }
    // Before the telemetry is finished: the queue waits of the outputs this tick hands to the host
    // belong to this tick's telemetry, and a recycled telemetry would drop them otherwise.
    if drain_outputs {
        host_io::drain(&mut v)?;
    }
    Ok(())
}

fn node_is_active(v: &SerialView<'_>, node_idx: usize) -> bool {
    if v.core.host_bridges.get(node_idx).copied().unwrap_or(false) {
        return false;
    }
    v.core
        .run_config
        .active_nodes
        .as_deref()
        .and_then(|mask| mask.get(node_idx).copied())
        .unwrap_or(true)
}

/// Whether every connected required input of `node_idx` received a value this tick.
fn required_inputs_ready(
    v: &SerialView<'_>,
    node_idx: usize,
    inputs: &[crate::io::NodePort],
) -> bool {
    let Some(required) = v.core.required_inputs.get(node_idx) else {
        return true;
    };
    required.edges.iter().all(|&edge_idx| {
        !edge_is_active(v, edge_idx)
            || v.edges.get(edge_idx).is_none_or(|edge| {
                let port = edge.target_port_id();
                inputs.iter().any(|(name, _)| name == port)
            })
    })
}

/// Whether every active required input edge of `node_idx` holds a value (peeked, nothing
/// popped), for [`NodeFire::All`](crate::plan::NodeFire::All) nodes.
fn required_inputs_held(v: &SerialView<'_>, node_idx: usize) -> bool {
    let Some(required) = v.core.required_inputs.get(node_idx) else {
        return true;
    };
    required.edges.iter().all(|&edge_idx| {
        !edge_is_active(v, edge_idx)
            || if edge_uses_direct_slot(v, edge_idx) {
                v.core
                    .direct_slots
                    .get(edge_idx)
                    .is_some_and(|slot| slot.access(v.direct_slot_access).occupied())
            } else {
                super::queue::edge_has_payload(edge_idx, &v.core.queues)
            }
    })
}

fn edge_is_active(v: &SerialView<'_>, edge_idx: usize) -> bool {
    v.core
        .run_config
        .active_edges
        .as_deref()
        .and_then(|mask| mask.get(edge_idx).copied())
        .unwrap_or(true)
}

fn edge_uses_direct_slot(v: &SerialView<'_>, edge_idx: usize) -> bool {
    v.core
        .run_config
        .active_direct_edges
        .as_deref()
        .and_then(|mask| mask.get(edge_idx).copied())
        .unwrap_or_else(|| v.core.direct_edges.get(edge_idx).copied().unwrap_or(false))
}

fn record_failure(
    telemetry: &mut ExecutionTelemetry,
    node_idx: usize,
    node_id: &str,
    error: &super::NodeError,
) {
    telemetry.errors.push(NodeFailure {
        node_idx,
        node_id: node_id.to_string(),
        code: error.code().to_string(),
        message: error.to_string(),
    });
}

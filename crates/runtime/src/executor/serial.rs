use daedalus_core::platform::Instant;

use daedalus_planner::{ComputeAffinity, NodeRef};

use crate::state::ExecutionContext;

use super::{ExecuteError, ExecutionTelemetry, Executor, NodeFailure, NodeHandler};

mod edges;
mod host_io;

use edges::{collect_inputs, publish_outputs};
pub(crate) use host_io::{HostNodeIo, drain_host_outputs, inject_host_inputs, resolve_host_nodes};

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
    inject_host_inputs(exec)?;
    let order = exec.schedule_order;
    run_order_timed(exec, order, costs).map(|mut telemetry| {
        telemetry.recompute_unattributed_runtime_duration();
        telemetry
    })
}

/// Per-segment wall-time accumulators (ns) and the segment of each node.
pub(crate) struct SegmentCosts<'c> {
    pub(crate) segment_of: &'c [usize],
    pub(crate) costs: &'c mut [u64],
}

pub(crate) fn run_order<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
) -> Result<ExecutionTelemetry, ExecuteError> {
    run_order_timed(exec, order, None)
}

fn run_order_timed<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
    mut costs: Option<SegmentCosts<'_>>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    let graph_span = tracing::debug_span!(
        target: "daedalus_runtime::executor",
        "runtime_graph_run",
        nodes = order.len(),
        fail_fast = exec.core.run_config.fail_fast,
        metrics_level = ?exec.core.run_config.metrics_level,
    );
    let _graph_span = graph_span.enter();
    let collect_basic_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_basic();
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let collect_trace = cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_trace();
    let graph_start = (collect_basic_metrics || collect_trace).then(Instant::now);
    let mut first_error = None;
    let nodes = exec.nodes.clone();
    if collect_basic_metrics {
        exec.core.telemetry.node_metrics.reserve_nodes(nodes.len());
    }

    for node_ref in order.iter().copied() {
        let node_idx = node_ref.0;
        if !node_is_active(exec, node_idx) {
            continue;
        }
        let Some(node) = nodes.get(node_idx) else {
            continue;
        };
        let node_span = tracing::debug_span!(
            target: "daedalus_runtime::executor",
            "runtime_node_run",
            node_index = node_idx,
            node_id = %node.id,
            compute = ?node.compute,
        );
        let _node_span = node_span.enter();
        let cost_start = costs.is_some().then(Instant::now);

        let inputs = collect_inputs(exec, node_idx)?;
        if !required_inputs_ready(exec, node_idx, &inputs) {
            // Not ready this tick: a connected required input has no value (its producer was
            // skipped or pushed nothing). Optional inputs never block.
            tracing::trace!(target: "daedalus_runtime::executor", node_id = %node.id, "node not ready");
            crate::io::recycle_ports(inputs);
            continue;
        }

        match node.compute {
            ComputeAffinity::CpuOnly => {
                exec.core.telemetry.cpu_segments =
                    exec.core.telemetry.cpu_segments.saturating_add(1);
            }
            ComputeAffinity::GpuPreferred => {
                if exec.core.gpu_available {
                    exec.core.telemetry.gpu_segments =
                        exec.core.telemetry.gpu_segments.saturating_add(1);
                } else {
                    exec.core.telemetry.gpu_fallbacks =
                        exec.core.telemetry.gpu_fallbacks.saturating_add(1);
                    exec.core.telemetry.warnings.push(format!(
                        "gpu_preferred node {} executed on CPU because no GPU handle is available",
                        node.id
                    ));
                    exec.core.telemetry.cpu_segments =
                        exec.core.telemetry.cpu_segments.saturating_add(1);
                }
            }
            ComputeAffinity::GpuRequired => {
                if exec.core.gpu_available {
                    exec.core.telemetry.gpu_segments =
                        exec.core.telemetry.gpu_segments.saturating_add(1);
                } else {
                    return Err(ExecuteError::GpuUnavailable {
                        segment: vec![NodeRef(node_idx)],
                    });
                }
            }
        }

        if collect_detailed_metrics {
            exec.core.telemetry.start_node_call(node_idx);
        }
        let node_start = (collect_basic_metrics || collect_trace).then(Instant::now);
        let cpu_start = exec
            .core
            .run_config
            .debug_config
            .node_cpu_time
            .then(super::thread_cpu_time)
            .flatten();
        let perf_guard = if crate::perf::node_perf_enabled(exec.core.run_config.debug_config) {
            crate::perf::PerfCounterGuard::start().ok()
        } else {
            None
        };
        let mut io = exec.core.node_io(node_idx, inputs);
        let ctx = ExecutionContext {
            state: exec.core.state.clone(),
            node_id: exec.core.node_ids[node_idx].clone(),
            metadata: exec.core.node_metadata[node_idx].clone(),
            graph_metadata: exec.core.graph_metadata.clone(),
            capabilities: exec.core.capabilities.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.core.gpu.clone(),
        };
        if collect_basic_metrics {
            exec.core.state.clear_node_custom_metrics(&node.id);
        }

        let handler_start = collect_detailed_metrics.then(Instant::now);
        let handler_span = tracing::debug_span!(
            target: "daedalus_runtime::executor",
            "runtime_handler_call",
            node_index = node_idx,
            node_id = %node.id,
        );
        let run_result = {
            let _handler_span = handler_span.enter();
            exec.handler.run(node, &ctx, &mut io)
        };
        if let Some(handler_start) = handler_start {
            exec.core
                .telemetry
                .record_node_handler_duration(node_idx, handler_start.elapsed());
        }
        let flush_result = if run_result.is_ok() {
            io.flush().err()
        } else {
            None
        };
        let outputs = io.take_outputs();

        if let Err(error) = run_result {
            record_failure(&mut exec.core.telemetry, node_idx, &node.id, &error);
            if exec.core.run_config.fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error,
                });
            }
            first_error.get_or_insert_with(|| ExecuteError::HandlerFailed {
                node: node.id.clone(),
                error,
            });
        } else if let Some(error) = flush_result {
            record_failure(&mut exec.core.telemetry, node_idx, &node.id, &error);
            if exec.core.run_config.fail_fast {
                return Err(ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error,
                });
            }
            first_error.get_or_insert_with(|| ExecuteError::HandlerFailed {
                node: node.id.clone(),
                error,
            });
        } else {
            if let Err(error) = publish_outputs(exec, node_idx, outputs) {
                record_failure(&mut exec.core.telemetry, node_idx, &node.id, &error);
                if exec.core.run_config.fail_fast {
                    return Err(ExecuteError::HandlerFailed {
                        node: node.id.clone(),
                        error,
                    });
                }
                first_error.get_or_insert_with(|| ExecuteError::HandlerFailed {
                    node: node.id.clone(),
                    error,
                });
            }
        }

        let elapsed = node_start
            .as_ref()
            .map(Instant::elapsed)
            .unwrap_or_default();
        if let Some(cpu_start) = cpu_start
            && let Some(cpu_end) = super::thread_cpu_time()
        {
            exec.core
                .telemetry
                .record_node_cpu_duration(node_idx, cpu_end.saturating_sub(cpu_start));
        }
        if let Some(perf_guard) = perf_guard
            && let Ok(sample) = perf_guard.finish()
        {
            exec.core.telemetry.record_node_perf(node_idx, sample);
        }
        if collect_basic_metrics {
            let metrics = exec.core.state.drain_node_custom_metrics(&node.id);
            exec.core
                .telemetry
                .record_node_custom_metrics(node_idx, metrics);
            exec.core.telemetry.record_node_duration(node_idx, elapsed);
        }
        if collect_trace
            && let (Some(graph_start), Some(node_start)) =
                (graph_start.as_ref(), node_start.as_ref())
        {
            exec.core.telemetry.record_trace_event(
                node_idx,
                node_start.saturating_duration_since(*graph_start),
                elapsed,
            );
        }
        exec.core.telemetry.nodes_executed = exec.core.telemetry.nodes_executed.saturating_add(1);
        if let (Some(start), Some(costs)) = (cost_start, costs.as_mut())
            && let Some(cost) = costs
                .segment_of
                .get(node_idx)
                .and_then(|&segment| costs.costs.get_mut(segment))
        {
            *cost += start.elapsed().as_nanos() as u64;
        }
    }

    if let Some(graph_start) = graph_start {
        exec.core.telemetry.graph_duration = graph_start.elapsed();
    }
    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    exec.core.telemetry.aggregate_groups(&nodes);

    if exec.core.run_config.fail_fast
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(std::mem::take(&mut exec.core.telemetry))
}

fn node_is_active<H: NodeHandler>(exec: &Executor<'_, H>, node_idx: usize) -> bool {
    if exec
        .nodes
        .get(node_idx)
        .is_some_and(super::is_host_bridge_node)
    {
        return false;
    }
    exec.core
        .run_config
        .active_nodes
        .as_deref()
        .and_then(|mask| mask.get(node_idx).copied())
        .unwrap_or(true)
}

/// Whether every connected required input of `node_idx` received a value this tick.
fn required_inputs_ready<H: NodeHandler>(
    exec: &Executor<'_, H>,
    node_idx: usize,
    inputs: &[crate::io::NodePort],
) -> bool {
    let Some(required) = exec.core.required_inputs.get(node_idx) else {
        return true;
    };
    required.iter().all(|&edge_idx| {
        !edge_is_active(exec, edge_idx)
            || exec.edges.get(edge_idx).is_none_or(|edge| {
                let port = edge.target_port_id();
                inputs.iter().any(|(name, _)| name == port)
            })
    })
}

fn edge_is_active<H: NodeHandler>(exec: &Executor<'_, H>, edge_idx: usize) -> bool {
    exec.core
        .run_config
        .active_edges
        .as_deref()
        .and_then(|mask| mask.get(edge_idx).copied())
        .unwrap_or(true)
}

fn edge_uses_direct_slot<H: NodeHandler>(exec: &Executor<'_, H>, edge_idx: usize) -> bool {
    exec.core
        .run_config
        .active_direct_edges
        .as_deref()
        .and_then(|mask| mask.get(edge_idx).copied())
        .unwrap_or_else(|| exec.core.direct_edges.contains(&edge_idx))
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

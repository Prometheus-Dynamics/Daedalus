use std::time::Instant;

use daedalus_planner::{ComputeAffinity, NodeRef};
use daedalus_transport::{AdaptRequest, Payload};
use smallvec::SmallVec;

use crate::handles::PortId;
use crate::io::NodeIo;
use crate::plan::RuntimeEdgePolicy;
use crate::state::ExecutionContext;

use super::queue::{ApplyPolicyOwnedArgs, apply_policy_owned, pop_edge};
use super::serial_direct_slot::{pop_direct_edge, push_direct_edge};
use super::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, ExecuteError, ExecutionTelemetry,
    Executor, NodeFailure, NodeHandler,
};

pub fn run<H: NodeHandler>(exec: Executor<'_, H>) -> Result<ExecutionTelemetry, ExecuteError> {
    let order = exec.schedule_order.to_vec();
    run_with_boundaries(exec, &order)
}

pub(crate) fn run_with_boundaries<H: NodeHandler>(
    mut exec: Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
) -> Result<ExecutionTelemetry, ExecuteError> {
    inject_host_inputs(&mut exec)?;
    let result = run_order(exec, order);
    result.map(|mut telemetry| {
        telemetry.recompute_unattributed_runtime_duration();
        telemetry
    })
}

pub(crate) fn run_order<H: NodeHandler>(
    mut exec: Executor<'_, H>,
    order: &[daedalus_planner::NodeRef],
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

    for node_ref in order.iter().copied() {
        let node_idx = node_ref.0;
        if !node_is_active(&exec, node_idx) {
            continue;
        }
        let Some(node) = exec.nodes.get(node_idx).cloned() else {
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
        let inputs = collect_inputs(&mut exec, node_idx)?;
        let mut io =
            NodeIo::from_inputs(inputs).with_const_coercers(exec.core.const_coercers.clone());
        let ctx = ExecutionContext {
            state: exec.core.state.clone(),
            node_id: node.id.clone().into(),
            metadata: exec.core.node_metadata[node_idx].clone(),
            graph_metadata: exec.core.graph_metadata.clone(),
            capabilities: exec.core.capabilities.clone(),
            #[cfg(feature = "gpu")]
            gpu: exec.core.gpu.clone(),
        };
        if collect_basic_metrics {
            let _ = exec.core.state.clear_node_custom_metrics(&node.id);
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
            exec.handler.run(&node, &ctx, &mut io)
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
            if let Err(error) = publish_outputs(&mut exec, node_idx, outputs) {
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
            match exec.core.state.drain_node_custom_metrics(&node.id) {
                Ok(metrics) => exec
                    .core
                    .telemetry
                    .record_node_custom_metrics(node_idx, metrics),
                Err(error) => exec.core.telemetry.warnings.push(format!(
                    "custom metrics unavailable for node {}: {error}",
                    node.id
                )),
            }
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
    }

    if let Some(graph_start) = graph_start {
        exec.core.telemetry.graph_duration = graph_start.elapsed();
    }
    exec.core
        .telemetry
        .recompute_unattributed_runtime_duration();
    let nodes = exec.nodes.clone();
    exec.core.telemetry.aggregate_groups(&nodes);

    if exec.core.run_config.fail_fast
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(std::mem::take(&mut exec.core.telemetry))
}

pub(crate) fn run_fused_linear<H: NodeHandler>(
    exec: Executor<'_, H>,
) -> Result<ExecutionTelemetry, ExecuteError> {
    run(exec)
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

pub(crate) fn inject_host_inputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
) -> Result<(), ExecuteError> {
    let Some(bridges) = exec.core.host_bridges.clone() else {
        return Ok(());
    };
    let host_nodes: Vec<_> = exec.schedule.host_nodes.iter().copied().collect();
    for node_ref in host_nodes {
        let Some(node) = exec.nodes.get(node_ref.0) else {
            continue;
        };
        let node_id = node.id.clone();
        let alias = node.label.as_deref().unwrap_or(&node.id);
        let Some(handle) = bridges.handle(alias) else {
            continue;
        };
        let outgoing: SmallVec<[usize; 4]> = exec
            .outgoing_edges
            .get(node_ref.0)
            .map(|edges| edges.iter().copied().collect())
            .unwrap_or_default();
        let active_ports = outgoing.iter().filter_map(|edge_idx| {
            let edge = exec.edges.get(*edge_idx)?;
            (edge_is_active(exec, *edge_idx) && node_is_active(exec, edge.to().0))
                .then(|| PortId::from(edge.source_port()))
        });
        let active_ports = active_ports.fold(SmallVec::<[PortId; 4]>::new(), |mut ports, port| {
            if !ports.iter().any(|seen| seen == &port) {
                ports.push(port);
            }
            ports
        });
        for inbound in handle.take_inbound_for_ports_small(&active_ports) {
            let matching_edges: SmallVec<[(usize, RuntimeEdgePolicy); 4]> = outgoing
                .iter()
                .copied()
                .filter_map(|edge_idx| {
                    let edge = exec.edges.get(edge_idx)?;
                    (edge.source_port() == inbound.port.as_str()
                        && edge_is_active(exec, edge_idx)
                        && node_is_active(exec, edge.to().0))
                    .then(|| (edge_idx, edge.policy().clone()))
                })
                .collect();
            let last_edge = matching_edges.len().saturating_sub(1);
            let mut payload_slot = Some(CorrelatedPayload::from_edge(inbound.payload));
            for (idx, (edge_idx, policy)) in matching_edges.into_iter().enumerate() {
                let cloned_payload = idx != last_edge;
                let payload = if cloned_payload {
                    let Some(payload) = payload_slot.as_ref() else {
                        tracing::error!(
                            edge_idx,
                            "host payload slot unexpectedly empty before clone"
                        );
                        continue;
                    };
                    payload.clone()
                } else {
                    let Some(payload) = payload_slot.take() else {
                        tracing::error!(
                            edge_idx,
                            "host payload slot unexpectedly empty before handoff"
                        );
                        continue;
                    };
                    payload
                };
                if exec.core.run_config.metrics_level.is_detailed() {
                    exec.core.telemetry.record_edge_handoff(
                        edge_idx,
                        payload.inner.is_storage_unique(),
                        cloned_payload,
                        0,
                    );
                }
                if edge_uses_direct_slot(exec, edge_idx) {
                    push_direct_edge(exec, edge_idx, payload);
                    continue;
                }
                let queues = exec.core.queues.clone();
                let warnings_seen = exec.core.warnings_seen.clone();
                let data_size_inspectors = exec.core.data_size_inspectors.clone();
                let backpressure = exec.backpressure.clone();
                apply_policy_owned(ApplyPolicyOwnedArgs {
                    edge_idx,
                    policy: &policy,
                    payload,
                    queues: &queues,
                    warnings_seen: &warnings_seen,
                    telem: &mut exec.core.telemetry,
                    warning_label: None,
                    backpressure,
                    data_size_inspectors: &data_size_inspectors,
                })
                .map_err(|error| ExecuteError::HandlerFailed {
                    node: node_id.clone(),
                    error,
                })?;
            }
        }
    }
    Ok(())
}

pub(crate) fn drain_host_outputs<H: NodeHandler>(exec: &mut Executor<'_, H>) {
    let Some(bridges) = exec.core.host_bridges.clone() else {
        return;
    };
    let host_nodes: Vec<_> = exec.schedule.host_nodes.iter().copied().collect();
    for node_ref in host_nodes {
        let Some(node) = exec.nodes.get(node_ref.0) else {
            continue;
        };
        let alias = node.label.as_deref().unwrap_or(&node.id);
        let Some(handle) = bridges.handle(alias) else {
            continue;
        };
        let incoming: SmallVec<[usize; 4]> = exec
            .incoming_edges
            .get(node_ref.0)
            .map(|edges| edges.iter().copied().collect())
            .unwrap_or_default();
        for edge_idx in incoming {
            let Some(edge) = exec.edges.get(edge_idx) else {
                continue;
            };
            let to_port = edge.target_port();
            if !edge_is_active(exec, edge_idx) {
                continue;
            }
            if exec
                .core
                .run_config
                .selected_host_output_ports
                .as_ref()
                .is_some_and(|ports| !ports.contains(to_port))
            {
                continue;
            }
            if edge_uses_direct_slot(exec, edge_idx) {
                while let Some(payload) = pop_direct_edge(exec, edge_idx) {
                    handle.push_outbound_ref(to_port, payload.inner);
                }
                continue;
            }
            while let Some(payload) =
                pop_edge(edge_idx, &exec.core.queues, &exec.core.data_size_inspectors)
            {
                handle.push_outbound_ref(to_port, payload.inner);
            }
        }
    }
}

fn collect_inputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_idx: usize,
) -> Result<Vec<(String, CorrelatedPayload)>, ExecuteError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (exec.core.run_config.metrics_level.is_profile()
            || exec.core.run_config.metrics_level.is_trace());
    let mut inputs = Vec::new();
    let incoming: SmallVec<[usize; 4]> = exec
        .incoming_edges
        .get(node_idx)
        .map(|edges| edges.iter().copied().collect())
        .unwrap_or_default();
    for edge_idx in incoming {
        let Some(edge) = exec.edges.get(edge_idx) else {
            continue;
        };
        let to_port = edge.target_port().to_string();
        if !edge_is_active(exec, edge_idx) {
            continue;
        }
        if edge_uses_direct_slot(exec, edge_idx) {
            while let Some(payload) = pop_direct_edge(exec, edge_idx) {
                if collect_lifecycle {
                    let mut lifecycle = DataLifecycleRecord::new(
                        payload.correlation_id,
                        DataLifecycleStage::EdgeDequeued,
                    );
                    lifecycle.node_idx = Some(node_idx);
                    lifecycle.edge_idx = Some(edge_idx);
                    lifecycle.port = Some(to_port.clone());
                    lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
                    exec.core.telemetry.record_data_lifecycle(lifecycle);
                }
                if collect_detailed_metrics {
                    let bytes = exec
                        .core
                        .data_size_inspectors
                        .estimate_payload_bytes(&payload.inner);
                    exec.core
                        .telemetry
                        .record_node_transport_in(node_idx, bytes);
                }
                inputs.push((to_port.clone(), payload));
            }
            continue;
        }
        while let Some(mut payload) =
            pop_edge(edge_idx, &exec.core.queues, &exec.core.data_size_inspectors)
        {
            if collect_lifecycle {
                let mut lifecycle = DataLifecycleRecord::new(
                    payload.correlation_id,
                    DataLifecycleStage::EdgeDequeued,
                );
                lifecycle.node_idx = Some(node_idx);
                lifecycle.edge_idx = Some(edge_idx);
                lifecycle.port = Some(to_port.clone());
                lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
                exec.core.telemetry.record_data_lifecycle(lifecycle);
            }
            if collect_detailed_metrics {
                let bytes = exec
                    .core
                    .data_size_inspectors
                    .estimate_payload_bytes(&payload.inner);
                exec.core
                    .telemetry
                    .record_node_transport_in(node_idx, bytes);
            }
            payload = adapt_edge_payload(exec, edge_idx, payload, node_idx, &to_port)?;
            inputs.push((to_port.clone(), payload));
        }
    }

    let const_inputs = exec
        .const_inputs
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(node_idx)
        .cloned()
        .unwrap_or_default();
    for (port, value) in const_inputs {
        inputs.push((
            port,
            CorrelatedPayload::from_edge(Payload::owned("value", value)),
        ));
    }
    Ok(inputs)
}

fn adapt_edge_payload<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
    mut payload: CorrelatedPayload,
    node_idx: usize,
    port: &str,
) -> Result<CorrelatedPayload, ExecuteError> {
    let Some(edge_transport) = exec.edge_transports.get(edge_idx).and_then(Option::as_ref) else {
        return Ok(payload);
    };
    if edge_transport.adapter_steps.is_empty() {
        return Ok(payload);
    }
    let Some(runtime_transport) = exec.core.runtime_transport.clone() else {
        return Ok(payload);
    };

    let mut request = AdaptRequest::new(
        edge_transport
            .target_transport
            .clone()
            .or_else(|| edge_transport.transport_target.clone())
            .unwrap_or_else(|| payload.inner.type_key().clone()),
    );
    request.access = edge_transport.target_access;
    request.exclusive = edge_transport.target_exclusive;
    request.residency = edge_transport.target_residency;

    let steps: Vec<String> = edge_transport
        .adapter_steps
        .iter()
        .map(ToString::to_string)
        .collect();
    let adapter_detail = adapter_path_detail(edge_transport);
    let mut lifecycle =
        DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::AdapterStart);
    lifecycle.node_idx = Some(node_idx);
    lifecycle.edge_idx = Some(edge_idx);
    lifecycle.port = Some(port.to_string());
    lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
    lifecycle.adapter_steps = steps.clone();
    lifecycle.detail = adapter_detail.clone();
    exec.core.telemetry.record_data_lifecycle(lifecycle);

    tracing::debug!(
        target: "daedalus_runtime::transport",
        edge_index = edge_idx,
        node_index = node_idx,
        port,
        source_type = %payload.inner.type_key(),
        target_type = %request.target,
        target_residency = ?request.residency,
        target_access = ?request.access,
        target_exclusive = request.exclusive,
        adapter_steps = ?steps,
        detail = adapter_detail.as_deref(),
        "adapter path started"
    );
    let adapter_start = Instant::now();
    match runtime_transport.execute_adapter_path(
        payload.inner.clone(),
        &edge_transport.adapter_steps,
        &request,
    ) {
        Ok(adapted) => {
            exec.core
                .telemetry
                .record_edge_adapter_duration(edge_idx, adapter_start.elapsed());
            payload.inner = adapted;
            let mut lifecycle =
                DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::AdapterEnd);
            let elapsed = adapter_start.elapsed();
            lifecycle.node_idx = Some(node_idx);
            lifecycle.edge_idx = Some(edge_idx);
            lifecycle.port = Some(port.to_string());
            lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
            lifecycle.adapter_steps = steps;
            lifecycle.detail = adapter_detail;
            exec.core.telemetry.record_data_lifecycle(lifecycle);
            tracing::debug!(
                target: "daedalus_runtime::transport",
                edge_index = edge_idx,
                node_index = node_idx,
                port,
                output_type = %payload.inner.type_key(),
                elapsed_nanos = elapsed.as_nanos() as u64,
                "adapter path finished"
            );
            Ok(payload)
        }
        Err(error) => {
            exec.core.telemetry.record_edge_adapter_error(edge_idx);
            tracing::warn!(
                target: "daedalus_runtime::transport",
                edge_index = edge_idx,
                node_index = node_idx,
                port,
                error = %error,
                "adapter path failed"
            );
            let mut lifecycle =
                DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::AdapterError);
            lifecycle.node_idx = Some(node_idx);
            lifecycle.edge_idx = Some(edge_idx);
            lifecycle.port = Some(port.to_string());
            lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
            lifecycle.adapter_steps = steps;
            lifecycle.detail = Some(error.to_string());
            exec.core.telemetry.record_data_lifecycle(lifecycle);
            Err(ExecuteError::HandlerFailed {
                node: exec
                    .nodes
                    .get(node_idx)
                    .map(|node| node.id.clone())
                    .unwrap_or_else(|| format!("node_{node_idx}")),
                error: super::NodeError::InvalidInput(error.to_string()),
            })
        }
    }
}

fn adapter_path_detail(edge_transport: &crate::plan::RuntimeEdgeTransport) -> Option<String> {
    if edge_transport.adapter_path.is_empty() && edge_transport.expected_adapter_cost.is_none() {
        return None;
    }
    let steps = edge_transport
        .adapter_path
        .iter()
        .map(|step| format!("{}:{:?}", step.adapter, step.kind))
        .collect::<Vec<_>>()
        .join(" -> ");
    Some(format!(
        "adapter_path=[{}]; expected_cost={}",
        steps,
        edge_transport
            .expected_adapter_cost
            .map(|cost| cost.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ))
}

fn publish_outputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_idx: usize,
    outputs: Vec<(String, CorrelatedPayload)>,
) -> Result<(), super::NodeError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let outgoing: SmallVec<[usize; 4]> = exec
        .outgoing_edges
        .get(node_idx)
        .map(|edges| edges.iter().copied().collect())
        .unwrap_or_default();
    for (port, payload) in outputs {
        if collect_detailed_metrics {
            let bytes = exec
                .core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner);
            exec.core
                .telemetry
                .record_node_transport_out(node_idx, bytes);
        }
        let matching_edges: SmallVec<[(usize, RuntimeEdgePolicy); 4]> = outgoing
            .iter()
            .copied()
            .filter_map(|edge_idx| {
                let edge = exec.edges.get(edge_idx)?;
                (edge.source_port() == port.as_str() && edge_is_active(exec, edge_idx))
                    .then(|| (edge_idx, edge.policy().clone()))
            })
            .collect();
        let last_edge = matching_edges.len().saturating_sub(1);
        let mut payload_slot = Some(payload);
        for (idx, (edge_idx, policy)) in matching_edges.into_iter().enumerate() {
            let cloned_payload = idx != last_edge;
            let payload = if cloned_payload {
                let Some(payload) = payload_slot.as_ref() else {
                    tracing::error!(edge_idx, "payload slot unexpectedly empty before clone");
                    continue;
                };
                payload.clone()
            } else {
                let Some(payload) = payload_slot.take() else {
                    tracing::error!(edge_idx, "payload slot unexpectedly empty before handoff");
                    continue;
                };
                payload
            };
            if collect_detailed_metrics {
                exec.core.telemetry.record_edge_handoff(
                    edge_idx,
                    payload.inner.is_storage_unique(),
                    cloned_payload,
                    0,
                );
            }
            if edge_uses_direct_slot(exec, edge_idx) {
                push_direct_edge(exec, edge_idx, payload);
                continue;
            }
            let queues = exec.core.queues.clone();
            let warnings_seen = exec.core.warnings_seen.clone();
            let data_size_inspectors = exec.core.data_size_inspectors.clone();
            let backpressure = exec.backpressure.clone();
            apply_policy_owned(ApplyPolicyOwnedArgs {
                edge_idx,
                policy: &policy,
                payload,
                queues: &queues,
                warnings_seen: &warnings_seen,
                telem: &mut exec.core.telemetry,
                warning_label: None,
                backpressure,
                data_size_inspectors: &data_size_inspectors,
            })?;
        }
    }
    Ok(())
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

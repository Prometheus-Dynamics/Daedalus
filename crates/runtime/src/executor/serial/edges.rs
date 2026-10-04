//! Edge input collection, adapter application and output publishing for serial execution.

use daedalus_core::platform::Instant;

use daedalus_transport::{AdaptRequest, Payload};

use crate::io::NodePort;

use crate::executor::queue::{ApplyPolicyOwnedArgs, apply_policy_owned, pop_edge};
use crate::executor::serial_direct_slot::{pop_direct_edge, push_direct_edge};
use crate::executor::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, ExecuteError, Executor, NodeError,
    NodeHandler,
};

use super::{edge_is_active, edge_uses_direct_slot};

pub(super) fn collect_inputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_idx: usize,
) -> Result<Vec<NodePort>, ExecuteError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (exec.core.run_config.metrics_level.is_profile()
            || exec.core.run_config.metrics_level.is_trace());
    let mut inputs = crate::io::port_buffer();
    let edges = exec.edges;
    let incoming = exec.incoming_edges.clone();
    for &edge_idx in incoming.get(node_idx).into_iter().flatten() {
        let Some(edge) = edges.get(edge_idx) else {
            continue;
        };
        let to_port = edge.target_port_id();
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
                    lifecycle.port = Some(to_port.to_string());
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
                lifecycle.port = Some(to_port.to_string());
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
            payload = adapt_edge_payload(exec, edge_idx, payload, node_idx, to_port.as_str())?;
            inputs.push((to_port.clone(), payload));
        }
    }

    crate::executor::push_const_inputs(&exec.const_inputs, node_idx, &mut inputs);
    Ok(inputs)
}

fn adapt_edge_payload<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
    payload: CorrelatedPayload,
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
    let CorrelatedPayload {
        correlation_id,
        inner,
        enqueued_at,
    } = payload;

    let mut request = AdaptRequest::new(
        edge_transport
            .target_transport
            .clone()
            .or_else(|| edge_transport.transport_target.clone())
            .unwrap_or_else(|| inner.type_key().clone()),
    );
    request.access = edge_transport.target_access;
    request.exclusive = edge_transport.target_exclusive;
    request.residency = edge_transport.target_residency;

    // Lifecycle records (profile/trace metrics only) carry formatted step and path text.
    let collect_lifecycle = cfg!(feature = "metrics")
        && (exec.core.run_config.metrics_level.is_profile()
            || exec.core.run_config.metrics_level.is_trace());
    let record = |exec: &mut Executor<'_, H>, stage, payload: &Payload, detail| {
        if !collect_lifecycle {
            return;
        }
        let mut lifecycle = DataLifecycleRecord::new(correlation_id, stage);
        lifecycle.node_idx = Some(node_idx);
        lifecycle.edge_idx = Some(edge_idx);
        lifecycle.port = Some(port.to_string());
        lifecycle.payload = Some(format!("Payload({})", payload.type_key()));
        lifecycle.adapter_steps = edge_transport
            .adapter_steps
            .iter()
            .map(ToString::to_string)
            .collect();
        lifecycle.detail = detail;
        exec.core.telemetry.record_data_lifecycle(lifecycle);
    };
    record(
        exec,
        DataLifecycleStage::AdapterStart,
        &inner,
        collect_lifecycle
            .then(|| adapter_path_detail(edge_transport))
            .flatten(),
    );
    tracing::debug!(
        target: "daedalus_runtime::transport",
        edge_index = edge_idx,
        node_index = node_idx,
        port,
        source_type = %inner.type_key(),
        target_type = %request.target,
        target_residency = ?request.residency,
        target_access = ?request.access,
        target_exclusive = request.exclusive,
        adapter_steps = ?edge_transport.adapter_steps,
        "adapter path started"
    );
    let adapter_start = exec
        .core
        .run_config
        .metrics_level
        .is_detailed()
        .then(Instant::now);
    let source = collect_lifecycle.then(|| inner.clone());
    match runtime_transport.execute_adapter_path(inner, &edge_transport.adapter_steps, &request) {
        Ok(adapted) => {
            if let Some(start) = adapter_start {
                exec.core
                    .telemetry
                    .record_edge_adapter_duration(edge_idx, start.elapsed());
            }
            record(
                exec,
                DataLifecycleStage::AdapterEnd,
                &adapted,
                collect_lifecycle
                    .then(|| adapter_path_detail(edge_transport))
                    .flatten(),
            );
            tracing::debug!(
                target: "daedalus_runtime::transport",
                edge_index = edge_idx,
                node_index = node_idx,
                port,
                output_type = %adapted.type_key(),
                "adapter path finished"
            );
            Ok(CorrelatedPayload {
                correlation_id,
                inner: adapted,
                enqueued_at,
            })
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
            if let Some(source) = &source {
                record(
                    exec,
                    DataLifecycleStage::AdapterError,
                    source,
                    Some(error.to_string()),
                );
            }
            Err(ExecuteError::HandlerFailed {
                node: exec
                    .nodes
                    .get(node_idx)
                    .map(|node| node.id.clone())
                    .unwrap_or_else(|| format!("node_{node_idx}")),
                error: crate::executor::NodeError::InvalidInput(error.to_string()),
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

pub(super) fn publish_outputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_idx: usize,
    mut outputs: Vec<NodePort>,
) -> Result<(), NodeError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let edges = exec.edges;
    let outgoing = exec.outgoing_edges.clone();
    let outgoing = outgoing
        .get(node_idx)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for (port, payload) in outputs.drain(..) {
        if collect_detailed_metrics {
            let bytes = exec
                .core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner);
            exec.core
                .telemetry
                .record_node_transport_out(node_idx, bytes);
        }
        let routes = |exec: &Executor<'_, H>, edge_idx: usize| {
            edges[edge_idx].source_port_id() == &port && edge_is_active(exec, edge_idx)
        };
        fan_out(exec, outgoing, routes, payload)?;
    }
    crate::io::recycle_ports(outputs);
    Ok(())
}

/// Hand `payload` to every edge of `candidates` that `routes` accepts, cloning it for all but the
/// last one (no target list is collected, so wide fan-outs do not allocate).
pub(super) fn fan_out<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    candidates: &[usize],
    routes: impl Fn(&Executor<'_, H>, usize) -> bool,
    payload: CorrelatedPayload,
) -> Result<(), NodeError> {
    let Some(last) = candidates
        .iter()
        .rposition(|&edge_idx| routes(exec, edge_idx))
    else {
        return Ok(());
    };
    for &edge_idx in &candidates[..last] {
        if routes(exec, edge_idx) {
            deliver(exec, edge_idx, payload.clone(), true)?;
        }
    }
    deliver(exec, candidates[last], payload, false)
}

fn deliver<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
    payload: CorrelatedPayload,
    cloned_payload: bool,
) -> Result<(), NodeError> {
    if cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed() {
        exec.core.telemetry.record_edge_handoff(
            edge_idx,
            payload.inner.is_storage_unique(),
            cloned_payload,
            0,
        );
    }
    if edge_uses_direct_slot(exec, edge_idx) {
        push_direct_edge(exec, edge_idx, payload);
        return Ok(());
    }
    let edges = exec.edges;
    let core = &mut exec.core;
    apply_policy_owned(ApplyPolicyOwnedArgs {
        edge_idx,
        policy: edges[edge_idx].policy(),
        payload,
        queues: &core.queues,
        warnings_seen: &core.warnings_seen,
        telem: &mut core.telemetry,
        warning_label: None,
        backpressure: exec.backpressure.clone(),
        data_size_inspectors: &core.data_size_inspectors,
    })
}

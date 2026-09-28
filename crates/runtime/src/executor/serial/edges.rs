//! Edge input collection, adapter application and output publishing for serial execution.

use std::time::Instant;

use daedalus_transport::{AdaptRequest, Payload};
use smallvec::SmallVec;

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
) -> Result<SmallVec<[NodePort; 4]>, ExecuteError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (exec.core.run_config.metrics_level.is_profile()
            || exec.core.run_config.metrics_level.is_trace());
    let mut inputs = SmallVec::new();
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

    let const_inputs = exec.const_inputs.read();
    for (port, value) in const_inputs.get(node_idx).into_iter().flatten() {
        inputs.push((
            port.clone(),
            CorrelatedPayload::from_edge(Payload::owned("value", value.clone())),
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
    outputs: SmallVec<[NodePort; 4]>,
) -> Result<(), NodeError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && exec.core.run_config.metrics_level.is_detailed();
    let edges = exec.edges;
    let outgoing = exec.outgoing_edges.clone();
    let outgoing = outgoing
        .get(node_idx)
        .map(Vec::as_slice)
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
        let targets: SmallVec<[usize; 4]> = outgoing
            .iter()
            .copied()
            .filter(|&edge_idx| {
                edges[edge_idx].source_port_id() == &port && edge_is_active(exec, edge_idx)
            })
            .collect();
        fan_out(exec, &targets, payload)?;
    }
    Ok(())
}

/// Hand `payload` to every edge in `targets`, cloning it for all but the last edge.
pub(super) fn fan_out<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    targets: &[usize],
    payload: CorrelatedPayload,
) -> Result<(), NodeError> {
    let Some((&last, rest)) = targets.split_last() else {
        return Ok(());
    };
    for &edge_idx in rest {
        deliver(exec, edge_idx, payload.clone(), true)?;
    }
    deliver(exec, last, payload, false)
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

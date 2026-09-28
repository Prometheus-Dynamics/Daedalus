//! Edge input collection, adapter application and output publishing for serial execution.

use std::time::Instant;

use daedalus_transport::{AdaptRequest, Payload};
use smallvec::SmallVec;

use crate::plan::RuntimeEdgePolicy;

use crate::executor::queue::{ApplyPolicyOwnedArgs, apply_policy_owned, pop_edge};
use crate::executor::serial_direct_slot::{pop_direct_edge, push_direct_edge};
use crate::executor::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, ExecuteError, Executor, NodeHandler,
};

use super::{edge_is_active, edge_uses_direct_slot};

pub(super) fn collect_inputs<H: NodeHandler>(
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
    outputs: Vec<(String, CorrelatedPayload)>,
) -> Result<(), crate::executor::NodeError> {
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
                    tracing::error!(
                        target: "daedalus_runtime::executor",
                        edge_idx,
                        "payload slot unexpectedly empty before clone"
                    );
                    continue;
                };
                payload.clone()
            } else {
                let Some(payload) = payload_slot.take() else {
                    tracing::error!(
                        target: "daedalus_runtime::executor",
                        edge_idx,
                        "payload slot unexpectedly empty before handoff"
                    );
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

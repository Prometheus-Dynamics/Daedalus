//! Edge input collection, adapter application and output publishing for serial execution.

use crate::prelude::*;
use daedalus_transport::{AdaptRequest, Payload};

use crate::handles::PortId;
use crate::io::NodePort;

use crate::executor::queue::{ApplyPolicyOwnedArgs, apply_policy_owned, pop_edge};
use crate::executor::serial_direct_slot::{pop_direct_edge, push_direct_edge};
use crate::executor::{
    CorrelatedPayload, DataLifecycleRecord, DataLifecycleStage, ExecuteError, NodeError, ProbeCount,
};

use super::fused::{Carry, FusedOutputs, take_carried};
use super::{SerialView, edge_is_active, edge_uses_direct_slot};

/// Pop what the incoming edges of `node_idx` hold: everything, or with
/// `one_per_edge` (a `fire = "all"` node) only the oldest value of each edge. Fused edges hand
/// over what `carry` holds for them.
pub(super) fn collect_inputs(
    v: &mut SerialView<'_>,
    node_idx: usize,
    one_per_edge: bool,
    carry: &mut Carry,
) -> Result<Vec<NodePort>, ExecuteError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed();
    let collect_lifecycle = cfg!(feature = "metrics")
        && (v.core.run_config.metrics_level.is_profile()
            || v.core.run_config.metrics_level.is_trace());
    let mut inputs = crate::io::port_buffer();
    let edges = v.edges;
    let incoming = v.incoming_edges;
    for &edge_idx in incoming.get(node_idx).into_iter().flatten() {
        let Some(edge) = edges.get(edge_idx) else {
            continue;
        };
        let to_port = edge.target_port_id();
        if !edge_is_active(v, edge_idx) {
            continue;
        }
        if !carry.is_empty() && take_carried(v, node_idx, edge_idx, to_port, carry, &mut inputs) {
            continue;
        }
        if edge_uses_direct_slot(v, edge_idx) {
            while let Some(mut payload) = pop_direct_edge(v.core, v.direct_slot_access, edge_idx) {
                if collect_lifecycle {
                    record_dequeued(v, node_idx, edge_idx, to_port, &payload);
                }
                if collect_detailed_metrics {
                    let bytes = v
                        .core
                        .data_size_inspectors
                        .estimate_payload_bytes(&payload.inner);
                    v.core.telemetry.record_node_transport_in(node_idx, bytes);
                }
                if edge_adapts(v, edge_idx) {
                    payload = adapt_edge_payload(v, edge_idx, payload, node_idx, to_port.as_str())?;
                }
                inputs.push((to_port.clone(), payload));
                if one_per_edge {
                    break;
                }
            }
            continue;
        }
        while let Some(mut payload) =
            pop_edge(edge_idx, &v.core.queues, &v.core.data_size_inspectors)
        {
            record_queue_wait(v, edge_idx, &payload, collect_detailed_metrics);
            if collect_lifecycle {
                record_dequeued(v, node_idx, edge_idx, to_port, &payload);
            }
            if collect_detailed_metrics {
                let bytes = v
                    .core
                    .data_size_inspectors
                    .estimate_payload_bytes(&payload.inner);
                v.core.telemetry.record_node_transport_in(node_idx, bytes);
            }
            if edge_adapts(v, edge_idx) {
                payload = adapt_edge_payload(v, edge_idx, payload, node_idx, to_port.as_str())?;
            }
            inputs.push((to_port.clone(), payload));
            if one_per_edge {
                break;
            }
        }
    }

    Ok(inputs)
}

/// The profile/trace lifecycle record of a payload taken off `edge_idx` for `node_idx`'s `port`.
/// Outlined: it formats text, and only profile and trace metrics record it.
#[cold]
#[inline(never)]
fn record_dequeued(
    v: &mut SerialView<'_>,
    node_idx: usize,
    edge_idx: usize,
    port: &PortId,
    payload: &CorrelatedPayload,
) {
    let mut lifecycle =
        DataLifecycleRecord::new(payload.correlation_id, DataLifecycleStage::EdgeDequeued);
    lifecycle.node_idx = Some(node_idx);
    lifecycle.edge_idx = Some(edge_idx);
    lifecycle.port = Some(port.to_string());
    lifecycle.payload = Some(format!("Payload({})", payload.inner.type_key()));
    v.core.telemetry.record_data_lifecycle(lifecycle);
}

/// Enqueue-to-dequeue time of a queued payload (stamped with basic metrics or a frame probe).
fn record_queue_wait(
    v: &mut SerialView<'_>,
    edge_idx: usize,
    payload: &CorrelatedPayload,
    detailed: bool,
) {
    let Some(enqueued_at) = payload.enqueued_at else {
        return;
    };
    let core = &mut *v.core;
    let probe = core.run_config.frame_probe.as_deref();
    if !detailed && probe.is_none() {
        return;
    }
    let waited = core.clock.elapsed(enqueued_at);
    if detailed {
        core.telemetry.record_edge_wait(edge_idx, waited);
    }
    if let Some(probe) = probe {
        probe.record_queue_wait(edge_idx, waited);
    }
}

/// Whether `edge_idx` runs an adapter path on its payloads: the check [`adapt_edge_payload`] makes
/// first, so an edge that adapts nothing never moves its payload through that call.
fn edge_adapts(v: &SerialView<'_>, edge_idx: usize) -> bool {
    v.edge_transports
        .get(edge_idx)
        .and_then(Option::as_ref)
        .is_some_and(|transport| !transport.adapter_steps.is_empty())
        && v.core.runtime_transport.is_some()
}

fn adapt_edge_payload(
    v: &mut SerialView<'_>,
    edge_idx: usize,
    payload: CorrelatedPayload,
    node_idx: usize,
    port: &str,
) -> Result<CorrelatedPayload, ExecuteError> {
    let Some(edge_transport) = v.edge_transports.get(edge_idx).and_then(Option::as_ref) else {
        return Ok(payload);
    };
    if edge_transport.adapter_steps.is_empty() {
        return Ok(payload);
    }
    let Some(runtime_transport) = v.core.runtime_transport.clone() else {
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
        && (v.core.run_config.metrics_level.is_profile()
            || v.core.run_config.metrics_level.is_trace());
    let record = |v: &mut SerialView<'_>, stage, payload: &Payload, detail| {
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
        v.core.telemetry.record_data_lifecycle(lifecycle);
    };
    record(
        v,
        DataLifecycleStage::AdapterStart,
        &inner,
        collect_lifecycle
            .then(|| adapter_path_detail(edge_transport))
            .flatten(),
    );
    crate::trace::debug!(
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
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed();
    let probe = v.core.run_config.frame_probe.clone();
    let adapter_start = (collect_detailed_metrics || probe.is_some()).then(|| v.core.clock.now());
    let source = collect_lifecycle.then(|| inner.clone());
    match runtime_transport.execute_adapter_path(inner, &edge_transport.adapter_steps, &request) {
        Ok(adapted) => {
            if let Some(start) = adapter_start {
                let duration = v.core.clock.elapsed(start);
                if collect_detailed_metrics {
                    v.core
                        .telemetry
                        .record_edge_adapter_duration(edge_idx, duration);
                }
                if let Some(probe) = &probe {
                    let inspectors = &v.core.data_size_inspectors;
                    probe.record_adapter(edge_idx, duration, || {
                        inspectors.estimate_payload_bytes(&adapted)
                    });
                }
            }
            record(
                v,
                DataLifecycleStage::AdapterEnd,
                &adapted,
                collect_lifecycle
                    .then(|| adapter_path_detail(edge_transport))
                    .flatten(),
            );
            crate::trace::debug!(
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
            v.core.telemetry.record_edge_adapter_error(edge_idx);
            crate::trace::warn!(
                target: "daedalus_runtime::transport",
                edge_index = edge_idx,
                node_index = node_idx,
                port,
                error = %error,
                "adapter path failed"
            );
            if let Some(source) = &source {
                record(
                    v,
                    DataLifecycleStage::AdapterError,
                    source,
                    Some(error.to_string()),
                );
            }
            Err(ExecuteError::HandlerFailed {
                node: v
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

/// Deliver `node_idx`'s outputs to its edges; `fused` carries those of fused ports to the next
/// node instead.
pub(super) fn publish_outputs(
    v: &mut SerialView<'_>,
    node_idx: usize,
    mut outputs: Vec<NodePort>,
    mut fused: Option<FusedOutputs<'_>>,
) -> Result<(), NodeError> {
    let collect_detailed_metrics =
        cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed();
    let edges = v.edges;
    let outgoing = v
        .outgoing_edges
        .get(node_idx)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for (port, payload) in outputs.drain(..) {
        if collect_detailed_metrics {
            let bytes = v
                .core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner);
            v.core.telemetry.record_node_transport_out(node_idx, bytes);
        }
        let payload = match fused.as_mut() {
            Some(fused) => match fused.try_carry(v, &port, payload) {
                Some(payload) => payload,
                None => continue,
            },
            None => payload,
        };
        let routes = |v: &SerialView<'_>, edge_idx: usize| {
            edges[edge_idx].source_port_id() == &port && edge_is_active(v, edge_idx)
        };
        fan_out(v, outgoing, routes, payload)?;
    }
    crate::io::recycle_ports(outputs);
    Ok(())
}

/// Hand `payload` to every edge of `candidates` that `routes` accepts, cloning it for all but the
/// last one (no target list is collected, so wide fan-outs do not allocate).
pub(super) fn fan_out(
    v: &mut SerialView<'_>,
    candidates: &[usize],
    routes: impl Fn(&SerialView<'_>, usize) -> bool,
    payload: CorrelatedPayload,
) -> Result<(), NodeError> {
    let Some(last) = candidates.iter().rposition(|&edge_idx| routes(v, edge_idx)) else {
        return Ok(());
    };
    for &edge_idx in &candidates[..last] {
        if routes(v, edge_idx) {
            deliver(v, edge_idx, payload.clone(), true)?;
        }
    }
    deliver(v, candidates[last], payload, false)
}

fn deliver(
    v: &mut SerialView<'_>,
    edge_idx: usize,
    payload: CorrelatedPayload,
    cloned_payload: bool,
) -> Result<(), NodeError> {
    if cloned_payload && let Some(probe) = &v.core.run_config.frame_probe {
        probe.add_count(ProbeCount::SharedClones, 1);
    }
    if cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed() {
        v.core.telemetry.record_edge_handoff(
            edge_idx,
            payload.inner.is_storage_unique(),
            cloned_payload,
            0,
        );
    }
    if edge_uses_direct_slot(v, edge_idx) {
        push_direct_edge(v.core, v.edges, v.direct_slot_access, edge_idx, payload);
        return Ok(());
    }
    let edges = v.edges;
    let core = &mut *v.core;
    apply_policy_owned(ApplyPolicyOwnedArgs {
        edge_idx,
        policy: edges[edge_idx].policy(),
        payload,
        queues: &core.queues,
        warnings_seen: &core.warnings_seen,
        telem: &mut core.telemetry,
        warning_label: None,
        backpressure: v.backpressure.clone(),
        data_size_inspectors: &core.data_size_inspectors,
        stamp_enqueue: core.run_config.frame_probe.is_some(),
    })
}

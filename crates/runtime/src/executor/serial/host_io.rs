//! Host bridge input injection and output draining for serial execution.

use smallvec::SmallVec;

use crate::handles::PortId;
use crate::plan::RuntimeEdgePolicy;

use crate::executor::queue::{ApplyPolicyOwnedArgs, apply_policy_owned, pop_edge};
use crate::executor::serial_direct_slot::{pop_direct_edge, push_direct_edge};
use crate::executor::{CorrelatedPayload, ExecuteError, Executor, NodeHandler};

use super::{edge_is_active, edge_uses_direct_slot, node_is_active};

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

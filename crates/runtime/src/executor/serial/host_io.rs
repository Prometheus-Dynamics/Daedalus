//! Host bridge input injection and output draining for serial execution.
//!
//! Host-bridge nodes are resolved to their bridge handles and port wiring once, when bridges are
//! attached ([`resolve_host_nodes`]), so a tick never touches the bridge manager's map lock and
//! never allocates port ids.

use crate::portable::Arc;
use crate::prelude::*;

use daedalus_planner::NodeRef;
use smallvec::SmallVec;

use crate::handles::PortId;
use crate::host_bridge::{HostBridgeHandle, HostBridgeManager, InboundTake};
use crate::plan::{RuntimeEdge, RuntimeNode};

use crate::executor::queue::pop_edge;
use crate::executor::serial_direct_slot::pop_direct_edge;
use crate::executor::{
    CorrelatedPayload, ExecuteError, Executor, NodeError, NodeHandler, ProbeCount, ProbeTime,
};

use super::edges::{adapt_edge_payload, edge_adapts, fan_out};
use super::{SerialView, edge_is_active, edge_uses_direct_slot, node_is_active};

/// One host-bridge node with its bridge handle and edge wiring.
pub(crate) struct HostNodeIo {
    node_idx: usize,
    handle: HostBridgeHandle,
    /// Host → graph: outgoing edges grouped by the host port that feeds them.
    inbound: Box<[(PortId, SmallVec<[usize; 2]>)]>,
    /// Graph → host: incoming edge indices; each edge's target port is the host port.
    outbound: Box<[usize]>,
}

/// Resolve every host-bridge node against `bridges`, creating missing bridges and making the
/// inputs its metadata declares held (`HOST_HELD_INPUTS_KEY`).
pub(crate) fn resolve_host_nodes(
    bridges: &HostBridgeManager,
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    host_nodes: &[NodeRef],
    incoming: &[Vec<usize>],
    outgoing: &[Vec<usize>],
) -> Arc<[HostNodeIo]> {
    host_nodes
        .iter()
        .filter_map(|node_ref| {
            let node = nodes.get(node_ref.0)?;
            let mut inbound: Vec<(PortId, SmallVec<[usize; 2]>)> = Vec::new();
            for &edge_idx in outgoing.get(node_ref.0).into_iter().flatten() {
                let port = edges[edge_idx].source_port_id();
                match inbound.iter_mut().find(|(seen, _)| seen == port) {
                    Some((_, group)) => group.push(edge_idx),
                    None => inbound.push((port.clone(), SmallVec::from_elem(edge_idx, 1))),
                }
            }
            let handle = bridges.ensure_handle(node.host_alias());
            for port in daedalus_planner::host_held_inputs(&node.metadata) {
                handle.set_held_input(PortId::new(port));
            }
            // A host input's declared type: the planner's resolved type of the bridge's output
            // port (`push_value` converts through its key).
            let dynamic = daedalus_planner::DynamicPortMetadata::from_node_metadata(&node.metadata);
            for (port, _) in &inbound {
                let resolved = dynamic
                    .resolved_type(false, port.as_str())
                    .filter(|ty| !daedalus_planner::is_generic_marker(ty));
                if let Some(expr) = resolved {
                    handle.declare_input_key(
                        port.clone(),
                        daedalus_registry::typeexpr_transport_key(&expr),
                    );
                }
            }
            Some(HostNodeIo {
                node_idx: node_ref.0,
                handle,
                inbound: inbound.into(),
                outbound: incoming.get(node_ref.0).cloned().unwrap_or_default().into(),
            })
        })
        .collect()
}

/// Move each host bridge's inbound input into the edges it feeds, taking every port of a bridge
/// under one bridge lock so a batch push lands in one tick whole. A held port's edges are emptied
/// and refilled with its current value (an `Arc` clone) each tick, so they hold exactly that.
/// Inject the host inputs of `exec` (see [`inject`]).
pub(crate) fn inject_host_inputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
) -> Result<(), ExecuteError> {
    inject(&mut super::view(exec))
}

fn inject(v: &mut SerialView<'_>) -> Result<(), ExecuteError> {
    let host_nodes = v.core.host_nodes.clone();
    for host in host_nodes.iter() {
        let result = host.handle.with_inbound(|inbound| {
            for (port, group) in host.inbound.iter() {
                if !group.iter().any(|&edge_idx| routes(v, edge_idx)) {
                    continue;
                }
                let mut result: Result<(), NodeError> = Ok(());
                // A host version stamps the values this port delivers; only reuse reads it.
                let version = if v.core.reuse.is_some() {
                    inbound.version(port.as_str())
                } else {
                    None
                };
                let taken = inbound.take(port.as_str(), |payload| {
                    if result.is_ok() {
                        result = fan_out(
                            v,
                            group,
                            routes,
                            CorrelatedPayload::from_edge(payload).with_version(version),
                        );
                    }
                });
                result?;
                if let InboundTake::Held(held) = taken {
                    for &edge_idx in group.iter() {
                        if routes(v, edge_idx) {
                            clear_edge(v, edge_idx);
                        }
                    }
                    if let Some(payload) = held {
                        // The re-delivered value is an `Arc` clone of the held one.
                        if let Some(probe) = &v.core.run_config.frame_probe {
                            probe.add_count(ProbeCount::SharedClones, 1);
                        }
                        fan_out(
                            v,
                            group,
                            routes,
                            CorrelatedPayload::from_edge(payload).with_version(version),
                        )?;
                    }
                }
            }
            Ok(())
        });
        result.map_err(|error| ExecuteError::HandlerFailed {
            node: v.nodes[host.node_idx].id.clone(),
            error,
        })?;
    }
    Ok(())
}

/// Whether a host input edge delivers this tick: it and its target node are active.
fn routes(v: &SerialView<'_>, edge_idx: usize) -> bool {
    edge_is_active(v, edge_idx) && node_is_active(v, v.edges[edge_idx].to().0)
}

/// Drop whatever an edge still holds (a held input's previous value its consumer did not take).
fn clear_edge(v: &mut SerialView<'_>, edge_idx: usize) {
    if edge_uses_direct_slot(v, edge_idx) {
        while pop_direct_edge(v.core, v.direct_slot_access, edge_idx).is_some() {}
    } else {
        while pop_edge(edge_idx, &v.core.queues, &v.core.data_size_inspectors).is_some() {}
    }
}

/// Drain the host outputs of `exec` (see [`drain`]).
#[cfg(feature = "threads")]
pub(crate) fn drain_host_outputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
) -> Result<(), ExecuteError> {
    drain(&mut super::view(exec))
}

/// `payload`, taken from the edge `edge_idx` into a host output, through the edge's adapters (the
/// direct host route takes its output this way instead of draining).
pub(crate) fn adapt_host_output<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    edge_idx: usize,
    payload: CorrelatedPayload,
) -> Result<CorrelatedPayload, ExecuteError> {
    let mut v = super::view(exec);
    if !edge_adapts(&v, edge_idx) {
        return Ok(payload);
    }
    let edge = &v.edges[edge_idx];
    let (host, to) = (edge.to().0, edge.target_port_id());
    adapt_edge_payload(&mut v, edge_idx, payload, host, to)
}

/// Hand what the edges into host outputs hold to their host bridges, through each edge's
/// adapters (a planner-inserted conversion to the host port's type); an adapter's failure stops
/// the drain.
pub(super) fn drain(v: &mut SerialView<'_>) -> Result<(), ExecuteError> {
    let _scope = crate::executor::runtime_alloc_scope();
    let start = v
        .core
        .run_config
        .frame_probe
        .is_some()
        .then(|| v.core.clock.now());
    let edges = v.edges;
    let host_nodes = v.core.host_nodes.clone();
    for host in host_nodes.iter() {
        for &edge_idx in host.outbound.iter() {
            let port = edges[edge_idx].target_port();
            if !edge_is_active(v, edge_idx)
                || v.core
                    .run_config
                    .selected_host_output_ports
                    .as_ref()
                    .is_some_and(|ports| !ports.contains(port))
            {
                continue;
            }
            let adapts = edge_adapts(v, edge_idx);
            if edge_uses_direct_slot(v, edge_idx) {
                while let Some(mut payload) =
                    pop_direct_edge(v.core, v.direct_slot_access, edge_idx)
                {
                    if adapts {
                        let to = edges[edge_idx].target_port_id();
                        payload = adapt_edge_payload(v, edge_idx, payload, host.node_idx, to)?;
                    }
                    host.handle.push_outbound_ref(port, payload.inner);
                }
                continue;
            }
            while let Some(mut payload) =
                pop_edge(edge_idx, &v.core.queues, &v.core.data_size_inspectors)
            {
                if let (Some(probe), Some(enqueued_at)) =
                    (&v.core.run_config.frame_probe, payload.enqueued_at)
                {
                    probe.record_queue_wait(edge_idx, v.core.clock.elapsed(enqueued_at));
                }
                if adapts {
                    let to = edges[edge_idx].target_port_id();
                    payload = adapt_edge_payload(v, edge_idx, payload, host.node_idx, to)?;
                }
                host.handle.push_outbound_ref(port, payload.inner);
            }
        }
    }
    if let (Some(probe), Some(start)) = (&v.core.run_config.frame_probe, start) {
        probe.add_time(ProbeTime::Drain, v.core.clock.elapsed(start));
    }
    Ok(())
}

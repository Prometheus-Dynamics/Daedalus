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

use super::edges::fan_out;
use super::{edge_is_active, edge_uses_direct_slot, node_is_active};

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
pub(crate) fn inject_host_inputs<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
) -> Result<(), ExecuteError> {
    let host_nodes = exec.core.host_nodes.clone();
    for host in host_nodes.iter() {
        let result = host.handle.with_inbound(|inbound| {
            for (port, group) in host.inbound.iter() {
                if !group.iter().any(|&edge_idx| routes(exec, edge_idx)) {
                    continue;
                }
                let mut result: Result<(), NodeError> = Ok(());
                let taken = inbound.take(port.as_str(), |payload| {
                    if result.is_ok() {
                        result =
                            fan_out(exec, group, routes, CorrelatedPayload::from_edge(payload));
                    }
                });
                result?;
                if let InboundTake::Held(held) = taken {
                    for &edge_idx in group.iter() {
                        if routes(exec, edge_idx) {
                            clear_edge(exec, edge_idx);
                        }
                    }
                    if let Some(payload) = held {
                        // The re-delivered value is an `Arc` clone of the held one.
                        if let Some(probe) = &exec.core.run_config.frame_probe {
                            probe.add_count(ProbeCount::SharedClones, 1);
                        }
                        fan_out(exec, group, routes, CorrelatedPayload::from_edge(payload))?;
                    }
                }
            }
            Ok(())
        });
        result.map_err(|error| ExecuteError::HandlerFailed {
            node: exec.nodes[host.node_idx].id.clone(),
            error,
        })?;
    }
    Ok(())
}

/// Whether a host input edge delivers this tick: it and its target node are active.
fn routes<H: NodeHandler>(exec: &Executor<'_, H>, edge_idx: usize) -> bool {
    edge_is_active(exec, edge_idx) && node_is_active(exec, exec.edges[edge_idx].to().0)
}

/// Drop whatever an edge still holds (a held input's previous value its consumer did not take).
fn clear_edge<H: NodeHandler>(exec: &mut Executor<'_, H>, edge_idx: usize) {
    if edge_uses_direct_slot(exec, edge_idx) {
        while pop_direct_edge(exec, edge_idx).is_some() {}
    } else {
        while pop_edge(edge_idx, &exec.core.queues, &exec.core.data_size_inspectors).is_some() {}
    }
}

pub(crate) fn drain_host_outputs<H: NodeHandler>(exec: &mut Executor<'_, H>) {
    let _scope = crate::executor::runtime_alloc_scope();
    let start = exec
        .core
        .run_config
        .frame_probe
        .is_some()
        .then(|| exec.core.clock.now());
    let edges = exec.edges;
    let host_nodes = exec.core.host_nodes.clone();
    for host in host_nodes.iter() {
        for &edge_idx in host.outbound.iter() {
            let port = edges[edge_idx].target_port();
            if !edge_is_active(exec, edge_idx)
                || exec
                    .core
                    .run_config
                    .selected_host_output_ports
                    .as_ref()
                    .is_some_and(|ports| !ports.contains(port))
            {
                continue;
            }
            if edge_uses_direct_slot(exec, edge_idx) {
                while let Some(payload) = pop_direct_edge(exec, edge_idx) {
                    host.handle.push_outbound_ref(port, payload.inner);
                }
                continue;
            }
            while let Some(payload) =
                pop_edge(edge_idx, &exec.core.queues, &exec.core.data_size_inspectors)
            {
                if let (Some(probe), Some(enqueued_at)) =
                    (&exec.core.run_config.frame_probe, payload.enqueued_at)
                {
                    probe.record_queue_wait(edge_idx, exec.core.clock.elapsed(enqueued_at));
                }
                host.handle.push_outbound_ref(port, payload.inner);
            }
        }
    }
    if let (Some(probe), Some(start)) = (&exec.core.run_config.frame_probe, start) {
        probe.add_time(ProbeTime::Drain, exec.core.clock.elapsed(start));
    }
}

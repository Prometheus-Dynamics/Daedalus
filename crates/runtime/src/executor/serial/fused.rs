//! Fused edges in serial runs: a producer's payload goes straight into its consumer's inputs
//! when the consumer runs next, instead of through the edge's direct slot.

use smallvec::SmallVec;

use crate::executor::schedule_compile::FusedPort;
use crate::executor::{CorrelatedPayload, FusionTables};
use crate::handles::PortId;
use crate::io::NodePort;
use crate::prelude::*;

use super::{SerialView, edge_is_active, edge_uses_direct_slot, node_is_active};

/// Payloads on their way along fused edges to the next node: `(edge, payload)` in push order.
/// Two inline, so a chain allocates nothing.
pub(crate) type Carry = SmallVec<[(usize, CorrelatedPayload); 2]>;

/// What [`super::edges::publish_outputs`] needs to fuse a node's outputs.
pub(crate) struct FusedOutputs<'a> {
    /// The node's fused output ports: port, edge, consumer.
    pub(crate) ports: &'a [FusedPort],
    /// The node the schedule runs next.
    pub(crate) next: Option<usize>,
    pub(crate) carry: &'a mut Carry,
}

impl<'a> FusedOutputs<'a> {
    /// `node_idx`'s fused outputs, when it has any, running at `pos` of `order`.
    pub(crate) fn of(
        v: &SerialView<'_>,
        tables: Option<&'a FusionTables>,
        node_idx: usize,
        order: &[daedalus_planner::NodeRef],
        pos: usize,
        carry: &'a mut Carry,
    ) -> Option<Self> {
        let ports = tables?
            .outputs
            .get(node_idx)
            .filter(|ports| !ports.is_empty())?;
        // Host-bridge nodes never run here, so the consumer may follow one.
        let next = order
            .get(pos + 1..)?
            .iter()
            .map(|node| node.0)
            .find(|&idx| !v.core.host_bridges.get(idx).copied().unwrap_or(false));
        Some(Self { ports, next, carry })
    }

    /// Carry `payload` from `port` when that port is fused to the next node and its edge is live
    /// this tick; otherwise hand it back for ordinary delivery.
    pub(crate) fn try_carry(
        &mut self,
        v: &mut SerialView<'_>,
        port: &PortId,
        payload: CorrelatedPayload,
    ) -> Option<CorrelatedPayload> {
        let Some(&(_, edge_idx, consumer)) = self.ports.iter().find(|(fused, ..)| fused == port)
        else {
            return Some(payload);
        };
        // A slot that already holds a payload (left from an earlier tick, e.g. while the consumer
        // was inactive) must deliver it first, so such ticks go through the slot.
        let slot_empty = || {
            v.core
                .direct_slots
                .get(edge_idx)
                .is_some_and(|slot| !slot.access(v.direct_slot_access).occupied())
        };
        if self.next != Some(consumer)
            || !edge_is_active(v, edge_idx)
            || !edge_uses_direct_slot(v, edge_idx)
            || !node_is_active(v, consumer)
            || !slot_empty()
        {
            return Some(payload);
        }
        carry(v, self.carry, edge_idx, payload);
        None
    }
}

/// Hold `payload` for fused edge `edge_idx` as its slot would: appended for a buffer-all edge,
/// replacing the held one otherwise.
fn carry(v: &mut SerialView<'_>, carry: &mut Carry, edge_idx: usize, payload: CorrelatedPayload) {
    let core = &mut *v.core;
    if cfg!(feature = "metrics") && core.run_config.metrics_level.is_detailed() {
        core.telemetry
            .record_edge_fused_handoff(edge_idx, payload.inner.is_storage_unique());
    }
    if let Some(probe) = &core.run_config.frame_probe {
        probe.record_fused(edge_idx);
    }
    let keep_all = core
        .direct_slots
        .get(edge_idx)
        .is_some_and(|slot| slot.keeps_all());
    let held = carry.iter_mut().find(|(edge, _)| *edge == edge_idx);
    match held {
        Some((_, held)) if !keep_all => {
            *held = payload;
            if cfg!(feature = "metrics") && core.run_config.metrics_level.is_basic() {
                let reason =
                    crate::executor::serial_direct_slot::replace_reason(v.edges.get(edge_idx));
                core.telemetry
                    .record_edge_pressure_event(edge_idx, reason, 1);
            }
        }
        _ => carry.push((edge_idx, payload)),
    }
}

/// Move what `carry` holds for fused edge `edge_idx` into `inputs` at `port`; whether it held
/// any (the edge's slot is empty then, so the caller skips it).
pub(crate) fn take_carried(
    v: &mut SerialView<'_>,
    node_idx: usize,
    edge_idx: usize,
    port: &PortId,
    carry: &mut Carry,
    inputs: &mut Vec<NodePort>,
) -> bool {
    let mut taken = false;
    while let Some(at) = carry.iter().position(|(edge, _)| *edge == edge_idx) {
        let (_, payload) = carry.remove(at);
        if cfg!(feature = "metrics") && v.core.run_config.metrics_level.is_detailed() {
            let bytes = v
                .core
                .data_size_inspectors
                .estimate_payload_bytes(&payload.inner);
            v.core.telemetry.record_node_transport_in(node_idx, bytes);
        }
        inputs.push((port.clone(), payload));
        taken = true;
    }
    taken
}

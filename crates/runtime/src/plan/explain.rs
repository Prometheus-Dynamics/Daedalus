use crate::prelude::*;
use alloc::collections::BTreeMap;

use daedalus_planner::ComputeAffinity;
use serde::{Deserialize, Serialize};

use super::{
    BackpressureStrategy, DemandError, DemandSlice, RuntimeEdgePolicy, RuntimeEdgeTransport,
    RuntimePlan, RuntimeSink,
};
use crate::handles::PortId;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimePlanExplanation {
    #[serde(default)]
    pub backpressure: BackpressureStrategy,
    pub nodes: Vec<RuntimeNodeExplanation>,
    pub edges: Vec<RuntimeEdgeExplanation>,
    /// Edges whose adapter path copies a frame-like payload
    /// ([`RuntimeEdgeExplanation::copies_frame`]).
    #[serde(default)]
    pub copying_edges: Vec<usize>,
    /// Edges whose adapter path changes residency
    /// ([`RuntimeEdgeExplanation::crosses_residency`]).
    #[serde(default)]
    pub crossing_edges: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeNodeExplanation {
    pub index: usize,
    pub id: String,
    pub label: Option<String>,
    pub compute: ComputeAffinity,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEdgeExplanation {
    pub index: usize,
    pub from_node: usize,
    pub from_node_id: String,
    pub from_port: String,
    pub to_node: usize,
    pub to_node_id: String,
    pub to_port: String,
    pub policy: RuntimeEdgePolicy,
    pub transport: Option<RuntimeEdgeTransport>,
    pub adapter_steps: Vec<daedalus_transport::AdapterId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<RuntimeBranchExplanation>,
    pub handoff: RuntimeEdgeHandoff,
    pub handoff_reason: String,
    /// The edge carries a frame-like payload ([`RuntimeEdgeTransport::carries_frame`]) and its
    /// adapter path copies it ([`RuntimeEdgeTransport::copies_data`]) or changes its residency.
    #[serde(default)]
    pub copies_frame: bool,
    /// The adapter path moves the payload between residencies
    /// ([`RuntimeEdgeTransport::crosses_residency`]).
    #[serde(default)]
    pub crosses_residency: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBranchExplanation {
    pub kind: daedalus_transport::BranchKind,
    pub adapter_id: Option<daedalus_transport::AdapterId>,
    pub estimated_bytes: Option<u64>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEdgeHandoff {
    Queue,
    DirectSlot,
}

impl RuntimePlan {
    pub fn explain(&self) -> RuntimePlanExplanation {
        self.explain_with_slice(None)
    }

    pub fn explain_selected(
        &self,
        sinks: &[RuntimeSink],
    ) -> Result<RuntimePlanExplanation, DemandError> {
        let slice = self.demand_slice_for_sinks(sinks)?;
        Ok(self.explain_with_slice(Some(&slice)))
    }

    fn explain_with_slice(&self, slice: Option<&DemandSlice>) -> RuntimePlanExplanation {
        let mut source_port_counts: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
        let mut target_port_counts: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
        for (edge_idx, edge) in self.edges.iter().enumerate() {
            if slice.is_some_and(|slice| !slice.edge_active(edge_idx)) {
                continue;
            }
            *source_port_counts.entry(edge.source_key()).or_default() += 1;
            *target_port_counts.entry(edge.target_key()).or_default() += 1;
        }

        let nodes = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(index, _)| slice.is_none_or(|slice| slice.node_active(*index)))
            .map(|(index, node)| RuntimeNodeExplanation {
                index,
                id: node.id.clone(),
                label: node.label.clone(),
                compute: node.compute,
            })
            .collect();
        let full_direct = slice
            .is_none()
            .then(|| super::direct_edge_mask_for_active_edges(&self.edges, |_| true));
        let overridden = |policy: &super::RuntimeEdgePolicy| {
            self.backpressure != super::BackpressureStrategy::None
                && policy.bounded_capacity().is_some()
        };
        let edges = self
            .edges
            .iter()
            .enumerate()
            .filter(|(index, _)| slice.is_none_or(|slice| slice.edge_active(*index)))
            .map(|(index, edge)| {
                let transport = self.edge_transports.get(index).cloned().flatten();
                let adapter_steps = transport
                    .as_ref()
                    .map(|transport| transport.adapter_steps.clone())
                    .unwrap_or_default();
                let source_count = source_port_counts
                    .get(&edge.source_key())
                    .copied()
                    .unwrap_or(0);
                let target_count = target_port_counts
                    .get(&edge.target_key())
                    .copied()
                    .unwrap_or(0);
                let overridden = overridden(edge.policy());
                let direct_candidate = !overridden
                    && slice
                        .map(|slice| &slice.direct_edges)
                        .or(full_direct.as_ref())
                        .and_then(|mask| mask.get(index).copied())
                        .unwrap_or(false);
                let (handoff, handoff_reason) = if direct_candidate {
                    (
                        RuntimeEdgeHandoff::DirectSlot,
                        "sole producer of its target port, slot-compatible policy".to_string(),
                    )
                } else {
                    (
                        RuntimeEdgeHandoff::Queue,
                        explain_queue_reason(
                            target_count,
                            super::direct_slot_policy(&edge.policy().pressure),
                            overridden,
                        ),
                    )
                };
                let branch = branch_explanation(transport.as_ref(), source_count, target_count);
                let crosses_residency = transport
                    .as_ref()
                    .is_some_and(RuntimeEdgeTransport::crosses_residency);
                let copies_frame = transport.as_ref().is_some_and(|transport| {
                    transport.carries_frame() && (transport.copies_data() || crosses_residency)
                });
                RuntimeEdgeExplanation {
                    index,
                    from_node: edge.from().0,
                    from_node_id: self
                        .nodes
                        .get(edge.from().0)
                        .map(|node| node.id.clone())
                        .unwrap_or_else(|| format!("node_{}", edge.from().0)),
                    from_port: edge.source_port().to_string(),
                    to_node: edge.to().0,
                    to_node_id: self
                        .nodes
                        .get(edge.to().0)
                        .map(|node| node.id.clone())
                        .unwrap_or_else(|| format!("node_{}", edge.to().0)),
                    to_port: edge.target_port().to_string(),
                    policy: edge.policy().clone(),
                    transport,
                    adapter_steps,
                    branch,
                    handoff,
                    handoff_reason,
                    copies_frame,
                    crosses_residency,
                }
            })
            .collect::<Vec<_>>();
        let flagged = |flag: fn(&RuntimeEdgeExplanation) -> bool| {
            edges
                .iter()
                .filter(|edge| flag(edge))
                .map(|edge| edge.index)
                .collect()
        };
        RuntimePlanExplanation {
            backpressure: self.backpressure.clone(),
            nodes,
            copying_edges: flagged(|edge| edge.copies_frame),
            crossing_edges: flagged(|edge| edge.crosses_residency),
            edges,
        }
    }
}

fn explain_queue_reason(
    target_count: usize,
    slot_policy: bool,
    backpressure_override: bool,
) -> String {
    let mut reasons = Vec::new();
    if !slot_policy {
        reasons.push("pressure policy needs a queue".to_string());
    }
    if backpressure_override {
        reasons.push("graph backpressure strategy overrides bounded edges".to_string());
    }
    if target_count != 1 {
        reasons.push(format!("target fanin={target_count}"));
    }
    if reasons.is_empty() {
        "queue storage selected by current runtime lowering".to_string()
    } else {
        format!("queue storage required or selected: {}", reasons.join(", "))
    }
}

fn branch_explanation(
    transport: Option<&RuntimeEdgeTransport>,
    source_count: usize,
    target_count: usize,
) -> Option<RuntimeBranchExplanation> {
    let step = transport.and_then(|transport| {
        transport.adapter_path.iter().find(|step| {
            matches!(
                step.kind,
                daedalus_transport::AdaptKind::Branch
                    | daedalus_transport::AdaptKind::Cow
                    | daedalus_transport::AdaptKind::CowView
                    | daedalus_transport::AdaptKind::Materialize
            )
        })
    })?;
    let kind = match step.kind {
        daedalus_transport::AdaptKind::Cow | daedalus_transport::AdaptKind::CowView => {
            daedalus_transport::BranchKind::Cow
        }
        daedalus_transport::AdaptKind::Materialize => daedalus_transport::BranchKind::Materialize,
        daedalus_transport::AdaptKind::Branch => daedalus_transport::BranchKind::Domain,
        _ => daedalus_transport::BranchKind::Shared,
    };
    let estimated_bytes = match step.cost.bytes_copied {
        daedalus_transport::CopyCost::None | daedalus_transport::CopyCost::HeaderOnly => Some(0),
        daedalus_transport::CopyCost::Exact(bytes) => Some(bytes),
        daedalus_transport::CopyCost::Proportional => None,
    };
    Some(RuntimeBranchExplanation {
        kind,
        adapter_id: Some(step.adapter.clone()),
        estimated_bytes,
        reason: format!(
            "fanout requires independent payload; source_consumers={source_count}, target_inputs={target_count}"
        ),
    })
}

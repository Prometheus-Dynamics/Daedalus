//! Node fusion: which edges hand their payload straight from producer to consumer inside one
//! scheduled unit, without a direct slot or queue in between (see "Node fusion" in
//! `docs/development.md`).

use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::fmt;

use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, NodeRef, is_host_bridge_metadata};
use serde::{Deserialize, Serialize};

use super::{
    BackpressureStrategy, NodeFire, RuntimeEdge, RuntimeEdgeTransport, RuntimeNode,
    direct_edge_mask_for_active_edges,
};
use crate::handles::PortId;

/// Node metadata that forbids fusing the node with its neighbours: `false` (or `"never"`).
pub const NODE_FUSION_META_KEY: &str = "daedalus.node.fusion";

/// Why an edge is not fused ([`super::RuntimeEdgeExplanation::fusion_block`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FusionBlock {
    /// An end is a host-bridge node.
    HostBridge,
    /// An end is not `ComputeAffinity::CpuOnly`.
    Affinity,
    /// An end sets [`NODE_FUSION_META_KEY`] to `false`.
    OptedOut,
    /// The source port feeds this many edges.
    SourceFanOut(usize),
    /// The target port is fed by this many edges.
    TargetFanIn(usize),
    /// The edge keeps a queue (its pressure policy, or the graph's backpressure strategy).
    Queued,
    /// The edge runs an adapter path of this many steps.
    Adapter(usize),
    /// The consumer fires only when all its required inputs hold values (`fire = "all"`).
    FireAll,
    /// The consumer does not run right after the producer in the schedule order.
    NotAdjacent,
}

impl fmt::Display for FusionBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostBridge => f.write_str("host-bridge boundary"),
            Self::Affinity => f.write_str("an end is not CPU-only"),
            Self::OptedOut => write!(f, "an end opts out ({NODE_FUSION_META_KEY} = false)"),
            Self::SourceFanOut(n) => write!(f, "source port fans out to {n} edges"),
            Self::TargetFanIn(n) => write!(f, "target port has {n} producers"),
            Self::Queued => f.write_str("edge keeps a queue"),
            Self::Adapter(n) => write!(f, "adapter path of {n} steps"),
            Self::FireAll => f.write_str("consumer fires on all inputs"),
            Self::NotAdjacent => f.write_str("consumer does not run right after the producer"),
        }
    }
}

/// The fusion of a plan's edges and the units they form.
#[derive(Clone, Debug, Default)]
pub(crate) struct FusionAnalysis {
    /// Per edge: `None` when fused, else why not.
    pub(crate) edges: Vec<Option<FusionBlock>>,
    /// Runs of consecutive (host bridges aside) schedule-order nodes joined by fused edges, two
    /// or more nodes each.
    pub(crate) units: Vec<Vec<NodeRef>>,
}

impl FusionAnalysis {
    pub(crate) fn fused(&self, edge_idx: usize) -> bool {
        matches!(self.edges.get(edge_idx), Some(None))
    }
}

fn opted_out(node: &RuntimeNode) -> bool {
    match node.metadata.get(NODE_FUSION_META_KEY) {
        Some(Value::Bool(allowed)) => !allowed,
        Some(Value::String(value)) => matches!(value.as_ref(), "never" | "false" | "off"),
        _ => false,
    }
}

/// Decide fusion for every edge: an edge fuses when its producer output feeds only it, its
/// consumer input has no other producer, it is a direct-slot edge without adapters, both ends are
/// CPU-only, non-host-bridge nodes that allow fusion, the consumer fires on any input, and the
/// consumer is the next node the schedule runs after the producer.
pub(crate) fn analyze_fusion(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    transports: &[Option<RuntimeEdgeTransport>],
    order: &[NodeRef],
    backpressure: &BackpressureStrategy,
) -> FusionAnalysis {
    let host = |idx: usize| {
        nodes
            .get(idx)
            .is_none_or(|n| is_host_bridge_metadata(&n.metadata))
    };
    // Next non-host-bridge node the schedule runs after each node.
    let mut next = vec![usize::MAX; nodes.len()];
    let mut previous: Option<usize> = None;
    for node in order.iter().map(|node| node.0).filter(|&idx| !host(idx)) {
        if let Some(slot) = previous.and_then(|prev| next.get_mut(prev)) {
            *slot = node;
        }
        previous = Some(node);
    }
    let mut sources: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
    let mut targets: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
    for edge in edges {
        *sources.entry(edge.source_key()).or_default() += 1;
        *targets.entry(edge.target_key()).or_default() += 1;
    }
    let direct = direct_edge_mask_for_active_edges(edges, |_| true);
    let overridden = *backpressure != BackpressureStrategy::None;
    let block = |idx: usize, edge: &RuntimeEdge| -> Option<FusionBlock> {
        let (from, to) = (edge.from().0, edge.to().0);
        let (Some(producer), Some(consumer)) = (nodes.get(from), nodes.get(to)) else {
            return Some(FusionBlock::HostBridge);
        };
        if host(from) || host(to) {
            return Some(FusionBlock::HostBridge);
        }
        if producer.compute != ComputeAffinity::CpuOnly
            || consumer.compute != ComputeAffinity::CpuOnly
        {
            return Some(FusionBlock::Affinity);
        }
        if opted_out(producer) || opted_out(consumer) {
            return Some(FusionBlock::OptedOut);
        }
        let fan_out = sources.get(&edge.source_key()).copied().unwrap_or(0);
        if fan_out != 1 {
            return Some(FusionBlock::SourceFanOut(fan_out));
        }
        let fan_in = targets.get(&edge.target_key()).copied().unwrap_or(0);
        if fan_in != 1 {
            return Some(FusionBlock::TargetFanIn(fan_in));
        }
        if !direct[idx] || (overridden && edge.policy().bounded_capacity().is_some()) {
            return Some(FusionBlock::Queued);
        }
        let steps = transports
            .get(idx)
            .and_then(Option::as_ref)
            .map_or(0, |transport| transport.adapter_steps.len());
        if steps != 0 {
            return Some(FusionBlock::Adapter(steps));
        }
        if NodeFire::from_metadata(&consumer.metadata) == NodeFire::All {
            return Some(FusionBlock::FireAll);
        }
        (next[from] != to).then_some(FusionBlock::NotAdjacent)
    };
    let edge_blocks: Vec<Option<FusionBlock>> = edges
        .iter()
        .enumerate()
        .map(|(idx, edge)| block(idx, edge))
        .collect();

    let mut joined = BTreeMap::new();
    for (edge, blocked) in edges.iter().zip(&edge_blocks) {
        if blocked.is_none() {
            joined.insert(edge.from().0, edge.to().0);
        }
    }
    let mut units: Vec<Vec<NodeRef>> = Vec::new();
    let mut current: Vec<NodeRef> = Vec::new();
    for node in order.iter().copied().filter(|node| !host(node.0)) {
        let continues = current
            .last()
            .is_some_and(|last| joined.get(&last.0) == Some(&node.0));
        if !continues && current.len() > 1 {
            units.push(core::mem::take(&mut current));
        }
        if !continues {
            current.clear();
        }
        current.push(node);
    }
    if current.len() > 1 {
        units.push(current);
    }
    FusionAnalysis {
        edges: edge_blocks,
        units,
    }
}

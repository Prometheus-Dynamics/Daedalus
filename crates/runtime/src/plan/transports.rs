use std::collections::BTreeMap;

use daedalus_data::model::Value;
use daedalus_planner::{EdgeResolutionExplanation, EdgeResolutionKind};
use daedalus_registry::typeexpr_transport_key;

use super::{RuntimeEdge, RuntimeEdgeTransport, RuntimeNode};

/// Planner edge explanations keyed by `(from_node, from_port, to_node, to_port)` ids. When the
/// planner recorded several explanations for the same key, the first one wins.
pub(super) struct EdgeExplanations(
    BTreeMap<(String, String, String, String), EdgeResolutionExplanation>,
);

impl EdgeExplanations {
    pub(super) fn from_metadata(graph_metadata: &BTreeMap<String, Value>) -> Self {
        let mut index = BTreeMap::new();
        for edge in daedalus_planner::edge_explanations(graph_metadata) {
            let key = (
                edge.from_node.clone(),
                edge.from_port.clone(),
                edge.to_node.clone(),
                edge.to_port.clone(),
            );
            index.entry(key).or_insert(edge);
        }
        Self(index)
    }

    /// Explanation for a runtime edge between `from` and `to`.
    pub(super) fn get(
        &self,
        from: &RuntimeNode,
        to: &RuntimeNode,
        edge: &RuntimeEdge,
    ) -> Option<&EdgeResolutionExplanation> {
        if self.0.is_empty() {
            return None;
        }
        self.0.get(&(
            from.id.clone(),
            edge.source_port().to_string(),
            to.id.clone(),
            edge.target_port().to_string(),
        ))
    }
}

fn conversion_transport(edge: &EdgeResolutionExplanation) -> Option<RuntimeEdgeTransport> {
    if edge.resolution_kind != EdgeResolutionKind::Conversion || edge.converter_steps.is_empty() {
        return None;
    }
    Some(RuntimeEdgeTransport {
        from_type: edge.from_type.clone(),
        to_type: edge.to_type.clone(),
        source_transport: Some(typeexpr_transport_key(&edge.from_type)),
        target_transport: Some(
            edge.transport_target
                .clone()
                .unwrap_or_else(|| typeexpr_transport_key(&edge.to_type)),
        ),
        target_access: edge.target_access,
        target_exclusive: edge.target_exclusive,
        target_residency: edge.target_residency,
        transport_target: edge.transport_target.clone(),
        adapter_steps: edge
            .converter_steps
            .iter()
            .map(|step| daedalus_transport::AdapterId::new(step.clone()))
            .collect(),
        adapter_path: edge.adapter_path.clone(),
        expected_adapter_cost: Some(edge.total_cost),
    })
}

pub(super) fn runtime_edge_transports(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    explanations: &EdgeExplanations,
) -> Vec<Option<RuntimeEdgeTransport>> {
    let transports: Vec<_> = edges
        .iter()
        .map(|edge| {
            let from = nodes.get(edge.from().0)?;
            let to = nodes.get(edge.to().0)?;
            conversion_transport(explanations.get(from, to, edge)?)
        })
        .collect();
    if transports.iter().all(Option::is_none) {
        Vec::new()
    } else {
        transports
    }
}

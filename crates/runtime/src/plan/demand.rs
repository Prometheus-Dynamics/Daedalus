use std::collections::{BTreeMap, BTreeSet, VecDeque};

use daedalus_planner::GraphNodeSelector;
use serde::{Deserialize, Serialize};

use super::{
    RuntimeEdge, RuntimeEdgeTransport, RuntimeNode, RuntimePlan, RuntimeSink,
    direct_edge_mask_for_active_edges,
};
use crate::handles::PortId;

impl RuntimePlan {
    /// Compute an "active nodes" mask for demand-driven execution given a set of sinks.
    ///
    /// The returned mask can be passed to `Executor::with_active_nodes` to skip unrelated branches.
    pub fn active_nodes_for_sinks(&self, sinks: &[RuntimeSink]) -> Result<Vec<bool>, DemandError> {
        Ok(self.demand_slice_for_sinks(sinks)?.active_nodes)
    }

    pub fn demand_slice_for_sinks(
        &self,
        sinks: &[RuntimeSink],
    ) -> Result<DemandSlice, DemandError> {
        if sinks.is_empty() {
            return Ok(DemandSlice::full(
                self.nodes.len(),
                self.edges.len(),
                &self.edges,
                &self.edge_transports,
            ));
        }
        let mut cached = Vec::with_capacity(sinks.len());
        for sink in sinks {
            let Some(entry) = self.demand_slices.iter().find(|entry| entry.sink == *sink) else {
                return demand_slice_for_sinks_with_transports(
                    &self.nodes,
                    &self.edges,
                    &self.edge_transports,
                    sinks,
                );
            };
            cached.push(entry.slice.clone());
        }
        Ok(DemandSlice::union(
            cached,
            &self.nodes,
            &self.edges,
            &self.edge_transports,
        ))
    }

    pub fn demand_summary_for_slice(
        &self,
        selected_sinks: &[RuntimeSink],
        slice: &DemandSlice,
    ) -> DemandTelemetry {
        let full_clones = fanout_clone_count(&self.edges, None);
        let active_clones = fanout_clone_count(&self.edges, Some(slice));
        let planned_inactive_adapter_edges: Vec<usize> = self
            .edge_transports
            .iter()
            .enumerate()
            .filter_map(|(idx, transport)| {
                (!slice.edge_active(idx)
                    && transport
                        .as_ref()
                        .is_some_and(|transport| !transport.adapter_steps.is_empty()))
                .then_some(idx)
            })
            .collect();
        let avoided_transport_bytes = planned_inactive_adapter_edges
            .iter()
            .filter_map(|idx| {
                self.edge_transports
                    .get(*idx)
                    .and_then(Option::as_ref)
                    .and_then(|transport| transport.expected_adapter_cost)
            })
            .sum();
        DemandTelemetry {
            selected_sinks: selected_sinks.iter().map(runtime_sink_label).collect(),
            active_nodes: slice.active_node_count(),
            skipped_nodes: self.nodes.len().saturating_sub(slice.active_node_count()),
            active_edges: slice.active_edge_count(),
            skipped_edges: self.edges.len().saturating_sub(slice.active_edge_count()),
            avoided_clones: full_clones.saturating_sub(active_clones),
            avoided_adapter_calls: planned_inactive_adapter_edges.len() as u64,
            planned_inactive_adapter_edges,
            avoided_transport_bytes,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandSliceEntry {
    pub sink: RuntimeSink,
    pub slice: DemandSlice,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub host_input_ports: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub host_output_ports: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapter_edges: Vec<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandSlice {
    pub active_nodes: Vec<bool>,
    pub active_edges: Vec<bool>,
    pub direct_edges: Vec<bool>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub host_input_ports: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub host_output_ports: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapter_edges: Vec<usize>,
}

impl DemandSlice {
    pub fn full(
        node_count: usize,
        edge_count: usize,
        edges: &[RuntimeEdge],
        edge_transports: &[Option<RuntimeEdgeTransport>],
    ) -> Self {
        let active_edges = vec![true; edge_count];
        let mut slice = Self {
            active_nodes: vec![true; node_count],
            active_edges,
            direct_edges: Vec::new(),
            host_input_ports: BTreeSet::new(),
            host_output_ports: BTreeSet::new(),
            adapter_edges: Vec::new(),
        };
        enrich_demand_slice(&mut slice, &[], edges, edge_transports);
        slice
    }

    pub fn active_node_count(&self) -> usize {
        self.active_nodes.iter().filter(|active| **active).count()
    }

    pub fn active_edge_count(&self) -> usize {
        self.active_edges.iter().filter(|active| **active).count()
    }

    pub fn node_active(&self, index: usize) -> bool {
        self.active_nodes.get(index).copied().unwrap_or(false)
    }

    pub fn edge_active(&self, index: usize) -> bool {
        self.active_edges.get(index).copied().unwrap_or(false)
    }

    pub fn union(
        slices: Vec<Self>,
        nodes: &[RuntimeNode],
        edges: &[RuntimeEdge],
        edge_transports: &[Option<RuntimeEdgeTransport>],
    ) -> Self {
        let Some(first) = slices.first() else {
            return Self::default();
        };
        let mut out = Self {
            active_nodes: vec![false; first.active_nodes.len()],
            active_edges: vec![false; first.active_edges.len()],
            direct_edges: Vec::new(),
            host_input_ports: BTreeSet::new(),
            host_output_ports: BTreeSet::new(),
            adapter_edges: Vec::new(),
        };
        for slice in slices {
            for (idx, active) in slice.active_nodes.into_iter().enumerate() {
                if active {
                    out.active_nodes[idx] = true;
                }
            }
            for (idx, active) in slice.active_edges.into_iter().enumerate() {
                if active {
                    out.active_edges[idx] = true;
                }
            }
        }
        enrich_demand_slice(&mut out, nodes, edges, edge_transports);
        out
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandTelemetry {
    pub selected_sinks: Vec<String>,
    pub active_nodes: usize,
    pub skipped_nodes: usize,
    pub active_edges: usize,
    pub skipped_edges: usize,
    pub avoided_clones: u64,
    pub avoided_adapter_calls: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planned_inactive_adapter_edges: Vec<usize>,
    pub avoided_transport_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DemandError {
    SinkSelectorDidNotMatch { sink: RuntimeSink },
}

impl std::fmt::Display for DemandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DemandError::SinkSelectorDidNotMatch { sink } => {
                write!(f, "runtime sink selector did not match any nodes: {sink:?}")
            }
        }
    }
}

impl std::error::Error for DemandError {}

impl DemandTelemetry {
    pub fn is_empty(&self) -> bool {
        self.selected_sinks.is_empty()
            && self.active_nodes == 0
            && self.skipped_nodes == 0
            && self.active_edges == 0
            && self.skipped_edges == 0
            && self.avoided_clones == 0
            && self.avoided_adapter_calls == 0
            && self.planned_inactive_adapter_edges.is_empty()
            && self.avoided_transport_bytes == 0
    }
}

/// Compute a node-activity mask for demand-driven execution given a set of sinks.
///
/// The returned mask can be passed to `Executor::with_active_nodes` to skip unrelated branches.
pub fn active_nodes_mask_for_sinks(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    sinks: &[RuntimeSink],
) -> Result<Vec<bool>, DemandError> {
    Ok(demand_slice_for_sinks(nodes, edges, sinks)?.active_nodes)
}

pub fn demand_slice_for_sinks(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    sinks: &[RuntimeSink],
) -> Result<DemandSlice, DemandError> {
    demand_slice_for_sinks_with_transports(nodes, edges, &[], sinks)
}

pub(super) fn demand_slice_for_sinks_with_transports(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    edge_transports: &[Option<RuntimeEdgeTransport>],
    sinks: &[RuntimeSink],
) -> Result<DemandSlice, DemandError> {
    if sinks.is_empty() {
        return Ok(DemandSlice::full(
            nodes.len(),
            edges.len(),
            edges,
            edge_transports,
        ));
    }

    fn resolve_indices(nodes: &[RuntimeNode], selector: &GraphNodeSelector) -> Vec<usize> {
        if let Some(index) = selector.index {
            if index < nodes.len() {
                return vec![index];
            }
            return Vec::new();
        }
        if let Some(meta) = selector.metadata.as_ref() {
            let key = meta.key.trim();
            if !key.is_empty() {
                return nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, node)| {
                        node.metadata
                            .get(key)
                            .filter(|value| *value == &meta.value)
                            .map(|_| idx)
                    })
                    .collect();
            }
        }
        if let Some(id) = selector.id.as_ref() {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                return nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, node)| (node.id == trimmed).then_some(idx))
                    .collect();
            }
        }
        Vec::new()
    }

    let mut incoming_edges: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (edge_idx, edge) in edges.iter().enumerate() {
        if edge.to().0 < incoming_edges.len() {
            incoming_edges[edge.to().0].push(edge_idx);
        }
    }

    let mut active = vec![false; nodes.len()];
    let mut active_edges = vec![false; edges.len()];
    // Only apply the port filter at the sink node (initial cut). A sink node may be selected
    // through multiple input ports (for example `io.host_output` with `overlay` plus `tx/ty`),
    // so we need to preserve the union of requested ports instead of letting later entries
    // overwrite earlier ones.
    enum SinkPortFilter {
        Any,
        Ports(BTreeSet<String>),
    }
    let mut edge_port_filter: std::collections::HashMap<usize, SinkPortFilter> =
        std::collections::HashMap::new();
    let mut q: VecDeque<usize> = VecDeque::new();

    for sink in sinks {
        let indices = resolve_indices(nodes, &sink.node);
        if indices.is_empty() {
            return Err(DemandError::SinkSelectorDidNotMatch { sink: sink.clone() });
        }
        for idx in indices {
            if let Some(port) = sink.port.as_ref() {
                let trimmed = port.as_str().trim();
                if !trimmed.is_empty() {
                    match edge_port_filter.entry(idx) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            match entry.get_mut() {
                                SinkPortFilter::Any => {}
                                SinkPortFilter::Ports(ports) => {
                                    ports.insert(trimmed.to_string());
                                }
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let mut ports = BTreeSet::new();
                            ports.insert(trimmed.to_string());
                            entry.insert(SinkPortFilter::Ports(ports));
                        }
                    }
                }
            } else {
                edge_port_filter.insert(idx, SinkPortFilter::Any);
            }
            if !active[idx] {
                active[idx] = true;
                q.push_back(idx);
            }
        }
    }

    while let Some(node_idx) = q.pop_front() {
        let filter_ports = edge_port_filter.get(&node_idx);
        for &eidx in incoming_edges
            .get(node_idx)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
        {
            let edge = &edges[eidx];
            if let Some(SinkPortFilter::Ports(filter_ports)) = filter_ports
                && !filter_ports
                    .iter()
                    .any(|port| edge.target_port().eq_ignore_ascii_case(port))
            {
                continue;
            }
            active_edges[eidx] = true;
            let src = edge.from().0;
            if src < active.len() && !active[src] {
                active[src] = true;
                q.push_back(src);
            }
        }
    }

    let mut slice = DemandSlice {
        active_nodes: active,
        active_edges,
        direct_edges: Vec::new(),
        host_input_ports: BTreeSet::new(),
        host_output_ports: BTreeSet::new(),
        adapter_edges: Vec::new(),
    };
    enrich_demand_slice(&mut slice, nodes, edges, edge_transports);
    Ok(slice)
}

pub(super) fn build_host_output_demand_slices(runtime: &RuntimePlan) -> Vec<DemandSliceEntry> {
    let mut entries = Vec::new();
    for (edge_idx, edge) in runtime.edges.iter().enumerate() {
        if !runtime
            .nodes
            .get(edge.to().0)
            .is_some_and(is_host_bridge_runtime_node)
        {
            continue;
        }
        let to_port = edge.target_port().to_string();
        let mut sinks = vec![RuntimeSink::node_index(edge.to().0).port(to_port.clone())];
        if let Some(node) = runtime.nodes.get(edge.to().0) {
            sinks.push(RuntimeSink::node_id(node.id.clone()).port(to_port));
        }
        for sink in sinks {
            let Ok(slice) = demand_slice_for_sinks_with_transports(
                &runtime.nodes,
                &runtime.edges,
                &runtime.edge_transports,
                std::slice::from_ref(&sink),
            ) else {
                continue;
            };
            if !slice.edge_active(edge_idx) {
                continue;
            }
            entries.push(DemandSliceEntry {
                sink,
                host_input_ports: slice.host_input_ports.clone(),
                host_output_ports: slice.host_output_ports.clone(),
                adapter_edges: slice.adapter_edges.clone(),
                slice,
            });
        }
    }
    entries
}

fn enrich_demand_slice(
    slice: &mut DemandSlice,
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    edge_transports: &[Option<RuntimeEdgeTransport>],
) {
    slice.direct_edges = active_direct_edge_set(edges, edge_transports, slice);
    slice.adapter_edges = edge_transports
        .iter()
        .enumerate()
        .filter_map(|(idx, transport)| {
            (slice.edge_active(idx)
                && transport
                    .as_ref()
                    .is_some_and(|transport| !transport.adapter_steps.is_empty()))
            .then_some(idx)
        })
        .collect();
    slice.host_input_ports.clear();
    slice.host_output_ports.clear();
    for (idx, edge) in edges.iter().enumerate() {
        if !slice.edge_active(idx) {
            continue;
        }
        if nodes
            .get(edge.from().0)
            .is_some_and(is_host_bridge_runtime_node)
        {
            slice
                .host_input_ports
                .insert(edge.source_port().to_string());
        }
        if nodes
            .get(edge.to().0)
            .is_some_and(is_host_bridge_runtime_node)
        {
            slice
                .host_output_ports
                .insert(edge.target_port().to_string());
        }
    }
}

fn active_direct_edge_set(
    edges: &[RuntimeEdge],
    edge_transports: &[Option<RuntimeEdgeTransport>],
    slice: &DemandSlice,
) -> Vec<bool> {
    direct_edge_mask_for_active_edges(edges, edge_transports, |idx| slice.edge_active(idx))
}

fn fanout_clone_count(edges: &[RuntimeEdge], slice: Option<&DemandSlice>) -> u64 {
    let mut counts: BTreeMap<(usize, PortId), u64> = BTreeMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        if slice.is_some_and(|slice| !slice.edge_active(idx)) {
            continue;
        }
        *counts.entry(edge.source_key()).or_default() += 1;
    }
    counts.values().map(|count| count.saturating_sub(1)).sum()
}

fn runtime_sink_label(sink: &RuntimeSink) -> String {
    let node = if let Some(index) = sink.node.index {
        format!("#{index}")
    } else if let Some(id) = sink.node.id.as_ref() {
        id.clone()
    } else if sink.node.metadata.is_some() {
        "metadata".to_string()
    } else {
        "unknown".to_string()
    };
    match sink.port.as_ref() {
        Some(port) => format!("{node}.{port}"),
        None => node,
    }
}

fn is_host_bridge_runtime_node(node: &RuntimeNode) -> bool {
    daedalus_planner::is_host_bridge_metadata(&node.metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEdgePolicy;
    use daedalus_planner::ComputeAffinity;
    use daedalus_planner::NodeRef;

    fn test_node(id: &str) -> RuntimeNode {
        RuntimeNode {
            id: id.to_string(),
            stable_id: 0,
            bundle: None,
            label: None,
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        }
    }

    #[test]
    fn active_nodes_mask_keeps_multiple_sink_ports_on_same_node() {
        let nodes = vec![
            test_node("overlay_src"),
            test_node("value_src"),
            test_node("io.host_output"),
        ];
        let edges = vec![
            RuntimeEdge::new(
                NodeRef(0),
                "frame".to_string(),
                NodeRef(2),
                "overlay".to_string(),
                RuntimeEdgePolicy::default(),
            ),
            RuntimeEdge::new(
                NodeRef(1),
                "value".to_string(),
                NodeRef(2),
                "tx".to_string(),
                RuntimeEdgePolicy::default(),
            ),
        ];
        let sinks = vec![
            RuntimeSink {
                node: GraphNodeSelector {
                    index: Some(2),
                    id: None,
                    metadata: None,
                },
                port: Some(crate::handles::PortId::new("overlay")),
            },
            RuntimeSink {
                node: GraphNodeSelector {
                    index: Some(2),
                    id: None,
                    metadata: None,
                },
                port: Some(crate::handles::PortId::new("tx")),
            },
        ];

        let active = active_nodes_mask_for_sinks(&nodes, &edges, &sinks).expect("active mask");
        assert_eq!(active, vec![true, true, true]);
    }

    #[test]
    fn demand_slice_reports_unmatched_sink_with_typed_error() {
        let nodes = vec![test_node("source")];
        let err = demand_slice_for_sinks(
            &nodes,
            &[],
            &[RuntimeSink {
                node: GraphNodeSelector {
                    index: Some(10),
                    id: None,
                    metadata: None,
                },
                port: None,
            }],
        )
        .expect_err("unmatched sink should be typed");

        assert!(matches!(
            err,
            DemandError::SinkSelectorDidNotMatch { sink }
                if sink.node.index == Some(10)
        ));
    }
}

//! The adapter path text a lifecycle record names: its step ids and its detail. Built once per
//! edge and shared, so recording an adapter path clones handles and allocates nothing once the
//! edge is warm.

use alloc::collections::BTreeMap;
use alloc::string::ToString;
use alloc::vec::Vec;

use daedalus_registry::capability::AdapterPathStep;
use daedalus_transport::IdStr;

use super::lifecycle::AdapterSteps;
use crate::plan::RuntimeEdgeTransport;

/// What a lifecycle record names about an edge's adapter path.
#[derive(Clone, Debug, Default)]
pub(crate) struct AdapterText {
    pub(crate) steps: AdapterSteps,
    pub(crate) detail: Option<IdStr>,
}

impl AdapterText {
    fn of(transport: &RuntimeEdgeTransport) -> Self {
        Self {
            steps: AdapterSteps::new(transport.adapter_steps.clone()),
            detail: adapter_path_detail(transport),
        }
    }
}

/// The texts built so far, by edge index. An entry keeps the path it was built from, so an edge
/// whose path later differs (a telemetry reused with another plan) rebuilds its text.
#[derive(Clone, Debug, Default)]
pub(crate) struct AdapterTexts(BTreeMap<usize, AdapterTextEntry>);

#[derive(Clone, Debug)]
struct AdapterTextEntry {
    path: Vec<AdapterPathStep>,
    cost: Option<u64>,
    text: AdapterText,
}

impl PartialEq for AdapterTexts {
    /// Caches never make two telemetries differ.
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for AdapterTexts {}

impl AdapterTexts {
    /// The text of `transport`, the path of edge `edge_idx`. Building it allocates; reading a
    /// built one clones two handles.
    pub(crate) fn get(&mut self, edge_idx: usize, transport: &RuntimeEdgeTransport) -> AdapterText {
        if let Some(entry) = self.0.get(&edge_idx)
            && &*entry.text.steps == transport.adapter_steps.as_slice()
            && entry.path == transport.adapter_path
            && entry.cost == transport.expected_adapter_cost
        {
            return entry.text.clone();
        }
        let text = AdapterText::of(transport);
        self.0.insert(
            edge_idx,
            AdapterTextEntry {
                path: transport.adapter_path.clone(),
                cost: transport.expected_adapter_cost,
                text: text.clone(),
            },
        );
        text
    }
}

/// `adapter_path=[<step>:<kind> -> ...]; expected_cost=<cost>`, or nothing for an edge with no
/// planned path and no cost.
fn adapter_path_detail(edge_transport: &RuntimeEdgeTransport) -> Option<IdStr> {
    if edge_transport.adapter_path.is_empty() && edge_transport.expected_adapter_cost.is_none() {
        return None;
    }
    let steps = edge_transport
        .adapter_path
        .iter()
        .map(|step| format!("{}:{:?}", step.adapter, step.kind))
        .collect::<Vec<_>>()
        .join(" -> ");
    Some(IdStr::new(format!(
        "adapter_path=[{}]; expected_cost={}",
        steps,
        edge_transport
            .expected_adapter_cost
            .map(|cost| cost.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    )))
}

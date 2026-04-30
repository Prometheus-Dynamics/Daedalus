use std::collections::BTreeMap;
use std::time::Duration;

use crate::handles::PortId;
use daedalus_transport::{AdapterId, TypeKey};

use super::{
    DataLifecycleEvent, EdgeMetrics, MetricsLevel, NodeFailure, NodeMetrics, NodePerfMetrics,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TelemetryReportFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<PortId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_key: Option<TypeKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_id: Option<AdapterId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TelemetryReport {
    pub metrics_level: MetricsLevel,
    pub graph_duration: Duration,
    pub unattributed_runtime_duration: Duration,
    pub nodes_executed: usize,
    pub gpu_segments: usize,
    pub gpu_fallbacks: usize,
    pub backpressure_events: usize,
    pub node_timing: BTreeMap<usize, NodeMetrics>,
    pub edge_timing: BTreeMap<usize, EdgeMetrics>,
    pub transport: BTreeMap<usize, EdgeMetrics>,
    pub ownership: BTreeMap<usize, OwnershipReport>,
    pub adapter_paths: Vec<AdapterPathReport>,
    pub capability_sources: Vec<String>,
    pub lifecycle: Vec<DataLifecycleEvent>,
    pub warnings: Vec<String>,
    pub errors: Vec<NodeFailure>,
    pub fallbacks: Vec<String>,
    pub skipped_nodes: Vec<usize>,
    pub hardware_counters: BTreeMap<usize, NodePerfMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnershipReport {
    pub unique_handoffs: u64,
    pub shared_handoffs: u64,
    pub payload_clones: u64,
    pub copied_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AdapterPathReport {
    pub edge: Option<usize>,
    pub node: Option<usize>,
    pub port: Option<String>,
    pub correlation_id: u64,
    pub steps: Vec<String>,
    pub detail: Option<String>,
}

impl TelemetryReport {
    pub fn filter(mut self, filter: &TelemetryReportFilter) -> Self {
        if let Some(node) = filter.node {
            self.node_timing.retain(|idx, _| *idx == node);
            self.hardware_counters.retain(|idx, _| *idx == node);
            self.lifecycle.retain(|event| event.node_idx == Some(node));
            self.adapter_paths.retain(|path| path.node == Some(node));
        }
        if let Some(edge) = filter.edge {
            self.edge_timing.retain(|idx, _| *idx == edge);
            self.transport.retain(|idx, _| *idx == edge);
            self.ownership.retain(|idx, _| *idx == edge);
            self.lifecycle.retain(|event| event.edge_idx == Some(edge));
            self.adapter_paths.retain(|path| path.edge == Some(edge));
        }
        if let Some(port) = &filter.port {
            self.lifecycle
                .retain(|event| event.port.as_deref() == Some(port.as_str()));
            self.adapter_paths
                .retain(|path| path.port.as_deref() == Some(port.as_str()));
        }
        if let Some(type_key) = &filter.type_key {
            self.lifecycle.retain(|event| {
                event
                    .payload
                    .as_ref()
                    .is_some_and(|payload| payload.contains(type_key.as_str()))
            });
        }
        if let Some(adapter_id) = &filter.adapter_id {
            self.lifecycle.retain(|event| {
                event
                    .adapter_steps
                    .iter()
                    .any(|step| step == adapter_id.as_str())
            });
            self.adapter_paths
                .retain(|path| path.steps.iter().any(|step| step == adapter_id.as_str()));
        }
        if let Some(correlation_id) = filter.correlation_id {
            self.lifecycle
                .retain(|event| event.correlation_id == correlation_id);
            self.adapter_paths
                .retain(|path| path.correlation_id == correlation_id);
        }
        self
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_table(&self) -> String {
        let mut out = String::from("section\tid\tmetric\tvalue\n");
        for (idx, node) in &self.node_timing {
            out.push_str(&format!(
                "node\t{idx}\ttotal_ns\t{}\n",
                node.total_duration.as_nanos()
            ));
            out.push_str(&format!(
                "node\t{idx}\thandler_ns\t{}\n",
                node.handler_duration.as_nanos()
            ));
            out.push_str(&format!(
                "node\t{idx}\toverhead_ns\t{}\n",
                node.total_duration
                    .saturating_sub(node.handler_duration)
                    .as_nanos()
            ));
            out.push_str(&format!("node\t{idx}\tcalls\t{}\n", node.calls));
            for (name, value) in &node.custom {
                out.push_str(&format!(
                    "node\t{idx}\tcustom.{name}\t{}\n",
                    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
                ));
            }
        }
        for (idx, edge) in &self.edge_timing {
            out.push_str(&format!(
                "edge\t{idx}\twait_ns\t{}\n",
                edge.total_wait.as_nanos()
            ));
            out.push_str(&format!(
                "edge\t{idx}\tadapter_ns\t{}\n",
                edge.adapter_duration.as_nanos()
            ));
            out.push_str(&format!("edge\t{idx}\tdrops\t{}\n", edge.drops));
            out.push_str(&format!(
                "edge\t{idx}\tpressure_events\t{}\n",
                edge.pressure_events.total
            ));
            out.push_str(&format!(
                "edge\t{idx}\tpeak_queue_bytes\t{}\n",
                edge.peak_queue_bytes
            ));
        }
        for failure in &self.errors {
            out.push_str(&format!(
                "failure\t{}\t{}\t{}\n",
                failure.node_idx, failure.code, failure.message
            ));
        }
        out
    }
}

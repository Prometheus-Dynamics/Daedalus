use std::collections::BTreeMap;
use std::time::Duration;

/// Aggregated timing + diagnostics for a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionTelemetry {
    pub nodes_executed: usize,
    pub cpu_segments: usize,
    pub gpu_segments: usize,
    pub gpu_fallbacks: usize,
    pub backpressure_events: usize,
    pub warnings: smallvec::SmallVec<[String; 8]>,
    pub graph_duration: Duration,
    /// Per-node-instance metrics keyed by the planned node index (`NodeRef.0`).
    pub node_metrics: BTreeMap<usize, NodeMetrics>,
    /// Per-group aggregate metrics keyed by group id (e.g. embedded graphs).
    pub group_metrics: BTreeMap<String, NodeMetrics>,
    /// Per-edge queue wait metrics keyed by the planned edge index.
    pub edge_metrics: BTreeMap<usize, EdgeMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    pub total_duration: Duration,
    pub calls: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeMetrics {
    pub total_wait: Duration,
    pub samples: usize,
}

impl EdgeMetrics {
    pub fn merge(&mut self, other: EdgeMetrics) {
        self.total_wait += other.total_wait;
        self.samples += other.samples;
    }
}

impl NodeMetrics {
    pub fn record(&mut self, duration: Duration) {
        self.total_duration += duration;
        self.calls += 1;
    }

    pub fn merge(&mut self, other: NodeMetrics) {
        self.total_duration += other.total_duration;
        self.calls += other.calls;
    }
}

impl ExecutionTelemetry {
    pub fn merge(&mut self, other: ExecutionTelemetry) {
        self.nodes_executed += other.nodes_executed;
        self.cpu_segments += other.cpu_segments;
        self.gpu_segments += other.gpu_segments;
        self.gpu_fallbacks += other.gpu_fallbacks;
        self.backpressure_events += other.backpressure_events;
        self.warnings.extend(other.warnings);
        self.graph_duration = self.graph_duration.max(other.graph_duration);
        for (node, metrics) in other.node_metrics {
            self.node_metrics.entry(node).or_default().merge(metrics);
        }
        for (group, metrics) in other.group_metrics {
            self.group_metrics.entry(group).or_default().merge(metrics);
        }
        for (edge, metrics) in other.edge_metrics {
            self.edge_metrics.entry(edge).or_default().merge(metrics);
        }
    }

    pub fn record_node_duration(&mut self, node_idx: usize, duration: Duration) {
        self.node_metrics
            .entry(node_idx)
            .or_default()
            .record(duration);
    }

    pub fn record_edge_wait(&mut self, edge_idx: usize, duration: Duration) {
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.total_wait += duration;
        entry.samples += 1;
    }

    pub fn aggregate_groups(&mut self, nodes: &[crate::plan::RuntimeNode]) {
        const GROUP_KEY: &str = "daedalus.embedded_group";
        for (idx, metrics) in &self.node_metrics {
            let Some(node) = nodes.get(*idx) else {
                continue;
            };
            let Some(daedalus_data::model::Value::String(group)) = node.metadata.get(GROUP_KEY) else {
                continue;
            };
            let trimmed = group.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.group_metrics
                .entry(trimmed.to_string())
                .or_default()
                .merge(metrics.clone());
        }
    }
}

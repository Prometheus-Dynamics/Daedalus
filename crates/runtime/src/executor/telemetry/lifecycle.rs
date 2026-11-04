use crate::handles::PortId;
use crate::prelude::*;
use daedalus_transport::IdStr;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceEvent {
    pub node_idx: usize,
    pub start_ns: u64,
    pub duration_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataLifecycleStage {
    Created,
    AcceptedBySource,
    EdgeEnqueued,
    EdgeDequeued,
    Queued,
    Dequeued,
    AdapterStart,
    AdapterEnd,
    AdapterError,
    Adapted,
    Branched,
    Materialized,
    ResidencyCacheHit,
    ResidencyCacheMiss,
    MovedToNode,
    BorrowedByNode,
    MutatedByNode,
    ProducedByNode,
    DeliveredToOutput,
    DroppedByPolicy,
    Released,
    Recycled,
    ReleaseFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DataLifecycleEvent {
    pub correlation_id: u64,
    pub stage: DataLifecycleStage,
    pub at_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_idx: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_idx: Option<usize>,
    /// The port the event concerns. A clone of the port's shared name: no text is copied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<PortId>,
    /// `Payload(<type key>)`, shared by every event that names the type: the telemetry builds each
    /// text once and clones it after that.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<IdStr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapter_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataLifecycleRecord {
    pub correlation_id: u64,
    pub stage: DataLifecycleStage,
    pub node_idx: Option<usize>,
    pub edge_idx: Option<usize>,
    pub port: Option<PortId>,
    pub payload: Option<IdStr>,
    pub adapter_steps: Vec<String>,
    pub detail: Option<String>,
}

impl DataLifecycleRecord {
    pub fn new(correlation_id: u64, stage: DataLifecycleStage) -> Self {
        Self {
            correlation_id,
            stage,
            node_idx: None,
            edge_idx: None,
            port: None,
            payload: None,
            adapter_steps: Vec::new(),
            detail: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeFailure {
    pub node_idx: usize,
    pub node_id: String,
    pub code: String,
    pub message: String,
}

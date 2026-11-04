use crate::handles::PortId;
use crate::portable::Arc;
use crate::prelude::*;
use daedalus_transport::{AdapterId, IdStr};

/// The step ids of the adapter path a lifecycle record names, in order. An edge's list is built
/// once and shared (see `AdapterText`), so naming a path clones a handle and copies no ids. The
/// JSON is the same array of step ids a `Vec<String>` gave.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdapterSteps(Option<Arc<[AdapterId]>>);

impl AdapterSteps {
    /// The steps of a path (an empty path holds no list).
    pub fn new(steps: Vec<AdapterId>) -> Self {
        if steps.is_empty() {
            Self(None)
        } else {
            Self(Some(Arc::from(steps)))
        }
    }

    /// Whether the path has no steps (no list is held).
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl core::ops::Deref for AdapterSteps {
    type Target = [AdapterId];

    fn deref(&self) -> &[AdapterId] {
        self.0.as_deref().unwrap_or(&[])
    }
}

impl serde::Serialize for AdapterSteps {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> serde::Deserialize<'de> for AdapterSteps {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<AdapterId>::deserialize(deserializer).map(Self::new)
    }
}

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
    #[serde(default, skip_serializing_if = "AdapterSteps::is_empty")]
    pub adapter_steps: AdapterSteps,
    /// The adapter path's detail text, or the error text of an `AdapterError`. A shared text: the
    /// telemetry builds the path's detail once per edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<IdStr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataLifecycleRecord {
    pub correlation_id: u64,
    pub stage: DataLifecycleStage,
    pub node_idx: Option<usize>,
    pub edge_idx: Option<usize>,
    pub port: Option<PortId>,
    pub payload: Option<IdStr>,
    pub adapter_steps: AdapterSteps,
    pub detail: Option<IdStr>,
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
            adapter_steps: AdapterSteps::default(),
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

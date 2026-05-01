use serde::{Deserialize, Serialize};

/// How to align multiple input ports within a sync group.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SyncPolicy {
    /// Wait until all ports in the group have at least one payload.
    #[default]
    AllReady,
    /// Use the latest payload per port, emit immediately on any arrival.
    Latest,
    /// Align by an external tag (implementation-defined).
    ZipByTag,
}

/// Sync grouping metadata for a node.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SyncGroup {
    /// Logical name for the group.
    pub name: String,
    /// Alignment policy for this group.
    pub policy: SyncPolicy,
    /// Optional override for backpressure strategy; defaults to engine setting.
    #[serde(default)]
    pub backpressure: Option<crate::policy::BackpressureStrategy>,
    /// Optional override for buffer capacity; defaults to engine/edge depth.
    #[serde(default)]
    pub capacity: Option<usize>,
    /// Ports that participate in this group.
    pub ports: Vec<String>,
}

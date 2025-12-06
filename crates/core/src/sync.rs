use serde::{Deserialize, Serialize};

/// How to align multiple input ports within a sync group.
///
/// ```
/// use daedalus_core::sync::SyncPolicy;
/// let policy = SyncPolicy::Latest;
/// assert_eq!(policy, SyncPolicy::Latest);
/// ```
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
/// ```
/// use daedalus_core::sync::{SyncGroup, SyncPolicy};
///
/// let group = SyncGroup {
///     name: "aligned".to_string(),
///     policy: SyncPolicy::AllReady,
///     backpressure: None,
///     capacity: Some(4),
///     ports: vec!["left".into(), "right".into()],
/// };
/// assert_eq!(group.ports.len(), 2);
/// ```
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

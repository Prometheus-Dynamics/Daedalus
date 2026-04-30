//! Debug helpers for serializing/deserializing runtime plans.
//! ```
//! use daedalus_runtime::{debug, RuntimePlan, RuntimeSegment, RuntimeNode, RuntimeEdgePolicy};
//! use daedalus_planner::ComputeAffinity;
//!
//! let plan = RuntimePlan {
//!     default_policy: RuntimeEdgePolicy::default(),
//!     backpressure: daedalus_runtime::BackpressureStrategy::None,
//!     graph_metadata: Default::default(),
//!     nodes: vec![
//!         RuntimeNode {
//!             id: "a".into(),
//!             stable_id: 0,
//!             bundle: None,
//!             label: None,
//!             compute: ComputeAffinity::CpuOnly,
//!             const_inputs: vec![],
//!             sync_groups: vec![],
//!             metadata: Default::default(),
//!         }
//!     ],
//!     edges: vec![],
//!     edge_transports: vec![],
//!     gpu_edges: vec![],
//!     gpu_entries: vec![],
//!     gpu_exits: vec![],
//!     segments: vec![RuntimeSegment { nodes: vec![], compute: ComputeAffinity::CpuOnly }],
//!     schedule_order: vec![],
//!     gpu_segments: vec![],
//!     demand_slices: vec![],
//! };
//! let json = debug::to_pretty_json(&plan);
//! let round = debug::from_json(&json).unwrap();
//! assert_eq!(plan.nodes.len(), round.nodes.len());
//! ```

use crate::RuntimePlan;

/// Serialize a runtime plan to pretty-printed JSON.
pub fn to_pretty_json(plan: &RuntimePlan) -> String {
    serde_json::to_string_pretty(plan).expect("serialize runtime plan")
}

/// Serialize a runtime plan to compact JSON.
pub fn to_json(plan: &RuntimePlan) -> String {
    serde_json::to_string(plan).expect("serialize runtime plan")
}

/// Deserialize a runtime plan from JSON.
pub fn from_json(s: &str) -> Result<RuntimePlan, serde_json::Error> {
    serde_json::from_str(s)
}

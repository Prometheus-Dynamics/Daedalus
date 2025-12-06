//! Debug helpers for serializing/deserializing plans, useful for goldens/CLI.
//! ```
//! use daedalus_planner::{debug, Graph, ExecutionPlan};
//!
//! let plan = ExecutionPlan::new(Graph::default(), vec![]);
//! let json = debug::to_pretty_json(&plan);
//! let round = debug::from_json(&json).unwrap();
//! assert_eq!(plan.version, round.version);
//! ```
use crate::ExecutionPlan;

/// Serialize a plan to pretty-printed JSON.
pub fn to_pretty_json(plan: &ExecutionPlan) -> String {
    serde_json::to_string_pretty(plan).expect("serialize plan")
}

/// Serialize a plan to compact JSON.
pub fn to_json(plan: &ExecutionPlan) -> String {
    serde_json::to_string(plan).expect("serialize plan")
}

/// Deserialize a plan from JSON.
pub fn from_json(s: &str) -> Result<ExecutionPlan, serde_json::Error> {
    serde_json::from_str(s)
}

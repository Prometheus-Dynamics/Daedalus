//! Debug helpers for serializing/deserializing runtime plans.

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

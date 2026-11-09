//! A finished plan as JSON, for tools and tests.

use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::api::{PlanError, Planned};
use crate::diagnostic::Diagnostic;
use crate::explain::{Explain, explain_plan};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDump {
    pub explain: Explain,
    pub order: Vec<u32>,
    /// The plan's blob, in hex.
    pub blob: String,
    pub warnings: Vec<Diagnostic>,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DebugError {
    Plan(PlanError),
    Json(serde_json::Error),
}

impl fmt::Display for DebugError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DebugError::Plan(error) => error.fmt(f),
            DebugError::Json(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for DebugError {}

pub fn dump(planned: &Planned) -> Result<PlanDump, DebugError> {
    let bytes = planned.blob().map_err(DebugError::Plan)?;
    let mut blob = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(blob, "{byte:02x}");
    }
    Ok(PlanDump {
        explain: explain_plan(planned),
        order: planned.graph.order.clone(),
        blob,
        warnings: planned.warnings.clone(),
    })
}

pub fn to_json(planned: &Planned) -> Result<String, DebugError> {
    serde_json::to_string_pretty(&dump(planned)?).map_err(DebugError::Json)
}

pub fn from_json(text: &str) -> Result<PlanDump, DebugError> {
    serde_json::from_str(text).map_err(DebugError::Json)
}

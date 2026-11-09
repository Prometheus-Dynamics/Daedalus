//! Diagnostics as JSON, for logs: the step list and errors. Allocates, so it is never on the tick
//! path.

use std::string::{String, ToString};
use std::vec::Vec;

use serde::{Deserialize, Serialize};

use crate::HostError;
use crate::io::Host;

/// One step, as a report shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepReport {
    pub index: u32,
    pub wakes: u32,
    pub inputs: u32,
    pub outputs: u32,
    pub always: bool,
    pub may_pend: bool,
    pub reuse: bool,
}

/// The plan's step list, as a report shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReport {
    pub steps: Vec<StepReport>,
}

impl PlanReport {
    /// This plan as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Reads a report back from JSON.
    pub fn from_json(text: &str) -> Option<PlanReport> {
        serde_json::from_str(text).ok()
    }
}

impl Host<'_> {
    /// The step list of the graph this host runs.
    pub fn render_plan(&self) -> PlanReport {
        PlanReport {
            steps: (0..self.runner.step_count() as u32)
                .filter_map(|index| {
                    let meta = self.runner.step_meta(index)?;
                    Some(StepReport {
                        index,
                        wakes: meta.wake.len,
                        inputs: meta.inputs.len,
                        outputs: meta.outputs.len,
                        always: meta.always,
                        may_pend: meta.may_pend,
                        reuse: meta.reuse,
                    })
                })
                .collect(),
        }
    }
}

/// An error as JSON, for logs.
pub fn render_error(error: &HostError) -> String {
    serde_json::to_string(&error.to_string()).unwrap_or_default()
}

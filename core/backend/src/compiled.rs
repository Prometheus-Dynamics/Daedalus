//! What a backend hands back.

use std::fmt;

use daedalus_abi::Step;
use daedalus_types::NodeIndex;

use crate::PortRef;

/// The longest refusal reason, in bytes.
const REASON_CAPACITY: usize = 256;

/// What the planner needs to know about one unit of compiled work, in any form.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnitInfo {
    /// Every node this unit runs, so errors and telemetry can still name them.
    pub covers: Vec<NodeIndex>,
    pub reads: Vec<PortRef>,
    pub writes: Vec<PortRef>,
    /// Indexes into `Region::incoming` of the transfers this unit does as part of its own work.
    pub absorbed: Vec<u32>,
}

#[derive(Debug)]
pub struct StepUnit {
    pub step: Step,
    pub info: UnitInfo,
}

/// Generated Rust, for a graph sealed into a build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedCode {
    pub file: String,
    pub text: String,
}

#[derive(Debug)]
pub enum Compiled {
    /// Runs now, in this process.
    Steps(Vec<StepUnit>),
    /// A stored plan that a loader or small runner reads.
    Blob {
        bytes: Vec<u8>,
        units: Vec<UnitInfo>,
    },
    /// Code compiled ahead of time.
    Source {
        code: GeneratedCode,
        units: Vec<UnitInfo>,
    },
}

/// A backend won't take these nodes. The planner removes each one from this backend and assigns
/// again, until nothing is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// Empty means the whole region.
    pub nodes: Vec<NodeIndex>,
    pub reason: String,
}

impl Refusal {
    /// `reason` is cut to 256 bytes, on a char boundary.
    pub fn new(nodes: Vec<NodeIndex>, reason: &str) -> Refusal {
        let mut end = reason.len().min(REASON_CAPACITY);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        Refusal {
            nodes,
            reason: reason[..end].to_string(),
        }
    }

    pub fn whole_region(reason: &str) -> Refusal {
        Refusal::new(Vec::new(), reason)
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "refused {} node(s): {}", self.nodes.len(), self.reason)
    }
}

impl std::error::Error for Refusal {}

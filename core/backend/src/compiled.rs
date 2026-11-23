//! What a backend hands back.

use std::fmt;
use std::sync::Arc;

use daedalus_abi::Step;
use daedalus_graph::Lit;
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
    /// Whether this unit can return `Pending`. A reader in another region then waits for it.
    pub may_pend: bool,
    /// Inputs this unit converts as it reads them, which nothing else in the plan shows.
    pub decodes: Vec<Decode>,
}

/// An input a unit converts from the form its value arrives in to the one its node reads, as part
/// of its own work: the plan shows it like a transfer, so a cost the graph doesn't name is seen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decode {
    pub input: PortRef,
    /// The type the input is read as.
    pub ty: std::borrow::Cow<'static, str>,
    /// What it is converted from, and why.
    pub reason: std::borrow::Cow<'static, str>,
}

#[derive(Debug)]
pub struct StepUnit {
    pub step: Step,
    pub info: UnitInfo,
    /// How a later value for a constant reaches this unit's steps, when they can take one. `None`
    /// when the unit's constants are fixed as it is compiled.
    pub patch: Option<Arc<dyn Repatch>>,
}

/// Takes a later value for a constant of the steps a backend compiled. A host offers a value
/// between ticks; the step that reads the constant picks it up before it next runs. The value
/// is decoded as it is offered, so one that does not fit is refused there and nothing changes.
pub trait Repatch: Send + Sync + fmt::Debug {
    fn offer(&self, node: NodeIndex, port: &str, value: &Lit) -> Result<(), NotTaken>;

    /// Whether any step that reads through this is still alive.
    fn reads(&self) -> bool;
}

/// Why an offered constant was not taken.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotTaken {
    /// No step reads this constant through here.
    NoReader,
    /// The step that reads it refused the value, in the words it uses when it is compiled.
    Refused(String),
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

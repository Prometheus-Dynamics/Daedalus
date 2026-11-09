//! Why a node couldn't be placed.

use std::fmt;

/// What became of one body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BodyState {
    Took,
    Declined,
    /// The backend took it, then refused it at compile.
    Refused(String),
    /// The host didn't supply this backend.
    NotSupplied,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BodyStatus {
    pub backend: String,
    pub state: BodyState,
}

fn list(bodies: &[BodyStatus]) -> String {
    let parts: Vec<String> = bodies
        .iter()
        .map(|body| match &body.state {
            BodyState::Took => format!("{}: took it", body.backend),
            BodyState::Declined => format!("{}: declined", body.backend),
            BodyState::Refused(reason) => format!("{}: refused ({reason})", body.backend),
            BodyState::NotSupplied => format!("{}: not supplied", body.backend),
        })
        .collect();
    if parts.is_empty() {
        "no bodies".to_string()
    } else {
        parts.join("; ")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AssignError {
    /// The forced backend has no body for this node.
    ForcedUnknown {
        node: String,
        backend: String,
        bodies: Vec<BodyStatus>,
    },
    /// The forced backend has a body but didn't take the node.
    ForcedUnavailable {
        node: String,
        backend: String,
        bodies: Vec<BodyStatus>,
    },
    NodeUnplaceable {
        node: String,
        bodies: Vec<BodyStatus>,
    },
    /// The search went past its limit. Force some of these nodes.
    AssignmentBudget { nodes: Vec<String>, budget: u64 },
    /// More backends than a plan can number.
    TooManyBackends { count: usize },
}

impl fmt::Display for AssignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssignError::ForcedUnknown {
                node,
                backend,
                bodies,
            } => write!(
                f,
                "node `{node}` is forced onto `{backend}`, which has no body for it ({})",
                list(bodies)
            ),
            AssignError::ForcedUnavailable {
                node,
                backend,
                bodies,
            } => write!(
                f,
                "node `{node}` is forced onto `{backend}`, which didn't take it ({})",
                list(bodies)
            ),
            AssignError::NodeUnplaceable { node, bodies } => {
                write!(f, "no backend can run node `{node}` ({})", list(bodies))
            }
            AssignError::AssignmentBudget { nodes, budget } => write!(
                f,
                "placing {} went past {budget} steps; force a backend on some of them",
                nodes.join(", ")
            ),
            AssignError::TooManyBackends { count } => {
                write!(f, "{count} backends is more than a plan can number")
            }
        }
    }
}

impl std::error::Error for AssignError {}

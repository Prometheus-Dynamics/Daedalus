//! Turns an assignment into compiled regions.

mod compile;
mod form;
mod refusal;
pub mod rules;

use std::fmt;

use daedalus_backend::{Backend, Compiled, PortRef, RateId};
use daedalus_registry::Registry;
use daedalus_types::BackendIndex;

use crate::assign::Assignment;
use crate::assign::error::AssignError;
use crate::solve::Graph;
use crate::solve::Rate;
use crate::solve::type_table::TypeTable;
use crate::stitch::StitchError;
use crate::stitch::transfers::Routing;
pub use refusal::build;
use rules::Rule;

/// A region before compiling. Numbered by its lowest node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Formed {
    pub backend: BackendIndex,
    pub nodes: Vec<u32>,
    pub inputs: Vec<PortRef>,
    pub outputs: Vec<PortRef>,
    pub rate: RateId,
}

#[derive(Debug)]
pub struct CompiledRegion {
    pub formed: Formed,
    pub compiled: Compiled,
}

#[derive(Debug)]
pub struct Regions {
    pub assignment: Assignment,
    pub regions: Vec<CompiledRegion>,
    /// Indexed by `RateId`.
    pub rates: Vec<(Rate, Rate)>,
    /// How many rounds of assign and compile it took.
    pub rounds: u32,
    pub routing: Routing,
}

pub struct Input<'a> {
    pub graph: &'a Graph,
    pub registry: &'a Registry,
    /// Highest priority first.
    pub backends: &'a [&'a dyn Backend],
    pub types: &'a TypeTable,
    pub budget: u64,
    pub merge_allowed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegionError {
    Assign(Vec<AssignError>),
    Stitch(StitchError),
    /// A forced node's backend refused it. There's nowhere else for it to go.
    ForcedRefused {
        node: String,
        backend: String,
        reason: String,
    },
    /// A backend's output broke the contract.
    BackendContract {
        backend: String,
        region: u32,
        rule: &'static str,
        detail: String,
    },
    /// An internal error: formation made a region that breaks a rule.
    RuleBroken {
        rule: Rule,
        region: u32,
    },
}

impl fmt::Display for RegionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegionError::Assign(errors) => {
                let text: Vec<String> = errors.iter().map(ToString::to_string).collect();
                f.write_str(&text.join("; "))
            }
            RegionError::Stitch(error) => error.fmt(f),
            RegionError::ForcedRefused {
                node,
                backend,
                reason,
            } => {
                write!(
                    f,
                    "node `{node}` is forced onto `{backend}`, which refused it: {reason}"
                )
            }
            RegionError::BackendContract {
                backend,
                region,
                rule,
                detail,
            } => {
                write!(
                    f,
                    "backend `{backend}` broke {rule} on region {region}: {detail}"
                )
            }
            RegionError::RuleBroken { rule, region } => {
                write!(f, "internal error: region {region} breaks {rule}")
            }
        }
    }
}

impl std::error::Error for RegionError {}

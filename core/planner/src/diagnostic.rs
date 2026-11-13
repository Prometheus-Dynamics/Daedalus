//! One shape for everything the planner can report.

use serde::{Deserialize, Serialize};

use crate::api::PlanError;
use crate::assign::error::AssignError;
use crate::control::ControlError;
use crate::pipeline::SolveError;
use crate::regions::RegionError;
use crate::solve::consts::ConstError;
use crate::solve::convert::ConvertError;
use crate::solve::defaults::DefaultsError;
use crate::solve::exclusive::ExclusiveError;
use crate::solve::expand::ExpandError;
use crate::solve::hydrate::HydrateError;
use crate::solve::order::OrderError;
use crate::solve::ports::PortError;
use crate::solve::rates::RateError;
use crate::solve::rewrite::RewriteError;
use crate::solve::typecheck::TypeCheckError;
use crate::stitch::StitchError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Code {
    NodeMissing,
    PortMissing,
    PortExtra,
    PortDuplicate,
    GroupMissing,
    GroupDepth,
    NodeOutOfRange,
    BadFire,
    TypeInvalid,
    TypeMismatch,
    ConstMismatch,
    ConverterMissing,
    AccessViolation,
    RewriteFailed,
    ScheduleConflict,
    RateMismatch,
    SyncGroupRate,
    RateInvalid,
    RateOverflow,
    ForcedUnknown,
    ForcedUnavailable,
    ForcedRefused,
    NodeUnplaceable,
    AssignmentBudget,
    TooManyBackends,
    BackendContract,
    RuleBroken,
    NoTransfer,
    NoMemory,
    FormUnavailable,
    TransferUnavailable,
    Encode,
    Write,
    LintWarning,
    ControlBodyMissing,
    ControlBodyEdge,
    ControlBodyInvalid,
    ControlMaxInvalid,
    ControlBodyWaits,
}

/// Only errors stop planning. Warnings come back with the plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
}

/// Both ends of an edge whose types don't line up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mismatch {
    pub from_node: String,
    pub from_port: String,
    pub to_node: String,
    pub to_port: String,
    pub from_type: String,
    pub to_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: Code,
    pub severity: Severity,
    pub message: String,
    pub node: Option<String>,
    pub port: Option<String>,
    /// The lint that raised this.
    pub rule: Option<String>,
    pub suggestions: Vec<String>,
    pub mismatch: Option<Mismatch>,
}

impl Diagnostic {
    fn error(code: Code, message: impl ToString) -> Diagnostic {
        Diagnostic {
            code,
            severity: Severity::Error,
            message: message.to_string(),
            node: None,
            port: None,
            rule: None,
            suggestions: Vec::new(),
            mismatch: None,
        }
    }

    fn on(mut self, node: &str) -> Diagnostic {
        self.node = Some(node.to_string());
        self
    }

    fn at(self, node: &str, port: &str) -> Diagnostic {
        let mut out = self.on(node);
        out.port = Some(port.to_string());
        out
    }

    pub(crate) fn lint(rule: &str, node: &str, port: Option<&str>, message: String) -> Diagnostic {
        let mut out = Diagnostic::error(Code::LintWarning, message).on(node);
        out.severity = Severity::Warning;
        out.port = port.map(str::to_string);
        out.rule = Some(rule.to_string());
        out
    }
}

impl From<&ExpandError> for Diagnostic {
    fn from(error: &ExpandError) -> Diagnostic {
        let base = |code| Diagnostic::error(code, error);
        match error {
            ExpandError::GroupDepth { .. } => base(Code::GroupDepth),
            ExpandError::GroupMissing { id } => base(Code::GroupMissing).on(id),
            ExpandError::PortMissing { node, port, .. } => base(Code::PortMissing).at(node, port),
            ExpandError::NodeOutOfRange { .. } => base(Code::NodeOutOfRange),
        }
    }
}

impl From<&DefaultsError> for Diagnostic {
    fn from(error: &DefaultsError) -> Diagnostic {
        let DefaultsError::BadFire { node, .. } = error;
        Diagnostic::error(Code::BadFire, error).on(node)
    }
}

impl From<&HydrateError> for Diagnostic {
    fn from(error: &HydrateError) -> Diagnostic {
        let HydrateError::NodeMissing { id, suggestions } = error;
        let mut out = Diagnostic::error(Code::NodeMissing, error).on(id);
        out.suggestions = suggestions.clone();
        out
    }
}

impl From<&PortError> for Diagnostic {
    fn from(error: &PortError) -> Diagnostic {
        let base = |code| Diagnostic::error(code, error);
        match error {
            PortError::PortMissing { node, port, .. } => base(Code::PortMissing).at(node, port),
            PortError::PortExtra { node, port, .. } => base(Code::PortExtra).at(node, port),
            PortError::PortDuplicate { node, port, .. } => base(Code::PortDuplicate).at(node, port),
            PortError::TypeInvalid { node, port, .. } => base(Code::TypeInvalid).at(node, port),
            PortError::NodeOutOfRange { .. } => base(Code::NodeOutOfRange),
        }
    }
}

impl From<&TypeCheckError> for Diagnostic {
    fn from(error: &TypeCheckError) -> Diagnostic {
        let TypeCheckError::TypeMismatch {
            from_node,
            from_port,
            to_node,
            to_port,
            from_type,
            to_type,
        } = error;
        let mut out = Diagnostic::error(Code::TypeMismatch, error).at(to_node, to_port);
        out.mismatch = Some(Mismatch {
            from_node: from_node.clone(),
            from_port: from_port.clone(),
            to_node: to_node.clone(),
            to_port: to_port.clone(),
            from_type: from_type.clone(),
            to_type: to_type.clone(),
        });
        out
    }
}

impl From<&ConstError> for Diagnostic {
    fn from(error: &ConstError) -> Diagnostic {
        let ConstError::TypeMismatch { node, port, .. } = error;
        Diagnostic::error(Code::ConstMismatch, error).at(node, port)
    }
}

impl From<&ConvertError> for Diagnostic {
    fn from(error: &ConvertError) -> Diagnostic {
        let ConvertError::ConverterMissing {
            from_node,
            from_port,
            to_node,
            to_port,
            from_type,
            to_type,
            ..
        } = error;
        let mut out = Diagnostic::error(Code::ConverterMissing, error).at(to_node, to_port);
        out.mismatch = Some(Mismatch {
            from_node: from_node.clone(),
            from_port: from_port.clone(),
            to_node: to_node.clone(),
            to_port: to_port.clone(),
            from_type: from_type.clone(),
            to_type: to_type.clone(),
        });
        out
    }
}

impl From<&ExclusiveError> for Diagnostic {
    fn from(error: &ExclusiveError) -> Diagnostic {
        let ExclusiveError::CopyMissing { node, port, .. } = error;
        Diagnostic::error(Code::AccessViolation, error).at(node, port)
    }
}

impl From<&RewriteError> for Diagnostic {
    fn from(error: &RewriteError) -> Diagnostic {
        Diagnostic::error(Code::RewriteFailed, error)
    }
}

impl From<&OrderError> for Diagnostic {
    fn from(error: &OrderError) -> Diagnostic {
        Diagnostic::error(Code::ScheduleConflict, error)
    }
}

impl From<&RateError> for Diagnostic {
    fn from(error: &RateError) -> Diagnostic {
        let base = |code| Diagnostic::error(code, error);
        match error {
            RateError::RateMismatch { node, .. } => base(Code::RateMismatch).on(node),
            RateError::SyncGroupRate { node, .. } => base(Code::SyncGroupRate).on(node),
            RateError::RateInvalid { node } => base(Code::RateInvalid).on(node),
            RateError::RateOverflow { node } => base(Code::RateOverflow).on(node),
        }
    }
}

impl From<&ControlError> for Diagnostic {
    fn from(error: &ControlError) -> Diagnostic {
        let base = |code| Diagnostic::error(code, error);
        match error {
            ControlError::BodyMissing { node, .. } => base(Code::ControlBodyMissing).on(node),
            ControlError::BodyEdge { node, port, .. } => base(Code::ControlBodyEdge).at(node, port),
            ControlError::BodyInvalid { node, .. } | ControlError::BodyShape { node, .. } => {
                base(Code::ControlBodyInvalid).on(node)
            }
            ControlError::TypeMismatch { node, port, .. } => {
                base(Code::TypeMismatch).at(node, port)
            }
            ControlError::MaxInvalid { node, .. } => base(Code::ControlMaxInvalid).on(node),
            ControlError::BodyWaits { node, .. } => base(Code::ControlBodyWaits).on(node),
        }
    }
}

impl From<&SolveError> for Diagnostic {
    fn from(error: &SolveError) -> Diagnostic {
        match error {
            SolveError::Expand(error) => error.into(),
            SolveError::Defaults(error) => error.into(),
            SolveError::Hydrate(error) => error.into(),
            SolveError::Ports(error) => error.into(),
            SolveError::Types(error) => error.into(),
            SolveError::Consts(error) => error.into(),
            SolveError::Convert(error) => error.into(),
            SolveError::Exclusive(error) => error.into(),
            SolveError::Rewrite(error) => error.into(),
            SolveError::Order(error) => error.into(),
            SolveError::Rates(error) => error.into(),
            SolveError::Control(error) => error.into(),
        }
    }
}

impl From<&AssignError> for Diagnostic {
    fn from(error: &AssignError) -> Diagnostic {
        let base = |code| Diagnostic::error(code, error);
        match error {
            AssignError::ForcedUnknown { node, .. } => base(Code::ForcedUnknown).on(node),
            AssignError::ForcedUnavailable { node, .. } => base(Code::ForcedUnavailable).on(node),
            AssignError::NodeUnplaceable { node, .. } => base(Code::NodeUnplaceable).on(node),
            AssignError::AssignmentBudget { .. } => base(Code::AssignmentBudget),
            AssignError::TooManyBackends { .. } => base(Code::TooManyBackends),
        }
    }
}

impl From<&StitchError> for Diagnostic {
    fn from(error: &StitchError) -> Diagnostic {
        let code = match error {
            StitchError::NoTransfer { .. } => Code::NoTransfer,
            StitchError::NoMemory { .. } | StitchError::UnknownSpace { .. } => Code::NoMemory,
        };
        Diagnostic::error(code, error)
    }
}

impl RegionError {
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        match self {
            RegionError::Assign(errors) => errors.iter().map(Diagnostic::from).collect(),
            RegionError::Stitch(error) => vec![error.into()],
            RegionError::ForcedRefused { node, .. } => {
                vec![Diagnostic::error(Code::ForcedRefused, self).on(node)]
            }
            RegionError::BackendContract { .. } => {
                vec![Diagnostic::error(Code::BackendContract, self)]
            }
            RegionError::RuleBroken { .. } => vec![Diagnostic::error(Code::RuleBroken, self)],
        }
    }
}

impl PlanError {
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        match self {
            PlanError::Solve(errors) => errors.iter().map(Diagnostic::from).collect(),
            PlanError::Regions(error) => error.diagnostics(),
            PlanError::FormUnavailable { .. } => {
                vec![Diagnostic::error(Code::FormUnavailable, self)]
            }
            PlanError::TransferUnavailable { .. } => {
                vec![Diagnostic::error(Code::TransferUnavailable, self)]
            }
            PlanError::Control { .. } => vec![Diagnostic::error(Code::ControlBodyInvalid, self)],
            PlanError::Encode(_) => vec![Diagnostic::error(Code::Encode, self)],
            PlanError::Write { .. } => vec![Diagnostic::error(Code::Write, self)],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingPort {
    pub node: String,
    pub port: String,
}

/// What an editor needs to mark up a broken graph.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bundle {
    pub missing_nodes: Vec<String>,
    pub missing_ports: Vec<MissingPort>,
    pub missing_groups: Vec<String>,
    pub type_mismatches: Vec<Mismatch>,
}

pub fn bundle(diagnostics: &[Diagnostic]) -> Bundle {
    let mut out = Bundle::default();
    for item in diagnostics {
        match (item.code, &item.node, &item.port) {
            (Code::NodeMissing, Some(node), _) => out.missing_nodes.push(node.clone()),
            (Code::GroupMissing, Some(node), _) => out.missing_groups.push(node.clone()),
            (Code::PortMissing, Some(node), Some(port)) => {
                out.missing_ports.push(MissingPort {
                    node: node.clone(),
                    port: port.clone(),
                });
            }
            _ => {}
        }
        if let Some(mismatch) = &item.mismatch {
            out.type_mismatches.push(mismatch.clone());
        }
    }
    out
}

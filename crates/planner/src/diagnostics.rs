use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Where a diagnostic applies.
///
/// ```
/// use daedalus_planner::DiagnosticSpan;
/// let span = DiagnosticSpan { pass: "validate".into(), node: Some("n1".into()), port: None };
/// assert_eq!(span.pass, "validate");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticSpan {
    pub pass: String,
    pub node: Option<String>,
    pub port: Option<String>,
}

/// Planner diagnostic codes (non-exhaustive).
///
/// ```
/// use daedalus_planner::DiagnosticCode;
/// let code = DiagnosticCode::NodeMissing;
/// assert_eq!(format!("{code:?}"), "NodeMissing");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DiagnosticCode {
    NodeMissing,
    PortMissing,
    UnresolvedInput,
    ConverterMissing,
    TypeMismatch,
    GpuUnsupported,
    ScheduleConflict,
    AccessViolation,
    LintWarning,
}

/// Planner diagnostic entry.
///
/// ```
/// use daedalus_planner::{Diagnostic, DiagnosticCode};
/// let diag = Diagnostic::new(DiagnosticCode::PortMissing, "missing")
///     .in_pass("validate")
///     .at_node("node");
/// assert_eq!(diag.span.pass, "validate");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub message: String,
    pub span: DiagnosticSpan,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

impl Diagnostic {
    /// Create a diagnostic with a code and message.
    pub fn new(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            span: DiagnosticSpan {
                pass: String::new(),
                node: None,
                port: None,
            },
            metadata: BTreeMap::new(),
        }
    }

    /// Set the pass name that emitted this diagnostic.
    pub fn in_pass(mut self, pass: &'static str) -> Self {
        self.span.pass = pass.to_string();
        self
    }

    /// Attach node context for this diagnostic.
    pub fn at_node(mut self, node: impl Into<String>) -> Self {
        self.span.node = Some(node.into());
        self
    }

    /// Attach port context for this diagnostic.
    pub fn at_port(mut self, port: impl Into<String>) -> Self {
        self.span.port = Some(port.into());
        self
    }

    /// Attach machine-readable metadata to the diagnostic (for UI repair workflows).
    pub fn with_meta(
        mut self,
        key: impl Into<String>,
        value: daedalus_data::model::Value,
    ) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsBundle {
    pub missing_nodes: Vec<MissingNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_groups: Vec<MissingGroup>,
    pub missing_ports: Vec<MissingPort>,
    pub type_mismatches: Vec<TypeMismatch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingNode {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingGroup {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingPort {
    pub node_id: String,
    pub port: String,
    pub direction: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMismatch {
    pub node_id: String,
    pub port: String,
    pub detail: String,
}

pub fn bundle(diagnostics: &[Diagnostic]) -> DiagnosticsBundle {
    let mut out = DiagnosticsBundle::default();
    for diag in diagnostics {
        match diag.code {
            DiagnosticCode::NodeMissing => {
                let group_id = diag
                    .metadata
                    .get("missing_group_id")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::String(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .unwrap_or_default();
                let node_id = diag
                    .metadata
                    .get("missing_node_id")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::String(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .or_else(|| diag.span.node.clone())
                    .unwrap_or_default();
                let suggestions = diag
                    .metadata
                    .get("suggestions")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::List(items) => Some(
                            items
                                .iter()
                                .filter_map(|v| match v {
                                    daedalus_data::model::Value::String(s) => Some(s.to_string()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>(),
                        ),
                        _ => None,
                    })
                    .unwrap_or_default();
                if !group_id.is_empty() {
                    out.missing_groups.push(MissingGroup {
                        group_id,
                        suggestions,
                    });
                } else if !node_id.is_empty() {
                    out.missing_nodes.push(MissingNode { node_id, suggestions });
                }
            }
            DiagnosticCode::PortMissing => {
                let node_id = diag.span.node.clone().unwrap_or_default();
                let port = diag
                    .metadata
                    .get("missing_port")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::String(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .or_else(|| diag.span.port.clone())
                    .unwrap_or_default();
                let direction = diag
                    .metadata
                    .get("missing_port_direction")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::String(s) => {
                            let trimmed = s.trim();
                            (!trimmed.is_empty()).then(|| trimmed.to_string())
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown".to_string());
                let available = diag
                    .metadata
                    .get("available_ports")
                    .and_then(|v| match v {
                        daedalus_data::model::Value::List(items) => Some(
                            items
                                .iter()
                                .filter_map(|v| match v {
                                    daedalus_data::model::Value::String(s) => Some(s.to_string()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>(),
                        ),
                        _ => None,
                    })
                    .unwrap_or_default();
                if !node_id.is_empty() && !port.is_empty() {
                    out.missing_ports.push(MissingPort {
                        node_id,
                        port,
                        direction,
                        available,
                    });
                }
            }
            DiagnosticCode::TypeMismatch => {
                let node_id = diag.span.node.clone().unwrap_or_default();
                let port = diag.span.port.clone().unwrap_or_default();
                let detail = diag.message.clone();
                if !node_id.is_empty() {
                    out.type_mismatches.push(TypeMismatch { node_id, port, detail });
                }
            }
            _ => {}
        }
    }
    out
}

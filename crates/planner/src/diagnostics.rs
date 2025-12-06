use serde::{Deserialize, Serialize};

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub message: String,
    pub span: DiagnosticSpan,
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
}

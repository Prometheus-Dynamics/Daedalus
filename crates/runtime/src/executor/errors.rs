use crate::prelude::*;

/// Structured node error for better diagnostics.
///
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeError {
    Handler(String),
    MissingHandler { node: String, stable_id: u128 },
    ExternalHandlerUnavailable { node: String, stable_id: u128 },
    InvalidInput(String),
    BackpressureDrop(String),
}

impl core::fmt::Display for NodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NodeError::Handler(s) => write!(f, "{s}"),
            NodeError::MissingHandler { node, stable_id } => {
                write!(f, "missing handler for node {node} ({stable_id:x})")
            }
            NodeError::ExternalHandlerUnavailable { node, stable_id } => {
                write!(
                    f,
                    "external node {node} ({stable_id:x}) requires an external handler"
                )
            }
            NodeError::InvalidInput(s) => write!(f, "invalid input: {s}"),
            NodeError::BackpressureDrop(s) => write!(f, "backpressure drop: {s}"),
        }
    }
}

impl core::error::Error for NodeError {}

impl From<daedalus_transport::TypeKeyError> for NodeError {
    fn from(error: daedalus_transport::TypeKeyError) -> Self {
        NodeError::Handler(error.to_string())
    }
}

impl NodeError {
    /// `InvalidInput("missing <port>")`: a required input had no value (what `#[node]` handlers
    /// return; one out-of-line constructor instead of a `format!` per port).
    #[cold]
    pub fn missing_input(port: &str) -> Self {
        NodeError::InvalidInput(format!("missing {port}"))
    }

    /// Return a stable string code for this error.
    pub fn code(&self) -> &'static str {
        match self {
            NodeError::Handler(_) => "handler_error",
            NodeError::MissingHandler { .. } => "missing_handler",
            NodeError::ExternalHandlerUnavailable { .. } => "external_handler_unavailable",
            NodeError::InvalidInput(_) => "invalid_input",
            NodeError::BackpressureDrop(_) => "backpressure_drop",
        }
    }

    /// Whether the error is retryable.
    pub fn retryable(&self) -> bool {
        matches!(self, NodeError::BackpressureDrop(_))
    }
}

/// Execution errors surfaced by the runtime executor.
///
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum ExecuteError {
    /// GPU is required for a segment but no GPU handle is available.
    GpuUnavailable {
        segment: Vec<daedalus_planner::NodeRef>,
    },
    /// Handler failed with a message.
    HandlerFailed { node: String, error: NodeError },
    /// Handler panicked (caught and converted into an error).
    HandlerPanicked { node: String, message: String },
}

/// Errors detected while preparing executor-owned runtime state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutorBuildError {
    #[error("stable id collision: id='{previous}' and id='{current}' map to {stable_id:x}")]
    StableIdCollision {
        previous: String,
        current: String,
        stable_id: u128,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutorMaskError {
    #[error("{mask} mask length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        mask: &'static str,
        expected: usize,
        actual: usize,
    },
}

impl core::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExecuteError::GpuUnavailable { .. } => write!(f, "gpu unavailable for segment"),
            ExecuteError::HandlerFailed { node, error } => {
                write!(f, "handler failed on node {node}: {error}")
            }
            ExecuteError::HandlerPanicked { node, message } => {
                write!(f, "handler panicked on node {node}: {message}")
            }
        }
    }
}

impl core::error::Error for ExecuteError {}

impl ExecuteError {
    /// Return a stable string code for this error.
    pub fn code(&self) -> &'static str {
        match self {
            ExecuteError::GpuUnavailable { .. } => "gpu_unavailable",
            ExecuteError::HandlerFailed { .. } => "handler_failed",
            ExecuteError::HandlerPanicked { .. } => "handler_panicked",
        }
    }

    /// Whether the error is retryable.
    pub fn retryable(&self) -> bool {
        matches!(self, ExecuteError::GpuUnavailable { .. })
    }
}

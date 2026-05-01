use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable error codes for `daedalus-core`.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum CoreErrorCode {
    InvalidId,
    InvalidTick,
    InvalidSequence,
    ChannelClosed,
    ChannelFull,
    ChannelEmpty,
    Unsupported,
    Internal,
}

/// Structured core error with a stable code and human-friendly message.
///
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[non_exhaustive]
#[error("{code:?}: {message}")]
pub struct CoreError {
    code: CoreErrorCode,
    message: String,
}

impl CoreError {
    pub fn new(code: CoreErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> CoreErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Attach additional context while keeping the same code.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        let combined = format!("{}: {}", context.into(), self.message);
        Self {
            code: self.code,
            message: combined,
        }
    }
}

/// Convenience alias for core results.
pub type CoreResult<T> = Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip() {
        let err = CoreError::new(CoreErrorCode::InvalidId, "bad id");
        let json = serde_json::to_string(&err).expect("serialize");
        let back: CoreError = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.code(), CoreErrorCode::InvalidId);
        assert_eq!(back.message(), "bad id");
    }

    #[test]
    fn display_includes_code() {
        let err = CoreError::new(CoreErrorCode::ChannelFull, "full");
        let rendered = err.to_string();
        assert!(rendered.contains("ChannelFull"));
        assert!(rendered.contains("full"));
    }
}

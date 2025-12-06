use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable error codes for `daedalus-data`.
///
/// ```
/// use daedalus_data::errors::DataErrorCode;
/// let code = DataErrorCode::InvalidType;
/// assert_eq!(format!("{code:?}"), "InvalidType");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataErrorCode {
    InvalidDescriptor,
    InvalidType,
    UnknownConverter,
    CycleDetected,
    UnsupportedFeature,
    Serialization,
    Internal,
}

/// Structured data-layer error.
///
/// ```
/// use daedalus_data::errors::{DataError, DataErrorCode};
/// let err = DataError::new(DataErrorCode::InvalidDescriptor, "bad descriptor");
/// assert_eq!(err.code(), DataErrorCode::InvalidDescriptor);
/// ```
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("{code:?}: {message}")]
pub struct DataError {
    code: DataErrorCode,
    message: String,
}

impl DataError {
    pub fn new(code: DataErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> DataErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Convenience alias for data results.
pub type DataResult<T> = Result<T, DataError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip() {
        let err = DataError::new(DataErrorCode::InvalidType, "bad type");
        let json = serde_json::to_string(&err).expect("serialize");
        let back: DataError = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.code(), DataErrorCode::InvalidType);
        assert_eq!(back.message(), "bad type");
    }
}

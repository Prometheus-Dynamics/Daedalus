use std::io;
use thiserror::Error;

use daedalus_planner::Diagnostic;
use daedalus_registry::diagnostics::RegistryError;
use daedalus_runtime::executor::ExecuteError;

/// Engine errors surfaced to callers.
///
/// ```ignore
/// use daedalus_engine::EngineError;
/// let err = EngineError::FeatureDisabled("gpu");
/// assert!(format!("{err}").contains("gpu"));
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EngineError {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("registry error: {0}")]
    Registry(#[from] RegistryError),
    #[error("planner diagnostics: {0:?}")]
    Planner(Vec<Diagnostic>),
    #[error("runtime failed: {0}")]
    Runtime(#[from] ExecuteError),
    #[error("bundle parse error at {path}: {error}")]
    BundleParse { path: String, error: String },
    #[cfg(feature = "gpu")]
    #[error("gpu selection failed: {0}")]
    Gpu(#[from] daedalus_gpu::GpuError),
    #[error("feature '{0}' is disabled at compile time")]
    FeatureDisabled(&'static str),
}

impl EngineError {
    /// Convenience constructor for I/O errors.
    pub fn io(path: impl Into<String>, source: io::Error) -> Self {
        EngineError::Io {
            path: path.into(),
            source,
        }
    }
}

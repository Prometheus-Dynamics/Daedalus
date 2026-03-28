pub use daedalus_data::model::Value;
#[cfg(feature = "gpu")]
use daedalus_gpu::DataCell;
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

/// Runtime payload carried over edges. Keep minimal and cheap to clone.
///
/// ```
/// use daedalus_runtime::executor::RuntimeValue;
/// let payload = RuntimeValue::Unit;
/// assert!(matches!(payload, RuntimeValue::Unit));
/// ```
#[derive(Clone, Debug)]
pub enum RuntimeValue {
    Unit,
    Bytes(Arc<[u8]>),
    Value(Value),
    Any(Arc<dyn Any + Send + Sync>),
    #[cfg(feature = "gpu")]
    Data(DataCell),
}

/// Correlated payload with a shared emission identifier.
///
/// ```
/// use daedalus_runtime::executor::{CorrelatedValue, RuntimeValue};
/// let correlated = CorrelatedValue::from_edge(RuntimeValue::Unit);
/// assert!(correlated.correlation_id > 0);
/// ```
#[derive(Clone, Debug)]
pub struct CorrelatedValue {
    pub correlation_id: u64,
    pub inner: RuntimeValue,
    pub enqueued_at: Instant,
}

impl CorrelatedValue {
    /// Wrap an edge payload with a new correlation id.
    pub fn from_edge(inner: RuntimeValue) -> Self {
        Self {
            correlation_id: next_correlation_id(),
            inner,
            enqueued_at: Instant::now(),
        }
    }
}

/// Generate a new correlation id.
///
/// ```
/// use daedalus_runtime::executor::next_correlation_id;
/// let a = next_correlation_id();
/// let b = next_correlation_id();
/// assert!(b > a);
/// ```
pub fn next_correlation_id() -> u64 {
    static CORR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    CORR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

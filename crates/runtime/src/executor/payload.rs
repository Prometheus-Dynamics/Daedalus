pub use daedalus_data::model::Value;
#[cfg(feature = "gpu")]
use daedalus_gpu::ErasedPayload;
#[cfg(feature = "gpu")]
use daedalus_gpu::GpuImageHandle;
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

/// Runtime payload carried over edges. Keep minimal and cheap to clone.
///
/// ```
/// use daedalus_runtime::executor::EdgePayload;
/// let payload = EdgePayload::Unit;
/// assert!(matches!(payload, EdgePayload::Unit));
/// ```
#[derive(Clone, Debug)]
pub enum EdgePayload {
    Unit,
    Bytes(Arc<[u8]>),
    Value(Value),
    Any(Arc<dyn Any + Send + Sync>),
    #[cfg(feature = "gpu")]
    Payload(ErasedPayload),
    #[cfg(feature = "gpu")]
    GpuImage(GpuImageHandle),
}

/// Correlated payload with a shared emission identifier.
///
/// ```
/// use daedalus_runtime::executor::{CorrelatedPayload, EdgePayload};
/// let correlated = CorrelatedPayload::from_edge(EdgePayload::Unit);
/// assert!(correlated.correlation_id > 0);
/// ```
#[derive(Clone, Debug)]
pub struct CorrelatedPayload {
    pub correlation_id: u64,
    pub inner: EdgePayload,
    pub enqueued_at: Instant,
}

impl CorrelatedPayload {
    /// Wrap an edge payload with a new correlation id.
    pub fn from_edge(inner: EdgePayload) -> Self {
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

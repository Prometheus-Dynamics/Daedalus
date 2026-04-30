use std::time::Instant;

/// Correlated transport payload with a shared emission identifier.
#[derive(Clone, Debug)]
pub struct CorrelatedPayload {
    pub correlation_id: u64,
    pub inner: daedalus_transport::Payload,
    pub enqueued_at: Instant,
}

impl CorrelatedPayload {
    /// Wrap an edge payload with a new correlation id.
    pub fn from_edge(inner: daedalus_transport::Payload) -> Self {
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

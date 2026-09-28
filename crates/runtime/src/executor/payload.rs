use std::time::Instant;

/// Transport payload moving along a runtime edge.
#[derive(Clone, Debug)]
pub struct CorrelatedPayload {
    /// Emission identifier; taken from the payload lineage so host and edge events line up.
    pub correlation_id: u64,
    pub inner: daedalus_transport::Payload,
    /// Set when the payload is queued on an edge with basic metrics enabled.
    pub enqueued_at: Option<Instant>,
}

impl CorrelatedPayload {
    /// Wrap an edge payload, reusing its lineage correlation id.
    pub fn from_edge(inner: daedalus_transport::Payload) -> Self {
        Self {
            correlation_id: inner.correlation_id(),
            inner,
            enqueued_at: None,
        }
    }
}

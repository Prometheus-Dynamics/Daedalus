use daedalus_core::platform::Instant;

/// Transport payload moving along a runtime edge.
#[derive(Clone, Debug)]
pub struct CorrelatedPayload {
    /// Emission identifier; taken from the payload lineage so host and edge events line up.
    pub correlation_id: u64,
    pub inner: daedalus_transport::Payload,
    /// Set when the payload is queued on an edge with basic metrics enabled.
    pub enqueued_at: Option<Instant>,
    /// The host's version stamp for this value, when the host declared one for its input port
    /// (`HostBridgeHandle::set_input_version`). A reusable node compares this stamp instead of
    /// the payload's identity (see `executor::reuse`); nothing else reads it.
    pub version: Option<u64>,
}

impl CorrelatedPayload {
    /// Wrap an edge payload, reusing its lineage correlation id.
    pub fn from_edge(inner: daedalus_transport::Payload) -> Self {
        Self {
            correlation_id: inner.correlation_id(),
            inner,
            enqueued_at: None,
            version: None,
        }
    }

    /// This payload with the host's version stamp `version` (`None` clears it).
    pub(crate) fn with_version(mut self, version: Option<u64>) -> Self {
        self.version = version;
        self
    }
}

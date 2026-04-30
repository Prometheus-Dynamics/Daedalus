use serde::{Deserialize, Serialize};

/// Backpressure strategy applied to runtime edge queues.
///
/// These strategies are synchronous graph-tick policies. They never park the current thread or
/// await queue capacity; bounded strategies either keep the existing queued payload, drop/reject
/// the incoming payload, or report overflow according to the selected variant.
///
/// ```
/// use daedalus_core::policy::BackpressureStrategy;
/// let strategy = BackpressureStrategy::BoundedQueues;
/// assert_eq!(strategy, BackpressureStrategy::BoundedQueues);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BackpressureStrategy {
    /// No runtime-level override; each edge follows its own pressure policy.
    #[default]
    None,
    /// Preserve bounded queue capacity by rejecting incoming payloads when a bounded edge is full.
    ///
    /// This is a nonblocking drop/reject policy, not blocking backpressure. It records a
    /// backpressure event, retains the payload already in the queue, and does not enqueue the
    /// overflowing incoming payload.
    BoundedQueues,
    /// Reject incoming payloads when a bounded edge is full and record the pressure as overflow.
    ///
    /// The runtime records the overflow in telemetry; execution failure behavior is controlled by
    /// the executor's fail-fast configuration.
    ErrorOnOverflow,
}

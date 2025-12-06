use serde::{Deserialize, Serialize};

/// Backpressure strategy applied to queues.
///
/// ```
/// use daedalus_core::policy::BackpressureStrategy;
/// let strategy = BackpressureStrategy::BoundedQueues;
/// assert_eq!(strategy, BackpressureStrategy::BoundedQueues);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BackpressureStrategy {
    /// No special backpressure; allow defaults.
    #[default]
    None,
    /// Apply bounded queues to edges.
    BoundedQueues,
    /// Treat queue overflow as an error.
    ErrorOnOverflow,
}

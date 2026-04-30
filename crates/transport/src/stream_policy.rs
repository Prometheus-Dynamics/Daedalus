use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::CorrelationId;

/// Queue pressure policy used by sources, edges, and host outputs.
///
/// These policies are non-blocking. A producer either enqueues, replaces, drops,
/// or receives [`FeedOutcome::Backpressured`] when the selected policy cannot
/// accept more payloads immediately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PressurePolicy {
    BufferAll,
    DropNewest,
    DropOldest,
    LatestOnly,
    Bounded {
        capacity: usize,
        overflow: OverflowPolicy,
    },
    Coalesce {
        window: Duration,
        strategy: CoalesceStrategy,
    },
    ErrorOnFull,
}

impl Default for PressurePolicy {
    fn default() -> Self {
        Self::Bounded {
            capacity: 1,
            overflow: OverflowPolicy::DropOldest,
        }
    }
}

/// Overflow behavior for bounded pressure policies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    DropIncoming,
    DropOldest,
    Backpressure,
    Error,
}

/// Coalescing behavior for high-rate input streams.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoalesceStrategy {
    KeepNewest,
    KeepOldest,
}

/// Freshness policy used to decide whether queued payloads are still worth executing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessPolicy {
    #[default]
    PreserveAll,
    LatestBySequence,
    LatestByTimestamp,
    MaxAge(Duration),
    MaxLag {
        frames: u64,
    },
}

/// Reason a feed/drop decision was made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    Backpressure,
    DropNewest,
    DropOldest,
    LatestOnlyReplace,
    MaxAge,
    MaxLag,
    Closed,
    ErrorOnFull,
}

/// Result of feeding a payload into a continuous graph input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedOutcome {
    Accepted {
        correlation_id: CorrelationId,
    },
    Replaced {
        old: CorrelationId,
        new: CorrelationId,
    },
    Dropped {
        correlation_id: CorrelationId,
        reason: DropReason,
    },
    Backpressured,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PolicyValidationError {
    #[error("PreserveAll requires bounded capacity or buffer-all pressure")]
    UnboundedPreserveAll,
}

pub fn validate_stream_policy(
    pressure: &PressurePolicy,
    freshness: &FreshnessPolicy,
) -> Result<(), PolicyValidationError> {
    if matches!(freshness, FreshnessPolicy::PreserveAll)
        && !matches!(
            pressure,
            PressurePolicy::Bounded { .. } | PressurePolicy::BufferAll
        )
    {
        return Err(PolicyValidationError::UnboundedPreserveAll);
    }
    Ok(())
}

use serde::{Deserialize, Serialize};

use daedalus_data::model::Value;
use daedalus_transport::{FreshnessPolicy, OverflowPolicy, PressurePolicy};

pub const EDGE_PRESSURE_POLICY_KEY: &str = "daedalus.edge.pressure";
pub const EDGE_CAPACITY_KEY: &str = "daedalus.edge.capacity";
pub const EDGE_FRESHNESS_POLICY_KEY: &str = "daedalus.edge.freshness";
pub const EDGE_PRESSURE_FIFO: &str = "fifo";
pub const EDGE_PRESSURE_LATEST_ONLY: &str = "latest_only";
pub const EDGE_PRESSURE_NEWEST_WINS: &str = "newest_wins";
pub const EDGE_PRESSURE_BOUNDED: &str = "bounded";
pub const EDGE_PRESSURE_DROP_NEWEST: &str = "drop_newest";
pub const EDGE_PRESSURE_DROP_OLDEST: &str = "drop_oldest";
pub const EDGE_PRESSURE_ERROR_ON_FULL: &str = "error_on_full";
pub const EDGE_PRESSURE_COALESCE: &str = "coalesce";
pub const EDGE_OVERFLOW_POLICY_KEY: &str = "daedalus.edge.overflow";
pub const EDGE_FRESHNESS_LATEST_BY_SEQUENCE: &str = "latest_by_sequence";
pub const EDGE_FRESHNESS_LATEST_BY_TIMESTAMP: &str = "latest_by_timestamp";
pub const EDGE_FRESHNESS_MAX_AGE: &str = "max_age";
pub const EDGE_FRESHNESS_MAX_LAG: &str = "max_lag";

/// Runtime policy for one internal edge.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEdgePolicy {
    #[serde(default)]
    pub pressure: PressurePolicy,
    #[serde(default)]
    pub freshness: FreshnessPolicy,
}

impl RuntimeEdgePolicy {
    pub fn fifo() -> Self {
        Self {
            pressure: PressurePolicy::BufferAll,
            freshness: FreshnessPolicy::PreserveAll,
        }
    }

    pub fn latest_only() -> Self {
        Self {
            pressure: PressurePolicy::LatestOnly,
            freshness: FreshnessPolicy::LatestBySequence,
        }
    }

    pub fn bounded(capacity: usize) -> Self {
        Self {
            pressure: PressurePolicy::Bounded {
                capacity: capacity.max(1),
                overflow: OverflowPolicy::DropOldest,
            },
            freshness: FreshnessPolicy::PreserveAll,
        }
    }

    pub fn is_latest_only(&self) -> bool {
        matches!(self.pressure, PressurePolicy::LatestOnly)
    }

    pub fn bounded_capacity(&self) -> Option<usize> {
        match self.pressure {
            PressurePolicy::Bounded { capacity, .. } => Some(capacity.max(1)),
            _ => None,
        }
    }
}

impl Default for RuntimeEdgePolicy {
    fn default() -> Self {
        Self::fifo()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EdgePolicyMetadataError {
    UnknownPressurePolicy { policy: String },
    UnknownFreshnessPolicy { policy: String },
}

pub(crate) fn edge_policy_from_metadata(
    metadata: &std::collections::BTreeMap<String, Value>,
) -> Result<RuntimeEdgePolicy, EdgePolicyMetadataError> {
    let policy = metadata
        .get(EDGE_PRESSURE_POLICY_KEY)
        .and_then(|value| match value {
            Value::String(s) => Some(s.as_ref()),
            _ => None,
        })
        .unwrap_or(EDGE_PRESSURE_FIFO);
    let pressure = match policy {
        EDGE_PRESSURE_LATEST_ONLY | EDGE_PRESSURE_NEWEST_WINS => PressurePolicy::LatestOnly,
        EDGE_PRESSURE_DROP_NEWEST => PressurePolicy::DropNewest,
        EDGE_PRESSURE_DROP_OLDEST => PressurePolicy::DropOldest,
        EDGE_PRESSURE_ERROR_ON_FULL => PressurePolicy::ErrorOnFull,
        EDGE_PRESSURE_BOUNDED => {
            let cap = metadata
                .get(EDGE_CAPACITY_KEY)
                .and_then(|value| match value {
                    Value::Int(n) => u64::try_from(*n).ok(),
                    _ => None,
                })
                .and_then(|cap| usize::try_from(cap).ok())
                .unwrap_or(1)
                .max(1);
            PressurePolicy::Bounded {
                capacity: cap,
                overflow: OverflowPolicy::DropOldest,
            }
        }
        EDGE_PRESSURE_FIFO => PressurePolicy::BufferAll,
        unknown => {
            return Err(EdgePolicyMetadataError::UnknownPressurePolicy {
                policy: unknown.to_string(),
            });
        }
    };
    let freshness = metadata
        .get(EDGE_FRESHNESS_POLICY_KEY)
        .and_then(|value| match value {
            Value::String(s) => Some(s.as_ref()),
            _ => None,
        })
        .map(|policy| match policy {
            EDGE_FRESHNESS_LATEST_BY_SEQUENCE => Ok(FreshnessPolicy::LatestBySequence),
            EDGE_FRESHNESS_LATEST_BY_TIMESTAMP => Ok(FreshnessPolicy::LatestByTimestamp),
            EDGE_FRESHNESS_MAX_AGE => Ok(FreshnessPolicy::MaxAge(std::time::Duration::ZERO)),
            EDGE_FRESHNESS_MAX_LAG => Ok(FreshnessPolicy::MaxLag { frames: 0 }),
            unknown => Err(EdgePolicyMetadataError::UnknownFreshnessPolicy {
                policy: unknown.to_string(),
            }),
        })
        .transpose()?
        .unwrap_or(FreshnessPolicy::PreserveAll);
    Ok(RuntimeEdgePolicy {
        pressure,
        freshness,
    })
}
